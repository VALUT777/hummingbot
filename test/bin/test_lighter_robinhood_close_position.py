from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from bin.lighter_robinhood_close_position import (
    ClosePositionError,
    ConnectorCloseClient,
    close_long_position,
)
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType


ACCOUNT_INDEX = 31855


def snapshot(position, *, active_orders=None, pending=False, orders=None, fetched_at=1):
    return {
        "account_index": ACCOUNT_INDEX,
        "request_started_at": fetched_at,
        "fetched_at": fetched_at,
        "net_position": Decimal(position),
        "net_position_known": True,
        "active_orders": active_orders or [],
        "orders_by_client_id": orders or {},
        "pending_submissions_unknown": pending,
        "available_margin": Decimal("0"),
        "available_margin_known": True,
        "collateral_token": "USDG",
        "position_mode": "ONEWAY",
        "leverage": Decimal("5"),
        "leverage_confirmed": True,
        "market_state_known": True,
        "market_status": "active",
    }


def terminal(client_id, fill, status="filled"):
    return {
        client_id: {
            "client_order_id": client_id,
            "status": status,
            "terminal": True,
            "cumulative_fill_base": Decimal(fill),
            "cumulative_fill_known": True,
        }
    }


class FakeCloseClient:
    def __init__(self, snapshots, client_ids=None):
        self.snapshots = list(snapshots)
        self.client_ids = list(client_ids or ["101", "102"])
        self.submissions = []
        self.snapshot_requests = []

    async def snapshot(self, client_order_ids):
        self.snapshot_requests.append(tuple(client_order_ids))
        if not self.snapshots:
            raise AssertionError("unexpected snapshot request")
        return self.snapshots.pop(0)

    async def submit_reduce_only_market_sell(self, amount):
        self.submissions.append({"amount": amount, "reduce_only": True, "side": "SELL"})
        return self.client_ids.pop(0)


@pytest.mark.asyncio
async def test_closes_long_reduce_only_and_requires_two_distinct_flat_observations():
    client = FakeCloseClient([
        snapshot("51", fetched_at=1),
        snapshot("0", orders=terminal("101", "51"), fetched_at=2),
        snapshot("0", orders=terminal("101", "51"), fetched_at=3),
    ])

    result = await close_long_position(client, expected_account_index=ACCOUNT_INDEX, poll_interval=0)

    assert result.initial_position == Decimal("51")
    assert result.closed_amount == Decimal("51")
    assert result.attempts == 1
    assert client.submissions == [{"amount": Decimal("51"), "reduce_only": True, "side": "SELL"}]


@pytest.mark.asyncio
async def test_already_flat_is_idempotent_and_does_not_submit():
    client = FakeCloseClient([
        snapshot("0", fetched_at=1),
        snapshot("0", fetched_at=2),
    ])

    result = await close_long_position(client, expected_account_index=ACCOUNT_INDEX, poll_interval=0)

    assert result.already_flat is True
    assert result.attempts == 0
    assert client.submissions == []


@pytest.mark.asyncio
async def test_terminal_partial_fill_submits_only_authoritative_residual():
    client = FakeCloseClient([
        snapshot("51", fetched_at=1),
        snapshot("21", orders=terminal("101", "30"), fetched_at=2),
        snapshot("0", orders={**terminal("101", "30"), **terminal("102", "21")}, fetched_at=3),
        snapshot("0", orders={**terminal("101", "30"), **terminal("102", "21")}, fetched_at=4),
    ])

    result = await close_long_position(
        client, expected_account_index=ACCOUNT_INDEX, max_attempts=2, poll_interval=0,
    )

    assert [call["amount"] for call in client.submissions] == [Decimal("51"), Decimal("21")]
    assert result.closed_amount == Decimal("51")
    assert result.attempts == 2


@pytest.mark.asyncio
async def test_unknown_order_outcome_never_retries():
    client = FakeCloseClient([
        snapshot("51", fetched_at=1),
        snapshot("51", pending=True, orders={
            "101": {
                "client_order_id": "101", "status": "unknown", "terminal": False,
                "cumulative_fill_base": None, "cumulative_fill_known": False,
            }
        }, fetched_at=2),
        snapshot("51", pending=True, orders={}, fetched_at=3),
    ])

    with pytest.raises(ClosePositionError, match="outcome is unknown"):
        await close_long_position(
            client, expected_account_index=ACCOUNT_INDEX, poll_limit=2, poll_interval=0,
        )

    assert len(client.submissions) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("position", ["-1", "NaN"])
async def test_short_or_invalid_position_fails_before_submission(position):
    client = FakeCloseClient([snapshot(position)])

    with pytest.raises(ClosePositionError):
        await close_long_position(client, expected_account_index=ACCOUNT_INDEX, poll_interval=0)

    assert client.submissions == []


@pytest.mark.asyncio
async def test_wrong_account_or_existing_orders_fails_before_submission():
    wrong = snapshot("51")
    wrong["account_index"] = ACCOUNT_INDEX + 1
    client = FakeCloseClient([wrong])
    with pytest.raises(ClosePositionError, match="account index"):
        await close_long_position(client, expected_account_index=ACCOUNT_INDEX, poll_interval=0)
    assert client.submissions == []

    client = FakeCloseClient([snapshot("51", active_orders=[{"client_order_id": "manual"}])])
    with pytest.raises(ClosePositionError, match="active order"):
        await close_long_position(client, expected_account_index=ACCOUNT_INDEX, poll_interval=0)
    assert client.submissions == []


@pytest.mark.asyncio
async def test_incoherent_terminal_fill_and_position_never_retries():
    client = FakeCloseClient([
        snapshot("51", fetched_at=1),
        snapshot("10", orders=terminal("101", "30"), fetched_at=2),
    ])

    with pytest.raises(ClosePositionError, match="inconsistent"):
        await close_long_position(
            client, expected_account_index=ACCOUNT_INDEX, poll_limit=1, poll_interval=0,
        )

    assert len(client.submissions) == 1


@pytest.mark.asyncio
async def test_connector_adapter_uses_only_reduce_only_market_close():
    connector = MagicMock()
    connector._update_trading_rules = AsyncMock()
    connector.market_info_for_trading_pair.return_value.raw_info = {"last_trade_price": "5.5"}
    connector.quantize_order_amount.return_value = Decimal("51")
    connector._new_client_order_id.return_value = "101"
    connector._place_order = AsyncMock(return_value=("101", 1))

    client_id = await ConnectorCloseClient(connector).submit_reduce_only_market_sell(Decimal("51"))

    assert client_id == "101"
    connector._place_order.assert_awaited_once_with(
        order_id="101",
        trading_pair="LIT-USDG",
        amount=Decimal("51"),
        trade_type=TradeType.SELL,
        order_type=OrderType.MARKET,
        price=Decimal("5.5"),
        position_action=PositionAction.CLOSE,
    )
    connector.quantize_order_price.assert_not_called()
