import asyncio
import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from hummingbot.connector.exchange.paper_trade.paper_trade_exchange import QuantizationParams
from hummingbot.connector.derivative.lighter_perpetual import lighter_perpetual_constants as LIGHTER_CONSTANTS
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_api_utils import LighterMarketInfo
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_derivative import LighterPerpetualDerivative
from hummingbot.connector.test_support.mock_paper_exchange import MockPaperExchange
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.clock import Clock
from hummingbot.core.clock_mode import ClockMode
from hummingbot.core.data_type.common import OrderType, PositionMode, TradeType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import OrderCancelledEvent, OrderFilledEvent
from scripts.lighter_robinhood_neutral_grid import (
    GridState,
    LighterRobinhoodNeutralGrid,
    LighterRobinhoodNeutralGridConfig,
    eligible_grid_prices,
)
from scripts.lighter_robinhood_grid_risk import JournalError


PAIR = "LIT-USDG"
CONNECTOR = "lighter_perpetual_robinhood"


def enabled_config(**updates):
    values = {
        "enabled": True,
        "lower_price": Decimal("0.50"),
        "upper_price": Decimal("1.50"),
        "margin_reserve_usdg": Decimal("10"),
    }
    values.update(updates)
    return LighterRobinhoodNeutralGridConfig(**values)


def test_disabled_config_allows_missing_bounds_but_enabled_config_requires_safe_constants():
    disabled = LighterRobinhoodNeutralGridConfig()
    assert not disabled.enabled
    assert disabled.lower_price is None and disabled.upper_price is None

    with pytest.raises(ValidationError):
        LighterRobinhoodNeutralGridConfig(
            enabled=True, lower_price=Decimal("1"), upper_price=Decimal("2")
        )
    with pytest.raises(ValidationError):
        LighterRobinhoodNeutralGridConfig(lower_price=Decimal("NaN"))
    with pytest.raises(ValidationError):
        LighterRobinhoodNeutralGridConfig(margin_reserve_usdg=Decimal("NaN"))
    with pytest.raises(ValidationError):
        LighterRobinhoodNeutralGridConfig(controllers_config=["unrelated.yml"])

    for update in [
        {},
        {"lower_price": Decimal("1"), "upper_price": Decimal("1")},
        {"lower_price": Decimal("NaN"), "upper_price": Decimal("2")},
        {"connector_name": "other", "lower_price": Decimal("1"), "upper_price": Decimal("2")},
        {"trading_pair": "BTC-USDG", "lower_price": Decimal("1"), "upper_price": Decimal("2")},
        {"max_abs_net_position": Decimal("1000.01"), "lower_price": Decimal("1"), "upper_price": Decimal("2")},
        {"leverage": 4, "lower_price": Decimal("1"), "upper_price": Decimal("2")},
        {"refresh_seconds": float("nan"), "lower_price": Decimal("1"), "upper_price": Decimal("2")},
        {"max_data_age_seconds": float("inf"), "lower_price": Decimal("1"), "upper_price": Decimal("2")},
    ]:
        with pytest.raises(ValidationError):
            LighterRobinhoodNeutralGridConfig(enabled=True, **update)


def test_grid_has_21_inclusive_levels_and_chooses_nearest_strict_post_only_prices():
    bid, ask, levels = eligible_grid_prices(
        Decimal("0.5000"), Decimal("1.5000"), 21, Decimal("0.0001"),
        best_bid=Decimal("1.0099"), best_ask=Decimal("1.0101"),
    )

    assert len(levels) == 21
    assert levels[0] == Decimal("0.5000")
    assert levels[-1] == Decimal("1.5000")
    assert bid == Decimal("1.0000")
    assert ask == Decimal("1.0500")
    assert bid < Decimal("1.0101") and ask > Decimal("1.0099")


def test_nondefault_order_size_and_grid_levels_are_accepted_and_select_expected_prices():
    config = enabled_config(grid_levels=5, order_amount_base=Decimal("25"))

    bid, ask, levels = eligible_grid_prices(
        config.lower_price, config.upper_price, config.grid_levels, Decimal("0.0001"),
        best_bid=Decimal("1.19"), best_ask=Decimal("1.21"),
    )

    assert config.order_amount_base == Decimal("25")
    assert levels == [Decimal("0.5000"), Decimal("0.7500"), Decimal("1.0000"),
                      Decimal("1.2500"), Decimal("1.5000")]
    assert bid == Decimal("1.0000")
    assert ask == Decimal("1.2500")


@pytest.mark.parametrize("grid_levels", [1, True, 2.5, "21"])
def test_grid_levels_must_be_an_exact_integer_of_at_least_two(grid_levels):
    with pytest.raises(ValidationError):
        enabled_config(grid_levels=grid_levels)


def test_order_amount_cannot_exceed_position_cap():
    with pytest.raises(ValidationError):
        enabled_config(order_amount_base=Decimal("1000.01"))


def test_grid_level_count_cannot_exceed_distinct_runtime_price_ticks():
    with pytest.raises(ValueError, match="distinct exchange price ticks"):
        eligible_grid_prices(
            Decimal("1.00"), Decimal("1.02"), 4, Decimal("0.01"),
            best_bid=Decimal("1.00"), best_ask=Decimal("1.02"),
        )


def test_collapsed_grid_is_rejected_before_allocating_levels():
    with pytest.raises(ValueError, match="distinct exchange price ticks"):
        eligible_grid_prices(
            Decimal("1.001"), Decimal("1.009"), 21, Decimal("0.01"),
            best_bid=Decimal("1.00"), best_ask=Decimal("1.01"),
        )


def test_wide_external_spread_still_selects_coherent_levels_around_midpoint():
    bid, ask, _ = eligible_grid_prices(
        Decimal("1"), Decimal("2"), 21, Decimal("0.01"),
        best_bid=Decimal("1.10"), best_ask=Decimal("1.90"),
    )
    assert bid == Decimal("1.45")
    assert ask == Decimal("1.55")
    assert bid < ask


def test_runtime_quantization_cannot_silently_return_fewer_than_configured_levels():
    with pytest.raises(ValueError, match="collapse after runtime price quantization"):
        eligible_grid_prices(
            Decimal("1.005"), Decimal("1.035"), 3, Decimal("0.01"),
            best_bid=Decimal("1.01"), best_ask=Decimal("1.03"),
        )


class FakeRobinhoodConnector(MockPaperExchange):
    def __init__(self):
        super().__init__()
        self.snapshots = []
        self.position_mode_calls = []
        self.leverage_calls = []
        self.last_snapshot = None
        self.snapshot_calls = 0
        self.restored_in_flight_orders = {}
        self.set_quantization_param(QuantizationParams(PAIR, 4, 8, 2, 8))
        self._fake_trading_rules = {PAIR: TradingRule(
            PAIR,
            min_order_size=Decimal("5"),
            min_notional_size=Decimal("10"),
            min_price_increment=Decimal("0.0001"),
            min_base_amount_increment=Decimal("0.01"),
        )}
        self.set_balanced_order_book(PAIR, 1.2, 0.4, 1.6, 0.02, 100)

    async def get_grid_account_snapshot(self, trading_pair, client_order_ids=None, force_refresh=True):
        assert trading_pair == PAIR and force_refresh
        self.snapshot_calls += 1
        if not self.snapshots:
            if self.last_snapshot is None:
                raise RuntimeError("no snapshot queued")
            return self.last_snapshot
        self.last_snapshot = self.snapshots.pop(0)
        return self.last_snapshot

    @property
    def trading_rules(self):
        return self._fake_trading_rules

    @property
    def in_flight_orders(self):
        return self.restored_in_flight_orders

    def set_position_mode(self, mode):
        self.position_mode_calls.append(mode)

    def set_leverage(self, trading_pair, leverage=1):
        self.leverage_calls.append((trading_pair, leverage))


class MemoryIntentJournal:
    def __init__(self, entries=None):
        self.entries = entries or {}
        for entry in self.entries.values():
            entry.setdefault("connector_domain", CONNECTOR)
            entry.setdefault("trading_pair", PAIR)
            entry.setdefault("account_index", 724450)

    def reserve(self, provisional_id, side, amount, baseline, connector_domain, trading_pair, account_index):
        self.entries[provisional_id] = {
            "side": side,
            "amount": str(amount),
            "baseline": str(baseline),
            "connector_domain": connector_domain,
            "trading_pair": trading_pair,
            "account_index": account_index,
            "client_order_id": None,
        }

    def bind_client_order_id(self, provisional_id, client_order_id):
        self.entries[provisional_id]["client_order_id"] = client_order_id

    def clear(self):
        self.entries = {}


class RecordingGrid(LighterRobinhoodNeutralGrid):
    def __init__(self, connectors, config):
        self.mutations = []
        self._next_id = 0
        super().__init__(connectors, config)
        self._snapshot_poll_seconds = 0
        self._account_index = 724450

    def _create_intent_journal(self):
        return MemoryIntentJournal()

    def buy(self, connector_name, trading_pair, amount, order_type, price, position_action=None):
        self._next_id += 1
        order_id = f"buy-{self._next_id}"
        self.mutations.append(("buy", order_id, amount, order_type, price))
        return order_id

    def sell(self, connector_name, trading_pair, amount, order_type, price, position_action=None):
        self._next_id += 1
        order_id = f"sell-{self._next_id}"
        self.mutations.append(("sell", order_id, amount, order_type, price))
        return order_id

    def cancel(self, connector_name, trading_pair, order_id):
        self.mutations.append(("cancel", order_id))


def snapshot(revision, position="0", active_orders=None, margin="1000", fetched_at=100):
    return {
        "account_index": 724450,
        "request_started_at": Decimal(str(fetched_at)),
        "fetched_at": Decimal(str(fetched_at)),
        "revision": revision,
        "net_position": Decimal(position),
        "net_position_known": True,
        "active_orders": active_orders or [],
        "pending_submissions_unknown": False,
        "available_margin": Decimal(margin),
        "available_margin_known": True,
        "collateral_token": "USDG",
        "position_mode": "ONEWAY",
        "leverage": 5,
        "leverage_confirmed": True,
        "private_stream_connected": True,
        "private_stream_last_recv_time": Decimal(str(fetched_at)),
        "public_data_last_recv_time": Decimal(str(fetched_at)),
        "market_state_known": True,
        "market_tradable": True,
        "force_reduce_only": False,
        "market_status": "active",
    }


@pytest.mark.asyncio
async def test_disabled_start_tick_and_stop_make_no_signed_or_cancel_calls():
    connector = FakeRobinhoodConnector()
    config = LighterRobinhoodNeutralGridConfig()
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, config)
    strategy.start(MagicMock(), 100)
    strategy.tick(100)
    await strategy.on_stop()

    assert strategy.state is GridState.DISABLED
    assert strategy.mutations == []
    assert connector.position_mode_calls == []
    assert connector.leverage_calls == []


@pytest.mark.asyncio
async def test_enabled_reconciles_twice_then_reserves_and_schedules_one_bid_and_ask():
    connector = FakeRobinhoodConnector()
    connector.snapshots = [snapshot("r1"), snapshot("r2", fetched_at=101)]
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)

    for timestamp in (100, 100.1, 101, 101.1, 102):
        strategy.tick(timestamp)
        await asyncio.sleep(0)

    orders = [mutation for mutation in strategy.mutations if mutation[0] in {"buy", "sell"}]
    assert connector.position_mode_calls == [PositionMode.ONEWAY]
    assert connector.leverage_calls == [(PAIR, 5)]
    assert len(orders) == 2
    assert {order[0] for order in orders} == {"buy", "sell"}
    assert all(order[3] is OrderType.LIMIT_MAKER for order in orders)
    assert strategy.state is GridState.QUOTING
    assert strategy.epoch.reserved_buy == Decimal("10.00")
    assert strategy.epoch.reserved_sell == Decimal("10.00")
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_external_order_or_stale_or_disconnected_data_pauses_without_quoting():
    cases = [
        snapshot("external", active_orders=[{"client_order_id": "manual"}]),
        {**snapshot("stale", fetched_at=1), "public_data_last_recv_time": Decimal("1")},
        {**snapshot("disconnect"), "private_stream_connected": False},
        {**snapshot("private-stale"), "private_stream_last_recv_time": Decimal("1")},
    ]
    for account_snapshot in cases:
        connector = FakeRobinhoodConnector()
        connector.snapshots = [account_snapshot]
        with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
            "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
        ):
            strategy = RecordingGrid({CONNECTOR: connector}, enabled_config())
        strategy.start(MagicMock(), 99)
        strategy.tick(100)
        await asyncio.sleep(0)
        strategy.tick(101)
        assert not any(mutation[0] in {"buy", "sell"} for mutation in strategy.mutations)
        assert strategy.state in {GridState.PAUSED, GridState.DRAINING, GridState.RECONCILING}
        await strategy.on_stop()


@pytest.mark.asyncio
async def test_idle_private_heartbeat_older_than_data_age_is_still_healthy():
    connector = FakeRobinhoodConnector()
    healthy_idle = {**snapshot("idle"), "private_stream_last_recv_time": Decimal("70")}
    connector.snapshots = [healthy_idle, {
        **healthy_idle,
        "revision": "idle-2",
        "fetched_at": Decimal("101"),
        "request_started_at": Decimal("101"),
    }]
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    for timestamp in (100, 100.1, 101, 101.1, 102):
        strategy.tick(timestamp)
        await asyncio.sleep(0)
    assert strategy.state is GridState.QUOTING
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_throttled_snapshot_older_than_data_age_is_rejected():
    connector = FakeRobinhoodConnector()
    delayed = {**snapshot("delayed", fetched_at=100), "request_started_at": Decimal("80")}
    connector.snapshots = [delayed]
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    for timestamp in (100, 100.1, 101):
        strategy.tick(timestamp)
        await asyncio.sleep(0)
    assert strategy.state is GridState.PAUSED
    assert strategy.mutations == []
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_authoritative_snapshot_requests_are_bounded_to_ten_second_cadence():
    connector = FakeRobinhoodConnector()
    connector.snapshots = [snapshot("r1", fetched_at=100)]
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = LighterRobinhoodNeutralGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    strategy.ready_to_trade = True
    strategy.tick(100)
    await asyncio.sleep(0)
    strategy.tick(101)
    await asyncio.sleep(0)
    strategy.tick(109.9)
    await asyncio.sleep(0)
    assert connector.snapshot_calls == 1
    strategy.tick(110)
    await asyncio.sleep(0)
    assert connector.snapshot_calls == 2
    await strategy.on_stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "update",
    [
        {"leverage": None, "leverage_confirmed": False},
        {"net_position": None, "net_position_known": False},
        {"available_margin": None, "available_margin_known": False},
        {"available_margin": Decimal("NaN"), "available_margin_known": True},
        {"available_margin": Decimal("-1"), "available_margin_known": True},
    ],
)
async def test_unknown_authoritative_account_fields_pause_without_crashing(update):
    connector = FakeRobinhoodConnector()
    connector.snapshots = [{**snapshot("unknown"), **update}]
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    strategy.tick(100)
    await asyncio.sleep(0)
    strategy.tick(101)
    await asyncio.sleep(0)
    strategy.tick(102)
    assert strategy.state is GridState.PAUSED
    assert not any(m[0] in {"buy", "sell"} for m in strategy.mutations)
    await strategy.on_stop()


def test_numeric_zero_client_order_id_is_normalized_instead_of_treated_as_missing():
    assert LighterRobinhoodNeutralGrid._active_order_ids([{"client_order_id": 0}]) == {"0"}
    assert LighterRobinhoodNeutralGrid._active_order_ids([{"client_order_id_str": "123"}]) == {"123"}


@pytest.mark.asyncio
async def test_partial_fill_cancel_late_fill_account_lag_then_replacement():
    connector = FakeRobinhoodConnector()
    connector.snapshots = [snapshot("r1"), snapshot("r2", fetched_at=101)]
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    for timestamp in (100, 100.1, 101, 101.1, 102):
        strategy.tick(timestamp)
        await asyncio.sleep(0)

    buy_id = next(m[1] for m in strategy.mutations if m[0] == "buy")
    strategy.did_fill_order(OrderFilledEvent(
        103, buy_id, PAIR, TradeType.BUY, OrderType.LIMIT_MAKER,
        Decimal("1"), Decimal("3"), AddedToCostTradeFee(), "trade-1", "ex-buy",
    ))
    strategy.tick(103)
    assert strategy.state is GridState.DRAINING
    assert len([m for m in strategy.mutations if m[0] == "cancel"]) == 2
    strategy.did_cancel_order(OrderCancelledEvent(104, buy_id))
    sell_id = next(m[1] for m in strategy.mutations if m[0] == "sell")
    strategy.did_cancel_order(OrderCancelledEvent(104, sell_id))
    strategy.did_fill_order(OrderFilledEvent(
        104.5, buy_id, PAIR, TradeType.BUY, OrderType.LIMIT_MAKER,
        Decimal("1"), Decimal("2"), AddedToCostTradeFee(), "trade-2", "ex-buy",
    ))
    connector.snapshots = [
        {**snapshot("lag", position="3", fetched_at=105), "orders_by_client_id": {
            buy_id: {"terminal": True, "cumulative_fill_known": True, "cumulative_fill_base": Decimal("5")},
            sell_id: {"terminal": True, "cumulative_fill_known": True, "cumulative_fill_base": Decimal("0")},
        }},
        {**snapshot("match1", position="5", fetched_at=106), "orders_by_client_id": {
            buy_id: {"terminal": True, "cumulative_fill_known": True, "cumulative_fill_base": Decimal("5")},
            sell_id: {"terminal": True, "cumulative_fill_known": True, "cumulative_fill_base": Decimal("0")},
        }},
        {**snapshot("match2", position="5", fetched_at=107), "orders_by_client_id": {
            buy_id: {"terminal": True, "cumulative_fill_known": True, "cumulative_fill_base": Decimal("5")},
            sell_id: {"terminal": True, "cumulative_fill_known": True, "cumulative_fill_base": Decimal("0")},
        }},
    ]
    for timestamp in (105, 105.1, 106, 106.1, 107, 107.1, 108):
        strategy.tick(timestamp)
        await asyncio.sleep(0)

    assert len([m for m in strategy.mutations if m[0] in {"buy", "sell"}]) == 4
    assert strategy.epoch.baseline == Decimal("5")
    await strategy.on_stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "baseline, expected_side",
    [(Decimal("995"), "sell"), (Decimal("-995"), "buy")],
)
async def test_near_position_cap_only_capacity_safe_side_is_scheduled(baseline, expected_side):
    connector = FakeRobinhoodConnector()
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config())
    from scripts.lighter_robinhood_grid_risk import ExposureEpoch
    strategy.epoch = ExposureEpoch(baseline, Decimal("1000"), Decimal("0.01"))
    strategy._now = 100

    strategy._quote_if_safe(Decimal("1000"))

    submitted = [m[0] for m in strategy.mutations if m[0] in {"buy", "sell"}]
    assert submitted == [expected_side]
    assert strategy.epoch.baseline + strategy.epoch.reserved_buy <= Decimal("1000")
    assert strategy.epoch.baseline - strategy.epoch.reserved_sell >= Decimal("-1000")
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_near_cap_quantizes_order_down_to_remaining_side_capacity():
    connector = FakeRobinhoodConnector()
    connector.set_balanced_order_book(PAIR, 2.5, 2.0, 3.0, 0.02, 100)
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config(
            lower_price=Decimal("2"), upper_price=Decimal("3")
        ))
    from scripts.lighter_robinhood_grid_risk import ExposureEpoch
    strategy.epoch = ExposureEpoch(Decimal("995"), Decimal("1000"), Decimal("0.01"))
    strategy._now = 100

    strategy._quote_if_safe(Decimal("1000"))

    buys = [m for m in strategy.mutations if m[0] == "buy"]
    sells = [m for m in strategy.mutations if m[0] == "sell"]
    assert len(buys) == len(sells) == 1
    assert buys[0][2] == Decimal("5.00")
    assert sells[0][2] == Decimal("10.00")
    assert strategy.epoch.baseline + strategy.epoch.reserved_buy == Decimal("1000.00")
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_nondefault_grid_strategy_submits_configured_size_at_expected_prices():
    connector = FakeRobinhoodConnector()
    connector.get_price = MagicMock(
        side_effect=lambda _pair, is_buy: Decimal("1.21" if is_buy else "1.19")
    )
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config(
            grid_levels=5, order_amount_base=Decimal("25"),
        ))
    from scripts.lighter_robinhood_grid_risk import ExposureEpoch
    strategy.epoch = ExposureEpoch(Decimal("0"), Decimal("1000"), Decimal("0.01"))

    strategy._quote_if_safe(Decimal("1000"))

    buy = next(mutation for mutation in strategy.mutations if mutation[0] == "buy")
    sell = next(mutation for mutation in strategy.mutations if mutation[0] == "sell")
    assert (buy[2], buy[4]) == (Decimal("25.00"), Decimal("1.0000"))
    assert (sell[2], sell[4]) == (Decimal("25.00"), Decimal("1.2500"))
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_runtime_grid_larger_than_distinct_ticks_pauses_before_submitting():
    connector = FakeRobinhoodConnector()
    connector.get_price = MagicMock(
        side_effect=lambda _pair, is_buy: Decimal("1.0002" if is_buy else "1.0000")
    )
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config(
            lower_price=Decimal("1.0000"), upper_price=Decimal("1.0002"), grid_levels=4,
        ))
    from scripts.lighter_robinhood_grid_risk import ExposureEpoch
    strategy.epoch = ExposureEpoch(Decimal("0"), Decimal("1000"), Decimal("0.01"))

    strategy._quote_if_safe(Decimal("1000"))

    assert strategy.state is GridState.PAUSED
    assert "distinct exchange price ticks" in strategy._pause_reason
    assert not any(mutation[0] in {"buy", "sell"} for mutation in strategy.mutations)
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_unknown_submission_keeps_full_reservation_and_pauses():
    connector = FakeRobinhoodConnector()

    class UnknownSubmissionGrid(RecordingGrid):
        def buy(self, *args, **kwargs):
            raise TimeoutError("outcome unknown")

    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = UnknownSubmissionGrid({CONNECTOR: connector}, enabled_config())
    from scripts.lighter_robinhood_grid_risk import ExposureEpoch
    strategy.epoch = ExposureEpoch(Decimal("0"), Decimal("1000"), Decimal("0.01"))

    strategy._reserve_then_submit("BUY", Decimal("10"), Decimal("1"))

    assert strategy.state is GridState.PAUSED
    assert strategy.epoch.reserved_buy == Decimal("10")
    assert list(strategy.epoch.orders) == ["pending-1"]
    assert strategy._journal.entries["pending-1"]["client_order_id"] is None
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_returned_order_id_is_cancelable_when_durable_binding_fails():
    connector = FakeRobinhoodConnector()

    class FailingBindJournal(MemoryIntentJournal):
        def bind_client_order_id(self, provisional_id, client_order_id):
            raise JournalError("injected fsync failure")

    class FailingBindGrid(RecordingGrid):
        def _create_intent_journal(self):
            return FailingBindJournal()

    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = FailingBindGrid({CONNECTOR: connector}, enabled_config())
    from scripts.lighter_robinhood_grid_risk import ExposureEpoch
    strategy.epoch = ExposureEpoch(Decimal("0"), Decimal("1000"), Decimal("0.01"))

    strategy._reserve_then_submit("BUY", Decimal("10"), Decimal("1"))

    assert strategy.state is GridState.DRAINING
    assert "buy-1" in strategy._owned_order_ids
    assert ("cancel", "buy-1") in strategy.mutations
    assert strategy._journal.entries["pending-1"]["client_order_id"] is None
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_restart_with_unidentified_durable_intent_never_requotes():
    connector = FakeRobinhoodConnector()
    connector.snapshots = [snapshot("r1"), snapshot("r2", fetched_at=101)]

    class RestartGrid(RecordingGrid):
        def _create_intent_journal(self):
            return MemoryIntentJournal({
                "pending-old": {
                    "side": "BUY", "amount": "10", "baseline": "990", "client_order_id": None,
                }
            })

    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RestartGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    for timestamp in (100, 100.1, 101, 101.1, 102):
        strategy.tick(timestamp)
        await asyncio.sleep(0)
    assert strategy.state is GridState.PAUSED
    assert strategy.mutations == []
    assert strategy._unidentified_recovery
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_restart_with_known_intent_waits_for_terminal_proof_before_requoting():
    connector = FakeRobinhoodConnector()
    evidence = {
        "77": {"terminal": True, "cumulative_fill_known": True, "cumulative_fill_base": Decimal("10")}
    }
    connector.snapshots = [
        {**snapshot("r1", position="10"), "orders_by_client_id": evidence},
        {**snapshot("r2", position="10", fetched_at=101), "orders_by_client_id": evidence},
    ]

    class RestartGrid(RecordingGrid):
        def _create_intent_journal(self):
            return MemoryIntentJournal({
                "pending-old": {
                    "side": "BUY", "amount": "10", "baseline": "0", "client_order_id": "77",
                }
            })

    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RestartGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    for timestamp in (100, 100.1, 101, 101.1, 102):
        strategy.tick(timestamp)
        await asyncio.sleep(0)
    assert strategy.state is GridState.QUOTING
    assert len([m for m in strategy.mutations if m[0] in {"buy", "sell"}]) == 2
    assert strategy.epoch.baseline == Decimal("10")
    assert "77" not in strategy._recovery_order_ids
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_restart_waits_for_filled_intent_position_to_catch_up_before_requoting():
    connector = FakeRobinhoodConnector()
    evidence = {
        "77": {"terminal": True, "cumulative_fill_known": True, "cumulative_fill_base": Decimal("10")}
    }
    connector.snapshots = [
        {**snapshot("stale-1", position="999.999", fetched_at=100), "orders_by_client_id": evidence},
        {**snapshot("stale-2", position="999.999", fetched_at=101), "orders_by_client_id": evidence},
        {**snapshot("match-1", position="1000", fetched_at=102), "orders_by_client_id": evidence},
        {**snapshot("match-2", position="1000", fetched_at=103), "orders_by_client_id": evidence},
    ]

    class RestartGrid(RecordingGrid):
        def _create_intent_journal(self):
            return MemoryIntentJournal({
                "pending-old": {
                    "side": "BUY", "amount": "10", "baseline": "990", "client_order_id": "77",
                }
            })

    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RestartGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    for timestamp in (100, 100.1, 101, 101.1, 102, 102.1):
        strategy.tick(timestamp)
        await asyncio.sleep(0)
    assert not any(mutation[0] in {"buy", "sell"} for mutation in strategy.mutations)
    assert strategy._journal.entries

    for timestamp in (103, 103.1, 104):
        strategy.tick(timestamp)
        await asyncio.sleep(0)
    assert strategy.state is GridState.QUOTING
    assert len([m for m in strategy.mutations if m[0] in {"buy", "sell"}]) == 1
    assert strategy.epoch.baseline == Decimal("1000")
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_active_restored_owned_order_is_canceled_but_kept_reserved_for_reconciliation():
    connector = FakeRobinhoodConnector()
    connector.restored_in_flight_orders = {"77": MagicMock()}
    connector.snapshots = [{
        **snapshot("active", active_orders=[{"client_order_id_str": "77"}]),
        "orders_by_client_id": {
            "77": {"terminal": False, "cumulative_fill_known": True, "cumulative_fill_base": Decimal("0")}
        },
        "pending_submissions_unknown": False,
    }]

    class RestartGrid(RecordingGrid):
        def _create_intent_journal(self):
            return MemoryIntentJournal({
                "pending-old": {
                    "side": "BUY", "amount": "10", "baseline": "0", "client_order_id": "77",
                }
            })

    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RestartGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    for timestamp in (100, 100.1, 101):
        strategy.tick(timestamp)
        await asyncio.sleep(0)
    assert strategy.state is GridState.DRAINING
    assert ("cancel", "77") in strategy.mutations
    assert "77" in strategy._recovery_order_ids
    assert strategy._journal.entries
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_tracker_only_restored_order_is_never_adopted_or_canceled_without_journal_authority():
    connector = FakeRobinhoodConnector()
    connector.restored_in_flight_orders = {"88": MagicMock()}
    connector.snapshots = [snapshot("r1"), snapshot("r2", fetched_at=101)]
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    for timestamp in (100, 100.1, 101, 101.1, 102):
        strategy.tick(timestamp)
        await asyncio.sleep(0)

    assert strategy.state is GridState.PAUSED
    assert not any(mutation[0] in {"buy", "sell", "cancel"} for mutation in strategy.mutations)
    assert "88" not in strategy._recovery_order_ids
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_restart_rejects_journal_authority_from_a_different_account():
    connector = FakeRobinhoodConnector()
    connector.snapshots = [snapshot("r1")]

    class RestartGrid(RecordingGrid):
        def _create_intent_journal(self):
            journal = MemoryIntentJournal({
                "pending-old": {
                    "side": "BUY", "amount": "10", "baseline": "0", "client_order_id": "77",
                }
            })
            journal.entries["pending-old"]["account_index"] = 999
            return journal

    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RestartGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    for timestamp in (100, 100.1, 101):
        strategy.tick(timestamp)
        await asyncio.sleep(0)

    assert strategy.state is GridState.PAUSED
    assert "different account" in strategy._pause_reason
    assert strategy._journal.entries
    assert not any(mutation[0] in {"buy", "sell", "cancel"} for mutation in strategy.mutations)
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_restart_rejects_duplicate_bound_ids_without_collapsing_intents():
    connector = FakeRobinhoodConnector()

    class RestartGrid(RecordingGrid):
        def _create_intent_journal(self):
            return MemoryIntentJournal({
                "pending-1": {
                    "side": "BUY", "amount": "10", "baseline": "0", "client_order_id": "77",
                },
                "pending-2": {
                    "side": "SELL", "amount": "10", "baseline": "0", "client_order_id": "77",
                },
            })

    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RestartGrid({CONNECTOR: connector}, enabled_config())

    assert strategy.state is GridState.PAUSED
    assert "duplicate client order ids" in strategy._pause_reason
    assert len(strategy._journal.entries) == 2
    await strategy.on_stop()


def test_default_intent_journal_uses_stable_hummingbot_data_path(tmp_path):
    strategy = LighterRobinhoodNeutralGrid.__new__(LighterRobinhoodNeutralGrid)
    with patch("scripts.lighter_robinhood_neutral_grid.data_path", return_value=str(tmp_path)):
        journal = strategy._create_intent_journal()

    assert journal.path == tmp_path / "lighter_robinhood_neutral_grid_intents.json"
    assert journal.path.is_absolute()


@pytest.mark.asyncio
async def test_known_margin_below_conservative_estimate_still_quotes_with_warning():
    connector = FakeRobinhoodConnector()
    connector.snapshots = [
        snapshot("r1", margin="309.99"),
        snapshot("r2", margin="309.99", fetched_at=101),
    ]
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    for timestamp in (100, 100.1, 101, 101.1, 102):
        strategy.tick(timestamp)
        await asyncio.sleep(0)
    assert strategy.state is GridState.QUOTING
    assert len([m for m in strategy.mutations if m[0] in {"buy", "sell"}]) == 2
    assert not any(m[0] == "cancel" for m in strategy.mutations)
    assert "available USDG 309.99" in strategy.format_status()
    assert "conservative estimate 310.00" in strategy.format_status()
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_persistent_low_margin_warns_once_and_clears_when_resolved():
    connector = FakeRobinhoodConnector()
    connector.snapshots = [snapshot("r1"), snapshot("r2", fetched_at=101)]
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    for timestamp in (100, 100.1, 101, 101.1, 102):
        strategy.tick(timestamp)
        await asyncio.sleep(0)
    active_orders = [{"client_order_id": order_id} for order_id in strategy._owned_order_ids]
    evidence = {
        order_id: {"terminal": False, "cumulative_fill_known": True, "cumulative_fill_base": Decimal("0")}
        for order_id in strategy._owned_order_ids
    }
    connector.snapshots = [
        {**snapshot("low1", margin="309.99", active_orders=active_orders, fetched_at=103),
         "orders_by_client_id": evidence},
        {**snapshot("low2", margin="309.99", active_orders=active_orders, fetched_at=104),
         "orders_by_client_id": evidence},
    ]
    logger = MagicMock()
    with patch.object(strategy, "logger", return_value=logger):
        for timestamp in (103, 103.1, 104, 104.1, 105):
            strategy.tick(timestamp)
            await asyncio.sleep(0)

    assert logger.warning.call_count == 1
    assert "available USDG 309.99" in strategy.format_status()
    assert not any(m[0] == "cancel" for m in strategy.mutations)

    connector.snapshots = [{
        **snapshot("recovered", margin="310", active_orders=active_orders, fetched_at=106),
        "orders_by_client_id": evidence,
    }]
    for timestamp in (106, 106.1, 107):
        strategy.tick(timestamp)
        await asyncio.sleep(0)
    assert "margin warning" not in strategy.format_status().lower()
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_market_moving_outside_fixed_range_drains_without_recentering():
    connector = FakeRobinhoodConnector()
    connector.snapshots = [snapshot("r1"), snapshot("r2", fetched_at=101)]
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    for timestamp in (100, 100.1, 101, 101.1, 102):
        strategy.tick(timestamp)
        await asyncio.sleep(0)
    connector.set_balanced_order_book(PAIR, 1.6, 1.5, 1.8, 0.02, 100)
    connector.snapshots = [snapshot("outside", fetched_at=103)]

    for timestamp in (103, 103.1, 104):
        strategy.tick(timestamp)
        await asyncio.sleep(0)

    assert strategy.state is GridState.DRAINING
    assert len([m for m in strategy.mutations if m[0] == "cancel"]) == 2
    assert strategy.config.lower_price == Decimal("0.50")
    assert strategy.config.upper_price == Decimal("1.50")
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_closed_or_reduce_only_market_drains_owned_orders():
    connector = FakeRobinhoodConnector()
    connector.snapshots = [snapshot("r1"), snapshot("r2", fetched_at=101)]
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    for timestamp in (100, 100.1, 101, 101.1, 102):
        strategy.tick(timestamp)
        await asyncio.sleep(0)
    connector.snapshots = [{
        **snapshot("closed", fetched_at=103),
        "market_tradable": False,
        "market_status": "closed",
    }]
    for timestamp in (103, 103.1, 104):
        strategy.tick(timestamp)
        await asyncio.sleep(0)
    assert strategy.state is GridState.DRAINING
    assert len([m for m in strategy.mutations if m[0] == "cancel"]) == 2
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_position_drift_that_can_breach_cap_drains_and_retries_active_cancels():
    connector = FakeRobinhoodConnector()
    connector.snapshots = [snapshot("r1"), snapshot("r2", fetched_at=101)]
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config())
    strategy.start(MagicMock(), 99)
    for timestamp in (100, 100.1, 101, 101.1, 102):
        strategy.tick(timestamp)
        await asyncio.sleep(0)
    buy_id = next(m[1] for m in strategy.mutations if m[0] == "buy")
    sell_id = next(m[1] for m in strategy.mutations if m[0] == "sell")
    active_orders = [{"client_order_id": buy_id}, {"client_order_id": sell_id}]
    evidence = {
        buy_id: {"terminal": False, "cumulative_fill_known": True, "cumulative_fill_base": Decimal("0")},
        sell_id: {"terminal": False, "cumulative_fill_known": True, "cumulative_fill_base": Decimal("0")},
    }
    connector.snapshots = [
        {**snapshot("drift1", position="995", active_orders=active_orders, fetched_at=103),
         "orders_by_client_id": evidence},
        {**snapshot("drift2", position="995", active_orders=active_orders, fetched_at=104),
         "orders_by_client_id": evidence},
    ]
    for timestamp in (103, 103.1, 104, 104.1, 105):
        strategy.tick(timestamp)
        await asyncio.sleep(0)

    assert strategy.state is GridState.DRAINING
    cancels = [m for m in strategy.mutations if m[0] == "cancel"]
    assert len(cancels) >= 4
    assert {cancel[1] for cancel in cancels} == {buy_id, sell_id}
    assert strategy.epoch.baseline + strategy.epoch.reserved_buy <= Decimal("1000")
    assert Decimal("995") + strategy.epoch.reserved_buy > Decimal("1000")
    await strategy.on_stop()


@pytest.mark.asyncio
async def test_real_strategy_submission_is_visible_in_hummingbot_order_tracker():
    connector = FakeRobinhoodConnector()
    connector.snapshots = [snapshot("r1", fetched_at=100), snapshot("r2", fetched_at=101)]

    class TrackingGrid(LighterRobinhoodNeutralGrid):
        def _create_intent_journal(self):
            return MemoryIntentJournal()

    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = TrackingGrid({CONNECTOR: connector}, enabled_config())
    strategy._snapshot_poll_seconds = 0
    clock = Clock(ClockMode.BACKTEST, 1, 99, 110)
    clock.add_iterator(connector)
    clock.add_iterator(strategy)
    try:
        for timestamp in (100, 101, 102, 103):
            clock.backtest_til(timestamp)
            await asyncio.sleep(0)

        active_orders = strategy.get_active_orders(CONNECTOR)
        assert len(active_orders) == 2
        assert {order.client_order_id for order in active_orders} == strategy._owned_order_ids
        assert {order.is_buy for order in active_orders} == {True, False}
        assert strategy.epoch.reserved_buy == Decimal("10.00")
        assert strategy.epoch.reserved_sell == Decimal("10.00")
    finally:
        await strategy.on_stop()
        clock.remove_iterator(strategy)
        clock.remove_iterator(connector)


@pytest.mark.asyncio
async def test_real_lighter_snapshot_contract_reconciles_strategy_then_quotes():
    connector = LighterPerpetualDerivative(
        trading_pairs=[PAIR], trading_required=False, domain=LIGHTER_CONSTANTS.ROBINHOOD_DOMAIN
    )
    connector._account_index = 724450
    connector._ensure_account_ready = AsyncMock()
    raw_market = {
        "market_id": 5,
        "symbol": "LIT",
        "status": "active",
        "market_type": "perp",
        "min_base_amount": "5",
        "min_quote_amount": "10",
        "supported_size_decimals": 2,
        "supported_price_decimals": 4,
        "maker_fee": "0",
        "taker_fee": "0.0002",
        "min_initial_margin_fraction": 2000,
        "market_config": {"hidden": False, "force_reduce_only": False},
    }
    market = LighterMarketInfo(
        market_id=5,
        exchange_symbol="LIT",
        trading_pair=PAIR,
        base_asset="LIT",
        quote_asset="USDG",
        market_type="perp",
        min_base_amount=Decimal("5"),
        min_quote_amount=Decimal("10"),
        size_decimals=2,
        price_decimals=4,
        maker_fee=Decimal("0"),
        taker_fee=Decimal("0.0002"),
        raw_info=raw_market,
    )
    connector._markets_by_id = {5: market}
    connector._markets_by_trading_pair = {PAIR: market}
    connector._markets_by_exchange_symbol = {"LIT": market}
    connector._trading_rules = {PAIR: market.trading_rule(collateral_token="USDG")}
    stream_time = time.time()
    connector._user_stream_tracker = SimpleNamespace(last_recv_time=stream_time)
    connector._order_book_tracker = SimpleNamespace(
        ready=True,
        data_source=SimpleNamespace(_ws_assistant=SimpleNamespace(last_recv_time=stream_time)),
    )
    connector.get_price = MagicMock(side_effect=lambda _pair, is_buy: Decimal("1.21" if is_buy else "1.19"))
    account = {
        "accounts": [{
            "account_index": 724450,
            "available_balance": "500",
            "assets": [{
                "asset_id": 3, "symbol": "USDG", "margin_balance": "500", "locked_balance": "0"
            }],
            "positions": [{
                "market_id": 5, "position": "0", "sign": 0, "initial_margin_fraction": 2000
            }],
        }]
    }

    async def api_get(path_url, **_kwargs):
        return {
            LIGHTER_CONSTANTS.BALANCE_PATH_URL: account,
            LIGHTER_CONSTANTS.ACCOUNT_ACTIVE_ORDERS_PATH_URL: {"orders": []},
            LIGHTER_CONSTANTS.ACCOUNT_INACTIVE_ORDERS_PATH_URL: {"orders": []},
            LIGHTER_CONSTANTS.TRADES_PATH_URL: {"trades": []},
            LIGHTER_CONSTANTS.ORDER_BOOK_DETAILS_PATH_URL: {"order_book_details": [raw_market]},
        }[path_url]

    connector._api_get = AsyncMock(side_effect=api_get)
    with patch("hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator"), patch(
        "hummingbot.strategy.strategy_v2_base.MarketDataProvider"
    ):
        strategy = RecordingGrid({CONNECTOR: connector}, enabled_config())
    try:
        first = await connector.get_grid_account_snapshot(PAIR)
        strategy._now = first["fetched_at"]
        strategy._process_snapshot(first)
        await asyncio.sleep(0.001)
        second = await connector.get_grid_account_snapshot(PAIR)
        strategy._now = second["fetched_at"]
        strategy._process_snapshot(second)

        orders = [mutation for mutation in strategy.mutations if mutation[0] in {"buy", "sell"}]
        assert strategy.state is GridState.QUOTING
        assert len(orders) == 2
        assert {order[0] for order in orders} == {"buy", "sell"}
        assert all(order[3] is OrderType.LIMIT_MAKER for order in orders)
        assert strategy.epoch.reserved_buy == Decimal("10.00")
        assert strategy.epoch.reserved_sell == Decimal("10.00")
        assert connector._api_get.await_count == 10
    finally:
        await strategy.on_stop()
