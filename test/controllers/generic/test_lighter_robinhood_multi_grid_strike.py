import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from controllers.generic.multi_grid_strike import GridConfig
from hummingbot.core.data_type.common import OrderType, PositionMode, TradeType
from hummingbot.strategy_v2.executors.position_executor.data_types import TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction

from controllers.generic.lighter_robinhood_multi_grid_strike import (
    LighterRobinhoodMultiGridStrike,
    LighterRobinhoodMultiGridStrikeConfig,
)


CONNECTOR = "lighter_perpetual_robinhood"
PAIR = "LIT-USDG"


def grid(grid_id="lit-main", start="5", end="6", limit="4.9999", allocation="1", enabled=True):
    return GridConfig(
        grid_id=grid_id,
        start_price=Decimal(start),
        end_price=Decimal(end),
        limit_price=Decimal(limit),
        side=TradeType.BUY,
        amount_quote_pct=Decimal(allocation),
        enabled=enabled,
    )


def config(**updates):
    values = {
        "id": "lit-grid",
        "grids": [grid()],
    }
    values.update(updates)
    return LighterRobinhoodMultiGridStrikeConfig(**values)


def snapshot(now=100, position="0", active_orders=None, pending=False):
    return {
        "account_index": 724450,
        "request_started_at": Decimal(str(now)),
        "fetched_at": Decimal(str(now)),
        "public_data_last_recv_time": Decimal(str(now)),
        "private_stream_last_recv_time": Decimal(str(now)),
        "private_stream_connected": True,
        "net_position": Decimal(position),
        "net_position_known": True,
        "active_orders": active_orders or [],
        "pending_submissions_unknown": pending,
        "position_mode": "ONEWAY",
        "leverage": Decimal("5"),
        "leverage_confirmed": True,
        "market_state_known": True,
        "market_tradable": True,
        "force_reduce_only": False,
        "collateral_token": "USDG",
        "available_margin": Decimal("0"),
        "available_margin_known": True,
    }


def controller_with_snapshot(account_snapshot, cfg=None):
    connector = MagicMock()
    connector.get_grid_account_snapshot = AsyncMock(return_value=account_snapshot)
    connector.in_flight_orders = {}
    connector.trading_rules = {PAIR: MagicMock(
        min_notional_size=Decimal("10"),
        min_order_size=Decimal("5"),
        min_base_amount_increment=Decimal("0.01"),
    )}
    provider = MagicMock()
    provider.connectors = {CONNECTOR: connector}
    provider.time.return_value = float(account_snapshot["fetched_at"])
    provider.get_price_by_type.return_value = Decimal("5.5")
    result = LighterRobinhoodMultiGridStrike(
        config=cfg or config(), market_data_provider=provider, actions_queue=MagicMock(spec=asyncio.Queue)
    )
    return result, provider, connector


def test_safe_profile_defaults_build_native_long_grid_executor_config():
    cfg = config()
    controller, _, _ = controller_with_snapshot(snapshot(), cfg)
    asyncio.run(controller.update_processed_data())

    actions = controller.determine_executor_actions()

    assert len(actions) == 1 and isinstance(actions[0], CreateExecutorAction)
    native = actions[0].executor_config
    assert native.connector_name == CONNECTOR and native.trading_pair == PAIR
    assert native.side is TradeType.BUY
    assert native.total_amount_quote == Decimal("3000")
    assert native.start_price == Decimal("5") and native.end_price == Decimal("6")
    assert native.limit_price == Decimal("4.9999")
    assert native.leverage == 5 and native.keep_position is True
    assert native.min_spread_between_orders == Decimal("0.018")
    assert native.min_order_amount_quote == Decimal("250")
    assert native.max_open_orders == 2 and native.max_orders_per_batch == 1
    assert native.order_frequency == 3 and native.activation_bounds is None
    assert native.triple_barrier_config.take_profit == Decimal("0.018")
    assert native.triple_barrier_config.open_order_type is OrderType.LIMIT_MAKER
    assert native.triple_barrier_config.take_profit_order_type is OrderType.LIMIT_MAKER
    assert native.triple_barrier_config.stop_loss is None
    assert native.triple_barrier_config.time_limit is None
    assert native.triple_barrier_config.trailing_stop is None
    assert controller.determine_executor_actions() == []


def test_native_grid_tuning_fields_are_configurable_with_safe_values():
    cfg = config(
        min_spread_between_orders=Decimal("0.02"),
        min_order_amount_quote=Decimal("300"),
        max_open_orders=3,
        max_orders_per_batch=2,
        order_frequency=5,
        activation_bounds=Decimal("0.1"),
    )
    controller, _, _ = controller_with_snapshot(snapshot(), cfg)
    asyncio.run(controller.update_processed_data())
    native = controller.determine_executor_actions()[0].executor_config
    assert native.min_spread_between_orders == Decimal("0.02")
    assert native.min_order_amount_quote == Decimal("300")
    assert native.max_open_orders == 3
    assert native.max_orders_per_batch == 2
    assert native.order_frequency == 5
    assert native.activation_bounds == Decimal("0.1")


@pytest.mark.parametrize(
    "updates",
    [
        {"connector_name": "lighter_perpetual"},
        {"trading_pair": "BTC-USDG"},
        {"leverage": 4},
        {"position_mode": PositionMode.HEDGE},
        {"keep_position": False},
        {"total_amount_quote": Decimal("5000.01")},
        {"total_amount_quote": Decimal("NaN")},
        {"grids": [grid(limit="5")]},
        {"grids": [grid(allocation="0")]},
        {"grids": [grid(allocation="0.01")]},
        {"grids": [grid("same", allocation="0.5"), grid("same", allocation="0.5")]},
        {"grids": [grid("one", allocation="0.6"), grid("two", allocation="0.5")]},
        {"grids": [GridConfig(**{**grid().model_dump(), "side": TradeType.SELL})]},
        {"triple_barrier_config": TripleBarrierConfig(take_profit=Decimal("0.018"))},
    ],
)
def test_unsafe_or_overallocated_profiles_are_rejected(updates):
    with pytest.raises(ValidationError):
        config(**updates)


@pytest.mark.parametrize(
    "change",
    [
        {"position": "1"},
        {"active_orders": [{"client_order_id": "manual"}]},
        {"pending": True},
    ],
)
def test_initial_generation_requires_flat_account_without_orders_or_unknown_submissions(change):
    controller, _, _ = controller_with_snapshot(snapshot(**change))
    asyncio.run(controller.update_processed_data())
    assert controller.determine_executor_actions() == []
    assert "BLOCKED" in "\n".join(controller.to_format_status())


def test_stale_or_unconfirmed_account_snapshot_blocks_native_creation():
    account = snapshot(now=80)
    account["leverage_confirmed"] = False
    controller, provider, _ = controller_with_snapshot(account)
    provider.time.return_value = 100
    asyncio.run(controller.update_processed_data())
    assert controller.determine_executor_actions() == []
    assert "BLOCKED" in "\n".join(controller.to_format_status())


def test_missing_or_invalid_margin_data_blocks_creation_but_zero_margin_is_informational():
    invalid = snapshot()
    invalid["available_margin_known"] = False
    controller, _, _ = controller_with_snapshot(invalid)
    asyncio.run(controller.update_processed_data())
    assert controller.determine_executor_actions() == []

    valid, _, _ = controller_with_snapshot(snapshot())
    asyncio.run(valid.update_processed_data())
    assert len(valid.determine_executor_actions()) == 1


def test_runtime_market_minimum_blocks_grid_allocation_native_would_inflate():
    cfg = config(
        min_order_amount_quote=Decimal("1"),
        grids=[grid(allocation="0.01")],
    )
    controller, _, _ = controller_with_snapshot(snapshot(), cfg)
    asyncio.run(controller.update_processed_data())
    assert controller.determine_executor_actions() == []
    assert "market minimum" in "\n".join(controller.to_format_status()).lower()


def test_same_generation_can_dispatch_later_grid_once_when_existing_orders_are_bot_tracked():
    cfg = config(grids=[grid("inside", allocation="0.5"), grid("later", start="6", end="7", limit="5.9999", allocation="0.5")])
    controller, provider, connector = controller_with_snapshot(snapshot(), cfg)
    asyncio.run(controller.update_processed_data())
    first = controller.determine_executor_actions()
    assert [action.executor_config.level_id for action in first] == ["inside"]

    active_executor = MagicMock()
    active_executor.id = "executor-inside"
    active_executor.is_active = True
    active_executor.config.level_id = "inside"
    controller.executors_info = [active_executor]
    provider.get_price_by_type.return_value = Decimal("6.5")
    provider.time.return_value = 111
    connector.in_flight_orders = {"owned-order": MagicMock()}
    connector.get_grid_account_snapshot.return_value = snapshot(
        now=111, position="100", active_orders=[{"client_order_id": "owned-order"}]
    )
    asyncio.run(controller.update_processed_data())

    second = controller.determine_executor_actions()
    assert [action.executor_config.level_id for action in second] == ["later"]
    assert controller.determine_executor_actions() == []


def test_foreign_active_order_blocks_remaining_grid_in_same_generation():
    cfg = config(grids=[grid("inside", allocation="0.5"), grid("later", start="6", end="7", limit="5.9999", allocation="0.5")])
    controller, provider, connector = controller_with_snapshot(snapshot(), cfg)
    asyncio.run(controller.update_processed_data())
    controller.determine_executor_actions()
    active_executor = MagicMock(id="executor-inside", is_active=True)
    active_executor.config.level_id = "inside"
    controller.executors_info = [active_executor]
    provider.get_price_by_type.return_value = Decimal("6.5")
    provider.time.return_value = 111
    connector.in_flight_orders = {"owned-order": MagicMock()}
    connector.get_grid_account_snapshot.return_value = snapshot(
        now=111, position="100", active_orders=[{"client_order_id": "foreign-order"}]
    )
    asyncio.run(controller.update_processed_data())
    assert controller.determine_executor_actions() == []


def test_held_position_never_recreates_until_two_distinct_fresh_flat_observations():
    controller, provider, connector = controller_with_snapshot(snapshot())
    asyncio.run(controller.update_processed_data())
    assert len(controller.determine_executor_actions()) == 1
    terminated = MagicMock(id="old-executor", is_active=False)
    terminated.config.level_id = "lit-main"
    controller.executors_info = [terminated]
    controller._grid_executor_mapping = {"lit-main": "old-executor"}

    provider.time.return_value = 111
    connector.get_grid_account_snapshot.return_value = snapshot(now=111, position="100")
    asyncio.run(controller.update_processed_data())
    assert controller.determine_executor_actions() == []

    provider.time.return_value = 122
    connector.get_grid_account_snapshot.return_value = snapshot(now=122)
    asyncio.run(controller.update_processed_data())
    assert controller.determine_executor_actions() == []

    provider.time.return_value = 133
    connector.get_grid_account_snapshot.return_value = snapshot(now=133)
    asyncio.run(controller.update_processed_data())
    assert len(controller.determine_executor_actions()) == 1
    assert controller._grid_executor_mapping == {}


def test_unseen_issued_create_action_never_resets_or_reissues_after_flat_observations():
    controller, provider, connector = controller_with_snapshot(snapshot())
    asyncio.run(controller.update_processed_data())
    assert len(controller.determine_executor_actions()) == 1
    for now in (111, 122, 133):
        provider.time.return_value = now
        connector.get_grid_account_snapshot.return_value = snapshot(now=now)
        asyncio.run(controller.update_processed_data())
        assert controller.determine_executor_actions() == []
    assert "awaiting executor" in "\n".join(controller.to_format_status()).lower()


def test_stale_flat_snapshot_does_not_advance_generation_reset():
    controller, provider, connector = controller_with_snapshot(snapshot())
    asyncio.run(controller.update_processed_data())
    controller.determine_executor_actions()
    terminated = MagicMock(id="old-executor", is_active=False)
    terminated.config.level_id = "lit-main"
    controller.executors_info = [terminated]

    provider.time.return_value = 111
    connector.get_grid_account_snapshot.return_value = snapshot(now=80)
    asyncio.run(controller.update_processed_data())
    provider.time.return_value = 122
    connector.get_grid_account_snapshot.return_value = snapshot(now=122)
    asyncio.run(controller.update_processed_data())
    assert controller.determine_executor_actions() == []


def test_snapshot_refresh_exception_is_visible_in_status():
    controller, _, connector = controller_with_snapshot(snapshot())
    connector.get_grid_account_snapshot.side_effect = RuntimeError("synthetic failure")
    asyncio.run(controller.update_processed_data())
    status = "\n".join(controller.to_format_status())
    assert "BLOCKED" in status and "refresh failed" in status


def test_config_edit_during_generation_cannot_dispatch_new_or_resized_grid():
    controller, provider, connector = controller_with_snapshot(snapshot())
    asyncio.run(controller.update_processed_data())
    assert len(controller.determine_executor_actions()) == 1
    controller.config.grids = [grid(start="5.1")]
    provider.time.return_value = 111
    connector.get_grid_account_snapshot.return_value = snapshot(now=111)
    asyncio.run(controller.update_processed_data())
    assert controller.determine_executor_actions() == []


def test_native_removed_grid_stop_action_is_forced_to_keep_position():
    cfg = config(grids=[grid("one"), grid("two", enabled=False)])
    controller, _, _ = controller_with_snapshot(snapshot(), cfg)
    controller._grid_executor_mapping = {"removed": "executor-1"}
    controller._last_config_hash = "old"
    actions = controller.determine_executor_actions()
    stops = [action for action in actions if isinstance(action, StopExecutorAction)]
    assert len(stops) == 1 and stops[0].executor_id == "executor-1"
    assert stops[0].keep_position is True
