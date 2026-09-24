"""Generic neutral grid controller, the single NeutralGridExecutor and the Robinhood launcher (NG-ARCH-001/003).

The executor tests drive the real engine through the deterministic FakeExchange and a real SQLite store; no
connector, network or credential is involved.
"""
import asyncio
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from pydantic import ValidationError

from controllers.generic.neutral_grid import NeutralGrid, NeutralGridConfig
from hummingbot.core.data_type.common import PositionMode
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import EngineState, OrderState
from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import NeutralGridExecutorConfig
from hummingbot.strategy_v2.executors.neutral_grid_executor.executor import (
    EXECUTOR_TYPE,
    NeutralGridExecutor,
    register_executor_type,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import FakeClock, FakeExchange
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from scripts.lighter_robinhood_fixed_neutral_grid import (
    CONTROLLER_CONFIG_NAME,
    LighterRobinhoodFixedNeutralGridConfig,
    confirmation_phrase,
    validate_api_credentials,
    validate_profile,
)

D = Decimal
ROOT = Path(__file__).resolve().parents[3]
EXAMPLE_CONTROLLER = ROOT / "conf/controllers/lighter_robinhood_fixed_neutral_grid.yml.example"
EXAMPLE_SCRIPT = ROOT / "conf/scripts/lighter_robinhood_fixed_neutral_grid.yml.example"


def controller_config(**updates):
    values = dict(id="ng-test", lower_price=D("5"), upper_price=D("6"), cell_count=10,
                  expected_initial_position=D("0"))
    values.update(updates)
    return NeutralGridConfig(**values)


def make_controller(config):
    provider = MagicMock()
    provider.time.return_value = 1_790_000_000.0
    return NeutralGrid(config, provider, asyncio.Queue())


# ------------------------------------------------------------------------------------------ config / example
def test_example_config_is_disabled_and_matches_the_spec_profile():
    data = yaml.safe_load(EXAMPLE_CONTROLLER.read_text())
    cfg = NeutralGridConfig(**data)
    assert cfg.enabled is False
    assert (cfg.connector_name, cfg.trading_pair, cfg.position_mode) == \
        ("lighter_perpetual_robinhood", "LIT-USDG", PositionMode.ONEWAY)
    assert (cfg.lower_price, cfg.upper_price, cfg.cell_count, cfg.order_amount_base) == (D("5"), D("6"), 55, D("10"))
    assert (cfg.leverage, cfg.max_abs_net_position, cfg.max_gross_position, cfg.max_active_orders) == \
        (D("5"), D("1000"), D("1000"), 120)
    assert (cfg.history_freshness_s, cfg.settlement_delay_s, cfg.settlement_scans, cfg.history_overlap_s,
            cfg.poll_interval_s) == (D("10"), D("5"), 2, D("60"), D("5"))
    assert (cfg.entry_order_type.value, cfg.tp_order_type.value) == ("LIMIT_MAKER", "LIMIT")
    assert validate_profile(cfg) == []
    text = (EXAMPLE_CONTROLLER.read_text() + EXAMPLE_SCRIPT.read_text()).lower()
    for secret in ("private_key:", "api_key:", "password", "http://", "https://", "secret:"):
        assert secret not in text
    script = LighterRobinhoodFixedNeutralGridConfig(**yaml.safe_load(EXAMPLE_SCRIPT.read_text()))
    assert script.controllers_config == [CONTROLLER_CONFIG_NAME] and script.live_start_confirmation is None


@pytest.mark.parametrize("updates, message", [
    ({"entry_order_type": "MARKET"}, "entry_order_type"),
    ({"position_mode": PositionMode.HEDGE}, "ONEWAY"),
    ({"enabled": True, "lower_price": None}, "lower_price"),
    ({"enabled": True, "expected_initial_position": None}, "expected_initial_position"),
    ({"lower_price": D("6"), "upper_price": D("5")}, "lower_price"),
    ({"order_amount_base": D("0")}, "order_amount_base"),
    ({"max_active_orders": 0}, "max_active_orders"),
])
def test_config_rejects_unsafe_values(updates, message):
    with pytest.raises(ValidationError, match=message):
        controller_config(**updates)


def test_profile_and_confirmation_policy():
    cfg = controller_config(enabled=True, leverage=D("10"))
    assert any("leverage" in e for e in validate_profile(cfg))
    assert any("max_abs_net_position" in e for e in validate_profile(controller_config(max_abs_net_position=D("2000"))))
    assert confirmation_phrase("g1", D("-12.5")) == "START g1 ON lighter_perpetual_robinhood LIT-USDG WITH B=-12.5"
    with pytest.raises(ValidationError):
        LighterRobinhoodFixedNeutralGridConfig(controllers_config=["other.yml"])
    with pytest.raises(ValidationError):
        LighterRobinhoodFixedNeutralGridConfig(max_global_drawdown_quote=10)


def test_api_key_validator_is_the_existing_80_hex_rule():
    connector = MagicMock()
    connector._api_private_key = "0x" + "ab" * 40
    assert validate_api_credentials(connector) is None
    connector._api_private_key = "cd" * 32                      # 64-hex wallet key
    assert "wallet key" in validate_api_credentials(connector)
    connector._api_private_key = "zz" * 40
    assert "80 hexadecimal" in validate_api_credentials(connector)
    connector._api_private_key = None
    assert validate_api_credentials(connector) is not None


# ------------------------------------------------------------------------------------------ controller
def test_controller_creates_exactly_one_executor_and_never_recreates():
    controller = make_controller(controller_config(enabled=True))
    actions = controller.determine_executor_actions()
    assert len(actions) == 1 and isinstance(actions[0], CreateExecutorAction)
    ex_cfg = actions[0].executor_config
    assert isinstance(ex_cfg, NeutralGridExecutorConfig) and ex_cfg.type == EXECUTOR_TYPE
    assert ex_cfg.db_path and ex_cfg.cell_count == 10 and ex_cfg.controller_id == "ng-test"
    assert controller.determine_executor_actions() == []            # one executor for all cells, ever
    assert ExecutorOrchestrator._executor_mapping[EXECUTOR_TYPE] is NeutralGridExecutor


def test_disabled_controller_creates_nothing_and_says_so():
    controller = make_controller(controller_config(enabled=False))
    assert controller.determine_executor_actions() == []
    assert "enabled=false" in controller.to_format_status()[0]


# ------------------------------------------------------------------------------------------ executor
def _strategy(clock):
    strategy = MagicMock()
    strategy.connectors = {}
    type(strategy).current_timestamp = property(lambda self: clock())
    return strategy


def _executor(tmp_path, clock, fx, **updates):
    values = dict(connector_name="lighter_perpetual_robinhood", trading_pair="LIT-USDG", grid_id="g-exec",
                  lower_price=D("5"), upper_price=D("6"), cell_count=10, order_amount_base=D("10"),
                  leverage=D("5"), expected_initial_position=D("0"), max_abs_net_position=D("1000"),
                  max_gross_position=D("1000"), max_active_orders=120, enabled=True,
                  db_path=str(tmp_path / "ng.sqlite3"), operator_confirmed_start=True,
                  operator_confirmed_baseline=True, timestamp=clock())
    values.update(updates)
    config = NeutralGridExecutorConfig(**values)
    return NeutralGridExecutor(_strategy(clock), config, port=fx, clock=clock)


def _run(loop, executor, clock, ticks):
    for _ in range(ticks):
        loop.run_until_complete(executor.control_task())
        clock.advance(1.0)


def test_executor_hosts_the_engine_bootstraps_trades_and_stops_without_flattening(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    import hummingbot.strategy_v2.executors.neutral_grid_executor.store as store_mod
    monkeypatch.setattr(store_mod, "default_lock_dir", lambda base_dir=None: tmp_path / "locks")
    clock = FakeClock()
    fx = FakeExchange(clock, domain="lighter_perpetual_robinhood")
    executor = _executor(tmp_path, clock, fx)
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(executor.on_start())
        engine = executor.engine
        assert engine is not None and engine.fatal_reason is None
        _run(loop, executor, clock, 25)                                 # launcher-confirmed start + baseline
        assert engine.bootstrapped and engine.engine_state == EngineState.NORMAL
        assert len(fx.submits()) == 10
        info = executor.executor_info
        assert info.type == EXECUTOR_TYPE and info.custom_info["snapshot"]["engine_state"] == "NORMAL"
        assert "state NORMAL" in executor.status_text()
        cell = max(c for c, ledger in engine.cells.items() if ledger.spec.entry_side.value == "BUY")
        entry = next(leg for leg in engine.cells[cell].legs() if leg.state == OrderState.LIVE)
        fx.fill(entry.cid, D("10"))
        _run(loop, executor, clock, 3)
        executor.early_stop(keep_position=False)                        # never flattens regardless
        for _ in range(60):
            _run(loop, executor, clock, 1)
            if executor.close_type is not None:
                break
        assert executor.close_type == CloseType.EARLY_STOP
        assert engine.engine_state == EngineState.STOPPED_WITH_INVENTORY
        assert fx.net_position == D("10") and fx.open_orders(owned=True) == []
        assert executor._collect_held_position_orders() == []           # orchestrator builds no PositionHold
    finally:
        if executor.engine is not None and executor.engine.store is not None and not executor.engine.store.closed:
            executor.engine.store.close()
        loop.close()


def test_executor_refuses_disabled_config(tmp_path):
    clock = FakeClock()
    executor = _executor(tmp_path, clock, FakeExchange(clock), enabled=False)
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(executor.on_start())
        assert executor.engine is None and executor.close_type == CloseType.FAILED
        assert "enabled=false" in executor.start_error
    finally:
        loop.close()


def test_executor_fails_closed_when_the_store_refuses(tmp_path, monkeypatch):
    import hummingbot.strategy_v2.executors.neutral_grid_executor.store as store_mod
    monkeypatch.setattr(store_mod, "default_lock_dir", lambda base_dir=None: tmp_path / "locks")
    clock = FakeClock()
    db = tmp_path / "ng.sqlite3"
    db.write_bytes(b"not a sqlite database at all" * 8)                 # corrupt file of a previous run
    executor = _executor(tmp_path, clock, FakeExchange(clock), db_path=str(db))
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(executor.on_start())
        assert executor.start_error and "STORE_OPEN_REFUSED" in executor.start_error
        loop.run_until_complete(executor.control_task())
        assert executor.engine.engine_state == EngineState.DEGRADED
        executor.early_stop()
        assert executor.close_type == CloseType.EARLY_STOP
    finally:
        loop.close()


def test_register_executor_type_is_idempotent():
    register_executor_type()
    register_executor_type()
    assert ExecutorOrchestrator._executor_mapping[EXECUTOR_TYPE] is NeutralGridExecutor


def test_launcher_baseline_mismatch_is_refused_once_not_spammed(tmp_path, monkeypatch):
    import hummingbot.strategy_v2.executors.neutral_grid_executor.store as store_mod
    monkeypatch.setattr(store_mod, "default_lock_dir", lambda base_dir=None: tmp_path / "locks")
    clock = FakeClock()
    fx = FakeExchange(clock, initial_position=D("7"))                    # venue does not match B=0
    executor = _executor(tmp_path, clock, fx)
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(executor.on_start())
        _run(loop, executor, clock, 40)
        engine = executor.engine
        assert not engine.bootstrapped and fx.submits() == []
        confirms = [c for c in engine.store.list_commands() if c.kind == "confirm_baseline"]
        assert len(confirms) == 1 and confirms[0].result["error"] == "BASELINE_MISMATCH"
        assert "BASELINE_MISMATCH" in executor.start_error
    finally:
        executor.engine.store.close()
        loop.close()
