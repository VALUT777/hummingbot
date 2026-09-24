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
    register_durable_cids,
    register_executor_type,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import FakeClock, FakeExchange
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from scripts.lighter_robinhood_fixed_neutral_grid import (
    CONTROLLER_CONFIG_NAME,
    LighterRobinhoodFixedNeutralGrid,
    LighterRobinhoodFixedNeutralGridConfig,
    confirmation_phrase,
    validate_api_credentials,
    validate_profile,
)

D = Decimal
ROOT = Path(__file__).resolve().parents[3]
EXAMPLE_CONTROLLER = ROOT / "conf/controllers/lighter_robinhood_fixed_neutral_grid.yml.example"
EXAMPLE_SCRIPT = ROOT / "conf/scripts/lighter_robinhood_fixed_neutral_grid.yml.example"


@pytest.fixture(autouse=True)
def _isolated_neutral_grid_host_dir(tmp_path, monkeypatch):
    # Never touch the real host-wide lock/marker directory (~/.hummingbot/neutral_grid).
    monkeypatch.setenv("HUMMINGBOT_NEUTRAL_GRID_HOST_DIR", str(tmp_path / "ng-host"))


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
    assert validate_profile(controller_config(max_abs_net_position=D("2000"))) == []    # a user limit, not a ceiling
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
    assert ex_cfg.cell_count == 10 and ex_cfg.controller_id == "ng-test"
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


def test_executor_wires_live_history_wakeups_as_hints_only(tmp_path):
    clock = FakeClock()
    fx = FakeExchange(clock)
    subscribed = []
    fx.subscribe_history_wakeups = subscribed.append                    # the LighterExchangePort hook
    fx.unsubscribe_history_wakeups = lambda cb: subscribed.remove(cb)
    executor = _executor(tmp_path, clock, fx)
    import hummingbot.strategy_v2.executors.neutral_grid_executor.store as store_mod
    original = store_mod.default_lock_dir
    store_mod.default_lock_dir = lambda base_dir=None: tmp_path / "locks"
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(executor.on_start())
        assert len(subscribed) == 1
        before = executor.engine.scanner._wake_requested
        subscribed[0]()                                                   # private-stream activity
        assert executor.engine.scanner._wake_requested and not before     # only a poll hint
        assert executor.engine.all_legs() == []                           # never evidence
        executor.on_stop()
        assert subscribed == [] and executor.engine.store.closed          # lock released on stop
    finally:
        store_mod.default_lock_dir = original
        if executor.engine.store is not None and not executor.engine.store.closed:
            executor.engine.store.close()
        loop.close()


# ------------------------------------------------------------------------------------------ CID ownership / stop
def test_durable_cids_are_registered_before_polling_and_released_once_final(tmp_path):
    clock = FakeClock()
    fx = FakeExchange(clock, domain="lighter_perpetual_robinhood")
    first = _executor(tmp_path, clock, fx)
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(first.on_start())
        _run(loop, first, clock, 25)
        engine = first.engine
        cell = max(c for c, ledger in engine.cells.items() if ledger.spec.entry_side.value == "BUY")
        entry = next(leg for leg in engine.cells[cell].legs() if leg.state == OrderState.LIVE)
        fx.fill(entry.cid, D("10"))                                     # entry final, its TP now live
        for _ in range(40):
            _run(loop, first, clock, 1)
            if engine.leg_by_cid(entry.cid).state == OrderState.TERMINAL:
                break
        assert engine.leg_by_cid(entry.cid).state == OrderState.TERMINAL
        assert entry.cid in fx.released_tracking                        # connector stops tracking it (no events)
        live = {o.client_order_id for o in fx.open_orders(owned=True)}
        assert live <= fx.history_reconciled                            # submitted CIDs are engine-owned
        first.on_stop()                                                 # process exit: orders stay on the venue
        assert fx.open_orders(owned=True)
        # Next process: a fresh connector knows nothing; the launcher registers every durable CID that may have
        # reached transport BEFORE the connector's polling loops start (read-only ledger access, no lock).
        fx.history_reconciled.clear()
        fx.released_tracking.clear()
        cids = register_durable_cids(fx, first.config.db_path)
        assert live <= set(cids) and entry.cid in cids and fx.history_reconciled == set(cids)
        # Hummingbot's generic stop/exit cancel path skips engine-owned CIDs (the real connector filters them).
        assert fx.hummingbot_cancel_all() == [] and fx.open_orders(owned=True)
        second = _executor(tmp_path, clock, fx)
        loop.run_until_complete(second.on_start())                    # idempotent re-registration + release
        assert set(second.registered_cids) == set(cids) <= fx.history_reconciled
        assert entry.cid in fx.released_tracking and not (live & fx.released_tracking)
        second.on_stop()
    finally:
        loop.close()


def test_register_durable_cids_needs_an_owner_and_an_existing_ledger(tmp_path):
    clock = FakeClock()
    assert register_durable_cids(FakeExchange(clock), str(tmp_path / "missing.sqlite3")) == []
    assert register_durable_cids(object(), str(tmp_path / "missing.sqlite3")) == []


def test_launcher_stop_drives_the_engine_drain(tmp_path):
    """on_stop of the launcher is the ONLY stop path of engine-owned CID orders: StopExecutorAction ->
    executor.early_stop -> durable STOP command -> engine cancels its own orders -> proven drain -> executor
    closes -> detached. The running executor control loop does the work (nothing is cancelled by the launcher)."""
    clock = FakeClock()
    fx = FakeExchange(clock, domain="lighter_perpetual_robinhood")
    values = dict(connector_name="lighter_perpetual_robinhood", trading_pair="LIT-USDG", grid_id="g-stop",
                  lower_price=D("5"), upper_price=D("6"), cell_count=10, order_amount_base=D("10"),
                  leverage=D("5"), expected_initial_position=D("0"), max_abs_net_position=D("1000"),
                  max_gross_position=D("1000"), max_active_orders=120, enabled=True,
                  db_path=str(tmp_path / "ng.sqlite3"), operator_confirmed_start=True,
                  operator_confirmed_baseline=True, timestamp=clock(), controller_id="ng-ctl")
    executor = NeutralGridExecutor(_strategy(clock), NeutralGridExecutorConfig(**values), update_interval=0.001,
                                   port=fx, clock=clock)
    orchestrator = ExecutorOrchestrator.__new__(ExecutorOrchestrator)   # no strategy/recorder side effects
    orchestrator.active_executors = {"ng-ctl": [executor]}
    orchestrator.cached_performance = {"ng-ctl": object()}
    orchestrator.positions_held = {"ng-ctl": []}
    launcher = LighterRobinhoodFixedNeutralGrid.__new__(LighterRobinhoodFixedNeutralGrid)
    launcher.controllers = {}
    launcher.executor_orchestrator = orchestrator

    async def scenario():
        running = True

        async def advance_clock():
            while running:
                clock.advance(0.5)
                await asyncio.sleep(0.002)

        advancer = asyncio.ensure_future(advance_clock())
        executor.start()
        for _ in range(5000):
            if executor.engine is not None and executor.engine.engine_state == EngineState.NORMAL \
                    and len(fx.open_orders(owned=True)) == 10:
                break
            await asyncio.sleep(0.002)
        engine = executor.engine
        assert engine.engine_state == EngineState.NORMAL
        cell = max(c for c, ledger in engine.cells.items() if ledger.spec.entry_side.value == "BUY")
        fx.fill(next(leg for leg in engine.cells[cell].legs() if leg.state == OrderState.LIVE).cid, D("10"))
        await asyncio.sleep(0.05)
        cancels_before = len(fx.cancels())
        clean = await launcher.drain_neutral_executors(timeout_s=30.0, poll_s=0.005)
        running = False
        await advancer
        return engine, clean, cancels_before

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        engine, clean, cancels_before = loop.run_until_complete(scenario())
        assert clean and executor.is_closed and executor.close_type == CloseType.EARLY_STOP
        assert orchestrator.active_executors["ng-ctl"] == []                # detached before V2 persistence
        assert engine.meta.stop_outcome == EngineState.STOPPED_WITH_INVENTORY.value
        assert fx.open_orders(owned=True) == [] and fx.net_position == D("10")   # drained, never flattened
        assert len(fx.cancels()) > cancels_before                            # the engine sent the cancels
        assert engine.store.closed                                           # lock released by executor.on_stop
        from hummingbot.strategy_v2.executors.neutral_grid_executor.store import NeutralGridStore
        ro = NeutralGridStore.open_readonly(values["db_path"])
        try:
            cancelled = {c.client_order_id for c in fx.cancels()}
            intents = {o.cid for cid in cancelled for o in ro.outbox_for_cid(cid) if o.kind == "CANCEL"}
            assert cancelled == intents                                      # every cancel had a durable intent
            assert [c.kind for c in ro.list_commands() if c.kind == "stop"]
        finally:
            ro.close()
    finally:
        asyncio.set_event_loop(None)
        loop.close()


# ------------------------------------------------------------------------------------------ review package 2
def test_a_hummingbot_stop_is_resent_after_a_conflict_until_the_engine_applies_it(tmp_path):
    from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CommandKind, CommandStatus
    clock = FakeClock()
    fx = FakeExchange(clock, domain="lighter_perpetual_robinhood")
    executor = _executor(tmp_path, clock, fx)
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(executor.on_start())
        _run(loop, executor, clock, 25)
        engine = executor.engine
        assert engine.engine_state == EngineState.NORMAL and len(fx.open_orders(owned=True)) == 10
        engine.enqueue(CommandKind.PAUSE, {"reason": "web pause"}, key="web-pause")   # applied first
        executor.early_stop()                                        # carries the same (soon stale) revisions
        for _ in range(80):
            _run(loop, executor, clock, 1)
            if executor.close_type is not None:
                break
        stops = [c for c in engine.store.list_commands(limit=50) if c.kind == "stop"]
        assert any(c.status == CommandStatus.CONFLICT for c in stops)          # the race really happened
        assert any(c.status == CommandStatus.APPLIED for c in stops)           # re-sent with fresh revisions
        assert engine.meta.stop_requested_ms is not None
        assert executor.close_type == CloseType.EARLY_STOP and fx.open_orders(owned=True) == []
    finally:
        if executor.engine is not None and executor.engine.store is not None and not executor.engine.store.closed:
            executor.engine.store.close()
        loop.close()


def _drain_harness(tmp_path, clock, fx, **updates):
    values = dict(connector_name="lighter_perpetual_robinhood", trading_pair="LIT-USDG", grid_id="g-stop",
                  lower_price=D("5"), upper_price=D("6"), cell_count=10, order_amount_base=D("10"),
                  leverage=D("5"), expected_initial_position=D("0"), max_abs_net_position=D("1000"),
                  max_gross_position=D("1000"), max_active_orders=120, enabled=True,
                  db_path=str(tmp_path / "ng.sqlite3"), operator_confirmed_start=True,
                  operator_confirmed_baseline=True, timestamp=clock(), controller_id="ng-ctl")
    values.update(updates)
    executor = NeutralGridExecutor(_strategy(clock), NeutralGridExecutorConfig(**values), update_interval=0.001,
                                   port=fx, clock=clock)
    orchestrator = ExecutorOrchestrator.__new__(ExecutorOrchestrator)
    orchestrator.active_executors = {"ng-ctl": [executor]}
    orchestrator.cached_performance = {"ng-ctl": object()}
    orchestrator.positions_held = {"ng-ctl": []}
    launcher = LighterRobinhoodFixedNeutralGrid.__new__(LighterRobinhoodFixedNeutralGrid)
    launcher.controllers = {}
    launcher.executor_orchestrator = orchestrator
    return executor, launcher


def test_a_drain_reports_the_durable_outcome_and_waits_at_least_the_uncertain_bound(tmp_path):
    from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import EngineOptions
    from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import CancelBehavior
    from scripts.lighter_robinhood_fixed_neutral_grid import STOP_DRAIN_TIMEOUT_S
    assert STOP_DRAIN_TIMEOUT_S >= EngineOptions().stop_uncertain_after_s
    clock = FakeClock()
    fx = FakeExchange(clock, domain="lighter_perpetual_robinhood")
    executor, launcher = _drain_harness(tmp_path, clock, fx)

    async def scenario():
        running = True

        async def advance_clock():
            while running:
                clock.advance(0.5)
                await asyncio.sleep(0.002)

        advancer = asyncio.ensure_future(advance_clock())
        executor.start()
        for _ in range(5000):
            if executor.engine is not None and len(fx.open_orders(owned=True)) == 10:
                break
            await asyncio.sleep(0.002)
        victim = next(o.client_order_id for o in fx.open_orders(owned=True))
        for _ in range(1000):
            fx.script_cancel(CancelBehavior.TIMEOUT_NOT_LANDED, victim)      # this order's cancel never lands
        clean = await launcher.drain_neutral_executors(timeout_s=3.0, poll_s=0.005)
        running = False
        await advancer
        return clean

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        clean = loop.run_until_complete(scenario())
        assert clean is False
        assert launcher.last_drain_outcome == "STOP_UNCERTAIN"           # read from the durable engine state
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def test_e_controller_default_database_is_per_account_and_market():
    cfg = controller_config(enabled=True)
    assert cfg.resolved_db_path() is None                             # store default: <domain>.<account>.<pair>
    assert make_controller(cfg).config.executor_config().db_path is None
    from hummingbot.strategy_v2.executors.neutral_grid_executor.executor import engine_db_path
    clock = FakeClock()
    fx = FakeExchange(clock, domain="lighter_perpetual_robinhood")
    path = engine_db_path(None, "lighter_perpetual_robinhood", "LIT-USDG", fx)
    assert path.endswith(f"lighter_perpetual_robinhood.{fx.account_index}.LIT-USDG.sqlite3")
    assert "g-exec" not in path and engine_db_path("/x/y.sqlite3", "c", "p", fx) == "/x/y.sqlite3"


def test_h_profile_caps_are_defaults_not_a_product_ceiling():
    from scripts.lighter_robinhood_fixed_neutral_grid import profile_warnings
    cfg = controller_config(enabled=True, max_abs_net_position=D("1500"), max_gross_position=D("1500"))
    assert validate_profile(cfg) == []
    assert any("1500" in w for w in profile_warnings(cfg))
    assert profile_warnings(controller_config(enabled=True)) == []


def test_i_operator_confirmations_cannot_be_loaded_from_yaml():
    for name in ("operator_confirmed_start", "operator_confirmed_baseline"):
        with pytest.raises(ValidationError):
            controller_config(enabled=True, **{name: True})
    cfg = controller_config(enabled=True)
    ex = cfg.executor_config()
    assert ex.operator_confirmed_start is False and ex.operator_confirmed_baseline is False
    cfg.mark_operator_confirmed(start=True, baseline=True)          # only the fixed launcher calls this
    ex = cfg.executor_config()
    assert ex.operator_confirmed_start is True and ex.operator_confirmed_baseline is True


def test_b_e_launcher_confirmations_bind_resume_and_migration_to_explicit_phrases():
    from scripts.lighter_robinhood_fixed_neutral_grid import (
        evaluate_launch,
        migration_phrase,
        resume_phrase,
    )
    connector = MagicMock()
    connector._api_private_key = "0x" + "ab" * 40
    cfg = controller_config(enabled=True, connector_name="lighter_perpetual_robinhood", trading_pair="LIT-USDG",
                            grid_id="g1")
    phrase = confirmation_phrase("g1", cfg.expected_initial_position)
    script = LighterRobinhoodFixedNeutralGridConfig(live_start_confirmation=phrase)
    decision = evaluate_launch(cfg, script, connector, durable_stop_ms=None)
    assert decision.refusal is None and decision.resume_stop_ms is None and not decision.migrate
    # a durable stop is never resumed by the standing start phrase alone
    decision = evaluate_launch(cfg, script, connector, durable_stop_ms=1790000123000)
    assert decision.resume_stop_ms is None and "RESUME" in (decision.notice or "")
    script = LighterRobinhoodFixedNeutralGridConfig(live_start_confirmation=phrase,
                                                    resume_after_stop_confirmation=resume_phrase("g1", 1790000123000))
    assert evaluate_launch(cfg, script, connector, durable_stop_ms=1790000123000).resume_stop_ms == 1790000123000
    assert evaluate_launch(cfg, script, connector, durable_stop_ms=1790000999000).resume_stop_ms is None
    script = LighterRobinhoodFixedNeutralGridConfig(live_start_confirmation=phrase,
                                                    migrate_grid_confirmation=migration_phrase("g1"))
    assert evaluate_launch(cfg, script, connector, durable_stop_ms=None).migrate is True
