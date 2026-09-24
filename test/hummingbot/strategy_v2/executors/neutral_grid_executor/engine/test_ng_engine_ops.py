"""Operator commands, pause/stop semantics, config mutation and committed snapshots/CLI status:
NG-OPS-001/003, NG-UI-003 (engine side), AC-35/36/52, snapshot schema, CLI status."""
import json
from decimal import Decimal

import pytest
from ng_engine_harness import Harness, make_config

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CommandKind,
    CommandStatus,
    EngineState,
    LegRole,
    OrderState,
    OrderTypePolicy,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.engine import NeutralGridEngine
from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import CancelBehavior
from hummingbot.strategy_v2.executors.neutral_grid_executor.snapshot import format_status

D = Decimal


def _started(h, baseline=D("0")):
    h.bootstrap(baseline)
    h.tick(2)


def _entry_submits(h):
    return [c for c in h.fx.submits() if h.engine.leg_by_cid(c.client_order_id).identity.role == LegRole.ENTRY]


def _cmd(h, key):
    return h.engine.store.get_command(idempotency_key=key)


@pytest.fixture
def h(tmp_path):
    harness = Harness(tmp_path)
    yield harness
    harness.close()


def test_ng_ops_001_pause_stops_entries_keeps_tps_and_resume_passes_gate(h):
    _started(h)
    h.command(CommandKind.PAUSE, {"reason": "operator"}, key="p1")
    h.tick()
    assert h.state == EngineState.PAUSED and "OPERATOR_PAUSE" in h.engine.reasons
    entries = len(_entry_submits(h))
    cell = h.buy_cells()[-1]
    h.fx.fill(h.live_order(cell, LegRole.ENTRY).cid, D("10"))
    h.fx.venue_cancel(h.live_order(h.buy_cells()[0], LegRole.ENTRY).cid)
    h.tick(15)
    assert h.live_order(cell, LegRole.TP) is not None                  # TP obligation still served
    assert len(_entry_submits(h)) == entries                            # no new entries/cycles
    assert h.engine.history_complete                                    # reconciliation continues
    h.command(CommandKind.RESUME, key="r1")
    h.tick(3)
    assert _cmd(h, "r1").status == CommandStatus.APPLIED
    assert h.state == EngineState.NORMAL
    h.tick(3)
    assert len(_entry_submits(h)) > entries


def test_resume_is_rejected_while_blocked(h):
    _started(h)
    h.command(CommandKind.PAUSE, key="p")
    h.tick()
    h.fx.rules_available = False
    h.fx.position_available = False
    h.tick(15)
    h.command(CommandKind.RESUME, key="r")
    h.tick()
    cmd = _cmd(h, "r")
    assert cmd.status == CommandStatus.REJECTED and "POSITION_NOT_FRESH" in cmd.result["blockers"]


def test_ac35_stop_success_with_inventory_never_flattens(h):
    _started(h)
    cell = h.buy_cells()[-1]
    h.fx.fill(h.live_order(cell, LegRole.ENTRY).cid, D("10"))
    h.tick(2)
    tp = h.live_order(cell, LegRole.TP)
    h.fx.fill(tp.cid, D("4"))                                           # partially closed obligation
    h.tick(2)
    net_before = h.fx.net_position
    h.command(CommandKind.STOP, {"reason": "operator"}, key="s1")
    h.run_until(lambda: h.engine.is_stopped, max_ticks=60)
    assert h.state == EngineState.STOPPED_WITH_INVENTORY
    assert h.engine.non_final_legs() == []                               # every own order proven terminal
    assert h.fx.open_orders(owned=True) == []
    assert h.fx.net_position == net_before == D("6")                    # position preserved, no flatten
    b = h.cell(cell).buckets()
    assert b.E == D("10") and b.X == D("4")                             # obligation kept in the ledger
    for call in h.fx.submits():
        assert call.request.order_type in (OrderTypePolicy.LIMIT_MAKER, OrderTypePolicy.LIMIT)
    submits = len(h.fx.submits())
    h.tick(10)
    assert len(h.fx.submits()) == submits                               # nothing new after stop


def test_stop_without_inventory_is_stopped(h):
    _started(h)
    h.command(CommandKind.STOP, key="s")
    h.run_until(lambda: h.engine.is_stopped, max_ticks=60)
    assert h.state == EngineState.STOPPED


def test_ac36_stop_with_unknown_cancel_is_stop_uncertain_and_restart_keeps_reconciling(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        victim = h.live_order(h.buy_cells()[0], LegRole.ENTRY)
        for _ in range(3):
            h.fx.script_cancel(CancelBehavior.TIMEOUT_NOT_LANDED, client_order_id=victim.cid)
        h.command(CommandKind.STOP, key="s")
        h.tick(4)
        assert h.state == EngineState.STOP_UNCERTAIN                     # never a false STOPPED
        assert not h.engine.is_stopped
        assert h.fx.order_by_cid(victim.cid).is_open
        h.restart()
        h.tick(2)
        assert h.state == EngineState.STOP_UNCERTAIN                     # durable, reconciliation continues
        h.run_until(lambda: h.engine.is_stopped, max_ticks=80)           # cancel retry eventually proven
        assert h.state == EngineState.STOPPED
        assert not h.fx.order_by_cid(victim.cid).is_open
    finally:
        h.close()


def test_ac52_running_grid_dimensions_cannot_change_and_db_is_not_reset(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        h.fx.fill(h.live_order(h.buy_cells()[-1], LegRole.ENTRY).cid, D("10"))
        h.tick(5)
        h.engine.store.close()
        h.config = make_config(upper_price=D("6.5"))                      # new dimensions for a running grid
        engine = h.open_failclosed()
        assert engine.fatal_reason and "ConfigMutation" in engine.fatal_reason
        h.loop.run_until_complete(engine.tick())
        assert engine.engine_state == EngineState.DEGRADED
        h.config = make_config()                                          # original config reopens intact
        h.engine = h.open()
        h.tick(3)
        assert h.engine.bootstrapped and h.cell(h.buy_cells()[-1]).cycles[-1].E == D("10")
        assert h.engine.store.grid().fingerprint == h.engine.fingerprint
    finally:
        h.close()


def test_command_idempotency_revision_conflict_and_single_start(h):
    first = h.command(CommandKind.START, key="start-1")
    again = h.command(CommandKind.START, key="start-1")
    assert again.duplicate and again.id == first.id                     # refresh/retry: same row
    other = h.command(CommandKind.START, key="start-2")
    assert other.status == CommandStatus.CONFLICT                        # concurrent start while queued
    h.tick()
    assert _cmd(h, "start-1").status == CommandStatus.APPLIED
    h.command(CommandKind.START, key="start-3")
    h.tick()
    assert _cmd(h, "start-3").result.get("already_started") is True    # never a second engine
    stale = h.command(CommandKind.PAUSE, key="p-stale", expected=(0, 0))
    assert stale.status == CommandStatus.CONFLICT                        # stale preview/revision
    h.tick()
    assert not h.engine.meta.operator_paused
    rev = h.engine.store.engine()
    h.command(CommandKind.PAUSE, key="p-ok", expected=(rev.config_revision, rev.engine_revision))
    h.tick()
    assert _cmd(h, "p-ok").status == CommandStatus.APPLIED and h.engine.meta.operator_paused
    h.command(CommandKind.PAUSE, key="p-ok", expected=(rev.config_revision, rev.engine_revision))
    h.tick()
    applied = [c for c in h.engine.store.list_commands() if c.idempotency_key == "p-ok"]
    assert len(applied) == 1


def test_enabled_false_refuses_live_start(tmp_path):
    h = Harness(tmp_path, enabled=False)
    try:
        h.engine.offline_demo = False
        h.command(CommandKind.START, key="s")
        h.tick(2)
        cmd = _cmd(h, "s")
        assert cmd.status == CommandStatus.REJECTED and cmd.result["error"] == "CONFIG_DISABLED"
        assert h.fx.submits() == []
    finally:
        h.close()


def test_committed_snapshot_schema_and_cli_status_without_secrets(h):
    _started(h)
    cell = h.buy_cells()[-1]
    h.fx.fill(h.live_order(cell, LegRole.ENTRY).cid, D("4"))
    h.tick(2)
    stored = h.engine.store.latest_snapshot()
    doc = json.loads(stored.snapshot_json)
    for key in ("snapshot_version", "config_revision", "engine_revision", "committed_at", "engine_state",
                "reasons", "summary", "cells", "unmatched_evidence", "errors", "commands"):
        assert key in doc
    summary = doc["summary"]
    for key in ("baseline", "authoritative_net", "P", "P_min", "P_max", "gross_worst", "max_abs_net_position",
                "max_gross_position", "slots", "armed", "queued", "owned_active", "unknown_orders", "history",
                "margin", "runtime_rules", "dust_total"):
        assert key in summary
    assert isinstance(summary["P_max"], str) and isinstance(summary["baseline"], str)
    view = next(c for c in doc["cells"] if c["cell_id"] == cell)
    assert view["entry"]["filled"] == "4" and isinstance(view["entry"]["cid"], str)
    assert "ENTRY_LIVE" in view["state_flags"] and view["obligation"]["E"] == "4"
    text = format_status(doc, now=h.clock())
    assert f"state {doc['engine_state']}" in text and "baseline 0" in text and f"cell {cell:>3}" in text
    lowered = (stored.snapshot_json + text).lower()
    for secret in ("private_key", "api_key", "password", "secret", "token"):
        assert secret not in lowered
    old = format_status(doc, now=h.clock() + 60)
    assert "SNAPSHOT STALE" in old                                        # staleness is honest


def test_fail_closed_engine_reports_degraded_not_normal(tmp_path):
    h = Harness(tmp_path)
    try:
        engine = NeutralGridEngine(h.config, None, h.fx, h.clock, fatal_reason="STORE_OPEN_REFUSED:test")
        h.loop.run_until_complete(engine.tick())
        assert engine.engine_state == EngineState.DEGRADED and engine.reasons == ["STORE_OPEN_REFUSED:test"]
        snap = engine.current_snapshot()
        assert snap["engine_state"] == "DEGRADED" and snap["summary"]["fatal_reason"] == "STORE_OPEN_REFUSED:test"
    finally:
        h.close()


def test_status_states_are_honest_during_bootstrap_and_reconcile(tmp_path):
    from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import EngineOptions
    h = Harness(tmp_path, options=EngineOptions(tick_interval_s=1.0, min_wake_interval_s=1.0,
                                                max_scan_pages_per_tick=1))
    try:
        assert h.state == EngineState.BOOTSTRAPPING
        h.tick(2)
        assert h.state == EngineState.BOOTSTRAPPING and "BASELINE_NOT_CONFIRMED" in h.engine.reasons
        _started(h)
        h.tick(4)
        assert h.state == EngineState.NORMAL
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, LegRole.ENTRY)
        h.restart()
        for _ in range(100):                                            # > one page of own trades since the cursor
            h.fx.fill(entry.cid, D("0.1"))
        submits = len(h.fx.submits())
        h.tick()
        # a restart with a history backlog larger than one bounded scanner step: honest RECONCILING, nothing sent
        assert h.state == EngineState.RECONCILING, (h.state, h.engine.reasons)
        assert h.engine.scanner.progress_summary()["resumable"]
        assert len(h.fx.submits()) == submits
        while h.engine.scanner.progress_summary()["resumable"]:
            assert h.state != EngineState.NORMAL, "NORMAL before the history walk completed"
            h.tick()
        h.run_until(lambda: h.state == EngineState.NORMAL, max_ticks=10)
        assert h.engine.history_complete and h.engine.startup_reconciled
        assert h.cell(cell).cycles[-1].E == D("10")                     # the whole backlog was applied
        assert all(leg.state != OrderState.INTENT for leg in h.engine.non_final_legs())
    finally:
        h.close()


def test_command_failing_after_a_store_write_is_rolled_back_then_rejected(h, monkeypatch):
    """A store guard that fails after writing poisons the transaction: the engine never catch-and-commits. The
    whole command transaction rolls back, memory is rebuilt from disk and the command is REJECTED in a new one."""
    from hummingbot.strategy_v2.executors.neutral_grid_executor.store import InvalidTransitionError
    _started(h)
    engine = h.engine

    def poisoned(action, payload, now, tx):
        engine.store.kv_set(tx, "poison", {"written": True})
        engine.meta.operator_paused = True                         # memory ahead of the failing transaction
        raise InvalidTransitionError("guard refused after a write")

    monkeypatch.setattr(engine, "_cmd_reconcile", poisoned)
    h.command(CommandKind.BASELINE_AUDIT, {"action": "ack_risk_blocked"}, key="poison-cmd")
    h.tick()
    record = _cmd(h, "poison-cmd")
    assert record.status == CommandStatus.REJECTED and "InvalidTransitionError" in record.result["detail"]
    assert engine.store.kv_get("poison") is None                   # nothing of the failed command committed
    assert engine.meta.operator_paused is False and engine.persistence_error is None
    h.tick(2)
    assert h.state == EngineState.NORMAL, h.engine.reasons
