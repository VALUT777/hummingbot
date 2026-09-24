"""Orchestrator review package 2 (engine @ bcd2eebef, persistence / restart / ops): engine-level regressions.

Every test drives the real engine through ``FakeExchange`` + the real SQLite store. Fix tests failed on the
package-1 tip (red evidence in docs/neutral-grid/trace/ws-d-engine.md); test-honesty items (J) are proven by
mutation. Controller/executor/launcher items (A, E-controller, H, I) live in test/controllers/generic.
"""
import sqlite3
from decimal import Decimal

from ng_engine_harness import Harness, make_config, start_payload

from hummingbot.strategy_v2.executors.neutral_grid_executor import risk
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CommandKind,
    CommandStatus,
    EngineState,
    LegRole,
    OrderState,
    Side,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import EngineOptions
from hummingbot.strategy_v2.executors.neutral_grid_executor.engine import NeutralGridEngine, open_engine
from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import CancelBehavior
from hummingbot.strategy_v2.executors.neutral_grid_executor.snapshot import format_status
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import CID_EPOCH_SHIFT

D = Decimal
ENTRY, TP = LegRole.ENTRY, LegRole.TP
_FAST_RULES = EngineOptions(tick_interval_s=1.0, min_wake_interval_s=1.0, rules_refresh_s=1.0,
                            weight_budget_per_min=100000)


def _started(h, baseline=D("0")):
    h.bootstrap(baseline)
    h.tick(2)


def _cmd(h, key):
    return h.engine.store.get_command(idempotency_key=key)


def _launcher_start(key_suffix="p2"):
    return dict(start_payload(), source="launcher")


# ------------------------------------------------------------------------------------------ B durable stop
def test_b_automatic_launcher_start_never_overrides_a_durable_stop(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        victim = h.live_order(h.buy_cells()[-1], ENTRY)
        h.fx.script_cancel(CancelBehavior.TIMEOUT_NOT_LANDED, victim.cid)     # one cancel stays unknown
        h.command(CommandKind.STOP, key="stop-b")
        h.run_until(lambda: h.engine.meta.stop_outcome == "STOP_UNCERTAIN", max_ticks=30)
        h.restart()
        submits = len(h.fx.submits())
        h.command(CommandKind.START, _launcher_start(), key="launcher-start-NEWPROCESS")   # new random key
        h.tick(6)
        assert _cmd(h, "launcher-start-NEWPROCESS").status == CommandStatus.REJECTED
        assert h.engine.meta.stop_requested_ms is not None and h.engine.meta.stop_outcome == "STOP_UNCERTAIN"
        assert len(h.fx.submits()) == submits and h.state == EngineState.STOP_UNCERTAIN
        # an explicit operator START (key + expected revisions of a viewed snapshot) is the only way back
        h.command(CommandKind.START, start_payload(), key="operator-start")
        h.tick(2)
        assert _cmd(h, "operator-start").status == CommandStatus.APPLIED
        assert h.engine.meta.stop_requested_ms is None
    finally:
        h.close()


def test_b_launcher_resume_must_name_the_durable_stop_it_resumes(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        h.command(CommandKind.STOP, key="stop-b2")
        h.run_until(lambda: h.engine.is_stopped, max_ticks=40)
        stop_ms = h.engine.meta.stop_requested_ms
        h.command(CommandKind.START, dict(_launcher_start(), resume_stop_ms=stop_ms - 1), key="resume-wrong")
        h.tick()
        assert _cmd(h, "resume-wrong").status == CommandStatus.REJECTED
        h.command(CommandKind.START, dict(_launcher_start(), resume_stop_ms=stop_ms), key="resume-right")
        h.tick(3)
        assert _cmd(h, "resume-right").status == CommandStatus.APPLIED and h.engine.meta.stop_requested_ms is None
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ C pre-send in planning
def test_c_off_tick_prices_are_durable_visible_blockers_without_any_intent_or_cid(tmp_path):
    h = Harness(tmp_path, options=_FAST_RULES)
    try:
        _started(h)
        tp_cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(tp_cell, ENTRY).cid, D("10"))
        idle = h.buy_cells()[0]
        h.fx.venue_cancel(h.live_order(idle, ENTRY).cid)     # this cell becomes idle and eligible again
        h.fx.set_rules(tick_size=D("0.0007"))                # every fixed price is now off tick
        h.tick(4)
        cids = len(h.engine.store.legs())
        h.tick(200)                                          # far beyond any reject backoff
        assert len(h.engine.store.legs()) == cids, "an intent / CID was created for an off-tick price"
        view = {c["cell_id"]: c for c in h.engine.last_snapshot["cells"]}
        assert "PRICE_NOT_ON_TICK" in (view[tp_cell]["blocker"] or "")
        assert view[tp_cell]["queue_age_s"] is not None and D(view[tp_cell]["queue_age_s"]) >= 200
        stored = {c.cell_id: c for c in h.engine.store.cells(h.engine.grid_id)}
        assert "PRICE_NOT_ON_TICK" in (stored[tp_cell].blocker or "")   # durable, not only in memory
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ D baseline audit
def test_d_baseline_audit_is_refused_while_an_own_fill_may_be_missing_from_history(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.manual_trade(Side.BUY, D("7"), D("5.4"))          # outside trade: drift, entries blocked
        h.tick(8)
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("4"), history_lag_s=30)   # own fill, history lags
        h.tick(6)
        assert h.fx.net_position == D("11")
        h.command(CommandKind.BASELINE_AUDIT, {"observed_position": "11", "note": "audit"}, key="d-early")
        h.tick()
        assert _cmd(h, "d-early").status == CommandStatus.REJECTED, _cmd(h, "d-early").result
        assert h.engine.effective_baseline == D("0")
        h.tick(40)                                              # history delivers the own fill
        h.command(CommandKind.BASELINE_AUDIT, {"observed_position": "11", "note": "audit"}, key="d-late")
        h.tick(2)
        assert _cmd(h, "d-late").status == CommandStatus.APPLIED, _cmd(h, "d-late").result
        assert h.engine.effective_baseline == D("7")           # only the manual trade is re-based
        h.tick(10)
        assert h.engine.endpoints.P == h.fx.net_position == D("11")
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ E grid migration
def test_e_quiescent_grid_is_migrated_by_an_audited_command_keeping_old_cycles(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))
        h.run_until(lambda: h.live_order(cell, TP) is not None, max_ticks=10)
        h.fx.fill(h.live_order(cell, TP).cid, D("10"))
        h.tick(10)
        h.command(CommandKind.STOP, key="stop-e")
        h.run_until(lambda: h.engine.is_stopped, max_ticks=40)
        old_grid = h.engine.grid_id
        old_cycles = len(h.engine.store._cycles("grid_id = ?", (old_grid,)))
        h.fx.remove_ws_listener(h.engine.wake)
        h.engine.store.close()
        new = make_config(grid_id="grid-2", lower_price=D("5.1"), upper_price=D("6.1"))
        refused = open_engine(new, str(h.db_path), h.fx, clock=h.clock, options=h.options,
                              lock_dir=str(h.lock_dir))
        assert refused.fatal_reason and "ConfigMutationError" in refused.fatal_reason     # never silently
        engine = open_engine(new, str(h.db_path), h.fx, clock=h.clock, options=h.options, lock_dir=str(h.lock_dir),
                             allow_grid_migration=True, offline_demo=True)
        h.engine, h.config = engine, new
        h.fx.add_ws_listener(engine.wake)
        h.tick(8)
        assert engine.state != EngineState.NORMAL and "FROZEN:CONFIG_MISMATCH" in engine.reasons
        h.command(CommandKind.BASELINE_AUDIT, {"action": "migrate_grid", "note": "next grid"}, key="migrate")
        h.tick(2)
        assert _cmd(h, "migrate").status == CommandStatus.APPLIED, _cmd(h, "migrate").result
        assert engine.grid_id == "grid-2"
        assert [e for e in engine.store.audit_events() if e.kind == "grid_migration"]
        assert len(engine.store._cycles("grid_id = ?", (old_grid,))) == old_cycles >= 1   # old cycles preserved
        assert engine.effective_baseline == D("0")                       # never re-based by a migration
        h.command(CommandKind.START, start_payload(), key="start-grid-2")
        h.tick(8)
        prices = {c.spec.entry_price for c in engine.cells.values()}
        assert min(prices) >= D("5.1") and h.fx.open_orders(owned=True)
    finally:
        h.close()


def test_e_migration_is_refused_while_the_old_grid_has_obligations(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))        # obligation E=10 stays open
        h.tick(6)
        h.fx.remove_ws_listener(h.engine.wake)
        h.engine.store.close()
        new = make_config(grid_id="grid-3", lower_price=D("5.1"), upper_price=D("6.1"))
        engine = open_engine(new, str(h.db_path), h.fx, clock=h.clock, options=h.options, lock_dir=str(h.lock_dir),
                             allow_grid_migration=True, offline_demo=True)
        h.engine, h.config = engine, new
        h.tick(6)
        h.command(CommandKind.BASELINE_AUDIT, {"action": "migrate_grid", "note": "x"}, key="migrate-busy")
        h.tick()
        assert _cmd(h, "migrate-busy").status == CommandStatus.REJECTED
        assert engine.grid_id == "grid-t"
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ F CID collision
def test_f_cid_collision_is_recovered_by_an_audited_retire(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        epoch = h.engine.b_engine.cid_epoch
        next_cid = (epoch << CID_EPOCH_SHIFT) | (len(h.fx.submits()) + 1)
        h.fx.place_manual_order(Side.BUY, D("4.5"), D("1"), client_order_id=next_cid)
        h.tick(6)
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))
        h.tick(6)
        assert "CID_ALLOCATION" in h.engine.meta.freezes and h.live_order(cell, TP) is None
        h.command(CommandKind.BASELINE_AUDIT, {"action": "retire_colliding_cid", "note": "foreign order owns it"},
                  key="retire")
        h.tick(6)
        assert _cmd(h, "retire").status == CommandStatus.APPLIED, _cmd(h, "retire").result
        assert "CID_ALLOCATION" not in h.engine.meta.freezes
        tp = h.live_order(cell, TP)
        assert tp is not None and tp.cid != next_cid
        assert [e for e in h.engine.store.audit_events() if e.kind == "cid_retired"]
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ G CLI status
def test_g_cli_status_shows_cursor_progress_and_tp_remaining(tmp_path):
    h = Harness(tmp_path, options=EngineOptions(tick_interval_s=1.0, min_wake_interval_s=1.0,
                                                max_scan_pages_per_tick=1))
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))
        h.run_until(lambda: h.live_order(cell, TP) is not None, max_ticks=10)
        h.fx.fill(h.live_order(cell, TP).cid, D("4"))
        h.tick(6)
        text = format_status(h.engine.last_snapshot, now=h.clock())
        assert "trades cursor" in text and "orders cursor" in text and "pages" in text
        assert "remaining 6" in text
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ J test honesty
def test_j_ac12_no_entry_while_history_is_incomplete_and_resumption_after_recovery(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        h.fx.set_book(D("6.2"), D("6.3"))                       # outside bounds: every entry cancelled
        h.run_until(lambda: not [x for x in h.engine.non_final_legs() if x.identity.role == ENTRY], max_ticks=40)
        entries = len([c for c in h.fx.submits() if h.engine.leg_by_cid(c.client_order_id).identity.role == ENTRY])
        h.fx.inject_page_fault("trades", "page_error", page=1, times=100000)   # history cannot complete
        h.engine.wake({"type": "order", "status": "canceled"})
        h.run_until(lambda: not h.engine.history_complete, max_ticks=10)
        h.fx.set_book(D("5.3999"), D("5.4001"))                 # idle cells are eligible again
        for _ in range(10):
            h.tick()
            assert not h.engine.history_complete
            assert len([c for c in h.fx.submits()
                        if h.engine.leg_by_cid(c.client_order_id).identity.role == ENTRY]) == entries
        h.fx._page_faults["trades"].clear()                    # the venue recovers
        h.run_until(lambda: h.engine.history_complete and h.engine.non_final_legs(), max_ticks=60)
        assert len([c for c in h.fx.submits()
                    if h.engine.leg_by_cid(c.client_order_id).identity.role == ENTRY]) > entries
    finally:
        h.close()


def test_j_ac42_release_waits_the_full_settlement_delay_at_tick_granularity(tmp_path):
    h = Harness(tmp_path, settlement_delay_s=D("20"))
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, ENTRY)
        h.fx.fill(entry.cid, D("10"))
        released_at = None
        for _ in range(60):
            h.tick()
            if h.engine.leg_by_cid(entry.cid).state == OrderState.TERMINAL:
                released_at = h.clock() - 1.0                    # the tick that proved it
                break
        first_seen = h.engine.order_meta[entry.cid].first_terminal_seen_ms / 1000
        assert released_at is not None
        assert released_at - first_seen >= 20, (released_at, first_seen)
        assert released_at - first_seen < 20 + 6                 # and not held far beyond delay + a scan
    finally:
        h.close()


def test_j_ac55_intent_write_failure_sends_nothing(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))
        h.hooks.fail_statement("INSERT INTO outbox", sqlite3.OperationalError("disk I/O error"))
        submits = len(h.fx.submits())
        h.tick()                                                 # the TP intent write fails
        assert not h.hooks._statement_faults                     # the fault really fired at the intent write
        assert len(h.fx.submits()) == submits                    # no submit without a committed intent
        assert not h.legs(cell, TP)                              # nothing half-applied
        assert h.state == EngineState.DEGRADED and "PERSISTENCE_FAILURE" in h.engine.reasons
        h.tick(12)
        assert h.live_order(cell, TP) is not None                # recovered, then dispatched
    finally:
        h.close()


def test_j_ac55_dispatch_mark_failure_sends_nothing(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))
        h.hooks.fail_statement("SET status = 'DISPATCHED'", sqlite3.OperationalError("disk I/O error"))
        submits = len(h.fx.submits())
        h.tick()                                                 # the dispatch-mark write fails
        assert not h.hooks._statement_faults
        assert len(h.fx.submits()) == submits
        assert h.state == EngineState.DEGRADED and "PERSISTENCE_FAILURE" in h.engine.reasons
        tp = [t for t in h.legs(cell, TP)]
        assert tp and tp[0].state == OrderState.INTENT          # committed intent, never sent
        h.tick(12)
        assert h.live_order(cell, TP) is not None and h.submits_for(tp[0].cid)   # same CID after recovery
    finally:
        h.close()


def test_j_ac55_cancel_intent_failure_sends_no_cancel(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        h.hooks.fail_statement("INSERT INTO outbox", sqlite3.OperationalError("disk I/O error"))
        cancels = len(h.fx.cancels())
        h.fx.set_book(D("6.2"), D("6.3"))                       # entry cancels would be sent now
        h.tick()                                                 # the first cancel-intent write fails
        assert not h.hooks._statement_faults
        assert len(h.fx.cancels()) == cancels
        assert h.state == EngineState.DEGRADED and "PERSISTENCE_FAILURE" in h.engine.reasons
        h.tick(12)
        assert len(h.fx.cancels()) > cancels                     # after recovery, with durable intents
    finally:
        h.close()


def _non_final_remainders(h, side):
    return sum((x.remaining for x in h.engine.non_final_legs() if x.side == side), D("0"))


def test_j_ac17_unknown_submit_counts_exactly_in_the_reachable_interval_and_slots(tmp_path):
    from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import SubmitBehavior
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))
        h.fx.script_submit(SubmitBehavior.TIMEOUT_NOT_LANDED)
        h.run_until(lambda: any(t.state == OrderState.SUBMIT_UNKNOWN for t in h.legs(cell, TP)), max_ticks=10)
        h.tick(3)
        ep = h.engine.endpoints
        assert ep.P == D("10")
        assert ep.P_min == ep.P - _non_final_remainders(h, Side.SELL)     # includes the unknown TP's 10
        assert ep.P_max == ep.P + _non_final_remainders(h, Side.BUY)
        unknown = next(t for t in h.legs(cell, TP) if t.state == OrderState.SUBMIT_UNKNOWN)
        assert unknown.remaining == D("10")
        assert h.engine.admission_plan.slots.actual == len(h.engine.non_final_legs())   # the unknown holds a slot
        without = risk.endpoints_from_ledgers(h.engine.effective_baseline, list(h.engine.cells.values()))
        assert without.P_min == ep.P_min
    finally:
        h.close()


def test_j_ac21_cancel_unknown_keeps_the_full_remainder_reachable(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, ENTRY)
        h.fx.fill(entry.cid, D("3"))
        h.fx.script_cancel(CancelBehavior.TIMEOUT_NOT_LANDED, entry.cid)
        h.fx.set_book(D("6.2"), D("6.3"))                       # the entry is cancelled (outside bounds)
        h.run_until(lambda: h.engine.leg_by_cid(entry.cid).state == OrderState.CANCEL_UNKNOWN, max_ticks=10)
        h.tick(2)
        ep = h.engine.endpoints
        assert h.engine.leg_by_cid(entry.cid).remaining == D("7")
        assert ep.P_max == ep.P + _non_final_remainders(h, Side.BUY) and ep.P_max >= ep.P + D("7")
        assert h.engine.admission_plan.slots.actual == len(h.engine.non_final_legs())
    finally:
        h.close()


def test_j_honest_stopping_state_during_a_drain(tmp_path):
    h = Harness(tmp_path, settlement_delay_s=D("10"))
    try:
        _started(h)
        h.command(CommandKind.STOP, key="stop-j")
        states = []
        for _ in range(40):
            h.tick()
            states.append(h.state)
            if h.engine.is_stopped:
                break
        assert states[0] == EngineState.STOPPING and EngineState.STOPPING in states[:5]
        assert states[-1] in (EngineState.STOPPED, EngineState.STOPPED_WITH_INVENTORY)
        assert all(s in (EngineState.STOPPING, EngineState.STOPPED, EngineState.STOPPED_WITH_INVENTORY)
                   for s in states), states
    finally:
        h.close()


def test_review2_engine_type_is_importable():
    assert NeutralGridEngine is not None
