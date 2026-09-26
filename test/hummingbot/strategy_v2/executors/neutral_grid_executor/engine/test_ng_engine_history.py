"""Authoritative-history integration (FakeExchange + HistoryScanner + real SQLite store):
AC-09/10/11/12/13/14/22/40/41/42/53."""
import json
from decimal import Decimal

import pytest
from ng_engine_harness import Harness

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    MAX_CLIENT_ORDER_ID,
    CommandKind,
    EngineState,
    LegRole,
    OrderState,
    Side,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import EngineOptions

D = Decimal


def _started(h, baseline=D("0")):
    h.bootstrap(baseline)
    h.tick(2)


def _entry(h, cell_id):
    leg = h.live_order(cell_id, LegRole.ENTRY)
    assert leg is not None
    return leg


def _tps(h, cell_id):
    return h.legs(cell_id, LegRole.TP)


@pytest.fixture
def h(tmp_path):
    harness = Harness(tmp_path)
    yield harness
    harness.close()


def test_ac09_ws_duplicates_and_reorder_never_double_fills_or_prove_terminal(h):
    _started(h)
    cell = h.buy_cells()[-1]
    entry = _entry(h, cell)
    h.fx.ws_duplicate = True
    h.fx.fill(entry.cid, D("10"), history_lag_s=15)          # WS now (twice), history later
    h.engine.wake({"type": "order", "status": "filled", "client_order_id": entry.cid})   # reordered terminal event
    h.engine.wake({"type": "trade", "trade_id": "999", "client_order_id": entry.cid})
    h.tick(8)
    leg = h.legs(cell, LegRole.ENTRY)[0]
    assert leg.filled == 0 and _tps(h, cell) == []           # no fill without history
    assert leg.state != OrderState.TERMINAL                  # no terminal proof from WS
    h.run_until(lambda: _tps(h, cell) != [], max_ticks=40)
    assert h.cell(cell).cycles[-1].E == D("10")              # exactly once
    assert [t.requested for t in _tps(h, cell)] == [D("10")]
    assert len(h.engine.store.fills(entry.cid)) == 1


def _flood_inactive(h, n):
    """n unowned zero-fill orders placed and cancelled by the operator (newer inactive rows)."""
    for _ in range(n):
        order = h.fx.place_manual_order(Side.BUY, D("4.0"), D("1"))
        h.fx.venue_cancel(order.client_order_id)
        h.clock.advance(0.01)


def test_ac10_target_terminal_row_on_a_later_inactive_page_is_found(h):
    _started(h)
    cell = h.buy_cells()[-1]
    entry = _entry(h, cell)
    h.fx.fill(entry.cid, D("6"))
    h.fx.venue_cancel(entry.cid)                             # our terminal row ...
    h.clock.advance(1)
    _flood_inactive(h, 150)                                  # ... pushed behind 150 newer rows
    h.settle(max_ticks=60)
    leg = h.legs(cell, LegRole.ENTRY)[0]
    assert leg.state == OrderState.TERMINAL and leg.terminal_cumulative == D("6")
    assert max(p for t, e, c, p in h.fx.page_log if e == "inactive_orders") >= 2   # first page was not "complete"


def test_ac11_more_than_100_trades_exact_cumulative_with_boundary_duplicates(tmp_path):
    h = Harness(tmp_path, fx_kwargs={"min_base": D("0.1"), "min_notional": D("0.1")})
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = _entry(h, cell)
        h.fx.duplicate_boundary["trades"] = True
        for _ in range(150):                                  # 150 own fills, all visible at once later
            h.fx.fill(entry.cid, D("0.05"), history_lag_s=20)
            h.clock.advance(0.01)
        h.tick(40)
        assert h.cell(cell).cycles[-1].E == D("7.50")
        assert len(h.engine.store.fills(entry.cid)) == 150
        assert sum(t.requested for t in _tps(h, cell)) <= D("7.5")
        assert max(p for t, e, c, p in h.fx.page_log if e == "trades") >= 2
    finally:
        h.close()


@pytest.mark.parametrize("fault", ["repeat_cursor", "malformed_cursor", "page_error"])
def test_ac12_bad_pagination_makes_history_incomplete_and_blocks_entries(tmp_path, fault):
    h = Harness(tmp_path, fx_kwargs={"min_base": D("0.1"), "min_notional": D("0.1")})
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = _entry(h, cell)
        for _ in range(120):
            h.fx.fill(entry.cid, D("0.05"), history_lag_s=3)
        h.fx.inject_page_fault("trades", fault, page=2)
        h.clock.advance(4)
        submits_before = len(h.fx.submits())
        seen_incomplete = False
        for _ in range(6):
            h.tick()
            if not h.engine.history_complete:
                seen_incomplete = True
                assert any(b.startswith("HISTORY_INCOMPLETE") for b in h.engine.entry_blockers)
                assert h.state != EngineState.NORMAL
                assert not [c for c in h.fx.submits()[submits_before:]
                            if h.engine.leg_by_cid(c.client_order_id).identity.role == LegRole.ENTRY]
        assert seen_incomplete
        h.run_until(lambda: h.engine.history_complete, max_ticks=80)      # recovers once the fault is gone
        assert h.cell(cell).cycles[-1].E == D("6.00")
    finally:
        h.close()


def test_ac12_ac40_conflicting_duplicate_freezes_exposure(tmp_path):
    h = Harness(tmp_path, fx_kwargs={"min_base": D("0.1"), "min_notional": D("0.1")})
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = _entry(h, cell)
        trade_id = h.fx.fill(entry.cid, D("2"))
        h.tick(8)
        assert h.cell(cell).cycles[-1].E == D("2")
        h.fx.inject_conflicting_trade(trade_id, D("3"))       # same key, different size in a later scan
        for _ in range(20):
            h.fx.fill(entry.cid, D("0.01"))
        h.tick(12)
        assert h.state == EngineState.FROZEN, (h.state, h.engine.reasons)
        assert h.engine.b_engine.manual_reconcile_required
        assert h.cell(cell).cycles[-1].E == D("2.20")          # the conflicting size (3) is never applied
        entries_before = [c for c in h.fx.submits() if h.engine.leg_by_cid(c.client_order_id).identity.role
                          == LegRole.ENTRY]
        h.tick(10)
        entries_after = [c for c in h.fx.submits() if h.engine.leg_by_cid(c.client_order_id).identity.role
                         == LegRole.ENTRY]
        assert entries_after == entries_before                 # exposure stopped
    finally:
        h.close()


def test_ac13_history_lag_wakes_poller_tp_waits_and_lag_visible(h):
    _started(h)
    cell = h.buy_cells()[-1]
    entry = _entry(h, cell)
    h.tick(3)
    pages_before = len(h.fx.page_log)
    h.fx.fill(entry.cid, D("10"), history_lag_s=6)
    h.tick(2)
    assert len(h.fx.page_log) > pages_before                  # WS wake triggered a scan before the poll interval
    assert _tps(h, cell) == []                                # TP waits for history
    lag = D(h.engine.last_snapshot["summary"]["history"]["lag_s"])
    assert lag > 0
    h.run_until(lambda: _tps(h, cell) != [], max_ticks=20)
    h.tick()
    assert h.engine.last_snapshot["summary"]["history"]["lag_s"] == "0.0"


def test_ac14_tp_dispatch_within_slo_and_blocker_queue_age_visible(tmp_path):
    h = Harness(tmp_path, options=EngineOptions(max_submits_per_tick=1, min_wake_interval_s=1.0))
    try:
        _started(h)
        h.tick(15)                                            # all entries placed at 1 submit/tick
        cells = [h.buy_cells()[-1], h.sell_cells()[0]]
        for c in cells:
            h.fx.fill(_entry(h, c).cid, D("10"))
        h.tick()
        dispatched = [c for c in cells if _tps(h, c)]
        waiting = [c for c in cells if not _tps(h, c)]
        assert len(dispatched) == 1 and len(waiting) == 1
        latency = D(h.engine.last_snapshot["summary"]["tp_dispatch"]["last_latency_s"])
        assert latency <= D("2")                              # history commit -> router within SLO
        h.tick()
        view = next(x for x in h.engine.last_snapshot["cells"] if x["cell_id"] == waiting[0])
        if not _tps(h, waiting[0]):
            assert view["blocker"] and view["queue_age_s"] is not None
        h.run_until(lambda: all(_tps(h, c) or "WAIT_TP_FIFO" in (h.engine.cell_blockers.get(c) or "")
                                for c in cells), max_ticks=10)
        blocked = [x for x in h.engine.last_snapshot["cells"] if x["blocker"]]
        for x in blocked:
            assert x["queue_age_s"] is not None or x["obligation"]["unassigned"] == "0"
    finally:
        h.close()


def test_ac22_ids_beyond_float_range_stay_exact_strings(h):
    _started(h)
    cell = h.buy_cells()[-1]
    entry = _entry(h, cell)
    h.fx.fill(entry.cid, D("10"))
    h.settle()
    leg = h.legs(cell, LegRole.ENTRY)[0]
    venue = h.fx.order_by_cid(entry.cid)
    assert leg.exchange_order_id == venue.order_index and int(leg.exchange_order_id) > 2 ** 53
    fills = h.engine.store.fills(entry.cid)
    assert fills and int(fills[0].trade_id_str) > 2 ** 60
    assert fills[0].trade_id_str == h.fx.trade_legs[0].trade_id
    snap = h.engine.store.latest_snapshot()
    doc = json.loads(snap.snapshot_json)
    view = next(c for c in doc["cells"] if c["cell_id"] == cell)
    assert view["entry"]["exchange_id"] == venue.order_index             # string in JSON, not a lossy number
    assert f'"{venue.order_index}"' in snap.snapshot_json
    for call in h.fx.submits():
        assert 0 < call.client_order_id <= MAX_CLIENT_ORDER_ID


def test_ac40_trade_cumulative_above_order_cumulative_stops_exposure(h):
    _started(h)
    cell = h.buy_cells()[-1]
    entry = _entry(h, cell)
    venue = h.fx.order_by_cid(entry.cid)
    venue.history_override = {"filled_base_amount": "4", "remaining_base_amount": "0"}   # venue row says 4
    h.fx.fill(entry.cid, D("6"))                                                           # trades say 6
    h.fx.venue_cancel(entry.cid)
    h.tick(20)
    assert h.state == EngineState.FROZEN, (h.state, h.engine.reasons)
    leg = h.legs(cell, LegRole.ENTRY)[0]
    assert leg.state != OrderState.TERMINAL                    # quantity is never guessed
    assert h.engine.store_entry_blockers


def test_ac41_duplicates_and_found_ids_do_not_stop_the_scan(h):
    _started(h)
    cell = h.buy_cells()[-1]
    entry = _entry(h, cell)
    h.fx.fill(entry.cid, D("10"))
    h.clock.advance(1)
    _flood_inactive(h, 120)
    h.fx.duplicate_boundary["inactive_orders"] = True
    h.fx.duplicate_boundary["trades"] = True
    h.settle(max_ticks=60)
    assert h.legs(cell, LegRole.ENTRY)[0].state == OrderState.TERMINAL
    streams = {e for t, e, c, p in h.fx.page_log if p >= 2}
    assert "inactive_orders" in streams


def test_ac42_release_waits_delay_and_repeat_scans(h):
    _started(h)
    cell = h.buy_cells()[-1]
    entry = _entry(h, cell)
    h.fx.fill(entry.cid, D("10"))
    filled_at = h.clock()
    h.run_until(lambda: h.legs(cell, LegRole.ENTRY)[0].state == OrderState.TERMINAL, max_ticks=40)
    meta = h.engine.order_meta[entry.cid]
    assert meta.first_terminal_seen_ms is not None
    assert h.clock() - meta.first_terminal_seen_ms / 1000 >= float(h.config.settlement_delay_s)
    assert h.clock() - filled_at >= float(h.config.settlement_delay_s)
    completed = [s for s in h.engine.scanner.completed_scans if s.started_at >= meta.first_terminal_seen_ms / 1000]
    assert len(completed) >= h.config.settlement_scans


def test_ac42_super_delayed_fill_after_reuse_freezes_and_goes_to_old_cycle_only_by_audit(tmp_path):
    h = Harness(tmp_path, fx_kwargs={"min_base": D("1")})
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = _entry(h, cell)
        venue = h.fx.order_by_cid(entry.cid)
        venue.history_override = {"filled_base_amount": "0", "remaining_base_amount": "0"}   # stale venue row
        h.fx.fill(entry.cid, D("3"), history_lag_s=200)                                      # invisible for long
        h.fx.set_position_external(D("-3"))            # ... and the account endpoint lags the same way
        h.fx.venue_cancel(entry.cid)
        h.run_until(lambda: h.cell(cell).generation == 2 and h.live_order(cell, LegRole.ENTRY) is not None,
                    max_ticks=60)                                                             # cell reused
        h.clock.advance(200)
        h.fx.set_position_external(D("3"))
        h.tick(10)
        # The execution contradicts the settled order row (cumulative 0): the scanner withholds the key, the engine
        # applies NO quantity for it (AC-40) and freezes the market for an operator audit.
        old = h.cell(cell).cycles[1]
        assert old.generation == 1 and old.E == D("0")
        assert not h.engine.store.fills(entry.cid)
        assert h.state == EngineState.FROZEN, (h.state, h.engine.reasons)
        assert [r.size for r in h.engine._withheld_late_trades()] == [D("3")]
        entries = len([c for c in h.fx.submits()
                       if h.engine.leg_by_cid(c.client_order_id).identity.role == LegRole.ENTRY])
        h.tick(5)
        assert h.cell(cell).cycles[1].E == D("0")
        assert len([c for c in h.fx.submits()
                    if h.engine.leg_by_cid(c.client_order_id).identity.role == LegRole.ENTRY]) == entries
        # Operator audit: in ONE transaction the withheld execution is committed to the OLD cycle, WS-A
        # acknowledge_late_evidence corrects the leg, the audited cumulative is persisted and the conflicts resolved.
        h.command(CommandKind.BASELINE_AUDIT, {"action": "ack_late_evidence", "note": "venue export reviewed"})
        h.tick()
        old = h.cell(cell).cycles[1]
        assert old.generation == 1 and old.E == D("3")                  # assigned to the OLD cycle
        leg = h.engine.leg_by_cid(entry.cid)
        assert leg.state == OrderState.TERMINAL and leg.terminal_cumulative == D("3") and not leg.late_evidence
        assert h.engine.order_meta[entry.cid].audited_cumulative == "3"
        assert h.engine.store.cycle(h.engine.grid_id, cell, 1).late_evidence == 2
        assert not h.engine.open_conflicts
        audit = h.engine.store.audit_events(kind="manual_reconcile")[0]                   # newest first
        evidence = audit.payload["evidence"]
        assert evidence["corrected_legs"][0]["cid"] == str(entry.cid)
        assert evidence["corrected_legs"][0]["audited_cumulative"] == "3"
        assert evidence["corrected_legs"][0]["venue_terminal_filled"] == "0"
        assert [t["size"] for t in evidence["accepted_trades"]] == ["3"]
        h.tick(12)
        assert h.state != EngineState.FROZEN, h.engine.reasons                # audit unfreezes the market
        # The stale order row still contradicts the audited executions in every walk, but it carries no new
        # evidence: history is complete again (no endless RECONCILING after an audit).
        assert h.engine.history_complete and h.state == EngineState.NORMAL, h.engine.reasons
        assert h.engine.endpoints.P == h.fx.net_position == D("3")          # obligation visible, not guessed
        # After the audit an ordinary TP of the OLD cycle closes the late obligation at the fixed target.
        old_tp = next(t for t in h.legs(cell, LegRole.TP) if t.identity.generation == 1)
        assert (old_tp.side, old_tp.price, old_tp.requested) == (Side.SELL, D("5.4"), D("3"))
        h.fx.fill(old_tp.cid, D("3"))
        h.tick(15)
        old = h.cell(cell).cycles[1]
        assert old.E == old.X == D("3") and not h.engine.open_conflicts          # resolved, no re-latch
        assert h.state != EngineState.FROZEN
        h.restart()                                                              # the audit survives a restart
        h.tick(3)
        leg = h.engine.leg_by_cid(entry.cid)
        assert leg.terminal_cumulative == D("3") and not leg.late_evidence
        assert h.cell(cell).cycles[1].E == h.cell(cell).cycles[1].X == D("3")
    finally:
        h.close()


def _audited_late_fill(h, late_qty=D("3")):
    """Settled entry (stale venue row: cumulative 0) whose execution shows up after the cell was reused, then the
    operator audit. Returns (cell, entry leg)."""
    _started(h)
    cell = h.buy_cells()[-1]
    entry = _entry(h, cell)
    h.fx.order_by_cid(entry.cid).history_override = {"filled_base_amount": "0", "remaining_base_amount": "0"}
    h.fx.fill(entry.cid, late_qty, history_lag_s=200)
    h.fx.set_position_external(-late_qty)
    h.fx.venue_cancel(entry.cid)
    h.run_until(lambda: h.cell(cell).generation == 2 and h.live_order(cell, LegRole.ENTRY) is not None,
                max_ticks=60)
    h.clock.advance(200)
    h.fx.set_position_external(late_qty)
    h.tick(10)
    assert h.state == EngineState.FROZEN and h.cell(cell).cycles[1].E == 0
    h.command(CommandKind.BASELINE_AUDIT, {"action": "ack_late_evidence", "note": "venue export reviewed"})
    h.tick(8)
    assert h.cell(cell).cycles[1].E == late_qty
    return cell, entry


def test_ac40_new_contradiction_after_late_audit_is_never_masked_or_applied(tmp_path):
    h = Harness(tmp_path, fx_kwargs={"min_base": D("5")})
    try:
        cell, entry = _audited_late_fill(h)
        assert h.engine.history_complete and h.state == EngineState.NORMAL, h.engine.reasons
        audited = h.engine.store.fills(entry.cid)[0]
        h.fx.inject_conflicting_trade(audited.trade_id_str, D("4"))       # same trade id, other payload
        h.tick(8)
        assert h.state == EngineState.FROZEN, h.engine.reasons
        assert any("payload_mismatch" in r for r in h.engine.reasons)
        assert h.cell(cell).cycles[1].E == D("3") and h.engine.store.fills(entry.cid)[0].size == D("3")
        assert h.engine._withheld_late_trades() == []                   # a payload conflict is not late evidence
    finally:
        h.close()


def test_ac39_aggregate_tp_over_audited_late_and_current_cycle_is_exact_and_durable(tmp_path):
    h = Harness(tmp_path, fx_kwargs={"min_base": D("5")})
    try:
        cell, _ = _audited_late_fill(h)
        assert not h.legs(cell, LegRole.TP)                                 # 3 < min_base: DUST alone
        gen2 = h.live_order(cell, LegRole.ENTRY)
        h.fx.fill(gen2.cid, D("4"))                                         # gen2 obligation 4: below min too
        h.fx.venue_cancel(gen2.cid)                                         # both entries final -> aggregate
        h.run_until(lambda: h.legs(cell, LegRole.TP), max_ticks=40)
        tp = h.legs(cell, LegRole.TP)[0]
        # One aggregate TP at the fixed target, hosted by the newest generation, with exact shares (AC-39).
        assert (tp.side, tp.price, tp.requested, tp.identity.generation) == (Side.SELL, D("5.4"), D("7"), 2)
        assert tp.allocation == {1: D("3"), 2: D("4")}
        assert h.engine.store.leg_allocation(tp.cid) == {1: D("3"), 2: D("4")}
        h.fx.fill(tp.cid, D("5"))                                            # water-fill: oldest generation first
        h.tick(8)
        assert (h.cell(cell).cycles[1].X, h.cell(cell).cycles[2].X) == (D("3"), D("2"))
        h.crash_restart()
        h.tick(3)
        tp = h.engine.leg_by_cid(tp.cid)
        assert tp.allocation == {1: D("3"), 2: D("4")} and tp.filled == D("5")
        assert (h.cell(cell).cycles[1].X, h.cell(cell).cycles[2].X) == (D("3"), D("2"))
        h.fx.fill(tp.cid, D("2"))
        h.tick(12)
        assert (h.cell(cell).cycles[1].E, h.cell(cell).cycles[1].X) == (D("3"), D("3"))
        assert (h.cell(cell).cycles[2].E, h.cell(cell).cycles[2].X) == (D("4"), D("4"))
        assert h.engine.store.verify_ledger() == []
    finally:
        h.close()


def test_ac53_retention_gap_blocks_until_audited_manual_reconcile(tmp_path):
    h = Harness(tmp_path, fx_kwargs={"retention_s": 300})
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.fill(_entry(h, cell).cid, D("10"))
        h.tick(15)
        baseline = h.engine.effective_baseline
        cells_before = {c: (h.cell(c).generation, h.cell(c).cycles[-1].E) for c in h.engine.cells}
        store = h.engine.store
        store.close()                                            # downtime longer than venue retention
        h.clock.advance(1000)
        h.engine = h.open()
        h.tick(10)
        assert h.state in (EngineState.RISK_BLOCKED, EngineState.FROZEN), (h.state, h.engine.reasons)
        assert h.engine.b_engine.manual_reconcile_required
        assert "retention" in (h.engine.b_engine.manual_reconcile_reason or "")
        entries = len(h.fx.submits())
        h.tick(5)
        assert len(h.fx.submits()) == entries                   # start/reuse blocked
        assert h.engine.effective_baseline == baseline          # no rebaseline
        assert {c: (h.cell(c).generation, h.cell(c).cycles[-1].E) for c in h.engine.cells} == cells_before
        h.command(CommandKind.BASELINE_AUDIT, {"action": "ack_retention_gap", "note": "audited venue export"})
        h.tick(12)
        assert not h.engine.b_engine.manual_reconcile_required
        assert h.engine.history_complete
        assert h.engine.effective_baseline == baseline
        events = [e.kind for e in h.engine.store.audit_events()]
        assert "manual_reconcile" in events
    finally:
        h.close()
