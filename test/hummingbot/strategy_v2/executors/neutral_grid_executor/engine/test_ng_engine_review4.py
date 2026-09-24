"""Orchestrator round 4 (last engine round): engine-level regressions on FakeExchange + real SQLite.

Fix tests fail on the round-3 tip 624d7e643 (red evidence in docs/neutral-grid/trace/ws-d-engine.md, "Round 4");
test-gap items are proven by mutation. Executor items (H2 re-send, L1 rebind path) live in test/controllers/generic.
"""
import json
from decimal import Decimal

import pytest
from ng_engine_harness import Harness, make_config, start_payload

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CommandKind,
    CommandStatus,
    EngineState,
    LegRole,
    OrderState,
    Side,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import EngineOptions
from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import SubmitBehavior
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import StoreError

D = Decimal
ENTRY, TP = LegRole.ENTRY, LegRole.TP


def _started(h, baseline=D("0")):
    h.bootstrap(baseline)
    h.tick(2)


def _cmd(h, key):
    return h.engine.store.get_command(idempotency_key=key)


def _seen(h):
    """What the operator reviewed: the latest committed snapshot (web / CLI)."""
    return h.engine.latest_committed_snapshot()["summary"]


def _ack(h, key, set_id=None, accepted=None, **extra):
    payload = {"action": "ack_history_conflict", "note": "reviewed"}
    if set_id is not None:
        payload["conflict_set_id"] = set_id
    if accepted is not None:
        payload["accepted"] = accepted
    payload.update(extra)
    h.command(CommandKind.BASELINE_AUDIT, payload, key=key)
    h.tick()
    return _cmd(h, key)


def _unknown_tp(h, cell):
    h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))
    h.fx.script_submit(SubmitBehavior.TIMEOUT_LANDED)
    h.run_until(lambda: any(t.state == OrderState.SUBMIT_UNKNOWN for t in h.legs(cell, TP)), max_ticks=15)
    return next(t for t in h.legs(cell, TP) if t.state == OrderState.SUBMIT_UNKNOWN)


def _resolve_every_tick(h, cid, ticks, prefix):
    """The operator retries resolve_unknown_submit every tick; returns (seconds since start, result) of the first
    APPLIED, else None."""
    t0 = h.clock()
    for i in range(ticks):
        key = f"{prefix}-{i}"
        h.command(CommandKind.BASELINE_AUDIT, {"action": "resolve_unknown_submit", "cid": str(cid), "note": "x"},
                  key=key)
        h.tick()
        record = _cmd(h, key)
        if record.status == CommandStatus.APPLIED:
            return h.clock() - t0, record.result
    return None


# ------------------------------------------------------------------------------------------ H1 (D2-15 + critic)
def test_h1_ws_trade_signal_of_the_cid_blocks_a_not_landed_resolution(tmp_path):
    """The landed TP fills completely (WS trade event under its trade id); history lags far behind; the filled order
    is correctly absent from the active list. 'Never landed' must not be accepted, however long the operator waits."""
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        tp = _unknown_tp(h, cell)
        trade = h.fx.fill(tp.cid, D("10"), history_lag_s=900)
        assert trade in h.engine.ws_pending and h.engine._ws_label_cid[trade] == tp.cid
        assert _resolve_every_tick(h, tp.cid, 200, "h1ws") is None
        assert h.engine.leg_by_cid(tp.cid).state == OrderState.SUBMIT_UNKNOWN
        assert [t.cid for t in h.legs(cell, TP)] == [tp.cid] and h.fx.net_position == D("0")
    finally:
        h.close()


@pytest.mark.parametrize("lag", [15.0, 30.0])
def test_h1_active_list_lag_beyond_settlement_never_yields_a_second_tp(tmp_path, lag):
    h = Harness(tmp_path, fx_kwargs={"active_lag_s": lag})
    try:
        _started(h)
        h.tick(8)
        cell = h.buy_cells()[-1]
        tp = _unknown_tp(h, cell)
        assert _resolve_every_tick(h, tp.cid, 150, "h1lag") is None
        assert [t.cid for t in h.legs(cell, TP)] == [tp.cid]
        assert h.engine.leg_by_cid(tp.cid).state == OrderState.LIVE          # the lagging list proved it landed
        assert len([o for o in h.fx.open_orders(owned=True) if o.side == Side.SELL
                    and o.client_order_id in {t.cid for t in h.legs(cell, TP)}]) == 1
    finally:
        h.close()


def test_h1_not_landed_submit_is_resolvable_only_after_the_unknown_resolution_delay(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))
        h.fx.script_submit(SubmitBehavior.TIMEOUT_NOT_LANDED)
        h.run_until(lambda: any(t.state == OrderState.SUBMIT_UNKNOWN for t in h.legs(cell, TP)), max_ticks=15)
        tp = next(t for t in h.legs(cell, TP) if t.state == OrderState.SUBMIT_UNKNOWN)
        sent_ms = max(o.dispatched_at_ms for o in h.engine.store.outbox_for_cid(tp.cid) if o.kind == "SUBMIT")
        applied = _resolve_every_tick(h, tp.cid, 200, "h1nl")
        assert applied is not None, "a genuinely absent submit must stay resolvable (liveness)"
        assert h.clock() * 1000 - sent_ms >= h.engine.options.unknown_resolution_delay_s * 1000
        assert h.engine.options.unknown_resolution_delay_s >= 120
    finally:
        h.close()


def test_h1_a_ws_fill_of_an_audit_resolved_order_stays_visible_as_lag(tmp_path):
    """The fundamental residual: the active list never shows the landed order and the delay elapses, so the audit
    applies. A later WS fill for that CID is new evidence (never a 'settled replay'): it stays pending until
    history commits it, and a baseline rebase is refused meanwhile."""
    h = Harness(tmp_path, fx_kwargs={"active_lag_s": 100000})
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        tp = _unknown_tp(h, cell)
        assert _resolve_every_tick(h, tp.cid, 200, "h1res") is not None
        assert h.engine.leg_by_cid(tp.cid).state == OrderState.REJECTED_ZERO_FILL
        trade = h.fx.fill(tp.cid, D("10"), history_lag_s=900)
        h.tick(40)
        assert trade in h.engine.ws_pending and h.engine.history_lag_s() > 30, h.engine.ws_pending
        h.command(CommandKind.BASELINE_AUDIT, {"observed_position": str(h.fx.net_position), "note": "x"},
                  key="h1-audit")
        h.tick()
        assert _cmd(h, "h1-audit").status == CommandStatus.REJECTED, _cmd(h, "h1-audit").result
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ H2 engine side
def test_h2_launcher_resume_naming_a_stop_that_a_later_stop_reaffirmed_is_refused(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        h.fx.fill(h.live_order(h.buy_cells()[-1], ENTRY).cid, D("10"))
        h.tick(4)
        h.fx.script_submit(SubmitBehavior.TIMEOUT_NOT_LANDED)               # keeps the drain STOPPING
        h.command(CommandKind.STOP, key="h2-stop-1")
        h.tick()
        stop_ms = h.engine.meta.stop_requested_ms
        assert stop_ms is not None
        h.tick(2)
        h.command(CommandKind.STOP, key="h2-stop-2")                         # the operator's Hummingbot stop
        h.tick()
        assert _cmd(h, "h2-stop-2").status == CommandStatus.APPLIED
        resume = dict(start_payload(), source="launcher", resume_stop_ms=stop_ms)
        h.command(CommandKind.START, resume, key="h2-resume")
        h.tick()
        record = _cmd(h, "h2-resume")
        assert record.status == CommandStatus.REJECTED, record.result
        assert record.result["error"] == "DURABLE_STOP_ACTIVE"
        assert h.engine.meta.stop_requested_ms is not None
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ M1 conflict binding
def _conflict_on(h, cell, size=D("3"), wrong=D("1"), persistent=True):
    entry = h.live_order(cell, ENTRY)
    h.fx.fill(entry.cid, size)
    h.tick(4)
    t1 = h.engine.store.fills(entry.cid)[0].trade_id_str
    h.fx.inject_conflicting_trade(t1, wrong, persistent=persistent)
    h.tick(4)
    return entry, t1


def test_m1_snapshot_publishes_the_history_conflict_set(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        _conflict_on(h, cell)
        summary = _seen(h)
        assert isinstance(summary["conflict_set_id"], str) and len(summary["conflict_set_id"]) >= 16
        [entry] = [c for c in summary["history_conflicts"] if c["stream"] == "trades"]
        assert isinstance(entry["key"], str)
        sizes = {v["summary"]["size"]: v["committed"] for v in entry["versions"]}
        assert sizes == {"3": True, "1": False}, entry
        assert all(isinstance(v["fingerprint"], str) for v in entry["versions"])
        assert all(isinstance(x, str) for v in entry["versions"] for x in v["summary"].values())
    finally:
        h.close()


def test_m1_ack_is_bound_to_the_reviewed_conflict_set(tmp_path):
    """Probe P1: a contradiction that appears after the operator's view is never audited away by that view's ack."""
    h = Harness(tmp_path)
    try:
        _started(h)
        a, b = h.buy_cells()[-1], h.buy_cells()[-2]
        ea, eb = h.live_order(a, ENTRY), h.live_order(b, ENTRY)
        h.fx.fill(ea.cid, D("3"))
        h.fx.fill(eb.cid, D("10"))
        h.tick(6)
        h.fx.inject_conflicting_trade(h.engine.store.fills(ea.cid)[0].trade_id_str, D("1"), persistent=True)
        h.run_until(lambda: h.state == EngineState.FROZEN, max_ticks=10)
        reviewed = _seen(h)["conflict_set_id"]
        h.fx.order_by_cid(eb.cid).history_override = {"filled_base_amount": "7", "remaining_base_amount": "3"}
        h.tick(6)
        assert _seen(h)["conflict_set_id"] != reviewed
        assert _ack(h, "m1-none").result["error"] == "CONFLICT_SET_ID_REQUIRED"
        stale = _ack(h, "m1-stale", reviewed)
        assert stale.status == CommandStatus.REJECTED and stale.result["error"] == "CONFLICT_SET_CHANGED", stale.result
        assert h.engine.meta.audited_payloads == {} and h.state == EngineState.FROZEN
        h.tick(6)
        assert h.state == EngineState.FROZEN and not h.engine.history_complete
        current = _ack(h, "m1-current", _seen(h)["conflict_set_id"])
        assert current.status == CommandStatus.APPLIED, current.result
        streams = {e["stream"] for e in current.result["audited_payloads"]}
        assert streams == {"trades", "inactive_orders"}, current.result
    finally:
        h.close()


def test_m1_ack_with_nothing_to_audit_is_refused(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        record = _ack(h, "m1-nothing", _seen(h)["conflict_set_id"])
        assert record.status == CommandStatus.REJECTED and record.result["error"] == "NOTHING_TO_AUDIT", record.result
    finally:
        h.close()


def test_m1_never_committed_duplicate_needs_an_accepted_choice_then_history_completes(tmp_path):
    """CR-3(a): both versions arrive in the first walk, so no committed payload exists; the operator picks one."""
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, ENTRY)
        trade = h.fx.fill(entry.cid, D("3"))
        h.fx.inject_conflicting_trade(trade, D("1"), persistent=True)
        h.tick(6)
        assert not h.engine.history_complete and h.cell(cell).cycles[-1].E == D("0")
        [conflict] = [c for c in _seen(h)["history_conflicts"] if c["stream"] == "trades"]
        assert not any(v["committed"] for v in conflict["versions"])
        missing = _ack(h, "m1-nochoice", _seen(h)["conflict_set_id"])
        assert missing.result["error"] == "ACCEPTED_CHOICE_REQUIRED", missing.result
        right = next(v["fingerprint"] for v in conflict["versions"] if v["summary"]["size"] == "3")
        record = _ack(h, "m1-choice", _seen(h)["conflict_set_id"], accepted={conflict["key"]: right})
        assert record.status == CommandStatus.APPLIED, record.result
        h.tick(10)
        assert h.engine.history_complete, h.engine.history_incomplete_reason
        assert h.cell(cell).cycles[-1].E == D("3")
        h.command(CommandKind.STOP, key="m1-stop")
        h.run_until(lambda: h.engine.is_stopped, max_ticks=80)
        assert h.engine.meta.stop_outcome == "STOPPED_WITH_INVENTORY"
    finally:
        h.close()


def test_m1_flapping_key_converges_after_one_ack(tmp_path):
    """CR-3(b): the venue alternates two versions of a committed row across walks; one ack covers both."""
    h = Harness(tmp_path)
    try:
        _started(h)
        b = h.buy_cells()[-2]
        eb = h.live_order(b, ENTRY)
        h.fx.fill(eb.cid, D("10"))
        h.tick(6)
        order = h.fx.order_by_cid(eb.cid)
        for nonce in (901, 902):
            order.history_override = {"nonce": nonce}
            h.tick(4)
        assert h.state == EngineState.FROZEN
        record = _ack(h, "m1-flap", _seen(h)["conflict_set_id"])
        assert record.status == CommandStatus.APPLIED, record.result
        for nonce in (901, 902, 901, 902):
            order.history_override = {"nonce": nonce}
            h.tick(4)
            assert h.engine.history_complete and h.state != EngineState.FROZEN, (nonce, h.engine.reasons)
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ M2 transient conflict
def test_m2_transient_contradiction_keeps_the_cell_blocked_until_acknowledged(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        a = h.buy_cells()[-1]
        entry, _ = _conflict_on(h, a, persistent=False)                   # served once, then never again
        h.tick(6)
        h.fx.fill(entry.cid, D("2"))                                       # committed E reaches 5
        h.tick(10)
        assert h.cell(a).cycles[-1].E == D("5")
        assert not h.legs(a, TP), "a TP was built from an unaudited contradicted quantity"
        assert h.state == EngineState.FROZEN
        h.restart()                                                        # durable, not per-walk
        h.tick(10)
        assert not h.legs(a, TP)
        assert any(r.startswith(f"CELL_TP_BLOCKED:{a}:") for r in h.engine.reasons), h.engine.reasons
        record = _ack(h, "m2-ack", _seen(h)["conflict_set_id"])
        assert record.status == CommandStatus.APPLIED, record.result
        h.tick(6)
        assert h.live_order(a, TP) is not None and h.live_order(a, TP).requested == D("5")
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ M3 scoped startup
def test_m3_restart_with_a_cell_attributed_conflict_keeps_other_cells_exits_and_cancels(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        a = h.buy_cells()[-1]
        entry, _ = _conflict_on(h, a)
        h.fx.fill(entry.cid, D("2"))
        h.tick(6)
        h.restart()
        h.tick(6)
        other = h.sell_cells()[0]
        submits = len(h.fx.submits())
        h.fx.fill(h.live_order(other, ENTRY).cid, D("10"))
        h.tick(10)
        assert h.live_order(other, TP) is not None, (h.engine.tp_blockers, h.engine.cell_blockers)
        assert not h.legs(a, TP)
        new = [c for c in h.fx.submits()[submits:] if h.engine.order_meta[c.client_order_id].role == ENTRY.value]
        assert new == [] and h.engine.entry_blockers, "entries stay blocked globally"
        cancels = len(h.fx.cancels())
        h.fx.set_book(D("6.2"), D("6.3"))
        h.tick(8)
        assert len(h.fx.cancels()) > cancels, "outside-bounds cancels withheld by one cell's conflict"
    finally:
        h.close()


def test_m3_restart_with_a_recurring_history_refusal_keeps_unaffected_exits_and_cancels(tmp_path, monkeypatch):
    h = Harness(tmp_path)
    try:
        _started(h)
        x = h.buy_cells()[-1]
        h.fx.fill(h.live_order(x, ENTRY).cid, D("10"))
        h.tick(6)
        assert h.live_order(x, TP) is not None
        h.restart()
        original = h.engine._apply_history

        def refuse(result, now, in_progress):
            if result.new_trades or result.new_orders:
                raise StoreError("simulated: history batch refused")
            return original(result, now, in_progress)

        monkeypatch.setattr(h.engine, "_apply_history", refuse)
        other = h.sell_cells()[0]
        h.fx.fill(h.live_order(other, ENTRY).cid, D("10"))                   # its row recurs in every walk
        h.tick(10)
        assert "RECONCILING" not in h.engine.tp_blockers, h.engine.tp_blockers
        assert other in h.engine.tp_blocked_cells and x not in h.engine.tp_blocked_cells, h.engine.tp_blocked_cells
        cancels = len(h.fx.cancels())
        h.fx.set_book(D("6.2"), D("6.3"))
        h.tick(8)
        assert len(h.fx.cancels()) > cancels
    finally:
        h.close()


def test_m3_scoped_startup_keeps_risk_reducing_cancels_under_another_tp_blocker(tmp_path):
    h = Harness(tmp_path, options=EngineOptions(tick_interval_s=1.0, min_wake_interval_s=1.0, rules_refresh_s=1.0,
                                                weight_budget_per_min=100000))
    try:
        _started(h)
        _conflict_on(h, h.buy_cells()[-1])
        h.restart()
        h.tick(6)
        h.fx.set_rules(supports_limit=False)                               # a second, global TP blocker
        h.run_until(lambda: "MARKET_NOT_TRADABLE" in h.engine.tp_blockers, max_ticks=10)
        cancels = len(h.fx.cancels())
        h.fx.set_book(D("6.2"), D("6.3"))
        h.tick(8)
        assert len(h.fx.cancels()) > cancels
    finally:
        h.close()


def test_m3_unattributable_conflict_after_restart_keeps_the_global_block(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        manual = h.fx.manual_trade(Side.SELL, D("1"), D("4.2"))
        h.tick(6)
        h.fx.inject_conflicting_trade(manual, D("2"), persistent=True)
        h.tick(4)
        h.restart()
        h.tick(6)
        other = h.sell_cells()[0]
        h.fx.fill(h.live_order(other, ENTRY).cid, D("10"))
        h.tick(10)
        assert not h.legs(other, TP)
        assert any("UNATTRIBUTED" in b or b == "RECONCILING" for b in h.engine.tp_blockers), h.engine.tp_blockers
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ M4 baseline audit
@pytest.mark.parametrize("fails", [1000, 3])
def test_m4_audit_never_rebases_while_active_orders_are_unknown(tmp_path, fails):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.manual_trade(Side.BUY, D("7"), D("5.4"))
        h.tick(8)
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("4"), history_lag_s=30, ws=False)
        h.fx.fail_next("active_orders", fails)
        for i in range(45):
            key = f"m4a-{i}"
            h.command(CommandKind.BASELINE_AUDIT, {"observed_position": str(h.fx.net_position), "note": "x"},
                      key=key)
            h.tick()
            record = _cmd(h, key)
            if record.status == CommandStatus.APPLIED:
                assert record.result["new_effective_baseline"] == "7", record.result
                break
        else:
            assert fails == 1000
        assert h.engine.effective_baseline in (D("0"), D("7"))
    finally:
        h.close()


def test_m4_audit_rebases_only_after_history_delivers_the_fill_the_active_row_shows(tmp_path):
    """D2-18 pinned: loops past the settlement window, so removing only the 'active ahead' check fails."""
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.manual_trade(Side.BUY, D("7"), D("5.4"))
        h.tick(8)
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("4"), history_lag_s=30, ws=False)
        applied = None
        for i in range(60):
            key = f"m4b-{i}"
            h.command(CommandKind.BASELINE_AUDIT, {"observed_position": str(h.fx.net_position), "note": "x"},
                      key=key)
            h.tick()
            record = _cmd(h, key)
            if record.status == CommandStatus.APPLIED:
                applied = record.result
                break
        assert applied is not None and applied["new_effective_baseline"] == "7", applied
    finally:
        h.close()


def test_m4_audit_refused_when_a_manual_trade_was_committed_after_the_position_read(tmp_path):
    """D2-07 unmatched variant: removing only the history-row ordering check fails this test."""
    h = Harness(tmp_path)
    try:
        _started(h)
        h.fx.manual_trade(Side.BUY, D("7"), D("5.4"))
        h.tick(14)
        for _ in range(10):
            before = h.engine._last_account_poll_at
            h.tick()
            if h.engine._last_account_poll_at != before:
                break
        assert h.engine.position.net_base == D("7")
        h.fx.manual_trade(Side.BUY, D("2"), D("5.4"))
        h.tick()
        trades = [r for r in h.engine.unmatched if r.stream == "TRADES"]
        assert h.engine.position.net_base == D("7") and len(trades) == 2, h.engine.unmatched   # committed after it
        h.command(CommandKind.BASELINE_AUDIT, {"observed_position": "7", "note": "x"}, key="m4c")
        h.tick()
        record = _cmd(h, "m4c")
        assert record.status == CommandStatus.REJECTED, record.result
        assert "history row" in json.dumps(record.result), record.result
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ L1 (E-09)
@pytest.mark.parametrize("source", ["operator", "launcher"])
def test_l1_start_before_bootstrap_rebinds_a_changed_config(tmp_path, source):
    h = Harness(tmp_path)
    try:
        h.command(CommandKind.START, dict(start_payload(), source=source), key="l1-start")
        h.tick()
        assert _cmd(h, "l1-start").status == CommandStatus.APPLIED
        first = h.engine.meta.start_config_fingerprint
        h.config = make_config(order_amount_base=D("20"), max_abs_net_position=D("2000"))
        h.restart()
        h.command(CommandKind.START, dict(start_payload(), source=source), key="l1-restart")
        h.tick()
        record = _cmd(h, "l1-restart")
        assert record.status == CommandStatus.APPLIED and record.result.get("rebound") is True, record.result
        assert h.engine.meta.start_config_fingerprint == h.engine.full_fingerprint != first
        audit = h.engine.store.audit_events(kind="start")[0]
        assert audit.payload.get("rebound_from") == first
        h.run_until(lambda: h.engine.bootstrap_ready(h.clock())[0], max_ticks=60)
        h.command(CommandKind.CONFIRM_BASELINE, {"expected_initial_position": "0"}, key="l1-confirm")
        h.tick()
        assert _cmd(h, "l1-confirm").status == CommandStatus.APPLIED, _cmd(h, "l1-confirm").result
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ M1 web parity
def _web_ack(summary, accepted, set_id=None):
    """The ack exactly as WS-E builds, normalizes and gates it (real web code: test_ngweb_conflicts.ack_payload)."""
    from web.neutral_grid.commands import CommandService
    set_id = summary["conflict_set_id"] if set_id is None else set_id
    body = {"action": "ack_history_conflict", "note": "сверено по истории биржи", "acknowledge": True,
            "conflict_set_id": set_id, "accepted": accepted, "confirmation": f"ПРИНЯТЬ НАБОР {set_id}"}
    service = CommandService.__new__(CommandService)
    normalized = service._normalize_only("baseline_audit", body)
    assert normalized == body, normalized
    return normalized, service._conflict_set_blocker(normalized, {"summary": summary})


def test_m1_web_parity_keys_fingerprints_and_the_normalized_ack_apply(tmp_path):
    from web.neutral_grid.commands import _OPAQUE_RE
    h = Harness(tmp_path)
    try:
        _started(h)
        a, b = h.buy_cells()[-1], h.buy_cells()[-2]
        ea, eb = h.live_order(a, ENTRY), h.live_order(b, ENTRY)
        trade = h.fx.fill(ea.cid, D("3"))                                  # never committed: two versions at once
        h.fx.fill(eb.cid, D("10"))
        h.fx.inject_conflicting_trade(trade, D("1"), persistent=True)
        h.tick(6)
        h.fx.order_by_cid(eb.cid).history_override = {"nonce": 901}        # committed row contradicted
        h.tick(6)
        summary = _seen(h)
        conflicts = summary["history_conflicts"]
        assert _OPAQUE_RE.fullmatch(summary["conflict_set_id"])
        for c in conflicts:
            assert _OPAQUE_RE.fullmatch(c["key"]), c["key"]
            assert all(_OPAQUE_RE.fullmatch(v["fingerprint"]) for v in c["versions"]), c
        assert len({c["key"] for c in conflicts}) == len(conflicts)
        keys = {c["key"] for c in conflicts}
        assert f"trade:{trade}:BUY:{h.fx.order_by_cid(ea.cid).order_index}" in keys, keys      # canonical form
        assert f"order:{h.fx.order_by_cid(eb.cid).order_index}" in keys, keys
        pick = {c["key"]: next(v["fingerprint"] for v in c["versions"] if v["summary"]["size"] == "3")
                for c in conflicts if not any(v["committed"] for v in c["versions"])}   # the web's rule
        assert len(pick) == 1
        normalized, blocker = _web_ack(summary, pick)
        assert blocker is None, blocker.body
        h.command(CommandKind.BASELINE_AUDIT, normalized, key="parity-ack")
        h.tick()
        record = _cmd(h, "parity-ack")
        assert record.status == CommandStatus.APPLIED, record.result
        h.tick(10)
        assert h.engine.history_complete, h.engine.history_incomplete_reason
        assert h.cell(a).cycles[-1].E == D("3") and h.state != EngineState.FROZEN
    finally:
        h.close()


def test_m1_web_parity_changed_set_and_empty_set_are_refused_by_the_engine(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        normalized, blocker = _web_ack(_seen(h), {})                       # nothing to audit: the web lets it through
        assert blocker is None and _seen(h)["history_conflicts"] == []
        h.command(CommandKind.BASELINE_AUDIT, normalized, key="parity-empty")
        h.tick()
        assert _cmd(h, "parity-empty").result["error"] == "NOTHING_TO_AUDIT"
        a = h.buy_cells()[-1]
        _conflict_on(h, a)
        h.run_until(lambda: h.state == EngineState.FROZEN, max_ticks=10)
        viewed = _seen(h)
        normalized, blocker = _web_ack(viewed, {})
        assert blocker is None
        b = h.buy_cells()[-2]
        eb = h.live_order(b, ENTRY)
        h.fx.fill(eb.cid, D("10"))
        h.tick(6)
        h.fx.order_by_cid(eb.cid).history_override = {"nonce": 902}         # appears after the operator's view
        h.tick(6)
        h.command(CommandKind.BASELINE_AUDIT, normalized, key="parity-stale")   # enqueued before the web re-read
        h.tick()
        record = _cmd(h, "parity-stale")
        assert record.status == CommandStatus.REJECTED and record.result["error"] == "CONFLICT_SET_CHANGED"
        assert h.engine.meta.audited_payloads == {} and h.state == EngineState.FROZEN
    finally:
        h.close()
