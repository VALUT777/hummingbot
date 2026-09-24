"""Orchestrator round 3 (fix verification + critics): engine-level regressions on FakeExchange + real SQLite.

Fix tests fail on the package-2 tip (red evidence in docs/neutral-grid/trace/ws-d-engine.md); test-gap items
(D1-11, D1-17, D2-07) are proven by mutation. Executor / launcher items live in test/controllers/generic.
"""
import asyncio
import json
import logging
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

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
from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import FakeExchange, SubmitBehavior
from hummingbot.strategy_v2.executors.neutral_grid_executor.snapshot import format_status
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import StoreError

D = Decimal
ENTRY, TP = LegRole.ENTRY, LegRole.TP
TOKEN = "SeCrEtToKeN0123456789abcdefXYZ"
LEAK = ("200, message='Attempt to decode JSON with unexpected mimetype: application/octet-stream', "
        f"url='https://mainnet.zklighter.elliot.ai/api/v1/accountActiveOrders?account_index=4242&market_id=5&"
        f"auth={TOKEN}'")
_FAST_RULES = EngineOptions(tick_interval_s=1.0, min_wake_interval_s=1.0, rules_refresh_s=1.0,
                            weight_budget_per_min=100000)


def _started(h, baseline=D("0")):
    h.bootstrap(baseline)
    h.tick(2)


def _cmd(h, key):
    return h.engine.store.get_command(idempotency_key=key)


def _tick_until_poll(h, max_ticks=10):
    for _ in range(max_ticks):
        h.tick()
        if h.engine._last_account_poll_at == h.clock() - 1.0:
            return
    raise AssertionError("no account poll")


def _db_bytes(h) -> bytes:
    return b"".join(p.read_bytes() for p in h.db_path.parent.glob(h.db_path.name + "*"))


# ------------------------------------------------------------------------------------------ C1 secret redaction
def test_c1_auth_token_never_reaches_snapshot_errors_status_logs_or_the_ledger(tmp_path, caplog):
    caplog.set_level(logging.DEBUG)
    h = Harness(tmp_path)
    try:
        _started(h)
        original_active, original_submit = h.fx.active_orders, h.fx.submit

        async def failing_active():
            raise RuntimeError(LEAK)

        async def failing_submit(req):
            raise RuntimeError(LEAK)

        h.fx.active_orders = failing_active
        h.fx.submit = failing_submit
        h.fx.fill(h.live_order(h.buy_cells()[-1], ENTRY).cid, D("10"))     # a TP transport will raise too
        h.tick(12)
        h.fx.active_orders, h.fx.submit = original_active, original_submit
        h.tick(2)
        assert any(e["code"] == "ACTIVE_ORDERS_UNAVAILABLE" for e in h.engine.errors)   # visible, redacted
        texts = [json.dumps(h.engine.last_snapshot, default=str), json.dumps(list(h.engine.errors), default=str),
                 format_status(h.engine.last_snapshot, now=h.clock()), caplog.text,
                 json.dumps(h.engine.health(), default=str)]
        assert all(TOKEN not in t for t in texts)
        assert TOKEN.encode() not in _db_bytes(h)
    finally:
        h.close()


def test_c1_lighter_port_wraps_non_history_errors_with_type_only_detail():
    from hummingbot.strategy_v2.executors.neutral_grid_executor.lighter_port import LighterExchangePort
    connector = MagicMock()
    for name in ("fetch_active_orders", "fetch_account_position", "fetch_trades_page", "fetch_inactive_orders_page",
                 "_update_trading_rules"):
        setattr(connector, name, AsyncMock(side_effect=RuntimeError(LEAK)))
    port = LighterExchangePort(connector, "LIT-USDG")
    loop = asyncio.new_event_loop()
    try:
        for make in (port.active_orders, port.position, port.trading_rules,
                     lambda: port.trades_page(None), lambda: port.inactive_orders_page(None)):
            with pytest.raises(Exception) as caught:
                loop.run_until_complete(make())
            assert TOKEN not in str(caught.value) and caught.value.__cause__ is None
            assert "RuntimeError" in str(caught.value)
    finally:
        loop.close()


# ------------------------------------------------------------------------------------------ C2 withheld conflicts
def test_c2_scanner_withheld_conflict_blocks_the_contradicted_cells_tp(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, ENTRY)
        h.fx.fill(entry.cid, D("3"))
        h.tick(4)
        assert h.cell(cell).cycles[-1].E == D("3") and not h.legs(cell, TP)      # held below the 5 LIT floor
        t1 = h.engine.store.fills(entry.cid)[0].trade_id_str
        h.fx.inject_conflicting_trade(t1, D("1"), persistent=True)    # the venue contradicts the committed fill
        h.tick(4)
        assert h.state == EngineState.FROZEN
        h.fx.fill(entry.cid, D("2"))                                  # committed E would reach 5
        h.tick(10)
        assert not h.legs(cell, TP), "a TP was built from a contradicted quantity"
        assert "HISTORY_CONFLICT" in (h.engine.cell_blockers.get(cell) or ""), h.engine.cell_blockers
        other = h.sell_cells()[0]                                     # an exact, unrelated cell keeps its exits
        h.fx.fill(h.live_order(other, ENTRY).cid, D("10"))
        h.tick(10)
        assert h.live_order(other, TP) is not None
    finally:
        h.close()


def test_c2_unattributable_withheld_conflict_blocks_every_new_tp_fail_closed(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        manual = h.fx.manual_trade(Side.SELL, D("1"), D("4.2"))       # not ours: cannot be mapped to a cell
        h.tick(6)
        h.fx.inject_conflicting_trade(manual, D("2"), persistent=True)
        h.tick(4)
        other = h.sell_cells()[0]
        h.fx.fill(h.live_order(other, ENTRY).cid, D("10"))
        h.tick(10)
        assert not h.legs(other, TP)
        assert any("HISTORY_CONFLICT_UNATTRIBUTED" in r for r in h.engine.reasons), h.engine.reasons
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ C3 audited payload
def test_c3_audited_payload_conflict_restores_completeness_and_stop_finishes_honestly(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, ENTRY)
        h.fx.fill(entry.cid, D("3"))
        h.tick(4)
        t1 = h.engine.store.fills(entry.cid)[0].trade_id_str
        h.fx.inject_conflicting_trade(t1, D("1"), persistent=True)
        h.tick(6)
        assert not h.engine.history_complete and h.state == EngineState.FROZEN
        h.command(CommandKind.BASELINE_AUDIT, {"action": "ack_history_conflict",
                                               "note": "venue export: the committed size 3 is right"}, key="c3-ack")
        h.tick(8)
        assert _cmd(h, "c3-ack").status == CommandStatus.APPLIED
        audit = h.engine.store.audit_events(kind="manual_reconcile")[0]
        assert audit.payload["evidence"].get("audited_payloads")               # durable, in the same tx
        assert h.engine.history_complete and h.state != EngineState.FROZEN, h.engine.reasons
        h.restart()                                                            # the audit survives a restart
        h.tick(6)
        assert h.engine.history_complete, h.engine.history_incomplete_reason
        h.command(CommandKind.STOP, key="c3-stop")
        h.run_until(lambda: h.engine.is_stopped, max_ticks=60)
        assert h.engine.meta.stop_outcome == "STOPPED_WITH_INVENTORY"
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ C5 phantom trade lag
def test_c5_trade_signal_that_is_never_committed_expires_after_a_settled_covering_walk(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        h.engine.wake({"type": "trade", "trade_id": "987654321987", "client_order_id": None})   # not this market
        h.tick(30)
        assert h.engine.history_lag_s() == 0.0, h.engine.ws_pending
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ D1-02 / D2-04 / D1-16
def test_d102_recurring_active_row_contradiction_is_isolated_and_frozen_is_committed(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        a = h.buy_cells()[0]
        ea = h.live_order(a, ENTRY)
        h.fx.order_by_cid(ea.cid).active_override = {"initial_base_amount": "11"}   # every poll contradicts leg
        h.tick(8)
        assert h.engine.store.engine().engine_state == EngineState.FROZEN            # committed, not only memory
        assert h.engine.last_snapshot["engine_state"] == "FROZEN"
        assert "ACTIVE_EVIDENCE" in (h.engine.cell_blockers.get(a) or h.engine.tp_blocked_cells.get(a) or "")
        other = h.sell_cells()[0]
        h.fx.fill(h.live_order(other, ENTRY).cid, D("10"))
        h.tick(8)
        assert h.live_order(other, TP) is not None                                  # exits keep flowing
        c0 = len(h.fx.cancels())
        h.fx.set_book(D("6.2"), D("6.3"))
        h.tick(6)
        assert len(h.fx.cancels()) > c0                                             # risk-reducing cancels too
    finally:
        h.close()


def test_d204_any_recurring_store_refusal_still_commits_frozen_and_keeps_risk_reducing_work(tmp_path, monkeypatch):
    h = Harness(tmp_path)
    try:
        _started(h)

        def refuse(now):
            raise StoreError("simulated recurring refusal")

        monkeypatch.setattr(h.engine, "_detect_drift", refuse)
        h.tick(4)
        assert h.engine.store.engine().engine_state == EngineState.FROZEN
        assert h.engine.last_snapshot["engine_state"] == "FROZEN"
        other = h.sell_cells()[0]
        h.fx.fill(h.live_order(other, ENTRY).cid, D("10"))
        h.tick(8)
        assert h.live_order(other, TP) is not None
        c0 = len(h.fx.cancels())
        h.fx.set_book(D("6.2"), D("6.3"))
        h.tick(6)
        assert len(h.fx.cancels()) > c0
    finally:
        h.close()


def test_d116_invariant_freeze_survives_an_immediate_crash(tmp_path, monkeypatch):
    h = Harness(tmp_path)
    try:
        _started(h)
        original = h.engine._reconcile_active

        def refuse_once(now):
            monkeypatch.setattr(h.engine, "_reconcile_active", original)
            raise StoreError("simulated store refusal")

        monkeypatch.setattr(h.engine, "_reconcile_active", refuse_once)
        h.tick()
        h.crash_restart()                                    # dies before any further tick
        h.tick(2)
        assert "LEDGER_INVARIANT" in h.engine.meta.freezes and h.state == EngineState.FROZEN
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ D1-11 test gap
def test_d111_committed_unsent_cancel_is_resumed_on_the_normal_path(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        h.fx.set_book(D("6.2"), D("6.3"))                   # outside bounds: entry cancels
        h.hooks.arm("after_cancel_intent_commit")
        assert h.tick_crashing(1)
        pending = [x for x in h.engine.non_final_legs() if x.state == OrderState.CANCEL_PENDING]
        assert len(pending) == 1
        h.tick(12)
        assert not h.fx.order_by_cid(pending[0].cid).is_open
    finally:
        h.close()


def test_d111_committed_unsent_cancel_is_resumed_while_tps_are_blocked(tmp_path):
    h = Harness(tmp_path, options=_FAST_RULES)
    try:
        _started(h)
        h.fx.set_book(D("6.2"), D("6.3"))
        h.hooks.arm("after_cancel_intent_commit")
        assert h.tick_crashing(1)
        pending = [x for x in h.engine.non_final_legs() if x.state == OrderState.CANCEL_PENDING]
        assert len(pending) == 1
        h.fx.set_rules(supports_limit=False, supports_post_only=False)   # MARKET_NOT_TRADABLE blocks TPs
        h.tick(12)
        assert "MARKET_NOT_TRADABLE" in h.engine.tp_blockers
        assert not h.fx.order_by_cid(pending[0].cid).is_open
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ D1-17 test gap
class _RacyFx(FakeExchange):
    pending_fill = None

    async def inactive_orders_page(self, cursor, limit=100):
        if self.pending_fill is not None and cursor is None:
            cid, qty = self.pending_fill
            self.pending_fill = None
            self.fill(cid, qty)
        return await super().inactive_orders_page(cursor, limit)


def _release_run(tmp_path, inject_at=None, variant=None):
    h = Harness(tmp_path)
    h.fx.remove_ws_listener(h.engine.wake)
    h.engine.store.close()
    h.fx = _RacyFx(h.clock)
    h.engine = h.open()
    try:
        _started(h)
        a, b = h.buy_cells()[-1], h.buy_cells()[0]
        h.fx.fill(h.live_order(a, ENTRY).cid, D("10"))
        h.run_until(lambda: h.live_order(a, TP) is not None, max_ticks=10)
        h.fx.fill(h.live_order(a, TP).cid, D("10"))
        gen = h.cell(a).generation
        for i in range(40):
            if i == inject_at:
                if variant == "weight":
                    h.fx.pending_fill = (h.live_order(b, ENTRY).cid, D("10"))   # P moves in this tick ...
                    h.fx.weights["account"] = 10 ** 9                      # ... and no re-read fits the budget
                elif variant == "manual":
                    h.fx.manual_trade(Side.BUY, D("1"), D("4.2"))
                h.engine.wake({"type": "order", "status": "filled"})
            h.tick()
            if i == inject_at and variant == "weight":
                h.fx.weights["account"] = 300
            if h.cell(a).current is None or h.cell(a).generation != gen:
                return i
        return None
    finally:
        h.close()


@pytest.mark.parametrize("variant", ["weight", "manual"])
def test_d117_release_never_uses_a_stale_position_flag(tmp_path, variant):
    release_tick = _release_run(tmp_path / "probe")
    assert release_tick is not None
    tick = _release_run(tmp_path / variant, inject_at=release_tick, variant=variant)
    assert tick is None or tick > release_tick, (variant, release_tick, tick)


# ------------------------------------------------------------------------------------------ D2-07 test gap
def test_d207_audit_refused_when_the_position_read_predates_a_committed_fill(tmp_path):
    h = Harness(tmp_path, settlement_delay_s=D("0"))
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, ENTRY)
        _tick_until_poll(h)
        h.fx.fill(entry.cid, D("4"))                        # committed before the next poll
        h.tick()
        assert h.cell(cell).cycles[-1].E == D("4")
        h.command(CommandKind.BASELINE_AUDIT, {"observed_position": str(h.engine.position.net_base), "note": "x"},
                  key="d207")
        h.tick()
        record = _cmd(h, "d207")
        assert record.status == CommandStatus.REJECTED, record.result
        assert "predates" in json.dumps(record.result)
        assert h.engine.effective_baseline == D("0")
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ D2-15 / D2-18
def test_d215_resolve_unknown_submit_never_accepts_while_the_order_is_live(tmp_path):
    h = Harness(tmp_path, fx_kwargs={"active_lag_s": 7})
    try:
        _started(h)
        h.tick(8)
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))
        h.fx.script_submit(SubmitBehavior.TIMEOUT_LANDED)
        h.run_until(lambda: any(t.state == OrderState.SUBMIT_UNKNOWN for t in h.legs(cell, TP)), max_ticks=10)
        tp = next(t for t in h.legs(cell, TP) if t.state == OrderState.SUBMIT_UNKNOWN)
        for i in range(25):                                  # the operator retries every tick
            h.command(CommandKind.BASELINE_AUDIT, {"action": "resolve_unknown_submit", "cid": str(tp.cid),
                                                   "note": "x"}, key=f"d215-{i}")
            h.tick()
            assert _cmd(h, f"d215-{i}").status != CommandStatus.APPLIED, i
        assert h.fx.order_by_cid(tp.cid).is_open and len(h.legs(cell, TP)) == 1
    finally:
        h.close()


def test_d218_audit_refused_while_an_active_row_shows_executions_history_lacks(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.manual_trade(Side.BUY, D("7"), D("5.4"))
        h.tick(8)
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("4"), history_lag_s=30, ws=False)   # no WS hint at all
        for i in range(12):
            h.command(CommandKind.BASELINE_AUDIT, {"observed_position": str(h.fx.net_position), "note": "x"},
                      key=f"d218-{i}")
            h.tick()
            assert _cmd(h, f"d218-{i}").status != CommandStatus.APPLIED, _cmd(h, f"d218-{i}").result
        assert h.engine.effective_baseline == D("0")
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ E-09 / E-03
def test_e09_confirm_baseline_refuses_a_config_changed_after_start(tmp_path):
    h = Harness(tmp_path)
    try:
        h.command(CommandKind.START, start_payload(), key="e09-start")
        h.tick()
        assert _cmd(h, "e09-start").status == CommandStatus.APPLIED
        h.config = make_config(order_amount_base=D("20"), max_abs_net_position=D("2000"))   # before bootstrap
        h.restart()
        h.run_until(lambda: h.engine.bootstrap_ready(h.clock())[0], max_ticks=60)
        h.command(CommandKind.CONFIRM_BASELINE, {"expected_initial_position": "0"}, key="e09-confirm")
        h.tick()
        assert _cmd(h, "e09-confirm").status == CommandStatus.REJECTED, _cmd(h, "e09-confirm").result
        assert not h.engine.bootstrapped and h.engine.meta.started is False and h.fx.submits() == []
        h.command(CommandKind.START, start_payload(), key="e09-start-2")          # acknowledge the new config
        h.tick()
        h.run_until(lambda: h.engine.bootstrap_ready(h.clock())[0], max_ticks=60)
        h.command(CommandKind.CONFIRM_BASELINE, {"expected_initial_position": "0"}, key="e09-confirm-2")
        h.tick()
        assert _cmd(h, "e09-confirm-2").status == CommandStatus.APPLIED
    finally:
        h.close()


def test_e03_health_sidecar_heartbeat_even_when_healthy(tmp_path):
    h = Harness(tmp_path)
    try:
        path = h.db_path.parent / (h.db_path.name + ".health.json")
        h.tick()
        first = float(json.loads(path.read_text())["at"])
        h.tick(40)
        later = json.loads(path.read_text())
        assert later["persistence_error"] is None and float(later["at"]) > first + 20   # bounded-interval heartbeat
        assert float(later["at"]) >= h.clock() - 1.0 - h.engine.options.health_heartbeat_s
    finally:
        h.close()
