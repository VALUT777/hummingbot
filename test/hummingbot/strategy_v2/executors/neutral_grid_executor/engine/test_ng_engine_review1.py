"""Orchestrator review package 1 (engine @ bcd2eebef): one regression test per confirmed finding + web contract W1-W4.

Every test drives the real engine through ``FakeExchange`` + the real SQLite store. Each one failed on the reviewed
engine (red evidence in docs/neutral-grid/trace/ws-d-engine.md) and passes with the fix. Test-gap items (#14, #15)
are proven by a mutation instead.
"""
import json
from decimal import Decimal

import pytest
from ng_engine_harness import PREVIEW_ID, Harness, make_config, start_payload

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CommandKind,
    CommandStatus,
    EngineState,
    LegRole,
    OrderState,
    Side,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import EngineOptions
from hummingbot.strategy_v2.executors.neutral_grid_executor.engine import NeutralGridEngine, open_engine_store
from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import (
    FakeExchange,
    FakeTradeLeg,
    SubmitBehavior,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import StoreError

D = Decimal
ENTRY, TP = LegRole.ENTRY, LegRole.TP


def _started(h, baseline=D("0")):
    h.bootstrap(baseline)
    h.tick(2)


def _role(h, cid):
    leg = h.engine.leg_by_cid(cid)
    return None if leg is None else leg.identity.role


def _entry_submits(h):
    return [c for c in h.fx.submits() if _role(h, c.client_order_id) == ENTRY]


def _cmd(h, key):
    return h.engine.store.get_command(idempotency_key=key)


# ------------------------------------------------------------------------------------------ #1 TP dispatch SLO
def test_r01_every_tp_intent_commits_within_slo_although_each_transport_takes_time(tmp_path):
    h = Harness(tmp_path, cell_count=30, fx_kwargs={"transport_delay_s": 0.4})
    try:
        _started(h)
        h.tick(3)
        buys = [c for c in h.buy_cells() if h.live_order(c, ENTRY) is not None]
        assert len(buys) >= 8
        for c in buys:
            h.fx.fill(h.live_order(c, ENTRY).cid, D("10"))
        h.tick()
        store = h.engine.store
        received = {r.id: r.received_at_ms for r in store.inbox(limit=100000)}
        fill_commit = {f.cid: received[f.inbox_id] for f in store.fills()}
        delays = []
        for c in buys:
            entry = h.legs(c, ENTRY)[0]
            tps = h.legs(c, TP)
            assert tps, f"cell {c} got no TP intent in the fill tick"
            delays.append(store.leg(tps[0].cid).created_at_ms - fill_commit[entry.cid])
        assert max(delays) <= 2000, delays                     # durable TP intent <= 2 s after the fill commit
        latencies = [lat for _, lat in h.engine.tp_latencies]
        assert len(latencies) == len(buys) and max(latencies) <= 2.0
        assert sorted(round(d / 1000, 3) for d in delays) == sorted(round(x, 3) for x in latencies)   # measured
    finally:
        h.close()


def test_r01_tp_transports_are_sent_before_entry_cancels(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        buys = sorted(c for c in h.buy_cells() if h.live_order(c, ENTRY) is not None)[-3:]   # TPs cross no entry
        for c in buys:
            h.fx.fill(h.live_order(c, ENTRY).cid, D("10"))
        h.fx.set_book(D("6.2"), D("6.3"))                      # outside bounds: remaining entries are cancelled
        n = len(h.fx.calls)
        h.tick()
        calls = [c for c in h.fx.calls[n:] if c.kind in ("submit", "cancel")]
        tp_idx = [i for i, c in enumerate(calls) if c.kind == "submit" and _role(h, c.client_order_id) == TP]
        cancel_idx = [i for i, c in enumerate(calls) if c.kind == "cancel"]
        assert len(tp_idx) == 3 and cancel_idx
        assert max(tp_idx) < min(cancel_idx), [(c.kind, _role(h, c.client_order_id)) for c in calls]
    finally:
        h.close()


def test_r01_slo_clock_starts_when_the_obligation_becomes_dispatchable(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, ENTRY)
        h.fx.fill(entry.cid, D("2"))                           # below the 5 LIT minimum: nothing dispatchable
        h.tick(60)
        assert not h.legs(cell, TP)
        h.fx.fill(entry.cid, D("3"))                           # 5 LIT: dispatchable now
        h.tick(2)
        assert h.legs(cell, TP)
        assert h.engine.tp_latencies[-1][1] <= 2.0, list(h.engine.tp_latencies)
        assert D(h.engine.last_snapshot["summary"]["tp_dispatch"]["last_latency_s"]) <= 2
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ #2 scoped freezes
def _overfill_trade(h, cid, price):
    """A venue row for this CID and its real order that exceeds the order amount: a store OVERFILL conflict."""
    tid = h.fx._new_trade_id()
    h.fx.trade_legs.append(FakeTradeLeg(
        trade_id=tid, own_side=Side.BUY, order_index=h.fx.order_by_cid(cid).order_index, client_order_id=cid,
        size=D("11"), price=price, is_maker=True, timestamp_ms=h.fx._now_ms(), visible_at=h.clock(),
        raw={"trade_id": tid, "size": "11", "price": str(price)}, account_index=h.fx.account_index))


def test_r02_one_cells_history_conflict_does_not_withhold_other_cells_tps_or_risk_cancels(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        a, b = h.buy_cells()[0], h.buy_cells()[-1]
        ea, eb = h.live_order(a, ENTRY), h.live_order(b, ENTRY)
        _overfill_trade(h, ea.cid, ea.price)                       # contradicts A's order: conflict on A only
        h.tick(10)
        assert any(c.cid == ea.cid for c in h.engine.open_conflicts)
        assert h.state == EngineState.FROZEN
        entries = len(_entry_submits(h))
        h.fx.fill(eb.cid, D("10"))                                 # unrelated, exact history
        h.tick(10)
        assert h.live_order(b, TP) is not None, (h.engine.tp_blockers, h.engine.cell_blockers)
        assert "HISTORY_CONFLICT" in (h.engine.cell_blockers.get(a) or "")
        assert len(_entry_submits(h)) == entries                   # new entries stay blocked globally
        c0 = len(h.fx.cancels())
        h.fx.set_book(D("6.2"), D("6.3"))                          # book leaves the range
        h.tick(6)
        assert len(h.fx.cancels()) > c0                            # risk-reducing entry cancels still flow
    finally:
        h.close()


def test_r02_ledger_invariant_freeze_keeps_tps_of_exact_cells_and_outside_bounds_cancels(tmp_path, monkeypatch):
    h = Harness(tmp_path)
    try:
        _started(h)
        b = h.buy_cells()[-1]
        eb = h.live_order(b, ENTRY)
        original = h.engine._reconcile_active

        def refuse_once(now):
            monkeypatch.setattr(h.engine, "_reconcile_active", original)
            raise StoreError("simulated store refusal")

        monkeypatch.setattr(h.engine, "_reconcile_active", refuse_once)
        h.tick()
        assert "LEDGER_INVARIANT" in h.engine.meta.freezes
        h.fx.fill(eb.cid, D("10"))
        h.tick(10)
        assert h.state == EngineState.FROZEN
        assert h.live_order(b, TP) is not None, (h.engine.tp_blockers, h.engine.reasons)
        c0 = len(h.fx.cancels())
        h.fx.set_book(D("6.2"), D("6.3"))
        h.tick(6)
        assert len(h.fx.cancels()) > c0
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ #3 / #17 drift race
class _RacyFx(FakeExchange):
    """``pending_fill`` executes right before the next inactive-orders page is served, i.e. AFTER the engine read
    the account (position + active orders) in the same tick: REST reads are not atomic with each other."""
    pending_fill = None
    polled = False

    async def active_orders(self):
        self.polled = True
        return await super().active_orders()

    async def inactive_orders_page(self, cursor, limit=100):
        if self.pending_fill is not None and cursor is None and self.polled:
            cid, qty = self.pending_fill
            self.pending_fill = None
            self.fill(cid, qty)
        self.polled = False
        return await super().inactive_orders_page(cursor, limit)


def _racy(tmp_path, **config):
    h = Harness(tmp_path, **config)
    h.fx.remove_ws_listener(h.engine.wake)
    h.engine.store.close()
    h.fx = _RacyFx(h.clock, **({"initial_position": config.get("expected_initial_position", D("0"))}))
    h.engine = h.open()
    return h


def test_r03_position_read_before_the_same_ticks_history_commit_is_not_drift(tmp_path):
    h = _racy(tmp_path, expected_initial_position=D("25"), max_abs_net_position=D("30"))
    try:
        _started(h, D("25"))
        h.tick(3)
        sells = [c for c in h.sell_cells() if h.live_order(c, ENTRY) is not None]
        assert sells
        h.fx.pending_fill = (h.live_order(sells[0], ENTRY).cid, D("10"))
        h.tick(8)
        assert h.fx.pending_fill is None
        assert h.fx.net_position == h.engine.endpoints.P == D("15")
        assert not h.engine.b_engine.manual_reconcile_required, h.engine.b_engine.manual_reconcile_reason
        assert h.state != EngineState.RISK_BLOCKED, h.engine.reasons
    finally:
        h.close()


def _release_scenario(tmp_path, inject_at=None):
    """Cell A completes a cycle; returns the tick index at which A released. With ``inject_at``, another cell's
    entry fills right before that tick, so the release tick also commits a fill that moves the ledger P while the
    last position read predates it."""
    h = Harness(tmp_path)
    try:
        _started(h)
        a, b = h.buy_cells()[-1], h.buy_cells()[0]
        h.fx.fill(h.live_order(a, ENTRY).cid, D("10"))
        h.run_until(lambda: h.live_order(a, TP) is not None, max_ticks=10)
        h.fx.fill(h.live_order(a, TP).cid, D("10"))
        gen = h.cell(a).generation
        for i in range(40):
            if i == inject_at:
                h.fx.fill(h.live_order(b, ENTRY).cid, D("10"))
            h.tick()
            if h.cell(a).current is None or h.cell(a).generation != gen:
                return i, h.engine.position is not None and h.engine.position.net_base
        return None, None
    finally:
        h.close()


def test_r17_release_needs_a_position_read_after_the_closing_fill_commit(tmp_path):
    release_tick, _ = _release_scenario(tmp_path / "probe")
    assert release_tick is not None
    # Same deterministic run, but another cell's fill is committed in exactly the release tick: the last
    # position read (0) no longer equals the ledger P (10), so condition (5) is not proven in that tick.
    tick, position = _release_scenario(tmp_path / "race", inject_at=release_tick)
    assert tick is None or tick > release_tick, (release_tick, tick)


def _tick_until_poll(h, max_ticks=10):
    """Tick until an account poll (position + active list) happened in the last tick."""
    for _ in range(max_ticks):
        h.tick()
        if h.engine._last_account_poll_at == h.clock() - 1.0:
            return
    raise AssertionError("no account poll")


# ------------------------------------------------------------------------------------------ #4 rules refresh
def test_r04_failing_rules_refresh_backs_off_and_does_not_starve_history(tmp_path):
    h = Harness(tmp_path, options=EngineOptions(tick_interval_s=1.0, min_wake_interval_s=1.0, rules_refresh_s=5.0))
    try:
        _started(h)
        h.fx.fail_next("order_book_details", 100000)
        before_rules = sum(1 for _, ep, _ in h.fx.weight_log if ep == "order_book_details")
        before_trades = sum(1 for _, ep, _ in h.fx.weight_log if ep == "trades")
        h.tick(120)
        rules_calls = sum(1 for _, ep, _ in h.fx.weight_log if ep == "order_book_details") - before_rules
        trade_pages = sum(1 for _, ep, _ in h.fx.weight_log if ep == "trades") - before_trades
        assert rules_calls <= 12, rules_calls                  # exponential backoff, not every tick
        assert trade_pages >= 20, trade_pages                  # history keeps its cadence
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ #5 minimum decrease
def test_r05_runtime_minimum_decrease_never_emergency_cancels_other_cells_entries(tmp_path):
    h = Harness(tmp_path, max_active_orders=30,
                options=EngineOptions(tick_interval_s=1.0, min_wake_interval_s=1.0, rules_refresh_s=1.0))
    try:
        _started(h)
        h.tick(3)
        h.fx.set_rules(min_base=D("1"), min_notional=D("1"))     # venue lowers the minimum; cap unchanged
        h.tick(3)
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, ENTRY)
        for _ in range(4):
            h.fx.fill(entry.cid, D("1"))
            h.tick(2)
        reasons = [h.engine.order_meta[c.client_order_id].cancel_reason or "" for c in h.fx.cancels()]
        assert not [r for r in reasons if "EMERGENCY" in r], reasons
        assert not h.legs(cell, TP)                            # accumulating inside the reserved slots
        assert "TP_ACCUMULATING" in (h.engine.cell_blockers.get(cell) or ""), h.engine.cell_blockers.get(cell)
        h.fx.fill(entry.cid, D("6"))                           # entry complete
        h.tick(12)
        assert sum(t.requested for t in h.legs(cell, TP)) == D("10")
        assert not [r for r in (h.engine.order_meta[c.client_order_id].cancel_reason or "" for c in h.fx.cancels())
                    if "EMERGENCY" in r]
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ #6 rejected intents
def test_r06_off_tick_prices_latch_a_blocker_instead_of_dead_legs_every_tick(tmp_path):
    h = Harness(tmp_path, options=EngineOptions(tick_interval_s=1.0, min_wake_interval_s=1.0, rules_refresh_s=1.0))
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))
        h.fx.set_rules(tick_size=D("0.0007"))                    # fixed TP 5.4 / entries are now off tick
        h.tick(30)
        assert len(h.legs(cell, TP)) <= 1, [(t.state.value, t.cid) for t in h.legs(cell, TP)]
        assert "PRICE_NOT_ON_TICK" in (h.engine.cell_blockers.get(cell) or ""), h.engine.cell_blockers
        h.fx.set_rules(tick_size=D("0.0001"))                    # rules fixed: the TP goes out
        h.tick(6)
        assert h.live_order(cell, TP) is not None
    finally:
        h.close()


def test_r06_definitive_venue_reject_is_latched_with_backoff_not_resent_every_tick(tmp_path):
    h = Harness(tmp_path)
    try:
        h.fx.reject_prices.add(D("5.2"))                         # the venue persistently refuses one entry price
        _started(h)
        cell = next(c for c in h.buy_cells() if h.cell(c).spec.entry_price == D("5.2"))
        h.tick(60)
        sent = [c for c in h.fx.submits() if c.request.price == D("5.2")]
        assert len(sent) <= 6, len(sent)
        assert "VENUE_REJECT" in (h.engine.cell_blockers.get(cell) or ""), h.engine.cell_blockers.get(cell)
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ #7 stop drain
def test_r07_stop_drain_dispatches_a_committed_but_unsent_cancel(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        h.command(CommandKind.STOP, key="stop-r07")
        h.hooks.arm("after_cancel_intent_commit")            # die right after the first cancel intent commits
        assert h.tick_crashing(1)
        pending = [leg for leg in h.engine.non_final_legs() if leg.state == OrderState.CANCEL_PENDING]
        assert pending
        h.run_until(lambda: h.engine.is_stopped, max_ticks=60)
        assert h.fx.open_orders(owned=True) == []
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ #8 resolve audit
def _unknown_landed_submit(h):
    cell = h.buy_cells()[-1]
    h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))
    h.fx.script_submit(SubmitBehavior.TIMEOUT_LANDED)         # the TP lands, transport outcome unknown
    h.run_until(lambda: any(t.state == OrderState.SUBMIT_UNKNOWN for t in h.legs(cell, TP)), max_ticks=10)
    return cell, next(t for t in h.legs(cell, TP) if t.state == OrderState.SUBMIT_UNKNOWN)


def test_r08_resolve_unknown_submit_is_refused_without_an_active_list(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell, tp = _unknown_landed_submit(h)
        h.fx.fail_next("active_orders", 100000)
        h.tick(8)                                              # active list unavailable (None)
        h.command(CommandKind.BASELINE_AUDIT, {"action": "resolve_unknown_submit", "cid": str(tp.cid),
                                               "note": "x"}, key="r08-a")
        h.tick()
        assert _cmd(h, "r08-a").status == CommandStatus.REJECTED
        assert h.engine.leg_by_cid(tp.cid).state != OrderState.REJECTED_ZERO_FILL
    finally:
        h.close()


def test_r08_resolve_unknown_submit_is_refused_with_an_active_list_older_than_the_dispatch(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        _tick_until_poll(h)
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))
        h.fx.script_submit(SubmitBehavior.TIMEOUT_LANDED)
        h.tick()                                               # TP dispatched after the last active-list read
        tp = next(t for t in h.legs(cell, TP) if t.state == OrderState.SUBMIT_UNKNOWN)
        h.command(CommandKind.BASELINE_AUDIT, {"action": "resolve_unknown_submit", "cid": str(tp.cid),
                                               "note": "x"}, key="r08-b")
        h.tick()                                               # commands run before this tick's account read
        assert _cmd(h, "r08-b").status == CommandStatus.REJECTED, _cmd(h, "r08-b").result
        assert h.fx.order_by_cid(tp.cid).is_open
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ #9 stop outcome
def test_r09_stop_is_never_stopped_with_an_unknown_position_and_counts_the_baseline(tmp_path):
    h = Harness(tmp_path, expected_initial_position=D("330"), fx_kwargs={"initial_position": D("330")},
                options=EngineOptions(tick_interval_s=1.0, min_wake_interval_s=1.0, stop_uncertain_after_s=60.0))
    try:
        _started(h, D("330"))
        h.fx.fail_next("account", 100000)                        # position unknown from now on
        h.command(CommandKind.STOP, key="stop-r09")
        h.tick(30)
        assert h.engine.meta.stop_outcome not in ("STOPPED", "STOPPED_WITH_INVENTORY"), h.engine.meta.stop_outcome
        h.tick(40)
        assert h.engine.meta.stop_outcome == "STOP_UNCERTAIN"
        h.fx.fail_endpoints.clear()
        h.tick(10)
        assert h.engine.meta.stop_outcome == "STOPPED_WITH_INVENTORY"   # B = 330 held
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ #10 lookback
def test_r10_proven_live_orders_do_not_extend_the_history_lookback(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        h.tick(5)
        assert h.engine.non_final_legs() and all(leg.state == OrderState.LIVE for leg in h.engine.non_final_legs())
        assert h.engine.oldest_unresolved_ms() is None
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))
        h.fx.script_submit(SubmitBehavior.TIMEOUT_NOT_LANDED)
        h.run_until(lambda: any(t.state == OrderState.SUBMIT_UNKNOWN for t in h.legs(cell, TP)), max_ticks=10)
        unknown = next(t for t in h.legs(cell, TP) if t.state == OrderState.SUBMIT_UNKNOWN)
        assert h.engine.oldest_unresolved_ms() == h.engine.order_meta[unknown.cid].intent_ms
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ #11 stale active rows
def test_r11_active_row_older_than_the_terminal_commit_is_ignored_not_a_store_refusal(tmp_path):
    h = _racy(tmp_path)
    try:
        _started(h)
        h.tick(3)
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, ENTRY)
        _tick_until_poll(h)
        h.fx.fill(entry.cid, D("3"))                           # partial fill: the next active row differs (cache miss)
        h.fx.pending_fill = (entry.cid, D("7"))               # the rest fills between the next poll and history read
        for _ in range(8):
            h.engine.wake({"type": "order", "status": "filled"})   # a scan in every tick, so also in the poll tick
            h.tick()
        assert h.fx.pending_fill is None
        assert "LEDGER_INVARIANT" not in h.engine.meta.freezes, h.engine.meta.freezes
        assert not [e for e in h.engine.errors if e["code"] == "STORE_REFUSED"], list(h.engine.errors)
        assert h.live_order(cell, TP) is not None
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ #12 phantom lag
def test_r12_ws_signal_for_an_already_committed_trade_is_not_history_lag(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, ENTRY)
        h.fx.fill(entry.cid, D("10"))
        h.tick(4)
        trade = h.engine.store.fills(entry.cid)[0]
        h.engine.wake({"type": "trade", "trade_id": trade.trade_id_str, "client_order_id": str(entry.cid)})
        h.engine.wake({"type": "trade", "client_order_id": str(entry.cid)})      # cid-only late duplicate
        h.tick(20)
        assert h.engine.history_lag_s() == 0.0, h.engine.ws_pending
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ #13 risk-blocked TPs
def test_r13_risk_blocked_tps_name_their_cells_and_every_reason(tmp_path):
    h = Harness(tmp_path, max_abs_net_position=D("25"))
    try:
        _started(h)
        h.fx.manual_trade(Side.SELL, D("40"), D("5.4"))          # outside trade: short 40
        h.tick(8)
        h.command(CommandKind.BASELINE_AUDIT, {"observed_position": str(h.fx.net_position), "note": "audited"},
                  key="r13-audit")
        h.tick(4)
        cells = sorted(c for c in h.buy_cells() if h.live_order(c, ENTRY) is not None)[-2:]
        assert len(cells) == 2
        for c in cells:
            h.fx.fill(h.live_order(c, ENTRY).cid, D("10"))     # short -40 -> -20: each SELL TP would breach -25
        h.tick(8)
        detail = h.engine.meta.freezes.get("RISK_BLOCKED") or ""
        for c in cells:
            assert not h.legs(c, TP)
            assert "RISK_BLOCKED" in (h.engine.cell_blockers.get(c) or ""), (c, h.engine.cell_blockers)
            assert f"tp:{c}:" in detail, detail
        cell_view = {v["cell_id"]: v for v in h.engine.last_snapshot["cells"]}
        assert all("RISK_BLOCKED" in (cell_view[c]["blocker"] or "") for c in cells)
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ #14 release condition 5
def test_r14_release_waits_until_the_account_position_equals_the_ledger(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, ENTRY).cid, D("10"))
        h.run_until(lambda: h.live_order(cell, TP) is not None, max_ticks=10)
        h.fx.fill(h.live_order(cell, TP).cid, D("10"))
        h.fx.set_position_external(D("1"))                     # venue off the ledger P=0, inside [P_min, P_max]
        gen = h.cell(cell).generation
        h.tick(20)
        assert h.engine.endpoints.P_min <= D("1") <= h.engine.endpoints.P_max
        assert h.cell(cell).generation == gen and h.live_order(cell, ENTRY) is None   # no release, no re-arm
        assert not h.engine.b_engine.manual_reconcile_required
        h.fx.set_position_external(D("-1"))                    # venue agrees with the ledger again
        h.run_until(lambda: h.live_order(cell, ENTRY) is not None, max_ticks=20)
        assert h.cell(cell).generation == gen + 1
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ #16 invariant freeze
def test_r16_ledger_invariant_freeze_survives_reload_and_restart_until_acknowledged(tmp_path, monkeypatch):
    h = Harness(tmp_path)
    try:
        _started(h)
        original = h.engine._reconcile_active

        def refuse_once(now):
            monkeypatch.setattr(h.engine, "_reconcile_active", original)
            raise StoreError("simulated store refusal")

        monkeypatch.setattr(h.engine, "_reconcile_active", refuse_once)
        h.tick()
        h.tick(3)                                              # the next tick reloads from the store
        assert "LEDGER_INVARIANT" in h.engine.meta.freezes
        h.restart()
        h.tick(3)
        assert "LEDGER_INVARIANT" in h.engine.meta.freezes and h.state == EngineState.FROZEN
        h.command(CommandKind.BASELINE_AUDIT, {"action": "ack_history_conflict", "note": "reviewed"}, key="r16")
        h.tick(4)
        assert "LEDGER_INVARIANT" not in h.engine.meta.freezes
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ W1 start gate
def test_w1_confirm_baseline_requires_an_applied_acknowledged_start(tmp_path):
    h = Harness(tmp_path)
    try:
        h.run_until(lambda: h.engine.bootstrap_ready(h.clock())[0], max_ticks=60)
        h.command(CommandKind.CONFIRM_BASELINE, {"expected_initial_position": "0"}, key="w1-lone")
        h.tick(3)
        assert _cmd(h, "w1-lone").status == CommandStatus.REJECTED
        assert not h.engine.bootstrapped and h.fx.submits() == []
        h.command(CommandKind.START, {}, key="w1-bare-start")         # no risk acknowledgement / preview
        h.tick()
        assert _cmd(h, "w1-bare-start").status == CommandStatus.REJECTED
        assert h.engine.meta.started is False
        h.command(CommandKind.START, start_payload(), key="w1-start")
        h.tick()
        assert _cmd(h, "w1-start").status == CommandStatus.APPLIED
        h.run_until(lambda: h.engine.bootstrap_ready(h.clock())[0], max_ticks=60)
        h.command(CommandKind.CONFIRM_BASELINE, {"expected_initial_position": "0"}, key="w1-confirm")
        h.tick()
        assert _cmd(h, "w1-confirm").status == CommandStatus.APPLIED and h.engine.bootstrapped
    finally:
        h.close()


def test_w1_confirm_baseline_applies_the_enabled_gate(tmp_path):
    h = Harness(tmp_path)
    h.fx.remove_ws_listener(h.engine.wake)
    h.engine.store.close()
    config = make_config(enabled=False)
    store = open_engine_store(str(h.db_path), config, h.fx, lock_dir=str(h.lock_dir), clock=h.clock)
    engine = NeutralGridEngine(config, store, h.fx, h.clock, options=h.options, offline_demo=False)
    h.engine = engine
    try:
        h.run_until(lambda: engine.bootstrap_ready(h.clock())[0], max_ticks=60)
        h.command(CommandKind.CONFIRM_BASELINE, {"expected_initial_position": "0"}, key="w1-disabled")
        h.tick(2)
        record = _cmd(h, "w1-disabled")
        assert record.status == CommandStatus.REJECTED and not engine.bootstrapped
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ W2 published config
def test_w2_snapshot_publishes_engine_config_started_and_runtime_rules_bounds(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        summary = h.engine.last_snapshot["summary"]
        from hummingbot.strategy_v2.executors.neutral_grid_executor import grid
        published = summary["engine_config"]
        assert published["fingerprint"] == grid.config_fingerprint(h.config)
        for name, value in h.config.__dict__.items():
            if isinstance(value, Decimal):
                assert published[name] == str(value)
            elif hasattr(value, "value"):
                assert published[name] == value.value
            else:
                assert published[name] == value
        assert summary["started"] is True
        rr = summary["runtime_rules"]
        assert rr["supports_limit"] is True and rr["supports_post_only"] is True
        assert isinstance(rr["fetched_at"], str) and D(rr["fetched_at"]) > 0
        assert isinstance(rr["max_age_s"], str) and D(rr["max_age_s"]) >= D(str(h.engine.options.rules_refresh_s))
        web_runtime = pytest.importorskip("web.neutral_grid.runtime")
        config, fingerprint = web_runtime.engine_config_from_snapshot(h.engine.last_snapshot)
        assert config == h.config and fingerprint == published["fingerprint"]
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ W3 health sidecar
def test_w3_health_sidecar_reports_persistence_failure_and_recovery(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        path = h.db_path.parent / (h.db_path.name + ".health.json")
        h.hooks.simulate_disk_full()
        h.fx.fill(h.live_order(h.buy_cells()[-1], ENTRY).cid, D("10"))
        h.tick(3)
        data = json.loads(path.read_text())
        assert data["persistence_error"] and data["fatal_reason"] is None
        assert isinstance(data["at"], str) and isinstance(data["engine_revision"], int)
        h.hooks.restore_storage()
        h.tick(12)
        data = json.loads(path.read_text())
        assert data["persistence_error"] is None and data["fatal_reason"] is None
    finally:
        h.close()


def test_w3_health_sidecar_reports_a_fail_closed_engine(tmp_path):
    h = Harness(tmp_path)
    h.fx.remove_ws_listener(h.engine.wake)
    h.engine.store.close()
    h.db_path.write_bytes(b"not a sqlite database at all" * 8)
    try:
        engine = h.open_failclosed()
        h.engine = engine
        h.tick()
        data = json.loads((h.db_path.parent / (h.db_path.name + ".health.json")).read_text())
        assert data["fatal_reason"] and "STORE_OPEN_REFUSED" in data["fatal_reason"]
    finally:
        h.close()


# ------------------------------------------------------------------------------------------ W4 anti-flap
def test_w4_no_per_tick_degraded_normal_flapping(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        h.tick(3)
        assert h.state == EngineState.NORMAL
        rev0 = h.engine.store.engine().engine_revision
        states = []
        for i in range(12):
            h.fx.set_book(None, None) if i % 2 == 0 else h.fx.set_book(D("5.3999"), D("5.4001"))
            h.tick()
            states.append(h.state)
        flips = sum(1 for a, b in zip(states, states[1:]) if a != b)
        assert flips <= 2, states
        assert h.engine.store.engine().engine_revision - rev0 <= 2
        h.fx.set_book(D("5.3999"), D("5.4001"))
        h.run_until(lambda: h.state == EngineState.NORMAL, max_ticks=10)
    finally:
        h.close()


def test_harness_start_payload_matches_the_web_contract():
    assert set(start_payload()) == {"expected_initial_position", "risk_acknowledged", "baseline_acknowledged",
                                    "preview_id"} and len(PREVIEW_ID) == 24
