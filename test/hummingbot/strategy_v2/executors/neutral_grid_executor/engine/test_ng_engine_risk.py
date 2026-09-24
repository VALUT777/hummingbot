"""Risk, baseline, ownership and routing integration: AC-03/04/24/25/26/28/29/30/31/32/34/37/38/43/44/45."""
import asyncio
import threading
from decimal import Decimal

import pytest
from ng_engine_harness import Harness

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CommandKind,
    CommandStatus,
    EngineState,
    LegRole,
    OrderState,
    OrderTypePolicy,
    Side,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import CID_EPOCH_SHIFT

D = Decimal
FINAL = (OrderState.TERMINAL, OrderState.REJECTED_UNSENT, OrderState.REJECTED_ZERO_FILL)


def _started(h, baseline=D("0")):
    h.bootstrap(baseline)
    h.tick(2)


def _live_entries(h, side=None):
    out = []
    for c in h.engine.cells:
        leg = h.live_order(c, LegRole.ENTRY)
        if leg is not None and leg.state not in FINAL and leg.remaining > 0 and (side is None or leg.side == side):
            out.append(leg)
    return out


def _entry_submits(h):
    return [c for c in h.fx.submits() if h.engine.leg_by_cid(c.client_order_id).identity.role == LegRole.ENTRY]


def _last_command(h):
    return h.engine.recent_commands[-1]


def test_ac03_sample_grid_55_cells_22_buy_33_sell_and_fixed_prices(tmp_path):
    h = Harness(tmp_path, cell_count=55)
    try:
        _started(h)
        prices = h.engine.grid_record.prices
        assert len(prices) == 56 and len(h.engine.cells) == 55
        assert len(h.buy_cells()) == 22 and len(h.sell_cells()) == 33
        assert all(b > a for a, b in zip(prices, prices[1:]))
        before = {c: (h.cell(c).spec.entry_price, h.cell(c).spec.tp_price, h.cell(c).spec.entry_side)
                  for c in h.engine.cells}
        h.fx.set_mid(D("5.9"))                                          # price bounce changes nothing
        h.tick(3)
        h.fx.set_mid(D("5.1"))
        h.tick(3)
        assert {c: (h.cell(c).spec.entry_price, h.cell(c).spec.tp_price, h.cell(c).spec.entry_side)
                for c in h.engine.cells} == before
        assert h.engine.grid_record.anchor == D("5.4")
        for call in h.fx.submits():
            assert call.request.price in set(prices)                    # only fixed grid lines are ever quoted
    finally:
        h.close()


@pytest.mark.parametrize("overrides, error", [
    ({"lower_price": D("5.00005")}, "grid"),                       # bound not a tick multiple
    ({"order_amount_base": D("10.05")}, "order_amount_base"),      # Q not a size-step multiple
    ({"order_amount_base": D("4")}, "invalid"),                    # full Q below the runtime minimum
])
def test_ac03_invalid_config_is_rejected_at_confirmation(tmp_path, overrides, error):
    h = Harness(tmp_path, **overrides)
    try:
        h.command(CommandKind.START)
        h.run_until(lambda: h.engine.bootstrap_ready(h.clock())[0], max_ticks=40)
        h.command(CommandKind.CONFIRM_BASELINE, {"expected_initial_position": "0"})
        h.tick(3)
        cmd = [c for c in h.engine.recent_commands if c["kind"] == "confirm_baseline"][-1]
        assert cmd["status"] == "REJECTED" and cmd["result"]["error"] == "CONFIG_INVALID"
        assert any(error in e for e in cmd["result"]["errors"])
        assert not h.engine.bootstrapped and h.fx.submits() == []
    finally:
        h.close()


def test_ac04_55_cells_one_task_no_polling_storm(tmp_path):
    h = Harness(tmp_path, cell_count=55)
    try:
        _started(h)
        h.tick(10)
        tasks_before = len(asyncio.all_tasks(h.loop)) if h.loop.is_running() else 0
        threads_before = threading.active_count()
        weights_before = len(h.fx.weight_log)
        pages_before = len(h.fx.page_log)
        h.tick(30)                                                      # 30 s steady state, 55 cells
        assert threading.active_count() == threads_before              # no per-cell threads
        assert tasks_before == 0 and not asyncio.all_tasks(h.loop)     # no background tasks left behind
        account_polls = sum(1 for t, e, w in h.fx.weight_log[weights_before:] if e == "account")
        scans = len(h.fx.page_log) - pages_before
        assert account_polls <= 30 // 5 + 1                             # coalesced cadence, not per cell
        assert scans <= 2 * (30 // 5 + 1)                               # trades + inactive per scan
        assert h.engine.weight_used(h.clock()) <= h.engine.options.weight_budget_per_min
        assert len({c.client_order_id for c in h.fx.submits()}) == len(h.fx.submits())
    finally:
        h.close()


def test_ac24_net_long_cap_rejects_pending_buy(tmp_path):
    h = Harness(tmp_path, max_abs_net_position=D("25"))
    try:
        _started(h)
        h.tick(3)
        buys = _live_entries(h, Side.BUY)
        assert sum(x.remaining for x in buys) <= D("25") and len(buys) == 2
        assert h.engine.endpoints.P_max <= D("25")
        assert any("NET_CAP_LONG" in (h.engine.cell_blockers.get(c) or "") for c in h.buy_cells())
        assert len(_live_entries(h, Side.SELL)) >= 2                   # other safe work continues
    finally:
        h.close()


def test_ac25_net_short_cap_rejects_pending_sell_shorts_within_cap_work(tmp_path):
    h = Harness(tmp_path, max_abs_net_position=D("25"))
    try:
        _started(h)
        h.tick(3)
        sells = _live_entries(h, Side.SELL)
        assert len(sells) == 2 and h.engine.endpoints.P_min >= D("-25")
        cell = sells[0].identity.cell_id
        h.fx.fill(sells[0].cid, D("10"))                                 # short within cap works
        h.tick(3)
        assert h.live_order(cell, LegRole.TP) is not None
        assert h.fx.net_position == D("-10") and h.engine.endpoints.P_min >= D("-25")
    finally:
        h.close()


def test_ac26_gross_cap_binds_even_when_net_is_flat(tmp_path):
    h = Harness(tmp_path, max_gross_position=D("30"))
    try:
        _started(h)
        h.tick(3)
        entries = _live_entries(h)
        assert len(entries) == 3                                         # 3 * 10 = gross 30
        assert {e.side for e in entries} == {Side.BUY, Side.SELL}       # offsetting sides do not bypass gross
        assert h.engine.endpoints.gross_worst <= D("30")
        buy = next(e for e in entries if e.side == Side.BUY)
        sell = next(e for e in entries if e.side == Side.SELL)
        h.fx.fill(buy.cid, D("10"))
        h.fx.fill(sell.cid, D("10"))
        h.tick(5)
        assert h.fx.net_position == 0                                   # venue flat ...
        assert h.engine.endpoints.gross_worst <= D("30")               # ... gross still counted
        assert len(_live_entries(h)) <= 1
    finally:
        h.close()


@pytest.mark.parametrize("baseline", [D("330"), D("0"), D("-200")])
def test_ac28_signed_baseline_accepted_without_seed_or_flatten(tmp_path, baseline):
    h = Harness(tmp_path, expected_initial_position=baseline, fx_kwargs={"initial_position": baseline})
    try:
        _started(h, baseline)
        h.tick(3)
        assert h.engine.baseline == baseline and h.engine.endpoints.P == baseline
        assert h.engine.endpoints.P_min == baseline - D("60") and h.engine.endpoints.P_max == baseline + D("40")
        cell_prices = {h.cell(c).spec.entry_price for c in h.engine.cells}
        for call in h.fx.submits():                                      # only cell entries: no seed/TP/flatten
            assert call.request.amount == D("10") and call.request.price in cell_prices
            assert h.engine.leg_by_cid(call.client_order_id).identity.role == LegRole.ENTRY
        assert len(h.fx.submits()) == len(h.engine.cells)
        assert h.fx.net_position == baseline
    finally:
        h.close()


def test_ac29_unknown_active_order_blocks_start_and_is_never_touched(tmp_path):
    h = Harness(tmp_path)
    try:
        manual = h.fx.place_manual_order(Side.BUY, D("5.05"), D("5"))
        h.command(CommandKind.START)
        h.tick(20)
        assert not h.engine.bootstrap_ready(h.clock())[0]
        assert h.engine.bootstrap_ready(h.clock())[1] == "unknown active orders on market"
        h.command(CommandKind.CONFIRM_BASELINE, {"expected_initial_position": "0"})
        h.tick(2)
        assert _last_command(h)["result"]["error"] == "BOOTSTRAP_NOT_READY"
        assert h.fx.submits() == [] and h.fx.cancels() == []            # never cancelled, never adopted
        assert h.fx.order_by_cid(manual.client_order_id).is_open
        snapshot = h.engine.last_snapshot
        assert snapshot["summary"]["unknown_active_orders"][0]["client_order_id"] == str(manual.client_order_id)
    finally:
        h.close()


def test_ac29_unknown_active_order_at_restart_blocks_everything(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        h.engine.store.close()
        manual = h.fx.place_manual_order(Side.SELL, D("5.95"), D("5"))
        submits = len(h.fx.submits())
        h.engine = h.open()
        h.tick(10)
        assert h.state == EngineState.RISK_BLOCKED
        assert "UNKNOWN_ACTIVE_ORDER_AT_STARTUP" in h.engine.reasons
        assert len(h.fx.submits()) == submits
        assert not [c for c in h.fx.cancels() if c.client_order_id == manual.client_order_id]
    finally:
        h.close()


def test_ac30_manual_trade_freezes_entries_keeps_tp_and_needs_audit(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, LegRole.ENTRY)
        h.fx.manual_trade(Side.SELL, D("7"), D("5.45"))                 # operator trades by hand
        h.tick(8)
        assert h.state == EngineState.RISK_BLOCKED
        assert any("unmatched" in b for b in h.engine.store_entry_blockers)
        entries_before = len(_entry_submits(h))
        h.fx.fill(entry.cid, D("10"))                                   # TP obligation must still be served
        h.tick(4)
        assert h.live_order(cell, LegRole.TP) is not None
        h.fx.venue_cancel(h.live_order(h.buy_cells()[0], LegRole.ENTRY).cid)
        h.tick(15)
        assert len(_entry_submits(h)) == entries_before                 # no new entries while frozen
        h.command(CommandKind.RESUME)
        h.tick()
        assert _last_command(h)["status"] == "REJECTED"                 # plain resume is not enough
        obligations = {c: h.cell(c).buckets() for c in h.engine.cells}
        h.command(CommandKind.BASELINE_AUDIT, {"observed_position": str(h.fx.net_position), "note": "manual"})
        h.tick(2)
        assert _last_command(h)["status"] == "APPLIED", _last_command(h)
        assert h.engine.effective_baseline == D("-7")
        assert {c: h.cell(c).buckets() for c in h.engine.cells} == obligations   # obligations untouched
        h.tick(10)
        assert len(_entry_submits(h)) > entries_before                   # entries resumed after the audit
    finally:
        h.close()


def test_ac31_self_trade_conflict_cancels_entry_and_tp_waits_for_terminal(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        lower, upper = h.sell_cells()[0], h.sell_cells()[1]     # upper TP BUY@5.5 crosses lower SELL entry@5.5
        h.fx.fill(h.live_order(upper, LegRole.ENTRY).cid, D("10"))
        crossing_seen = False
        for _ in range(20):
            h.tick()
            own = h.fx.open_orders(owned=True)
            bids = [o.price for o in own if o.side == Side.BUY]
            asks = [o.price for o in own if o.side == Side.SELL]
            crossing_seen |= bool(bids and asks and max(bids) >= min(asks))
            if h.live_order(upper, LegRole.TP) is not None and h.live_order(upper, LegRole.TP).state == OrderState.LIVE:
                break
        assert not crossing_seen                                          # never self-match
        lower_entry = h.legs(lower, LegRole.ENTRY)[0]
        assert h.engine.order_meta[lower_entry.cid].cancel_reason.startswith("SELF_TRADE_TP_PRIORITY")
        assert lower_entry.state == OrderState.TERMINAL                  # TP waited for history terminal
        tp = h.live_order(upper, LegRole.TP)
        assert tp.price == D("5.5") and tp.side == Side.BUY
    finally:
        h.close()


def test_ac32_outside_bounds_cancels_entries_keeps_tp_no_recenter(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, LegRole.ENTRY).cid, D("10"))
        h.tick(2)
        tp = h.live_order(cell, LegRole.TP)
        h.fx.set_book(D("6.2"), D("6.3"))                              # book fully above upper bound
        h.tick(20)
        assert "OUTSIDE_BOUNDS" in h.engine.reasons
        assert all(x.identity.role == LegRole.TP for x in h.engine.non_final_legs())
        assert h.engine.leg_by_cid(tp.cid).state == OrderState.LIVE     # fixed TP kept
        cancelled = [h.engine.leg_by_cid(c.client_order_id) for c in h.fx.cancels()]
        assert cancelled and all(x.identity.role == LegRole.ENTRY and x.state == OrderState.TERMINAL
                                 for x in cancelled)                    # terminal proof, not a cancel ack
        assert h.fx.net_position == D("10")                             # no flatten
        submits = len(h.fx.submits())
        h.tick(5)
        assert len(h.fx.submits()) == submits
        h.fx.set_book(D("5.3999"), D("5.4001"))
        h.tick(5)
        prices = {h.cell(c).spec.entry_price for c in h.engine.cells}
        assert all(c.request.price in prices | {h.cell(cell).spec.tp_price} for c in h.fx.submits())
    finally:
        h.close()


def test_ac34_runtime_minimum_change_blocks_invalid_submits_without_resize(tmp_path):
    h = Harness(tmp_path, options=None)
    try:
        _started(h)
        h.engine.options = type(h.engine.options)(rules_refresh_s=1.0)
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, LegRole.ENTRY)
        h.fx.set_rules(min_base=D("20"))                                 # Q=10 now invalid, TP 6 too
        h.fx.fill(entry.cid, D("6"))
        h.tick(4)
        assert h.engine.leg_by_cid(entry.cid).state == OrderState.LIVE   # accepted order kept
        assert h.legs(cell, LegRole.TP) == []                            # no rounding up to 20
        assert "BELOW_MIN" in (h.engine.cell_blockers.get(cell) or "")   # visible blocker
        h.fx.venue_cancel(h.live_order(h.buy_cells()[0], LegRole.ENTRY).cid)
        h.tick(15)
        blocked = h.engine.admission_plan.blocked.get(h.buy_cells()[0], "")
        assert "FULL_Q_INVALID" in blocked                               # new entry blocked, not resized
        assert not h.fx.violations
        h.fx.set_rules(min_base=D("5"))
        h.tick(4)
        assert [t.requested for t in h.legs(cell, LegRole.TP)] == [D("6")]
    finally:
        h.close()


@pytest.mark.parametrize("available, blocks", [(D("5"), False), (None, True), (D("NaN"), True)])
def test_ac37_margin_shortfall_warns_unknown_blocks(tmp_path, available, blocks):
    h = Harness(tmp_path, fx_kwargs={"available_collateral": available})
    try:
        h.bootstrap()
        h.tick(3)
        entries = _entry_submits(h)
        if blocks:
            assert entries == [] and any(r.startswith("MARGIN_UNKNOWN") for r in h.engine.reasons)
            assert h.state == EngineState.RISK_BLOCKED
        else:
            assert entries and h.engine.margin_warning and "MARGIN_SHORTFALL" in h.engine.margin_warning
            assert h.engine.last_snapshot["summary"]["margin"]["warning"] == h.engine.margin_warning
    finally:
        h.close()


def test_ac38_post_only_rejection_and_blockers_never_fall_back_to_market(tmp_path):
    h = Harness(tmp_path)
    try:
        h.bootstrap(confirm_tick=False)
        h.hooks.arm("after_intent_commit", action=lambda p: h.fx.set_book(D("5.25"), D("5.26")))
        h.tick(12)                                                       # first entries hit a moved book
        post_only = [o for o in h.fx.orders.values() if o.status == "canceled-post-only"]
        assert post_only                                                  # venue cancelled the crossing post-only
        h.fx.set_rules(min_base=D("20"))
        h.fx.rules_available = False
        h.tick(10)
        for call in h.fx.submits():
            assert call.request.order_type in (OrderTypePolicy.LIMIT_MAKER, OrderTypePolicy.LIMIT)
            assert call.request.reduce_only is False
        assert not [v for v in h.fx.violations if "unsupported order type" in v]
    finally:
        h.close()


def test_ac43_cid_collision_fails_closed_without_new_id(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        epoch = h.engine.b_engine.cid_epoch
        next_cid = (epoch << CID_EPOCH_SHIFT) | (len(h.fx.submits()) + 1)      # the CID the store would issue next
        h.fx.place_manual_order(Side.BUY, D("4.5"), D("1"), client_order_id=next_cid)   # foreign order uses it
        h.tick(6)                                                             # seen as an unknown active order
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, LegRole.ENTRY).cid, D("10"))            # a TP now needs a new CID
        submits = len(h.fx.submits())
        h.tick(6)
        assert len(h.fx.submits()) == submits                                 # nothing sent
        assert "CID_ALLOCATION" in h.engine.meta.freezes                      # fail closed
        assert h.state == EngineState.DEGRADED, h.engine.reasons              # latched CID failure: system fault
        assert "FROZEN:CID_ALLOCATION" in h.engine.tp_blockers                 # no TP with an unusable allocator
        assert all(c.client_order_id != next_cid for c in h.fx.submits())     # never reused / truncated
        assert not [v for v in h.fx.violations if "duplicate client order id" in v]
    finally:
        h.close()


def test_ac43_cid_map_binds_full_leg_identity_durably(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        for call in h.fx.submits():
            identity = h.engine.store.identity_for_cid(call.client_order_id)
            leg = h.engine.leg_by_cid(call.client_order_id)
            assert identity == leg.identity and identity.grid_id == "grid-t"
            assert h.engine.store.cid_for(identity) == call.client_order_id
            assert 0 < call.client_order_id <= (1 << 48) - 1
        h.crash_restart()
        h.tick(2)
        for call in h.fx.submits():
            assert h.engine.store.identity_for_cid(call.client_order_id) == \
                h.engine.leg_by_cid(call.client_order_id).identity
    finally:
        h.close()


def test_ac44_full_cap_tp_priority_cancels_cap_consuming_entry(tmp_path):
    h = Harness(tmp_path, max_active_orders=12)                  # 3 slots per cell -> 4 armed cells
    try:
        _started(h)
        h.tick(2)
        assert len(_live_entries(h)) == 4
        h.fx.set_rules(max_active_orders_venue=4)                    # venue cap reduced below reservations
        h.engine.options = type(h.engine.options)(rules_refresh_s=1.0)
        cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(cell, LegRole.ENTRY).cid, D("5"))
        for _ in range(30):
            h.tick()
            if h.live_order(cell, LegRole.TP) is not None:
                break
        tp = h.live_order(cell, LegRole.TP)
        assert tp is not None and tp.requested == D("5")
        reasons = [h.engine.order_meta[c.client_order_id].cancel_reason for c in h.fx.cancels()]
        assert any(r.startswith("SLOT_EMERGENCY_TP_PRIORITY") for r in reasons), reasons
        assert len(h.fx.open_orders(owned=True)) <= 4
    finally:
        h.close()


def test_ac44_tp_tp_conflict_is_fifo_without_netting(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        long_cell, short_cell = h.buy_cells()[-1], h.sell_cells()[0]    # TP SELL@5.4 vs TP BUY@5.4
        h.fx.fill(h.live_order(long_cell, LegRole.ENTRY).cid, D("10"))
        h.tick()
        h.fx.fill(h.live_order(short_cell, LegRole.ENTRY).cid, D("10"))
        h.tick(3)
        first = h.live_order(long_cell, LegRole.TP)
        assert first is not None and first.requested == D("10")
        assert h.legs(short_cell, LegRole.TP) == []                      # waits FIFO, not netted
        assert "WAIT_TP_FIFO" in h.engine.cell_blockers.get(short_cell, "")
        h.fx.fill(first.cid, D("10"))
        h.run_until(lambda: h.live_order(short_cell, LegRole.TP) is not None, max_ticks=30)
        assert h.live_order(short_cell, LegRole.TP).requested == D("10")   # full, never reduced by netting
    finally:
        h.close()


def test_ac45_stable_bootstrap_cut_once_and_never_recaptured(tmp_path):
    h = Harness(tmp_path, expected_initial_position=D("30"))
    try:
        h.fx.manual_trade(Side.BUY, D("30"), D("5.1"))                  # pre-bootstrap history
        h.clock.advance(5)
        h.command(CommandKind.START)
        h.run_until(lambda: h.engine.bootstrap_ready(h.clock())[0], max_ticks=40)
        h.command(CommandKind.CONFIRM_BASELINE, {"expected_initial_position": "25"})
        h.tick()
        assert _last_command(h)["result"]["error"] == "BASELINE_CONFIG_MISMATCH"
        h.command(CommandKind.CONFIRM_BASELINE, {"expected_initial_position": "30"})
        h.tick(3)
        assert h.engine.bootstrapped and h.engine.baseline == D("30")
        assert h.engine.unmatched == [] and not h.engine.store_entry_blockers   # pre-cut rows never counted
        assert h.engine.endpoints.P == D("30")
        h.restart()
        h.tick(8)
        assert h.engine.baseline == D("30") and h.engine.endpoints.P == D("30")
        h.command(CommandKind.CONFIRM_BASELINE, {"expected_initial_position": "30"})
        h.tick()
        assert _last_command(h)["result"]["error"] == "BASELINE_ALREADY_CONFIRMED"
        assert h.engine.store.engine().initial_baseline == D("30")
        assert h.state == EngineState.NORMAL
    finally:
        h.close()


def test_ac45_observed_position_must_equal_confirmed_b(tmp_path):
    h = Harness(tmp_path, expected_initial_position=D("0"), fx_kwargs={"initial_position": D("12")})
    try:
        h.command(CommandKind.START)
        h.run_until(lambda: h.engine.bootstrap_ready(h.clock())[0], max_ticks=40)
        h.command(CommandKind.CONFIRM_BASELINE, {"expected_initial_position": "0"})
        h.tick()
        cmd = _last_command(h)
        assert cmd["status"] == CommandStatus.REJECTED.value and cmd["result"]["error"] == "BASELINE_MISMATCH"
        assert cmd["result"]["observed_position"] == "12" and not h.engine.bootstrapped
    finally:
        h.close()


@pytest.mark.parametrize("rules, blocker, tp_blocked", [
    ({"supports_limit": False, "supports_post_only": False}, "MARKET_NOT_TRADABLE", True),
    ({"supports_post_only": False}, "POST_ONLY_UNSUPPORTED", False),      # LIMIT_MAKER entries, LIMIT TPs
])
def test_untradable_market_blocks_new_exposure_and_presend(tmp_path, rules, blocker, tp_blocked):
    from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import EngineOptions
    h = Harness(tmp_path, options=EngineOptions(tick_interval_s=1.0, min_wake_interval_s=1.0, rules_refresh_s=1.0))
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.set_rules(**rules)                     # the connector reports the market as not (fully) tradable
        h.fx.fill(h.live_order(cell, LegRole.ENTRY).cid, D("10"))
        entries = [c for c in h.fx.submits() if h.engine.leg_by_cid(c.client_order_id).identity.role == LegRole.ENTRY]
        h.tick(12)
        assert blocker in h.engine.entry_blockers and h.state == EngineState.DEGRADED, h.engine.reasons
        assert (blocker in h.engine.tp_blockers) is tp_blocked
        assert [c for c in h.fx.submits()
                if h.engine.leg_by_cid(c.client_order_id).identity.role == LegRole.ENTRY] == entries
        tps = [c for c in h.fx.submits() if h.engine.leg_by_cid(c.client_order_id).identity.role == LegRole.TP]
        assert bool(tps) is not tp_blocked                          # a LIMIT TP still closes the obligation
        from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import SubmitRequest
        req = SubmitRequest(client_order_id=1, side=Side.BUY, price=D("5.3"), amount=D("10"),
                            order_type=OrderTypePolicy.LIMIT_MAKER, reduce_only=False, expiry_ms=None)
        assert h.engine._pre_send_blocker(req) == blocker                # nothing reaches transport
    finally:
        h.close()
