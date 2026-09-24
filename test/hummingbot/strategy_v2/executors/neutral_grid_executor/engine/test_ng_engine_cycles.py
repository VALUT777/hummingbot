"""Cell cycle integration (FakeExchange + real SQLite store): AC-01/02/05/06/07/08/23/27/33/51/57."""
from decimal import Decimal

import pytest
from ng_engine_harness import Harness

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CellState,
    LegRole,
    OrderState,
    OrderTypePolicy,
    Side,
)

D = Decimal
FINAL = (OrderState.TERMINAL, OrderState.REJECTED_UNSENT, OrderState.REJECTED_ZERO_FILL)


@pytest.fixture
def h(tmp_path):
    harness = Harness(tmp_path)
    yield harness
    harness.close()


def _started(h, baseline=D("0")):
    h.bootstrap(baseline)
    h.tick(2)


def _entry(h, cell_id):
    leg = h.live_order(cell_id, LegRole.ENTRY)
    assert leg is not None, [(x.identity, x.state) for x in h.legs(cell_id)]
    return leg


def _tp_legs(h, cell_id, generation=None):
    return [x for x in h.legs(cell_id, LegRole.TP) if generation is None or x.identity.generation == generation]


def test_ac01_normal_buy_cycle_rearms_buy(h):
    _started(h)
    cell = h.buy_cells()[-1]                       # [5.3, 5.4]: BUY @5.3, TP SELL @5.4
    spec = h.cell(cell).spec
    entry = _entry(h, cell)
    assert (entry.side, entry.price, entry.requested) == (Side.BUY, D("5.3"), D("10"))
    h.fx.set_book(D("5.2999"), D("5.3001"))
    h.fx.fill(entry.cid, D("10"))
    h.tick()
    tp = h.live_order(cell, LegRole.TP)
    assert tp is not None and (tp.side, tp.price, tp.requested) == (Side.SELL, spec.high_price, D("10"))
    call = h.submits_for(tp.cid)[0]
    assert call.request.order_type == OrderTypePolicy.LIMIT and call.request.expiry_ms is not None   # LIMIT GTT
    assert call.request.reduce_only is False
    h.fx.set_book(D("5.3999"), D("5.4001"))
    h.fx.fill(tp.cid, D("10"))
    h.run_until(lambda: h.cell(cell).generation == 2 and h.live_order(cell, LegRole.ENTRY) is not None
                and h.live_order(cell, LegRole.ENTRY).identity.generation == 2)
    nxt = h.live_order(cell, LegRole.ENTRY)
    assert (nxt.side, nxt.price, nxt.requested) == (Side.BUY, D("5.3"), D("10"))
    assert h.cell(cell).cycles[1].closed and h.cell(cell).cycles[1].E == h.cell(cell).cycles[1].X == D("10")
    h.assert_safe_orders()


def test_ac02_normal_sell_cycle_rearms_sell(h):
    _started(h)
    cell = h.sell_cells()[0]                       # [5.4, 5.5]: SELL @5.5, TP BUY @5.4
    entry = _entry(h, cell)
    assert (entry.side, entry.price) == (Side.SELL, D("5.5"))
    h.fx.set_book(D("5.4999"), D("5.5001"))
    h.fx.fill(entry.cid, D("10"))
    h.tick()
    tp = h.live_order(cell, LegRole.TP)
    assert (tp.side, tp.price, tp.requested) == (Side.BUY, D("5.4"), D("10"))
    assert h.submits_for(tp.cid)[0].request.order_type == OrderTypePolicy.LIMIT
    h.fx.set_book(D("5.3999"), D("5.4001"))
    h.fx.fill(tp.cid, D("10"))
    h.run_until(lambda: h.cell(cell).generation == 2 and h.live_order(cell, LegRole.ENTRY) is not None)
    nxt = h.live_order(cell, LegRole.ENTRY)
    assert (nxt.side, nxt.price, nxt.requested, nxt.identity.generation) == (Side.SELL, D("5.5"), D("10"), 2)
    h.assert_safe_orders()


def test_ac05_partial_entry_2_3_5_with_floor_5(h):
    _started(h)
    cell = h.buy_cells()[-1]
    entry = _entry(h, cell)
    h.fx.fill(entry.cid, D("2"))
    h.tick()
    assert _tp_legs(h, cell) == []                                       # 2 < floor 5: undispatched
    assert h.live_order(cell, LegRole.ENTRY).state == OrderState.LIVE   # entry keeps resting
    view = next(c for c in h.engine.last_snapshot["cells"] if c["cell_id"] == cell)
    assert view["obligation"]["E"] == "2" and view["obligation"]["unassigned"] == "2"
    assert view["blocker"] and "BELOW_MIN" in view["blocker"]
    h.fx.fill(entry.cid, D("3"))
    h.tick()
    tps = _tp_legs(h, cell)
    assert [t.requested for t in tps] == [D("5")]
    h.fx.fill(entry.cid, D("5"))
    h.tick()
    tps = _tp_legs(h, cell)
    assert [t.requested for t in tps] == [D("5"), D("5")]
    assert sum(t.requested for t in tps) == D("10") == h.cell(cell).cycles[-1].E
    assert all(t.price == D("5.4") and t.side == Side.SELL for t in tps)
    b = h.cell(cell).buckets()
    assert b.E - b.X == b.live_tp_remainder + b.reserved_tp_unassigned + b.unassigned + b.dust


def test_ac06_partial_tp_does_not_unlock_or_repost(h):
    _started(h)
    cell = h.buy_cells()[-1]
    entry = _entry(h, cell)
    h.fx.fill(entry.cid, D("10"))
    h.tick()
    tp = h.live_order(cell, LegRole.TP)
    h.fx.fill(tp.cid, D("4"))
    h.tick(30)
    tps = _tp_legs(h, cell)
    assert len(tps) == 1 and tps[0].state == OrderState.LIVE and tps[0].filled == D("4")
    assert tps[0].price == D("5.4") and tps[0].remaining == D("6")
    assert h.cell(cell).generation == 1 and h.cell(cell).current is not None     # still locked
    assert not [c for c in h.fx.cancels() if c.client_order_id == tp.cid]         # no cancel/repost
    assert len([x for x in h.legs(cell, LegRole.ENTRY)]) == 1


def test_ac07_terminal_partial_entry_closes_actual_then_full_cycle(tmp_path):
    h = Harness(tmp_path, fx_kwargs={"min_base": D("1")})
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = _entry(h, cell)
        h.fx.fill(entry.cid, D("3"))
        h.tick()
        h.fx.venue_cancel(entry.cid)
        h.tick()
        tps = _tp_legs(h, cell)
        assert [t.requested for t in tps] == [D("3")]
        h.fx.fill(tps[0].cid, D("3"))
        h.run_until(lambda: h.cell(cell).generation == 2 and h.live_order(cell, LegRole.ENTRY) is not None)
        assert h.cell(cell).cycles[1].E == h.cell(cell).cycles[1].X == D("3")
        assert h.live_order(cell, LegRole.ENTRY).requested == D("10")        # full configured Q again
    finally:
        h.close()


def test_ac08_late_fill_after_cancel_event_extends_obligation(tmp_path):
    h = Harness(tmp_path, fx_kwargs={"min_base": D("1")})
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = _entry(h, cell)
        h.fx.fill(entry.cid, D("4"), history_lag_s=40)          # trade row arrives late
        h.fx.venue_cancel(entry.cid)                            # cancel event + terminal row now
        h.tick(12)
        leg = h.legs(cell, LegRole.ENTRY)[0]
        assert leg.state == OrderState.TERMINAL_UNKNOWN          # cancel event does not unlock
        assert h.cell(cell).current is not None and _tp_legs(h, cell) == []
        h.run_until(lambda: _tp_legs(h, cell) != [], max_ticks=60)
        assert [t.requested for t in _tp_legs(h, cell)] == [D("4")]
        h.settle()
        assert h.legs(cell, LegRole.ENTRY)[0].state == OrderState.TERMINAL
        assert h.cell(cell).cycles[1].E == D("4")
    finally:
        h.close()


def test_ac23_quantities_never_rounded_up_and_fees_do_not_touch_base(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = _entry(h, cell)
        h.fx.fill(entry.cid, D("7.3"))
        h.tick()
        assert [t.requested for t in _tp_legs(h, cell)] == [D("7.3")]      # exact, not 7 / not 7.5
        h.fx.venue_cancel(entry.cid)                                        # remainder 2.7 never trades
        cell2 = h.buy_cells()[-2]
        e2 = _entry(h, cell2)
        h.fx.fill(e2.cid, D("4.9"))
        h.fx.venue_cancel(e2.cid)
        h.settle()
        h.tick(3)
        assert _tp_legs(h, cell2) == []                                     # 4.9 < 5 is never rounded to 5
        assert h.cell(cell2).cycles[-1].dust == D("4.9")
        for call in h.fx.submits():
            assert call.request.amount % D("0.1") == 0
            leg = h.engine.leg_by_cid(call.client_order_id)
            if leg.identity.role == LegRole.TP:
                cycle = next(c for c in h.cell(leg.identity.cell_id).cycles
                             if c.generation == leg.identity.generation)
                assert sum(t.requested for t in cycle.tps) <= cycle.E
    finally:
        h.close()


def test_ac27_virtual_tp_crosses_zero_without_reduce_only(h):
    _started(h)
    buy_cell, sell_cell = h.buy_cells()[-1], h.sell_cells()[0]
    h.fx.fill(_entry(h, buy_cell).cid, D("10"))
    h.fx.fill(_entry(h, sell_cell).cid, D("10"))
    h.tick()
    assert h.fx.net_position == 0
    tp_long = h.live_order(buy_cell, LegRole.TP)
    assert tp_long.side == Side.SELL
    h.fx.fill(tp_long.cid, D("10"))                                     # venue goes net short
    h.tick(3)
    assert h.fx.net_position == D("-10")
    assert h.engine.endpoints.P == D("-10")                             # ledger agrees
    assert h.cell(sell_cell).buckets().open_obligation == D("10")       # short cell still owes its BUY TP
    assert all(c.request.reduce_only is False for c in h.fx.submits())


def test_ac33_dust_is_visible_durable_and_blocks_reset(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = _entry(h, cell)
        h.fx.fill(entry.cid, D("3"))
        h.fx.venue_cancel(entry.cid)
        h.settle()
        h.tick(5)
        cycle = h.cell(cell).cycles[-1]
        assert cycle.dust == D("3") and CellState.DUST in h.cell(cell).state_flags()
        view = next(c for c in h.engine.last_snapshot["cells"] if c["cell_id"] == cell)
        assert view["obligation"]["dust"] == "3"
        assert h.engine.last_snapshot["summary"]["dust_total"] == "3"
        assert h.cell(cell).generation == 1 and h.live_order(cell, LegRole.ENTRY) is None   # never re-armed
        h.restart()
        h.tick(3)
        assert h.cell(cell).cycles[-1].dust == D("3")                     # durable across restart
        assert not [c for c in h.fx.submits() if c.request.side == Side.SELL and c.request.price == D("5.4")]
    finally:
        h.close()


def test_ac51_gtt_renewal_only_after_terminal_with_exact_remainder(tmp_path):
    h = Harness(tmp_path, tp_gtt_seconds=40)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        h.fx.fill(_entry(h, cell).cid, D("10"))
        h.tick()
        first = h.live_order(cell, LegRole.TP)
        h.fx.fill(first.cid, D("4"))
        max_live = 0
        for _ in range(80):
            h.tick()
            live = [t for t in _tp_legs(h, cell) if t.state not in FINAL]
            max_live = max(max_live, len(live))
            if len(_tp_legs(h, cell)) >= 2:
                break
        tps = _tp_legs(h, cell)
        assert len(tps) == 2, [(t.state, t.filled) for t in tps]
        assert tps[0].state == OrderState.TERMINAL and tps[0].filled == D("4")
        assert tps[1].requested == D("6") and tps[1].price == D("5.4")     # exact remainder, same fixed target
        assert max_live == 1                                                # never two TPs for one remainder
        assert h.fx.order_by_cid(first.cid).status == "canceled-expired"
    finally:
        h.close()


def test_ac57_simultaneous_partial_fills_use_reserved_slots(tmp_path):
    # 3 slots per cell (1 entry + ceil(10/5) TP children); cap 24 arms exactly 8 of 10 cells.
    h = Harness(tmp_path, max_active_orders=24)
    try:
        _started(h)
        h.tick(2)
        plan = h.engine.admission_plan
        assert len(plan.armed) == 8 and len(plan.queued) == 2
        assert list(plan.queued) == [8, 9]                      # farthest entries queue deterministically
        armed = sorted(c for c in h.engine.cells if h.live_order(c, LegRole.ENTRY) is not None)
        assert len(armed) == 8
        # A one-directional move: every armed long cell gets a minimum-eligible partial fill at once.
        longs = [c for c in armed if h.cell(c).spec.entry_side == Side.BUY]
        for c in longs:
            h.fx.fill(h.live_order(c, LegRole.ENTRY).cid, D("5"))
        h.tick()
        top = max(longs)                                        # its TP crosses no own resting order
        assert [t.requested for t in _tp_legs(h, top)] == [D("5")]

        def covered(c):
            return sum(t.requested for t in _tp_legs(h, c)) == h.cell(c).cycles[-1].E

        for _ in range(60):
            assert len(h.fx.open_orders(owned=True)) <= 24                  # cap never exceeded
            assert h.engine.admission_plan is None or h.engine.admission_plan.slots.free >= 0
            assert not [b for b in h.engine.cell_blockers.values() if "WAIT_SLOT" in b]
            if all(covered(c) for c in longs):
                break
            h.tick()
        assert all(covered(c) for c in longs), {c: h.engine.cell_blockers.get(c) for c in longs}
        reasons = [h.engine.order_meta[c.client_order_id].cancel_reason for c in h.fx.cancels()]
        # Only the self-trade TP-priority rule cancels (the lower cell's TP crosses the next cell's entry);
        # nothing is ever cancelled to find a slot.
        assert all(r.startswith("SELF_TRADE_TP_PRIORITY") for r in reasons), reasons
        assert len(reasons) <= len(longs) - 1
    finally:
        h.close()
