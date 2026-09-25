from decimal import Decimal

import pytest
from ng_engine_harness import Harness

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import LegRole, OrderState, Side


D = Decimal


def _entry_submits(h):
    return [call for call in h.fx.submits()
            if h.engine.leg_by_cid(call.client_order_id).identity.role == LegRole.ENTRY]


def test_default_policy_blocks_both_entry_sides_outside_bounds(tmp_path):
    h = Harness(tmp_path)
    try:
        h.bootstrap(confirm_tick=False)
        h.fx.set_book(D("4.6"), D("4.7"))

        h.tick(4)

        assert _entry_submits(h) == []
        assert "OUTSIDE_BOUNDS" in h.engine.entry_blockers
    finally:
        h.close()


@pytest.mark.parametrize(("book", "allowed_side"), [
    ((D("4.6"), D("4.7")), Side.SELL),
    ((D("6.2"), D("6.3")), Side.BUY),
])
def test_directional_policy_submits_only_risk_reducing_side_outside_bounds(tmp_path, book, allowed_side):
    h = Harness(tmp_path, directional_outside_bounds_entries=True)
    try:
        h.bootstrap(confirm_tick=False)
        h.fx.set_book(*book)

        h.tick(4)

        entries = _entry_submits(h)
        assert entries
        assert {call.request.side for call in entries} == {allowed_side}
    finally:
        h.close()


@pytest.mark.parametrize("book", [(None, None), (D("5.5"), D("5.4"))])
def test_directional_policy_submits_no_entries_for_unknown_or_crossed_book(tmp_path, book):
    h = Harness(tmp_path, directional_outside_bounds_entries=True)
    try:
        h.bootstrap(confirm_tick=False)
        h.fx.set_book(*book)

        h.tick(4)

        assert _entry_submits(h) == []
    finally:
        h.close()


def test_restart_below_bounds_with_persisted_buy_intent_withdraws_without_transport(tmp_path):
    h = Harness(tmp_path, directional_outside_bounds_entries=True)
    try:
        h.bootstrap(confirm_tick=False)
        h.hooks.arm("after_intent_commit")
        assert h.tick_crashing(1)
        pending = [leg for leg in h.engine.non_final_legs()
                   if leg.identity.role == LegRole.ENTRY and leg.state == OrderState.INTENT]
        assert len(pending) == 1 and pending[0].side == Side.BUY
        cid = pending[0].cid
        assert h.submits_for(cid) == []

        h.fx.set_book(D("4.6"), D("4.7"))
        h.tick(2)

        assert h.submits_for(cid) == []
        assert h.engine.leg_by_cid(cid).state == OrderState.REJECTED_UNSENT
    finally:
        h.close()


def test_transition_below_bounds_cancels_buy_entries_but_keeps_sell_tp(tmp_path):
    h = Harness(tmp_path, directional_outside_bounds_entries=True)
    try:
        h.bootstrap()
        h.tick(2)
        buy_cell = h.buy_cells()[-1]
        h.fx.fill(h.live_order(buy_cell, LegRole.ENTRY).cid, D("10"))
        h.run_until(lambda: h.live_order(buy_cell, LegRole.TP) is not None, max_ticks=20)
        tp = h.live_order(buy_cell, LegRole.TP)

        h.fx.set_book(D("4.6"), D("4.7"))
        h.tick(20)

        assert h.engine.leg_by_cid(tp.cid).state == OrderState.LIVE
        assert tp.cid not in {call.client_order_id for call in h.fx.cancels()}
        assert all(leg.identity.role != LegRole.ENTRY or leg.side == Side.SELL
                   for leg in h.engine.non_final_legs())
        cancelled = [h.engine.leg_by_cid(call.client_order_id) for call in h.fx.cancels()]
        assert any(leg.identity.role == LegRole.ENTRY and leg.side == Side.BUY for leg in cancelled)
    finally:
        h.close()
