"""NG-ORD-003 router; AC-31 (self-trade), AC-44 (TP capacity / TP-TP FIFO), caps inside routing (AC-24/25)."""
import unittest

from hummingbot.strategy_v2.executors.neutral_grid_executor import grid, risk
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import LegRole, OrderState, Side
from hummingbot.strategy_v2.executors.neutral_grid_executor.router import (
    ActionKind,
    RouterIntent,
    RouterOrder,
    SlotBudget,
    crosses,
    plan_submits,
)

from .helpers import D, Harness, rules

LIMITS = risk.RiskLimits(D("1000"), D("1000"))


def order(key, side, price, remaining="10", state=OrderState.LIVE, role=LegRole.ENTRY, cell=None, seq=0, filled="0"):
    return RouterOrder(key=key, side=side, price=D(price), remaining=D(remaining), state=state, role=role,
                       cell_id=cell, seq=seq, filled=D(filled))


def tp(key, side, price, qty="10", cell=0, seq=0):
    return RouterIntent(key=key, side=side, price=D(price), qty=D(qty), role=LegRole.TP, cell_id=cell, seq=seq)


def entry(key, side, price, qty="10", cell=0, seq=0):
    return RouterIntent(key=key, side=side, price=D(price), qty=D(qty), role=LegRole.ENTRY, cell_id=cell, seq=seq)


class TestCrossing(unittest.TestCase):
    def test_crosses(self):
        self.assertTrue(crosses(Side.BUY, D("5.4"), Side.SELL, D("5.4")))
        self.assertTrue(crosses(Side.SELL, D("5.1"), Side.BUY, D("5.6")))
        self.assertFalse(crosses(Side.SELL, D("5.6"), Side.BUY, D("5.1")))
        self.assertFalse(crosses(Side.BUY, D("5.4"), Side.BUY, D("5.4")))


class TestSelfTrade(unittest.TestCase):
    def test_ac31_tp_cancels_conflicting_entry_and_waits_for_history_terminal(self):
        owned = [order("e22", Side.BUY, "5.4", remaining="5", cell=22, filled="5")]
        plan = plan_submits([tp("tp21", Side.SELL, "5.4", cell=21)], owned)
        self.assertEqual(("e22",), plan.cancels)
        self.assertEqual((), plan.submits)
        self.assertEqual("SELF_TRADE:WAIT_ENTRY_TERMINAL", plan.action_for("tp21").reason)
        # Next tick: cancel acknowledged but not proven -> still conflicting, no second cancel (no spam).
        for state in (OrderState.CANCEL_PENDING, OrderState.CANCEL_UNKNOWN, OrderState.TERMINAL_UNKNOWN):
            owned = [order("e22", Side.BUY, "5.4", remaining="5", state=state, cell=22, filled="5")]
            plan = plan_submits([tp("tp21", Side.SELL, "5.4", cell=21)], owned)
            self.assertEqual(((), ()), (plan.cancels, plan.submits), state)
        # History proves the entry terminal: it is no longer an owned non-final order -> TP goes.
        self.assertEqual(("tp21",), plan_submits([tp("tp21", Side.SELL, "5.4", cell=21)], []).submits)

    def test_unsent_entry_intent_is_withdrawn_unknown_submit_is_only_waited_for(self):
        plan = plan_submits([tp("t", Side.SELL, "5.4")],
                            [order("i", Side.BUY, "5.4", state=OrderState.INTENT)])
        self.assertEqual((("i",), ()), (plan.withdraws, plan.submits))
        plan = plan_submits([tp("t", Side.SELL, "5.4")],
                            [order("u", Side.BUY, "5.4", state=OrderState.SUBMIT_UNKNOWN)])
        self.assertEqual(((), (), ()), (plan.cancels, plan.withdraws, plan.submits))

    def test_fully_executed_entry_does_not_conflict_and_partial_fill_alone_never_cancels(self):
        owned = [order("done", Side.BUY, "5.4", remaining="0", filled="10"),
                 order("partial", Side.BUY, "5.3", remaining="5", filled="5")]
        plan = plan_submits([tp("t", Side.SELL, "5.4")], owned)
        self.assertEqual((("t",), ()), (plan.submits, plan.cancels))

    def test_tp_never_cancels_tp_or_unknown_role_orders_and_does_not_net(self):
        owned = [order("tp_old", Side.BUY, "5.6", role=LegRole.TP, cell=33),
                 order("unk", Side.BUY, "5.5", role=None)]
        plan = plan_submits([tp("tp_new", Side.SELL, "5.09", cell=4)], owned)
        self.assertEqual(((), (), ()), (plan.cancels, plan.withdraws, plan.submits))
        action = plan.action_for("tp_new")
        self.assertEqual(("SELF_TRADE:WAIT_TP_FIFO", ("tp_old", "unk")), (action.reason, action.blocked_by))

    def test_entry_conflicting_with_anything_waits_and_cancels_nothing(self):
        owned = [order("tpS", Side.SELL, "5.4", role=LegRole.TP, cell=21)]
        plan = plan_submits([entry("e22", Side.BUY, "5.4", cell=22)], owned)
        self.assertEqual(((), ()), (plan.submits, plan.cancels))
        self.assertEqual("SELF_TRADE:ENTRY_WAITS", plan.action_for("e22").reason)

    def test_all_live_pending_and_unknown_states_are_checked(self):
        for state in (OrderState.INTENT, OrderState.SUBMIT_UNKNOWN, OrderState.LIVE, OrderState.CANCEL_PENDING,
                      OrderState.CANCEL_UNKNOWN, OrderState.TERMINAL_UNKNOWN):
            plan = plan_submits([entry("e", Side.BUY, "5.4")],
                                [order("o", Side.SELL, "5.4", state=state, role=LegRole.TP)])
            self.assertEqual((), plan.submits, state)
        for state in (OrderState.TERMINAL, OrderState.REJECTED_UNSENT, OrderState.REJECTED_ZERO_FILL):
            plan = plan_submits([entry("e", Side.BUY, "5.4")],
                                [order("o", Side.SELL, "5.4", state=state, role=LegRole.TP)])
            self.assertEqual(("e",), plan.submits, state)

    def test_sample_grid_geometry_tp_of_lower_buy_cell_vs_resting_upper_buy_entry(self):
        cells = grid.assign_cells(grid.build_grid(D("5"), D("6"), 55, rules()), D("5.4"))
        low, up = Harness(cells[10]), Harness(cells[11])
        e_low, e_up = low.entry(), up.entry()
        up.fill(e_up, "5")                                     # upper level partially filled, remainder resting
        low.fill(e_low, "10")                                  # lower level filled
        low.ledger.confirm_terminal(e_low, D("10"))
        owned = [RouterOrder.from_leg(leg) for h in (low, up) for leg in h.ledger.non_final_legs()]
        up_tp_candidates = up.ledger.tp_obligation_to_dispatch(up.rules).items
        candidates = [tp("tp10", Side.SELL, str(cells[10].tp_price), qty="10", cell=10, seq=1),
                      tp("tp11", Side.SELL, str(cells[11].tp_price), qty=str(up_tp_candidates[0].qty), cell=11,
                         seq=2)]
        plan = plan_submits(candidates, owned)
        self.assertEqual((str(up.leg(e_up).cid),), plan.cancels)   # cell 11 entry @P[11] crosses TP SELL @P[11]
        self.assertEqual(("tp11",), plan.submits)                   # TP @P[12] crosses nothing
        self.assertEqual("SELF_TRADE:WAIT_ENTRY_TERMINAL", plan.action_for("tp10").reason)


class TestFifo(unittest.TestCase):
    def test_tp_tp_conflicts_are_fifo_and_later_ones_cannot_overtake(self):
        cands = [tp("b_late", Side.BUY, "5.6", cell=33, seq=5),
                 tp("s_early", Side.SELL, "5.09", cell=4, seq=1),
                 tp("s_other", Side.SELL, "5.2", cell=9, seq=3)]
        plan = plan_submits(cands, [])
        self.assertEqual(("s_early", "s_other"), plan.submits)          # same side: no conflict between them
        self.assertEqual(("SELF_TRADE:WAIT_TP_FIFO", ("s_early", "s_other")),
                         (plan.action_for("b_late").reason, plan.action_for("b_late").blocked_by))
        # A waiting earlier TP blocks later conflicting ones even if the owner of the conflict is not live yet.
        owned = [order("tp_live", Side.SELL, "5.3", role=LegRole.TP)]
        cands = [tp("first", Side.BUY, "5.5", seq=1), tp("second", Side.SELL, "5.45", seq=2)]
        plan = plan_submits(cands, owned)
        self.assertEqual((), plan.submits)
        self.assertEqual(("first",), plan.action_for("second").blocked_by)

    def test_tp_priority_over_entries_in_same_plan(self):
        cands = [entry("e", Side.BUY, "5.4", cell=22, seq=1), tp("t", Side.SELL, "5.4", cell=21, seq=9)]
        plan = plan_submits(cands, [], slots=SlotBudget(free=0, cell_unused={21: 1, 22: 3}))
        self.assertEqual(("t",), plan.submits)
        self.assertEqual("SELF_TRADE:ENTRY_WAITS", plan.action_for("e").reason)

    def test_paused_entries_wait_tps_continue(self):
        cands = [entry("e", Side.BUY, "5.0", cell=0, seq=1), tp("t", Side.SELL, "5.9", cell=50, seq=2)]
        plan = plan_submits(cands, [], entries_allowed=False, entries_blocker="PAUSED")
        self.assertEqual(("t",), plan.submits)
        self.assertEqual("PAUSED", plan.action_for("e").reason)


class TestCapsAndHeadroom(unittest.TestCase):
    def test_entries_respect_net_and_gross_caps_across_the_plan(self):
        ep = risk.RiskEndpoints(P=D("0"), P_min=D("0"), P_max=D("985"), gross_worst=D("0"))
        cands = [entry("e1", Side.BUY, "5.1", cell=1, seq=1), entry("e2", Side.BUY, "5.0", cell=0, seq=2)]
        plan = plan_submits(cands, [], endpoints=ep, limits=LIMITS)
        self.assertEqual(("e1",), plan.submits)                         # 985 + 10 ok, + 10 more exceeds 1000
        self.assertTrue(plan.action_for("e2").reason.startswith("NET_CAP_LONG"))
        ep = risk.RiskEndpoints(P=D("0"), P_min=D("-995"), P_max=D("0"), gross_worst=D("0"))
        plan = plan_submits([entry("s", Side.SELL, "5.9", cell=50)], [], endpoints=ep, limits=LIMITS)
        self.assertTrue(plan.action_for("s").reason.startswith("NET_CAP_SHORT"))

    def test_tp_headroom_cancels_same_side_entries_then_waits(self):
        limits = risk.RiskLimits(D("20"), D("1000"))
        owned = [order("buy_far", Side.BUY, "5.0", cell=0), order("buy_near", Side.BUY, "5.3", cell=16)]
        ep = risk.RiskEndpoints(P=D("-10"), P_min=D("-10"), P_max=D("10"), gross_worst=D("30"))
        plan = plan_submits([tp("t", Side.BUY, "5.6", qty="15", cell=33)], owned, endpoints=ep, limits=limits,
                            mid=D("5.4"))
        self.assertEqual(("buy_far",), plan.cancels)                    # 10 + 15 - 20 = 5 -> one entry, farthest
        self.assertEqual(ActionKind.WAIT, plan.action_for("t").kind)

    def test_tp_risk_blocked_when_confirmed_position_leaves_no_headroom(self):
        limits = risk.RiskLimits(D("20"), D("1000"))
        ep = risk.RiskEndpoints(P=D("25"), P_min=D("15"), P_max=D("25"), gross_worst=D("30"))
        plan = plan_submits([tp("t", Side.BUY, "5.6", qty="5", cell=33)], [], endpoints=ep, limits=limits)
        self.assertTrue(plan.risk_blocked)
        self.assertEqual(ActionKind.BLOCKED, plan.action_for("t").kind)
        self.assertEqual((), plan.submits)


class TestTpPriorityHeadroom(unittest.TestCase):
    def test_entries_cannot_consume_headroom_owed_to_undispatched_exits(self):
        # Regression (property sweep seed 193): SELL cell short 10 owes a BUY TP; a BUY entry that fits the
        # order-only P_max must still wait, otherwise the owed BUY TP would later be RISK_BLOCKED.
        limits = risk.RiskLimits(D("20"), D("1000"))
        ep = risk.RiskEndpoints(P=D("0"), P_min=D("0"), P_max=D("10"), gross_worst=D("20"))
        cand = [entry("e", Side.BUY, "5.1", cell=1)]
        self.assertEqual(("e",), plan_submits(cand, [], endpoints=ep, limits=limits).submits)
        plan = plan_submits(cand, [], endpoints=ep, limits=limits, owed=(D("10"), D("0")))
        self.assertTrue(plan.action_for("e").reason.startswith("NET_CAP_LONG"))
        # Once the owed exit is routed in the same plan it is an order, still counted exactly once.
        cands = [tp("t", Side.BUY, "5.9", qty="10", cell=11, seq=1), entry("e", Side.BUY, "5.1", cell=1, seq=2)]
        plan = plan_submits(cands, [], endpoints=ep, limits=limits, owed=(D("10"), D("0")))
        self.assertEqual(("t",), plan.submits)
        self.assertTrue(plan.action_for("e").reason.startswith("NET_CAP_LONG"))

    def test_obligation_totals_from_ledgers(self):
        cells = grid.assign_cells(grid.build_grid(D("5"), D("6"), 55, rules()), D("5.4"))
        short, long_ = Harness(cells[40]), Harness(cells[3])
        for h in (short, long_):
            e = h.entry()
            h.fill(e, "10")
        self.assertEqual((D("10"), D("10")), risk.obligation_totals([short.ledger, long_.ledger]))
        short.dispatch_tps()
        self.assertEqual((D("0"), D("10")), risk.obligation_totals([short.ledger, long_.ledger]))
        ep = risk.endpoints_from_ledgers(D("0"), [short.ledger, long_.ledger])
        owed = risk.with_obligations(ep, *risk.obligation_totals([short.ledger, long_.ledger]))
        self.assertEqual((D("-10"), D("10")), (owed.P_min, owed.P_max))

    def test_risk_blocked_tp_does_not_hold_fifo_head_for_other_exits(self):
        limits = risk.RiskLimits(D("20"), D("1000"))
        ep = risk.RiskEndpoints(P=D("15"), P_min=D("15"), P_max=D("15"), gross_worst=D("30"))
        cands = [tp("blocked_buy", Side.BUY, "5.9", qty="10", cell=11, seq=1),
                 tp("sell_exit", Side.SELL, "5.1", qty="10", cell=1, seq=2),
                 entry("e", Side.SELL, "5.85", qty="1", cell=10, seq=3)]
        plan = plan_submits(cands, [], endpoints=ep, limits=limits)
        self.assertEqual(ActionKind.BLOCKED, plan.action_for("blocked_buy").kind)
        self.assertEqual(("sell_exit",), plan.submits)                  # the exit that frees headroom goes
        self.assertEqual("SELF_TRADE:ENTRY_WAITS", plan.action_for("e").reason)
        self.assertEqual(("blocked_buy",), plan.action_for("e").blocked_by)  # entries never cross a blocked TP


class TestTpCapacity(unittest.TestCase):
    def test_ac44_full_cap_tp_cancels_one_cap_consuming_entry_then_waits_without_spam(self):
        owned = [order("e_near", Side.BUY, "5.38", cell=21), order("e_far", Side.SELL, "5.9", cell=49),
                 order("e_part", Side.BUY, "5.0", cell=0, remaining="5", filled="5"),
                 order("tp_live", Side.SELL, "5.95", role=LegRole.TP, cell=52, remaining="5")]
        full = SlotBudget(free=0, cell_unused={})
        plan = plan_submits([tp("t", Side.BUY, "5.7", cell=35)], owned, slots=full, mid=D("5.4"))
        self.assertEqual(("e_far",), plan.cancels)                      # unfilled, farthest from mid
        self.assertEqual("WAIT_SLOT:ENTRY_CANCEL_REQUESTED", plan.action_for("t").reason)
        owned[1] = order("e_far", Side.SELL, "5.9", cell=49, state=OrderState.CANCEL_PENDING)
        plan = plan_submits([tp("t", Side.BUY, "5.7", cell=35)], owned, slots=full, mid=D("5.4"))
        self.assertEqual((), plan.cancels)                              # in-flight cancel covers it
        self.assertEqual("WAIT_SLOT:ENTRY_CANCEL_IN_FLIGHT", plan.action_for("t").reason)
        plan = plan_submits([tp("t", Side.BUY, "5.7", cell=35)], owned[:1] + owned[2:],
                            slots=SlotBudget(free=1), mid=D("5.4"))
        self.assertEqual(("t",), plan.submits)

    def test_ac44_tp_uses_own_reservation_before_global_and_tp_fifo_for_last_slot(self):
        budget = SlotBudget(free=1, cell_unused={7: 1})
        cands = [tp("a", Side.SELL, "5.2", cell=7, seq=2), tp("b", Side.SELL, "5.3", cell=12, seq=3),
                 tp("c", Side.SELL, "5.35", cell=15, seq=4)]
        plan = plan_submits(cands, [], slots=budget)
        self.assertEqual(("a", "b"), plan.submits)                      # a: own slot, b: last global slot
        self.assertEqual("WAIT_SLOT:NO_CANCELLABLE_ENTRY", plan.action_for("c").reason)

    def test_entry_needs_its_own_reserved_slot(self):
        plan = plan_submits([entry("e", Side.BUY, "5.0", cell=0)], [], slots=SlotBudget(free=10, cell_unused={}))
        self.assertEqual("NOT_ARMED:NO_RESERVED_SLOT", plan.action_for("e").reason)


if __name__ == "__main__":
    unittest.main()
