"""NG-RISK-005 admission and slot ledger; AC-34, AC-57 (plus the preview numbers used by AC-46)."""
import unittest

from hummingbot.strategy_v2.executors.neutral_grid_executor import admission, grid
from hummingbot.strategy_v2.executors.neutral_grid_executor.admission import CellAdmission
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import OrderState, Side

from .helpers import BUY_CELL, Q, CoreSim, D, Harness, rules


def sample_cells(r=None):
    r = r or rules()
    return grid.assign_cells(grid.build_grid(D("5"), D("6"), 55, r), D("5.4"))


class TestSlotFormula(unittest.TestCase):
    def test_min_valid_tp_qty_and_required_slots(self):
        r = rules(min_base="5", min_notional="10")
        self.assertEqual(D("5"), admission.min_valid_tp_qty(r, D("5.4")))
        self.assertEqual(3, admission.required_slots(Q, r, D("5.4")))                 # 1 + ceil(10/5)
        r4 = rules(min_base="4", min_notional="0")
        self.assertEqual(4, admission.required_slots(Q, r4, D("5.4")))                # 1 + ceil(10/4)
        notional = rules(min_base="0", min_notional="30", step="1")
        self.assertEqual(D("6"), admission.min_valid_tp_qty(notional, D("5.4")))      # ceil(5.55..) to step 1
        self.assertEqual(3, admission.required_slots(Q, notional, D("5.4")))

    def test_sample_cap_120_arms_40_of_55_closest_first(self):
        cells = sample_cells()
        plan = admission.plan([CellAdmission(c, open_cycle=False) for c in cells], cap=120, mid=D("5.4"),
                              rules=rules(), order_amount_base=Q)
        self.assertEqual((40, 15), (len(plan.newly_armed), len(plan.queued)))
        self.assertEqual((120, 0, 120, 0), (plan.slots.cap, plan.slots.actual, plan.slots.reserved_unused,
                                            plan.slots.free))
        # Closest fixed entry price first: SELL cell 22 @5.4181 (0.0181) beats BUY cell 21 @5.3818 (0.0182).
        self.assertEqual((22, 21), plan.newly_armed[:2])
        distances = [abs(cells[c].entry_price - D("5.4")) for c in plan.newly_armed + plan.queued]
        self.assertEqual(sorted(distances), distances)
        self.assertLessEqual(max(distances[:40]), min(distances[40:]))
        # SELL side reaches 0.6 away, BUY side only 0.4: the far SELL cells and far BUY cells queue.
        self.assertIn(54, plan.queued)
        self.assertIn(0, plan.queued)
        self.assertEqual("SLOT_CAP", plan.blocker)

    def test_venue_cap_limits_user_cap(self):
        plan = admission.plan([CellAdmission(c, open_cycle=False) for c in sample_cells()], cap=120, mid=D("5.4"),
                              rules=rules(venue_cap=60), order_amount_base=Q)
        self.assertEqual(20, len(plan.newly_armed))

    def test_exit_obligations_are_reserved_before_new_entries(self):
        cells = sample_cells()
        views = [CellAdmission(c, open_cycle=(c.cell_id < 5), actual_orders=2 if c.cell_id < 5 else 0,
                               reserved_slots=0) for c in cells]
        plan = admission.plan(views, cap=20, mid=D("5.4"), rules=rules(), order_amount_base=Q)
        for c in range(5):
            self.assertEqual(3, plan.reservations[c])
        self.assertEqual(1, len(plan.newly_armed))                  # 20 - 15 = 5 free -> one more cell (3)
        self.assertEqual(2, plan.slots.free)

    def test_actual_and_reserved_are_not_double_counted(self):
        cells = sample_cells()[:3]
        views = [CellAdmission(cells[0], open_cycle=True, actual_orders=2, reserved_slots=3),
                 CellAdmission(cells[1], open_cycle=True, actual_orders=3, reserved_slots=3),
                 CellAdmission(cells[2], open_cycle=False)]
        plan = admission.plan(views, cap=10, mid=D("5.4"), rules=rules(), order_amount_base=Q)
        # Before arming: actual 5 (2+3), unused 1 (cell 0), free 4 -> cell 2 (needs 3) is armed.
        self.assertEqual((2,), plan.newly_armed)
        # After: unused 1 + 3; each slot counted exactly once: 5 + 4 + 1 == cap.
        self.assertEqual((5, 4, 1), (plan.slots.actual, plan.slots.reserved_unused, plan.slots.free))

    def test_paused_or_blocked_entries_keep_exit_reservations(self):
        cells = sample_cells()[:4]
        views = [CellAdmission(cells[0], open_cycle=True, actual_orders=1)] + \
                [CellAdmission(c, open_cycle=False) for c in cells[1:]]
        plan = admission.plan(views, cap=120, mid=D("5.4"), rules=rules(), order_amount_base=Q,
                              entries_allowed=False, entries_blocker="PAUSED")
        self.assertEqual(((), (3, 2, 1)), (plan.newly_armed, plan.queued))
        self.assertEqual("PAUSED", plan.blocker)
        self.assertEqual({0: 3}, plan.reservations)

    def test_slot_need_shrinks_when_entry_finished(self):
        h = Harness(BUY_CELL, r=rules(min_base="5", min_notional="0"))
        e = h.entry()
        self.assertEqual(3, admission.slot_need_from_ledger(h.ledger, h.rules))
        h.fill(e, "5")
        h.dispatch_tps()
        self.assertEqual(3, admission.slot_need_from_ledger(h.ledger, h.rules))
        h.fill(e, "5")
        h.dispatch_tps()
        self.assertEqual(3, admission.slot_need_from_ledger(h.ledger, h.rules))      # entry not yet proven
        h.ledger.confirm_terminal(e, D("10"))
        self.assertEqual(2, admission.slot_need_from_ledger(h.ledger, h.rules))      # entry slot returned
        views = [CellAdmission(BUY_CELL, open_cycle=True, actual_orders=2, reserved_slots=3,
                               slot_need=admission.slot_need_from_ledger(h.ledger, h.rules))]
        plan = admission.plan(views, cap=3, mid=D("5.4"), rules=h.rules, order_amount_base=Q)
        self.assertEqual((2, 1), (plan.reservations[BUY_CELL.cell_id], plan.slots.free))


class TestHeadOfLine(unittest.TestCase):
    def test_head_needing_more_slots_blocks_cells_behind_and_arms_first(self):
        # Review #7: min_notional 27 gives heterogeneous needs. SELL cell 23 (entry 5.4363, closest to 5.44)
        # has TP 5.4181 -> min_valid 4.99 -> 4 slots; BUY cell 21 has TP 5.4 -> min_valid 5 -> 3 slots.
        cells = sample_cells()
        r = rules(min_base="0", min_notional="27")
        self.assertEqual((4, 3), (admission.required_slots(Q, r, cells[23].tp_price),
                                  admission.required_slots(Q, r, cells[21].tp_price)))
        idle = [CellAdmission(cells[21], open_cycle=False), CellAdmission(cells[23], open_cycle=False)]
        busy = CellAdmission(cells[30], open_cycle=True, actual_orders=1, reserved_slots=4, slot_need=4)
        plan = admission.plan(idle + [busy], cap=7, mid=D("5.44"), rules=r, order_amount_base=Q)
        self.assertEqual(3, plan.slots.free)
        self.assertEqual(((), (23, 21)), (plan.newly_armed, plan.queued))  # 21 would fit, but must not skip
        plan = admission.plan(idle, cap=6, mid=D("5.44"), rules=r, order_amount_base=Q)   # cell 30 released
        self.assertEqual(((23,), (21,)), (plan.newly_armed, plan.queued))  # the head arms first
        plan = admission.plan(idle, cap=7, mid=D("5.44"), rules=r, order_amount_base=Q)
        self.assertEqual(((23, 21), ()), (plan.newly_armed, plan.queued))


class TestCapReduction(unittest.TestCase):
    def test_cap_drop_trims_stale_reservations_and_tp_goes_to_emergency_path(self):
        # Review #2: cap 3 -> venue cap 1 with one live entry: no unused reservation may survive, a TP for a
        # new partial fill must not be sent over the cap; it cancel-requests the cap-consuming entry and waits.
        from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import LegRole
        from hummingbot.strategy_v2.executors.neutral_grid_executor.router import (
            RouterIntent,
            RouterOrder,
            SlotBudget,
            plan_submits,
        )
        views = [CellAdmission(BUY_CELL, open_cycle=True, actual_orders=1, reserved_slots=3, slot_need=3)]
        plan = admission.plan(views, cap=3, mid=D("5.4"), rules=rules(venue_cap=1), order_amount_base=Q)
        self.assertEqual((1, 1, 0, 0), (plan.slots.cap, plan.slots.actual, plan.slots.reserved_unused,
                                        plan.slots.free))
        self.assertEqual(0, plan.unused(BUY_CELL.cell_id))
        owned = [RouterOrder(key="e", side=Side.BUY, price=BUY_CELL.low_price, remaining=D("5"),
                             state=OrderState.LIVE, role=LegRole.ENTRY, cell_id=BUY_CELL.cell_id, filled=D("5"))]
        tp = RouterIntent("tp", Side.SELL, BUY_CELL.high_price, D("5"), LegRole.TP, BUY_CELL.cell_id, 1)
        rplan = plan_submits([tp], owned, slots=SlotBudget.from_plan(plan), mid=D("5.4"))
        self.assertEqual(((), ("e",)), (rplan.submits, rplan.cancels))
        self.assertEqual("WAIT_SLOT:ENTRY_CANCEL_REQUESTED", rplan.action_for("tp").reason)

    def test_oversubscription_trims_entry_side_reservations_before_exit_ones(self):
        cells = sample_cells()
        views = [CellAdmission(cells[21], open_cycle=True, actual_orders=1, reserved_slots=3, slot_need=3,
                               entry_live=True),
                 CellAdmission(cells[20], open_cycle=True, actual_orders=1, reserved_slots=3, slot_need=2)]
        plan = admission.plan(views, cap=4, mid=D("5.4"), rules=rules(), order_amount_base=Q)
        # actual 2, cap 4 -> only 2 unused slots may remain: the exit-only cell 20 keeps its one extra slot.
        self.assertEqual((2, 2, 0), (plan.slots.actual, plan.slots.reserved_unused, plan.slots.free))
        self.assertEqual((1, 1), (plan.unused(20), plan.unused(21)))
        plan = admission.plan(views, cap=3, mid=D("5.4"), rules=rules(), order_amount_base=Q)
        self.assertEqual((1, 0), (plan.unused(20), plan.unused(21)))

    def test_sim_never_submits_over_a_reduced_cap(self):
        sim = CoreSim([BUY_CELL], cap=3, r=rules(min_base="5", min_notional="0"))
        sim.tick()
        (entry,) = sim.non_final()
        sim.fill(entry, D("5"))
        sim.cap = 1
        plan = sim.tick()
        self.assertEqual(((), (str(entry.cid),)), (plan.submits, plan.cancels))
        self.assertEqual(1, len(sim.non_final()))
        sim.ledgers[BUY_CELL.cell_id].set_state(entry.identity, OrderState.TERMINAL_UNKNOWN)
        sim.prove_terminal(entry)                                     # history terminal of the cancelled entry
        plan = sim.tick()
        self.assertEqual(1, len(plan.submits))                        # TP 5 now fits the reduced cap
        self.assertEqual(1, len(sim.non_final()))


class TestRuntimeMinimumChange(unittest.TestCase):
    def test_ac34_higher_floor_blocks_new_entries_without_resize(self):
        cells = sample_cells()
        r = rules(min_base="11")                                    # Q=10 now invalid everywhere
        plan = admission.plan([CellAdmission(c, open_cycle=False) for c in cells], cap=120, mid=D("5.4"),
                              rules=r, order_amount_base=Q)
        self.assertEqual((), plan.newly_armed)
        self.assertTrue(all(reason.startswith("FULL_Q_INVALID") for reason in plan.blocked.values()))
        self.assertEqual(55, len(plan.blocked))

    def test_ac34_higher_floor_leaves_existing_orders_and_makes_tp_blocker_visible(self):
        h = Harness(BUY_CELL, r=rules(min_base="5", min_notional="0"))
        e = h.entry()
        h.fill(e, "5")
        _, (tp,) = h.dispatch_tps()
        h.fill(e, "3")
        higher = rules(min_base="6", min_notional="0")
        plan = h.ledger.tp_obligation_to_dispatch(higher)
        self.assertEqual((), plan.quantities)                        # 3 < 6: no resize, no rounding up
        self.assertTrue(plan.blocker.startswith("BELOW_MIN:3<6"))
        self.assertEqual((OrderState.LIVE, D("5")), (h.leg(tp).state, h.leg(tp).requested))  # kept as is
        self.assertEqual(OrderState.LIVE, h.leg(e).state)
        with self.assertRaises(Exception):
            h.ledger.add_tp_intent(D("3"), 555, higher)              # invalid submit refused

    def test_ac34_lower_floor_grows_open_reservations_before_entries(self):
        cells = sample_cells()
        lower = rules(min_base="2", min_notional="0")                # required_slots 1 + ceil(10/2) = 6
        views = [CellAdmission(c, open_cycle=(c.cell_id == 21), actual_orders=1 if c.cell_id == 21 else 0,
                               reserved_slots=3 if c.cell_id == 21 else 0) for c in cells]
        plan = admission.plan(views, cap=9, mid=D("5.4"), rules=lower, order_amount_base=Q)
        self.assertEqual(6, plan.reservations[21])
        self.assertEqual((), plan.newly_armed)                        # 9 - 6 = 3 < 6 -> no new entry
        plan = admission.plan(views, cap=5, mid=D("5.4"), rules=lower, order_amount_base=Q)
        self.assertEqual(({21: 1}, "EXIT_RESERVATION_SHORTFALL"), (plan.shortfall, plan.blocker))
        self.assertEqual((5, 0), (plan.reservations[21], plan.slots.free))

    def test_ac34_runtime_floor_invalid_only_at_tp_price_blocks_arming(self):
        # Review #6: min_notional 54.8 -> SELL cell 26 entry 5.4909 (10 x = 54.909, valid) but TP 5.4727
        # (54.727 < 54.8): arming it would guarantee permanent DUST, so admission and the ledger both refuse.
        from hummingbot.strategy_v2.executors.neutral_grid_executor.cells import CellLedger, LedgerError
        cells = sample_cells()
        floor = rules(min_base="0", min_notional="54.8")
        self.assertEqual((D("5.4909"), D("5.4727")), (cells[26].entry_price, cells[26].tp_price))
        plan = admission.plan([CellAdmission(c, open_cycle=False) for c in cells], cap=120, mid=D("5.4"),
                              rules=floor, order_amount_base=Q)
        self.assertTrue(plan.blocked[26].startswith("FULL_Q_INVALID_TP"), plan.blocked.get(26))
        self.assertTrue(plan.blocked[25].startswith("FULL_Q_INVALID_ENTRY"), plan.blocked.get(25))
        self.assertNotIn(26, plan.newly_armed + plan.queued)
        self.assertIn(28, plan.newly_armed)                          # entry 5.5272, TP 5.5090: valid
        with self.assertRaisesRegex(LedgerError, "tp price 5.4727"):
            CellLedger("g1", cells[26], Q).begin_entry(1, floor)

    def test_invalid_rules_never_arm(self):
        plan = admission.plan([CellAdmission(c, open_cycle=False) for c in sample_cells()], cap=120,
                              mid=D("5.4"), rules=None, order_amount_base=Q)
        self.assertEqual((), plan.newly_armed)
        self.assertTrue(plan.blocker.startswith("RULES_INVALID"))


class TestSimultaneousPartialFills(unittest.TestCase):
    def assert_slots_sane(self, sim):
        slots = sim.last_admission.slots
        self.assertFalse(slots.oversubscribed)
        self.assertEqual(slots.cap, slots.actual + slots.reserved_unused + slots.free)
        self.assertLessEqual(len(sim.non_final()), sim.cap)

    def assert_deterministic_queue(self, sim, mid):
        adm = sim.last_admission
        idle = adm.newly_armed + adm.queued
        cells = {c: sim.ledgers[c].spec for c in idle}
        self.assertEqual(sorted(idle, key=lambda c: (abs(cells[c].entry_price - mid), c)), list(idle))

    def test_ac57_many_minimum_partial_fills_use_reserved_slots_no_cancel_and_queue_progresses(self):
        cells = sample_cells()
        sim = CoreSim(cells, cap=120, r=rules(min_base="5", min_notional="0"))
        sim.tick()
        entries = {leg.identity.cell_id: leg for leg in sim.non_final()}
        self.assertEqual(40, len(entries))
        buy_cells = sorted(c for c in entries if cells[c].entry_side == Side.BUY)
        lowest = buy_cells[0]
        # One history batch: the price sweeps down through every armed BUY cell. Each cell receives
        # minimum-eligible partial executions (5 + 5); the lowest touched level only its first 5.
        for c in buy_cells:
            sim.fill(entries[c], D("5"))
            if c != lowest:
                sim.fill(entries[c], D("5"))
        plan = sim.tick()
        self.assertEqual(len(buy_cells), len(plan.submits))           # every TP dispatched in the same tick
        self.assertTrue(all(k.startswith("tp:") for k in plan.submits))
        self.assertEqual(((), ()), (plan.cancels, plan.withdraws))    # no entry cancel, no slot emergency
        self.assert_slots_sane(sim)
        # Fully executed entries can no longer fill: each such cell now needs 2 slots, not 3. The 19 freed
        # slots arm exactly the head of the durable queue (6 cells x 3) - nothing else is re-planned.
        queue_before = sim.last_admission.queued
        plan = sim.tick()
        self.assertEqual(queue_before[:6], sim.last_admission.newly_armed)
        self.assertEqual(queue_before[6:], sim.last_admission.queued)
        self.assertEqual(6, len(plan.submits))
        self.assertTrue(all(k.startswith("entry:") for k in plan.submits))
        self.assertEqual((), plan.cancels)
        self.assert_slots_sane(sim)
        self.assertEqual((), sim.tick().actions)                     # steady state: no spam
        # The lowest cell completes its entry: the second TP child uses its own reserved third slot.
        self.assertEqual(1, sim.last_admission.unused(lowest))
        sim.fill(entries[lowest], D("5"))
        plan = sim.tick()
        self.assertEqual(((f"tp:{lowest}:0:0",), ()), (plan.submits, plan.cancels))
        self.assertEqual(3, len(sim.ledgers[lowest].non_final_legs()))  # entry (unproven) + 2 TP children
        self.assert_slots_sane(sim)
        # Entries proven terminal, TPs fill and are proven; BUY cells release. The price now sits low (5.05):
        # queued cells near it are armed first, resting SELL entries are never cancelled to chase priority.
        for c in buy_cells:
            sim.prove_terminal(entries[c])
        for leg in [x for x in sim.non_final() if x.identity.role.value == "TP"]:
            sim.fill(leg, leg.remaining)
            sim.prove_terminal(leg)
        sim.mid = D("5.05")
        first = sim.tick()                   # finished cycles need 0 slots: the rest of the queue arms in order
        self.assert_deterministic_queue(sim, D("5.05"))               # (releases happen at end of tick)
        armed_first = set(sim.last_admission.newly_armed)
        plan = sim.tick()                                             # released cells re-queue by distance
        self.assert_deterministic_queue(sim, D("5.05"))
        armed_now = armed_first | set(sim.last_admission.newly_armed)
        self.assertEqual(15, len(sim.last_admission.queued))          # 120 slots / 3 = 40 cells armed again
        self.assertEqual(((), ()), (first.cancels, plan.cancels))
        for c in entries:
            if cells[c].entry_side == Side.SELL:
                self.assertEqual(OrderState.LIVE, entries[c].state)
        self.assert_slots_sane(sim)
        for c in armed_now:
            leg = sim.ledgers[c].non_final_legs()[0]
            # Re-armed cells keep their fixed first side and exact entry price (BUY-cell rearms BUY, SELL rearms SELL).
            self.assertEqual((cells[c].entry_side, cells[c].entry_price, Q), (leg.side, leg.price, leg.requested))
        self.assertTrue(any(cells[c].entry_side == Side.BUY and sim.ledgers[c].generation == 1 for c in armed_now))


if __name__ == "__main__":
    unittest.main()
