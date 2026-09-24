"""NG-CELL-001..004, NG-GRID-003 quantities; AC-01/02 (core part), AC-05..08, AC-23, AC-33, AC-42 (ledger)."""
import json
import unittest

from hummingbot.strategy_v2.executors.neutral_grid_executor.cells import (
    CellLedger,
    FillOutcome,
    IllegalTransition,
    LedgerError,
    TerminalOutcome,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CellState,
    LegRole,
    OrderState,
    OrderTypePolicy,
    Side,
    TransportOutcome,
    TransportResult,
)

from .helpers import BUY_CELL, GRID_ID, SELL_CELL, D, Harness, rules


def full_cycle(h: Harness):
    """Entry fully filled, TP fully filled, both proven terminal."""
    e = h.entry()
    h.fill(e, "10")
    h.ledger.confirm_terminal(e, D("10"))
    _, (tp,) = h.dispatch_tps()
    h.fill(tp, "10")
    h.ledger.confirm_terminal(tp, D("10"))
    return e, tp


class TestNormalCycles(unittest.TestCase):
    def test_buy_cell_cycle_rearms_buy_at_same_fixed_prices(self):
        h = Harness(BUY_CELL)
        e, tp = full_cycle(h)
        entry, tp_leg = h.leg(e), h.leg(tp)
        self.assertEqual((Side.BUY, BUY_CELL.low_price), (entry.side, entry.price))
        self.assertEqual((Side.SELL, BUY_CELL.high_price, D("10")), (tp_leg.side, tp_leg.price, tp_leg.requested))
        self.assertTrue(h.ledger.can_release(position_reconciled=True).ok)
        self.assertEqual(0, h.ledger.release(position_reconciled=True))
        self.assertEqual(frozenset({CellState.IDLE}), h.ledger.state_flags())
        e2 = h.entry()
        nxt = h.leg(e2)
        self.assertEqual((1, Side.BUY, BUY_CELL.low_price, D("10")),
                         (e2.generation, nxt.side, nxt.price, nxt.requested))

    def test_sell_cell_cycle_rearms_sell(self):
        h = Harness(SELL_CELL)
        e, tp = full_cycle(h)
        self.assertEqual((Side.SELL, SELL_CELL.high_price), (h.leg(e).side, h.leg(e).price))
        self.assertEqual((Side.BUY, SELL_CELL.low_price), (h.leg(tp).side, h.leg(tp).price))
        h.ledger.release(True)
        self.assertEqual(Side.SELL, h.leg(h.entry()).side)

    def test_market_order_type_cannot_be_built(self):
        # AC-38 (core part): there is no MARKET policy and no fallback path to one.
        h = Harness(BUY_CELL)
        e = h.entry(accept=False)
        for bad in ("MARKET", None, "LIMIT"):
            with self.assertRaises(LedgerError):
                h.ledger.submit_request(e, bad)
        self.assertNotIn("MARKET", [p.value for p in OrderTypePolicy])

    def test_submit_request_is_exact_and_never_reduce_only(self):
        h = Harness(BUY_CELL)
        e = h.entry(accept=False)
        req = h.ledger.submit_request(e, OrderTypePolicy.LIMIT_MAKER)
        self.assertEqual((Side.BUY, BUY_CELL.low_price, D("10"), False),
                         (req.side, req.price, req.amount, req.reduce_only))
        self.assertEqual(h.leg(e).cid, req.client_order_id)


class TestPartialEntry(unittest.TestCase):
    def test_ac05_partial_entry_2_3_5_with_floor_5(self):
        h = Harness(BUY_CELL, r=rules(min_base="5", min_notional="0"))
        e = h.entry()
        h.fill(e, "2")
        plan = h.ledger.tp_obligation_to_dispatch(h.rules)
        self.assertEqual((), plan.quantities)
        self.assertEqual(D("2"), plan.pending_below_min)
        self.assertTrue(plan.blocker.startswith("BELOW_MIN"))
        self.assertEqual(D("0"), plan.dust)                      # entry still live: not dust
        self.assertEqual(OrderState.LIVE, h.leg(e).state)        # entry keeps resting
        self.assertEqual({CellState.ENTRY_LIVE, CellState.TP_REQUIRED}, set(h.ledger.state_flags()))
        self.assertEqual(D("2"), h.ledger.buckets().unassigned)

        h.fill(e, "3")
        plan, (tp1,) = h.dispatch_tps()
        self.assertEqual((D("5"),), plan.quantities)
        self.assertEqual({CellState.ENTRY_LIVE, CellState.TP_LIVE}, set(h.ledger.state_flags()))

        h.fill(e, "5")
        plan, (tp2,) = h.dispatch_tps()
        self.assertEqual((D("5"),), plan.quantities)
        b = h.ledger.buckets()
        self.assertEqual((D("10"), D("0"), D("10"), D("0"), D("0"), D("0")),
                         (b.E, b.X, b.live_tp_remainder, b.reserved_tp_unassigned, b.unassigned, b.dust))
        self.assertEqual(D("10"), h.leg(tp1).requested + h.leg(tp2).requested)
        self.assertEqual([], h.ledger.check_invariants())

    def test_entry_live_and_tp_live_are_simultaneous_leg_states(self):
        h = Harness(BUY_CELL)
        e = h.entry()
        h.fill(e, "6")
        _, (tp,) = h.dispatch_tps()
        self.assertEqual((OrderState.LIVE, OrderState.LIVE), (h.leg(e).state, h.leg(tp).state))
        self.assertIn(CellState.ENTRY_LIVE, h.ledger.state_flags())
        self.assertIn(CellState.TP_LIVE, h.ledger.state_flags())
        view = h.ledger.to_view()
        self.assertEqual(["ENTRY_LIVE", "TP_LIVE"], view["state_flags"])
        self.assertEqual("6", view["tp_children"][0]["requested"])
        self.assertEqual("4", view["entry"]["remaining"])

    def test_normal_partial_fill_never_cancels_entry(self):
        h = Harness(BUY_CELL)
        e = h.entry()
        for q in ("1", "1", "5"):
            h.fill(e, q)
            h.dispatch_tps()
        self.assertEqual(OrderState.LIVE, h.leg(e).state)

    def test_step_remainder_is_held_while_entry_live_and_never_rounded_up(self):
        h = Harness(BUY_CELL, r=rules(step="1", min_base="5", min_notional="0"))
        h.ledger.order_amount_base = D("10")
        e = h.entry()
        # A fill in a finer step than the current rules (runtime step change) is kept exactly.
        h.ledger.apply_fill(("k", 1), e, D("7.5"), BUY_CELL.low_price, Side.BUY)
        plan = h.ledger.tp_obligation_to_dispatch(h.rules)
        self.assertEqual((D("7"),), plan.quantities)
        self.assertEqual(D("0.5"), plan.pending_below_min)


class TestPartialTp(unittest.TestCase):
    def test_ac06_partial_tp_does_not_unlock_and_keeps_exact_target(self):
        h = Harness(BUY_CELL)
        e = h.entry()
        h.fill(e, "10")
        h.ledger.confirm_terminal(e, D("10"))
        _, (tp,) = h.dispatch_tps()
        h.fill(tp, "4")
        self.assertFalse(h.ledger.can_release(True).ok)
        self.assertEqual((), h.ledger.tp_obligation_to_dispatch(h.rules).quantities)   # no repost / no duplicate
        leg = h.leg(tp)
        self.assertEqual((OrderState.LIVE, D("6"), BUY_CELL.high_price), (leg.state, leg.remaining, leg.price))
        self.assertEqual(D("6"), h.ledger.buckets().live_tp_remainder)
        with self.assertRaises(LedgerError):
            h.ledger.begin_entry(99, h.rules)

    def test_tp_terminal_with_remainder_renews_exact_remainder_same_target(self):
        h = Harness(BUY_CELL)
        e = h.entry()
        h.fill(e, "10")
        h.ledger.confirm_terminal(e, D("10"))
        _, (tp,) = h.dispatch_tps()
        h.fill(tp, "4")
        h.ledger.set_state(tp, OrderState.TERMINAL_UNKNOWN)            # GTT expiry seen, not proven
        self.assertEqual((), h.ledger.tp_obligation_to_dispatch(h.rules).quantities)
        self.assertEqual(TerminalOutcome.TERMINAL, h.ledger.confirm_terminal(tp, D("4")))
        plan, (renewal,) = h.dispatch_tps()
        self.assertEqual((D("6"),), plan.quantities)
        self.assertEqual((1, BUY_CELL.high_price), (renewal.revision, h.leg(renewal).price))


class TestTerminalPartialEntryAndLateFills(unittest.TestCase):
    def test_ac07_terminal_partial_entry_closes_actual_qty_then_full_q(self):
        h = Harness(BUY_CELL, r=rules(min_base="1", min_notional="0"))
        e = h.entry()
        h.fill(e, "4")
        h.ledger.set_state(e, OrderState.CANCEL_PENDING)
        h.ledger.set_state(e, OrderState.TERMINAL_UNKNOWN)             # cancel ack: not a terminal proof
        self.assertIn("ENTRY_NOT_TERMINAL", h.ledger.can_release(True).reasons)
        self.assertEqual(TerminalOutcome.TERMINAL, h.ledger.confirm_terminal(e, D("4")))
        plan, (tp,) = h.dispatch_tps()
        self.assertEqual((D("4"),), plan.quantities)
        h.fill(tp, "4")
        h.ledger.confirm_terminal(tp, D("4"))
        h.ledger.release(True)
        self.assertEqual(D("10"), h.leg(h.entry()).requested)

    def test_ac08_late_fill_after_cancel_event_increases_obligation_of_same_cycle(self):
        h = Harness(BUY_CELL, r=rules(min_base="1", min_notional="0"))
        e = h.entry()
        h.fill(e, "5")
        h.dispatch_tps()
        h.ledger.set_state(e, OrderState.CANCEL_PENDING)
        h.ledger.set_state(e, OrderState.TERMINAL_UNKNOWN)
        # A local terminal event never fixes cumulative without history.
        self.assertEqual(TerminalOutcome.MISSING_FILLS, h.ledger.confirm_terminal(e, D("7")))
        self.assertEqual(OrderState.TERMINAL_UNKNOWN, h.leg(e).state)
        outcome, _ = h.fill(e, "2")                                    # late history fill
        self.assertEqual(FillOutcome.APPLIED, outcome)
        plan, (tp2,) = h.dispatch_tps()
        self.assertEqual((D("2"),), plan.quantities)
        self.assertEqual(TerminalOutcome.TERMINAL, h.ledger.confirm_terminal(e, D("7")))
        self.assertEqual(D("7"), h.ledger.buckets().live_tp_remainder)
        self.assertEqual(0, h.ledger.current.generation)

    def test_late_fill_after_release_goes_to_old_cycle_and_requires_audit(self):
        # Entry proven terminal at 6 of 10, cycle closed and released; the venue later reveals one more
        # execution of that entry (late evidence beyond the settlement window).
        h2 = Harness(BUY_CELL, r=rules(min_base="1", min_notional="0"))
        e2 = h2.entry()
        h2.fill(e2, "6")
        h2.ledger.confirm_terminal(e2, D("6"))
        _, (tp2,) = h2.dispatch_tps()
        h2.fill(tp2, "6")
        h2.ledger.confirm_terminal(tp2, D("6"))
        h2.ledger.release(True)
        outcome, _ = h2.fill(e2, "1")
        self.assertEqual(FillOutcome.LATE_EVIDENCE, outcome)
        old = h2.ledger.cycles[0]
        self.assertEqual((D("7"), D("6"), True), (old.E, old.X, old.late_evidence))
        self.assertIn(CellState.TP_REQUIRED, h2.ledger.state_flags())
        with self.assertRaises(LedgerError):
            h2.ledger.next_entry_identity()                            # no reuse while old cycle carries obligation
        self.assertTrue(any("terminal cumulative" in v for v in h2.ledger.check_invariants()))


class TestLateEvidenceResolution(unittest.TestCase):
    """AC-08/AC-42 resolution: late evidence -> audit -> ordinary TP closes it -> the cell keeps cycling."""

    def test_resolution_tp_fills_do_not_relatch_and_later_cycles_release(self):
        h = Harness(BUY_CELL, r=rules(min_base="1", min_notional="0"))
        e0 = h.entry()
        h.fill(e0, "4")
        h.ledger.set_state(e0, OrderState.CANCEL_PENDING)
        h.ledger.set_state(e0, OrderState.TERMINAL_UNKNOWN)
        h.ledger.confirm_terminal(e0, D("4"))
        _, (tp0,) = h.dispatch_tps()
        h.fill(tp0, "4")
        h.ledger.confirm_terminal(tp0, D("4"))
        h.ledger.release(True)
        e1 = h.entry()                                              # healthy, independent cycle 1
        h.fill(e1, "6")
        _, (tp1,) = h.dispatch_tps()
        # Late execution of the cycle-0 entry surfaces after reuse.
        outcome, _ = h.fill(e0, "1")
        self.assertEqual(FillOutcome.LATE_EVIDENCE, outcome)
        self.assertTrue(h.ledger.check_invariants())                # proven cumulative contradicted -> visible
        self.assertIn("LATE_EVIDENCE_AUDIT", h.ledger.can_release(True).reasons)
        # Operator audit: the executed quantity becomes the proven cumulative; the obligation stays.
        self.assertEqual([e0], h.ledger.acknowledge_late_evidence(0))
        self.assertEqual([], h.ledger.check_invariants())
        self.assertEqual(D("5"), h.leg(e0).terminal_cumulative)
        plan, legs = h.dispatch_tps()
        self.assertEqual(((0, D("1")),), tuple((i.generation, i.qty) for i in plan.items))
        (resolution,) = legs
        self.assertEqual(FillOutcome.APPLIED, h.fill(resolution, "1")[0])   # ordinary fill, no re-latch
        h.ledger.confirm_terminal(resolution, D("1"))
        self.assertFalse(any(c.late_evidence for c in h.ledger.cycles))
        self.assertEqual((D("5"), D("5")), (h.ledger.cycles[0].E, h.ledger.cycles[0].X))
        # Cycle 1 completes and releases; cycle 2 starts with full Q on the fixed side.
        h.fill(e1, "4")
        h.ledger.confirm_terminal(e1, D("10"))
        _, (tp1b,) = h.dispatch_tps()
        for leg, q in ((tp1, "6"), (tp1b, "4")):
            h.fill(leg, q)
            h.ledger.confirm_terminal(leg, D(q))
        self.assertTrue(h.ledger.can_release(True).ok)
        h.ledger.release(True)
        e2 = h.entry()
        self.assertEqual((2, Side.BUY, D("10")), (e2.generation, h.leg(e2).side, h.leg(e2).requested))

    def test_audited_execution_of_a_rejected_leg_becomes_terminal(self):
        h = Harness(BUY_CELL, r=rules(min_base="1", min_notional="0"))
        e = h.entry(accept=False)
        h.ledger.record_transport(e, TransportResult(TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL))
        self.assertEqual(FillOutcome.LATE_EVIDENCE, h.fill(e, "2")[0])   # venue contract broken -> audit
        self.assertTrue(any("rejected leg has fills" in v for v in h.ledger.check_invariants()))
        with self.assertRaises(LedgerError):
            h.ledger.next_entry_identity()
        h.ledger.acknowledge_late_evidence(0)
        self.assertEqual((OrderState.TERMINAL, D("2")), (h.leg(e).state, h.leg(e).terminal_cumulative))
        self.assertEqual([], h.ledger.check_invariants())
        plan, (tp,) = h.dispatch_tps()
        self.assertEqual((D("2"),), plan.quantities)
        h.fill(tp, "2")
        h.ledger.confirm_terminal(tp, D("2"))
        self.assertTrue(h.ledger.can_release(True).ok)


class TestIdempotencyAndConflicts(unittest.TestCase):
    def test_fill_idempotent_by_canonical_key(self):
        h = Harness()
        e = h.entry()
        outcome, key = h.fill(e, "3")
        self.assertEqual(FillOutcome.APPLIED, outcome)
        again = h.ledger.apply_fill(key, e, D("3"), BUY_CELL.low_price, Side.BUY)
        self.assertEqual(FillOutcome.DUPLICATE, again)
        self.assertEqual(D("3"), h.leg(e).filled)
        conflict = h.ledger.apply_fill(key, e, D("4"), BUY_CELL.low_price, Side.BUY)
        self.assertEqual(FillOutcome.CONFLICT_KEY, conflict)
        self.assertEqual(D("3"), h.leg(e).filled)

    def test_overfill_side_mismatch_and_unknown_leg_are_conflicts_not_applied(self):
        h = Harness()
        e = h.entry()
        self.assertEqual(FillOutcome.CONFLICT_OVERFILL, h.ledger.apply_fill(("a",), e, D("11"), D("5"), Side.BUY))
        self.assertEqual(FillOutcome.CONFLICT_SIDE, h.ledger.apply_fill(("b",), e, D("1"), D("5"), Side.SELL))
        self.assertEqual(FillOutcome.CONFLICT_QTY, h.ledger.apply_fill(("c",), e, D("0"), D("5"), Side.BUY))
        other = e.__class__(GRID_ID, 99, 0, LegRole.ENTRY, 0)
        self.assertEqual(FillOutcome.UNKNOWN_LEG, h.ledger.apply_fill(("d",), other, D("1"), D("5"), Side.BUY))
        self.assertEqual(D("0"), h.leg(e).filled)

    def test_terminal_cumulative_below_applied_is_conflict(self):
        h = Harness()
        e = h.entry()
        h.fill(e, "5")
        self.assertEqual(TerminalOutcome.CONFLICT, h.ledger.confirm_terminal(e, D("4")))
        self.assertEqual(TerminalOutcome.CONFLICT, h.ledger.confirm_terminal(e, D("11")))
        self.assertEqual(TerminalOutcome.TERMINAL, h.ledger.confirm_terminal(e, D("5")))
        self.assertEqual(TerminalOutcome.DUPLICATE, h.ledger.confirm_terminal(e, D("5")))
        self.assertEqual(TerminalOutcome.CONFLICT, h.ledger.confirm_terminal(e, D("6")))

    def test_fees_are_not_an_input_to_base_quantities(self):
        # AC-23: the ledger only accepts base execution size; TP equals exact confirmed base.
        h = Harness()
        e = h.entry()
        h.fill(e, "10")
        plan = h.ledger.tp_obligation_to_dispatch(h.rules)
        self.assertEqual((D("10"),), plan.quantities)
        with self.assertRaises(TypeError):
            h.ledger.apply_fill(("fee",), e, D("1"), D("5"), Side.BUY, fee=D("0.01"))


class TestUnknownOutcomes(unittest.TestCase):
    def test_unknown_tp_keeps_reservation_and_no_duplicate_tp(self):
        h = Harness()
        e = h.entry()
        h.fill(e, "10")
        _, (tp,) = h.dispatch_tps(accept=False)
        h.ledger.record_transport(tp, TransportResult(TransportOutcome.UNKNOWN, "timeout"))
        self.assertEqual(OrderState.SUBMIT_UNKNOWN, h.leg(tp).state)
        self.assertEqual((), h.ledger.tp_obligation_to_dispatch(h.rules).quantities)
        self.assertEqual(D("10"), h.ledger.buckets().reserved_tp_unassigned)
        self.assertIn(CellState.TP_INTENT, h.ledger.state_flags())
        # Only a documented definitive zero-fill rejection releases the reservation.
        h.ledger.record_transport(tp, TransportResult(TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL))
        plan, (tp2,) = h.dispatch_tps()
        self.assertEqual((D("10"),), plan.quantities)
        self.assertEqual(1, tp2.revision)

    def test_restart_marks_intent_unknown_and_keeps_cid(self):
        h = Harness()
        e = h.entry(accept=False)
        cid = h.leg(e).cid
        self.assertEqual([e], h.ledger.mark_intents_unknown())
        self.assertEqual((OrderState.SUBMIT_UNKNOWN, cid), (h.leg(e).state, h.leg(e).cid))
        with self.assertRaises(LedgerError):
            h.ledger.next_entry_identity()                            # locked: no new CID/identity

    def test_fill_evidence_promotes_unknown_submit_to_live(self):
        h = Harness()
        e = h.entry(accept=False)
        h.ledger.record_transport(e, TransportResult(TransportOutcome.UNKNOWN))
        h.fill(e, "1")
        self.assertEqual(OrderState.LIVE, h.leg(e).state)

    def test_rejections_require_zero_fill_and_proper_state(self):
        h = Harness()
        e = h.entry()
        h.fill(e, "1")
        with self.assertRaises(IllegalTransition):
            h.ledger.record_transport(e, TransportResult(TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL))
        with self.assertRaises(IllegalTransition):
            h.ledger.record_transport(e, TransportResult(TransportOutcome.NOT_SENT))

    def test_zero_fill_rejected_entry_gets_new_revision_in_same_generation(self):
        h = Harness()
        e = h.entry(accept=False)
        h.ledger.record_transport(e, TransportResult(TransportOutcome.NOT_SENT))
        e2 = h.entry()
        self.assertEqual((0, 1), (e2.generation, e2.revision))

    def test_illegal_transitions_raise(self):
        h = Harness()
        e = h.entry()
        with self.assertRaises(IllegalTransition):
            h.ledger.set_state(e, OrderState.TERMINAL)                  # only via history proof
        with self.assertRaises(IllegalTransition):
            h.ledger.set_state(e, OrderState.INTENT)
        h.fill(e, "10")
        h.ledger.confirm_terminal(e, D("10"))
        with self.assertRaises(IllegalTransition):
            h.ledger.set_state(e, OrderState.LIVE)


class TestReleaseAndDust(unittest.TestCase):
    def test_release_requires_each_ng_cell_001_condition(self):
        h = Harness(r=rules(min_base="5", min_notional="0"))
        e = h.entry()
        h.fill(e, "10")
        self.assertIn("ENTRY_NOT_TERMINAL", h.ledger.can_release(True).reasons)          # (1)
        h.ledger.confirm_terminal(e, D("10"))
        self.assertTrue(any(r.startswith("OBLIGATION_OPEN") for r in h.ledger.can_release(True).reasons))  # (2)
        _, (tp,) = h.dispatch_tps()
        h.fill(tp, "10")
        self.assertIn("ORDERS_NOT_TERMINAL", h.ledger.can_release(True).reasons)         # (3)
        h.ledger.confirm_terminal(tp, D("10"))
        self.assertEqual(("POSITION_NOT_RECONCILED",), h.ledger.can_release(False).reasons)  # (5)
        self.assertTrue(h.ledger.can_release(True).ok)

    def test_ac33_dust_is_durable_visible_and_blocks_reset(self):
        h = Harness(r=rules(min_base="5", min_notional="0"))
        e = h.entry()
        h.fill(e, "8")
        _, (tp,) = h.dispatch_tps()                                     # TP 8
        h.fill(e, "2")
        h.ledger.confirm_terminal(e, D("10"))
        plan = h.ledger.tp_obligation_to_dispatch(h.rules)
        self.assertEqual(((), D("2")), (plan.quantities, plan.dust))
        self.assertEqual({0: D("2")}, h.ledger.refresh_dust(h.rules))
        h.fill(tp, "8")
        h.ledger.confirm_terminal(tp, D("8"))
        self.assertEqual(CellState.DUST, h.ledger.primary_state())
        check = h.ledger.can_release(True)
        self.assertIn("DUST", check.reasons)
        with self.assertRaises(LedgerError):
            h.ledger.release(True)
        rec = json.loads(json.dumps(h.ledger.to_record()))
        restored = CellLedger.from_record(rec)
        self.assertEqual(D("2"), restored.buckets().dust)
        self.assertIn("DUST", restored.can_release(True).reasons)

    def test_dust_merges_with_returned_tp_remainder_same_side_and_target(self):
        h = Harness(r=rules(min_base="5", min_notional="0"))
        e = h.entry()
        h.fill(e, "7")
        _, (tp,) = h.dispatch_tps()                                     # TP 7
        h.fill(e, "3")
        h.ledger.confirm_terminal(e, D("10"))
        h.ledger.refresh_dust(h.rules)
        self.assertEqual(D("3"), h.ledger.buckets().dust)
        h.fill(tp, "5")
        h.ledger.confirm_terminal(tp, D("5"))                          # 2 returns to unassigned: 3 + 2 = 5
        plan, (tp2,) = h.dispatch_tps()
        self.assertEqual((D("5"),), plan.quantities)
        self.assertEqual(D("0"), h.ledger.buckets().dust)              # dust consumed by the merged TP
        self.assertEqual({}, h.ledger.refresh_dust(h.rules))

    def test_full_q_below_runtime_minimum_is_not_armed(self):
        h = Harness(r=rules(min_base="11"))
        with self.assertRaises(LedgerError):
            h.entry()

    def test_record_round_trip_is_exact(self):
        h = Harness()
        e = h.entry()
        h.fill(e, "6")
        h.dispatch_tps()
        rec = json.loads(json.dumps(h.ledger.to_record()))
        restored = CellLedger.from_record(rec)
        self.assertEqual(h.ledger.to_record(), restored.to_record())
        self.assertEqual(h.ledger.buckets(), restored.buckets())
        key = next(iter(h.ledger.fills))
        self.assertEqual(FillOutcome.DUPLICATE, restored.apply_fill(key, e, D("6"), BUY_CELL.low_price, Side.BUY))


if __name__ == "__main__":
    unittest.main()
