"""NG-GRID-003 dust; AC-33 (visible, durable, blocks reset, same side+target only), AC-39 (ledger aggregate TP)."""
import itertools
import json
import unittest

from hummingbot.strategy_v2.executors.neutral_grid_executor import dust, grid
from hummingbot.strategy_v2.executors.neutral_grid_executor.cells import (
    CellLedger,
    FillOutcome,
    LedgerError,
    TerminalOutcome,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    OrderState,
    Side,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.dust import AggregateTp, DustLot

from .helpers import D, Harness, rules


def lot(cell, gen, side, target, qty):
    return DustLot(cell_id=cell, generation=gen, side=side, target=D(target), qty=D(qty))


class TestGrouping(unittest.TestCase):
    def test_only_same_side_and_exact_target_aggregate(self):
        lots = [lot(3, 0, Side.SELL, "5.0727", "2"), lot(3, 1, Side.SELL, "5.0727", "2.5"),
                lot(4, 0, Side.SELL, "5.0909", "3"), lot(9, 0, Side.BUY, "5.0727", "4")]
        groups = dust.group(lots)
        self.assertEqual([(Side.BUY, D("5.0727"), D("4")), (Side.SELL, D("5.0727"), D("4.5")),
                          (Side.SELL, D("5.0909"), D("3"))],
                         [(g.side, g.target, g.total) for g in groups])

    def test_aggregate_quantity_never_rounded_up_and_allocation_is_exact(self):
        lots = [lot(3, 1, Side.SELL, "5.0727", "2.5"), lot(3, 0, Side.SELL, "5.0727", "3.7")]
        (plan,) = dust.aggregate(lots, rules(step="1", min_base="5", min_notional="0"))
        self.assertEqual(D("6"), plan.qty)                               # floor(6.2) with step 1
        self.assertEqual((((3, 0), D("3.7")), ((3, 1), D("2.3"))), plan.allocation)
        self.assertEqual((((3, 1), D("0.2")),), plan.residual)           # stays DUST, exact
        self.assertEqual(plan.qty, sum(q for _, q in plan.allocation))
        self.assertEqual([], dust.aggregate(lots, rules(step="1", min_base="7", min_notional="0")))

    def test_aggregate_above_max_base_is_split_into_valid_balanced_orders(self):
        # Review #4 (dust side): a group above max_base forms several valid orders instead of none.
        lots = [lot(3, 0, Side.SELL, "5.0727", "4"), lot(3, 1, Side.SELL, "5.0727", "6")]
        plans = dust.aggregate(lots, rules(step="1", min_base="5", min_notional="0", max_base="6"))
        self.assertEqual([D("5"), D("5")], [p.qty for p in plans])
        self.assertEqual([(((3, 0), D("4")), ((3, 1), D("1"))), (((3, 1), D("5")),)],
                         [p.allocation for p in plans])
        self.assertEqual(((), ()), (plans[0].residual, plans[1].residual))

    def test_geometry_no_two_cells_share_tp_side_and_target(self):
        cells = grid.assign_cells(grid.build_grid(D("5"), D("6"), 55, rules()), D("5.4"))
        keys = [(c.tp_side, c.tp_price) for c in cells]
        self.assertEqual(len(keys), len(set(keys)))


class TestAggregateFills(unittest.TestCase):
    def plan(self):
        lots = [lot(3, 0, Side.SELL, "5.0727", "2"), lot(3, 1, Side.SELL, "5.0727", "3"),
                lot(3, 2, Side.SELL, "5.0727", "1")]
        (plan,) = dust.aggregate(lots, rules(step="1", min_base="5", min_notional="0"))
        return plan

    def test_ac39_partial_fills_split_exactly_and_idempotently(self):
        agg = AggregateTp.from_plan(self.plan())
        self.assertEqual({(3, 0): D("2"), (3, 1): D("1")}, agg.apply_fill("t1", D("3")))
        self.assertEqual({}, agg.apply_fill("t1", D("3")))              # duplicate: no effect
        self.assertIsNone(agg.apply_fill("t1", D("2")))                 # same key, different size: conflict
        self.assertIsNone(agg.apply_fill("t9", D("4")))                 # would overfill 6
        self.assertEqual({(3, 1): D("2"), (3, 2): D("1")}, agg.apply_fill("t2", D("3")))
        self.assertEqual({(3, 0): D("2"), (3, 1): D("3"), (3, 2): D("1")}, agg.per_lot_filled())

    def test_ac39_per_lot_result_independent_of_fill_order(self):
        fills = [("a", D("1")), ("b", D("2.5")), ("c", D("0.5")), ("d", D("1"))]
        results = set()
        for perm in itertools.permutations(fills):
            agg = AggregateTp.from_plan(self.plan())
            for key, q in perm + perm:                                  # replay included
                agg.apply_fill(key, q)
            results.add(tuple(sorted(agg.per_lot_filled().items())))
        self.assertEqual(1, len(results))


class TestLedgerAggregateTp(unittest.TestCase):
    """AC-39 end to end through CellLedger: one aggregate TP over the DUST of two cycles of one cell."""

    def two_dust_cycles(self):
        r = rules(step="1", min_base="5", min_notional="0")
        h = Harness(r=r)
        e0 = h.entry()
        h.fill(e0, "6")
        h.ledger.set_state(e0, OrderState.CANCEL_PENDING)
        h.ledger.set_state(e0, OrderState.TERMINAL_UNKNOWN)
        h.ledger.confirm_terminal(e0, D("6"))
        _, (tp0,) = h.dispatch_tps()
        h.fill(tp0, "6")
        h.ledger.confirm_terminal(tp0, D("6"))
        h.ledger.release(True)
        e1 = h.entry()
        h.fill(e1, "3")
        h.ledger.set_state(e1, OrderState.CANCEL_PENDING)
        h.ledger.set_state(e1, OrderState.TERMINAL_UNKNOWN)
        h.ledger.confirm_terminal(e1, D("3"))
        self.assertEqual({1: D("3")}, h.ledger.refresh_dust(r))                  # alone: DUST 3 < 5
        self.assertEqual(FillOutcome.LATE_EVIDENCE, h.fill(e0, "2")[0])         # gen 0 re-opened by 2
        h.ledger.acknowledge_late_evidence(0)
        return h, r

    def test_ac39_ledger_dispatches_one_aggregate_and_splits_fills_exactly(self):
        h, r = self.two_dust_cycles()
        plan = h.ledger.tp_obligation_to_dispatch(r)
        (item,) = plan.items
        self.assertEqual((1, D("5"), ((0, D("2")), (1, D("3")))), (item.generation, item.qty, item.allocation))
        self.assertEqual(D("0"), plan.dust)                                     # aggregatable: not DUST
        self.assertEqual({1: D("0")}, h.ledger.refresh_dust(r))
        leg = h.ledger.add_tp_intent(item.qty, 777, r, generation=item.generation, allocation=item.allocation)
        self.assertEqual({0: D("2"), 1: D("3")}, leg.allocation)
        h.ledger.record_transport(leg.identity, TransportResult(TransportOutcome.ACCEPTED, exchange_order_id="x777"))
        before = h.ledger.to_record()
        b0, b1 = (c.buckets() for c in h.ledger.cycles)
        self.assertEqual((D("2"), D("3")), (b0.live_tp_remainder, b1.live_tp_remainder))
        self.assertEqual((), h.ledger.tp_obligation_to_dispatch(r).items)       # no duplicate TP
        out, k1 = h.fill(leg.identity, "1")
        self.assertEqual(FillOutcome.APPLIED, out)
        self.assertEqual((D("7"), D("0")), (h.ledger.cycles[0].X, h.ledger.cycles[1].X))
        h.fill(leg.identity, "3")
        self.assertEqual((D("8"), D("2")), (h.ledger.cycles[0].X, h.ledger.cycles[1].X))
        self.assertEqual(FillOutcome.DUPLICATE, h.ledger.apply_fill(k1, leg.identity, D("1"), leg.price, leg.side))
        self.assertEqual(FillOutcome.CONFLICT_KEY,
                         h.ledger.apply_fill(k1, leg.identity, D("2"), leg.price, leg.side))
        h.fill(leg.identity, "1")
        self.assertEqual(TerminalOutcome.TERMINAL, h.ledger.confirm_terminal(leg.identity, D("5")))
        for c in h.ledger.cycles:
            self.assertEqual(c.E, c.X)
        self.assertEqual([], h.ledger.check_invariants())
        # Persistence: allocation survives the round trip; replay is idempotent.
        restored = CellLedger.from_record(json.loads(json.dumps(h.ledger.to_record())))
        self.assertEqual(h.ledger.to_record(), restored.to_record())
        self.assertEqual([c.X for c in h.ledger.cycles], [c.X for c in restored.cycles])
        # Order independence: the same executions in any order give the same per-cycle split.
        fills = [(("t", "a"), D("1")), (("t", "b"), D("3")), (("t", "c"), D("1"))]
        splits = set()
        for perm in itertools.permutations(fills):
            replica = CellLedger.from_record(json.loads(json.dumps(before)))
            for key, q in perm:
                replica.apply_fill(key, leg.identity, q, leg.price, leg.side)
            splits.add(tuple(c.X for c in replica.cycles))
        self.assertEqual({(D("8"), D("3"))}, splits)
        self.assertTrue(h.ledger.can_release(True).ok)
        h.ledger.release(True)
        self.assertEqual(2, h.ledger.next_entry_identity().generation)

    def test_aggregate_leg_is_visible_and_checked_by_invariants(self):
        h, r = self.two_dust_cycles()
        (item,) = h.ledger.tp_obligation_to_dispatch(r).items
        leg = h.ledger.add_tp_intent(item.qty, 778, r, allocation=item.allocation)
        self.assertEqual([], h.ledger.check_invariants())
        self.assertEqual({"0": "2", "1": "3"}, h.ledger.to_view()["tp_children"][0]["allocation"])
        leg.allocation[0] = D("3")                                      # corrupt: shares no longer sum to qty
        self.assertTrue(any("allocation" in v for v in h.ledger.check_invariants()))

    def test_aggregate_intent_is_validated_against_each_cycle(self):
        h, r = self.two_dust_cycles()
        with self.assertRaises(LedgerError):                                    # gen 0 owes only 2
            h.ledger.add_tp_intent(D("5"), 900, r, allocation={0: D("3"), 1: D("2")})
        with self.assertRaises(LedgerError):                                    # sum != qty
            h.ledger.add_tp_intent(D("5"), 901, r, allocation={0: D("2"), 1: D("2")})
        with self.assertRaises(LedgerError):                                    # unknown generation
            h.ledger.add_tp_intent(D("5"), 902, r, allocation={0: D("2"), 7: D("3")})
        with self.assertRaises(LedgerError):                                    # host must be newest gen
            h.ledger.add_tp_intent(D("5"), 903, r, generation=0, allocation={0: D("2"), 1: D("3")})


class TestDustVisibility(unittest.TestCase):
    def test_ac33_collect_shows_durable_dust_per_cell_cycle(self):
        h = Harness(r=rules(min_base="5", min_notional="0"))
        e = h.entry()
        h.fill(e, "8")
        h.dispatch_tps()
        h.fill(e, "2")
        h.ledger.confirm_terminal(e, D("10"))
        h.ledger.refresh_dust(h.rules)
        (found,) = dust.collect([h.ledger])
        self.assertEqual((h.ledger.cell_id, 0, Side.SELL, h.ledger.spec.tp_price, D("2")),
                         (found.cell_id, found.generation, found.side, found.target, found.qty))
        self.assertEqual(D("2"), dust.dust_total([found]))
        self.assertIn("DUST", h.ledger.can_release(True).reasons)


if __name__ == "__main__":
    unittest.main()
