"""NG-GRID-003 dust; AC-33 (visible, durable, blocks reset, same side+target only), AC-39 (exact idempotent split)."""
import itertools
import unittest

from hummingbot.strategy_v2.executors.neutral_grid_executor import dust, grid
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import Side
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
