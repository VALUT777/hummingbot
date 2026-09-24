"""Seeded randomized property tests (hypothesis is not installed in the prepared env).

Invariants checked after every event, over random fill/cancel/unknown/terminal/replay/price orderings:

* ``P_min <= actual position <= P_max`` for every outcome reachable before the next routing decision, and the
  interval never widens without a new submit;
* net/gross caps are never violated, even counting every owed TP exit (TP priority), so without external
  drift a TP is never RISK_BLOCKED; slots never oversubscribed;
* confirmed quantities (per leg, E and X per cycle) never decrease;
* a cycle is never closed (reset) with an obligation, a non-final/unknown order or DUST;
* ledger application is idempotent (replays are DUPLICATE and change nothing; record round trip is exact);
* fixed prices/sides, never MARKET, never reduce-only.

Seeds/steps can be raised with NG_CORE_PROPERTY_SEEDS / NG_CORE_PROPERTY_STEPS.
"""
import os
import random
import unittest
from decimal import Decimal

from hummingbot.strategy_v2.executors.neutral_grid_executor import grid
from hummingbot.strategy_v2.executors.neutral_grid_executor.cells import CellLedger, FillOutcome
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    LegRole,
    OrderState,
    OrderTypePolicy,
    TransportOutcome,
    TransportResult,
)

from .helpers import CoreSim, D, rules

SEEDS = int(os.environ.get("NG_CORE_PROPERTY_SEEDS", "60"))
STEPS = int(os.environ.get("NG_CORE_PROPERTY_STEPS", "70"))


def make_sim(rng: random.Random) -> CoreSim:
    r = rules(tick="0.01", step="1", min_base=str(rng.choice([1, 3, 5])), min_notional="0")
    prices = grid.build_grid(D("5"), D("6"), 12, r)
    anchor = D(str(rng.choice(["5.05", "5.3", "5.5", "5.77", "5.95"])))
    cells = grid.assign_cells(prices, anchor)
    baseline = D(rng.randint(-30, 30))
    max_abs = D(rng.choice([45, 70, 1000]))
    max_gross = D(rng.choice([40, 75, 1000]))
    if abs(baseline) > max_abs:
        baseline = D(0)
    sim = CoreSim(cells, q=D("10"), r=r, cap=rng.choice([9, 14, 40]), mid=anchor, baseline=baseline,
                  max_abs=max_abs, max_gross=max_gross)
    return sim


class Checker:
    def __init__(self, tc: unittest.TestCase, sim: CoreSim, strict_risk: bool = True):
        self.tc = tc
        self.sim = sim
        self.strict_risk = strict_risk   # False only after injected late evidence (outside any reachable-set proof)
        self.reopened = set()            # (cell, generation) released cycles re-opened by injected late evidence
        self.filled = {}
        self.cycle_eq = {}
        self.cycles_count = {}
        self.window = None
        self.owed_window = None

    def after_tick(self):
        ep = self.sim.endpoints()
        self.window = (ep.P_min, ep.P_max)
        owed = self.sim.endpoints_with_obligations()
        self.owed_window = (owed.P_min, owed.P_max)
        if self.strict_risk and self.sim.last_router is not None:
            self.tc.assertFalse(self.sim.last_router.risk_blocked, self.sim.last_router.actions)
        self.check()

    def check(self):
        sim, tc = self.sim, self.tc
        ep = sim.endpoints()
        pos = sim.position()
        tc.assertEqual(pos, ep.P)
        tc.assertLessEqual(ep.P_min, pos)
        tc.assertLessEqual(pos, ep.P_max)
        if self.strict_risk:
            self.check_risk(ep, pos)
        self.check_ledgers()

    def check_risk(self, ep, pos):
        sim, tc = self.sim, self.tc
        if self.window is not None:
            lo, hi = self.window
            tc.assertLessEqual(lo, ep.P_min)                  # never widens between routing decisions
            tc.assertLessEqual(ep.P_max, hi)
            tc.assertLessEqual(lo, pos)
            tc.assertLessEqual(pos, hi)
        owed = sim.endpoints_with_obligations()
        tc.assertLessEqual(owed.P_max, sim.limits.max_abs_net_position)
        tc.assertGreaterEqual(owed.P_min, -sim.limits.max_abs_net_position)
        if self.owed_window is not None:
            tc.assertLessEqual(self.owed_window[0], owed.P_min)
            tc.assertLessEqual(owed.P_max, self.owed_window[1])
        tc.assertLessEqual(ep.P_max, sim.limits.max_abs_net_position)
        tc.assertGreaterEqual(ep.P_min, -sim.limits.max_abs_net_position)
        tc.assertLessEqual(ep.gross_worst, sim.limits.max_gross_position)

    def check_ledgers(self):
        sim, tc = self.sim, self.tc
        tc.assertLessEqual(len(sim.non_final()), sim.cap)
        if sim.last_admission is not None:
            tc.assertFalse(sim.last_admission.slots.oversubscribed)
        for cid, ledger in sim.ledgers.items():
            tc.assertEqual([], ledger.check_invariants(), f"cell {cid}")
            tc.assertGreaterEqual(len(ledger.cycles), self.cycles_count.get(cid, 0))
            self.cycles_count[cid] = len(ledger.cycles)
            for cycle in ledger.cycles:
                key = (cid, cycle.generation)
                e, x = cycle.E, cycle.X
                pe, px = self.cycle_eq.get(key, (Decimal(0), Decimal(0)))
                tc.assertGreaterEqual(e, pe)
                tc.assertGreaterEqual(x, px)
                self.cycle_eq[key] = (e, x)
                b = cycle.buckets()
                tc.assertEqual(e - x, b.live_tp_remainder + b.reserved_tp_unassigned + b.unassigned + b.dust)
                for part in (b.live_tp_remainder, b.reserved_tp_unassigned, b.unassigned, b.dust):
                    tc.assertGreaterEqual(part, 0)
                if cycle.closed and key not in self.reopened:
                    tc.assertEqual(e, x)
                    tc.assertTrue(all(leg.is_final for leg in cycle.legs))
                    tc.assertEqual(0, cycle.dust)
            for leg in ledger.legs():
                ident = leg.identity
                tc.assertGreaterEqual(leg.filled, self.filled.get(ident, Decimal(0)))
                self.filled[ident] = leg.filled
                if ident.role == LegRole.ENTRY:
                    tc.assertEqual((ledger.spec.entry_side, ledger.spec.entry_price, sim.q),
                                   (leg.side, leg.price, leg.requested))
                else:
                    tc.assertEqual((ledger.spec.tp_side, ledger.spec.tp_price), (leg.side, leg.price))
                tc.assertIn(leg.order_type, (OrderTypePolicy.LIMIT, OrderTypePolicy.LIMIT_MAKER))
                if leg.cid is not None:
                    tc.assertFalse(ledger.submit_request(ident, leg.order_type).reduce_only)


def random_event(rng: random.Random, sim: CoreSim, checker: Checker, tc: unittest.TestCase):
    kind = rng.choices(["fill", "fill_all", "cancel", "ack", "prove", "resolve_unknown", "expire_tp", "replay",
                        "mid", "pause", "transport"],
                       weights=[30, 8, 6, 8, 14, 6, 4, 6, 4, 2, 3])[0]
    legs = sim.non_final()
    if kind in ("fill", "fill_all"):
        cands = [leg for leg in legs if leg.remaining > 0]
        if cands:
            leg = rng.choice(cands)
            qty = leg.remaining if kind == "fill_all" else D(rng.randint(1, int(leg.remaining)))
            tc.assertEqual(FillOutcome.APPLIED, sim.fill(leg, qty))
    elif kind == "cancel":
        cands = [leg for leg in legs if leg.identity.role == LegRole.ENTRY and leg.state == OrderState.LIVE]
        if cands:
            leg = rng.choice(cands)
            sim.ledgers[leg.identity.cell_id].set_state(leg.identity, OrderState.CANCEL_PENDING)
    elif kind == "ack":
        cands = [leg for leg in legs if leg.state in (OrderState.CANCEL_PENDING, OrderState.CANCEL_UNKNOWN)]
        if cands:
            leg = rng.choice(cands)
            sim.ledgers[leg.identity.cell_id].set_state(leg.identity, OrderState.TERMINAL_UNKNOWN)
    elif kind == "prove":
        cands = [leg for leg in legs if leg.state == OrderState.TERMINAL_UNKNOWN or
                 (leg.remaining == 0 and leg.state == OrderState.LIVE)]
        if cands:
            leg = rng.choice(cands)
            ledger = sim.ledgers[leg.identity.cell_id]
            # History cumulative ahead of applied trades never proves terminal.
            tc.assertEqual("MISSING_FILLS", ledger.confirm_terminal(leg.identity, leg.filled + 1).value
                           if leg.filled < leg.requested else "MISSING_FILLS")
            ledger.confirm_terminal(leg.identity, leg.filled)
    elif kind == "resolve_unknown":
        cands = [leg for leg in legs if leg.state == OrderState.SUBMIT_UNKNOWN]
        if cands:
            leg = rng.choice(cands)
            ledger = sim.ledgers[leg.identity.cell_id]
            if leg.filled == 0 and rng.random() < 0.4:
                ledger.record_transport(leg.identity, TransportResult(TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL))
            else:
                ledger.set_state(leg.identity, OrderState.LIVE)
    elif kind == "expire_tp":
        cands = [leg for leg in legs if leg.identity.role == LegRole.TP and leg.state == OrderState.LIVE]
        if cands:
            leg = rng.choice(cands)
            sim.ledgers[leg.identity.cell_id].set_state(leg.identity, OrderState.TERMINAL_UNKNOWN)
    elif kind == "replay" and sim.history:
        key, ident, qty, price, side = rng.choice(sim.history)
        ledger = sim.ledgers[ident.cell_id]
        before = ledger.to_record()
        tc.assertEqual(FillOutcome.DUPLICATE, ledger.apply_fill(key, ident, qty, price, side))
        tc.assertEqual(before, ledger.to_record())
        tc.assertEqual(FillOutcome.CONFLICT_KEY, ledger.apply_fill(key, ident, qty + 1, price, side))
        tc.assertEqual(before, ledger.to_record())
    elif kind == "mid":
        sim.mid = D(str(rng.choice(["5.02", "5.25", "5.5", "5.71", "5.98"])))
    elif kind == "pause":
        sim.entries_allowed = not sim.entries_allowed
    elif kind == "transport":
        sim.transport = "UNKNOWN" if sim.transport == "ACCEPTED" else "ACCEPTED"
    checker.check()


def drain(sim: CoreSim, checker: Checker, tc: unittest.TestCase):
    """Stop entries, resolve everything and let every obligation complete: no deadlock, no starvation."""
    sim.entries_allowed = False
    sim.transport = "ACCEPTED"
    for _ in range(200):
        for leg in sim.non_final():
            ledger = sim.ledgers[leg.identity.cell_id]
            if leg.state == OrderState.SUBMIT_UNKNOWN:
                ledger.set_state(leg.identity, OrderState.LIVE)
            if leg.identity.role == LegRole.ENTRY and leg.state == OrderState.LIVE and leg.remaining > 0:
                ledger.set_state(leg.identity, OrderState.CANCEL_PENDING)
            elif leg.identity.role == LegRole.TP and leg.remaining > 0:
                sim.fill(leg, leg.remaining)
            if leg.state in (OrderState.CANCEL_PENDING, OrderState.CANCEL_UNKNOWN):
                ledger.set_state(leg.identity, OrderState.TERMINAL_UNKNOWN)
            ledger.confirm_terminal(leg.identity, leg.filled)
        checker.check()
        sim.tick()
        checker.after_tick()
        if not sim.non_final() and all(not L.tp_obligation_to_dispatch(sim.rules).items
                                       for L in sim.ledgers.values()):
            break
    tc.assertEqual([], sim.non_final())
    for ledger in sim.ledgers.values():
        plan = ledger.tp_obligation_to_dispatch(sim.rules)
        tc.assertEqual((), plan.items)
        b = ledger.buckets()
        tc.assertEqual(b.E - b.X, b.dust)                          # only DUST can remain, and it is visible
        if b.dust > 0:
            tc.assertIn("DUST", ledger.can_release(True).reasons)
    tc.assertEqual(sim.position() - sim.baseline,
                   sum((ledger.buckets().dust * (1 if ledger.spec.entry_side.value == "BUY" else -1)
                        for ledger in sim.ledgers.values()), Decimal(0)))


def run_random(tc: unittest.TestCase, rng: random.Random, steps: int):
    sim = make_sim(rng)
    checker = Checker(tc, sim)
    sim.tick()
    checker.after_tick()
    for _ in range(steps):
        for _ in range(rng.randint(1, 4)):
            random_event(rng, sim, checker, tc)
        sim.tick()
        checker.after_tick()
    return sim, checker


class TestCoreProperties(unittest.TestCase):
    def test_late_evidence_is_resolved_by_ordinary_tps_and_never_latches(self):
        """AC-42 (ledger part): late executions of already-final entries (incl. released cycles) are applied to
        their own cycle, flagged for audit, and after the audit closed by ordinary TP legs; no cell stays locked."""
        injected = 0
        for seed in range(max(SEEDS // 2, 1)):
            with self.subTest(seed=seed):
                rng = random.Random(10_000 + seed)
                sim, checker = run_random(self, rng, STEPS // 2)
                finals = [leg for leg in sim.legs() if leg.identity.role == LegRole.ENTRY
                          and leg.state == OrderState.TERMINAL and leg.remaining > 0]
                rng.shuffle(finals)
                late_checker = Checker(self, sim, strict_risk=False)
                for leg in finals[:3]:
                    ledger = sim.ledgers[leg.identity.cell_id]
                    self.assertEqual(FillOutcome.LATE_EVIDENCE, sim.fill(leg, D(rng.randint(1, int(leg.remaining)))))
                    self.assertIn("LATE_EVIDENCE_AUDIT", ledger.can_release(True).reasons)
                    late_checker.reopened.add((leg.identity.cell_id, leg.identity.generation))
                    injected += 1
                for ledger in sim.ledgers.values():
                    for cycle in ledger.cycles:
                        if cycle.late_evidence:
                            ledger.acknowledge_late_evidence(cycle.generation)
                late_checker.check()                            # invariants clean again after the audit
                drain(sim, late_checker, self)
                for ledger in sim.ledgers.values():
                    self.assertFalse(any(c.late_evidence for c in ledger.cycles))
                    for cycle in ledger.cycles:                 # re-opened cycles are closed out again (or DUST)
                        self.assertEqual(cycle.E - cycle.X, cycle.buckets().dust)
                        self.assertTrue(all(leg.is_final for leg in cycle.legs))
                    if ledger.buckets().dust == 0:
                        if ledger.current is not None:
                            self.assertTrue(ledger.can_release(True).ok, ledger.can_release(True).reasons)
                            ledger.release(True)
                        ledger.next_entry_identity()           # the cell can start a new cycle again
        self.assertGreater(injected, 0)

    def test_random_orderings_preserve_invariants(self):
        for seed in range(SEEDS):
            with self.subTest(seed=seed):
                rng = random.Random(seed)
                sim = make_sim(rng)
                checker = Checker(self, sim)
                sim.tick()
                checker.after_tick()
                for _ in range(STEPS):
                    for _ in range(rng.randint(1, 4)):
                        random_event(rng, sim, checker, self)
                    sim.tick()
                    checker.after_tick()
                # Idempotent ledger application: exact round trip and full replay of every applied execution.
                for ledger in sim.ledgers.values():
                    restored = CellLedger.from_record(ledger.to_record())
                    self.assertEqual(ledger.to_record(), restored.to_record())
                for key, ident, qty, price, side in sim.history:
                    self.assertEqual(FillOutcome.DUPLICATE,
                                     sim.ledgers[ident.cell_id].apply_fill(key, ident, qty, price, side))
                drain(sim, checker, self)


if __name__ == "__main__":
    unittest.main()
