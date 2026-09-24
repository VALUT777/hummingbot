"""Seeded randomized property tests through the real engine, fake venue and SQLite store (spec section 13.3).

Random interleavings of partial fills, venue cancels, price moves, transport timeouts, clean restarts and
crashes. After every tick the spec invariants must hold:

* venue net position lies inside the engine's conservative reachable interval: ``P_min <= actual <= P_max``;
* configured net/gross caps are never exceeded by the reachable interval, venue open orders never exceed the cap;
* confirmed quantities never decrease (per CID, across restarts);
* a cell never starts a new cycle while the previous one has an obligation, a non-final order or dust;
* the ledger effect is idempotent: every venue execution of an own order is stored at most once and
  ``verify_ledger()`` is clean;
* no MARKET, no reduce-only, never a second CID for one leg.

``hypothesis`` is not installed in the prepared env; ``NG_PROPERTY_SEEDS`` / ``NG_PROPERTY_STEPS`` raise the sweep.
"""
import os
import random
from decimal import Decimal

import pytest
from ng_engine_harness import Harness

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CommandKind,
    LegRole,
    OrderTypePolicy,
    Side,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import EngineOptions
from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import CancelBehavior, SubmitBehavior
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import SimulatedCrash

D = Decimal
SEEDS = int(os.environ.get("NG_PROPERTY_SEEDS", "12"))
STEPS = int(os.environ.get("NG_PROPERTY_STEPS", "70"))
CAP = D("35")
GROSS = D("60")


class Checker:
    def __init__(self, h: Harness):
        self.h = h
        self.max_filled = {}
        self.generations = {}

    def check(self) -> None:
        h = self.h
        e = h.engine
        if not e.bootstrapped or e.fatal_reason:
            return
        ep = e.endpoints
        if ep is not None:
            venue = h.fx.net_position
            assert ep.P_min <= venue <= ep.P_max, (ep, venue)
            assert ep.P_max <= CAP and ep.P_min >= -CAP, ep
            assert ep.gross_worst <= GROSS, ep
        assert len(h.fx.open_orders(owned=True)) <= h.config.max_active_orders
        for leg in e.all_legs():
            if leg.cid is None:
                continue
            before = self.max_filled.get(leg.cid, D("0"))
            assert leg.filled >= before, (leg.cid, before, leg.filled)     # confirmed quantities never decrease
            self.max_filled[leg.cid] = leg.filled
        for cell_id, ledger in e.cells.items():
            gen = ledger.generation
            previous = self.generations.get(cell_id, gen)
            if gen > previous:
                for cycle in ledger.cycles:
                    if 0 < cycle.generation < gen and not cycle.late_evidence:
                        assert cycle.closed and cycle.E == cycle.X and cycle.dust == 0, (cell_id, cycle)
                        assert all(leg.is_final for leg in cycle.legs)
            self.generations[cell_id] = gen
        stored = [(f.trade_id_str, f.own_side) for f in e.store.fills()]
        assert len(stored) == len(set(stored))
        venue_legs = {(t.trade_id, t.own_side) for t in h.fx.trade_legs}
        assert set(stored) <= venue_legs
        for call in h.fx.submits():
            assert call.request.reduce_only is False
            assert call.request.order_type in (OrderTypePolicy.LIMIT_MAKER, OrderTypePolicy.LIMIT)
        ids = {}
        for call in h.fx.submits():
            identity = e.store.identity_for_cid(call.client_order_id)
            assert identity is not None
            ids.setdefault(identity, set()).add(call.client_order_id)
        assert all(len(v) == 1 for v in ids.values())


def _random_step(h: Harness, rng: random.Random) -> None:
    roll = rng.random()
    own_open = [o for o in h.fx.open_orders(owned=True)]
    if roll < 0.40 and own_open:
        order = rng.choice(own_open)
        step = D("0.1")
        units = int(order.remaining / step)
        qty = step * rng.randint(1, units) if units > 0 else order.remaining
        lag = rng.choice([None, None, 0.0, 2.0, 7.0])
        h.fx.fill(order.client_order_id, qty, history_lag_s=lag)
    elif roll < 0.50 and own_open:
        h.fx.venue_cancel(rng.choice(own_open).client_order_id)
    elif roll < 0.65:
        mid = D("5") + D(rng.randint(-2, 12)) / D("10") + D(rng.randint(0, 9)) / D("100")
        h.fx.set_mid(mid)
    elif roll < 0.70:
        h.fx.script_submit(rng.choice([SubmitBehavior.TIMEOUT_LANDED, SubmitBehavior.TIMEOUT_NOT_LANDED,
                                       SubmitBehavior.REJECT_ZERO_FILL, SubmitBehavior.RAISE_LANDED]))
    elif roll < 0.74:
        h.fx.script_cancel(rng.choice([CancelBehavior.TIMEOUT_LANDED, CancelBehavior.TIMEOUT_NOT_LANDED,
                                       CancelBehavior.NOT_FOUND]))
    elif roll < 0.77:
        h.restart()
    elif roll < 0.80:
        h.hooks.arm(rng.choice(["after_intent_commit", "before_transport", "after_transport",
                                "before_cursor_commit", "mid_history_batch", "before_result_commit",
                                "after_cancel_intent_commit", "before_snapshot_commit"]),
                    skip=rng.randint(0, 2))


@pytest.mark.parametrize("seed", range(SEEDS))
def test_invariants_hold_under_random_fill_cancel_restart_orderings(tmp_path, seed):
    rng = random.Random(seed)
    h = Harness(tmp_path, max_abs_net_position=CAP, max_gross_position=GROSS, max_active_orders=30,
                fx_kwargs={"min_base": D("1"), "min_notional": D("1")},
                options=EngineOptions(stop_uncertain_after_s=10_000.0))
    try:
        h.bootstrap()
        checker = Checker(h)
        for _ in range(STEPS):
            _random_step(h, rng)
            for _ in range(rng.randint(1, 3)):
                try:
                    h.loop.run_until_complete(h.engine.tick())
                except SimulatedCrash:
                    h.crash_restart()
                h.clock.advance(1.0)
                checker.check()
        h.hooks.disarm()
        # Quiesce: stop and let every own order settle; the ledger then equals the venue exactly.
        h.command(CommandKind.STOP, key=f"stop-{seed}")
        for _ in range(120):
            h.tick()
            checker.check()
            if h.engine.is_stopped:
                break
        if h.engine.is_stopped:
            venue_own = {Side.BUY: D("0"), Side.SELL: D("0")}
            for t in h.fx.trade_legs:
                if t.client_order_id in h.engine.order_meta:
                    venue_own[t.own_side] += t.size
            buys, sells = h.engine._confirmed_fills()
            assert (buys, sells) == (venue_own[Side.BUY], venue_own[Side.SELL])
            assert h.engine.endpoints.P == h.fx.net_position
            assert h.engine.store.verify_ledger() == []
    finally:
        h.close()


def test_ledger_replay_is_idempotent_across_restarts(tmp_path):
    h = Harness(tmp_path)
    try:
        h.bootstrap()
        h.tick(2)
        cells = h.buy_cells()
        for c in cells:
            leg = h.live_order(c, LegRole.ENTRY)
            h.fx.fill(leg.cid, D("3"))
            h.fx.fill(leg.cid, D("2"))
        h.tick(8)
        snapshot = {c: (h.cell(c).cycles[-1].E, [t.requested for t in h.legs(c, LegRole.TP)]) for c in cells}
        fills = sorted((f.dedupe_key, f.size) for f in h.engine.store.fills())
        for _ in range(3):
            h.crash_restart()
            h.tick(8)
            assert {c: (h.cell(c).cycles[-1].E, [t.requested for t in h.legs(c, LegRole.TP)]) for c in cells} \
                == snapshot
            assert sorted((f.dedupe_key, f.size) for f in h.engine.store.fills()) == fills
    finally:
        h.close()
