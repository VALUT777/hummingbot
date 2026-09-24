"""Shared fixtures for neutral-grid core tests (sample profile of the spec, not hard-coded product values)."""
from decimal import Decimal
from itertools import count
from typing import Optional

from hummingbot.strategy_v2.executors.neutral_grid_executor.cells import CellLedger
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    LegRole,
    OrderState,
    CellSpec,
    GridConfig,
    LegIdentity,
    OrderTypePolicy,
    Side,
    TradingRules,
    TransportOutcome,
    TransportResult,
)

D = Decimal
GRID_ID = "g1"
Q = D("10")


def rules(tick="0.0001", step="0.01", min_base="5", min_notional="10", max_base=None, max_leverage="20",
          venue_cap: Optional[int] = None, supports_limit=True, supports_post_only=True) -> TradingRules:
    return TradingRules(
        tick_size=D(tick), size_step=D(step), min_base=D(min_base), min_notional=D(min_notional),
        max_base=None if max_base is None else D(max_base),
        max_leverage=None if max_leverage is None else D(max_leverage),
        supports_limit=supports_limit, supports_post_only=supports_post_only, fetched_at=0.0,
        max_active_orders_venue=venue_cap)


def config(**overrides) -> GridConfig:
    base = dict(
        grid_id=GRID_ID, connector_name="lighter_perpetual_robinhood", trading_pair="LIT-USDG", account_index=7,
        lower_price=D("5"), upper_price=D("6"), cell_count=55, order_amount_base=Q, leverage=D("5"),
        expected_initial_position=D("0"), max_abs_net_position=D("1000"), max_gross_position=D("1000"),
        max_active_orders=120)
    base.update(overrides)
    return GridConfig(**base)


BUY_CELL = CellSpec(cell_id=3, low_price=D("5.0545"), high_price=D("5.0727"), entry_side=Side.BUY)
SELL_CELL = CellSpec(cell_id=40, low_price=D("5.7272"), high_price=D("5.7454"), entry_side=Side.SELL)


class Harness:
    """Drives one CellLedger the way the engine would, with monotonic CIDs and canonical fill keys."""

    def __init__(self, spec: CellSpec = BUY_CELL, q: Decimal = Q, r: Optional[TradingRules] = None):
        self.ledger = CellLedger(GRID_ID, spec, q)
        self.rules = r or rules()
        self._cid = count(1000)
        self._trade = count(1)

    def entry(self, accept=True) -> LegIdentity:
        leg = self.ledger.begin_entry(next(self._cid), self.rules, order_type=OrderTypePolicy.LIMIT_MAKER)
        if accept:
            self.ledger.record_transport(leg.identity, TransportResult(TransportOutcome.ACCEPTED,
                                                                       exchange_order_id=f"x{leg.cid}"))
        return leg.identity

    def dispatch_tps(self, accept=True):
        plan = self.ledger.tp_obligation_to_dispatch(self.rules)
        out = []
        for item in plan.items:
            leg = self.ledger.add_tp_intent(item.qty, next(self._cid), self.rules, generation=item.generation,
                                            order_type=OrderTypePolicy.LIMIT)
            if accept:
                self.ledger.record_transport(leg.identity, TransportResult(TransportOutcome.ACCEPTED,
                                                                           exchange_order_id=f"x{leg.cid}"))
            out.append(leg.identity)
        return plan, out

    def fill(self, identity: LegIdentity, qty: str, key=None):
        leg = self.ledger.find_leg(identity)[1]
        key = key if key is not None else ("lighter", 7, 5, str(next(self._trade)), leg.side.value,
                                           f"x{leg.cid}")
        return self.ledger.apply_fill(key, identity, D(qty), leg.price, leg.side), key

    def leg(self, identity: LegIdentity):
        return self.ledger.find_leg(identity)[1]


class CoreSim:
    """Deterministic engine-like driver built *only* from the public core API (admission -> router -> ledger).

    It models what WS-D must do each tick, with no IO: transport outcomes are chosen by the test.
    """

    def __init__(self, cells, q=Q, r=None, cap=120, mid=D("5.4"), baseline=D("0"),
                 max_abs=D("1000"), max_gross=D("1000")):
        from hummingbot.strategy_v2.executors.neutral_grid_executor import admission, risk, router
        from hummingbot.strategy_v2.executors.neutral_grid_executor.cid import CidAllocator
        self.admission, self.risk, self.router = admission, risk, router
        self.rules = r or rules()
        self.q = q
        self.cap = cap
        self.mid = mid
        self.baseline = baseline
        self.limits = risk.RiskLimits(max_abs, max_gross)
        self.ledgers = {c.cell_id: CellLedger(GRID_ID, c, q) for c in cells}
        self.alloc = CidAllocator(exists=lambda cid: False)
        self.reservations = {}
        self.entries_allowed = True
        self.transport = "ACCEPTED"           # outcome for new submits: ACCEPTED | UNKNOWN
        self._seq = count(1)
        self._obligation_seq = {}
        self._trade = count(1)
        self.by_cid = {}
        self.history = []                     # every applied fill: (key, identity, qty, price, side)
        self.last_admission = None
        self.last_router = None

    # ---------------------------------------------------------------- views
    def legs(self):
        return [leg for L in self.ledgers.values() for leg in L.legs()]

    def non_final(self):
        return [leg for L in self.ledgers.values() for leg in L.non_final_legs()]

    def position(self):
        pos = self.baseline
        for leg in self.legs():
            pos += leg.filled if leg.side == Side.BUY else -leg.filled
        return pos

    def endpoints(self):
        return self.risk.endpoints_from_ledgers(self.baseline, list(self.ledgers.values()))

    def endpoints_with_obligations(self):
        return self.risk.with_obligations(self.endpoints(), *self.risk.obligation_totals(list(self.ledgers.values())))

    def admission_views(self):
        from hummingbot.strategy_v2.executors.neutral_grid_executor.admission import CellAdmission, slot_need_from_ledger
        views = []
        for cid, L in sorted(self.ledgers.items()):
            open_ = bool(L.open_cycles())
            entry_live = any(not e.is_final for c in L.open_cycles() for e in c.entries)
            views.append(CellAdmission(cell=L.spec, open_cycle=open_, actual_orders=len(L.non_final_legs()),
                                       reserved_slots=self.reservations.get(cid, 0),
                                       slot_need=slot_need_from_ledger(L, self.rules) if open_ else None,
                                       entry_live=entry_live))
        return views

    # ---------------------------------------------------------------- one tick
    def tick(self):
        adm = self.admission.plan(self.admission_views(), self.cap, self.mid, self.rules, self.q,
                                  entries_allowed=self.entries_allowed)
        self.last_admission = adm
        self.reservations = {k: v for k, v in adm.reservations.items()}
        candidates = []
        meta = {}
        for cid, L in sorted(self.ledgers.items()):
            plan = L.tp_obligation_to_dispatch(self.rules)
            for n, item in enumerate(plan.items):
                seq = self._obligation_seq.setdefault((cid, item.generation, len(L._cycle_for_tp(item.generation).tps) + n),
                                                      next(self._seq))
                key = f"tp:{cid}:{item.generation}:{n}"
                candidates.append(self.router.RouterIntent(key, L.spec.tp_side, L.spec.tp_price, item.qty, LegRole.TP,
                                                           cid, seq))
                meta[key] = (cid, item)
        for cid in adm.newly_armed:
            L = self.ledgers[cid]
            key = f"entry:{cid}"
            candidates.append(self.router.RouterIntent(key, L.spec.entry_side, L.spec.entry_price, self.q,
                                                       LegRole.ENTRY, cid, next(self._seq)))
            meta[key] = (cid, None)
        owned = [self.router.RouterOrder.from_leg(leg) for leg in self.non_final()]
        rplan = self.router.plan_submits(candidates, owned, endpoints=self.endpoints(), limits=self.limits,
                                         slots=self.router.SlotBudget.from_plan(adm), mid=self.mid,
                                         entries_allowed=self.entries_allowed,
                                         owed=self.risk.obligation_totals(list(self.ledgers.values())))
        self.last_router = rplan
        for action in rplan.actions:
            if action.kind == self.router.ActionKind.SUBMIT:
                cid, item = meta[action.key]
                L = self.ledgers[cid]
                if item is None:
                    ident = L.next_entry_identity()
                    leg = L.begin_entry(self.alloc.allocate(ident), self.rules, order_type=OrderTypePolicy.LIMIT_MAKER,
                                        seq=next(self._seq))
                else:
                    ident = L.next_tp_identity(item.generation)
                    leg = L.add_tp_intent(item.qty, self.alloc.allocate(ident), self.rules, generation=item.generation,
                                          order_type=OrderTypePolicy.LIMIT, seq=next(self._seq))
                self.by_cid[leg.cid] = (cid, leg.identity)
                outcome = TransportOutcome.ACCEPTED if self.transport == "ACCEPTED" else TransportOutcome.UNKNOWN
                L.record_transport(leg.identity, TransportResult(outcome, exchange_order_id=f"x{leg.cid}"
                                                                 if outcome == TransportOutcome.ACCEPTED else None))
            elif action.kind == self.router.ActionKind.CANCEL:
                cid, ident = self.by_cid[int(action.key)]
                self.ledgers[cid].set_state(ident, OrderState.CANCEL_PENDING)
            elif action.kind == self.router.ActionKind.WITHDRAW:
                cid, ident = self.by_cid[int(action.key)]
                self.ledgers[cid].record_transport(ident, TransportResult(TransportOutcome.NOT_SENT))
        for cid, L in self.ledgers.items():
            L.refresh_dust(self.rules)
            if L.current is not None and L.can_release(True).ok:
                L.release(True)
                self.reservations.pop(cid, None)
        return rplan

    # ---------------------------------------------------------------- venue events (history-proven)
    def fill(self, leg, qty):
        key = ("lighter", 7, 5, str(next(self._trade)), leg.side.value, f"x{leg.cid}")
        L = self.ledgers[leg.identity.cell_id]
        out = L.apply_fill(key, leg.identity, qty, leg.price, leg.side)
        if out.applied:
            self.history.append((key, leg.identity, qty, leg.price, leg.side))
        return out

    def prove_terminal(self, leg):
        return self.ledgers[leg.identity.cell_id].confirm_terminal(leg.identity, leg.filled)
