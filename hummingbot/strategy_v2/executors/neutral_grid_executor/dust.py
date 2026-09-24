"""DUST visibility and same side+target aggregation (NG-GRID-003, AC-33, AC-39).

A DUST lot is an exact TP obligation remainder that cannot form a valid order under current rules after its
entry became final. It stays durable and operator-visible and blocks the cell's reset (see ``cells``).

Aggregation is allowed only for lots with the *same TP side and the same fixed target price*. The aggregate
order quantity is ``quantize_down(sum, step)`` (never rounded up, never priced differently, never MARKET) and is
allocated exactly to lots in deterministic ``(cell_id, generation)`` order. Fills of the aggregate are
distributed by water-filling cumulative quantity over that order, so per-lot totals depend only on the set of
applied fills (idempotent by canonical key, order independent).

Note on geometry: with fixed first sides, BUY cell ``i`` has TP SELL@P[i+1] and SELL cell ``j`` has TP BUY@P[j],
so two *different* cells never share (TP side, target). A multi-lot group can therefore only arise from several
cycles of one cell (late evidence re-opening an old cycle) - the functions below stay generic.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, Hashable, Iterable, List, Optional, Sequence, Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import Side, TradingRules
from hummingbot.strategy_v2.executors.neutral_grid_executor.grid import partition_valid, require_decimal

ZERO = Decimal("0")

LotKey = Tuple[int, int]      # (cell_id, generation)


@dataclass(frozen=True)
class DustLot:
    cell_id: int
    generation: int
    side: Side                # TP side of the obligation
    target: Decimal           # fixed TP target price
    qty: Decimal

    @property
    def key(self) -> LotKey:
        return (self.cell_id, self.generation)


@dataclass(frozen=True)
class DustGroup:
    side: Side
    target: Decimal
    lots: Tuple[DustLot, ...]

    @property
    def total(self) -> Decimal:
        return sum((lot.qty for lot in self.lots), ZERO)


@dataclass(frozen=True)
class AggregatePlan:
    side: Side
    target: Decimal
    qty: Decimal                               # valid order quantity (step multiple, >= min)
    allocation: Tuple[Tuple[LotKey, Decimal], ...]   # exact per-lot allocation, sums to qty
    residual: Tuple[Tuple[LotKey, Decimal], ...]     # what stays DUST per lot (on the group's last plan)


def collect(ledgers: Iterable) -> List[DustLot]:
    """Visible DUST lots from ``cells.CellLedger`` objects (uses the durable per-cycle ``dust`` field)."""
    lots: List[DustLot] = []
    for ledger in ledgers:
        for cycle in ledger.open_cycles():
            dust = cycle.buckets().dust
            if dust > 0:
                lots.append(DustLot(cell_id=ledger.cell_id, generation=cycle.generation, side=ledger.spec.tp_side,
                                    target=ledger.spec.tp_price, qty=dust))
    return lots


def dust_total(lots: Iterable[DustLot]) -> Decimal:
    return sum((lot.qty for lot in lots), ZERO)


def group(lots: Sequence[DustLot]) -> List[DustGroup]:
    """Group strictly by (side, exact target price); deterministic ordering."""
    seen = set()
    buckets: Dict[Tuple[Side, Decimal], List[DustLot]] = {}
    for lot in lots:
        require_decimal(lot.qty, "dust qty")
        require_decimal(lot.target, "dust target")
        if lot.qty <= 0:
            raise ValueError(f"dust lot {lot.key} must be positive")
        if (lot.key, lot.side) in seen:
            raise ValueError(f"duplicate dust lot {lot.key}")
        seen.add((lot.key, lot.side))
        buckets.setdefault((lot.side, lot.target), []).append(lot)
    groups = [DustGroup(side=s, target=t, lots=tuple(sorted(v, key=lambda x: x.key)))
              for (s, t), v in buckets.items()]
    groups.sort(key=lambda g: (g.side.value, g.target))
    return groups


def aggregate(lots: Sequence[DustLot], rules: TradingRules) -> List[AggregatePlan]:
    """Aggregate orders that are valid under ``rules``; what still cannot form one stays DUST.

    A group above ``max_base`` becomes several balanced valid orders (``grid.partition_valid``); lots are
    consumed in ``(cell_id, generation)`` order and each order carries its exact per-lot allocation.
    """
    plans = []
    for g in group(lots):
        chunks = partition_valid(g.total, g.target, rules)
        if not chunks:
            continue
        left = {lot.key: lot.qty for lot in g.lots}
        order = [lot.key for lot in g.lots]
        for chunk in chunks:
            need = chunk
            allocation: List[Tuple[LotKey, Decimal]] = []
            for key in order:
                take = min(left[key], need)
                if take > 0:
                    allocation.append((key, take))
                    left[key] -= take
                    need -= take
                if need == 0:
                    break
            plans.append(AggregatePlan(side=g.side, target=g.target, qty=chunk, allocation=tuple(allocation),
                                       residual=()))
        residual = tuple((key, left[key]) for key in order if left[key] > 0)
        plans[-1] = AggregatePlan(side=plans[-1].side, target=plans[-1].target, qty=plans[-1].qty,
                                  allocation=plans[-1].allocation, residual=residual)
    return plans


@dataclass
class AggregateTp:
    """Fill distribution for one aggregate TP order (exact, idempotent, order independent)."""
    side: Side
    target: Decimal
    allocation: Tuple[Tuple[LotKey, Decimal], ...]
    fills: Dict[Hashable, Decimal] = field(default_factory=dict)

    @classmethod
    def from_plan(cls, plan: AggregatePlan) -> "AggregateTp":
        return cls(side=plan.side, target=plan.target, allocation=plan.allocation)

    @property
    def qty(self) -> Decimal:
        return sum((q for _, q in self.allocation), ZERO)

    @property
    def filled(self) -> Decimal:
        return sum(self.fills.values(), ZERO)

    def per_lot_filled(self) -> Dict[LotKey, Decimal]:
        left = self.filled
        out: Dict[LotKey, Decimal] = {}
        for key, cap in self.allocation:
            take = min(cap, left)
            out[key] = take
            left -= take
        return out

    def apply_fill(self, fill_key: Hashable, qty: Decimal) -> Optional[Dict[LotKey, Decimal]]:
        """Apply one execution; returns the per-lot *delta* (empty for a duplicate), None on conflict."""
        require_decimal(qty, "qty")
        if fill_key in self.fills:
            return {} if self.fills[fill_key] == qty else None
        if qty <= 0 or self.filled + qty > self.qty:
            return None
        before = self.per_lot_filled()
        self.fills[fill_key] = qty
        after = self.per_lot_filled()
        return {k: after[k] - before[k] for k in after if after[k] != before[k]}
