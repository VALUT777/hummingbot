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
from hummingbot.strategy_v2.executors.neutral_grid_executor.grid import (
    min_valid_order_qty,
    order_qty_blocker,
    quantize_down,
    require_decimal,
)

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
    residual: Tuple[Tuple[LotKey, Decimal], ...]     # what stays DUST per lot


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
    """Aggregate orders that are valid under ``rules``; groups that still cannot form one stay DUST."""
    plans = []
    for g in group(lots):
        qty = quantize_down(g.total, rules.size_step)
        if qty <= 0 or qty < min_valid_order_qty(rules, g.target) or order_qty_blocker(qty, g.target, rules):
            continue
        remaining = qty
        allocation: List[Tuple[LotKey, Decimal]] = []
        residual: List[Tuple[LotKey, Decimal]] = []
        for lot in g.lots:
            take = min(lot.qty, remaining)
            remaining -= take
            if take > 0:
                allocation.append((lot.key, take))
            if lot.qty - take > 0:
                residual.append((lot.key, lot.qty - take))
        plans.append(AggregatePlan(side=g.side, target=g.target, qty=qty, allocation=tuple(allocation),
                                   residual=tuple(residual)))
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
