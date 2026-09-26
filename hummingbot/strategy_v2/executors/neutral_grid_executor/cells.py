"""Per-cell cycle ledger (NG-CELL-001..004, NG-GRID-003 quantities, AC-05..08, AC-23, AC-33).

A cell owns a list of durable cycles. A cycle has one physical entry order (plus zero-fill rejected retries)
and any number of TP child orders. Every leg carries its own ``OrderState``; the cell aggregate state is
*derived* (``state_flags``/``primary_state``) and never the only truth, so ``ENTRY_LIVE + TP_LIVE`` coexist.

Accounting per cycle (all exact ``Decimal``, never rounded up, fees never touch base quantities):

* ``E`` - confirmed entry quantity (sum of history fills applied to the entry legs);
* ``X`` - confirmed exit quantity (sum of history fills applied to TP children);
* ``E - X - S`` is partitioned into disjoint buckets, where ``S`` is externally settled quantity:
  ``live_tp_remainder`` (proven live TP remainder) + ``reserved_tp_unassigned`` (TP remainder in
  intent/submit-unknown/cancel-unknown/terminal-unknown legs; reservation is kept, no duplicate TP)
  + ``unassigned`` (obligation without a TP child yet, e.g. below-minimum while the entry may still fill)
  + ``dust`` (entry is final and the exact remainder cannot form a valid order).
* invariant: ``X + S + reserved_TP_unfilled <= E`` where ``reserved_TP_unfilled`` is the remainder of non-final
  TP legs.

Pure and deterministic: no IO, no clocks. The engine is the only caller that asserts history proofs.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Hashable, Iterable, List, Optional, Sequence, Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    MAX_CLIENT_ORDER_ID,
    CellSpec,
    CellState,
    LegIdentity,
    LegRole,
    OrderState,
    OrderTypePolicy,
    Side,
    SubmitRequest,
    TradingRules,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.grid import (
    min_valid_order_qty,
    order_qty_blocker,
    partition_valid,
    require_decimal,
    rules_blockers,
)

ZERO = Decimal("0")

FINAL_STATES = frozenset({OrderState.TERMINAL, OrderState.REJECTED_UNSENT, OrderState.REJECTED_ZERO_FILL})
REJECTED_STATES = frozenset({OrderState.REJECTED_UNSENT, OrderState.REJECTED_ZERO_FILL})
# Proven accepted and possibly resting on the book.
LIVE_STATES = frozenset({OrderState.LIVE, OrderState.CANCEL_PENDING})
# Remainder reserved because the order is undispatched or its outcome is not proven.
UNKNOWN_STATES = frozenset({OrderState.INTENT, OrderState.SUBMIT_UNKNOWN, OrderState.CANCEL_UNKNOWN,
                            OrderState.TERMINAL_UNKNOWN})

# Allowed order-level transitions through ``set_state``. TERMINAL is reachable only through
# ``confirm_terminal`` (history proof with cumulative equality), rejections only through ``record_transport``.
_TRANSITIONS: Dict[OrderState, frozenset] = {
    OrderState.INTENT: frozenset({OrderState.SUBMIT_UNKNOWN, OrderState.LIVE, OrderState.TERMINAL_UNKNOWN}),
    OrderState.SUBMIT_UNKNOWN: frozenset({OrderState.LIVE, OrderState.CANCEL_PENDING, OrderState.TERMINAL_UNKNOWN}),
    OrderState.LIVE: frozenset({OrderState.CANCEL_PENDING, OrderState.TERMINAL_UNKNOWN}),
    OrderState.CANCEL_PENDING: frozenset({OrderState.CANCEL_UNKNOWN, OrderState.LIVE, OrderState.TERMINAL_UNKNOWN}),
    OrderState.CANCEL_UNKNOWN: frozenset({OrderState.CANCEL_PENDING, OrderState.LIVE, OrderState.TERMINAL_UNKNOWN}),
    OrderState.TERMINAL_UNKNOWN: frozenset({OrderState.LIVE, OrderState.CANCEL_PENDING}),
    OrderState.TERMINAL: frozenset(),
    OrderState.REJECTED_UNSENT: frozenset(),
    OrderState.REJECTED_ZERO_FILL: frozenset(),
}


class LedgerError(Exception):
    """Programming/ownership error: the requested ledger mutation is illegal."""


class IllegalTransition(LedgerError):
    pass


class FillOutcome(str, Enum):
    APPLIED = "APPLIED"
    DUPLICATE = "DUPLICATE"                  # identical payload for a known canonical key: idempotent no-op
    CONFLICT_KEY = "CONFLICT_KEY"            # same canonical key, different payload -> history conflict, not applied
    CONFLICT_OVERFILL = "CONFLICT_OVERFILL"  # would exceed the leg's requested quantity -> not applied
    CONFLICT_SIDE = "CONFLICT_SIDE"          # evidence side differs from the leg side -> not applied
    CONFLICT_QTY = "CONFLICT_QTY"            # non-positive execution size -> not applied
    UNKNOWN_LEG = "UNKNOWN_LEG"              # identity not owned by this cell -> not applied
    LATE_EVIDENCE = "LATE_EVIDENCE"          # applied to a final leg / released cycle: market must freeze for audit

    @property
    def applied(self) -> bool:
        return self in (FillOutcome.APPLIED, FillOutcome.LATE_EVIDENCE)

    @property
    def is_conflict(self) -> bool:
        return self in (FillOutcome.CONFLICT_KEY, FillOutcome.CONFLICT_OVERFILL, FillOutcome.CONFLICT_SIDE,
                        FillOutcome.CONFLICT_QTY, FillOutcome.UNKNOWN_LEG)


class TerminalOutcome(str, Enum):
    TERMINAL = "TERMINAL"            # leg is now TERMINAL with proven cumulative
    DUPLICATE = "DUPLICATE"          # already TERMINAL with the same cumulative
    MISSING_FILLS = "MISSING_FILLS"  # venue cumulative > applied fills: wait for trades, not terminal
    CONFLICT = "CONFLICT"            # applied fills > venue cumulative, cumulative > requested, or re-terminal mismatch
    UNKNOWN_LEG = "UNKNOWN_LEG"


@dataclass(frozen=True)
class FillRecord:
    identity: LegIdentity
    qty: Decimal
    price: Decimal
    side: Optional[Side]

    def same_payload(self, other: "FillRecord") -> bool:
        return (self.identity == other.identity and self.qty == other.qty and self.price == other.price
                and self.side == other.side)


@dataclass
class Leg:
    """One physical order (entry or TP child) with its own order-level state."""
    identity: LegIdentity
    side: Side
    price: Decimal
    requested: Decimal
    state: OrderState = OrderState.INTENT
    cid: Optional[int] = None
    exchange_order_id: Optional[str] = None
    filled: Decimal = ZERO
    terminal_cumulative: Optional[Decimal] = None
    order_type: Optional[OrderTypePolicy] = None
    expiry_ms: Optional[int] = None
    seq: int = 0                       # engine-provided global FIFO sequence (outbox order)
    late_evidence: bool = False
    # Aggregate TP only: generation -> exact share of ``requested`` (same cell, same side+target). The leg lives
    # in its newest cycle; fills are split over the allocation by water-filling in generation order.
    allocation: Optional[Dict[int, Decimal]] = None

    @property
    def role(self) -> LegRole:
        return self.identity.role

    @property
    def remaining(self) -> Decimal:
        """Possibly executable remainder: full submitted remainder until terminal is proven."""
        return self.requested - self.filled

    @property
    def is_final(self) -> bool:
        return self.state in FINAL_STATES

    @property
    def possibly_executable(self) -> bool:
        return not self.is_final and self.remaining > 0

    def allocated_filled(self) -> Dict[int, Decimal]:
        """Per-generation share of ``filled`` for an aggregate TP (depends only on the cumulative fill)."""
        left = self.filled
        out: Dict[int, Decimal] = {}
        for gen in sorted(self.allocation or {}):
            take = min(self.allocation[gen], left)
            out[gen] = take
            left -= take
        return out

    def to_record(self) -> Dict[str, Any]:
        return {
            "identity": _identity_to_record(self.identity),
            "side": self.side.value,
            "price": str(self.price),
            "requested": str(self.requested),
            "state": self.state.value,
            "cid": self.cid,
            "exchange_order_id": self.exchange_order_id,
            "filled": str(self.filled),
            "terminal_cumulative": None if self.terminal_cumulative is None else str(self.terminal_cumulative),
            "order_type": None if self.order_type is None else self.order_type.value,
            "expiry_ms": self.expiry_ms,
            "seq": self.seq,
            "late_evidence": self.late_evidence,
            "allocation": None if self.allocation is None else {str(g): str(q) for g, q in sorted(self.allocation.items())},
        }

    @classmethod
    def from_record(cls, rec: Dict[str, Any]) -> "Leg":
        return cls(
            identity=_identity_from_record(rec["identity"]),
            side=Side(rec["side"]),
            price=Decimal(rec["price"]),
            requested=Decimal(rec["requested"]),
            state=OrderState(rec["state"]),
            cid=_opt_int(rec.get("cid")),
            exchange_order_id=rec.get("exchange_order_id"),
            filled=Decimal(rec["filled"]),
            terminal_cumulative=None if rec.get("terminal_cumulative") is None else Decimal(rec["terminal_cumulative"]),
            order_type=None if rec.get("order_type") is None else OrderTypePolicy(rec["order_type"]),
            expiry_ms=_opt_int(rec.get("expiry_ms")),
            seq=int(rec.get("seq", 0)),
            late_evidence=bool(rec.get("late_evidence", False)),
            allocation=_allocation_from_record(rec.get("allocation")),
        )


@dataclass(frozen=True)
class Buckets:
    """Disjoint partition of the open obligation ``E - X - S`` of one cycle (or a sum over cycles)."""
    E: Decimal
    X: Decimal
    live_tp_remainder: Decimal
    reserved_tp_unassigned: Decimal
    unassigned: Decimal
    dust: Decimal
    external_settled: Decimal = ZERO
    external_entered: Decimal = ZERO

    @property
    def open_obligation(self) -> Decimal:
        return self.E - self.X - self.external_settled

    @property
    def reserved_tp_unfilled(self) -> Decimal:
        return self.live_tp_remainder + self.reserved_tp_unassigned

    def __add__(self, other: "Buckets") -> "Buckets":
        return Buckets(*(a + b for a, b in zip(self._tuple(), other._tuple())))

    def _tuple(self) -> Tuple[Decimal, ...]:
        return (self.E, self.X, self.live_tp_remainder, self.reserved_tp_unassigned, self.unassigned, self.dust,
                self.external_settled, self.external_entered)


EMPTY_BUCKETS = Buckets(ZERO, ZERO, ZERO, ZERO, ZERO, ZERO, ZERO, ZERO)


@dataclass(frozen=True)
class TpDispatchItem:
    generation: int                     # cycle hosting the new TP leg (the newest one for an aggregate)
    qty: Decimal
    allocation: Optional[Tuple[Tuple[int, Decimal], ...]] = None   # aggregate TP: (generation, share) pairs


@dataclass(frozen=True)
class TpDispatchPlan:
    """What may be dispatched as new TP children right now (exact quantities, each a valid order)."""
    items: Tuple[TpDispatchItem, ...]
    unassigned: Decimal          # total obligation without a TP child (incl. dust), before this plan
    pending_below_min: Decimal   # held because the entry may still fill (not dust yet)
    dust: Decimal                # entry final and remainder cannot form a valid order
    blocker: Optional[str]

    @property
    def quantities(self) -> Tuple[Decimal, ...]:
        return tuple(i.qty for i in self.items)


@dataclass(frozen=True)
class ReleaseCheck:
    ok: bool
    reasons: Tuple[str, ...]


@dataclass
class Cycle:
    generation: int
    entry_side: Side
    planned_qty: Decimal
    entries: List[Leg] = field(default_factory=list)
    tps: List[Leg] = field(default_factory=list)
    dust: Decimal = ZERO            # durable, operator-visible DUST classification (<= unassigned)
    closed: bool = False
    late_evidence: bool = False
    external_settled: Decimal = ZERO
    external_entered: Decimal = ZERO
    # All cycles of the owning cell (set by CellLedger): needed to see aggregate TP legs hosted in a newer cycle.
    peers: Optional[List["Cycle"]] = field(default=None, repr=False, compare=False)

    @property
    def legs(self) -> List[Leg]:
        return self.entries + self.tps

    def tp_parts(self) -> List[Tuple[Leg, Decimal, Decimal]]:
        """``(leg, requested share, filled share)`` of every TP leg that serves this cycle's obligation."""
        parts = []
        for c in (self.peers if self.peers is not None else [self]):
            for t in c.tps:
                if t.allocation is None:
                    if c is self:
                        parts.append((t, t.requested, t.filled))
                elif self.generation in t.allocation:
                    parts.append((t, t.allocation[self.generation], t.allocated_filled()[self.generation]))
        return parts

    @property
    def E(self) -> Decimal:
        return sum((e.filled for e in self.entries), ZERO) + self.external_entered

    @property
    def X(self) -> Decimal:
        return sum((filled for _, _, filled in self.tp_parts()), ZERO)

    @property
    def effective_exit(self) -> Decimal:
        return self.X + self.external_settled

    @property
    def open_obligation(self) -> Decimal:
        return self.E - self.effective_exit

    @property
    def entry_final(self) -> bool:
        return (bool(self.entries) or self.external_entered > ZERO) and all(e.is_final for e in self.entries)

    @property
    def active_entry(self) -> Optional[Leg]:
        for e in self.entries:
            if e.state not in REJECTED_STATES:
                return e
        return None

    def unassigned_total(self) -> Decimal:
        """``E - X - (remainder of non-final TP legs)``; includes dust. Negative means invariant violation."""
        reserved = sum((req - filled for t, req, filled in self.tp_parts() if not t.is_final), ZERO)
        return self.open_obligation - reserved

    def buckets(self) -> Buckets:
        parts = self.tp_parts()
        live = sum((req - filled for t, req, filled in parts if t.state in LIVE_STATES), ZERO)
        unknown = sum((req - filled for t, req, filled in parts if t.state in UNKNOWN_STATES), ZERO)
        total_unassigned = self.open_obligation - live - unknown
        dust = min(self.dust, max(total_unassigned, ZERO))
        return Buckets(E=self.E, X=self.X, live_tp_remainder=live, reserved_tp_unassigned=unknown,
                       unassigned=total_unassigned - dust, dust=dust,
                       external_settled=self.external_settled, external_entered=self.external_entered)

    def has_open_obligation_or_orders(self) -> bool:
        return (self.open_obligation != 0 or any(not leg.is_final for leg in self.legs) or self.dust > 0
                or any(not t.is_final for t, _, _ in self.tp_parts()))

    def to_record(self) -> Dict[str, Any]:
        return {
            "generation": self.generation,
            "entry_side": self.entry_side.value,
            "planned_qty": str(self.planned_qty),
            "entries": [e.to_record() for e in self.entries],
            "tps": [t.to_record() for t in self.tps],
            "dust": str(self.dust),
            "closed": self.closed,
            "late_evidence": self.late_evidence,
            "external_settled": str(self.external_settled),
            "external_entered": str(self.external_entered),
        }

    @classmethod
    def from_record(cls, rec: Dict[str, Any]) -> "Cycle":
        return cls(
            generation=int(rec["generation"]),
            entry_side=Side(rec["entry_side"]),
            planned_qty=Decimal(rec["planned_qty"]),
            entries=[Leg.from_record(r) for r in rec["entries"]],
            tps=[Leg.from_record(r) for r in rec["tps"]],
            dust=Decimal(rec.get("dust", "0")),
            closed=bool(rec.get("closed", False)),
            late_evidence=bool(rec.get("late_evidence", False)),
            external_settled=Decimal(rec.get("external_settled", "0")),
            external_entered=Decimal(rec.get("external_entered", "0")),
        )


class CellLedger:
    """Durable per-cell model. ``to_record``/``from_record`` round-trip exactly (Decimals and ids as strings)."""

    def __init__(self, grid_id: str, spec: CellSpec, order_amount_base: Decimal):
        require_decimal(order_amount_base, "order_amount_base")
        if order_amount_base <= 0:
            raise LedgerError("order_amount_base must be positive")
        self.grid_id = grid_id
        self.spec = spec
        self.order_amount_base = order_amount_base
        self.cycles: List[Cycle] = []
        self.fills: Dict[Hashable, FillRecord] = {}

    # ------------------------------------------------------------------ lookup
    @property
    def cell_id(self) -> int:
        return self.spec.cell_id

    @property
    def current(self) -> Optional[Cycle]:
        """The open (not yet released) cycle, if any."""
        if self.cycles and not self.cycles[-1].closed:
            return self.cycles[-1]
        return None

    @property
    def generation(self) -> int:
        """Generation of the current/last cycle (``-1`` before the first cycle)."""
        return self.cycles[-1].generation if self.cycles else -1

    def open_cycles(self) -> List[Cycle]:
        """Cycles that still carry exposure: the current one plus released cycles re-opened by late evidence."""
        return [c for c in self.cycles if not c.closed or c.has_open_obligation_or_orders()]

    def find_leg(self, identity: LegIdentity) -> Optional[Tuple[Cycle, Leg]]:
        if identity.grid_id != self.grid_id or identity.cell_id != self.cell_id:
            return None
        for cycle in self.cycles:
            if cycle.generation != identity.generation:
                continue
            for leg in cycle.legs:
                if leg.identity == identity:
                    return cycle, leg
        return None

    def _leg(self, identity: LegIdentity) -> Tuple[Cycle, Leg]:
        found = self.find_leg(identity)
        if found is None:
            raise LedgerError(f"unknown leg {identity}")
        return found

    def legs(self) -> List[Leg]:
        return [leg for c in self.cycles for leg in c.legs]

    def non_final_legs(self) -> List[Leg]:
        return [leg for c in self.cycles for leg in c.legs if not leg.is_final]

    # ------------------------------------------------------------------ entry
    def next_entry_identity(self) -> LegIdentity:
        """Identity the next ``begin_entry`` will use (allocate the CID for it first, NG-HIST-004)."""
        for c in self.cycles:
            if c.late_evidence or (c.closed and c.has_open_obligation_or_orders()):
                raise LedgerError(f"cell {self.cell_id} cycle {c.generation} has late evidence/obligation (audit)")
        cur = self.current
        if cur is None:
            return LegIdentity(self.grid_id, self.cell_id, self.generation + 1, LegRole.ENTRY, 0)
        if self._can_retry_entry(cur):
            return LegIdentity(self.grid_id, self.cell_id, cur.generation, LegRole.ENTRY, len(cur.entries))
        raise LedgerError(f"cell {self.cell_id} is locked by cycle {cur.generation}")

    @staticmethod
    def _can_retry_entry(cycle: Cycle) -> bool:
        return (bool(cycle.entries) and all(e.state in REJECTED_STATES and e.filled == 0 for e in cycle.entries)
                and not cycle.tps and not cycle.late_evidence)

    def begin_entry(self, cid: int, rules: TradingRules, *, order_type: Optional[OrderTypePolicy] = None,
                    seq: int = 0) -> Leg:
        """Entry intent of a full ``order_amount_base`` on this cell's fixed entry side/price.

        Allowed only when the cell is unlocked (no open cycle), or as a new revision after a proven zero-fill
        rejection of every previous entry revision of the same cycle (NG-DB-005).
        """
        _require_cid(cid)
        if rules_blockers(rules):
            raise LedgerError(f"trading rules invalid: {rules_blockers(rules)}")
        q = self.order_amount_base
        for leg_name, price in (("entry", self.spec.entry_price), ("tp", self.spec.tp_price)):
            blocker = order_qty_blocker(q, price, rules)
            if blocker is not None:
                raise LedgerError(f"full order_amount_base invalid at {leg_name} price {price}: {blocker}")
        identity = self.next_entry_identity()
        if self.current is None:
            self.cycles.append(Cycle(generation=identity.generation, entry_side=self.spec.entry_side, planned_qty=q))
            self._link()
        cycle = self.current
        assert cycle is not None
        leg = Leg(identity=identity, side=self.spec.entry_side, price=self.spec.entry_price, requested=q,
                  cid=cid, order_type=order_type, seq=seq)
        cycle.entries.append(leg)
        return leg

    # ------------------------------------------------------------------ TP obligations
    def _link(self) -> None:
        for c in self.cycles:
            c.peers = self.cycles

    def _dispatch_analysis(self, rules: Optional[TradingRules]):
        """Per-cycle split, then same side+target aggregation of what no single cycle can dispatch.

        Returns ``(items, total_unassigned, pending, dust_by_generation, blocker)``.
        """
        items: List[TpDispatchItem] = []
        total_unassigned = ZERO
        pending = ZERO
        leftovers: Dict[int, Decimal] = {}
        blocker: Optional[str] = None
        rule_errors = rules_blockers(rules)
        for cycle in self.open_cycles():
            unassigned = cycle.unassigned_total()
            if unassigned < 0:
                blocker = blocker or f"INVARIANT_VIOLATION:gen{cycle.generation}:unassigned={unassigned}"
                continue
            if unassigned == 0:
                continue
            total_unassigned += unassigned
            if rule_errors:
                blocker = blocker or "RULES_UNKNOWN"
                pending += unassigned
                continue
            quantities = _split_valid(unassigned, self.spec.tp_price, rules)
            items.extend(TpDispatchItem(cycle.generation, q) for q in quantities)
            leftover = unassigned - sum(quantities, ZERO)
            if leftover > 0:
                if cycle.entry_final:
                    leftovers[cycle.generation] = leftover
                else:
                    pending += leftover
                if not quantities:
                    min_valid = min_valid_order_qty(rules, self.spec.tp_price)
                    blocker = blocker or f"BELOW_MIN:{leftover}<{min_valid}@{self.spec.tp_price}"
        dust_by_gen = dict(leftovers)
        if len(leftovers) >= 2 and not rule_errors:
            # Several cycles of this cell owe the same side at the same fixed target: one aggregate TP with an
            # exact per-generation allocation (AC-39); what still cannot form a valid order stays DUST.
            from hummingbot.strategy_v2.executors.neutral_grid_executor.dust import DustLot, aggregate
            lots = [DustLot(self.cell_id, gen, self.spec.tp_side, self.spec.tp_price, q)
                    for gen, q in sorted(leftovers.items())]
            for plan in aggregate(lots, rules):
                allocation = tuple((gen, q) for (_, gen), q in plan.allocation)
                items.append(TpDispatchItem(max(g for g, _ in allocation), plan.qty, allocation))
                for gen, q in allocation:
                    dust_by_gen[gen] -= q
        dust_by_gen = {g: q for g, q in dust_by_gen.items() if q > 0}
        return items, total_unassigned, pending, dust_by_gen, blocker

    def tp_obligation_to_dispatch(self, rules: Optional[TradingRules]) -> TpDispatchPlan:
        """Exact TP quantities dispatchable now for every open cycle (no mutation).

        * never rounds up; each item is a size-step multiple satisfying min base/notional (and max base, split
          into balanced chunks so no avoidable DUST is created);
        * below-minimum obligation stays undispatched while the entry may still fill (AC-05);
        * once the entry is final, what cannot form a valid order is DUST (AC-33), unless several cycles of the
          cell together form one: then an aggregate item carries the exact per-generation ``allocation`` (AC-39);
        * unknown TP outcomes keep their reservation, so their quantity is never re-dispatched.
        """
        items, total_unassigned, pending, dust_by_gen, blocker = self._dispatch_analysis(rules)
        if blocker is not None and blocker.startswith("BELOW_MIN") and not dust_by_gen and not pending:
            blocker = None      # every per-cycle leftover went into an aggregate item: nothing is stuck
        return TpDispatchPlan(items=tuple(items), unassigned=total_unassigned, pending_below_min=pending,
                              dust=sum(dust_by_gen.values(), ZERO), blocker=blocker)

    def next_tp_identity(self, generation: Optional[int] = None) -> LegIdentity:
        cycle = self._cycle_for_tp(generation)
        return LegIdentity(self.grid_id, self.cell_id, cycle.generation, LegRole.TP, len(cycle.tps))

    def _cycle_for_tp(self, generation: Optional[int]) -> Cycle:
        if generation is None:
            cur = self.current
            if cur is None:
                raise LedgerError(f"cell {self.cell_id} has no open cycle")
            return cur
        for c in self.cycles:
            if c.generation == generation:
                return c
        raise LedgerError(f"cell {self.cell_id} has no cycle {generation}")

    def add_tp_intent(self, qty: Decimal, cid: int, rules: TradingRules, *, generation: Optional[int] = None,
                      order_type: Optional[OrderTypePolicy] = None, expiry_ms: Optional[int] = None,
                      seq: int = 0, allocation: Optional[Any] = None) -> Leg:
        """Create a TP child intent of exactly ``qty`` at the fixed TP target (reduce_only is never used).

        ``allocation`` (mapping or ``(generation, share)`` pairs) makes it an aggregate TP over several open
        cycles of this cell: every share must fit that cycle's unassigned obligation, shares sum to ``qty``,
        and the leg is hosted by the newest allocated cycle (``generation``, if given, must be that one).
        """
        _require_cid(cid)
        require_decimal(qty, "qty")
        blocker = order_qty_blocker(qty, self.spec.tp_price, rules)
        if blocker is not None:
            raise LedgerError(f"TP qty {qty} invalid: {blocker}")
        shares: Optional[Dict[int, Decimal]] = None
        if allocation is not None:
            pairs = allocation.items() if isinstance(allocation, dict) else allocation
            shares = {}
            for gen, share in pairs:
                if isinstance(gen, bool) or not isinstance(gen, int) or gen in shares:
                    raise LedgerError(f"invalid allocation generation {gen!r}")
                require_decimal(share, "allocation share")
                if share <= 0:
                    raise LedgerError(f"allocation share for generation {gen} must be positive")
                shares[gen] = share
            if not shares or sum(shares.values(), ZERO) != qty:
                raise LedgerError(f"allocation {shares} does not sum to qty {qty}")
            open_gens = {c.generation: c for c in self.open_cycles()}
            for gen, share in shares.items():
                if gen not in open_gens:
                    raise LedgerError(f"allocation generation {gen} is not an open cycle of cell {self.cell_id}")
                if share > open_gens[gen].unassigned_total():
                    raise LedgerError(f"allocation {share} exceeds gen{gen} unassigned "
                                      f"{open_gens[gen].unassigned_total()} (X + reserved <= E)")
            host = max(shares)
            if generation is not None and generation != host:
                raise LedgerError(f"aggregate TP must be hosted by newest allocated generation {host}")
            cycle = open_gens[host]
        else:
            cycle = self._cycle_for_tp(generation)
            unassigned = cycle.unassigned_total()
            if qty > unassigned:
                raise LedgerError(f"TP qty {qty} exceeds unassigned obligation {unassigned} (X + reserved <= E)")
        identity = LegIdentity(self.grid_id, self.cell_id, cycle.generation, LegRole.TP, len(cycle.tps))
        leg = Leg(identity=identity, side=self.spec.tp_side, price=self.spec.tp_price, requested=qty, cid=cid,
                  order_type=order_type, expiry_ms=expiry_ms, seq=seq,
                  allocation=None if shares is None else dict(sorted(shares.items())))
        cycle.tps.append(leg)
        for c in self.cycles:
            c.dust = min(c.dust, max(c.unassigned_total(), ZERO))
        return leg

    def refresh_dust(self, rules: Optional[TradingRules]) -> Dict[int, Decimal]:
        """Persistable DUST classification per open cycle (generation -> dust). Visible, exact, never resized.

        What a same side+target aggregate can dispatch is not DUST.
        """
        changes: Dict[int, Decimal] = {}
        if rules_blockers(rules):
            return changes
        _, _, _, dust_by_gen, _ = self._dispatch_analysis(rules)
        for cycle in self.open_cycles():
            new_dust = dust_by_gen.get(cycle.generation, ZERO)
            if new_dust != cycle.dust:
                cycle.dust = new_dust
                changes[cycle.generation] = new_dust
        return changes

    # ------------------------------------------------------------------ order-level transitions
    def set_state(self, identity: LegIdentity, state: OrderState, *, exchange_order_id: Optional[str] = None) -> Leg:
        cycle, leg = self._leg(identity)
        if state == leg.state:
            if exchange_order_id is not None:
                _set_exchange_id(leg, exchange_order_id)
            return leg
        if state not in _TRANSITIONS[leg.state]:
            raise IllegalTransition(f"{identity}: {leg.state.value} -> {state.value} not allowed via set_state")
        leg.state = state
        if exchange_order_id is not None:
            _set_exchange_id(leg, exchange_order_id)
        return leg

    def record_transport(self, identity: LegIdentity, result: TransportResult) -> Leg:
        """Durable response classification (NG-DB-002/005). Only proven outcomes release anything."""
        cycle, leg = self._leg(identity)
        outcome = result.outcome
        if outcome == TransportOutcome.NOT_SENT:
            if leg.state != OrderState.INTENT or leg.filled != 0:
                raise IllegalTransition(f"{identity}: NOT_SENT only valid for an unsent zero-fill intent")
            leg.state = OrderState.REJECTED_UNSENT
        elif outcome == TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL:
            if leg.state not in (OrderState.INTENT, OrderState.SUBMIT_UNKNOWN) or leg.filled != 0:
                raise IllegalTransition(f"{identity}: definitive zero-fill reject impossible in {leg.state.value} "
                                        f"with filled={leg.filled}")
            leg.state = OrderState.REJECTED_ZERO_FILL
        elif outcome == TransportOutcome.ACCEPTED:
            if leg.state in (OrderState.INTENT, OrderState.SUBMIT_UNKNOWN):
                leg.state = OrderState.LIVE
            if result.exchange_order_id is not None:
                _set_exchange_id(leg, result.exchange_order_id)
        elif outcome == TransportOutcome.UNKNOWN:
            if leg.state == OrderState.INTENT:
                leg.state = OrderState.SUBMIT_UNKNOWN
        else:  # pragma: no cover - exhaustive enum
            raise IllegalTransition(f"unsupported outcome {outcome}")
        return leg

    def mark_intents_unknown(self) -> List[LegIdentity]:
        """Restart recovery: an INTENT whose transport call is not proven absent becomes SUBMIT_UNKNOWN (AC-16).

        The CID is kept; no new CID is ever issued for the same leg.
        """
        changed = []
        for leg in self.legs():
            if leg.state == OrderState.INTENT:
                leg.state = OrderState.SUBMIT_UNKNOWN
                changed.append(leg.identity)
        return changed

    # ------------------------------------------------------------------ history evidence
    def apply_fill(self, key: Hashable, identity: LegIdentity, qty: Decimal, price: Decimal,
                   side: Optional[Side] = None) -> FillOutcome:
        """Apply one authoritative history execution, idempotent by canonical dedupe ``key``."""
        require_decimal(qty, "qty")
        require_decimal(price, "price")
        record = FillRecord(identity=identity, qty=qty, price=price, side=side)
        known = self.fills.get(key)
        if known is not None:
            return FillOutcome.DUPLICATE if known.same_payload(record) else FillOutcome.CONFLICT_KEY
        found = self.find_leg(identity)
        if found is None:
            return FillOutcome.UNKNOWN_LEG
        cycle, leg = found
        if qty <= 0:
            return FillOutcome.CONFLICT_QTY
        if side is not None and side != leg.side:
            return FillOutcome.CONFLICT_SIDE
        if leg.filled + qty > leg.requested:
            return FillOutcome.CONFLICT_OVERFILL
        # Late evidence == an execution for a leg already proven final. A released cycle only ever holds final
        # legs, so a non-final leg in a closed cycle is a resolution TP created after the late evidence: its
        # fills are ordinary and must not re-latch the audit flag.
        late = leg.is_final
        self.fills[key] = record
        leg.filled += qty
        if late:
            leg.late_evidence = True
            cycle.late_evidence = True
            return FillOutcome.LATE_EVIDENCE
        if leg.state in (OrderState.INTENT, OrderState.SUBMIT_UNKNOWN):
            # A history execution proves the order reached the venue.
            leg.state = OrderState.LIVE
        return FillOutcome.APPLIED

    def confirm_terminal(self, identity: LegIdentity, cumulative_filled: Decimal) -> TerminalOutcome:
        """History-proven terminal (exact terminal row + full scan + settlement are asserted by the caller).

        The ledger itself enforces the cumulative equality: applied executions must equal venue cumulative.
        """
        require_decimal(cumulative_filled, "cumulative_filled")
        found = self.find_leg(identity)
        if found is None:
            return TerminalOutcome.UNKNOWN_LEG
        cycle, leg = found
        if leg.state == OrderState.TERMINAL:
            return TerminalOutcome.DUPLICATE if leg.terminal_cumulative == cumulative_filled \
                else TerminalOutcome.CONFLICT
        if leg.state in REJECTED_STATES:
            return TerminalOutcome.CONFLICT
        if cumulative_filled < 0 or cumulative_filled > leg.requested or leg.filled > cumulative_filled:
            return TerminalOutcome.CONFLICT
        if leg.filled < cumulative_filled:
            return TerminalOutcome.MISSING_FILLS
        leg.state = OrderState.TERMINAL
        leg.terminal_cumulative = cumulative_filled
        return TerminalOutcome.TERMINAL

    def acknowledge_late_evidence(self, generation: int) -> List[LegIdentity]:
        """Operator audit acknowledged late evidence for ``generation``; returns the corrected legs.

        The audited executions become the leg's proven cumulative (a "rejected" leg that did execute becomes
        TERMINAL with that cumulative). The resulting obligation stays and is closed by ordinary TP legs;
        the engine must record the audit event durably together with this transition.
        """
        for c in self.cycles:
            if c.generation != generation:
                continue
            corrected = []
            for leg in c.legs:
                if leg.late_evidence:
                    if leg.state in REJECTED_STATES or leg.state == OrderState.TERMINAL:
                        leg.state = OrderState.TERMINAL
                        leg.terminal_cumulative = leg.filled
                    corrected.append(leg.identity)
                leg.late_evidence = False
            c.late_evidence = False
            return corrected
        raise LedgerError(f"no cycle {generation}")

    # ------------------------------------------------------------------ release (NG-CELL-001)
    def can_release(self, position_reconciled: bool) -> ReleaseCheck:
        cur = self.current
        if cur is None:
            # Nothing to release, but still report what keeps the cell from a new cycle (operator visibility).
            reasons = ["NO_OPEN_CYCLE"]
            if any(c.dust > 0 or c.buckets().dust > 0 for c in self.open_cycles()):
                reasons.append("DUST")
            if any(c.late_evidence for c in self.cycles):
                reasons.append("LATE_EVIDENCE_AUDIT")
            if any(c.has_open_obligation_or_orders() for c in self.cycles):
                reasons.append("OLD_CYCLE_OBLIGATION")
            return ReleaseCheck(False, tuple(reasons))
        reasons: List[str] = []
        if not cur.entry_final:
            reasons.append("ENTRY_NOT_TERMINAL")
        for e in cur.entries:
            if e.state == OrderState.TERMINAL and e.terminal_cumulative != e.filled:
                reasons.append("ENTRY_CUMULATIVE_MISMATCH")
        if cur.open_obligation != 0:
            reasons.append(f"OBLIGATION_OPEN:E={cur.E},X={cur.X},S={cur.external_settled}")
        if any(not leg.is_final for leg in cur.legs):
            reasons.append("ORDERS_NOT_TERMINAL")
        if any(c.dust > 0 or c.buckets().dust > 0 for c in self.open_cycles()):
            reasons.append("DUST")
        if cur.late_evidence or any(c.late_evidence for c in self.cycles):
            reasons.append("LATE_EVIDENCE_AUDIT")
        if any(c is not cur and c.has_open_obligation_or_orders() for c in self.cycles):
            reasons.append("OLD_CYCLE_OBLIGATION")
        if not position_reconciled:
            reasons.append("POSITION_NOT_RECONCILED")
        return ReleaseCheck(not reasons, tuple(reasons))

    def release(self, position_reconciled: bool) -> int:
        """Close the current cycle; the next ``begin_entry`` starts a full-Q cycle on the same entry side."""
        check = self.can_release(position_reconciled)
        if not check.ok:
            raise LedgerError(f"cell {self.cell_id} cannot be released: {', '.join(check.reasons)}")
        cur = self.current
        assert cur is not None
        cur.closed = True
        return cur.generation

    # ------------------------------------------------------------------ views
    def buckets(self) -> Buckets:
        total = EMPTY_BUCKETS
        for c in self.open_cycles():
            total = total + c.buckets()
        return total

    def check_invariants(self) -> List[str]:
        """Violations of the cell model (empty list == healthy). Never raises."""
        errors = []
        for c in self.cycles:
            for leg in c.legs:
                if leg.filled < 0 or leg.filled > leg.requested:
                    errors.append(f"gen{c.generation} {leg.identity.role.value}#{leg.identity.revision}: "
                                  f"filled {leg.filled} outside [0,{leg.requested}]")
                if leg.state == OrderState.TERMINAL and leg.terminal_cumulative != leg.filled:
                    errors.append(f"gen{c.generation} {leg.identity.role.value}#{leg.identity.revision}: "
                                  f"filled {leg.filled} != terminal cumulative {leg.terminal_cumulative}")
                if leg.state in REJECTED_STATES and leg.filled != 0:
                    errors.append(f"gen{c.generation}: rejected leg has fills {leg.filled}")
            reserved = sum((req - filled for t, req, filled in c.tp_parts() if not t.is_final), ZERO)
            if c.external_settled < 0 or c.effective_exit + reserved > c.E:
                errors.append(f"gen{c.generation}: X {c.X} + external {c.external_settled} + reserved TP "
                              f"{reserved} > E {c.E}")
            for t in c.tps:
                if t.allocation is None:
                    continue
                gens = {x.generation for x in self.cycles}
                if (sum(t.allocation.values(), ZERO) != t.requested or max(t.allocation) != c.generation
                        or not set(t.allocation) <= gens or any(q <= 0 for q in t.allocation.values())):
                    errors.append(f"gen{c.generation} TP#{t.identity.revision}: allocation {t.allocation} "
                                  f"inconsistent with requested {t.requested} / host gen{c.generation}")
            if c.E > c.planned_qty:
                errors.append(f"gen{c.generation}: E {c.E} > planned {c.planned_qty}")
            if c.dust < 0 or c.dust > max(c.unassigned_total(), ZERO):
                errors.append(f"gen{c.generation}: dust {c.dust} outside unassigned {c.unassigned_total()}")
            physical = [e for e in c.entries if e.state not in REJECTED_STATES]
            if len(physical) > 1:
                errors.append(f"gen{c.generation}: {len(physical)} physical entries (MVP allows one)")
            if any(e.side != c.entry_side for e in c.entries) or c.entry_side != self.spec.entry_side:
                errors.append(f"gen{c.generation}: entry side differs from fixed cell side")
            if any(t.side != self.spec.tp_side or t.price != self.spec.tp_price for t in c.tps):
                errors.append(f"gen{c.generation}: TP leg off fixed target")
        return errors

    def state_flags(self) -> frozenset:
        """All aggregate states that currently hold (several may hold at once, e.g. ENTRY_LIVE + TP_LIVE)."""
        flags = set()
        for cycle in self.open_cycles():
            for e in cycle.entries:
                if e.state in (OrderState.INTENT, OrderState.SUBMIT_UNKNOWN):
                    flags.add(CellState.ENTRY_INTENT)
                elif e.state in (OrderState.LIVE, OrderState.CANCEL_PENDING, OrderState.CANCEL_UNKNOWN):
                    flags.add(CellState.ENTRY_LIVE)
                elif e.state == OrderState.TERMINAL_UNKNOWN:
                    flags.add(CellState.ENTRY_TERMINAL_UNKNOWN)
            for t in cycle.tps:
                if t.state in (OrderState.INTENT, OrderState.SUBMIT_UNKNOWN):
                    flags.add(CellState.TP_INTENT)
                elif t.state in (OrderState.LIVE, OrderState.CANCEL_PENDING, OrderState.CANCEL_UNKNOWN):
                    flags.add(CellState.TP_LIVE)
                elif t.state == OrderState.TERMINAL_UNKNOWN:
                    flags.add(CellState.TP_TERMINAL_UNKNOWN)
            b = cycle.buckets()
            if b.unassigned > 0:
                flags.add(CellState.TP_REQUIRED)
            if b.dust > 0 or cycle.dust > 0:
                flags.add(CellState.DUST)
            if not cycle.closed and all(leg.is_final for leg in cycle.legs) and cycle.open_obligation == 0 \
                    and cycle.dust == 0:
                flags.add(CellState.SETTLING)
        if not flags:
            flags.add(CellState.IDLE)
        return frozenset(flags)

    _PRIMARY_ORDER = (CellState.ENTRY_TERMINAL_UNKNOWN, CellState.TP_TERMINAL_UNKNOWN, CellState.DUST,
                      CellState.TP_REQUIRED, CellState.TP_INTENT, CellState.TP_LIVE, CellState.ENTRY_INTENT,
                      CellState.ENTRY_LIVE, CellState.SETTLING, CellState.IDLE)

    def primary_state(self) -> CellState:
        flags = self.state_flags()
        for s in self._PRIMARY_ORDER:
            if s in flags:
                return s
        return CellState.IDLE  # pragma: no cover

    def submit_request(self, identity: LegIdentity, order_type: OrderTypePolicy,
                       expiry_ms: Optional[int] = None) -> SubmitRequest:
        """Exact submit for a committed intent: fixed price, exact qty, ``reduce_only=False`` (NG-ORD-002)."""
        cycle, leg = self._leg(identity)
        if leg.cid is None:
            raise LedgerError(f"{identity}: no CID allocated")
        if not isinstance(order_type, OrderTypePolicy):
            raise LedgerError(f"order type {order_type!r} not allowed")
        return SubmitRequest(client_order_id=leg.cid, side=leg.side, price=leg.price, amount=leg.requested,
                             order_type=order_type, reduce_only=False,
                             expiry_ms=expiry_ms if expiry_ms is not None else leg.expiry_ms)

    def to_view(self) -> Dict[str, Any]:
        """JSON-ready view (Decimals/ids as strings) for snapshots (CONTRACTS.md cell schema)."""
        cur = self.cycles[-1] if self.cycles else None
        entry = cur.active_entry if cur is not None else None
        if entry is None and cur is not None and cur.entries:
            entry = cur.entries[-1]
        b = self.buckets()
        return {
            "cell_id": self.cell_id,
            "low": str(self.spec.low_price),
            "high": str(self.spec.high_price),
            "entry_side": self.spec.entry_side.value,
            "generation": self.generation,
            "state": self.primary_state().value,
            "state_flags": sorted(s.value for s in self.state_flags()),
            "entry": None if entry is None else _leg_view(entry),
            "tp_children": [] if cur is None else [_leg_view(t) for t in cur.tps],
            "obligation": {
                "E": str(b.E), "X": str(b.X), "live_tp": str(b.live_tp_remainder),
                "reserved_unassigned": str(b.reserved_tp_unassigned), "unassigned": str(b.unassigned),
                "dust": str(b.dust),
            },
            "late_evidence": any(c.late_evidence for c in self.cycles),
        }

    # ------------------------------------------------------------------ persistence helpers
    def to_record(self) -> Dict[str, Any]:
        return {
            "grid_id": self.grid_id,
            "cell": {"cell_id": self.cell_id, "low_price": str(self.spec.low_price),
                     "high_price": str(self.spec.high_price), "entry_side": self.spec.entry_side.value},
            "order_amount_base": str(self.order_amount_base),
            "cycles": [c.to_record() for c in self.cycles],
            "fills": [{"key": _key_to_record(k), "identity": _identity_to_record(f.identity), "qty": str(f.qty),
                       "price": str(f.price), "side": None if f.side is None else f.side.value}
                      for k, f in self.fills.items()],
        }

    @classmethod
    def from_record(cls, rec: Dict[str, Any]) -> "CellLedger":
        cell = rec["cell"]
        spec = CellSpec(cell_id=int(cell["cell_id"]), low_price=Decimal(cell["low_price"]),
                        high_price=Decimal(cell["high_price"]), entry_side=Side(cell["entry_side"]))
        ledger = cls(rec["grid_id"], spec, Decimal(rec["order_amount_base"]))
        ledger.cycles = [Cycle.from_record(c) for c in rec["cycles"]]
        ledger._link()
        for f in rec["fills"]:
            ledger.fills[_key_from_record(f["key"])] = FillRecord(
                identity=_identity_from_record(f["identity"]), qty=Decimal(f["qty"]), price=Decimal(f["price"]),
                side=None if f.get("side") is None else Side(f["side"]))
        return ledger


# ---------------------------------------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------------------------------------

def _split_valid(qty: Decimal, price: Decimal, rules: TradingRules) -> List[Decimal]:
    """Split an exact obligation into valid order quantities without rounding up (see ``grid.partition_valid``)."""
    return partition_valid(qty, price, rules)


def _require_cid(cid: Any) -> None:
    if isinstance(cid, bool) or not isinstance(cid, int) or not 0 < cid <= MAX_CLIENT_ORDER_ID:
        raise LedgerError(f"client order id must be an int in [1, 2**48-1], got {cid!r}")


def _set_exchange_id(leg: Leg, exchange_order_id: str) -> None:
    if not isinstance(exchange_order_id, str):
        raise LedgerError(f"exchange order id must be str, got {type(exchange_order_id).__name__}")
    if leg.exchange_order_id is not None and leg.exchange_order_id != exchange_order_id:
        raise LedgerError(f"{leg.identity}: exchange id conflict {leg.exchange_order_id} != {exchange_order_id}")
    leg.exchange_order_id = exchange_order_id


def _leg_view(leg: Leg) -> Dict[str, Any]:
    return {
        "cid": None if leg.cid is None else str(leg.cid),
        "exchange_id": leg.exchange_order_id,
        "revision": leg.identity.revision,
        "side": leg.side.value,
        "price": str(leg.price),
        "requested": str(leg.requested),
        "filled": str(leg.filled),
        "remaining": str(leg.remaining),
        "state": leg.state.value,
        "expiry": None if leg.expiry_ms is None else str(leg.expiry_ms),
        "allocation": None if leg.allocation is None else {str(g): str(q) for g, q in sorted(leg.allocation.items())},
    }


def _allocation_from_record(rec: Optional[Dict[str, str]]) -> Optional[Dict[int, Decimal]]:
    if rec is None:
        return None
    return {int(g): Decimal(q) for g, q in rec.items()}


def _identity_to_record(identity: LegIdentity) -> Dict[str, Any]:
    return {"grid_id": identity.grid_id, "cell_id": identity.cell_id, "generation": identity.generation,
            "role": identity.role.value, "revision": identity.revision}


def _identity_from_record(rec: Dict[str, Any]) -> LegIdentity:
    return LegIdentity(grid_id=rec["grid_id"], cell_id=int(rec["cell_id"]), generation=int(rec["generation"]),
                       role=LegRole(rec["role"]), revision=int(rec["revision"]))


def _key_to_record(key: Hashable) -> Any:
    if isinstance(key, tuple):
        return {"t": [_key_to_record(k) for k in key]}
    if isinstance(key, (str, int)) and not isinstance(key, bool):
        return key
    raise LedgerError(f"fill key element must be str/int/tuple, got {type(key).__name__}")


def _key_from_record(rec: Any) -> Hashable:
    if isinstance(rec, dict):
        return tuple(_key_from_record(k) for k in rec["t"])
    return rec


def _opt_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise LedgerError(f"expected int/str id, got {type(value).__name__}")
    return int(value)


def aggregate_buckets(ledgers: Iterable[CellLedger]) -> Buckets:
    total = EMPTY_BUCKETS
    for ledger in ledgers:
        total = total + ledger.buckets()
    return total


def all_open_legs(ledgers: Sequence[CellLedger]) -> List[Leg]:
    return [leg for ledger in ledgers for leg in ledger.non_final_legs()]
