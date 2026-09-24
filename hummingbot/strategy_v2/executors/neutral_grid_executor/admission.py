"""Admission and order-slot reservation (NG-RISK-005, AC-34, AC-44, AC-57).

Every armed cell reserves ``required_slots = 1 + ceil(Q / min_valid_TP_qty)`` slots before its entry is
submitted: one for the entry and enough for the maximum number of simultaneously live TP children (each TP
child is at least ``min_valid_TP_qty``). A physical order (live, partially filled, cancel-pending or UNKNOWN)
holds exactly one slot until it is proven final. The slot ledger separates *actual* orders from *unused*
reservations so no slot is counted twice: ``free = cap - actual - sum(max(reserved_i - actual_i, 0))``.

Exit obligations (cells with an open cycle) are always served before new entries; idle eligible cells are
armed by distance of their fixed entry price to the current price, then by stable ``cell_id``, head-of-line
(a cell that does not fit blocks the ones behind it, so nothing starves). Pure, deterministic.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal
from fractions import Fraction
from typing import Dict, List, Optional, Sequence, Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CellSpec, OrderState, TradingRules
from hummingbot.strategy_v2.executors.neutral_grid_executor.grid import (
    min_valid_order_qty,
    order_qty_blocker,
    require_decimal,
    rules_blockers,
)

ZERO = Decimal("0")


def min_valid_tp_qty(rules: TradingRules, tp_price: Decimal) -> Decimal:
    """``quantize_up(max(runtime_min_base, runtime_min_notional / tp_price), size_step)`` (>= one step)."""
    return min_valid_order_qty(rules, tp_price)


def required_slots(order_amount_base: Decimal, rules: TradingRules, tp_price: Decimal) -> int:
    """``1 + ceil(order_amount_base / min_valid_TP_qty)``."""
    q = require_decimal(order_amount_base, "order_amount_base")
    if q <= 0:
        raise ValueError(f"order_amount_base must be positive, got {q}")
    min_valid = min_valid_tp_qty(rules, tp_price)
    return 1 + math.ceil(Fraction(q) / Fraction(min_valid))


def effective_cap(cap: int, rules: Optional[TradingRules]) -> int:
    """User cap limited by the venue cap (the user may only raise it within the venue limit)."""
    if isinstance(cap, bool) or not isinstance(cap, int) or cap < 0:
        raise ValueError(f"cap must be a non-negative int, got {cap!r}")
    venue = rules.max_active_orders_venue if rules is not None else None
    return cap if venue is None else min(cap, venue)


@dataclass(frozen=True)
class CellAdmission:
    """Admission input for one cell."""
    cell: CellSpec
    open_cycle: bool                 # locked by a cycle (orders/obligation/dust): exit side, never dropped
    actual_orders: int = 0           # physical non-final orders of this cell (each holds one slot)
    reserved_slots: int = 0          # current durable reservation of this cell (0 if not armed)
    eligible: bool = True            # idle cell allowed to start a new cycle now
    blocker: Optional[str] = None    # why an idle cell is not eligible (UI)
    slot_need: Optional[int] = None  # open cycle: max simultaneous orders still possible (see slot_need_from_ledger)
    entry_live: bool = False         # open cycle whose entry may still fill (its spare slots are trimmed first)


@dataclass(frozen=True)
class SlotLedger:
    cap: int
    actual: int              # physical orders holding slots (including UNKNOWN)
    reserved_unused: int     # reservations not (yet) backed by an actual order
    free: int                # cap - actual - reserved_unused; negative => oversubscribed

    @property
    def oversubscribed(self) -> bool:
        return self.free < 0


@dataclass(frozen=True)
class AdmissionPlan:
    armed: Tuple[int, ...]                  # cells holding a reservation (open cycles + armed idle cells)
    newly_armed: Tuple[int, ...]            # idle cells that may submit an entry now (in priority order)
    queued: Tuple[int, ...]                 # eligible idle cells waiting in the bounded durable queue (ordered)
    reservations: Dict[int, int]            # cell_id -> reserved slots
    actual: Dict[int, int]                  # cell_id -> actual orders
    slots: SlotLedger
    blocked: Dict[int, str] = field(default_factory=dict)      # idle cells not eligible, with reason
    shortfall: Dict[int, int] = field(default_factory=dict)    # open cells lacking reservation (slots missing/trimmed)
    blocker: Optional[str] = None                              # global reason no new entry is armed

    def unused(self, cell_id: int) -> int:
        return max(self.reservations.get(cell_id, 0) - self.actual.get(cell_id, 0), 0)


def _distance_key(cell: CellSpec, mid: Optional[Decimal]):
    distance = abs(cell.entry_price - mid) if mid is not None else ZERO
    return (distance, cell.cell_id)


def plan(cells: Sequence[CellAdmission], cap: int, mid: Optional[Decimal], rules: Optional[TradingRules],
         order_amount_base: Decimal, *, entries_allowed: bool = True,
         entries_blocker: Optional[str] = None) -> AdmissionPlan:
    """Deterministic armed/queued split and slot ledger.

    ``entries_allowed=False`` (pause, drift, unknown data, out of bounds...) keeps every idle cell queued but
    still reserves for exits. Invalid rules or a full Q that is invalid under current rules never arm a cell.
    """
    ids = [c.cell.cell_id for c in cells]
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate cell ids in admission input")
    for c in cells:
        for name in ("actual_orders", "reserved_slots"):
            v = getattr(c, name)
            if isinstance(v, bool) or not isinstance(v, int) or v < 0:
                raise ValueError(f"cell {c.cell.cell_id}: {name} must be a non-negative int, got {v!r}")
    eff_cap = effective_cap(cap, rules)
    rule_errors = rules_blockers(rules)
    blocker: Optional[str] = None
    if rule_errors:
        blocker = "RULES_INVALID:" + ",".join(rule_errors)

    reservations: Dict[int, int] = {}
    actual: Dict[int, int] = {}
    shortfall: Dict[int, int] = {}
    blocked: Dict[int, str] = {}

    # 1. Exit side first: every open cycle keeps what it still may need (never less than its actual orders).
    open_cells = sorted((c for c in cells if c.open_cycle), key=lambda c: c.cell.cell_id)
    targets: Dict[int, int] = {}
    for c in open_cells:
        cid = c.cell.cell_id
        actual[cid] = c.actual_orders
        held = max(c.reserved_slots, c.actual_orders)
        if c.slot_need is not None:
            target = c.slot_need
        elif not rule_errors:
            target = required_slots(order_amount_base, rules, c.cell.tp_price)
        else:
            target = held
        targets[cid] = max(target, c.actual_orders)
        # Shrink first (a finished entry / closed TP returns its slot), growth is granted in step 2.
        reservations[cid] = min(held, targets[cid])
    for c in cells:
        if not c.open_cycle:
            actual[c.cell.cell_id] = c.actual_orders

    def ledger() -> SlotLedger:
        total_actual = sum(actual.values())
        unused = sum(max(reservations.get(k, 0) - actual.get(k, 0), 0) for k in reservations)
        return SlotLedger(cap=eff_cap, actual=total_actual, reserved_unused=unused,
                          free=eff_cap - total_actual - unused)

    # 2. Grow open reservations to what they may still need (e.g. runtime floors dropped) before any entry.
    for c in open_cells:
        cid = c.cell.cell_id
        missing = targets[cid] - reservations[cid]
        if missing <= 0:
            continue
        grant = max(min(missing, ledger().free), 0)
        reservations[cid] += grant
        if grant < missing:
            shortfall[cid] = missing - grant
    # 2b. The effective cap may have dropped below what is reserved (venue/user cap reduction): unused
    # reservations are trimmed so they never authorize an order beyond the cap. Entry-side spare slots go
    # first, exit-only cells last; within a group the farthest fixed entry price from mid first, then higher id.
    deficit = -ledger().free
    if deficit > 0:
        def trim_key(c: CellAdmission):
            distance = abs(c.cell.entry_price - mid) if mid is not None else ZERO
            return (not c.entry_live, -distance, -c.cell.cell_id)

        for c in sorted(open_cells, key=trim_key):
            if deficit <= 0:
                break
            cid = c.cell.cell_id
            spare = reservations[cid] - actual[cid]
            cut = min(max(spare, 0), deficit)
            if cut > 0:
                reservations[cid] -= cut
                shortfall[cid] = shortfall.get(cid, 0) + cut
                deficit -= cut
    slots = ledger()
    if slots.oversubscribed:
        blocker = blocker or f"SLOTS_OVERSUBSCRIBED:actual={slots.actual}>cap={slots.cap}"
    elif shortfall:
        blocker = blocker or "EXIT_RESERVATION_SHORTFALL"
    if not entries_allowed:
        blocker = blocker or entries_blocker or "ENTRIES_BLOCKED"

    # 3. Idle cells: deterministic priority, head-of-line.
    idle = sorted((c for c in cells if not c.open_cycle), key=lambda c: _distance_key(c.cell, mid))
    newly_armed: List[int] = []
    queued: List[int] = []
    head_blocked = blocker is not None or mid is None
    if mid is None and blocker is None:
        blocker = "MID_UNKNOWN"
    for c in idle:
        cid = c.cell.cell_id
        if not c.eligible:
            blocked[cid] = c.blocker or "NOT_ELIGIBLE"
            continue
        if rule_errors:
            queued.append(cid)
            continue
        invalid = _full_q_blocker(c.cell, order_amount_base, rules)
        if invalid is not None:
            blocked[cid] = invalid
            continue
        if head_blocked:
            queued.append(cid)
            continue
        need = required_slots(order_amount_base, rules, c.cell.tp_price)
        # Idle cells hold no order; their previous (not yet used) reservation is re-planned from scratch here.
        if ledger().free >= need:
            reservations[cid] = need
            newly_armed.append(cid)
        else:
            head_blocked = True
            queued.append(cid)
    if queued and blocker is None:
        blocker = "SLOT_CAP"
    armed = tuple(sorted(reservations))
    return AdmissionPlan(armed=armed, newly_armed=tuple(newly_armed), queued=tuple(queued),
                         reservations=reservations, actual=actual, slots=ledger(), blocked=blocked,
                         shortfall=shortfall, blocker=blocker)


def _full_q_blocker(cell: CellSpec, q: Decimal, rules: TradingRules) -> Optional[str]:
    """Full Q must stay valid at entry and TP price under *current* rules (AC-34); no resize."""
    for name, price in (("ENTRY", cell.entry_price), ("TP", cell.tp_price)):
        b = order_qty_blocker(q, price, rules)
        if b is not None:
            return f"FULL_Q_INVALID_{name}:{b}"
    return None


@dataclass(frozen=True)
class SlotEntry:
    """A live entry order that could be cancelled to free a slot (AC-44 emergency path)."""
    key: str
    cell_id: int
    price: Decimal
    filled: Decimal
    state: OrderState


def select_entries_to_cancel(needed: int, entries: Sequence[SlotEntry], mid: Optional[Decimal],
                             protected_cells: Sequence[int] = ()) -> Tuple[str, ...]:
    """Deterministically choose ``needed`` LIVE entries to cancel-request so TP obligations get a slot.

    Only used when reservations cannot cover a TP (venue cap reduced / hard conflict), never to chase priority.
    Order: unfilled before partially filled, farthest from ``mid`` first, then higher cell id.
    """
    if needed <= 0:
        return ()
    protected = set(protected_cells)
    candidates = [e for e in entries if e.state == OrderState.LIVE and e.cell_id not in protected]

    def key(e: SlotEntry):
        distance = abs(e.price - mid) if mid is not None else ZERO
        return (e.filled > 0, -distance, -e.cell_id, e.key)

    candidates.sort(key=key)
    return tuple(e.key for e in candidates[:needed])


def slot_need_from_ledger(ledger, rules: TradingRules) -> int:
    """Maximum number of simultaneously held slots a ``cells.CellLedger`` may still need.

    ``non-final legs`` (each holds one slot, UNKNOWN included) ``+`` future TP children: while an entry may still
    fill, ``ceil((unassigned + entry remainder) / min_valid_TP_qty)``; once every entry is final, only what is
    dispatchable now under ``rules`` (DUST does not hold a slot until rules make it dispatchable).
    """
    min_valid = min_valid_tp_qty(rules, ledger.spec.tp_price)
    plan = ledger.tp_obligation_to_dispatch(rules)
    need = 0
    for cycle in ledger.open_cycles():
        need += sum(1 for leg in cycle.legs if not leg.is_final)
        if cycle.entry_final:
            need += sum(1 for item in plan.items if item.generation == cycle.generation)
        else:
            entry_remaining = sum((e.remaining for e in cycle.entries if not e.is_final), ZERO)
            future = max(cycle.unassigned_total(), ZERO) + entry_remaining
            need += math.ceil(Fraction(future) / Fraction(min_valid))
    return need
