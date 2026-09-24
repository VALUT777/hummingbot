"""Submit router: self-trade prevention, TP priority, TP-TP FIFO, caps and slots (NG-ORD-003, AC-31, AC-44).

Input is the set of *candidate* submits (obligations the engine wants to turn into orders; not yet committed as
intents) and every owned order that is not proven final (live, cancel-pending, submit/cancel/terminal unknown,
committed-but-unsent intents). Output is a deterministic list of actions:

* ``SUBMIT`` - commit intent + CID + reservation, then call transport;
* ``CANCEL`` - cancel-request an owned LIVE *entry* (TP priority or AC-44 slot emergency);
* ``WITHDRAW`` - an owned entry intent that was never sent may be released as REJECTED_UNSENT;
* ``WAIT`` - candidate stays queued with an exact reason (self-trade, FIFO, headroom, slot, entries blocked);
* ``BLOCKED`` - operator required (RISK_BLOCKED): no market close, no netting, no resize.

Rules: every candidate is checked against all owned possibly-executable orders *and* earlier candidates of the
same plan (approved or waiting). TP candidates go first (by ``seq``), entries after. A TP conflicting with an
owned entry cancel-requests that entry and waits for its history terminal; a TP conflicting with another TP (or an
unknown-role order) waits FIFO; an entry conflicting with anything waits. Obligations are never netted internally
and a normal partial fill never cancels an entry (only an explicit conflict does). A TP that cannot be placed for a
non-conflict reason (RISK_BLOCKED, no slot obtainable under the effective cap) does not hold the FIFO head, so a
crossing TP that owns a slot is not starved; entries still never cross it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Sequence, Set, Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor import risk as risk_mod
from hummingbot.strategy_v2.executors.neutral_grid_executor.admission import SlotEntry, select_entries_to_cancel
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import LegRole, OrderState, Side

ZERO = Decimal("0")

_FINAL = frozenset({OrderState.TERMINAL, OrderState.REJECTED_UNSENT, OrderState.REJECTED_ZERO_FILL})
_CANCEL_IN_FLIGHT = frozenset({OrderState.CANCEL_PENDING, OrderState.CANCEL_UNKNOWN, OrderState.TERMINAL_UNKNOWN})


class ActionKind(str, Enum):
    SUBMIT = "SUBMIT"
    CANCEL = "CANCEL"
    WITHDRAW = "WITHDRAW"
    WAIT = "WAIT"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class RouterOrder:
    """An owned order that is not proven final."""
    key: str
    side: Side
    price: Decimal
    remaining: Decimal               # possibly executable remainder (requested - history filled)
    state: OrderState
    role: Optional[LegRole]          # None => unknown role (never cancelled by the router)
    cell_id: Optional[int] = None
    seq: int = 0
    filled: Decimal = ZERO

    @property
    def possibly_executable(self) -> bool:
        return self.state not in _FINAL and self.remaining > 0

    @property
    def holds_slot(self) -> bool:
        return self.state not in _FINAL

    @classmethod
    def from_leg(cls, leg) -> "RouterOrder":
        """From a ``cells.Leg`` (duck-typed)."""
        return cls(key=str(leg.cid), side=leg.side, price=leg.price, remaining=leg.remaining, state=leg.state,
                   role=leg.identity.role, cell_id=leg.identity.cell_id, seq=leg.seq, filled=leg.filled)


@dataclass(frozen=True)
class RouterIntent:
    """A candidate submit (not yet a committed intent)."""
    key: str
    side: Side
    price: Decimal
    qty: Decimal
    role: LegRole
    cell_id: int
    seq: int                         # FIFO order (e.g. commit sequence of the history fill creating it)


@dataclass(frozen=True)
class RouterAction:
    kind: ActionKind
    key: str
    reason: str
    blocked_by: Tuple[str, ...] = ()


@dataclass
class SlotBudget:
    """Slots available to this plan: global free pool plus each armed cell's unused reservation.

    ``headroom`` (``effective cap - actual orders``) is a hard ceiling on new orders in this plan whatever the
    reservations say, so a stale reservation can never put an order over a reduced cap.
    """
    free: int
    cell_unused: Dict[int, int] = field(default_factory=dict)
    headroom: Optional[int] = None

    @classmethod
    def from_plan(cls, plan) -> "SlotBudget":
        """From an ``admission.AdmissionPlan``."""
        return cls(free=max(plan.slots.free, 0), cell_unused={c: plan.unused(c) for c in plan.reservations},
                   headroom=plan.slots.cap - plan.slots.actual)

    def take(self, cell_id: int, allow_global: bool) -> Optional[str]:
        if self.headroom is not None and self.headroom <= 0:
            return None
        if self.cell_unused.get(cell_id, 0) > 0:
            self.cell_unused[cell_id] -= 1
            source = "CELL"
        elif allow_global and self.free > 0:
            self.free -= 1
            source = "GLOBAL"
        else:
            return None
        if self.headroom is not None:
            self.headroom -= 1
        return source


@dataclass(frozen=True)
class RouterPlan:
    actions: Tuple[RouterAction, ...]

    def of(self, kind: ActionKind) -> Tuple[RouterAction, ...]:
        return tuple(a for a in self.actions if a.kind == kind)

    @property
    def submits(self) -> Tuple[str, ...]:
        return tuple(a.key for a in self.of(ActionKind.SUBMIT))

    @property
    def cancels(self) -> Tuple[str, ...]:
        return tuple(a.key for a in self.of(ActionKind.CANCEL))

    @property
    def withdraws(self) -> Tuple[str, ...]:
        return tuple(a.key for a in self.of(ActionKind.WITHDRAW))

    @property
    def risk_blocked(self) -> bool:
        return any(a.kind == ActionKind.BLOCKED for a in self.actions)

    def action_for(self, key: str) -> Optional[RouterAction]:
        for a in self.actions:
            if a.key == key and a.kind in (ActionKind.SUBMIT, ActionKind.WAIT, ActionKind.BLOCKED):
                return a
        return None


def crosses(side_a: Side, price_a: Decimal, side_b: Side, price_b: Decimal) -> bool:
    """True if two own orders could match each other (BUY price >= SELL price)."""
    if side_a == side_b:
        return False
    buy, sell = (price_a, price_b) if side_a == Side.BUY else (price_b, price_a)
    return buy >= sell


def plan_submits(pending_intents: Sequence[RouterIntent], owned_orders: Sequence[RouterOrder], *,
                 endpoints: Optional[risk_mod.RiskEndpoints] = None, limits: Optional[risk_mod.RiskLimits] = None,
                 slots: Optional[SlotBudget] = None, mid: Optional[Decimal] = None,
                 entries_allowed: bool = True, entries_blocker: str = "ENTRIES_BLOCKED",
                 owed: Tuple[Decimal, Decimal] = (ZERO, ZERO)) -> RouterPlan:
    """Deterministic routing plan (see module docstring).

    ``endpoints`` are the order-only endpoints (spec formula) and must exclude the candidates. ``owed`` is
    ``risk.obligation_totals`` (TP obligations without an order, candidates included): entries are admitted
    against ``endpoints + owed`` so they never consume headroom an exit needs (TP priority).
    """
    keys = [i.key for i in pending_intents]
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate candidate keys")
    for i in pending_intents:
        if i.qty <= 0:
            raise ValueError(f"candidate {i.key}: qty must be positive")
    if (endpoints is None) != (limits is None):
        raise ValueError("endpoints and limits must be given together")

    live = [o for o in owned_orders if o.possibly_executable]
    actions: List[RouterAction] = []
    cancel_requested: Set[str] = set()
    withdraw_requested: Set[str] = set()
    approved: List[RouterIntent] = []
    waiting_tps: List[RouterIntent] = []
    # TPs that cannot be placed for a non-conflict reason (RISK_BLOCKED, no slot obtainable): they do not hold
    # the TP FIFO head (else a TP that owns a slot / fits the cap could wait forever behind them), but entries
    # never cross them. Self-trade safety is unaffected: conflicts are always checked against live orders.
    blocked_tps: List[RouterIntent] = []
    ep = endpoints
    owed_buy, owed_sell = owed
    budget = SlotBudget(free=slots.free, cell_unused=dict(slots.cell_unused),
                        headroom=slots.headroom) if slots is not None else None
    # Slots that will come back when already-requested entry cancels become terminal (avoid cancel spam).
    slot_frees_in_flight = sum(1 for o in owned_orders
                               if o.role == LegRole.ENTRY and o.state in _CANCEL_IN_FLIGHT)

    def request_cancel(order: RouterOrder, reason: str) -> None:
        if order.state == OrderState.LIVE:
            if order.key not in cancel_requested:
                cancel_requested.add(order.key)
                actions.append(RouterAction(ActionKind.CANCEL, order.key, reason))
        elif order.state == OrderState.INTENT:
            if order.key not in withdraw_requested:
                withdraw_requested.add(order.key)
                actions.append(RouterAction(ActionKind.WITHDRAW, order.key, reason))

    tps = sorted((i for i in pending_intents if i.role == LegRole.TP), key=lambda i: (i.seq, i.key))
    entries = sorted((i for i in pending_intents if i.role != LegRole.TP), key=lambda i: (i.seq, i.key))

    # ---------------------------------------------------------------- TP candidates (priority, FIFO)
    for tp in tps:
        owned_conflicts = [o for o in live if crosses(tp.side, tp.price, o.side, o.price)]
        earlier = [c for c in approved + waiting_tps if crosses(tp.side, tp.price, c.side, c.price)]
        if owned_conflicts or earlier:
            for o in owned_conflicts:
                if o.role == LegRole.ENTRY:
                    request_cancel(o, f"SELF_TRADE_TP_PRIORITY:{tp.key}")
            blockers = tuple(o.key for o in owned_conflicts) + tuple(c.key for c in earlier)
            kinds = []
            if any(o.role == LegRole.ENTRY for o in owned_conflicts):
                kinds.append("WAIT_ENTRY_TERMINAL")
            if any(o.role != LegRole.ENTRY for o in owned_conflicts) or earlier:
                kinds.append("WAIT_TP_FIFO")
            actions.append(RouterAction(ActionKind.WAIT, tp.key, "SELF_TRADE:" + "+".join(kinds), blockers))
            waiting_tps.append(tp)
            continue
        if ep is not None:
            blocker = risk_mod.check_submit(ep, tp.side, tp.qty, LegRole.TP, limits)
            if blocker is not None:
                entry_views = [risk_mod.OpenLeg(side=o.side, remaining=o.remaining, role=o.role, state=o.state,
                                                key=o.key, cell_id=o.cell_id, price=o.price)
                               for o in live if o.key not in cancel_requested and o.key not in withdraw_requested]
                in_flight_extra = [risk_mod.OpenLeg(side=o.side, remaining=o.remaining, role=o.role,
                                                    state=OrderState.CANCEL_PENDING, key=o.key, cell_id=o.cell_id,
                                                    price=o.price)
                                   for o in live if o.key in cancel_requested or o.key in withdraw_requested]
                decision = risk_mod.plan_tp_headroom(ep, tp.side, tp.qty, entry_views + in_flight_extra, limits, mid)
                if decision.risk_blocked:
                    # Operator required. It does not hold the TP FIFO head (another exit may be what frees the
                    # headroom); entries still may not cross it. Safety is unaffected: conflicts are always
                    # checked against live orders.
                    actions.append(RouterAction(ActionKind.BLOCKED, tp.key, decision.reason))
                    blocked_tps.append(tp)
                    continue
                by_key = {o.key: o for o in live}
                for k in decision.cancel + decision.withdraw:
                    request_cancel(by_key[k], f"NET_HEADROOM_TP_PRIORITY:{tp.key}")
                actions.append(RouterAction(ActionKind.WAIT, tp.key, decision.reason,
                                            decision.cancel + decision.withdraw))
                waiting_tps.append(tp)
                continue
        if budget is not None and budget.take(tp.cell_id, allow_global=True) is None:
            if slot_frees_in_flight > 0:
                slot_frees_in_flight -= 1
                actions.append(RouterAction(ActionKind.WAIT, tp.key, "WAIT_SLOT:ENTRY_CANCEL_IN_FLIGHT"))
                blocked_tps.append(tp)
                continue
            candidates = [SlotEntry(key=o.key, cell_id=o.cell_id if o.cell_id is not None else -1, price=o.price,
                                    filled=o.filled, state=o.state)
                          for o in owned_orders
                          if o.role == LegRole.ENTRY and o.key not in cancel_requested]
            chosen = select_entries_to_cancel(1, candidates, mid)
            if chosen:
                by_key = {o.key: o for o in owned_orders}
                request_cancel(by_key[chosen[0]], f"SLOT_EMERGENCY_TP_PRIORITY:{tp.key}")
                actions.append(RouterAction(ActionKind.WAIT, tp.key, "WAIT_SLOT:ENTRY_CANCEL_REQUESTED", chosen))
            else:
                actions.append(RouterAction(ActionKind.WAIT, tp.key, "WAIT_SLOT:NO_CANCELLABLE_ENTRY"))
            blocked_tps.append(tp)
            continue
        actions.append(RouterAction(ActionKind.SUBMIT, tp.key, "OK"))
        approved.append(tp)
        if ep is not None:
            ep = risk_mod.add_to_endpoints(ep, tp.side, tp.qty, LegRole.TP)
        # The obligation became an order: it is now counted by ``ep``.
        if tp.side == Side.BUY:
            owed_buy = max(owed_buy - tp.qty, ZERO)
        else:
            owed_sell = max(owed_sell - tp.qty, ZERO)

    # ---------------------------------------------------------------- entry candidates
    for entry in entries:
        if not entries_allowed:
            actions.append(RouterAction(ActionKind.WAIT, entry.key, entries_blocker))
            continue
        conflicts = tuple(o.key for o in live if crosses(entry.side, entry.price, o.side, o.price))
        conflicts += tuple(c.key for c in approved + waiting_tps + blocked_tps
                           if crosses(entry.side, entry.price, c.side, c.price))
        if conflicts:
            actions.append(RouterAction(ActionKind.WAIT, entry.key, "SELF_TRADE:ENTRY_WAITS", conflicts))
            continue
        if any(o.cell_id == entry.cell_id and o.key in cancel_requested for o in live):
            actions.append(RouterAction(ActionKind.WAIT, entry.key, "CELL_CANCEL_IN_FLIGHT"))
            continue
        if ep is not None:
            blocker = risk_mod.check_submit(risk_mod.with_obligations(ep, owed_buy, owed_sell), entry.side,
                                            entry.qty, entry.role, limits)
            if blocker is not None:
                actions.append(RouterAction(ActionKind.WAIT, entry.key, blocker))
                continue
        if budget is not None and budget.take(entry.cell_id, allow_global=False) is None:
            actions.append(RouterAction(ActionKind.WAIT, entry.key, "NOT_ARMED:NO_RESERVED_SLOT"))
            continue
        actions.append(RouterAction(ActionKind.SUBMIT, entry.key, "OK"))
        approved.append(entry)
        if ep is not None:
            ep = risk_mod.add_to_endpoints(ep, entry.side, entry.qty, entry.role)

    return RouterPlan(actions=tuple(actions))
