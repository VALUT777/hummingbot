"""Net/gross risk endpoints, caps, TP headroom and margin advisory (NG-RISK-002, NG-RISK-004, AC-24..28, AC-37).

* ``P = B + confirmed_buys - confirmed_sells``;
* ``P_max = P + sum(possibly executable BUY remainder)``, ``P_min = P - sum(possibly executable SELL remainder)``;
  every non-final order counts with its *full* submitted remainder until terminal + cumulative are proven,
  regardless of role (entry, TP or unknown);
* ``gross = sum(E - X - S)`` over cycles (unpaired virtual quantity, baseline excluded);
  ``gross_worst = gross + remainder of possibly executable entries and of unknown-role orders``.
  TP legs change net risk but never add virtual gross.

Pure, Decimal only.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, List, NamedTuple, Optional, Sequence, Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CellSpec,
    LegRole,
    OrderState,
    Side,
    TradingRules,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.grid import rules_blockers

ZERO = Decimal("0")

_FINAL = frozenset({OrderState.TERMINAL, OrderState.REJECTED_UNSENT, OrderState.REJECTED_ZERO_FILL})
# Entries the engine may still stop without venue interaction (durable intent, transport not yet called).
WITHDRAWABLE_STATES = frozenset({OrderState.INTENT})
# Entries that can be cancel-requested now.
CANCELLABLE_STATES = frozenset({OrderState.LIVE})
# Entries already on their way to terminal (their remainder is released only on history terminal).
CANCEL_IN_FLIGHT_STATES = frozenset({OrderState.CANCEL_PENDING, OrderState.CANCEL_UNKNOWN,
                                     OrderState.TERMINAL_UNKNOWN})
# Entries whose submission outcome is unknown: not cancellable yet, but they will resolve to LIVE (then
# cancellable) or to a definitive zero-fill reject (then freed), so they are pending headroom, not a dead end.
UNRESOLVED_SUBMIT_STATES = frozenset({OrderState.SUBMIT_UNKNOWN})


@dataclass(frozen=True)
class OpenLeg:
    """Conservative risk view of one owned order that is not proven final."""
    side: Side
    remaining: Decimal                   # full submitted remainder (requested - history-confirmed filled)
    role: Optional[LegRole] = None       # None => unknown role, counted conservatively
    state: OrderState = OrderState.LIVE
    key: Optional[str] = None            # stable id (e.g. CID as str) for cancel planning
    cell_id: Optional[int] = None
    price: Optional[Decimal] = None

    @property
    def counts(self) -> bool:
        return self.state not in _FINAL and self.remaining > 0

    @classmethod
    def from_leg(cls, leg: Any) -> "OpenLeg":
        """Build from a ``cells.Leg`` (duck-typed to avoid an import cycle)."""
        return cls(side=leg.side, remaining=leg.remaining, role=leg.identity.role, state=leg.state,
                   key=None if leg.cid is None else str(leg.cid), cell_id=leg.identity.cell_id, price=leg.price)


class RiskEndpoints(NamedTuple):
    P: Decimal
    P_min: Decimal
    P_max: Decimal
    gross_worst: Decimal
    long_entry_worst: Optional[Decimal] = None
    short_entry_worst: Optional[Decimal] = None


@dataclass(frozen=True)
class RiskLimits:
    max_abs_net_position: Decimal
    max_gross_position: Decimal
    directional_gross_limits: bool = False

    def __post_init__(self):
        for name in ("max_abs_net_position", "max_gross_position"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be a positive finite Decimal, got {value!r}")
        if type(self.directional_gross_limits) is not bool:
            raise ValueError("directional_gross_limits must be a bool")


def _dec(value: Any, name: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal, got {type(value).__name__}: {value!r}")
    return value


def endpoints(baseline: Decimal, confirmed_buys: Decimal, confirmed_sells: Decimal, open_legs: Iterable[OpenLeg],
              *, unpaired: Decimal = ZERO, long_unpaired: Optional[Decimal] = None,
              short_unpaired: Optional[Decimal] = None) -> RiskEndpoints:
    """Conservative reachable net interval and worst-case gross (NG-RISK-002).

    ``unpaired`` is the confirmed virtual gross ``sum(E - X - S)`` over all durable cycles (baseline excluded).
    """
    b = _dec(baseline, "baseline")
    buys = _dec(confirmed_buys, "confirmed_buys")
    sells = _dec(confirmed_sells, "confirmed_sells")
    gross = _dec(unpaired, "unpaired")
    if gross < ZERO:
        raise ValueError("unpaired must be non-negative")
    if long_unpaired is None and short_unpaired is None and gross == ZERO:
        long_unpaired = short_unpaired = ZERO
    if (long_unpaired is None) != (short_unpaired is None):
        raise ValueError("long_unpaired and short_unpaired must be given together")
    long_worst = None if long_unpaired is None else _dec(long_unpaired, "long_unpaired")
    short_worst = None if short_unpaired is None else _dec(short_unpaired, "short_unpaired")
    if long_worst is not None and (long_worst < ZERO or short_worst < ZERO or long_worst + short_worst != gross):
        raise ValueError("directional unpaired exposure must be non-negative and sum to unpaired")
    p = b + buys - sells
    up = ZERO
    down = ZERO
    gross_extra = ZERO
    for leg in open_legs:
        _dec(leg.remaining, "remaining")
        if not leg.counts:
            continue
        if leg.side == Side.BUY:
            up += leg.remaining
        else:
            down += leg.remaining
        if leg.role != LegRole.TP:
            gross_extra += leg.remaining
            if leg.side == Side.BUY and long_worst is not None:
                long_worst += leg.remaining
            elif leg.side == Side.SELL and short_worst is not None:
                short_worst += leg.remaining
    return RiskEndpoints(P=p, P_min=p - down, P_max=p + up, gross_worst=gross + gross_extra,
                         long_entry_worst=long_worst, short_entry_worst=short_worst)


def endpoints_from_ledgers(baseline: Decimal, ledgers: Sequence[Any],
                           extra_open_legs: Iterable[OpenLeg] = ()) -> RiskEndpoints:
    """Endpoints from ``cells.CellLedger`` objects (all durable cycles) plus extra owned legs of unknown role."""
    buys = ZERO
    sells = ZERO
    unpaired = ZERO
    long_unpaired = ZERO
    short_unpaired = ZERO
    legs: List[OpenLeg] = list(extra_open_legs)
    for ledger in ledgers:
        for cycle in ledger.cycles:
            unpaired += abs(cycle.open_obligation)
            if cycle.entry_side == Side.BUY:
                long_unpaired += abs(cycle.open_obligation)
            else:
                short_unpaired += abs(cycle.open_obligation)
            if cycle.entry_side == Side.BUY:
                buys += cycle.external_entered
            else:
                sells += cycle.external_entered
            # External settlement is a real account execution in the obligation-closing direction.
            if cycle.entry_side == Side.BUY:
                sells += cycle.external_settled
            else:
                buys += cycle.external_settled
            for leg in cycle.legs:
                if leg.side == Side.BUY:
                    buys += leg.filled
                else:
                    sells += leg.filled
                if not leg.is_final:
                    legs.append(OpenLeg.from_leg(leg))
    return endpoints(baseline, buys, sells, legs, unpaired=unpaired,
                     long_unpaired=long_unpaired, short_unpaired=short_unpaired)


def obligation_totals(ledgers: Sequence[Any]) -> Tuple[Decimal, Decimal]:
    """``(owed_buy, owed_sell)``: open TP obligations that are not (yet) an order (``unassigned + dust``).

    TP priority (NG-RISK-002): these exits are owed, so entries must not consume the net headroom they need.
    """
    owed_buy = ZERO
    owed_sell = ZERO
    for ledger in ledgers:
        for cycle in ledger.open_cycles():
            b = cycle.buckets()
            owed = max(b.unassigned, ZERO) + b.dust
            if owed <= 0:
                continue
            if ledger.spec.tp_side == Side.BUY:
                owed_buy += owed
            else:
                owed_sell += owed
    return owed_buy, owed_sell


def with_obligations(ep: RiskEndpoints, owed_buy: Decimal, owed_sell: Decimal) -> RiskEndpoints:
    """Endpoints that also include every owed exit as if it were a live order (used to admit entries).

    Fills never widen this interval and TP dispatch only turns an obligation into an order, so as long as every
    entry is admitted against it, no TP can be cap-blocked by headroom that entries consumed.
    """
    return ep._replace(P_max=ep.P_max + _dec(owed_buy, "owed_buy"), P_min=ep.P_min - _dec(owed_sell, "owed_sell"))


def reachable_interval(baseline: Decimal, cells: Sequence[CellSpec], order_amount_base: Decimal
                       ) -> Tuple[Decimal, Decimal]:
    """Preview interval with every cell's full entry resting: ``[B - Q*#SELL, B + Q*#BUY]``."""
    b = _dec(baseline, "baseline")
    q = _dec(order_amount_base, "order_amount_base")
    buys = sum(1 for c in cells if c.entry_side == Side.BUY)
    sells = len(cells) - buys
    return b - q * sells, b + q * buys


def check_submit(ep: RiskEndpoints, side: Side, qty: Decimal, role: Optional[LegRole], limits: RiskLimits
                 ) -> Optional[str]:
    """Blocker for adding one more possibly executable order to ``ep`` (None == within caps).

    The candidate must not already be included in ``ep``. Net caps apply to every role (a TP moves net);
    the gross cap applies to entries and unknown-role orders only.
    """
    q = _dec(qty, "qty")
    if q <= 0:
        return f"QTY_NOT_POSITIVE:{q}"
    cap = limits.max_abs_net_position
    if side == Side.BUY and ep.P_max + q > cap:
        return f"NET_CAP_LONG:P_max {ep.P_max}+{q}>{cap}"
    if side == Side.SELL and ep.P_min - q < -cap:
        return f"NET_CAP_SHORT:P_min {ep.P_min}-{q}<-{cap}"
    if role != LegRole.TP:
        if limits.directional_gross_limits:
            directional = ep.long_entry_worst if side == Side.BUY else ep.short_entry_worst
            if directional is None:
                return "GROSS_CAP_DIRECTIONAL_UNKNOWN"
            if directional + q > limits.max_gross_position:
                label = "GROSS_CAP_LONG" if side == Side.BUY else "GROSS_CAP_SHORT"
                return f"{label}:{directional}+{q}>{limits.max_gross_position}"
        elif ep.gross_worst + q > limits.max_gross_position:
            return f"GROSS_CAP:{ep.gross_worst}+{q}>{limits.max_gross_position}"
    return None


def add_to_endpoints(ep: RiskEndpoints, side: Side, qty: Decimal, role: Optional[LegRole]) -> RiskEndpoints:
    """Endpoints after reserving one more possibly executable order."""
    if side == Side.BUY:
        ep = ep._replace(P_max=ep.P_max + qty)
    else:
        ep = ep._replace(P_min=ep.P_min - qty)
    if role != LegRole.TP:
        ep = ep._replace(gross_worst=ep.gross_worst + qty)
        if side == Side.BUY and ep.long_entry_worst is not None:
            ep = ep._replace(long_entry_worst=ep.long_entry_worst + qty)
        elif side == Side.SELL and ep.short_entry_worst is not None:
            ep = ep._replace(short_entry_worst=ep.short_entry_worst + qty)
    return ep


def cap_violations(ep: RiskEndpoints, limits: RiskLimits) -> List[str]:
    """Current endpoints outside caps (e.g. after drift, late evidence or cap reduction): hard-risk conflict."""
    out = []
    if ep.P_max > limits.max_abs_net_position:
        out.append(f"NET_CAP_LONG_EXCEEDED:{ep.P_max}>{limits.max_abs_net_position}")
    if ep.P_min < -limits.max_abs_net_position:
        out.append(f"NET_CAP_SHORT_EXCEEDED:{ep.P_min}<-{limits.max_abs_net_position}")
    if limits.directional_gross_limits:
        if ep.long_entry_worst is None or ep.short_entry_worst is None:
            out.append("GROSS_CAP_DIRECTIONAL_UNKNOWN")
        else:
            if ep.long_entry_worst > limits.max_gross_position:
                out.append(f"GROSS_CAP_LONG_EXCEEDED:{ep.long_entry_worst}>{limits.max_gross_position}")
            if ep.short_entry_worst > limits.max_gross_position:
                out.append(f"GROSS_CAP_SHORT_EXCEEDED:{ep.short_entry_worst}>{limits.max_gross_position}")
    elif ep.gross_worst > limits.max_gross_position:
        out.append(f"GROSS_CAP_EXCEEDED:{ep.gross_worst}>{limits.max_gross_position}")
    return out


@dataclass(frozen=True)
class HeadroomDecision:
    """How a TP obligation obtains net headroom (TP has priority over entries, never market close)."""
    ok: bool                              # submit now
    cancel: Tuple[str, ...] = ()          # entry keys to cancel-request now (LIVE)
    withdraw: Tuple[str, ...] = ()        # entry keys to withdraw before transport (INTENT -> REJECTED_UNSENT)
    wait: bool = False                    # TP waits for history terminal of cancelled/in-flight entries
    risk_blocked: bool = False            # even cancelling all same-side entries cannot fit: operator required
    reason: str = ""


def plan_tp_headroom(ep: RiskEndpoints, tp_side: Side, tp_qty: Decimal, entries: Sequence[OpenLeg],
                     limits: RiskLimits, mid: Optional[Decimal] = None) -> HeadroomDecision:
    """NG-RISK-002 TP priority: free ``P_max`` (BUY TP) / ``P_min`` (SELL TP) by cancelling same-side entries.

    ``entries`` are owned possibly executable orders (only same-side ENTRY legs are considered). Cancelled
    entries keep counting until history terminal, so the TP waits. Confirmed position is never "freed".
    Deterministic order: farthest entry price from ``mid`` first, then higher cell id, then key.
    """
    q = _dec(tp_qty, "tp_qty")
    cap = limits.max_abs_net_position
    excess = (ep.P_max + q - cap) if tp_side == Side.BUY else (-(ep.P_min - q) - cap)
    if excess <= 0:
        return HeadroomDecision(ok=True, reason="WITHIN_CAP")
    same_side = [e for e in entries if e.side == tp_side and e.role == LegRole.ENTRY and e.counts]
    in_flight = sum((e.remaining for e in same_side if e.state in CANCEL_IN_FLIGHT_STATES), ZERO)
    unresolved = sum((e.remaining for e in same_side if e.state in UNRESOLVED_SUBMIT_STATES), ZERO)
    candidates = [e for e in same_side if e.state in CANCELLABLE_STATES or e.state in WITHDRAWABLE_STATES]

    def priority(e: OpenLeg):
        distance = abs(e.price - mid) if (mid is not None and e.price is not None) else ZERO
        return (-distance, -(e.cell_id if e.cell_id is not None else -1), e.key or "")

    candidates.sort(key=priority)
    # Every same-side entry remainder can eventually be freed (cancel, withdraw, in-flight cancel, or an unknown
    # submit that resolves). Only confirmed position + non-entry orders beyond the cap need the operator.
    freeable = in_flight + unresolved + sum((e.remaining for e in candidates), ZERO)
    if freeable < excess:
        return HeadroomDecision(ok=False, risk_blocked=True,
                                reason=f"RISK_BLOCKED:{tp_side.value} TP {q} needs {excess}, entries free {freeable}")
    cancel: List[str] = []
    withdraw: List[str] = []
    freed = in_flight
    for e in candidates:
        if freed >= excess:
            break
        freed += e.remaining
        (withdraw if e.state in WITHDRAWABLE_STATES else cancel).append(e.key or "")
    reason = f"WAIT_ENTRY_TERMINAL:{tp_side.value} TP {q} needs {excess}"
    if freed < excess:
        reason += f",WAIT_UNRESOLVED_SUBMIT:{unresolved}"
    return HeadroomDecision(ok=False, cancel=tuple(cancel), withdraw=tuple(withdraw), wait=True, reason=reason)


# ---------------------------------------------------------------------------------------------------------------------
# NG-RISK-004 margin advisory and unknown-data gate
# ---------------------------------------------------------------------------------------------------------------------

def _known_non_negative(value: Any) -> bool:
    return isinstance(value, Decimal) and value.is_finite() and value >= 0


def required_margin_estimate(p_min: Decimal, p_max: Decimal, mark_price: Optional[Decimal],
                             leverage: Optional[Decimal]) -> Optional[Decimal]:
    """Conservative initial margin of the worst reachable net position; None when any input is unknown/malformed."""
    if not (_known_non_negative(mark_price) and isinstance(leverage, Decimal) and leverage.is_finite()
            and leverage > 0 and isinstance(p_min, Decimal) and p_min.is_finite()
            and isinstance(p_max, Decimal) and p_max.is_finite()):
        return None
    return max(abs(p_min), abs(p_max)) * mark_price / leverage


def margin_advisory(available: Any, required_estimate: Any) -> Tuple[Optional[str], bool]:
    """``(warning, blocks)``. Known finite non-negative numbers never block (exchange applies margin rules);
    unknown or malformed inputs block new exposure (AC-37)."""
    if not _known_non_negative(available):
        return f"MARGIN_UNKNOWN:available={available!r}", True
    if not _known_non_negative(required_estimate):
        return f"MARGIN_UNKNOWN:required={required_estimate!r}", True
    if available < required_estimate:
        return f"MARGIN_SHORTFALL:available {available} < estimated {required_estimate}", False
    return None, False


@dataclass(frozen=True)
class ExposureInputs:
    """Facts required before any new exposure; ``None`` means unknown (NG-RISK-004)."""
    position_known: Optional[bool]
    leverage_ok: Optional[bool]
    position_mode_ok: Optional[bool]          # ONEWAY confirmed
    market_active: Optional[bool]
    rules: Optional[TradingRules]
    history_complete: Optional[bool]
    account_identity_ok: Optional[bool]
    data_age_s: Optional[Decimal]
    freshness_limit_s: Decimal
    margin_available: Any = None
    margin_required: Any = None


def exposure_blockers(inp: ExposureInputs) -> Tuple[List[str], Optional[str]]:
    """``(blockers, margin_warning)``: any unknown/false fact blocks new exposure; a known margin shortfall only warns."""
    blockers: List[str] = []
    for name in ("position_known", "leverage_ok", "position_mode_ok", "market_active", "history_complete",
                 "account_identity_ok"):
        value = getattr(inp, name)
        if value is not True:
            blockers.append(f"{name.upper()}:{'UNKNOWN' if value is None else 'FALSE'}")
    blockers.extend(rules_blockers(inp.rules))
    age = inp.data_age_s
    if not (isinstance(age, Decimal) and age.is_finite() and age >= 0):
        blockers.append("FRESHNESS:UNKNOWN")
    elif age > inp.freshness_limit_s:
        blockers.append(f"FRESHNESS:STALE:{age}>{inp.freshness_limit_s}")
    warning, blocks = margin_advisory(inp.margin_available, inp.margin_required)
    if blocks:
        blockers.append(warning or "MARGIN_UNKNOWN")
        warning = None
    return blockers, warning
