"""Fixed integer-tick grid, anchor/side assignment and config validation (NG-GRID-001..003, NG-RISK-005 bootstrap).

Pure functions: no IO, no asyncio, no float. Every price/quantity is ``Decimal``; exactness checks use
``fractions.Fraction`` so no Decimal context rounding can move a boundary.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from decimal import Decimal
from fractions import Fraction
from typing import Any, Dict, List, Optional, Sequence

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CellSpec,
    GridConfig,
    OrderTypePolicy,
    Side,
    TradingRules,
)

ZERO = Decimal("0")

# Allowed safe subset of order types (NG-ORD-001). MARKET is never allowed.
ALLOWED_ENTRY_ORDER_TYPES = frozenset({OrderTypePolicy.LIMIT_MAKER, OrderTypePolicy.LIMIT})
ALLOWED_TP_ORDER_TYPES = frozenset({OrderTypePolicy.LIMIT, OrderTypePolicy.LIMIT_MAKER})

# Design safety floors from spec table NG-ARCH-003 (configurable upwards, never below).
MIN_HISTORY_OVERLAP_S = Decimal("60")
MIN_POLL_INTERVAL_S = Decimal("5")


class GridValidationError(ValueError):
    """Invalid grid geometry / quantity. Never silently quantized away."""


# ---------------------------------------------------------------------------------------------------------------------
# Exact numeric helpers (shared by cells/admission/risk/dust)
# ---------------------------------------------------------------------------------------------------------------------

def _is_decimal(value: Any) -> bool:
    return isinstance(value, Decimal) and value.is_finite()


def require_decimal(value: Any, name: str) -> Decimal:
    """Reject float/str/int-as-quantity and non-finite Decimals (NaN/Inf)."""
    if not _is_decimal(value):
        raise GridValidationError(f"{name} must be a finite Decimal, got {type(value).__name__}: {value!r}")
    return value


def is_step_multiple(value: Decimal, step: Decimal) -> bool:
    if step <= 0:
        raise GridValidationError(f"step must be positive, got {step}")
    return (Fraction(value) / Fraction(step)).denominator == 1


def to_ticks(price: Decimal, tick: Decimal) -> int:
    """Exact price -> integer ticks. Raises if ``price`` is not an exact tick multiple (no silent quantization)."""
    require_decimal(price, "price")
    require_decimal(tick, "tick_size")
    if tick <= 0:
        raise GridValidationError(f"tick_size must be positive, got {tick}")
    ratio = Fraction(price) / Fraction(tick)
    if ratio.denominator != 1:
        raise GridValidationError(f"price {price} is not an exact multiple of tick {tick}")
    return ratio.numerator


def from_ticks(ticks: int, tick: Decimal) -> Decimal:
    if isinstance(ticks, bool) or not isinstance(ticks, int):
        raise GridValidationError(f"ticks must be int, got {type(ticks).__name__}")
    return Decimal(ticks) * tick


def quantize_down(qty: Decimal, step: Decimal) -> Decimal:
    """Largest step multiple <= qty (qty >= 0). Quantity is never rounded up (NG-GRID-003)."""
    if qty < 0:
        raise GridValidationError(f"quantity must be non-negative, got {qty}")
    if step <= 0:
        raise GridValidationError(f"step must be positive, got {step}")
    result = Decimal(math.floor(Fraction(qty) / Fraction(step))) * step
    return qty if result == qty else result


def quantize_up(qty: Decimal, step: Decimal) -> Decimal:
    """Smallest step multiple >= qty. Used only for *floors* (minimum valid size), never for order quantity."""
    if qty < 0:
        raise GridValidationError(f"quantity must be non-negative, got {qty}")
    if step <= 0:
        raise GridValidationError(f"step must be positive, got {step}")
    result = Decimal(math.ceil(Fraction(qty) / Fraction(step))) * step
    return qty if result == qty else result


def min_valid_order_qty(rules: TradingRules, price: Decimal) -> Decimal:
    """``quantize_up(max(min_base, min_notional / price), size_step)``, at least one size step (NG-RISK-005).

    Computed exactly with Fractions so the result satisfies ``qty * price >= min_notional`` precisely.
    """
    require_decimal(price, "price")
    if price <= 0:
        raise GridValidationError(f"price must be positive, got {price}")
    step = Fraction(rules.size_step)
    if step <= 0:
        raise GridValidationError(f"size_step must be positive, got {rules.size_step}")
    floor_base = Fraction(rules.min_base)
    floor_notional = Fraction(rules.min_notional) / Fraction(price)
    need = max(floor_base, floor_notional, step)
    return Decimal(math.ceil(need / step)) * rules.size_step


def order_qty_blocker(qty: Decimal, price: Decimal, rules: TradingRules) -> Optional[str]:
    """Why ``qty`` at ``price`` would be an invalid *new* order under ``rules`` (None if valid).

    Never proposes a resized quantity: the caller must keep the exact quantity as an obligation/dust.
    """
    if not _is_decimal(qty) or qty <= 0:
        return f"QTY_NOT_POSITIVE:{qty}"
    if not _is_decimal(price) or price <= 0:
        return f"PRICE_NOT_POSITIVE:{price}"
    if rules.size_step <= 0:
        return "RULES_INVALID:size_step"
    if not is_step_multiple(qty, rules.size_step):
        return f"QTY_NOT_STEP_MULTIPLE:{qty}/{rules.size_step}"
    if qty < rules.min_base:
        return f"BELOW_MIN_BASE:{qty}<{rules.min_base}"
    if Fraction(qty) * Fraction(price) < Fraction(rules.min_notional):
        return f"BELOW_MIN_NOTIONAL:{qty}*{price}<{rules.min_notional}"
    if rules.max_base is not None and qty > rules.max_base:
        return f"ABOVE_MAX_BASE:{qty}>{rules.max_base}"
    return None


def rules_blockers(rules: Optional[TradingRules]) -> List[str]:
    """Malformed/unknown trading rules block new exposure (NG-RISK-004)."""
    if rules is None:
        return ["RULES_UNKNOWN"]
    errors: List[str] = []
    for name in ("tick_size", "size_step"):
        value = getattr(rules, name)
        if not _is_decimal(value) or value <= 0:
            errors.append(f"RULES_INVALID:{name}={value!r}")
    for name in ("min_base", "min_notional"):
        value = getattr(rules, name)
        if not _is_decimal(value) or value < 0:
            errors.append(f"RULES_INVALID:{name}={value!r}")
    if rules.max_base is not None and (not _is_decimal(rules.max_base) or rules.max_base <= 0):
        errors.append(f"RULES_INVALID:max_base={rules.max_base!r}")
    if rules.max_leverage is not None and (not _is_decimal(rules.max_leverage) or rules.max_leverage <= 0):
        errors.append(f"RULES_INVALID:max_leverage={rules.max_leverage!r}")
    venue_cap = rules.max_active_orders_venue
    if venue_cap is not None and (isinstance(venue_cap, bool) or not isinstance(venue_cap, int) or venue_cap <= 0):
        errors.append(f"RULES_INVALID:max_active_orders_venue={venue_cap!r}")
    return errors


# ---------------------------------------------------------------------------------------------------------------------
# NG-GRID-001 / NG-GRID-002
# ---------------------------------------------------------------------------------------------------------------------

def _require_cell_count(cell_count: Any) -> int:
    if isinstance(cell_count, bool) or not isinstance(cell_count, int) or cell_count <= 0:
        raise GridValidationError(f"cell_count must be a positive int, got {cell_count!r}")
    return cell_count


def build_grid_ticks(lower: Decimal, upper: Decimal, cell_count: int, tick: Decimal) -> List[int]:
    """``P_i_tick = L_tick + floor(i*T/N)``, ``i = 0..N``; integer arithmetic only."""
    n = _require_cell_count(cell_count)
    low_tick = to_ticks(lower, tick)
    up_tick = to_ticks(upper, tick)
    if lower <= 0:
        raise GridValidationError(f"lower_price must be positive, got {lower}")
    if low_tick >= up_tick:
        raise GridValidationError(f"lower_price {lower} must be strictly below upper_price {upper}")
    span = up_tick - low_tick
    if span < n:
        raise GridValidationError(f"not enough ticks: T={span} < N={n} (bounds {lower}..{upper}, tick {tick})")
    ticks = [low_tick + (i * span) // n for i in range(n + 1)]
    # T >= N guarantees strictly increasing lines; keep the explicit check as a guard against regressions.
    for a, b in zip(ticks, ticks[1:]):
        if b <= a:
            raise GridValidationError("grid collapsed: non-increasing price lines")
    return ticks


def build_grid(lower: Decimal, upper: Decimal, cell_count: int, rules: TradingRules) -> List[Decimal]:
    """NG-GRID-001: ``N+1`` strictly increasing exact tick prices."""
    require_decimal(lower, "lower_price")
    require_decimal(upper, "upper_price")
    tick = require_decimal(rules.tick_size, "tick_size")
    return [from_ticks(t, tick) for t in build_grid_ticks(lower, upper, cell_count, tick)]


def compute_anchor(mid: Decimal, lower: Decimal, upper: Decimal) -> Decimal:
    """NG-GRID-002: ``anchor = clamp(mid, lower, upper)``; fixed forever after first bootstrap."""
    require_decimal(mid, "mid_price")
    if mid <= 0:
        raise GridValidationError(f"mid_price must be positive, got {mid}")
    return min(max(mid, lower), upper)


def assign_cells(prices: Sequence[Decimal], anchor: Decimal) -> List[CellSpec]:
    """``P[i] < anchor`` -> BUY entry at P[i] / SELL TP at P[i+1]; otherwise SELL entry at P[i+1] / BUY TP at P[i]."""
    require_decimal(anchor, "anchor")
    if len(prices) < 2:
        raise GridValidationError("grid needs at least two price lines")
    for a, b in zip(prices, prices[1:]):
        if not b > a:
            raise GridValidationError("price lines must be strictly increasing")
    cells = []
    for i in range(len(prices) - 1):
        side = Side.BUY if prices[i] < anchor else Side.SELL
        cells.append(CellSpec(cell_id=i, low_price=prices[i], high_price=prices[i + 1], entry_side=side))
    return cells


def side_counts(cells: Sequence[CellSpec]) -> Dict[str, int]:
    buys = sum(1 for c in cells if c.entry_side == Side.BUY)
    return {"BUY": buys, "SELL": len(cells) - buys}


# ---------------------------------------------------------------------------------------------------------------------
# NG-GRID-003 / NG-RISK-005 bootstrap / NG-ARCH-003 config validation
# ---------------------------------------------------------------------------------------------------------------------

def _positive_decimal_error(value: Any, name: str) -> Optional[str]:
    if not _is_decimal(value):
        return f"{name}: must be a finite Decimal (got {type(value).__name__})"
    if value <= 0:
        return f"{name}: must be positive (got {value})"
    return None


def validate_order_types(entry: Any, tp: Any, rules: Optional[TradingRules] = None) -> List[str]:
    errors = []
    for name, value, allowed in (("entry_order_type", entry, ALLOWED_ENTRY_ORDER_TYPES),
                                 ("tp_order_type", tp, ALLOWED_TP_ORDER_TYPES)):
        if not isinstance(value, OrderTypePolicy) or value not in allowed:
            raw = getattr(value, "value", value)
            errors.append(f"{name}: {raw!r} is not allowed (MARKET and unknown types are forbidden)")
            continue
        if rules is not None:
            if value == OrderTypePolicy.LIMIT_MAKER and not rules.supports_post_only:
                errors.append(f"{name}: venue does not support post-only LIMIT_MAKER")
            if value == OrderTypePolicy.LIMIT and not rules.supports_limit:
                errors.append(f"{name}: venue does not support LIMIT orders")
    return errors


def validate_full_q(cells: Sequence[CellSpec], qty: Decimal, rules: TradingRules) -> List[str]:
    """Full ``order_amount_base`` must be a valid order at the entry price AND the paired TP price of every cell."""
    errors = []
    for cell in cells:
        for leg, price in (("entry", cell.entry_price), ("tp", cell.tp_price)):
            blocker = order_qty_blocker(qty, price, rules)
            if blocker is not None:
                errors.append(f"cell {cell.cell_id} {leg} @ {price}: order_amount_base {qty} invalid ({blocker})")
    return errors


def validate_config(cfg: GridConfig, rules: Optional[TradingRules], mid: Optional[Decimal], *,
                    bootstrap: bool = True) -> List[str]:
    """All validation errors for ``cfg`` under fresh ``rules`` and ``mid`` (empty list == valid).

    ``bootstrap=True`` additionally requires the signed ``expected_initial_position`` (first start only).
    """
    errors: List[str] = []
    rule_errors = rules_blockers(rules)
    errors.extend(rule_errors)

    errors.extend(validate_order_types(cfg.entry_order_type, cfg.tp_order_type, None if rule_errors else rules))

    for name in ("order_amount_base", "leverage", "max_abs_net_position", "max_gross_position",
                 "history_freshness_s", "poll_interval_s", "history_overlap_s"):
        err = _positive_decimal_error(getattr(cfg, name), name)
        if err:
            errors.append(err)
    for name in ("lower_price", "upper_price"):
        err = _positive_decimal_error(getattr(cfg, name), name)
        if err:
            errors.append(err)
    if not _is_decimal(cfg.settlement_delay_s) or cfg.settlement_delay_s < 0:
        errors.append(f"settlement_delay_s: must be a non-negative Decimal (got {cfg.settlement_delay_s!r})")
    if isinstance(cfg.settlement_scans, bool) or not isinstance(cfg.settlement_scans, int) or cfg.settlement_scans < 1:
        errors.append(f"settlement_scans: must be an int >= 1 (got {cfg.settlement_scans!r})")
    if _is_decimal(cfg.history_overlap_s) and cfg.history_overlap_s < MIN_HISTORY_OVERLAP_S:
        errors.append(f"history_overlap_s: must be >= {MIN_HISTORY_OVERLAP_S}s (got {cfg.history_overlap_s})")
    if _is_decimal(cfg.poll_interval_s) and cfg.poll_interval_s < MIN_POLL_INTERVAL_S:
        errors.append(f"poll_interval_s: must be >= {MIN_POLL_INTERVAL_S}s (got {cfg.poll_interval_s})")
    if isinstance(cfg.tp_gtt_seconds, bool) or not isinstance(cfg.tp_gtt_seconds, int) or cfg.tp_gtt_seconds <= 0:
        errors.append(f"tp_gtt_seconds: must be a positive int (got {cfg.tp_gtt_seconds!r})")
    cap = cfg.max_active_orders
    if isinstance(cap, bool) or not isinstance(cap, int) or cap <= 0:
        errors.append(f"max_active_orders: must be a positive int (got {cap!r})")
    elif rules is not None and not rule_errors and rules.max_active_orders_venue is not None \
            and cap > rules.max_active_orders_venue:
        errors.append(f"max_active_orders: {cap} exceeds venue limit {rules.max_active_orders_venue}")

    if bootstrap:
        b = cfg.expected_initial_position
        if b is None:
            errors.append("expected_initial_position: required signed Decimal on first bootstrap")
        elif not _is_decimal(b):
            errors.append(f"expected_initial_position: must be a finite Decimal (got {type(b).__name__})")
        elif _is_decimal(cfg.max_abs_net_position) and abs(b) > cfg.max_abs_net_position:
            errors.append(f"expected_initial_position: |{b}| exceeds max_abs_net_position {cfg.max_abs_net_position}")

    if rule_errors:
        return errors

    if _is_decimal(cfg.leverage) and cfg.leverage > 0:
        if rules.max_leverage is None:
            errors.append("leverage: runtime venue maximum leverage unknown")
        elif cfg.leverage > rules.max_leverage:
            errors.append(f"leverage: {cfg.leverage} exceeds venue maximum {rules.max_leverage}")

    q = cfg.order_amount_base
    if _is_decimal(q) and q > 0 and not is_step_multiple(q, rules.size_step):
        errors.append(f"order_amount_base: {q} is not an exact multiple of size step {rules.size_step}")

    try:
        prices = build_grid(cfg.lower_price, cfg.upper_price, cfg.cell_count, rules)
    except GridValidationError as e:
        errors.append(f"grid: {e}")
        return errors

    if mid is None:
        errors.append("mid_price: unknown, cannot fix anchor")
        return errors
    try:
        anchor = compute_anchor(mid, cfg.lower_price, cfg.upper_price)
    except GridValidationError as e:
        errors.append(f"mid_price: {e}")
        return errors
    cells = assign_cells(prices, anchor)
    if _is_decimal(q) and q > 0:
        errors.extend(validate_full_q(cells, q, rules))
    return errors


def config_fingerprint(cfg: GridConfig) -> str:
    """Stable identity of grid dimensions (bounds, N, Q, pair, account, connector, grid id). Decimals as exact strings.

    Two configs with the same fingerprint describe the same fixed grid; any change of dimensions/Q is a new grid
    (NG-ARCH-003: dimensions/Q cannot change on a running grid).
    """
    payload = {
        "grid_id": cfg.grid_id,
        "connector_name": cfg.connector_name,
        "trading_pair": cfg.trading_pair,
        "account_index": int(cfg.account_index),
        "lower_price": _canon_decimal(cfg.lower_price),
        "upper_price": _canon_decimal(cfg.upper_price),
        "cell_count": int(cfg.cell_count),
        "order_amount_base": _canon_decimal(cfg.order_amount_base),
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _canon_decimal(value: Decimal) -> str:
    require_decimal(value, "decimal")
    if value == 0:
        return "0"
    text = format(value.normalize(), "f")
    return text


# ---------------------------------------------------------------------------------------------------------------------
# Preview (NG-UI-002 data; pure)
# ---------------------------------------------------------------------------------------------------------------------

@dataclass
class GridPreview:
    errors: List[str]
    prices: List[Decimal] = field(default_factory=list)
    cells: List[CellSpec] = field(default_factory=list)
    anchor: Optional[Decimal] = None
    buy_cells: int = 0
    sell_cells: int = 0
    baseline: Optional[Decimal] = None
    reachable_min: Optional[Decimal] = None
    reachable_max: Optional[Decimal] = None
    gross_worst: Optional[Decimal] = None
    armed: int = 0
    queued: int = 0
    slots_per_cell: Dict[int, int] = field(default_factory=dict)
    slots_reserved: int = 0
    slots_free: int = 0
    slot_cap: int = 0
    min_valid_tp_qty: Dict[int, Decimal] = field(default_factory=dict)
    estimated_max_notional: Optional[Decimal] = None


def build_preview(cfg: GridConfig, rules: Optional[TradingRules], mid: Optional[Decimal],
                  baseline: Optional[Decimal] = None, *, bootstrap: bool = True) -> GridPreview:
    """Everything the pre-start preview needs, computed from config + fresh rules only (no exchange state)."""
    from hummingbot.strategy_v2.executors.neutral_grid_executor import admission, risk

    errors = validate_config(cfg, rules, mid, bootstrap=bootstrap)
    preview = GridPreview(errors=errors)
    if rules is None or rules_blockers(rules):
        return preview
    try:
        preview.prices = build_grid(cfg.lower_price, cfg.upper_price, cfg.cell_count, rules)
    except GridValidationError:
        return preview
    if mid is None:
        return preview
    try:
        preview.anchor = compute_anchor(mid, cfg.lower_price, cfg.upper_price)
    except GridValidationError:
        return preview
    preview.cells = assign_cells(preview.prices, preview.anchor)
    counts = side_counts(preview.cells)
    preview.buy_cells, preview.sell_cells = counts["BUY"], counts["SELL"]
    b = baseline if baseline is not None else cfg.expected_initial_position
    q = cfg.order_amount_base
    if not (_is_decimal(q) and q > 0):
        return preview
    if b is not None and _is_decimal(b):
        preview.baseline = b
        preview.reachable_min, preview.reachable_max = risk.reachable_interval(b, preview.cells, q)
        preview.estimated_max_notional = max(abs(preview.reachable_min), abs(preview.reachable_max)) * mid
    preview.gross_worst = q * len(preview.cells)
    if is_step_multiple(q, rules.size_step):
        cap = cfg.max_active_orders if isinstance(cfg.max_active_orders, int) else 0
        plan = admission.plan(
            [admission.CellAdmission(cell=c, open_cycle=False) for c in preview.cells],
            cap=cap, mid=mid, rules=rules, order_amount_base=q)
        preview.armed = len(plan.armed)
        preview.queued = len(plan.queued)
        preview.slots_per_cell = {c.cell_id: admission.required_slots(q, rules, c.tp_price) for c in preview.cells}
        preview.min_valid_tp_qty = {c.cell_id: admission.min_valid_tp_qty(rules, c.tp_price) for c in preview.cells}
        preview.slots_reserved = plan.slots.reserved_unused + plan.slots.actual
        preview.slots_free = plan.slots.free
        preview.slot_cap = plan.slots.cap
    return preview
