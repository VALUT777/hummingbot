"""Config preview before start (NG-UI-002, AC-46).

The arithmetic is delegated to the core package (``grid.build_grid``, ``grid.assign_cells``,
``grid.validate_config``, ``admission.required_slots``) through :class:`GridCore`, so the preview and
the engine share one implementation. Values that depend on the current mid price (BUY/SELL split,
reachable range, armed/queued) are advisory: the anchor is fixed only by the engine after full
reconciliation at bootstrap.

``preview_id`` fingerprints what a Start confirmation is about: config, runtime rules and the
committed revisions. It deliberately excludes the moving mid price and the operator's baseline input,
so a Start is refused (409 + fresh preview) only when the grid/rules/revisions actually changed.
"""
from __future__ import annotations

import dataclasses
import hashlib
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Dict, List, Optional, Protocol

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CellSpec,
    GridConfig,
    OrderTypePolicy,
    Side,
    TradingRules,
)
from web.neutral_grid import jsonsafe


class GridCore(Protocol):
    grid_error: type

    def build_grid(self, lower: Decimal, upper: Decimal, cell_count: int, rules: TradingRules) -> List[Decimal]: ...

    def assign_cells(self, prices: List[Decimal], anchor: Decimal) -> List[CellSpec]: ...

    def validate_config(self, cfg: GridConfig, rules: TradingRules, mid: Optional[Decimal]) -> List[str]: ...

    def required_slots(self, q: Decimal, rules: TradingRules, tp_price: Decimal) -> int: ...


class PackageGridCore:
    """Adapter over the core package (WS-A) — the single implementation of grid/admission math."""

    def __init__(self) -> None:
        from hummingbot.strategy_v2.executors.neutral_grid_executor import admission, grid
        self._grid = grid
        self._admission = admission
        self.grid_error = getattr(grid, "GridValidationError", ValueError)

    def build_grid(self, lower, upper, cell_count, rules):
        return list(self._grid.build_grid(lower, upper, cell_count, rules))

    def assign_cells(self, prices, anchor):
        return list(self._grid.assign_cells(prices, anchor))

    def validate_config(self, cfg, rules, mid):
        return [str(e) for e in self._grid.validate_config(cfg, rules, mid)]

    def required_slots(self, q, rules, tp_price):
        return int(self._admission.required_slots(q, rules, tp_price))


@dataclass(frozen=True)
class MarketContext:
    rules: Optional[TradingRules]
    mid: Optional[Decimal]
    available_collateral: Optional[Decimal]
    fetched_at: Optional[float]
    source: str  # "fake_exchange" | "snapshot" | "unavailable"


MarketSource = Callable[[], Awaitable[MarketContext]]


def _s(value: Optional[Decimal]) -> Optional[str]:
    return None if value is None else format(value, "f")


def signed(value: Decimal) -> str:
    text = format(value, "f")
    return text if value < 0 else "+" + text


def parse_signed_decimal(value: object) -> Decimal:
    """Strict signed decimal from a JSON string (never from a float)."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("ожидается десятичное число строкой, например \"0\", \"330\" или \"-120.5\"")
    text = value.strip().replace(",", ".")
    if not all(ch in "+-0123456789." for ch in text):
        raise ValueError("допустимы только цифры, точка и знак")
    try:
        parsed = Decimal(text)
    except InvalidOperation:
        raise ValueError("не удалось разобрать число") from None
    if not parsed.is_finite():
        raise ValueError("число должно быть конечным")
    return parsed


def _config_dict(cfg: GridConfig) -> Dict[str, Any]:
    data = dataclasses.asdict(cfg)
    return jsonsafe.make_safe(data)


def _rules_dict(rules: Optional[TradingRules]) -> Optional[Dict[str, Any]]:
    return None if rules is None else jsonsafe.make_safe(dataclasses.asdict(rules))


def _quantize_up(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


class PreviewService:
    def __init__(self, config: GridConfig, market: MarketSource, *, core: Optional[GridCore] = None,
                 mode: str = "demo"):
        self.config = config
        self._market = market
        self._core = core
        self.mode = mode

    def core(self) -> GridCore:
        if self._core is None:
            self._core = PackageGridCore()
        return self._core

    def preview_id(self, rules: Optional[TradingRules], config_revision: int, engine_revision: int) -> str:
        rules_view = _rules_dict(rules)
        if rules_view is not None:
            rules_view.pop("fetched_at", None)
        material = jsonsafe.canonical({"config": _config_dict(self.config), "rules": rules_view,
                                       "config_revision": config_revision, "engine_revision": engine_revision})
        return hashlib.sha256(material.encode()).hexdigest()[:24]

    async def build(self, *, config_revision: int, engine_revision: int,
                    baseline_override: Optional[Decimal] = None,
                    baseline_confirmed_in_ledger: bool = False) -> Dict[str, Any]:
        cfg = self.config
        ctx = await self._market()
        errors: List[str] = []
        warnings: List[str] = []
        rules = ctx.rules
        result: Dict[str, Any] = {
            "mode": self.mode,
            "config_revision": config_revision,
            "engine_revision": engine_revision,
            "preview_id": self.preview_id(rules, config_revision, engine_revision),
            "config": _config_dict(cfg),
            "runtime_rules": _rules_dict(rules),
            "market_source": ctx.source,
            "rules_fetched_at": ctx.fetched_at,
        }
        if self.mode != "demo" and not cfg.enabled:
            errors.append("enabled=false: live-старт невозможен без изменения конфигурации и явного подтверждения.")
        for label, policy in (("entry", cfg.entry_order_type), ("TP", cfg.tp_order_type)):
            if not isinstance(policy, OrderTypePolicy):
                errors.append(f"Недопустимый тип ордера {label}: {policy} (MARKET запрещён).")

        baseline = baseline_override if baseline_override is not None else cfg.expected_initial_position
        result["baseline"] = {
            "value": _s(baseline), "signed": signed(baseline) if baseline is not None else None,
            "source": "operator_input" if baseline_override is not None else (
                "config" if cfg.expected_initial_position is not None else "missing"),
            "confirmed_in_ledger": baseline_confirmed_in_ledger,
        }
        result["live_confirmation_required"] = not baseline_confirmed_in_ledger
        if baseline is None:
            warnings.append("expected_initial_position не задан: его нужно ввести и подтвердить при первом старте. "
                            "Диапазон ниже рассчитан для B=0.")
        b = baseline if baseline is not None else Decimal(0)

        if rules is None:
            errors.append("Нет свежих торговых правил рынка: превью и старт невозможны (NG-RISK-004).")
            result.update(errors=errors, warnings=warnings, can_start=False)
            return result

        core = self.core()
        try:
            prices = core.build_grid(cfg.lower_price, cfg.upper_price, cfg.cell_count, rules)
        except core.grid_error as exc:
            errors.append(f"Сетка: {exc}")
            prices = []
        except (ValueError, ArithmeticError) as exc:
            errors.append(f"Сетка: {exc}")
            prices = []
        errors.extend(e for e in core.validate_config(cfg, rules, ctx.mid) if e not in errors)

        result["grid"] = {
            "boundaries": len(prices), "cells": max(len(prices) - 1, 0),
            "prices": [format(p, "f") for p in prices],
        }
        mid = ctx.mid
        anchor = None
        if mid is not None and prices:
            anchor = min(max(mid, cfg.lower_price), cfg.upper_price)
        result["anchor"] = {"mid": _s(mid), "anchor_estimate": _s(anchor),
                            "note": "Якорь фиксируется движком после полной сверки при bootstrap; "
                                    "здесь — оценка по текущей средней цене."}
        cells: List[CellSpec] = core.assign_cells(prices, anchor) if (anchor is not None and prices) else []
        buys = sum(1 for c in cells if c.entry_side == Side.BUY)
        sells = len(cells) - buys
        result["sides"] = {"buy": buys, "sell": sells, "known": bool(cells)}

        q = cfg.order_amount_base
        slots_per_cell: List[int] = []
        tp_min_qtys: List[Decimal] = []
        for cell in cells:
            slots_per_cell.append(core.required_slots(q, rules, cell.tp_price))
            tp_min_qtys.append(_quantize_up(max(rules.min_base, rules.min_notional / cell.tp_price), rules.size_step))
        venue_cap = rules.max_active_orders_venue
        effective_cap = cfg.max_active_orders if venue_cap is None else min(cfg.max_active_orders, venue_cap)
        if venue_cap is not None and cfg.max_active_orders > venue_cap:
            warnings.append(f"max_active_orders={cfg.max_active_orders} выше лимита площадки {venue_cap}; "
                            f"используется {effective_cap}.")
        armed_ids: List[int] = []
        reserved = 0
        if cells and mid is not None:
            order = sorted(zip(cells, slots_per_cell),
                           key=lambda pair: (abs(pair[0].entry_price - mid), pair[0].cell_id))
            for cell, need in order:
                if reserved + need > effective_cap:
                    break  # strict queue order: no skipping ahead to a cheaper cell
                armed_ids.append(cell.cell_id)
                reserved += need
        result["admission"] = {
            "effective_cap": effective_cap, "configured_cap": cfg.max_active_orders, "venue_cap": venue_cap,
            "slots_per_cell_min": min(slots_per_cell) if slots_per_cell else None,
            "slots_per_cell_max": max(slots_per_cell) if slots_per_cell else None,
            "armed": len(armed_ids) if cells else None, "queued": (len(cells) - len(armed_ids)) if cells else None,
            "slots_actual": 0, "slots_reserved": reserved if cells else None,
            "slots_free": (effective_cap - reserved) if cells else None,
            "note": "Порядок вооружения: расстояние фиксированной цены входа до текущей цены, затем cell_id.",
        }
        if cells and len(armed_ids) < len(cells):
            warnings.append(f"Лимит ордеров позволяет вооружить {len(armed_ids)} из {len(cells)} ячеек; "
                            f"остальные {len(cells) - len(armed_ids)} ждут в очереди.")

        p_max = b + q * buys
        p_min = b - q * sells
        cap = cfg.max_abs_net_position
        within_net = p_max <= cap and p_min >= -cap
        result["reachable"] = {"P_min": _s(p_min) if cells else None, "P_max": _s(p_max) if cells else None,
                               "net_cap": _s(cap), "within_cap": within_net if cells else None,
                               "baseline_used": _s(b)}
        if cells and not within_net:
            errors.append(f"Достижимый диапазон позиции [{p_min}, {p_max}] выходит за max_abs_net_position ±{cap}.")
        gross_worst = q * len(cells)
        result["gross"] = {"worst": _s(gross_worst) if cells else None, "cap": _s(cfg.max_gross_position),
                           "within_cap": (gross_worst <= cfg.max_gross_position) if cells else None}
        if cells and gross_worst > cfg.max_gross_position:
            errors.append(f"Худший gross {gross_worst} превышает max_gross_position {cfg.max_gross_position}.")
        result["leverage"] = {"configured": _s(cfg.leverage), "venue_max": _s(rules.max_leverage)}
        if rules.max_leverage is not None and cfg.leverage > rules.max_leverage:
            errors.append(f"Плечо {cfg.leverage} выше максимума площадки {rules.max_leverage}.")
        if cfg.leverage <= 0:
            errors.append("Плечо должно быть положительным.")

        mark = mid
        notional = (gross_worst * mark) if (mark is not None and cells) else None
        margin = (notional / cfg.leverage) if (notional is not None and cfg.leverage > 0) else None
        available = ctx.available_collateral
        margin_warning = None
        margin_blocks = False
        if margin is None or available is None or not available.is_finite() or available < 0:
            margin_warning = "Маржа неизвестна: движок заблокирует новую экспозицию до получения данных (NG-RISK-004)."
            margin_blocks = True
        elif margin > available:
            margin_warning = (f"Оценка маржи {margin.quantize(Decimal('0.01'))} больше доступного "
                              f"{available} USDG — только предупреждение; правила маржи применит биржа.")
        result["notional"] = {
            "mark": _s(mark), "gross_notional_estimate": _s(notional.quantize(Decimal("0.01"))) if notional else None,
            "margin_estimate": _s(margin.quantize(Decimal("0.01"))) if margin is not None else None,
            "available_collateral": _s(available), "warning": margin_warning, "blocks_exposure": margin_blocks,
        }
        if margin_warning:
            warnings.append(margin_warning)
        result["floors"] = {
            "tick_size": _s(rules.tick_size), "size_step": _s(rules.size_step), "min_base": _s(rules.min_base),
            "min_notional": _s(rules.min_notional), "max_base": _s(rules.max_base),
            "min_valid_tp_qty_min": _s(min(tp_min_qtys)) if tp_min_qtys else None,
            "min_valid_tp_qty_max": _s(max(tp_min_qtys)) if tp_min_qtys else None,
            "freshness_s": _s(cfg.history_freshness_s), "settlement_delay_s": _s(cfg.settlement_delay_s),
            "settlement_scans": cfg.settlement_scans, "history_overlap_s": _s(cfg.history_overlap_s),
            "poll_interval_s": _s(cfg.poll_interval_s),
        }
        result.update(errors=errors, warnings=warnings, can_start=not errors)
        return result
