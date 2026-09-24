"""Config preview before start (NG-UI-002, AC-46).

All arithmetic comes from the core package — ``grid.build_preview`` (integer-tick grid, anchor/sides,
full-Q validation, admission plan and slot ledger), ``risk.reachable_interval``,
``risk.required_margin_estimate`` and ``risk.margin_advisory`` — so the preview and the engine share
one implementation. This module only adds presentation: exact strings, Russian labels, cap/advisory
warnings and the ``preview_id``. Mid-dependent values (BUY/SELL split, reachable range, armed/queued)
are advisory: the anchor is fixed by the engine after full reconciliation at bootstrap.

``preview_id`` fingerprints what a Start confirmation is about: config (including the signed baseline
``expected_initial_position``), runtime rules and the committed revisions. It deliberately excludes the
moving mid price, so a Start is refused (409 + fresh preview) only when the grid/rules/revisions changed.
"""
from __future__ import annotations

import dataclasses
import hashlib
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

from hummingbot.strategy_v2.executors.neutral_grid_executor import grid as core_grid, risk as core_risk
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import GridConfig, TradingRules
from web.neutral_grid import jsonsafe

_ERROR_LABELS = (
    ("grid:", "Сетка"),
    ("leverage:", "Плечо"),
    ("order_amount_base:", "Размер ордера Q"),
    ("expected_initial_position:", "Baseline B"),
    ("max_active_orders:", "Лимит ордеров"),
    ("max_abs_net_position:", "Лимит |net|"),
    ("max_gross_position:", "Лимит gross"),
    ("mid_price:", "Средняя цена"),
    ("RULES_", "Правила рынка"),
    ("history_", "Параметры истории"),
    ("poll_interval_s:", "Интервал опроса"),
    ("settlement_", "Выдержка"),
    ("tp_gtt_seconds:", "Срок GTT для TP"),
    ("entry", "Тип ордера входа"),
    ("tp", "Тип ордера TP"),
    ("cell", "Ячейка"),
)


@dataclass(frozen=True)
class MarketContext:
    rules: Optional[TradingRules]
    mid: Optional[Decimal]
    available_collateral: Optional[Decimal]
    fetched_at: Optional[float]
    source: str  # "fake_exchange" | "snapshot" | "unavailable"
    errors: Tuple[str, ...] = ()   # market data problems that block Start (missing/stale rules, stale snapshot)


# Returns (config, None) or (None, reason). Demo: a fixed config; attach: the engine's committed config.
ConfigSource = Callable[[], Tuple[Optional[GridConfig], Optional[str]]]


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


def localize_error(message: str) -> str:
    """Prefix a core validation message with a Russian label; the exact technical text is kept."""
    for prefix, label in _ERROR_LABELS:
        if message.startswith(prefix):
            return f"{label}: {message}"
    return f"Проверка: {message}"


def _config_dict(cfg: GridConfig) -> Dict[str, Any]:
    return jsonsafe.make_safe(dataclasses.asdict(cfg))


def _rules_dict(rules: Optional[TradingRules]) -> Optional[Dict[str, Any]]:
    return None if rules is None else jsonsafe.make_safe(dataclasses.asdict(rules))


class PreviewService:
    def __init__(self, config: Union[GridConfig, ConfigSource], market: MarketSource, *, mode: str = "demo"):
        self._config_source: ConfigSource = (config if callable(config) else (lambda: (config, None)))
        self._market = market
        self.mode = mode

    def current_config(self) -> Tuple[Optional[GridConfig], Optional[str]]:
        return self._config_source()

    @property
    def config(self) -> Optional[GridConfig]:
        return self.current_config()[0]

    def preview_id(self, rules: Optional[TradingRules], config_revision: int, engine_revision: int,
                   cfg: Optional[GridConfig] = None) -> str:
        cfg = cfg if cfg is not None else self.config
        rules_view = _rules_dict(rules)
        if rules_view is not None:
            rules_view.pop("fetched_at", None)
        material = jsonsafe.canonical({"config": _config_dict(cfg) if cfg is not None else None, "rules": rules_view,
                                       "config_revision": config_revision, "engine_revision": engine_revision})
        return hashlib.sha256(material.encode()).hexdigest()[:24]

    async def build(self, *, config_revision: int, engine_revision: int,
                    baseline_confirmed_in_ledger: bool = False) -> Dict[str, Any]:
        cfg, cfg_error = self.current_config()
        ctx = await self._market()
        rules, mid = ctx.rules, ctx.mid
        if cfg is None:
            return {"mode": self.mode, "config_revision": config_revision, "engine_revision": engine_revision,
                    "preview_id": self.preview_id(rules, config_revision, engine_revision, cfg=None),
                    "config": None, "runtime_rules": _rules_dict(rules), "market_source": ctx.source,
                    "rules_fetched_at": ctx.fetched_at, "live_confirmation_required": not baseline_confirmed_in_ledger,
                    "errors": [f"Конфигурация движка недоступна: {cfg_error}"] + list(ctx.errors),
                    "warnings": [], "can_start": False}
        warnings: List[str] = []
        bootstrap = not baseline_confirmed_in_ledger
        baseline = cfg.expected_initial_position
        gp = core_grid.build_preview(cfg, rules, mid, baseline, bootstrap=bootstrap)

        errors: List[str] = [localize_error(message) for message in gp.errors] + list(ctx.errors)
        if self.mode != "demo" and not cfg.enabled:
            errors.append("enabled=false: live-старт невозможен без изменения конфигурации и явного подтверждения.")
        if baseline is None:
            warnings.append("Диапазон позиции ниже рассчитан для B=0, потому что expected_initial_position не задан.")

        cells = gp.cells
        q = cfg.order_amount_base
        p_min, p_max = gp.reachable_min, gp.reachable_max
        baseline_used = baseline
        if cells and baseline is None:
            baseline_used = Decimal(0)
            p_min, p_max = core_risk.reachable_interval(baseline_used, cells, q)
        cap = cfg.max_abs_net_position
        within_net = None if p_min is None else (p_max <= cap and p_min >= -cap)
        if within_net is False:
            warnings.append(f"Достижимый диапазон [{p_min}, {p_max}] выходит за лимит ±{cap}: движок будет "
                            "отклонять входы, которые могут вывести позицию за лимит (AC-24/25).")
        gross_worst = gp.gross_worst
        within_gross = None if gross_worst is None else gross_worst <= cfg.max_gross_position
        if within_gross is False:
            warnings.append(f"Худший gross {gross_worst} больше лимита {cfg.max_gross_position}: часть ячеек "
                            "не сможет открыть цикл одновременно.")
        if cells and gp.queued:
            warnings.append(f"Лимит ордеров позволяет вооружить {gp.armed} из {len(cells)} ячеек; остальные "
                            f"{gp.queued} ждут в очереди (выход всегда в приоритете).")

        margin_required = None
        if p_min is not None and p_max is not None:
            margin_required = core_risk.required_margin_estimate(p_min, p_max, mid, cfg.leverage)
        margin_warning, margin_blocks = core_risk.margin_advisory(ctx.available_collateral, margin_required)
        if margin_warning:
            warnings.append(("Маржа неизвестна — движок заблокирует новую экспозицию до получения данных: "
                             if margin_blocks else "Предупреждение о марже (решает биржа): ") + margin_warning)
        gross_notional = (gross_worst * mid) if (gross_worst is not None and mid is not None) else None

        slots = list(gp.slots_per_cell.values())
        tp_min = list(gp.min_valid_tp_qty.values())
        venue_cap = rules.max_active_orders_venue if rules is not None else None
        result: Dict[str, Any] = {
            "mode": self.mode,
            "config_revision": config_revision,
            "engine_revision": engine_revision,
            "preview_id": self.preview_id(rules, config_revision, engine_revision, cfg=cfg),
            "config": _config_dict(cfg),
            "runtime_rules": _rules_dict(rules),
            "market_source": ctx.source,
            "rules_fetched_at": ctx.fetched_at,
            "live_confirmation_required": bootstrap,
            "baseline": {
                "value": _s(baseline), "signed": signed(baseline) if baseline is not None else None,
                "source": "config" if baseline is not None else "missing",
                "confirmed_in_ledger": baseline_confirmed_in_ledger,
            },
            "grid": {"boundaries": len(gp.prices), "cells": len(cells) if cells else max(len(gp.prices) - 1, 0),
                     "prices": [format(p, "f") for p in gp.prices]},
            "anchor": {"mid": _s(mid), "anchor_estimate": _s(gp.anchor),
                       "note": "Якорь фиксируется движком после полной сверки при bootstrap; здесь — оценка по "
                               "текущей средней цене. Цены ячеек после старта не меняются."},
            "sides": {"buy": gp.buy_cells, "sell": gp.sell_cells, "known": bool(cells)},
            "admission": {
                "effective_cap": gp.slot_cap if cells else None, "configured_cap": cfg.max_active_orders,
                "venue_cap": venue_cap,
                "slots_per_cell_min": min(slots) if slots else None, "slots_per_cell_max": max(slots) if slots else None,
                "armed": gp.armed if slots else None, "queued": gp.queued if slots else None,
                "slots_actual": 0 if slots else None, "slots_reserved": gp.slots_reserved if slots else None,
                "slots_free": gp.slots_free if slots else None,
                "note": "Порядок вооружения: расстояние фиксированной цены входа до текущей цены, затем cell_id.",
            },
            "reachable": {"P_min": _s(p_min), "P_max": _s(p_max), "net_cap": _s(cap), "within_cap": within_net,
                          "baseline_used": _s(baseline_used)},
            "gross": {"worst": _s(gross_worst), "cap": _s(cfg.max_gross_position), "within_cap": within_gross},
            "leverage": {"configured": _s(cfg.leverage),
                         "venue_max": _s(rules.max_leverage) if rules is not None else None},
            "notional": {
                "mark": _s(mid),
                "net_notional_estimate": _s(gp.estimated_max_notional),
                "gross_notional_estimate": _s(gross_notional),
                "margin_estimate": _s(margin_required.quantize(Decimal("0.01"))) if margin_required is not None else None,
                "available_collateral": _s(ctx.available_collateral),
                "warning": margin_warning, "blocks_exposure": margin_blocks,
            },
            "floors": {
                "tick_size": _s(rules.tick_size) if rules else None,
                "size_step": _s(rules.size_step) if rules else None,
                "min_base": _s(rules.min_base) if rules else None,
                "min_notional": _s(rules.min_notional) if rules else None,
                "max_base": _s(rules.max_base) if rules else None,
                "min_valid_tp_qty_min": _s(min(tp_min)) if tp_min else None,
                "min_valid_tp_qty_max": _s(max(tp_min)) if tp_min else None,
                "freshness_s": _s(cfg.history_freshness_s), "settlement_delay_s": _s(cfg.settlement_delay_s),
                "settlement_scans": cfg.settlement_scans, "history_overlap_s": _s(cfg.history_overlap_s),
                "poll_interval_s": _s(cfg.poll_interval_s),
            },
            "errors": errors,
            "warnings": warnings,
            "can_start": not errors,
        }
        return result
