"""Wiring for the two launcher modes (see ``bin/lighter_robinhood_neutral_grid_web.py``).

* **attach** — the engine runs elsewhere (Hummingbot executor). The web opens its SQLite store with
  ``NeutralGridStore.open_command_client`` (read committed rows, INSERT into ``commands`` only). Market
  context for the preview comes from the latest committed snapshot, never from an exchange call.
* **demo** — fully offline: a temporary SQLite ledger, the package ``FakeExchange`` and exactly one
  ``NeutralGridEngine`` ticked by :class:`~web.neutral_grid.host.EngineHost`. Demo buttons drive only the
  fake venue (fills, history lag, cancel ambiguity); the engine learns about them through its own history
  scan like it would on a real venue.
"""
from __future__ import annotations

import dataclasses
import json
import logging
import tempfile
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    GridConfig,
    LegRole,
    OrderTypePolicy,
    Side,
    TradingRules,
)
from web.neutral_grid.gateway import StoreGateway
from web.neutral_grid.keystore import KeystoreService
from web.neutral_grid.preview import MarketContext, PreviewService
from web.neutral_grid.security import SecurityPolicy
from web.neutral_grid.server import DemoControls, WebContext
from web.neutral_grid.terminal import PublicCandleProvider, TerminalService

LOGGER = logging.getLogger("web.neutral_grid.runtime")
_DECIMAL_FIELDS = {f.name for f in dataclasses.fields(GridConfig)
                   if f.type in ("Decimal", "Optional[Decimal]", Decimal)}
_INT_FIELDS = {"account_index", "cell_count", "max_active_orders", "settlement_scans", "tp_gtt_seconds"}


@dataclass
class Bundle:
    context: WebContext
    closers: List[Callable[[], Awaitable[None]]] = field(default_factory=list)

    async def close(self) -> None:
        for closer in reversed(self.closers):
            try:
                await closer()
            except Exception:  # noqa: BLE001 - best effort shutdown
                LOGGER.exception("shutdown step failed")


class RealClock:
    """Wall clock with the ``now()``/``__call__`` shape FakeExchange and the engine expect."""

    def __call__(self) -> float:
        return time.time()

    def now(self) -> float:
        return time.time()


_POLICY_FIELDS = {"entry_order_type", "tp_order_type"}
_BOOL_FIELDS = {"enabled", "directional_outside_bounds_entries", "directional_gross_limits"}
_STR_FIELDS = {"grid_id", "connector_name", "trading_pair"}
_NULLABLE = {"expected_initial_position"}
_LEGACY_DEFAULTS = {"directional_outside_bounds_entries": False, "directional_gross_limits": False}


def grid_config_from_engine_json(data: Dict[str, Any]) -> GridConfig:
    """Strict GridConfig from the engine's published ``summary.engine_config`` (decimals as strings).

    No unknown key is accepted and no value is coerced from a float. Newly added default-false policy flags may
    be absent from a legacy snapshot; every other GridConfig field must be present (NG-ARCH-003).
    """
    if not isinstance(data, dict):
        raise ValueError("engine_config: ожидается объект")
    names = {f.name for f in dataclasses.fields(GridConfig)}
    keys = set(data) - {"fingerprint"}
    missing, unknown = sorted(names - keys - set(_LEGACY_DEFAULTS)), sorted(keys - names)
    if missing:
        raise ValueError(f"engine_config: нет полей {missing}")
    if unknown:
        raise ValueError(f"engine_config: неизвестные поля {unknown}")
    kwargs: Dict[str, Any] = dict(_LEGACY_DEFAULTS)
    for key in sorted(names & keys):
        value = data[key]
        if key in _DECIMAL_FIELDS:
            if value is None and key in _NULLABLE:
                kwargs[key] = None
                continue
            if not isinstance(value, str):
                raise ValueError(f"engine_config.{key}: десятичное значение должно быть строкой")
            try:
                parsed = Decimal(value)
            except ArithmeticError:
                raise ValueError(f"engine_config.{key}: не число") from None
            if not parsed.is_finite():
                raise ValueError(f"engine_config.{key}: не конечное число")
            kwargs[key] = parsed
        elif key in _INT_FIELDS:
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"engine_config.{key}: ожидается целое")
            kwargs[key] = value
        elif key in _POLICY_FIELDS:
            try:
                kwargs[key] = OrderTypePolicy(value)
            except ValueError:
                raise ValueError(f"engine_config.{key}: недопустимый тип ордера {value!r}") from None
        elif key in _BOOL_FIELDS:
            if not isinstance(value, bool):
                raise ValueError(f"engine_config.{key}: ожидается true/false")
            kwargs[key] = value
        elif key in _STR_FIELDS:
            if not isinstance(value, str) or not value:
                raise ValueError(f"engine_config.{key}: ожидается непустая строка")
            kwargs[key] = value
        else:  # pragma: no cover - a new GridConfig field must be classified above
            raise ValueError(f"engine_config.{key}: поле не поддерживается веб-адаптером")
    return GridConfig(**kwargs)


def engine_config_from_snapshot(snapshot: Optional[Dict[str, Any]]) -> Tuple[GridConfig, str]:
    """(config, fingerprint) from the committed snapshot; the fingerprint is re-computed by core and must match."""
    from hummingbot.strategy_v2.executors.neutral_grid_executor import grid as core_grid

    published = ((snapshot or {}).get("summary") or {}).get("engine_config")
    if published is None:
        raise ValueError("движок ещё не опубликовал summary.engine_config")
    config = grid_config_from_engine_json(published)
    fingerprint = published.get("fingerprint")
    if not isinstance(fingerprint, str) or fingerprint != core_grid.config_fingerprint(config):
        raise ValueError("engine_config.fingerprint не совпадает с пересчитанным core-отпечатком")
    return config, fingerprint


def engine_identity_view(cfg: GridConfig) -> Dict[str, Any]:
    return {"grid_id": cfg.grid_id, "connector_name": cfg.connector_name, "trading_pair": cfg.trading_pair,
            "account_index": cfg.account_index}


def _dec(value: Any) -> Optional[Decimal]:
    try:
        return None if value in (None, "") else Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None


def _rules_flag(rr: Dict[str, Any], name: str, errors: List[str]) -> bool:
    value = rr.get(name)
    if not isinstance(value, bool):
        errors.append(f"Правила рынка: {name} неизвестен в снимке движка — поддержка не предполагается.")
        return False
    return value


def _ordinary_limit_blocker(rr: Dict[str, Any], errors: List[str]) -> Optional[str]:
    value = rr.get("ordinary_limit_blocker")
    allowed = {None, "API_KEY_MAKER_ONLY", "MAKER_ONLY_CAPABILITY_UNKNOWN"}
    if value not in allowed:
        errors.append("Правила рынка: ordinary_limit_blocker неизвестен или повреждён — старт заблокирован.")
        return None
    return value


def snapshot_market(gateway: Any, config_source: Callable[[], Tuple[Optional[GridConfig], Optional[str]]],
                    stale_after_s: float, clock: Callable[[], float] = time.time
                    ) -> Callable[[], Awaitable[MarketContext]]:
    """Preview market context from the committed snapshot (attach mode never calls the exchange).

    Limit/post-only support, the rules fetch time and the engine's own rules staleness bound (``max_age_s``)
    come from ``summary.runtime_rules``; unknown support, a missing bound, rules older than it or a stale
    snapshot are preview errors (Start disabled). History freshness is judged separately (snapshot age, lag).
    """
    async def source() -> MarketContext:
        snapshot = gateway.latest_snapshot() or {}
        summary = snapshot.get("summary") or {}
        rr = summary.get("runtime_rules") or None
        errors: List[str] = []
        rules = None
        fetched_at: Optional[float] = None
        now = clock()
        if rr:
            try:
                fetched_raw = rr.get("fetched_at")
                fetched_at = float(fetched_raw) if fetched_raw not in (None, "") else None
                supports_limit = _rules_flag(rr, "supports_limit", errors)
                supports_post_only = _rules_flag(rr, "supports_post_only", errors)
                ordinary_limit_blocker = _ordinary_limit_blocker(rr, errors)
                rules = TradingRules(
                    tick_size=Decimal(str(rr["tick_size"])), size_step=Decimal(str(rr["size_step"])),
                    min_base=Decimal(str(rr["min_base"])), min_notional=Decimal(str(rr["min_notional"])),
                    max_base=_dec(rr.get("max_base")), max_leverage=_dec(rr.get("max_leverage")),
                    supports_limit=supports_limit, supports_post_only=supports_post_only,
                    fetched_at=fetched_at if fetched_at is not None else 0.0,
                    max_active_orders_venue=(int(rr["max_active_orders_venue"])
                                             if rr.get("max_active_orders_venue") not in (None, "") else None),
                    ordinary_limit_blocker=ordinary_limit_blocker)
            except (KeyError, ValueError, ArithmeticError, TypeError):
                rules = None
                errors.append("Правила рынка в снимке движка неполные или повреждены.")
        if rules is not None:
            max_age = _dec(rr.get("max_age_s"))
            if fetched_at is None:
                errors.append("Правила рынка: время получения (fetched_at) неизвестно.")
            elif max_age is None or max_age <= 0:
                errors.append("Правила рынка: движок не опубликовал max_age_s — свежесть правил не проверить.")
            elif Decimal(str(now)) - Decimal(str(fetched_at)) > max_age:
                errors.append(f"Правила рынка устарели: получены {now - fetched_at:.0f} с назад "
                              f"(предел движка {max_age} с).")
        committed_at = snapshot.get("committed_at")
        if committed_at is None or now - float(committed_at) > stale_after_s:
            errors.append("Снимок движка устарел или отсутствует: превью по нему недостоверно.")
        bid, ask = _dec(summary.get("bid")), _dec(summary.get("ask"))
        mid = (bid + ask) / 2 if (bid is not None and ask is not None) else None
        available = _dec((summary.get("margin") or {}).get("available"))
        return MarketContext(rules=rules, mid=mid, available_collateral=available, fetched_at=fetched_at,
                             source="snapshot", errors=tuple(errors))
    return source


def health_file_provider(db_path: Path) -> Callable[[], Dict[str, Any]]:
    """Engine sidecar ``<db_path>.health.json`` (atomic write+fsync+replace by the engine).

    It carries the persistence/fatal error the engine cannot commit into a snapshot while its store is
    failing. Absent or unreadable -> ``known=False``; the caller decides how to present that.
    """
    path = Path(str(db_path) + ".health.json")

    def read() -> Dict[str, Any]:
        base: Dict[str, Any] = {"source": "health_file", "path": path.name}
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return dict(base, known=False, detail="нет файла здоровья движка")
        except OSError:
            return dict(base, known=False, detail="файл здоровья движка недоступен")
        try:
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError("not an object")
            at = data.get("at")
            revision = data.get("engine_revision")
            return dict(base, known=True,
                        persistence_error=_text_or_none(data.get("persistence_error")),
                        fatal_reason=_text_or_none(data.get("fatal_reason")),
                        at=float(at) if at not in (None, "") else None,
                        engine_revision=revision if isinstance(revision, int) and not isinstance(revision, bool)
                        else None)
        except (ValueError, TypeError):
            return dict(base, known=False, detail="файл здоровья движка повреждён")
    return read


def _text_or_none(value: Any) -> Optional[str]:
    return None if value in (None, "") else str(value)


def _security_policy(args: Any) -> SecurityPolicy:
    return SecurityPolicy(extra_hostnames=frozenset(getattr(args, "allowed_host", None) or []))


def attach_context(gateway: Any, args: Any, *, health_provider: Callable[[], Dict[str, Any]]) -> WebContext:
    """Attach mode: config, baseline check and identity all come from the engine's committed snapshot."""
    def config_source() -> Tuple[Optional[GridConfig], Optional[str]]:
        try:
            return engine_config_from_snapshot(gateway.latest_snapshot())[0], None
        except ValueError as exc:
            return None, str(exc)

    def identity() -> Dict[str, Any]:
        cfg, error = config_source()
        return engine_identity_view(cfg) if cfg is not None else {"grid_id": None, "config_error": error}

    preview = PreviewService(config_source, snapshot_market(gateway, config_source, args.stale_after), mode="attach")
    context = WebContext(
        gateway=gateway, preview=preview, keystore=None, engine_identity={},
        identity_provider=identity, mode="attach", bind_host=args.host, stale_after_s=args.stale_after,
        policy=_security_policy(args), health_provider=health_provider)
    context.terminal = TerminalService(gateway, PublicCandleProvider(), context.identity)
    return context


async def build_attach(args: Any) -> Bundle:
    gateway = StoreGateway.open(args.attach_db)
    context = attach_context(gateway, args, health_provider=health_file_provider(Path(args.attach_db)))

    async def close_gateway() -> None:
        gateway.close()
    return Bundle(context, [close_gateway])


# ------------------------------------------------------------------------------------------------ demo
DEMO_CONFIG = dict(
    grid_id="demo-lit-usdg", connector_name="lighter_perpetual_robinhood", trading_pair="LIT-USDG",
    account_index=4242, lower_price=Decimal("5"), upper_price=Decimal("6"), cell_count=55,
    order_amount_base=Decimal("10"), leverage=Decimal("5"), expected_initial_position=Decimal("0"),
    max_abs_net_position=Decimal("1000"), max_gross_position=Decimal("1000"), max_active_orders=120,
    settlement_delay_s=Decimal("2"), enabled=False,
)

DEMO_ACTIONS = {
    "partial_entry": "Частично исполнить ближайший вход (+3 LIT)",
    "fill_entry_rest": "Исполнить остаток ближайшего входа",
    "partial_tp": "Частично исполнить ближайший TP (+2 LIT)",
    "dust": "Пыль: вход +2 LIT, затем биржа отменяет остаток",
    "history_lag_on": "Задержка истории 30 с (WS приходит сразу)",
    "history_lag_off": "Убрать задержку истории",
    "cancel_blackhole": "Отмены «теряются» (итог неизвестен) — для STOP_UNCERTAIN",
    "price_up": "Цена +0.02",
    "price_down": "Цена −0.02",
    "manual_trade": "Ручная сделка вне бота (+1 LIT)",
}


class DemoDriver:
    """Drives the FakeExchange only; reads the store to pick an ENTRY/TP order by its durable role."""

    def __init__(self, fake: Any, store_reader: Any):
        self.fake = fake
        self.store = store_reader

    def _owned_open(self, role: LegRole) -> List[Any]:
        orders = []
        for order in self.fake.open_orders(owned=True):
            leg = self.store.leg(order.client_order_id) if order.client_order_id is not None else None
            if leg is not None and leg.role == role:
                orders.append(order)
        mid = self._mid()
        orders.sort(key=lambda o: (abs(o.price - mid) if mid is not None else Decimal(0), int(o.order_index)))
        return orders

    def _mid(self) -> Optional[Decimal]:
        if self.fake.bid is None or self.fake.ask is None:
            return None
        return (self.fake.bid + self.fake.ask) / 2

    async def run(self, action: str) -> Dict[str, Any]:
        fake = self.fake
        if action in ("partial_entry", "fill_entry_rest", "dust"):
            entries = self._owned_open(LegRole.ENTRY)
            if action == "dust":  # a pristine entry, so the exact partial fill is what stays below the floor
                entries = [o for o in entries if o.filled == 0]
            if not entries:
                return {"ok": False, "message": "нет подходящих активных входов (движок ещё не выставил ордера?)"}
            order = entries[0]
            qty = {"partial_entry": min(Decimal(3), order.remaining), "fill_entry_rest": order.remaining,
                   "dust": min(Decimal(2), order.remaining)}[action]
            trade_id = fake.fill(order.client_order_id, qty)
            message = f"исполнено {qty} по ордеру {order.client_order_id} (trade {trade_id})"
            if action == "dust" and order.is_open:
                fake.venue_cancel(order.client_order_id)
                message += "; остаток отменён биржей"
            return {"ok": True, "message": message, "cid": str(order.client_order_id), "qty": str(qty)}
        if action == "partial_tp":
            tps = self._owned_open(LegRole.TP)
            if not tps:
                return {"ok": False, "message": "нет активных TP"}
            order = tps[0]
            qty = min(Decimal(2), order.remaining)
            trade_id = fake.fill(order.client_order_id, qty)
            return {"ok": True, "message": f"TP {order.client_order_id}: исполнено {qty} (trade {trade_id})"}
        if action == "history_lag_on":
            fake.history_lag_s = 30.0
            return {"ok": True, "message": "история биржи отстаёт на 30 с"}
        if action == "history_lag_off":
            fake.history_lag_s = 1.0
            return {"ok": True, "message": "задержка истории 1 с"}
        if action == "cancel_blackhole":
            from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import CancelBehavior
            count = max(8, 6 * len(fake.open_orders(owned=True)))
            for _ in range(count):
                fake.script_cancel(CancelBehavior.TIMEOUT_NOT_LANDED)
            return {"ok": True, "message": f"следующие {count} отмен вернут неизвестный итог"}
        if action in ("price_up", "price_down"):
            mid = self._mid() or Decimal("5.4")
            step = Decimal("0.02") if action == "price_up" else Decimal("-0.02")
            fake.set_mid(mid + step)
            return {"ok": True, "message": f"средняя цена {mid + step}"}
        if action == "manual_trade":
            mid = self._mid() or Decimal("5.4")
            trade_id = fake.manual_trade(Side.BUY, Decimal(1), mid)
            return {"ok": True, "message": f"ручная сделка вне бота (trade {trade_id})"}
        return {"ok": False, "message": "неизвестное действие"}


async def build_demo(args: Any) -> Bundle:
    from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import EngineOptions
    from hummingbot.strategy_v2.executors.neutral_grid_executor.engine import open_engine
    from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import FakeExchange
    from web.neutral_grid.host import EngineHost

    data_dir = Path(args.data_dir) if getattr(args, "data_dir", None) else Path(tempfile.mkdtemp(prefix="ng-web-demo-"))
    data_dir.mkdir(parents=True, exist_ok=True)
    clock = RealClock()
    config = GridConfig(**DEMO_CONFIG)
    fake = FakeExchange(clock, account_index=config.account_index, history_lag_s=1.0)
    fake.auto_fill_crossing_limit = True
    db_path = data_dir / "neutral_grid_demo.sqlite3"
    options = EngineOptions(stop_uncertain_after_s=30.0, cancel_retry_s=5.0)
    # The engine's own factory opens the single-writer store (host lock + owner fencing); the web never does.
    engine = open_engine(config, str(db_path), fake, clock=clock, options=options, lock_dir=str(data_dir / "locks"),
                         create_if_missing=True, offline_demo=True)
    if engine.store is None:
        raise RuntimeError(f"demo engine store refused: {engine.fatal_reason}")
    writer = engine.store
    identity_id = f"{fake.domain}:{config.account_index}:{config.trading_pair}"
    await engine.tick()  # first committed snapshot (AWAITING_START) before the UI connects
    wake = getattr(engine, "wake", None)
    if callable(wake) and hasattr(fake, "add_ws_listener"):
        fake.add_ws_listener(wake)  # WS signal only wakes the history scan; it is never proof (NG-HIST-001)
    host = EngineHost(engine, identity_id, tick_interval_s=options.tick_interval_s)
    host.start()
    gateway = StoreGateway.open(db_path)

    async def market() -> MarketContext:
        rules = await fake.trading_rules()
        position = await fake.position()
        return MarketContext(rules=rules, mid=await fake.mid_price(), available_collateral=position.available_collateral,
                             fetched_at=rules.fetched_at, source="fake_exchange")

    driver = DemoDriver(fake, gateway._store)
    context = WebContext(
        gateway=gateway, preview=PreviewService(config, market, mode="demo"), keystore=KeystoreService(demo=True),
        engine_identity=dict(engine_identity_view(config), db_path=str(db_path)), mode="demo",
        bind_host=args.host, stale_after_s=args.stale_after, host_status=host.status, health_provider=host.health,
        demo=DemoControls(actions=dict(DEMO_ACTIONS), run=driver.run), policy=_security_policy(args))

    async def close_all() -> None:
        await host.close()
        gateway.close()
        writer.close()
    bundle = Bundle(context, [close_all])
    bundle.fake = fake  # type: ignore[attr-defined]  (tests drive the fake venue directly)
    bundle.engine = engine  # type: ignore[attr-defined]
    bundle.data_dir = data_dir  # type: ignore[attr-defined]
    return bundle
