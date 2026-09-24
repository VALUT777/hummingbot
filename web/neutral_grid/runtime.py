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
import logging
import tempfile
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

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


def grid_config_from_mapping(data: Dict[str, Any]) -> GridConfig:
    """Exact GridConfig from YAML/JSON (decimals via ``Decimal(str)``, never float arithmetic)."""
    names = {f.name for f in dataclasses.fields(GridConfig)}
    kwargs: Dict[str, Any] = {}
    for key, value in data.items():
        if key not in names:
            continue
        if key in _DECIMAL_FIELDS and value is not None:
            kwargs[key] = Decimal(str(value))
        elif key in _INT_FIELDS:
            kwargs[key] = int(value)
        elif key in ("entry_order_type", "tp_order_type"):
            kwargs[key] = OrderTypePolicy(str(value).upper())
        elif key == "enabled":
            kwargs[key] = value is True
        else:
            kwargs[key] = value
    return GridConfig(**kwargs)


def load_grid_config(path: Path) -> GridConfig:
    import yaml

    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path}: ожидается YAML-объект с полями GridConfig")
    return grid_config_from_mapping(data)


def engine_identity_view(cfg: GridConfig) -> Dict[str, Any]:
    return {"grid_id": cfg.grid_id, "connector_name": cfg.connector_name, "trading_pair": cfg.trading_pair,
            "account_index": cfg.account_index}


def _dec(value: Any) -> Optional[Decimal]:
    try:
        return None if value in (None, "") else Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None


def snapshot_market(gateway: StoreGateway) -> Callable[[], Awaitable[MarketContext]]:
    """Preview market context from the committed snapshot (attach mode never calls the exchange)."""
    async def source() -> MarketContext:
        snapshot = gateway.latest_snapshot() or {}
        summary = snapshot.get("summary") or {}
        rr = summary.get("runtime_rules") or None
        rules = None
        if rr:
            try:
                rules = TradingRules(
                    tick_size=Decimal(rr["tick_size"]), size_step=Decimal(rr["size_step"]),
                    min_base=Decimal(rr["min_base"]), min_notional=Decimal(rr["min_notional"]),
                    max_base=_dec(rr.get("max_base")), max_leverage=_dec(rr.get("max_leverage")),
                    supports_limit=True, supports_post_only=True,
                    fetched_at=float(rr.get("fetched_at") or 0.0),
                    max_active_orders_venue=(int(rr["max_active_orders_venue"])
                                             if rr.get("max_active_orders_venue") not in (None, "") else None))
            except (KeyError, ValueError, ArithmeticError):
                rules = None
        bid, ask = _dec(summary.get("bid")), _dec(summary.get("ask"))
        mid = (bid + ask) / 2 if (bid is not None and ask is not None) else None
        available = _dec((summary.get("margin") or {}).get("available"))
        return MarketContext(rules=rules, mid=mid, available_collateral=available,
                             fetched_at=snapshot.get("committed_at"), source="snapshot")
    return source


def _security_policy(args: Any) -> SecurityPolicy:
    return SecurityPolicy(extra_hostnames=frozenset(getattr(args, "allowed_host", None) or []))


async def build_attach(args: Any) -> Bundle:
    if args.config is None:
        raise ValueError("--attach-db требует --config с конфигурацией сетки (для превью)")
    config = load_grid_config(args.config)
    gateway = StoreGateway.open(args.attach_db)
    preview = PreviewService(config, snapshot_market(gateway), mode="attach")
    context = WebContext(
        gateway=gateway, preview=preview, keystore=KeystoreService(demo=False),
        engine_identity=engine_identity_view(config), mode="attach", bind_host=args.host,
        stale_after_s=args.stale_after, policy=_security_policy(args))

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
            if not entries:
                return {"ok": False, "message": "нет активных входов (движок ещё не выставил ордера?)"}
            order = entries[0]
            qty = {"partial_entry": min(Decimal(3), order.remaining), "fill_entry_rest": order.remaining,
                   "dust": min(Decimal(2), order.remaining)}[action]
            trade_id = fake.fill(order.client_order_id, qty)
            message = f"исполнено {qty} по ордеру {order.client_order_id} (trade {trade_id})"
            if action == "dust" and order.is_open:
                fake.venue_cancel(order.client_order_id)
                message += "; остаток отменён биржей"
            return {"ok": True, "message": message}
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
    from hummingbot.strategy_v2.executors.neutral_grid_executor import engine as engine_module
    from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import FakeExchange
    from hummingbot.strategy_v2.executors.neutral_grid_executor.store import EngineIdentity, NeutralGridStore
    from web.neutral_grid.host import EngineHost

    data_dir = Path(args.data_dir) if getattr(args, "data_dir", None) else Path(tempfile.mkdtemp(prefix="ng-web-demo-"))
    data_dir.mkdir(parents=True, exist_ok=True)
    clock = RealClock()
    config = GridConfig(**DEMO_CONFIG)
    fake = FakeExchange(clock, account_index=config.account_index, history_lag_s=1.0)
    fake.auto_fill_crossing_limit = True
    identity = EngineIdentity(connector_name=config.connector_name, connector_domain=fake.domain,
                              account_index=config.account_index, trading_pair=config.trading_pair)
    db_path = data_dir / "neutral_grid_demo.sqlite3"
    writer = NeutralGridStore.open(db_path, identity, create_if_missing=True, lock_dir=data_dir / "locks")
    options = None
    options_cls = getattr(engine_module, "EngineOptions", None)
    if options_cls is None:
        try:
            from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import EngineOptions as options_cls
        except ImportError:  # pragma: no cover - engine without options
            options_cls = None
    if options_cls is not None:
        options = options_cls(stop_uncertain_after_s=30.0, cancel_retry_s=5.0)
    engine = engine_module.NeutralGridEngine(config, writer, fake, clock, options=options, offline_demo=True)
    host = EngineHost(engine, identity.engine_id, tick_interval_s=1.0)
    await engine.tick()  # first committed snapshot (AWAITING_START) before the UI connects
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
        bind_host=args.host, stale_after_s=args.stale_after, host_status=host.status,
        demo=DemoControls(actions=dict(DEMO_ACTIONS), run=driver.run), policy=_security_policy(args))

    async def close_all() -> None:
        await host.close()
        gateway.close()
        writer.close()
    bundle = Bundle(context, [close_all])
    bundle.fake = fake  # type: ignore[attr-defined]  (tests and --demo-seed-fills)
    bundle.engine = engine  # type: ignore[attr-defined]
    bundle.data_dir = data_dir  # type: ignore[attr-defined]
    return bundle
