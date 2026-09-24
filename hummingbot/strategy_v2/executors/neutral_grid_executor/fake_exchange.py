"""Deterministic offline exchange implementing ``contracts.ExchangePort``.

Used by the engine integration tests and by the offline web demo. It never touches the
network. Everything that matters for the neutral grid safety arguments is controllable:

* partial fills, self-trades (two own legs of one trade), manual (unowned) orders/trades;
* WebSocket signal vs. authoritative history lag (per event or global);
* newest->oldest cursor pagination with ``limit<=100`` and opaque cursors, >100 rows;
* injected pagination faults: duplicate boundary rows, reordering, repeated/malformed
  cursors, conflicting duplicates, page errors;
* submit/cancel transport outcomes: accept, timeout with/without the side effect landing,
  documented definitive zero-fill reject, post-only cancellation, raised exceptions,
  cancel not-found ambiguity;
* runtime trading-rule changes, history retention window, request weight accounting.

Ids are exact strings/ints; quantities are Decimal. No float is used for quantities or ids;
timestamps are unix seconds (float) only because ``contracts`` defines them that way.
"""
from __future__ import annotations

import base64
import json
import random
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Callable, Deque, Dict, List, Optional, Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    ExchangeOrderRow,
    ExchangeTradeRow,
    HistoryPage,
    OrderTypePolicy,
    PositionSnapshot,
    Side,
    SubmitRequest,
    TradingRules,
    TransportOutcome,
    TransportResult,
)

TRADES_ENDPOINT = "trades"
INACTIVE_ENDPOINT = "inactive_orders"
ACTIVE_ENDPOINT = "active_orders"
POSITION_ENDPOINT = "account"            # same endpoint names as history.ENDPOINT_* / LighterExchangePort
RULES_ENDPOINT = "order_book_details"
SEND_TX_ENDPOINT = "send_tx"

# Standard account weights (spec NG-HIST-003).
DEFAULT_WEIGHTS: Dict[str, int] = {
    TRADES_ENDPOINT: 600,
    INACTIVE_ENDPOINT: 100,
    SEND_TX_ENDPOINT: 6,
}
DEFAULT_OTHER_WEIGHT = 300
DEFAULT_WEIGHT_POOL_PER_MIN = 18000

# Exchange ids start above 2**53 so any float round-trip is detectable (AC-22).
FIRST_ORDER_INDEX = (1 << 53) + 1001
FIRST_TRADE_ID = (1 << 60) + 7


class FakeClock:
    """Injectable monotonic clock (unix seconds)."""

    def __init__(self, start: float = 1_790_000_000.0):
        self._now = float(start)

    def __call__(self) -> float:
        return self._now

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> float:
        if seconds < 0:
            raise ValueError("clock cannot go backwards")
        self._now += float(seconds)
        return self._now

    def set(self, value: float) -> None:
        if value < self._now:
            raise ValueError("clock cannot go backwards")
        self._now = float(value)


class SubmitBehavior(str, Enum):
    ACCEPT = "ACCEPT"
    TIMEOUT_LANDED = "TIMEOUT_LANDED"            # order created, caller sees UNKNOWN
    TIMEOUT_NOT_LANDED = "TIMEOUT_NOT_LANDED"    # nothing created, caller sees UNKNOWN
    REJECT_ZERO_FILL = "REJECT_ZERO_FILL"        # documented definitive reject, nothing created
    RAISE = "RAISE"                              # transport raises (e.g. connection reset)
    RAISE_LANDED = "RAISE_LANDED"                # transport raises after the order landed


class CancelBehavior(str, Enum):
    ACCEPT = "ACCEPT"
    TIMEOUT_LANDED = "TIMEOUT_LANDED"
    TIMEOUT_NOT_LANDED = "TIMEOUT_NOT_LANDED"
    NOT_FOUND = "NOT_FOUND"                      # venue says unknown order: NOT a terminal proof
    RAISE = "RAISE"


class FakeTransportError(IOError):
    pass


class FakeRateLimitExceeded(IOError):
    pass


class FakeDataUnavailable(IOError):
    pass


OPEN_STATUS = "open"
FILLED_STATUS = "filled"
CANCELED_STATUS = "canceled"
CANCELED_POST_ONLY = "canceled-post-only"
CANCELED_EXPIRED = "canceled-expired"
CANCELED_SELF_TRADE = "canceled-self-trade"


@dataclass
class FakeOrder:
    order_index: str
    client_order_id: Optional[int]
    side: Side
    price: Decimal
    initial: Decimal
    order_type: OrderTypePolicy
    reduce_only: bool
    expiry_ms: Optional[int]
    created_ms: int
    nonce: str
    owned: bool
    filled: Decimal = Decimal("0")
    status: str = OPEN_STATUS
    updated_ms: int = 0
    active_visible_at: float = 0.0
    terminal_at: Optional[float] = None
    history_visible_at: Optional[float] = None
    # Rows exposed via inactive-order history may be overridden to exercise conflicts.
    history_override: Optional[Dict[str, object]] = None

    @property
    def remaining(self) -> Decimal:
        return self.initial - self.filled

    @property
    def is_open(self) -> bool:
        return self.status == OPEN_STATUS


@dataclass
class FakeTradeLeg:
    trade_id: str
    own_side: Side
    order_index: Optional[str]
    client_order_id: Optional[int]
    size: Decimal
    price: Decimal
    is_maker: Optional[bool]
    timestamp_ms: int
    visible_at: float
    raw: Dict[str, object]
    account_index: int


@dataclass
class _PageFault:
    kind: str                 # repeat_cursor | malformed_cursor | page_error | missing_page
    page: int                 # 1-based page number within a scan (page 1 = cursor None)
    times: int = 1
    value: object = None


@dataclass
class CallRecord:
    at: float
    kind: str                 # submit | cancel
    client_order_id: int
    request: Optional[SubmitRequest]
    exchange_order_id: Optional[str]
    behavior: str
    result: Optional[TransportResult]


def _canonical(obj: Dict[str, object]) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def _encode_cursor(endpoint: str, page: int, ts_ms: int, key: str) -> str:
    raw = f"{endpoint}|{page}|{ts_ms}|{key}".encode()
    return base64.urlsafe_b64encode(raw).decode()


def _decode_cursor(endpoint: str, cursor: str) -> Tuple[int, int, str]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        name, page, ts_ms, key = raw.split("|", 3)
        if name != endpoint:
            raise ValueError("cursor belongs to another endpoint")
        return int(page), int(ts_ms), key
    except Exception as exc:  # noqa: BLE001 - any decode failure is a venue-side 400
        raise FakeTransportError(f"invalid cursor for {endpoint}: {cursor!r}") from exc


class FakeExchange:
    """Deterministic fake of one account on one perpetual market (ExchangePort)."""

    def __init__(
        self,
        clock: Optional[FakeClock] = None,
        *,
        domain: str = "fake_lighter",
        account_index: int = 4242,
        market_id: int = 5,
        tick_size: Decimal = Decimal("0.0001"),
        size_step: Decimal = Decimal("0.1"),
        min_base: Decimal = Decimal("5"),
        min_notional: Decimal = Decimal("10"),
        max_base: Optional[Decimal] = Decimal("100000"),
        max_leverage: Optional[Decimal] = Decimal("10"),
        max_active_orders_venue: Optional[int] = 1000,
        initial_position: Decimal = Decimal("0"),
        available_collateral: Optional[Decimal] = Decimal("3000"),
        leverage: Optional[Decimal] = Decimal("5"),
        margin_mode: Optional[str] = "cross",
        bid: Optional[Decimal] = Decimal("5.3999"),
        ask: Optional[Decimal] = Decimal("5.4001"),
        history_lag_s: float = 0.0,
        active_lag_s: float = 0.0,
        retention_s: Optional[float] = None,
        weights: Optional[Dict[str, int]] = None,
        weight_pool_per_min: int = DEFAULT_WEIGHT_POOL_PER_MIN,
        enforce_weight_pool: bool = False,
        seed: int = 7,
    ):
        self.clock = clock or FakeClock()
        self.domain = domain
        self.account_index = account_index
        self.market_id = market_id
        self._rules = dict(
            tick_size=tick_size, size_step=size_step, min_base=min_base, min_notional=min_notional,
            max_base=max_base, max_leverage=max_leverage, supports_limit=True, supports_post_only=True,
            max_active_orders_venue=max_active_orders_venue,
        )
        self._position = Decimal(initial_position)
        self.available_collateral: Optional[Decimal] = available_collateral
        self.leverage: Optional[Decimal] = leverage
        self.margin_mode: Optional[str] = margin_mode
        self.bid = bid
        self.ask = ask
        self.history_lag_s = float(history_lag_s)
        self.active_lag_s = float(active_lag_s)
        self.retention_s = retention_s
        self.weights = dict(DEFAULT_WEIGHTS)
        if weights:
            self.weights.update(weights)
        self.weight_pool_per_min = weight_pool_per_min
        self.enforce_weight_pool = enforce_weight_pool
        self._rng = random.Random(seed)

        self.orders: Dict[str, FakeOrder] = {}                 # order_index -> order
        self._by_cid: Dict[int, str] = {}                        # client id -> order_index (latest)
        self.trade_legs: List[FakeTradeLeg] = []
        self._next_order_index = FIRST_ORDER_INDEX
        self._next_trade_id = FIRST_TRADE_ID
        self._nonce = 0
        self._manual_cid = 900_000_000_000

        self._submit_script: Deque[Tuple[Optional[int], SubmitBehavior]] = deque()
        self._cancel_script: Deque[Tuple[Optional[int], CancelBehavior]] = deque()
        self._page_faults: Dict[str, List[_PageFault]] = {TRADES_ENDPOINT: [], INACTIVE_ENDPOINT: []}
        self.duplicate_boundary: Dict[str, bool] = {TRADES_ENDPOINT: False, INACTIVE_ENDPOINT: False}
        self.reorder_pages: Dict[str, bool] = {TRADES_ENDPOINT: False, INACTIVE_ENDPOINT: False}
        self._conflicting_trades: Dict[str, Decimal] = {}      # trade id -> conflicting size
        self.fail_endpoints: Dict[str, int] = {}                  # endpoint -> remaining failures
        self.rules_available = True
        self.position_available = True
        self.auto_fill_crossing_limit = False
        self.auto_expire = True
        self.ws_duplicate = False

        self.calls: List[CallRecord] = []
        self.weight_log: List[Tuple[float, str, int]] = []
        self.page_log: List[Tuple[float, str, Optional[str], int]] = []
        self.violations: List[str] = []
        self._ws_listeners: List[Callable[[Dict[str, object]], None]] = []
        self.ws_events: List[Dict[str, object]] = []

    # ------------------------------------------------------------------ configuration
    def set_rules(self, **changes) -> None:
        unknown = set(changes) - set(self._rules)
        if unknown:
            raise KeyError(f"unknown rule fields {sorted(unknown)}")
        self._rules.update(changes)

    def set_book(self, bid: Optional[Decimal], ask: Optional[Decimal]) -> None:
        self.bid = bid
        self.ask = ask
        if self.auto_fill_crossing_limit:
            self._match_resting_against_book()

    def set_mid(self, mid: Decimal, half_spread: Decimal = Decimal("0.0001")) -> None:
        self.set_book(mid - half_spread, mid + half_spread)

    def set_position_external(self, delta: Decimal) -> None:
        """Position change with *no* trade row (e.g. liquidation/transfer): pure drift."""
        self._position += delta

    def script_submit(self, behavior: SubmitBehavior, client_order_id: Optional[int] = None) -> None:
        """Queue a behavior for the next submit (optionally only for a given CID)."""
        self._submit_script.append((client_order_id, SubmitBehavior(behavior)))

    def script_cancel(self, behavior: CancelBehavior, client_order_id: Optional[int] = None) -> None:
        self._cancel_script.append((client_order_id, CancelBehavior(behavior)))

    def inject_page_fault(self, endpoint: str, kind: str, page: int, times: int = 1, value: object = None) -> None:
        if kind not in ("repeat_cursor", "malformed_cursor", "page_error", "missing_page"):
            raise ValueError(kind)
        self._page_faults[endpoint].append(_PageFault(kind=kind, page=page, times=times, value=value))

    def inject_conflicting_trade(self, trade_id: str, size: Decimal) -> None:
        """Future pages repeat ``trade_id`` with a different size (same dedupe key, other payload)."""
        self._conflicting_trades[trade_id] = Decimal(size)

    def fail_next(self, endpoint: str, times: int = 1) -> None:
        self.fail_endpoints[endpoint] = self.fail_endpoints.get(endpoint, 0) + times

    def add_ws_listener(self, callback: Callable[[Dict[str, object]], None]) -> None:
        self._ws_listeners.append(callback)

    def remove_ws_listener(self, callback: Callable[[Dict[str, object]], None]) -> None:
        if callback in self._ws_listeners:
            self._ws_listeners.remove(callback)

    # ------------------------------------------------------------------ helpers
    @property
    def now(self) -> float:
        return self.clock.now()

    def _now_ms(self) -> int:
        return int(round(self.clock.now() * 1000))

    def _charge(self, endpoint: str) -> None:
        weight = self.request_weight(endpoint)
        now = self.clock.now()
        if self.enforce_weight_pool and self.weight_used(60.0) + weight > self.weight_pool_per_min:
            raise FakeRateLimitExceeded(f"weight pool exhausted for {endpoint}")
        self.weight_log.append((now, endpoint, weight))
        remaining = self.fail_endpoints.get(endpoint, 0)
        if remaining > 0:
            self.fail_endpoints[endpoint] = remaining - 1
            raise FakeDataUnavailable(f"injected failure for {endpoint}")

    def weight_used(self, window_s: float = 60.0) -> int:
        cutoff = self.clock.now() - window_s
        return sum(w for t, _, w in self.weight_log if t > cutoff)

    def _emit_ws(self, event: Dict[str, object]) -> None:
        event = dict(event, at=self.clock.now())
        repeats = 2 if self.ws_duplicate else 1
        for _ in range(repeats):
            self.ws_events.append(event)
            for listener in list(self._ws_listeners):
                listener(event)

    def _new_order_index(self) -> str:
        value = self._next_order_index
        self._next_order_index += 1
        return str(value)

    def _new_trade_id(self) -> str:
        value = self._next_trade_id
        self._next_trade_id += 1
        return str(value)

    def _history_visible_at(self, lag_s: Optional[float]) -> float:
        return self.clock.now() + (self.history_lag_s if lag_s is None else float(lag_s))

    def _process_time(self) -> None:
        if not self.auto_expire:
            return
        now_ms = self._now_ms()
        for order in list(self.orders.values()):
            if order.is_open and order.expiry_ms is not None and now_ms >= order.expiry_ms:
                self._terminate(order, CANCELED_EXPIRED, None)

    def _terminate(self, order: FakeOrder, status: str, lag_s: Optional[float]) -> None:
        order.status = status
        order.updated_ms = self._now_ms()
        order.terminal_at = self.clock.now()
        order.history_visible_at = self._history_visible_at(lag_s)
        self._emit_ws({"type": "order", "status": status, "order_index": order.order_index,
                       "client_order_id": order.client_order_id})

    def order_by_cid(self, client_order_id: int) -> Optional[FakeOrder]:
        index = self._by_cid.get(int(client_order_id))
        return self.orders.get(index) if index is not None else None

    def _resolve(self, client_order_id: Optional[int], order_index: Optional[str]) -> FakeOrder:
        if order_index is not None and order_index in self.orders:
            return self.orders[order_index]
        if client_order_id is not None:
            order = self.order_by_cid(client_order_id)
            if order is not None:
                return order
        raise KeyError(f"no order cid={client_order_id} index={order_index}")

    # ------------------------------------------------------------------ ExchangePort: market data
    async def trading_rules(self) -> TradingRules:
        self._charge(RULES_ENDPOINT)
        if not self.rules_available:
            raise FakeDataUnavailable("trading rules unavailable")
        return TradingRules(fetched_at=self.clock.now(), **self._rules)

    def rules_now(self) -> TradingRules:
        return TradingRules(fetched_at=self.clock.now(), **self._rules)

    async def mid_price(self) -> Optional[Decimal]:
        if self.bid is None or self.ask is None:
            return None
        return (self.bid + self.ask) / 2

    async def best_bid_ask(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        # The live adapter reads the WS-fed local order book: no REST weight.
        return self.bid, self.ask

    async def position(self) -> PositionSnapshot:
        self._charge(POSITION_ENDPOINT)
        self._process_time()
        if not self.position_available:
            raise FakeDataUnavailable("account position unavailable")
        return PositionSnapshot(
            net_base=self._position,
            fetched_at=self.clock.now(),
            leverage=self.leverage,
            margin_mode=self.margin_mode,
            available_collateral=self.available_collateral,
        )

    @property
    def net_position(self) -> Decimal:
        return self._position

    # ------------------------------------------------------------------ ExchangePort: orders
    def _order_raw(self, order: FakeOrder) -> Dict[str, object]:
        raw: Dict[str, object] = {
            "order_index": order.order_index,
            "order_id": order.order_index,
            "client_order_index": order.client_order_id,
            "client_order_id_str": None if order.client_order_id is None else str(order.client_order_id),
            "nonce": order.nonce,
            "owner_account_index": self.account_index,
            "market_index": self.market_id,
            "is_ask": order.side == Side.SELL,
            "price": str(order.price),
            "initial_base_amount": str(order.initial),
            "filled_base_amount": str(order.filled),
            "remaining_base_amount": str(order.remaining if order.is_open else Decimal("0")),
            "status": order.status,
            "reduce_only": order.reduce_only,
            "timestamp": order.updated_ms or order.created_ms,
            "order_expiry": order.expiry_ms,
            "type": "limit",
            "time_in_force": "post-only" if order.order_type == OrderTypePolicy.LIMIT_MAKER else "good-till-time",
        }
        if order.history_override:
            raw.update(order.history_override)
        return raw

    def _order_row(self, order: FakeOrder) -> ExchangeOrderRow:
        raw = self._order_raw(order)
        cid = raw["client_order_index"]
        return ExchangeOrderRow(
            client_order_id=None if cid is None else int(cid),
            client_order_id_str=raw["client_order_id_str"],
            order_id=str(raw["order_id"]),
            order_index=str(raw["order_index"]),
            nonce=str(raw["nonce"]),
            account_index=self.account_index,
            market_id=self.market_id,
            side=Side.SELL if raw["is_ask"] else Side.BUY,
            price=Decimal(str(raw["price"])),
            initial_base_amount=Decimal(str(raw["initial_base_amount"])),
            filled_base_amount=Decimal(str(raw["filled_base_amount"])),
            remaining_base_amount=Decimal(str(raw["remaining_base_amount"])),
            status=str(raw["status"]),
            reduce_only=bool(raw["reduce_only"]),
            timestamp_ms=int(raw["timestamp"]),
            raw_json=_canonical(raw),
        )

    async def active_orders(self) -> List[ExchangeOrderRow]:
        self._charge(ACTIVE_ENDPOINT)
        self._process_time()
        now = self.clock.now()
        rows = [self._order_row(o) for o in self.orders.values()
                if o.is_open and o.active_visible_at <= now]
        return sorted(rows, key=lambda r: int(r.order_index))

    # ------------------------------------------------------------------ history pagination
    def _visible_inactive(self) -> List[Tuple[int, str, FakeOrder]]:
        now = self.clock.now()
        floor_ms = None if self.retention_s is None else int(round((now - self.retention_s) * 1000))
        rows = []
        for o in self.orders.values():
            if o.is_open or o.history_visible_at is None or o.history_visible_at > now:
                continue
            ts = o.updated_ms
            if floor_ms is not None and ts < floor_ms:
                continue
            rows.append((ts, f"{int(o.order_index):024d}", o))
        rows.sort(key=lambda r: (r[0], r[1]), reverse=True)
        return rows

    def _visible_trades(self) -> List[Tuple[int, str, FakeTradeLeg]]:
        now = self.clock.now()
        floor_ms = None if self.retention_s is None else int(round((now - self.retention_s) * 1000))
        rows = []
        for leg in self.trade_legs:
            if leg.visible_at > now:
                continue
            if floor_ms is not None and leg.timestamp_ms < floor_ms:
                continue
            # key orders trade id desc, then own side so both self-trade legs are adjacent
            rows.append((leg.timestamp_ms, f"{int(leg.trade_id):024d}|{leg.own_side.value}", leg))
        rows.sort(key=lambda r: (r[0], r[1]), reverse=True)
        return rows

    def _page(self, endpoint: str, cursor: Optional[str], limit: int, visible, to_row) -> HistoryPage:
        if limit > 100 or limit <= 0:
            raise FakeTransportError("limit must be within 1..100")
        self._charge(endpoint)
        self._process_time()
        if cursor is None:
            page_no, start = 1, 0
            items = visible()
        else:
            if not isinstance(cursor, str):
                raise FakeTransportError(f"malformed cursor {cursor!r}")
            page_no, ts_ms, key = _decode_cursor(endpoint, cursor)
            page_no += 1
            items = visible()
            start = 0
            while start < len(items) and (items[start][0], items[start][1]) >= (ts_ms, key):
                start += 1
        self.page_log.append((self.clock.now(), endpoint, cursor, page_no))
        fault = self._take_fault(endpoint, page_no)
        if fault is not None and fault.kind in ("page_error", "missing_page"):
            raise FakeDataUnavailable(f"injected {fault.kind} on {endpoint} page {page_no}")
        chunk = items[start:start + limit]
        rows = [to_row(item[2]) for item in chunk]
        if self.duplicate_boundary[endpoint] and start > 0:
            rows.insert(0, to_row(items[start - 1][2]))
        if endpoint == TRADES_ENDPOINT and self._conflicting_trades and page_no > 1:
            for trade_id, size in list(self._conflicting_trades.items()):
                for item in items[:start]:
                    leg = item[2]
                    if leg.trade_id == trade_id:
                        rows.append(self._trade_row(leg, size_override=size))
                        del self._conflicting_trades[trade_id]
                        break
        if self.reorder_pages[endpoint]:
            self._rng.shuffle(rows)
        more = start + limit < len(items)
        next_cursor: Optional[str] = None
        if more:
            last = chunk[-1]
            next_cursor = _encode_cursor(endpoint, page_no, last[0], last[1])
        if fault is not None and fault.kind == "repeat_cursor":
            next_cursor = cursor if cursor is not None else next_cursor
            if cursor is None and next_cursor is None:
                next_cursor = _encode_cursor(endpoint, 0, 1 << 62, "~")
        if fault is not None and fault.kind == "malformed_cursor":
            next_cursor = fault.value if fault.value is not None else "%%%not-base64%%%"
        return HistoryPage(rows=rows, next_cursor=next_cursor, raw_cursor_sent=cursor)

    def _take_fault(self, endpoint: str, page_no: int) -> Optional[_PageFault]:
        for fault in self._page_faults[endpoint]:
            if fault.page == page_no and fault.times > 0:
                fault.times -= 1
                return fault
        return None

    async def inactive_orders_page(self, cursor: Optional[str], limit: int = 100) -> HistoryPage:
        return self._page(INACTIVE_ENDPOINT, cursor, limit, self._visible_inactive, self._order_row)

    def _trade_row(self, leg: FakeTradeLeg, size_override: Optional[Decimal] = None) -> ExchangeTradeRow:
        raw = dict(leg.raw)
        if size_override is not None:
            raw["size"] = str(size_override)
        payload = {"own_side": leg.own_side.value, "trade": raw}
        return ExchangeTradeRow(
            trade_id_str=leg.trade_id,
            account_index=leg.account_index,
            market_id=self.market_id,
            own_side=leg.own_side,
            own_exchange_order_id=leg.order_index,
            own_client_order_id=leg.client_order_id,
            size=Decimal(str(raw["size"])),
            price=leg.price,
            is_maker=leg.is_maker,
            timestamp_ms=leg.timestamp_ms,
            raw_json=_canonical(payload),
        )

    async def trades_page(self, cursor: Optional[str], limit: int = 100) -> HistoryPage:
        return self._page(TRADES_ENDPOINT, cursor, limit, self._visible_trades, self._trade_row)

    # ------------------------------------------------------------------ transport
    def _next_behavior(self, script: Deque, client_order_id: int, default):
        for i, (cid, behavior) in enumerate(script):
            if cid is None or cid == client_order_id:
                del script[i]
                return behavior
        return default

    def _validate_submit(self, req: SubmitRequest) -> Optional[str]:
        rules = self._rules
        if req.order_type not in (OrderTypePolicy.LIMIT_MAKER, OrderTypePolicy.LIMIT):
            return f"unsupported order type {req.order_type}"
        if req.amount <= 0 or req.price <= 0:
            return "non-positive amount/price"
        if req.amount % rules["size_step"] != 0:
            return f"amount {req.amount} not a multiple of step {rules['size_step']}"
        if req.price % rules["tick_size"] != 0:
            return f"price {req.price} not a multiple of tick {rules['tick_size']}"
        if req.amount < rules["min_base"]:
            return f"amount {req.amount} below min base {rules['min_base']}"
        if req.amount * req.price < rules["min_notional"]:
            return f"notional below min {rules['min_notional']}"
        if rules["max_base"] is not None and req.amount > rules["max_base"]:
            return "amount above max base"
        cap = rules["max_active_orders_venue"]
        if cap is not None and sum(1 for o in self.orders.values() if o.is_open) >= cap:
            return "too many open orders"
        if not (0 < req.client_order_id < (1 << 48)):
            return "client order id outside 48-bit range"
        return None

    def _crosses(self, side: Side, price: Decimal) -> bool:
        if side == Side.BUY:
            return self.ask is not None and price >= self.ask
        return self.bid is not None and price <= self.bid

    def _create_order(self, req: SubmitRequest) -> FakeOrder:
        self._nonce += 1
        now_ms = self._now_ms()
        order = FakeOrder(
            order_index=self._new_order_index(), client_order_id=req.client_order_id, side=req.side,
            price=req.price, initial=req.amount, order_type=req.order_type, reduce_only=req.reduce_only,
            expiry_ms=req.expiry_ms, created_ms=now_ms, updated_ms=now_ms, nonce=str(self._nonce), owned=True,
            active_visible_at=self.clock.now() + self.active_lag_s,
        )
        if req.client_order_id in self._by_cid:
            self.violations.append(f"duplicate client order id {req.client_order_id} reached venue")
        self.orders[order.order_index] = order
        self._by_cid[req.client_order_id] = order.order_index
        if req.order_type == OrderTypePolicy.LIMIT_MAKER and self._crosses(req.side, req.price):
            self._terminate(order, CANCELED_POST_ONLY, None)
        else:
            self._emit_ws({"type": "order", "status": OPEN_STATUS, "order_index": order.order_index,
                           "client_order_id": order.client_order_id})
            if self.auto_fill_crossing_limit and req.order_type == OrderTypePolicy.LIMIT \
                    and self._crosses(req.side, req.price):
                self.fill(order_index=order.order_index, qty=order.remaining, maker=False)
        return order

    async def submit(self, req: SubmitRequest) -> TransportResult:
        behavior = self._next_behavior(self._submit_script, req.client_order_id, SubmitBehavior.ACCEPT)
        if req.reduce_only:
            self.violations.append(f"reduce_only submit cid={req.client_order_id}")
        record = CallRecord(at=self.clock.now(), kind="submit", client_order_id=req.client_order_id,
                            request=req, exchange_order_id=None, behavior=behavior.value, result=None)
        self.calls.append(record)
        if behavior == SubmitBehavior.RAISE:
            raise FakeTransportError("connection reset during submit")
        if behavior == SubmitBehavior.TIMEOUT_NOT_LANDED:
            record.result = TransportResult(TransportOutcome.UNKNOWN, "timeout")
            return record.result
        if behavior == SubmitBehavior.REJECT_ZERO_FILL:
            record.result = TransportResult(TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL, "scripted venue reject")
            return record.result
        problem = self._validate_submit(req)
        if problem is not None:
            self.violations.append(f"invalid submit cid={req.client_order_id}: {problem}")
            record.result = TransportResult(TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL, problem)
            return record.result
        order = self._create_order(req)
        record.exchange_order_id = order.order_index
        if behavior == SubmitBehavior.TIMEOUT_LANDED:
            record.result = TransportResult(TransportOutcome.UNKNOWN, "timeout")
            return record.result
        if behavior == SubmitBehavior.RAISE_LANDED:
            raise FakeTransportError("connection reset after submit")
        record.result = TransportResult(TransportOutcome.ACCEPTED, "tx accepted", None)
        return record.result

    async def cancel(self, client_order_id: int, exchange_order_id: Optional[str]) -> TransportResult:
        behavior = self._next_behavior(self._cancel_script, client_order_id, CancelBehavior.ACCEPT)
        record = CallRecord(at=self.clock.now(), kind="cancel", client_order_id=client_order_id, request=None,
                            exchange_order_id=exchange_order_id, behavior=behavior.value, result=None)
        self.calls.append(record)
        self._process_time()
        if behavior == CancelBehavior.RAISE:
            raise FakeTransportError("connection reset during cancel")
        if behavior == CancelBehavior.TIMEOUT_NOT_LANDED:
            record.result = TransportResult(TransportOutcome.UNKNOWN, "timeout")
            return record.result
        try:
            order = self._resolve(client_order_id, exchange_order_id)
        except KeyError:
            order = None
        if behavior == CancelBehavior.NOT_FOUND or order is None or not order.is_open:
            record.result = TransportResult(TransportOutcome.UNKNOWN, "order not found")
            return record.result
        self._terminate(order, CANCELED_STATUS, None)
        if behavior == CancelBehavior.TIMEOUT_LANDED:
            record.result = TransportResult(TransportOutcome.UNKNOWN, "timeout")
            return record.result
        record.result = TransportResult(TransportOutcome.ACCEPTED, "cancel accepted", order.order_index)
        return record.result

    def request_weight(self, endpoint: str) -> int:
        return self.weights.get(endpoint, DEFAULT_OTHER_WEIGHT)

    # ------------------------------------------------------------------ venue-side events
    def _leg(self, trade_id: str, order: Optional[FakeOrder], side: Side, qty: Decimal, price: Decimal,
             is_maker: Optional[bool], raw: Dict[str, object], visible_at: float,
             client_order_id: Optional[int] = None, account_index: Optional[int] = None) -> FakeTradeLeg:
        return FakeTradeLeg(
            trade_id=trade_id, own_side=side, order_index=None if order is None else order.order_index,
            client_order_id=(order.client_order_id if order is not None else client_order_id),
            size=qty, price=price, is_maker=is_maker, timestamp_ms=self._now_ms(), visible_at=visible_at, raw=raw,
            account_index=self.account_index if account_index is None else account_index,
        )

    def fill(self, client_order_id: Optional[int] = None, qty: Decimal = Decimal("0"), *,
             order_index: Optional[str] = None, price: Optional[Decimal] = None, maker: bool = True,
             history_lag_s: Optional[float] = None, ws: bool = True) -> str:
        """Fill an own resting order against an external counterparty. Returns trade id."""
        order = self._resolve(client_order_id, order_index)
        qty = Decimal(qty)
        if not order.is_open:
            raise ValueError(f"order {order.order_index} is not open ({order.status})")
        if qty <= 0 or qty > order.remaining:
            raise ValueError(f"fill {qty} exceeds remaining {order.remaining}")
        exec_price = order.price if price is None else Decimal(price)
        trade_id = self._new_trade_id()
        visible_at = self._history_visible_at(history_lag_s)
        is_ask = order.side == Side.SELL
        external = self.account_index + 1
        raw = {
            "trade_id": trade_id, "trade_id_str": trade_id, "market_id": self.market_id,
            "size": str(qty), "price": str(exec_price), "timestamp": self._now_ms(),
            "ask_account_id": self.account_index if is_ask else external,
            "bid_account_id": external if is_ask else self.account_index,
            "ask_id_str": order.order_index if is_ask else "1",
            "bid_id_str": "1" if is_ask else order.order_index,
            "ask_client_id_str": str(order.client_order_id) if is_ask else "0",
            "bid_client_id_str": "0" if is_ask else str(order.client_order_id),
            "is_maker_ask": maker if is_ask else (not maker),
        }
        self.trade_legs.append(self._leg(trade_id, order, order.side, qty, exec_price, maker, raw, visible_at))
        order.filled += qty
        order.updated_ms = self._now_ms()
        self._position += qty if order.side == Side.BUY else -qty
        if order.remaining == 0:
            order.status = FILLED_STATUS
            order.terminal_at = self.clock.now()
            order.history_visible_at = visible_at
        if ws:
            self._emit_ws({"type": "trade", "trade_id": trade_id, "order_index": order.order_index,
                           "client_order_id": order.client_order_id, "size": str(qty)})
        return trade_id

    def self_trade(self, maker_cid: int, taker_cid: int, qty: Decimal, *,
                   history_lag_s: Optional[float] = None) -> str:
        """Two own orders match each other: one trade id with two own legs (NG-HIST-001)."""
        maker = self._resolve(maker_cid, None)
        taker = self._resolve(taker_cid, None)
        if maker.side == taker.side:
            raise ValueError("self-trade requires opposite sides")
        qty = Decimal(qty)
        if qty > maker.remaining or qty > taker.remaining:
            raise ValueError("self-trade exceeds remaining")
        trade_id = self._new_trade_id()
        visible_at = self._history_visible_at(history_lag_s)
        ask, bid = (maker, taker) if maker.side == Side.SELL else (taker, maker)
        raw = {
            "trade_id": trade_id, "trade_id_str": trade_id, "market_id": self.market_id,
            "size": str(qty), "price": str(maker.price), "timestamp": self._now_ms(),
            "ask_account_id": self.account_index, "bid_account_id": self.account_index,
            "ask_id_str": ask.order_index, "bid_id_str": bid.order_index,
            "ask_client_id_str": str(ask.client_order_id), "bid_client_id_str": str(bid.client_order_id),
            "is_maker_ask": maker is ask,
        }
        for order in (maker, taker):
            self.trade_legs.append(self._leg(trade_id, order, order.side, qty, maker.price, order is maker, raw,
                                             visible_at))
            order.filled += qty
            order.updated_ms = self._now_ms()
            if order.remaining == 0:
                order.status = FILLED_STATUS
                order.terminal_at = self.clock.now()
                order.history_visible_at = visible_at
        self._emit_ws({"type": "trade", "trade_id": trade_id, "self_trade": True})
        return trade_id

    def venue_cancel(self, client_order_id: int, status: str = CANCELED_STATUS,
                     history_lag_s: Optional[float] = None) -> None:
        """Venue-initiated terminal (expiry, self-trade prevention, margin...)."""
        order = self._resolve(client_order_id, None)
        if not order.is_open:
            raise ValueError("order not open")
        self._terminate(order, status, history_lag_s)

    def expire_all_due(self) -> None:
        self._process_time()

    def place_manual_order(self, side: Side, price: Decimal, amount: Decimal,
                           client_order_id: Optional[int] = None) -> FakeOrder:
        """An order placed by the operator outside the engine (unknown CID)."""
        self._nonce += 1
        self._manual_cid += 1
        cid = self._manual_cid if client_order_id is None else client_order_id
        now_ms = self._now_ms()
        order = FakeOrder(
            order_index=self._new_order_index(), client_order_id=cid, side=side, price=Decimal(price),
            initial=Decimal(amount), order_type=OrderTypePolicy.LIMIT, reduce_only=False, expiry_ms=None,
            created_ms=now_ms, updated_ms=now_ms, nonce=str(self._nonce), owned=False,
        )
        self.orders[order.order_index] = order
        self._by_cid[cid] = order.order_index
        self._emit_ws({"type": "order", "status": OPEN_STATUS, "order_index": order.order_index,
                       "client_order_id": cid})
        return order

    def manual_trade(self, side: Side, qty: Decimal, price: Decimal,
                     history_lag_s: Optional[float] = None) -> str:
        """A trade on the account by an order the engine does not own."""
        order = self.place_manual_order(side, price, qty)
        return self.fill(order.client_order_id, qty, history_lag_s=history_lag_s)

    # ------------------------------------------------------------------ book simulation (demo)
    def _match_resting_against_book(self) -> None:
        for order in sorted(self.orders.values(), key=lambda o: int(o.order_index)):
            if not order.is_open or not order.owned:
                continue
            if order.side == Side.BUY and self.ask is not None and self.ask <= order.price:
                self.fill(order_index=order.order_index, qty=order.remaining)
            elif order.side == Side.SELL and self.bid is not None and self.bid >= order.price:
                self.fill(order_index=order.order_index, qty=order.remaining)

    # ------------------------------------------------------------------ inspection helpers
    def open_orders(self, owned: Optional[bool] = None) -> List[FakeOrder]:
        return [o for o in self.orders.values() if o.is_open and (owned is None or o.owned == owned)]

    def submits(self) -> List[CallRecord]:
        return [c for c in self.calls if c.kind == "submit"]

    def cancels(self) -> List[CallRecord]:
        return [c for c in self.calls if c.kind == "cancel"]


@dataclass
class FakeScenario:
    """Named knobs for the offline web demo (no network, no credentials)."""
    history_lag_s: float = 1.0
    auto_fill_crossing_limit: bool = True
    notes: List[str] = field(default_factory=list)
