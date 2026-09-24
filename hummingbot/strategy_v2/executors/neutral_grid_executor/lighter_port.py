"""Lighter adapter implementing the neutral-grid ``ExchangePort`` over ``LighterPerpetualDerivative``.

Exact field mapping (NG-HIST-001, lighter-sdk 1.1.4 ``models/order.py`` / ``models/trade.py``):

Order row -> :class:`ExchangeOrderRow`
    ``client_order_index`` (int) / ``client_order_id`` (str) / ``client_order_id_str`` (str, if
    present) must all agree -> ``client_order_id`` (int) + ``client_order_id_str`` (exact str);
    ``order_id`` (str) and ``order_index`` (int) must agree (the connector already cancels by
    ``int(order_id)``) -> both kept as exact strings; ``nonce`` -> str (secondary corroboration
    only, see ``LighterPerpetualDerivative.submit_with_client_id``); ``owner_account_index``,
    ``market_index``; ``is_ask`` -> side; ``price``/``initial_base_amount``/``filled_base_amount``/
    ``remaining_base_amount`` -> Decimal from the venue strings; ``status``; ``reduce_only``;
    ``timestamp`` (configurable field, unit-normalized to ms).

Trade -> one :class:`ExchangeTradeRow` per *own* leg
    ``trade_id_str``/``trade_id`` (must agree); ``ask_account_id``/``bid_account_id``; own leg
    ``{ask,bid}_client_id_str``/``{ask,bid}_client_id`` (must agree) and ``{ask,bid}_id_str``/
    ``{ask,bid}_id`` (= own order index, must agree); ``size``/``price`` Decimal; ``timestamp``
    (ms); maker flag from ``is_maker_ask``. A self-trade (both accounts ours) yields TWO rows, one
    per side - the second leg is never dropped.

Ids are never converted through float: JSON integers decode to exact Python ints, floats and
booleans are rejected as schema errors, Decimals are built only from str/int.
"""
from __future__ import annotations

import asyncio
import json
import math
import time
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

from hummingbot.connector.derivative.lighter_perpetual import lighter_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_api_utils import (
    LighterHistoryResponseError,
    LighterTransportOutcome,
    exact_int,
)
from hummingbot.core.data_type.common import OrderType, TradeType
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
from hummingbot.strategy_v2.executors.neutral_grid_executor.history import (
    ENDPOINT_ACCOUNT,
    ENDPOINT_ACTIVE_ORDERS,
    ENDPOINT_INACTIVE_ORDERS,
    ENDPOINT_SEND_TX,
    ENDPOINT_TRADES,
    ENDPOINT_TRADING_RULES,
    HISTORY_PAGE_LIMIT,
    HistorySchemaError,
)

ENDPOINT_WEIGHTS: Dict[str, int] = {
    ENDPOINT_TRADES: CONSTANTS.WEIGHT_TRADES,                    # 600
    ENDPOINT_INACTIVE_ORDERS: CONSTANTS.WEIGHT_INACTIVE_ORDERS,  # 100
    ENDPOINT_ACTIVE_ORDERS: CONSTANTS.WEIGHT_DEFAULT,            # 300
    ENDPOINT_ACCOUNT: CONSTANTS.WEIGHT_DEFAULT,                  # 300
    ENDPOINT_TRADING_RULES: CONSTANTS.WEIGHT_DEFAULT,            # 300
    ENDPOINT_SEND_TX: CONSTANTS.WEIGHT_SEND_TX,                  # 6
}

_OUTCOME_MAP = {
    LighterTransportOutcome.NOT_SENT: TransportOutcome.NOT_SENT,
    LighterTransportOutcome.ACCEPTED: TransportOutcome.ACCEPTED,
    LighterTransportOutcome.DEFINITIVE_REJECT_ZERO_FILL: TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL,
    LighterTransportOutcome.UNKNOWN: TransportOutcome.UNKNOWN,
}


# ---------------------------------------------------------------------------------------------
# Exact parsing helpers (fail closed with HistorySchemaError)
# ---------------------------------------------------------------------------------------------
def canonical_raw_json(raw: Dict[str, Any]) -> str:
    return json.dumps(raw, sort_keys=True, separators=(",", ":"), default=str)


def _exact_int_field(raw: Dict[str, Any], field: str, label: str) -> int:
    value = raw.get(field)
    try:
        return exact_int(value, field)
    except (TypeError, ValueError) as exc:
        raise HistorySchemaError(f"{label}: {field} must be an exact integer") from exc


def _id_text(value: Any, field: str, label: str) -> Optional[str]:
    """Exact id text from a str or int source; None/"" mean absent. Floats/bools are rejected."""
    if value is None or value == "":
        return None
    if type(value) is int:
        if value < 0:
            raise HistorySchemaError(f"{label}: {field} must be non-negative")
        return str(value)
    if isinstance(value, str) and value.isascii() and value.isdigit() and (value == "0" or value[0] != "0"):
        return value
    raise HistorySchemaError(f"{label}: {field} must be an exact decimal id (got {type(value).__name__})")


def _agreeing_id(raw: Dict[str, Any], fields: Tuple[str, ...], label: str, required: bool) -> Optional[str]:
    values = [(field, _id_text(raw.get(field), field, label)) for field in fields]
    present = {value for _, value in values if value is not None}
    if len(present) > 1:
        raise HistorySchemaError(f"{label}: {'/'.join(fields)} disagree")
    if not present:
        if required:
            raise HistorySchemaError(f"{label}: missing {'/'.join(fields)}")
        return None
    return present.pop()


def _exact_decimal(raw: Dict[str, Any], field: str, label: str, positive: bool = False) -> Decimal:
    value = raw.get(field)
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise HistorySchemaError(f"{label}: {field} must be a decimal string (got {type(value).__name__})")
    try:
        parsed = Decimal(value)
    except Exception as exc:
        raise HistorySchemaError(f"{label}: {field} is not a decimal") from exc
    if not parsed.is_finite() or parsed < 0 or (positive and parsed == 0):
        raise HistorySchemaError(f"{label}: {field} out of range")
    return parsed


def _exact_bool(raw: Dict[str, Any], field: str, label: str) -> bool:
    value = raw.get(field)
    if type(value) is not bool:
        raise HistorySchemaError(f"{label}: {field} must be a boolean")
    return value


def timestamp_to_ms(value: Any, label: str = "row") -> int:
    """Integer-only unit normalization (s / ms / us / ns by magnitude) to milliseconds."""
    if type(value) is int:
        number = value
    elif isinstance(value, str) and value.isascii() and value.isdigit():
        number = int(value)
    else:
        raise HistorySchemaError(f"{label}: timestamp must be an exact integer")
    if number <= 0:
        raise HistorySchemaError(f"{label}: timestamp must be positive")
    if number < 10 ** 11:
        return number * 1000
    if number < 10 ** 14:
        return number
    if number < 10 ** 17:
        return number // 1000
    return number // 1_000_000


def order_row_from_raw(raw: Any, *, timestamp_field: str = "timestamp") -> ExchangeOrderRow:
    if not isinstance(raw, dict):
        raise HistorySchemaError("order row is not an object")
    label = "order"
    client_id_str = _agreeing_id(
        raw, ("client_order_id_str", "client_order_id", "client_order_index"), label, required=True
    )
    exchange_id = _agreeing_id(raw, ("order_id", "order_index"), label, required=True)
    nonce = _id_text(raw.get("nonce"), "nonce", label)
    status = raw.get("status")
    if not isinstance(status, str) or status == "":
        raise HistorySchemaError(f"{label}: status must be a non-empty string")
    return ExchangeOrderRow(
        client_order_id=int(client_id_str),
        client_order_id_str=client_id_str,
        order_id=exchange_id,
        order_index=exchange_id,
        nonce=nonce,
        account_index=_exact_int_field(raw, "owner_account_index", label),
        market_id=_exact_int_field(raw, "market_index", label),
        side=Side.SELL if _exact_bool(raw, "is_ask", label) else Side.BUY,
        price=_exact_decimal(raw, "price", label),
        initial_base_amount=_exact_decimal(raw, "initial_base_amount", label),
        filled_base_amount=_exact_decimal(raw, "filled_base_amount", label),
        remaining_base_amount=_exact_decimal(raw, "remaining_base_amount", label),
        status=status,
        reduce_only=_exact_bool(raw, "reduce_only", label),
        timestamp_ms=timestamp_to_ms(raw.get(timestamp_field), label),
        raw_json=canonical_raw_json(raw),
    )


def trade_rows_from_raw(raw: Any, *, account_index: int) -> List[ExchangeTradeRow]:
    """All own legs of one raw trade (two for a self-trade). Raises if the trade is not ours."""
    if not isinstance(raw, dict):
        raise HistorySchemaError("trade row is not an object")
    label = "trade"
    trade_id_str = _agreeing_id(raw, ("trade_id_str", "trade_id"), label, required=True)
    market_id = _exact_int_field(raw, "market_id", label)
    size = _exact_decimal(raw, "size", label, positive=True)
    price = _exact_decimal(raw, "price", label, positive=True)
    timestamp_ms = timestamp_to_ms(raw.get("timestamp"), label)
    ask_account = _exact_int_field(raw, "ask_account_id", label)
    bid_account = _exact_int_field(raw, "bid_account_id", label)
    is_maker_ask = raw.get("is_maker_ask")
    if type(is_maker_ask) is not bool:
        is_maker_ask = None
    raw_json = canonical_raw_json(raw)
    rows = []
    for prefix, own_account, side in (("ask", ask_account, Side.SELL), ("bid", bid_account, Side.BUY)):
        if own_account != account_index:
            continue
        exchange_order_id = _agreeing_id(raw, (f"{prefix}_id_str", f"{prefix}_id"), label, required=False)
        client_id_str = _agreeing_id(
            raw, (f"{prefix}_client_id_str", f"{prefix}_client_id"), label, required=False
        )
        is_maker = None if is_maker_ask is None else (is_maker_ask if side == Side.SELL else not is_maker_ask)
        rows.append(ExchangeTradeRow(
            trade_id_str=trade_id_str,
            account_index=own_account,
            market_id=market_id,
            own_side=side,
            own_exchange_order_id=exchange_order_id,
            own_client_order_id=None if client_id_str is None else int(client_id_str),
            size=size,
            price=price,
            is_maker=is_maker,
            timestamp_ms=timestamp_ms,
            raw_json=raw_json,
        ))
    if not rows:
        raise HistorySchemaError(f"{label}: trade {trade_id_str} does not involve the scanned account")
    return rows


def _decimal_from_book_price(value: Any) -> Optional[Decimal]:
    # Hummingbot's in-memory order book stores prices as float; this is a reference price for
    # anchor/bounds checks only (never an id, quantity or ledger value).
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) or value <= 0:
        return None
    return Decimal(repr(value))


# ---------------------------------------------------------------------------------------------
# Port
# ---------------------------------------------------------------------------------------------
class LighterExchangePort:
    """``contracts.ExchangePort`` over a (trading-enabled) ``LighterPerpetualDerivative``.

    History methods are single-page; pagination/completeness belongs to ``HistoryScanner``.
    """

    def __init__(self, connector: Any, trading_pair: str, *, order_timestamp_field: str = "timestamp",
                 clock: Callable[[], float] = time.time):
        self._connector = connector
        self._trading_pair = trading_pair
        self._order_timestamp_field = order_timestamp_field
        self._clock = clock

    # identity --------------------------------------------------------------------------------
    @property
    def domain(self) -> str:
        return self._connector.domain

    @property
    def account_index(self) -> int:
        account_index = self._connector.account_index
        if type(account_index) is not int:
            raise HistorySchemaError("connector account index is not resolved")
        return account_index

    @property
    def market_id(self) -> int:
        return self._connector.market_info_for_trading_pair(self._trading_pair).market_id

    @property
    def trading_pair(self) -> str:
        return self._trading_pair

    def request_weight(self, endpoint: str) -> int:
        return ENDPOINT_WEIGHTS.get(endpoint, CONSTANTS.WEIGHT_DEFAULT)

    # reads -----------------------------------------------------------------------------------
    async def trading_rules(self) -> TradingRules:
        await self._connector._update_trading_rules()
        market = self._connector.market_info_for_trading_pair(self._trading_pair)
        max_leverage = market.max_leverage
        return TradingRules(
            tick_size=market.min_price_increment,
            size_step=market.min_base_increment,
            min_base=market.min_base_amount,
            min_notional=market.min_quote_amount,
            max_base=None,
            max_leverage=max_leverage if max_leverage > 0 else None,
            supports_limit=True,
            supports_post_only=True,
            fetched_at=self._clock(),
        )

    async def best_bid_ask(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        try:
            order_book = self._connector.get_order_book(self._trading_pair)
            bid = _decimal_from_book_price(order_book.get_price(False))
            ask = _decimal_from_book_price(order_book.get_price(True))
        except Exception:
            return None, None
        return bid, ask

    async def mid_price(self) -> Optional[Decimal]:
        bid, ask = await self.best_bid_ask()
        if bid is None or ask is None or bid >= ask:
            return None
        return (bid + ask) / 2

    async def position(self) -> PositionSnapshot:
        snapshot = await self._connector.fetch_account_position(self._trading_pair)
        if snapshot.get("account_index") != self.account_index or snapshot.get("market_id") != self.market_id:
            raise HistorySchemaError("position snapshot is not scoped to the port account/market")
        return PositionSnapshot(
            net_base=snapshot["net_position"],
            fetched_at=snapshot["fetched_at"],
            leverage=snapshot.get("leverage"),
            margin_mode=snapshot.get("margin_mode"),
            available_collateral=snapshot.get("available_collateral"),
        )

    async def active_orders(self) -> List[ExchangeOrderRow]:
        try:
            page = await self._connector.fetch_active_orders(self._trading_pair)
        except LighterHistoryResponseError as exc:
            raise HistorySchemaError(str(exc)) from exc
        if page.next_cursor not in (None, ""):
            raise HistorySchemaError("active orders response is truncated (next_cursor present)")
        rows = [order_row_from_raw(raw, timestamp_field=self._order_timestamp_field) for raw in page.rows]
        self._check_scope(rows)
        return rows

    async def inactive_orders_page(self, cursor: Optional[str], limit: int = HISTORY_PAGE_LIMIT) -> HistoryPage:
        try:
            page = await self._connector.fetch_inactive_orders_page(self._trading_pair, cursor=cursor, limit=limit)
        except LighterHistoryResponseError as exc:
            raise HistorySchemaError(str(exc)) from exc
        rows = [order_row_from_raw(raw, timestamp_field=self._order_timestamp_field) for raw in page.rows]
        self._check_scope(rows)
        return HistoryPage(rows=rows, next_cursor=page.next_cursor, raw_cursor_sent=page.cursor_sent)

    async def trades_page(self, cursor: Optional[str], limit: int = HISTORY_PAGE_LIMIT) -> HistoryPage:
        try:
            page = await self._connector.fetch_trades_page(self._trading_pair, cursor=cursor, limit=limit)
        except LighterHistoryResponseError as exc:
            raise HistorySchemaError(str(exc)) from exc
        account_index = self.account_index
        rows: List[ExchangeTradeRow] = []
        for raw in page.rows:
            rows.extend(trade_rows_from_raw(raw, account_index=account_index))
        self._check_scope(rows)
        return HistoryPage(rows=rows, next_cursor=page.next_cursor, raw_cursor_sent=page.cursor_sent)

    def _check_scope(self, rows: List[Any]) -> None:
        account_index, market_id = self.account_index, self.market_id
        for row in rows:
            if row.account_index != account_index or row.market_id != market_id:
                raise HistorySchemaError("row outside the port account/market scope")

    # side effects ----------------------------------------------------------------------------
    async def submit(self, req: SubmitRequest) -> TransportResult:
        if not isinstance(req, SubmitRequest):
            return TransportResult(TransportOutcome.NOT_SENT, "pre_send_validation: not a SubmitRequest")
        if req.reduce_only is not False:
            return TransportResult(TransportOutcome.NOT_SENT, "pre_send_validation: neutral grid never sends reduce_only")
        order_type = {OrderTypePolicy.LIMIT_MAKER: OrderType.LIMIT_MAKER,
                      OrderTypePolicy.LIMIT: OrderType.LIMIT}.get(req.order_type)
        trade_type = {Side.BUY: TradeType.BUY, Side.SELL: TradeType.SELL}.get(req.side)
        if order_type is None or trade_type is None:
            return TransportResult(TransportOutcome.NOT_SENT, "pre_send_validation: unsupported order type or side")
        try:
            result = await self._connector.submit_with_client_id(
                client_order_id=req.client_order_id,
                trading_pair=self._trading_pair,
                trade_type=trade_type,
                price=req.price,
                amount=req.amount,
                order_type=order_type,
                order_expiry_ms=req.expiry_ms,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # cannot prove the transport was not called
            return TransportResult(TransportOutcome.UNKNOWN, f"adapter_exception: {type(exc).__name__}")
        return self._map(result)

    async def cancel(self, client_order_id: int, exchange_order_id: Optional[str]) -> TransportResult:
        try:
            result = await self._connector.cancel_with_client_id(
                trading_pair=self._trading_pair,
                client_order_id=client_order_id,
                exchange_order_index=exchange_order_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return TransportResult(TransportOutcome.UNKNOWN, f"adapter_exception: {type(exc).__name__}")
        return self._map(result)

    @staticmethod
    def _map(result: Any) -> TransportResult:
        outcome = _OUTCOME_MAP.get(getattr(result, "outcome", None), TransportOutcome.UNKNOWN)
        return TransportResult(
            outcome=outcome,
            detail=str(getattr(result, "detail", "")),
            exchange_order_id=getattr(result, "exchange_order_id", None),
        )
