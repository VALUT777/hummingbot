"""Authoritative, cursor-paginated exchange history for the neutral grid (spec NG-HIST-001..003).

The :class:`HistoryScanner` walks the two authoritative history endpoints of an
:class:`~.contracts.ExchangePort` (``trades`` and ``accountInactiveOrders``) from newest to oldest
with ``limit=100`` and an opaque cursor that is passed back verbatim. It never decides anything
from a WebSocket event, from disappearance from the active list or from a single page.

Completeness rules (NG-HIST-002, AC-10/11/12/41/53):

* a stream is covered only when it reached its natural end (no ``next_cursor``) or when it read
  past the verified overlap boundary ``min(high_water - overlap, oldest_unresolved - overlap)``;
  finding an unresolved ID or the first duplicate is *not* a stop condition;
* rows must arrive newest -> oldest (non-increasing timestamps; for trades also non-increasing
  numeric trade ids); anything else is an ``ordering_violation``;
* a repeated cursor, a malformed cursor, a schema error, a failed page fetch, an empty page that
  still carries a cursor, an oversized page, or running out of budget before the boundary makes
  the result ``complete=False``;
* the canonical trade dedupe key is ``ExchangeTradeRow.dedupe_key(domain)``
  ``(domain, account, market, trade_id_str, own_side, own_exchange_order_id)``; the same key with
  an identical payload is idempotent, the same key with a different payload is a conflict
  (``complete=False`` + ``conflicts``); a terminal order whose owned executions exceed its
  terminal cumulative fill is a conflict (AC-40);
* the durable high-water mark is only *returned* (``trades_high_water``/``orders_high_water``,
  only when ``complete``); the engine commits it in the same SQLite transaction as the inbox rows,
  dedupe keys and ledger transitions. The scanner itself never writes durable state, so an
  in-flight pagination cursor is never persisted separately from the rows it produced.

Work is bounded per :meth:`HistoryScanner.scan` call (weight budget + optional page cap) and is
resumable: an unfinished walk keeps its in-memory position and reports ``in_progress`` so the
engine can stay ``RECONCILING`` between ticks (NG-HIST-003). Concurrent ``scan`` calls are
coalesced onto the one in flight; failures back off exponentially.

:func:`evaluate_terminal_release` is the pure settlement predicate the engine calls before it
releases an order/cell (AC-42 scan part): exact terminal order row + complete history + sum of
owned executions == terminal cumulative fill + settlement delay + repeat complete scans.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Callable, Deque, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    ExchangeOrderRow,
    ExchangePort,
    ExchangeTradeRow,
    HistoryPage,
    HistoryScanResult,
)

HISTORY_PAGE_LIMIT = 100

STREAM_TRADES = "trades"
STREAM_ORDERS = "inactive_orders"

# Endpoint names understood by ExchangePort.request_weight().
ENDPOINT_TRADES = "trades"
ENDPOINT_INACTIVE_ORDERS = "inactive_orders"
ENDPOINT_ACTIVE_ORDERS = "active_orders"
ENDPOINT_ACCOUNT = "account"
ENDPOINT_TRADING_RULES = "order_book_details"
ENDPOINT_SEND_TX = "send_tx"

# Stable incomplete reasons (also used as prefixes of conflict strings).
REASON_IN_PROGRESS = "in_progress"
REASON_BACKOFF = "backoff"
REASON_REPEATED_CURSOR = "repeated_cursor"
REASON_MALFORMED_CURSOR = "malformed_cursor"
REASON_SCHEMA_ERROR = "schema_error"
REASON_PAGE_FETCH_ERROR = "page_fetch_error"
REASON_EMPTY_PAGE_WITH_CURSOR = "empty_page_with_cursor"
REASON_OVERSIZED_PAGE = "oversized_page"
REASON_ORDERING_VIOLATION = "ordering_violation"
REASON_CONFLICT = "conflict"
REASON_RETENTION_GAP = "retention_gap"

OPEN_ORDER_STATUSES = frozenset({"open", "pending", "in-progress"})
FILLED_ORDER_STATUS = "filled"
CANCELED_STATUS_PREFIX = "canceled"

_MAX_CURSOR_LEN = 4096

OrderKey = Tuple[str, int, int, str, str]
TradeKey = Tuple[str, int, int, str, str, str]


class HistorySchemaError(ValueError):
    """A venue history payload does not match the expected schema. Always fail closed."""


def is_terminal_order_status(status: str) -> bool:
    """Venue status of an order that can no longer execute (``filled`` or any ``canceled*``)."""
    return status == FILLED_ORDER_STATUS or status.startswith(CANCELED_STATUS_PREFIX)


def is_open_order_status(status: str) -> bool:
    return status in OPEN_ORDER_STATUSES


def canonical_decimal(value: Decimal) -> str:
    """Exact, representation-independent Decimal text ("1.0" and "1.00" both become "1")."""
    if not isinstance(value, Decimal) or not value.is_finite():
        raise HistorySchemaError("expected a finite Decimal")
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _canonical_json(payload: Dict[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def trade_payload_fingerprint(row: ExchangeTradeRow) -> str:
    """Execution-identity payload of one own trade leg; differing fingerprints under one key = conflict."""
    return _canonical_json({
        "size": canonical_decimal(row.size),
        "price": canonical_decimal(row.price),
        "own_side": row.own_side.value,
        "own_exchange_order_id": row.own_exchange_order_id,
        "own_client_order_id": None if row.own_client_order_id is None else str(row.own_client_order_id),
        "is_maker": row.is_maker,
        "timestamp_ms": row.timestamp_ms,
    })


def order_dedupe_key(domain: str, row: ExchangeOrderRow) -> OrderKey:
    exchange_id = row.order_index if row.order_index is not None else (row.order_id or "")
    client_id = row.client_order_id_str
    if client_id is None:
        client_id = "" if row.client_order_id is None else str(row.client_order_id)
    return (domain, row.account_index, row.market_id, exchange_id, client_id)


def order_payload_fingerprint(row: ExchangeOrderRow) -> str:
    """Identity payload of an (immutable, terminal) inactive order row."""
    return _canonical_json({
        "status": row.status,
        "side": row.side.value,
        "price": canonical_decimal(row.price),
        "initial_base_amount": canonical_decimal(row.initial_base_amount),
        "filled_base_amount": canonical_decimal(row.filled_base_amount),
        "remaining_base_amount": canonical_decimal(row.remaining_base_amount),
        "reduce_only": row.reduce_only,
        "order_id": row.order_id,
        "order_index": row.order_index,
        "client_order_id": None if row.client_order_id is None else str(row.client_order_id),
        "client_order_id_str": row.client_order_id_str,
        "nonce": row.nonce,
        "timestamp_ms": row.timestamp_ms,
    })


def order_exchange_ids(row: ExchangeOrderRow) -> Tuple[str, ...]:
    """Exact exchange ids an execution may reference for this order (trade ask_id/bid_id = order_index)."""
    return tuple(sorted({i for i in (row.order_index, row.order_id) if i}))


def _key_to_json(key: Tuple) -> List[object]:
    return list(key)


@dataclass(frozen=True)
class HighWaterMark:
    """Newest row of the last complete scan of one stream: exact timestamp and exact dedupe key."""
    timestamp_ms: int
    key: Tuple

    def encode(self) -> str:
        return _canonical_json({"v": 1, "ts_ms": self.timestamp_ms, "key": _key_to_json(self.key)})

    @classmethod
    def decode(cls, value: Optional[str]) -> Optional["HighWaterMark"]:
        if value is None:
            return None
        if not isinstance(value, str):
            raise HistorySchemaError("high-water mark must be a string")
        try:
            payload = json.loads(value)
        except ValueError as exc:
            raise HistorySchemaError("high-water mark is not valid JSON") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("v") != 1
            or type(payload.get("ts_ms")) is not int
            or not isinstance(payload.get("key"), list)
        ):
            raise HistorySchemaError("high-water mark has an unsupported shape")
        return cls(timestamp_ms=payload["ts_ms"], key=tuple(payload["key"]))


class HistoryCursorView(Protocol):
    """Read-only view of committed history state the scanner needs (implemented by the store/engine)."""

    def high_water(self, stream: str) -> Optional[str]:
        """Committed high-water mark (``HighWaterMark.encode()``) of the last complete scan, or None."""
        ...

    def committed_payload(self, stream: str, key: Tuple) -> Optional[str]:
        """Fingerprint committed for ``key`` (trade or order dedupe key), or None if never committed."""
        ...

    def oldest_unresolved_ms(self) -> Optional[int]:
        """Earliest intent/creation time (ms) of any own order whose terminal + cumulative is unproven."""
        ...

    def bootstrap_floor_ms(self, stream: str) -> Optional[int]:
        """Without a high-water mark: optional lower boundary (ms); None means walk to the natural end."""
        ...


class InMemoryHistoryCursorView:
    """Reference HistoryCursorView (tests, offline demo). ``commit`` mimics the engine's atomic commit."""

    def __init__(self, domain: str, oldest_unresolved_ms: Optional[int] = None,
                 bootstrap_floor_ms: Optional[int] = None):
        self.domain = domain
        self._high_water: Dict[str, str] = {}
        self._payloads: Dict[str, Dict[Tuple, str]] = {STREAM_TRADES: {}, STREAM_ORDERS: {}}
        self._rows: Dict[str, Dict[Tuple, object]] = {STREAM_TRADES: {}, STREAM_ORDERS: {}}
        self.unresolved_ms = oldest_unresolved_ms
        self.floor_ms = bootstrap_floor_ms

    def high_water(self, stream: str) -> Optional[str]:
        return self._high_water.get(stream)

    def committed_payload(self, stream: str, key: Tuple) -> Optional[str]:
        return self._payloads[stream].get(tuple(key))

    def oldest_unresolved_ms(self) -> Optional[int]:
        return self.unresolved_ms

    def bootstrap_floor_ms(self, stream: str) -> Optional[int]:
        return self.floor_ms

    @property
    def trades(self) -> List[ExchangeTradeRow]:
        return list(self._rows[STREAM_TRADES].values())

    @property
    def orders(self) -> List[ExchangeOrderRow]:
        return list(self._rows[STREAM_ORDERS].values())

    def commit(self, result: HistoryScanResult) -> None:
        """Apply new rows (idempotent by key) and, only for complete results, the high-water marks."""
        for row in result.new_trades:
            key = row.dedupe_key(self.domain)
            fingerprint = trade_payload_fingerprint(row)
            existing = self._payloads[STREAM_TRADES].get(key)
            if existing is not None and existing != fingerprint:
                raise HistorySchemaError(f"refusing to overwrite conflicting trade {key}")
            self._payloads[STREAM_TRADES][key] = fingerprint
            self._rows[STREAM_TRADES][key] = row
        for row in result.new_orders:
            key = order_dedupe_key(self.domain, row)
            fingerprint = order_payload_fingerprint(row)
            existing = self._payloads[STREAM_ORDERS].get(key)
            if existing is not None and existing != fingerprint:
                raise HistorySchemaError(f"refusing to overwrite conflicting order {key}")
            self._payloads[STREAM_ORDERS][key] = fingerprint
            self._rows[STREAM_ORDERS][key] = row
        if result.complete:
            if result.trades_high_water is not None:
                self._high_water[STREAM_TRADES] = result.trades_high_water
            if result.orders_high_water is not None:
                self._high_water[STREAM_ORDERS] = result.orders_high_water


@dataclass(frozen=True)
class ScanRecord:
    """Wall-clock bounds of one complete scan (input of the settlement predicate)."""
    started_at: float
    completed_at: float


@dataclass
class _StreamProgress:
    stream: str
    endpoint: str
    boundary_ms: Optional[int]
    previous_high_water: Optional[HighWaterMark]
    next_cursor: Optional[str] = None
    cursors_sent: List[Optional[str]] = field(default_factory=list)
    done: bool = False
    reached: Optional[str] = None           # "boundary" | "natural_end"
    newest: Optional[HighWaterMark] = None
    oldest_ts_ms: Optional[int] = None
    last_ts_ms: Optional[int] = None
    last_numeric_id: Optional[int] = None
    rows: Dict[Tuple, object] = field(default_factory=dict)       # insertion order = newest -> oldest
    fingerprints: Dict[Tuple, str] = field(default_factory=dict)
    conflicts: List[str] = field(default_factory=list)
    pages_read: int = 0
    duplicate_rows: int = 0
    saw_previous_high_water_row: bool = False


class HistoryScanner:
    """Resumable newest->oldest scanner over both authoritative history endpoints.

    ``overlap_s`` is the configured overlap (spec default >= 60 s) added below both the durable
    high-water mark and the oldest unresolved own order. ``weight_budget`` is the maximum request
    weight one :meth:`scan` call may spend (trades page 600, inactive orders page 100 on a Lighter
    Standard account); it must allow at least one page of each endpoint.
    """

    def __init__(
        self,
        port: ExchangePort,
        store_cursor_view: HistoryCursorView,
        overlap_s: Decimal = Decimal("60"),
        weight_budget: int = 1400,
        *,
        clock: Callable[[], float] = time.time,
        max_pages_per_call: Optional[int] = None,
        poll_interval_s: Decimal = Decimal("5"),
        min_wake_interval_s: Decimal = Decimal("1"),
        backoff_initial_s: Decimal = Decimal("5"),
        backoff_max_s: Decimal = Decimal("300"),
        retention_horizon_s: Optional[Decimal] = None,
        page_limit: int = HISTORY_PAGE_LIMIT,
        scan_log_size: int = 16,
    ):
        overlap = Decimal(str(overlap_s))
        if not overlap.is_finite() or overlap < 0:
            raise ValueError("overlap_s must be a finite non-negative number of seconds")
        if type(page_limit) is not int or not 1 <= page_limit <= HISTORY_PAGE_LIMIT:
            raise ValueError(f"page_limit must be an int in [1, {HISTORY_PAGE_LIMIT}]")
        if type(weight_budget) is not int or weight_budget <= 0:
            raise ValueError("weight_budget must be a positive int")
        heaviest = max(port.request_weight(ENDPOINT_TRADES), port.request_weight(ENDPOINT_INACTIVE_ORDERS))
        if weight_budget < heaviest:
            raise ValueError(f"weight_budget {weight_budget} cannot pay for one page (needs >= {heaviest})")
        if max_pages_per_call is not None and (type(max_pages_per_call) is not int or max_pages_per_call < 1):
            raise ValueError("max_pages_per_call must be a positive int or None")
        self._port = port
        self._view = store_cursor_view
        self._overlap_ms = int(overlap * 1000)
        self._weight_budget = weight_budget
        self._clock = clock
        self._max_pages_per_call = max_pages_per_call
        self._poll_interval_s = Decimal(str(poll_interval_s))
        self._min_wake_interval_s = Decimal(str(min_wake_interval_s))
        self._backoff_initial_s = Decimal(str(backoff_initial_s))
        self._backoff_max_s = Decimal(str(backoff_max_s))
        self._retention_horizon_ms = (
            None if retention_horizon_s is None else int(Decimal(str(retention_horizon_s)) * 1000)
        )
        self._page_limit = page_limit
        self._progress: Optional[Dict[str, _StreamProgress]] = None
        self._scan_started_at: Optional[float] = None
        self._inflight: Optional[asyncio.Future] = None
        self._wake_requested = False
        self._last_scan_started_at: Optional[float] = None
        self._backoff_until: Optional[float] = None
        self._consecutive_failures = 0
        self._retry_after_failure = False
        self.completed_scans: Deque[ScanRecord] = deque(maxlen=scan_log_size)
        self.last_result: Optional[HistoryScanResult] = None
        self.coalesced_calls = 0

    # ------------------------------------------------------------------ cadence / coalescing
    @property
    def domain(self) -> str:
        return self._port.domain

    @property
    def scanning(self) -> bool:
        return self._inflight is not None and not self._inflight.done()

    @property
    def resumable_work_pending(self) -> bool:
        return self._progress is not None

    @property
    def backoff_until(self) -> Optional[float]:
        return self._backoff_until

    def wake(self) -> None:
        """WS/private-stream signal: request a scan soon. Multiple wakeups coalesce into one scan."""
        self._wake_requested = True

    def should_scan(self, now: Optional[float] = None) -> bool:
        """Cadence gate: never parallel, respects backoff, resumes pending work, coalesces wakeups."""
        now = self._clock() if now is None else now
        if self.scanning:
            return False
        if self._backoff_until is not None and now < self._backoff_until:
            return False
        if self._progress is not None or self._last_scan_started_at is None or self._retry_after_failure:
            return True
        elapsed = Decimal(str(now)) - Decimal(str(self._last_scan_started_at))
        if self._wake_requested and elapsed >= self._min_wake_interval_s:
            return True
        return elapsed >= self._poll_interval_s

    def progress_summary(self) -> Dict[str, object]:
        """Cursor progress for status/UI (no secrets; cursors are opaque venue tokens, not credentials)."""
        streams = {}
        for name, progress in (self._progress or {}).items():
            streams[name] = {
                "pages_read": progress.pages_read,
                "boundary_ms": progress.boundary_ms,
                "done": progress.done,
                "reached": progress.reached,
                "oldest_ts_ms": progress.oldest_ts_ms,
            }
        return {
            "scanning": self.scanning,
            "resumable": self._progress is not None,
            "backoff_until": self._backoff_until,
            "consecutive_failures": self._consecutive_failures,
            "streams": streams,
        }

    async def scan(self) -> HistoryScanResult:
        """Do one bounded unit of scan work. Concurrent callers share the in-flight call's result."""
        inflight = self._inflight
        if inflight is not None and not inflight.done():
            self.coalesced_calls += 1
            return await asyncio.shield(inflight)
        future = asyncio.get_running_loop().create_future()
        self._inflight = future
        try:
            result = await self._scan_step()
        except BaseException as exc:
            if isinstance(exc, asyncio.CancelledError):
                future.cancel()
            else:
                future.set_exception(exc)
                future.exception()  # mark retrieved; the caller receives the exception below
            raise
        else:
            future.set_result(result)
            return result
        finally:
            self._inflight = None

    # ------------------------------------------------------------------ scan machinery
    def _begin(self, now: float) -> Dict[str, _StreamProgress]:
        self._scan_started_at = now
        unresolved_ms = self._view.oldest_unresolved_ms()
        progress = {}
        for stream, endpoint in ((STREAM_ORDERS, ENDPOINT_INACTIVE_ORDERS), (STREAM_TRADES, ENDPOINT_TRADES)):
            high_water = HighWaterMark.decode(self._view.high_water(stream))
            anchors = []
            if high_water is not None:
                anchors.append(high_water.timestamp_ms)
            else:
                floor_ms = self._view.bootstrap_floor_ms(stream)
                if floor_ms is not None:
                    anchors.append(int(floor_ms))
            if anchors and unresolved_ms is not None:
                anchors.append(int(unresolved_ms))
            boundary = min(anchors) - self._overlap_ms if anchors else None
            progress[stream] = _StreamProgress(
                stream=stream, endpoint=endpoint, boundary_ms=boundary, previous_high_water=high_water,
            )
        return progress

    def _result(self, complete: bool, reason: Optional[str], pages: Dict[str, int], weight: int,
                conflicts: Optional[List[str]] = None,
                high_water: Optional[Dict[str, Optional[str]]] = None) -> HistoryScanResult:
        new_trades: List[ExchangeTradeRow] = []
        new_orders: List[ExchangeOrderRow] = []
        all_conflicts = list(conflicts or [])
        for progress in (self._progress or {}).values():
            for key, row in progress.rows.items():
                if self._view.committed_payload(progress.stream, key) is not None:
                    continue
                (new_trades if progress.stream == STREAM_TRADES else new_orders).append(row)
            for conflict in progress.conflicts:
                if conflict not in all_conflicts:
                    all_conflicts.append(conflict)
        high_water = high_water or {}
        result = HistoryScanResult(
            complete=complete,
            incomplete_reason=None if complete else reason,
            new_trades=new_trades,
            new_orders=new_orders,
            conflicts=all_conflicts,
            trades_high_water=high_water.get(STREAM_TRADES) if complete else None,
            orders_high_water=high_water.get(STREAM_ORDERS) if complete else None,
            pages_read=dict(pages),
            weight_used=weight,
        )
        self.last_result = result
        return result

    def _fail(self, reason: str, pages: Dict[str, int], weight: int, now: float) -> HistoryScanResult:
        result = self._result(False, reason, pages, weight)
        self._progress = None
        self._consecutive_failures += 1
        self._retry_after_failure = True
        delay = self._backoff_initial_s * (2 ** (self._consecutive_failures - 1))
        delay = min(delay, self._backoff_max_s)
        self._backoff_until = float(Decimal(str(now)) + delay)
        return result

    async def _scan_step(self) -> HistoryScanResult:
        now = self._clock()
        pages = {STREAM_TRADES: 0, STREAM_ORDERS: 0}
        if self._backoff_until is not None and now < self._backoff_until:
            return self._result(False, REASON_BACKOFF, pages, 0)
        if self._progress is None:
            try:
                self._progress = self._begin(now)
            except HistorySchemaError as exc:
                self._progress = None
                return self._fail(f"{REASON_SCHEMA_ERROR}:{exc}", pages, 0, now)
            self._last_scan_started_at = now
            self._wake_requested = False
        budget = self._weight_budget
        weight_used = 0
        pages_total = 0
        while True:
            progressed = False
            for progress in self._progress.values():
                if progress.done:
                    continue
                if self._max_pages_per_call is not None and pages_total >= self._max_pages_per_call:
                    break
                weight = self._port.request_weight(progress.endpoint)
                if weight > budget:
                    continue
                budget -= weight
                weight_used += weight
                pages_total += 1
                pages[progress.stream] += 1
                error = await self._fetch_page(progress)
                if error is not None:
                    return self._fail(error, pages, weight_used, self._clock())
                progressed = True
            if not progressed or all(p.done for p in self._progress.values()):
                break
        if not all(p.done for p in self._progress.values()):
            return self._result(False, REASON_IN_PROGRESS, pages, weight_used)
        return self._finalize(pages, weight_used)

    async def _fetch_page(self, progress: _StreamProgress) -> Optional[str]:
        cursor = progress.next_cursor
        progress.cursors_sent.append(cursor)
        try:
            if progress.stream == STREAM_TRADES:
                page = await self._port.trades_page(cursor, limit=self._page_limit)
            else:
                page = await self._port.inactive_orders_page(cursor, limit=self._page_limit)
        except asyncio.CancelledError:
            raise
        except HistorySchemaError as exc:
            return f"{REASON_SCHEMA_ERROR}:{progress.stream}:{exc}"
        except Exception as exc:  # noqa: BLE001 - any transport failure is a missing page
            return f"{REASON_PAGE_FETCH_ERROR}:{progress.stream}:{type(exc).__name__}"
        progress.pages_read += 1
        if not isinstance(page, HistoryPage) or not isinstance(page.rows, list):
            return f"{REASON_SCHEMA_ERROR}:{progress.stream}:not a HistoryPage"
        if page.raw_cursor_sent != cursor:
            return f"{REASON_SCHEMA_ERROR}:{progress.stream}:cursor echo mismatch"
        row_type = ExchangeTradeRow if progress.stream == STREAM_TRADES else ExchangeOrderRow
        if any(not isinstance(row, row_type) for row in page.rows):
            return f"{REASON_SCHEMA_ERROR}:{progress.stream}:unexpected row type"
        # A raw trade may yield two own legs (self-trade), so a trades page may carry 2 * limit rows.
        max_rows = self._page_limit * (2 if progress.stream == STREAM_TRADES else 1)
        if len(page.rows) > max_rows:
            return f"{REASON_OVERSIZED_PAGE}:{progress.stream}"
        next_cursor = page.next_cursor
        if next_cursor is not None and not isinstance(next_cursor, str):
            return f"{REASON_MALFORMED_CURSOR}:{progress.stream}"
        if next_cursor == "":
            next_cursor = None
        if next_cursor is not None and (
            len(next_cursor) > _MAX_CURSOR_LEN or any(ch.isspace() or not ch.isprintable() for ch in next_cursor)
        ):
            return f"{REASON_MALFORMED_CURSOR}:{progress.stream}"
        if next_cursor is not None and next_cursor in progress.cursors_sent:
            return f"{REASON_REPEATED_CURSOR}:{progress.stream}"
        if next_cursor is not None and not page.rows:
            return f"{REASON_EMPTY_PAGE_WITH_CURSOR}:{progress.stream}"
        for row in page.rows:
            error = self._apply_row(progress, row)
            if error is not None:
                return error
        if (
            progress.boundary_ms is not None
            and progress.oldest_ts_ms is not None
            and progress.oldest_ts_ms < progress.boundary_ms
        ):
            progress.done, progress.reached = True, "boundary"
        elif next_cursor is None:
            progress.done, progress.reached = True, "natural_end"
        else:
            progress.next_cursor = next_cursor
        return None

    def _apply_row(self, progress: _StreamProgress, row: object) -> Optional[str]:
        if row.account_index != self._port.account_index or row.market_id != self._port.market_id:
            return f"{REASON_SCHEMA_ERROR}:{progress.stream}:row outside scanned account/market"
        timestamp_ms = row.timestamp_ms
        if type(timestamp_ms) is not int:
            return f"{REASON_SCHEMA_ERROR}:{progress.stream}:non-integer timestamp"
        if progress.last_ts_ms is not None and timestamp_ms > progress.last_ts_ms:
            return f"{REASON_ORDERING_VIOLATION}:{progress.stream}:timestamp increased"
        progress.last_ts_ms = timestamp_ms
        try:
            if progress.stream == STREAM_TRADES:
                key = row.dedupe_key(self.domain)
                fingerprint = trade_payload_fingerprint(row)
            else:
                key = order_dedupe_key(self.domain, row)
                fingerprint = order_payload_fingerprint(row)
        except (HistorySchemaError, AttributeError, TypeError) as exc:
            return f"{REASON_SCHEMA_ERROR}:{progress.stream}:{type(exc).__name__}"
        if progress.stream == STREAM_TRADES:
            if _is_ascii_digits(row.trade_id_str):
                numeric_id = int(row.trade_id_str)
                if progress.last_numeric_id is not None and numeric_id > progress.last_numeric_id:
                    return f"{REASON_ORDERING_VIOLATION}:{progress.stream}:trade id increased"
                progress.last_numeric_id = numeric_id
        elif not is_terminal_order_status(row.status):
            self._add_conflict(progress, f"{REASON_CONFLICT}:inactive_order_not_terminal:{_key_label(key)}")
        if progress.newest is None:
            progress.newest = HighWaterMark(timestamp_ms=timestamp_ms, key=key)
        progress.oldest_ts_ms = timestamp_ms
        previous = progress.previous_high_water
        if previous is not None and tuple(previous.key) == tuple(key):
            progress.saw_previous_high_water_row = True
            if previous.timestamp_ms != timestamp_ms:
                self._add_conflict(progress, f"{REASON_CONFLICT}:high_water_row_changed:{progress.stream}")
        seen = progress.fingerprints.get(key)
        if seen is not None:
            progress.duplicate_rows += 1
            if seen != fingerprint:
                self._add_conflict(progress, f"{REASON_CONFLICT}:duplicate_key_payload_mismatch:{_key_label(key)}")
            return None
        progress.fingerprints[key] = fingerprint
        progress.rows[key] = row
        committed = self._view.committed_payload(progress.stream, key)
        if committed is not None and committed != fingerprint:
            self._add_conflict(progress, f"{REASON_CONFLICT}:committed_payload_mismatch:{_key_label(key)}")
        return None

    @staticmethod
    def _add_conflict(progress: _StreamProgress, conflict: str) -> None:
        if conflict not in progress.conflicts:
            progress.conflicts.append(conflict)

    def _finalize(self, pages: Dict[str, int], weight_used: int) -> HistoryScanResult:
        now = self._clock()
        conflicts: List[str] = []
        retention_gap = False
        trades_progress = self._progress[STREAM_TRADES]
        orders_progress = self._progress[STREAM_ORDERS]
        conflicts.extend(cumulative_conflicts(orders_progress.rows.values(), trades_progress.rows.values()))
        for progress in self._progress.values():
            previous = progress.previous_high_water
            if previous is not None and not progress.saw_previous_high_water_row:
                below_oldest = progress.oldest_ts_ms is None or previous.timestamp_ms < progress.oldest_ts_ms
                if progress.reached == "natural_end" and below_oldest:
                    # The newest row we committed last time is no longer served: history was truncated.
                    retention_gap = True
                    conflicts.append(f"{REASON_RETENTION_GAP}:{progress.stream}:high_water_row_unavailable")
                else:
                    conflicts.append(f"{REASON_CONFLICT}:high_water_row_missing:{progress.stream}")
            if (
                self._retention_horizon_ms is not None
                and progress.boundary_ms is not None
                and int(Decimal(str(now)) * 1000) - progress.boundary_ms > self._retention_horizon_ms
            ):
                retention_gap = True
                conflicts.append(f"{REASON_RETENTION_GAP}:{progress.stream}:boundary_older_than_retention")
        high_water = {
            name: (progress.newest.encode() if progress.newest is not None
                   else self._view.high_water(name))
            for name, progress in self._progress.items()
        }
        stream_conflicts = [c for p in self._progress.values() for c in p.conflicts]
        if retention_gap:
            reason = REASON_RETENTION_GAP
        elif conflicts or stream_conflicts:
            reason = REASON_CONFLICT
        else:
            reason = None
        complete = reason is None
        result = self._result(complete, reason, pages, weight_used, conflicts=conflicts, high_water=high_water)
        started_at = self._scan_started_at if self._scan_started_at is not None else now
        self._progress = None
        self._consecutive_failures = 0
        self._retry_after_failure = False
        self._backoff_until = None
        if complete:
            self.completed_scans.append(ScanRecord(started_at=started_at, completed_at=now))
        return result


def _key_label(key: Tuple) -> str:
    return "/".join(str(part) for part in key[3:])


def _is_ascii_digits(value: object) -> bool:
    return isinstance(value, str) and value.isascii() and value.isdigit()


def cumulative_conflicts(orders: Iterable[ExchangeOrderRow], trades: Iterable[ExchangeTradeRow]) -> List[str]:
    """AC-40: owned executions of a *terminal* order may never exceed its terminal cumulative fill.

    Only rows present in the same evidence set are compared, so a lower trade sum is never a
    conflict (older fills may lie outside the window); a higher one always is.
    """
    by_exchange_id: Dict[str, Decimal] = {}
    by_client_id: Dict[int, Decimal] = {}
    for trade in trades:
        if trade.own_exchange_order_id:
            by_exchange_id[trade.own_exchange_order_id] = (
                by_exchange_id.get(trade.own_exchange_order_id, Decimal("0")) + trade.size
            )
        elif trade.own_client_order_id is not None:
            by_client_id[trade.own_client_order_id] = (
                by_client_id.get(trade.own_client_order_id, Decimal("0")) + trade.size
            )
    conflicts = []
    for order in orders:
        if not is_terminal_order_status(order.status):
            continue
        executed = sum((by_exchange_id.get(i, Decimal("0")) for i in order_exchange_ids(order)), Decimal("0"))
        if order.client_order_id is not None:
            executed += by_client_id.get(order.client_order_id, Decimal("0"))
        if executed > order.filled_base_amount:
            label = order.order_index or order.order_id or str(order.client_order_id)
            conflicts.append(f"{REASON_CONFLICT}:trades_exceed_order_cumulative:{label}")
    return conflicts


def attribute_trades_to_order(order: ExchangeOrderRow,
                              trades: Iterable[ExchangeTradeRow]) -> Tuple[List[ExchangeTradeRow], List[str]]:
    """Exact-ID attribution of own executions to one order; never by price/size/time.

    A leg belongs to the order when its ``own_exchange_order_id`` equals the order's
    ``order_index``/``order_id`` or its ``own_client_order_id`` equals the order's client id.
    Contradicting ids, side, account or market are conflicts. Duplicate legs collapse by
    ``(trade_id_str, own_side, own_exchange_order_id)``; the same leg with a different payload
    is a conflict.
    """
    exchange_ids = set(order_exchange_ids(order))
    matched: Dict[Tuple[str, str, str], ExchangeTradeRow] = {}
    fingerprints: Dict[Tuple[str, str, str], str] = {}
    conflicts: List[str] = []
    for trade in trades:
        by_exchange = bool(trade.own_exchange_order_id) and trade.own_exchange_order_id in exchange_ids
        by_client = (
            order.client_order_id is not None
            and trade.own_client_order_id is not None
            and trade.own_client_order_id == order.client_order_id
        )
        if not (by_exchange or by_client):
            continue
        leg_key = (trade.trade_id_str, trade.own_side.value, trade.own_exchange_order_id or "")
        if by_exchange and trade.own_client_order_id not in (None, order.client_order_id):
            conflicts.append(f"{REASON_CONFLICT}:client_id_mismatch:{trade.trade_id_str}")
            continue
        if by_client and trade.own_exchange_order_id and not by_exchange:
            conflicts.append(f"{REASON_CONFLICT}:exchange_id_mismatch:{trade.trade_id_str}")
            continue
        if (
            trade.own_side != order.side
            or trade.account_index != order.account_index
            or trade.market_id != order.market_id
        ):
            conflicts.append(f"{REASON_CONFLICT}:leg_identity_mismatch:{trade.trade_id_str}")
            continue
        fingerprint = trade_payload_fingerprint(trade)
        if leg_key in fingerprints:
            if fingerprints[leg_key] != fingerprint:
                conflicts.append(f"{REASON_CONFLICT}:duplicate_key_payload_mismatch:{trade.trade_id_str}")
            continue
        fingerprints[leg_key] = fingerprint
        matched[leg_key] = trade
    return list(matched.values()), conflicts


@dataclass(frozen=True)
class SettlementDecision:
    release: bool
    reason: str                   # "released" or the first unmet condition / conflict code
    trades_cumulative: Decimal
    terminal_filled: Optional[Decimal]
    conflict: bool = False
    conflicts: Tuple[str, ...] = ()


def evaluate_terminal_release(
    *,
    terminal_row: Optional[ExchangeOrderRow],
    owned_trades: Sequence[ExchangeTradeRow],
    history_complete: bool,
    first_terminal_seen_at: Optional[float],
    complete_scans: Sequence[ScanRecord],
    now: float,
    settlement_delay_s: Decimal,
    settlement_scans: int,
) -> SettlementDecision:
    """Pure NG-HIST-002 settlement predicate (AC-42 scan part).

    Release requires *all* of: the exact terminal inactive-order row; the latest history result
    complete; sum of exactly-attributed owned executions == terminal cumulative fill (greater is a
    conflict, AC-40; smaller means history lag); ``now - first_terminal_seen_at >= delay``; at
    least ``settlement_scans`` complete scans started at/after the terminal row was first
    committed, at least one of them started after the delay. WS events, cancel acks or absence
    from the active list are not inputs and can never satisfy this predicate.
    """
    zero = Decimal("0")
    if terminal_row is None:
        return SettlementDecision(False, "no_terminal_row", zero, None)
    if not is_terminal_order_status(terminal_row.status):
        return SettlementDecision(False, "order_row_not_terminal", zero, terminal_row.filled_base_amount)
    matched, conflicts = attribute_trades_to_order(terminal_row, owned_trades)
    total = sum((trade.size for trade in matched), zero)
    filled = terminal_row.filled_base_amount
    if conflicts:
        return SettlementDecision(False, "attribution_conflict", total, filled, True, tuple(conflicts))
    if filled > terminal_row.initial_base_amount:
        return SettlementDecision(False, "filled_exceeds_initial", total, filled, True,
                                  (f"{REASON_CONFLICT}:filled_exceeds_initial",))
    if total > filled:
        return SettlementDecision(False, "trades_exceed_terminal_cumulative", total, filled, True,
                                  (f"{REASON_CONFLICT}:trades_exceed_order_cumulative",))
    if total < filled:
        return SettlementDecision(False, "awaiting_owned_executions", total, filled)
    if not history_complete:
        return SettlementDecision(False, "history_incomplete", total, filled)
    if first_terminal_seen_at is None:
        return SettlementDecision(False, "terminal_seen_time_unknown", total, filled)
    delay = Decimal(str(settlement_delay_s))
    first_seen = Decimal(str(first_terminal_seen_at))
    if Decimal(str(now)) - first_seen < delay:
        return SettlementDecision(False, "settlement_delay", total, filled)
    after_seen = [scan for scan in complete_scans if Decimal(str(scan.started_at)) >= first_seen]
    if len(after_seen) < settlement_scans:
        return SettlementDecision(False, "awaiting_repeat_scans", total, filled)
    if not any(Decimal(str(scan.started_at)) >= first_seen + delay for scan in after_seen):
        return SettlementDecision(False, "awaiting_post_delay_scan", total, filled)
    return SettlementDecision(True, "released", total, filled)
