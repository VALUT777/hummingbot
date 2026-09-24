"""Durable single-writer SQLite store of the neutral fixed-cell grid.

Spec: ``docs/superpowers/specs/2026-09-24-neutral-grid-spec.md`` (NG-DB-001..005, NG-HIST-002, NG-HIST-004,
NG-RISK-001); contracts: ``docs/neutral-grid/CONTRACTS.md`` ("B store").

Durability
----------
The database runs in WAL mode with ``synchronous=FULL`` and ``fullfsync=ON``:

* WAL lets the local UI/CLI processes read committed snapshots (``open_readonly``) without ever blocking the
  engine's commits; in rollback-journal mode a long reader would hold a SHARED lock and make an engine commit
  fail with ``SQLITE_BUSY``, which would needlessly push the engine into DEGRADED;
* ``synchronous=FULL`` makes SQLite fsync the WAL on every commit, so a transaction that returned from COMMIT
  survives power loss (``NORMAL`` in WAL mode is only durable across application crashes);
* ``fullfsync`` uses ``F_FULLFSYNC`` on macOS where plain ``fsync`` does not flush the drive cache
  (no-op elsewhere). WAL requires a local filesystem; do not put the database on a network share.

Every state change goes through :meth:`NeutralGridStore.transaction` (``BEGIN IMMEDIATE`` ... ``COMMIT``).
Any SQLite error (disk full, I/O, read-only, busy, constraint/trigger violation) or commit failure rolls the
whole transaction back, raises :class:`PersistenceError` and sets :attr:`NeutralGridStore.degraded_reason`.
A degraded store refuses new SUBMIT intents until :meth:`NeutralGridStore.clear_degraded` (audited) succeeds.

Single writer
-------------
The engine opener takes two ``fcntl.flock`` locks -- one keyed by ``(connector domain, account index,
trading pair)`` in the host-wide directory ``~/.hummingbot/neutral_grid/locks`` (independent of the checkout's
``data_path``; see :func:`default_host_dir`) and one next to the database file -- and then claims the
``engine_owner`` row, which is re-checked inside every write transaction (fencing). This protects one host
only: **it is NOT a distributed lock**. Two hosts sharing the database file or the exchange account are
outside the guarantee (spec section 1); a foreign-host owner row is refused unless an operator explicitly
takes over.

Other processes may only use :meth:`NeutralGridStore.open_readonly` (``mode=ro`` + ``query_only``) or
:meth:`NeutralGridStore.open_command_client`, whose SQLite authorizer only permits INSERT into ``commands``.

Money and ids
-------------
Decimals are stored as canonical TEXT, exchange/trade ids as TEXT, client order ids (CID) as INTEGER;
there is no REAL column and every bound parameter is checked to be non-float. CIDs are allocated as
``cid_epoch << 40 | seq`` (always below ``2**48``); a database recreated after loss gets a new epoch so CIDs of
the lost database are never reused.
"""
from __future__ import annotations

import contextlib
import errno
import fcntl
import hashlib
import json
import logging
import os
import re
import socket
import sqlite3
import threading
import time
import uuid
import dataclasses
from dataclasses import dataclass, field, fields, is_dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Set, Tuple, Union

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    MAX_CLIENT_ORDER_ID,
    CellSpec,
    CellState,
    CommandKind,
    CommandStatus,
    EngineState,
    ExchangeOrderRow,
    ExchangeTradeRow,
    LegIdentity,
    LegRole,
    OrderState,
    OrderTypePolicy,
    Side,
    Snapshot,
    SubmitRequest,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.migrations import MIGRATIONS, Migration

logger = logging.getLogger(__name__)

__all__ = [
    "APPLICATION_ID", "AuditEvent", "CID_EPOCH_SHIFT", "FAULT_POINTS", "FINAL_ORDER_STATES", "STREAM_INACTIVE_ORDERS",
    "STREAM_TRADES", "BootstrapError", "BootstrapRecord", "CellRecord", "CellStateChange", "CidAllocationError",
    "CidCollisionError", "CidExhaustedError", "CommandRecord", "ConfigMutationError", "CursorRecord",
    "CursorUpdate", "CycleChange", "CycleRecord", "EngineIdentity", "EngineRecord", "EngineStateChange",
    "EntryBlockedError", "FaultHooks", "FillAllocation", "FillRecord", "GridMigration", "GridRecord",
    "HistoryBatchResult", "HistoryConflictRecord", "IdentityMismatchError", "InboxRecord", "IntentRecord",
    "InvalidTransitionError", "LedgerState", "LegRecord", "LegStateChange", "ManualRecovery",
    "NeutralGridStore", "OrderMatch", "OrderRecord", "OutboxRecord", "OwnershipLostError", "PersistenceError",
    "PositionLedger", "PriorRunEvidenceError", "ReadOnlyStoreError", "Reservation", "ReservationRecord",
    "ReservationRelease", "SchemaVersionError", "SimulatedCrash", "StoreClosedError", "StoreCorruptError",
    "StoreError", "StoreIntegrityError", "StoreLockedError", "StoreMissingError", "StoredSnapshot",
    "Transaction", "canonical_decimal", "canonical_json", "default_db_path", "default_host_dir", "default_lock_dir",
    "order_dedupe_key", "parse_decimal", "trade_dedupe_key",
]

APPLICATION_ID = 0x4E475244  # "NGRD"
SQLITE_HEADER = b"SQLite format 3\x00"
MIN_SQLITE_VERSION = (3, 37, 0)  # STRICT tables
CID_EPOCH_SHIFT = 40
CID_MAX_SEQ = (1 << CID_EPOCH_SHIFT) - 1
CID_MAX_EPOCH = 255
JS_SAFE_INT = (1 << 53) - 1
STREAM_TRADES = "TRADES"
STREAM_INACTIVE_ORDERS = "INACTIVE_ORDERS"
STREAMS = (STREAM_TRADES, STREAM_INACTIVE_ORDERS)
KIND_SUBMIT = "SUBMIT"
KIND_CANCEL = "CANCEL"
FINAL_ORDER_STATES = frozenset({OrderState.TERMINAL, OrderState.REJECTED_UNSENT, OrderState.REJECTED_ZERO_FILL})
_REJECTED_STATES = frozenset({OrderState.REJECTED_UNSENT, OrderState.REJECTED_ZERO_FILL})
# States the engine may set from venue/history evidence (set_leg_state). INTENT, CANCEL_* and REJECTED_UNSENT
# are only reachable through the outbox protocol (mark_dispatching / record_transport_result /
# record_cancel_intent) or an audited manual reconciliation.
_EVIDENCE_TARGETS = frozenset({OrderState.LIVE, OrderState.TERMINAL_UNKNOWN, OrderState.TERMINAL,
                               OrderState.REJECTED_ZERO_FILL})

FAULT_POINTS = frozenset({
    "before_commit", "after_commit",
    "after_cid_allocated",
    "before_intent_commit", "after_intent_commit",
    "before_dispatch_commit", "after_dispatch_commit",
    "before_transport", "after_transport",
    "after_transport_before_result_commit", "before_result_commit", "after_result_commit",
    "before_cancel_intent_commit", "after_cancel_intent_commit",
    "mid_history_batch", "before_cursor_commit", "after_history_commit",
    "before_snapshot_commit", "before_command_commit",
})

_KEEP: Any = object()


# --------------------------------------------------------------------------------------------------------------
# Errors
# --------------------------------------------------------------------------------------------------------------


class StoreError(Exception):
    """Base class of every store refusal."""


class PersistenceError(StoreError):
    """A write could not be made durable (disk full, I/O, read-only, commit failure, busy, constraint).

    Nothing of the failed transaction is applied. The engine must go DEGRADED and send nothing.
    """


class StoreIntegrityError(PersistenceError):
    """A SQLite constraint or append-only/immutability trigger rejected a write."""


class StoreClosedError(StoreError):
    pass


class ReadOnlyStoreError(StoreError):
    pass


class StoreLockedError(StoreError):
    """Another engine owns this account/market on this host (or the DB owner row names another host)."""


class OwnershipLostError(StoreLockedError):
    pass


class StoreMissingError(StoreError):
    pass


class PriorRunEvidenceError(StoreError):
    """Evidence of a previous engine exists but its database is missing/corrupt/replaced (AC-54)."""

    def __init__(self, message: str, evidence: Sequence[str] = ()):
        super().__init__(message)
        self.evidence = list(evidence)


class StoreCorruptError(PriorRunEvidenceError):
    pass


class SchemaVersionError(StoreError):
    pass


class IdentityMismatchError(StoreError):
    pass


class ConfigMutationError(StoreError):
    def __init__(self, message: str, blockers: Sequence[str] = ()):
        super().__init__(message)
        self.blockers = list(blockers)


class BootstrapError(StoreError):
    pass


class CidAllocationError(StoreError):
    pass


class CidExhaustedError(CidAllocationError):
    pass


class CidCollisionError(CidAllocationError):
    def __init__(self, message: str, cid: int):
        super().__init__(message)
        self.cid = cid


class InvalidTransitionError(StoreError):
    pass


class EntryBlockedError(InvalidTransitionError):
    def __init__(self, message: str, blockers: Sequence[str] = ()):
        super().__init__(message)
        self.blockers = list(blockers)


class SimulatedCrash(BaseException):
    """Raised by an armed fault point. BaseException so ordinary ``except Exception`` handlers do not swallow
    it. The store that raised it behaves like a dead process: its connection is dropped without commit and its
    locks are released; tests must reopen the database from disk."""


# --------------------------------------------------------------------------------------------------------------
# Exact values
# --------------------------------------------------------------------------------------------------------------


def canonical_decimal(value: Any, name: str = "value") -> str:
    """Canonical TEXT of an exact decimal: no exponent, no trailing fractional zeros, no negative zero.

    Never rounds (does not use the context precision). Rejects float, bool, NaN and infinities.
    """
    if isinstance(value, bool) or isinstance(value, float):
        raise TypeError(f"{name} must be Decimal or int, not {type(value).__name__}")
    if isinstance(value, int):
        value = Decimal(value)
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal, not {type(value).__name__}")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    sign, digits, exponent = value.as_tuple()
    if not any(digits):
        return "0"
    digit_list = list(digits)
    while exponent < 0 and digit_list[-1] == 0:
        digit_list.pop()
        exponent += 1
    return format(Decimal((sign, tuple(digit_list), exponent)), "f")


def parse_decimal(text: Optional[str]) -> Optional[Decimal]:
    if text is None:
        return None
    try:
        value = Decimal(text)
    except (InvalidOperation, TypeError) as exc:
        raise StoreIntegrityError(f"stored decimal is malformed: {text!r}") from exc
    if not value.is_finite():
        raise StoreIntegrityError(f"stored decimal is not finite: {text!r}")
    return value


def _jsonable(value: Any, allow_float: bool) -> Any:
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, int):
        return str(value) if abs(value) > JS_SAFE_INT else value
    if isinstance(value, float):
        if not allow_float:
            raise TypeError("float is not allowed in ledger data; use Decimal/str/int")
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError("non-finite float")
        return value
    if isinstance(value, Decimal):
        return canonical_decimal(value)
    if is_dataclass(value) and not isinstance(value, type):
        return {f.name: _jsonable(getattr(value, f.name), allow_float) for f in fields(value)}
    if isinstance(value, Mapping):
        out = {}
        for key, item in value.items():
            if isinstance(key, Enum):
                key = key.value
            if not isinstance(key, str):
                key = str(key)
            out[key] = _jsonable(item, allow_float)
        return out
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [_jsonable(item, allow_float) for item in value]
        return sorted(items, key=lambda x: json.dumps(x, sort_keys=True)) if isinstance(value, (set, frozenset)) \
            else items
    raise TypeError(f"cannot store {type(value).__name__} as JSON")


def canonical_json(value: Any, allow_float: bool = False) -> str:
    """Deterministic JSON: Decimals as canonical strings, ints beyond 2**53 as strings, sorted keys."""
    return json.dumps(_jsonable(value, allow_float), sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False)


def _loads(text: Optional[str]) -> Any:
    if text is None:
        return None
    return json.loads(text, parse_float=Decimal)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _check_params(params: Any) -> None:
    values = params.values() if isinstance(params, Mapping) else params
    for value in values:
        if isinstance(value, float):
            raise TypeError("float SQL parameter refused: money/ids must be Decimal text or int")
        if isinstance(value, Decimal):
            raise TypeError("Decimal SQL parameter refused: bind canonical_decimal(value) text")


def _require_text(value: Any, name: str, max_len: int = 512) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > max_len:
        raise ValueError(f"{name} must be a non-empty string (<= {max_len} chars)")
    return value


def _require_int(value: Any, name: str, minimum: Optional[int] = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be int")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


def _require_positive(value: Any, name: str) -> Decimal:
    text = canonical_decimal(value, name)
    result = Decimal(text)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _enum(enum_type, value: Any, name: str):
    try:
        return enum_type(value.value if isinstance(value, Enum) else value)
    except ValueError as exc:
        raise ValueError(f"invalid {name}: {value!r}") from exc


def trade_dedupe_key(domain: str, row: ExchangeTradeRow) -> str:
    """Canonical trade key ``(domain, account, market, trade_id_str, own_side, own_exchange_order_id)``."""
    return canonical_json(["TRADE", *row.dedupe_key(domain)])


def _client_id_text(row: ExchangeOrderRow) -> str:
    if row.client_order_id_str:
        return row.client_order_id_str
    return str(row.client_order_id) if row.client_order_id is not None else ""


def _row_exchange_id(row: ExchangeOrderRow) -> str:
    exchange_id = row.order_index or row.order_id
    if not exchange_id:
        raise ValueError("order row has no exchange order id (order_index/order_id)")
    return exchange_id


def order_dedupe_key(domain: str, row: ExchangeOrderRow) -> str:
    """Canonical order key ``(domain, account, market, exact exchange order id)`` -- the same identity as WS-C's
    ``history.order_dedupe_key``; client ids are payload, so a changed client id under one key is a conflict."""
    return canonical_json(["ORDER", domain, row.account_index, row.market_id, _row_exchange_id(row)])


def _trade_payload(row: ExchangeTradeRow) -> Dict[str, Any]:
    return {
        "trade_id_str": row.trade_id_str, "account_index": row.account_index, "market_id": row.market_id,
        "own_side": Side(row.own_side).value, "own_exchange_order_id": row.own_exchange_order_id,
        "own_client_order_id": row.own_client_order_id, "size": canonical_decimal(row.size, "size"),
        "price": canonical_decimal(row.price, "price"), "is_maker": row.is_maker, "timestamp_ms": row.timestamp_ms,
    }


def _order_payload(row: ExchangeOrderRow) -> Dict[str, Any]:
    return {
        "client_order_id": row.client_order_id, "client_order_id_str": row.client_order_id_str,
        "order_id": row.order_id, "order_index": row.order_index, "nonce": row.nonce,
        "account_index": row.account_index, "market_id": row.market_id, "side": Side(row.side).value,
        "price": canonical_decimal(row.price, "price"),
        "initial_base_amount": canonical_decimal(row.initial_base_amount, "initial_base_amount"),
        "filled_base_amount": canonical_decimal(row.filled_base_amount, "filled_base_amount"),
        "remaining_base_amount": canonical_decimal(row.remaining_base_amount, "remaining_base_amount"),
        "status": row.status, "reduce_only": bool(row.reduce_only), "timestamp_ms": row.timestamp_ms,
    }


def _row_client_id(row: ExchangeOrderRow) -> Optional[int]:
    if row.client_order_id is not None and not isinstance(row.client_order_id, bool):
        return int(row.client_order_id)
    text = row.client_order_id_str
    if text and text.isdigit():
        return int(text)
    return None


def _water_fill(shares: Mapping[int, Decimal], filled: Decimal) -> Dict[int, Decimal]:
    """Per-generation part of a cumulative aggregate fill, oldest generation first (WS-A ``allocated_filled``)."""
    left, out = filled, {}
    for generation in sorted(shares):
        take = min(shares[generation], left)
        out[generation] = take
        left -= take
    return out


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


# --------------------------------------------------------------------------------------------------------------
# Identity and paths
# --------------------------------------------------------------------------------------------------------------


_SAFE = re.compile(r"[^A-Za-z0-9._-]")


@dataclass(frozen=True)
class EngineIdentity:
    """Who owns a database: one account on one market of one connector domain."""
    connector_name: str
    connector_domain: str
    account_index: int
    trading_pair: str

    def __post_init__(self):
        _require_text(self.connector_name, "connector_name", 128)
        _require_text(self.connector_domain, "connector_domain", 128)
        _require_text(self.trading_pair, "trading_pair", 64)
        _require_int(self.account_index, "account_index", 0)

    @property
    def engine_id(self) -> str:
        return f"{self.connector_domain}:{self.account_index}:{self.trading_pair}"

    @property
    def lock_key(self) -> str:
        return "neutral_grid." + ".".join(
            _SAFE.sub("_", part) for part in (self.connector_domain, str(self.account_index), self.trading_pair))


HOST_DIR_ENV = "HUMMINGBOT_NEUTRAL_GRID_HOST_DIR"


def default_host_dir() -> Path:
    """Host-wide home of the single-writer locks and prior-run markers: ``~/.hummingbot/neutral_grid``.

    It deliberately does NOT depend on ``hummingbot.data_path()`` (which is per checkout), so two checkouts, worktrees
    or launchers of the same user on one host contend for the same ``(domain, account, pair)`` lock and see the same
    prior-run marker. ``$HUMMINGBOT_NEUTRAL_GRID_HOST_DIR`` overrides it (e.g. a directory shared by containers of one
    host). This is still NOT a distributed lock: other hosts, users or unshared mounts are not covered.
    """
    override = os.environ.get(HOST_DIR_ENV)
    return Path(override) if override else Path.home() / ".hummingbot" / "neutral_grid"


def default_lock_dir(base_dir: Optional[Union[str, Path]] = None) -> Path:
    """``<host dir>/locks`` (see :func:`default_host_dir`); ``base_dir`` keeps the old explicit layout."""
    if base_dir is None:
        return default_host_dir() / "locks"
    return Path(base_dir) / "neutral_grid" / "locks"


def default_db_path(identity: EngineIdentity, base_dir: Optional[Union[str, Path]] = None) -> Path:
    """``<hummingbot data path>/neutral_grid/<domain>.<account>.<pair>.sqlite3``."""
    if base_dir is None:
        from hummingbot import data_path
        base_dir = data_path()
    return Path(base_dir) / "neutral_grid" / f"{identity.lock_key}.sqlite3"


# --------------------------------------------------------------------------------------------------------------
# Fault injection
# --------------------------------------------------------------------------------------------------------------


@dataclass
class _Armed:
    exc: Optional[BaseException]
    skip: int
    action: Optional[Callable[[str], None]]


class FaultHooks:
    """Named crash points plus real SQLite storage faults for tests.

    * ``arm(point)`` makes the next hit of a named point raise :class:`SimulatedCrash` (or ``exc``, or run
      ``action`` -- e.g. ``os._exit`` in a subprocess test);
    * ``simulate_disk_full()`` caps ``max_page_count`` at the current size so SQLite itself returns
      ``SQLITE_FULL`` as soon as a write needs a new page;
    * ``simulate_read_only()`` sets ``query_only`` so SQLite returns ``SQLITE_READONLY``;
    * ``fail_commit(exc)`` / ``fail_statement(substring, exc)`` inject arbitrary errors (e.g. ``disk I/O error``).
    """

    def __init__(self):
        self._armed: Dict[str, List[_Armed]] = {}
        self.hits: List[str] = []
        self._statement_faults: List[Tuple[str, BaseException]] = []
        self._commit_faults: List[BaseException] = []
        self.storage_fault: Optional[str] = None
        self.storage_version = 0

    def arm(self, point: str, exc: Optional[BaseException] = None, *, skip: int = 0,
            action: Optional[Callable[[str], None]] = None) -> None:
        if point not in FAULT_POINTS and not point.startswith("engine."):
            raise ValueError(f"unknown fault point {point!r} (engine-defined points must start with 'engine.')")
        self._armed.setdefault(point, []).append(_Armed(exc=exc, skip=skip, action=action))

    def disarm(self, point: Optional[str] = None) -> None:
        if point is None:
            self._armed.clear()
        else:
            self._armed.pop(point, None)

    def hit(self, point: str) -> None:
        self.hits.append(point)
        queue = self._armed.get(point)
        if not queue:
            return
        armed = queue[0]
        if armed.skip > 0:
            armed.skip -= 1
            return
        queue.pop(0)
        if armed.action is not None:
            armed.action(point)
            return
        raise armed.exc if armed.exc is not None else SimulatedCrash(point)

    def fail_statement(self, contains: str, exc: BaseException) -> None:
        self._statement_faults.append((contains, exc))

    def fail_commit(self, exc: BaseException) -> None:
        self._commit_faults.append(exc)

    def simulate_disk_full(self) -> None:
        self.storage_fault = "disk_full"
        self.storage_version += 1

    def simulate_read_only(self) -> None:
        self.storage_fault = "read_only"
        self.storage_version += 1

    def restore_storage(self) -> None:
        self.storage_fault = None
        self.storage_version += 1

    def _statement_fault(self, sql: str) -> Optional[BaseException]:
        for index, (contains, exc) in enumerate(self._statement_faults):
            if contains in sql:
                del self._statement_faults[index]
                return exc
        return None

    def _commit_fault(self) -> Optional[BaseException]:
        return self._commit_faults.pop(0) if self._commit_faults else None


class _Connection:
    """Every statement of the store goes through this wrapper: float guard + fault injection."""

    def __init__(self, raw: sqlite3.Connection, hooks: FaultHooks):
        self.raw = raw
        self._hooks = hooks
        self._storage_version = 0
        self._default_max_pages = raw.execute("PRAGMA max_page_count").fetchone()[0]

    def _apply_storage_fault(self) -> None:
        if self._storage_version == self._hooks.storage_version:
            return
        self._storage_version = self._hooks.storage_version
        fault = self._hooks.storage_fault
        pages = self.raw.execute("PRAGMA page_count").fetchone()[0]
        self.raw.execute(f"PRAGMA max_page_count = {int(pages) if fault == 'disk_full' else self._default_max_pages}")
        self.raw.execute(f"PRAGMA query_only = {1 if fault == 'read_only' else 0}")

    def execute(self, sql: str, params: Any = ()) -> sqlite3.Cursor:
        _check_params(params)
        self._apply_storage_fault()
        injected = self._hooks._statement_fault(sql)
        if injected is not None:
            raise injected
        return self.raw.execute(sql, params)

    def executescript(self, sql: str) -> None:
        self.raw.executescript(sql)

    def commit(self) -> None:
        injected = self._hooks._commit_fault()
        if injected is not None:
            raise injected
        self.raw.execute("COMMIT")

    @property
    def in_transaction(self) -> bool:
        return self.raw.in_transaction

    def close(self) -> None:
        self.raw.close()


# --------------------------------------------------------------------------------------------------------------
# Host lock and prior-run markers
# --------------------------------------------------------------------------------------------------------------


class _HostLock:
    """``flock`` on a lock file: exclusive per open file description, released by the OS on process death.

    Host-local only; NOT a distributed lock. Lock files are never deleted (deleting would race)."""

    def __init__(self, path: Path):
        self.path = path
        self._fd: Optional[int] = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            if exc.errno in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                raise StoreLockedError(f"another neutral grid engine holds {self.path} on this host") from exc
            raise PersistenceError(f"cannot lock {self.path}: {exc}") from exc
        try:
            os.ftruncate(fd, 0)
            os.write(fd, f"pid={os.getpid()} host={socket.gethostname()}\n".encode())
        except OSError:
            pass
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        fd, self._fd = self._fd, None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_marker(path: Path, content: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as output:
            output.write(canonical_json(content))
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        _fsync_dir(path.parent)
    except OSError as exc:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise PersistenceError(f"cannot durably write prior-run marker {path}: {exc}") from exc


def _read_marker(path: Path) -> Optional[Dict[str, Any]]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return document if isinstance(document, dict) else None


# --------------------------------------------------------------------------------------------------------------
# Records (inputs and outputs)
# --------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ManualRecovery:
    """Explicit operator decision to recreate a lost database (AC-54). Never adopts the current position."""
    actor: str
    reason: str
    evidence: Mapping[str, Any]
    min_cid_epoch: Optional[int] = None


@dataclass(frozen=True)
class BootstrapRecord:
    """First bootstrap only (NG-RISK-001). ``baseline`` is the operator-confirmed signed position B."""
    grid_id: str
    config_fingerprint: str
    config: Any
    lower_price: Decimal
    upper_price: Decimal
    order_amount_base: Decimal
    prices: Sequence[Decimal]
    cells: Sequence[CellSpec]
    anchor: Decimal
    baseline: Decimal
    market_id: int
    bootstrap_cut_ts_ms: int
    actor: str
    confirmation: str
    trades_cut: Optional[str] = None
    orders_cut: Optional[str] = None


@dataclass(frozen=True)
class GridMigration:
    """Audited replacement of the grid (AC-52): only when the old grid is quiescent; old cycles are kept."""
    new_grid_id: str
    config_fingerprint: str
    config: Any
    lower_price: Decimal
    upper_price: Decimal
    order_amount_base: Decimal
    prices: Sequence[Decimal]
    cells: Sequence[CellSpec]
    anchor: Decimal
    actor: str
    reason: str


@dataclass(frozen=True)
class Reservation:
    """Risk/slot reservation committed with an intent; the full submitted amount stays reserved until a
    final leg state is proven."""
    side: Side
    amount: Decimal
    slots: int = 1


@dataclass(frozen=True)
class CursorUpdate:
    """``None`` leaves a field unchanged. ``high_water_ts_ms``/``last_full_scan_ms`` may never decrease."""
    stream: str
    cursor: Optional[str] = None
    clear_cursor: bool = False
    high_water: Optional[str] = None
    high_water_ts_ms: Optional[int] = None
    complete: Optional[bool] = None
    incomplete_reason: Optional[str] = None
    last_full_scan_ms: Optional[int] = None
    required_boundary_ts_ms: Optional[int] = None
    required_boundary_marker: Optional[str] = None
    oldest_available_ts_ms: Optional[int] = None


@dataclass(frozen=True)
class LegStateChange:
    cid: int
    to_state: OrderState
    reason: str = ""
    expected_from: Optional[Tuple[OrderState, ...]] = None


@dataclass(frozen=True)
class CellStateChange:
    grid_id: str
    cell_id: int
    to_state: CellState
    blocker: Any = _KEEP
    reason: str = ""


@dataclass(frozen=True)
class CycleChange:
    grid_id: str
    cell_id: int
    generation: int
    dust: Optional[Decimal] = None


@dataclass(frozen=True)
class EngineStateChange:
    state: EngineState
    reason: Optional[str] = None


@dataclass(frozen=True)
class ReservationRelease:
    cid: int
    reason: str


@dataclass(frozen=True)
class FillAllocation:
    dedupe_key: str
    parts: Tuple[Tuple[str, int, int, Decimal], ...]  # (grid_id, cell_id, generation, amount)


LedgerTransition = Union[LegStateChange, CellStateChange, CycleChange, EngineStateChange, ReservationRelease,
                         FillAllocation]


@dataclass(frozen=True)
class EngineRecord:
    db_uuid: str
    engine_id: str
    connector_name: str
    connector_domain: str
    account_index: int
    trading_pair: str
    market_id: Optional[int]
    cid_epoch: int
    created_at_ms: int
    recovered_from_loss: bool
    bootstrapped_at_ms: Optional[int]
    initial_grid_id: Optional[str]
    initial_baseline: Optional[Decimal]
    effective_baseline: Optional[Decimal]
    bootstrap_trades_cut: Optional[str]
    bootstrap_orders_cut: Optional[str]
    bootstrap_cut_ts_ms: Optional[int]
    bootstrap_actor: Optional[str]
    current_grid_id: Optional[str]
    config_revision: int
    engine_revision: int
    reconciliation_revision: int
    engine_state: EngineState
    state_reason: Optional[str]
    pause_reason: Optional[str]
    stop_reason: Optional[str]
    manual_reconcile_required: bool
    manual_reconcile_reason: Optional[str]
    updated_at_ms: int

    @property
    def bootstrapped(self) -> bool:
        return self.bootstrapped_at_ms is not None


@dataclass(frozen=True)
class GridRecord:
    grid_id: str
    fingerprint: str
    config_revision: int
    lower_price: Decimal
    upper_price: Decimal
    cell_count: int
    order_amount_base: Decimal
    prices: Tuple[Decimal, ...]
    anchor: Decimal
    status: str
    migrated_from: Optional[str]
    created_at_ms: int


@dataclass(frozen=True)
class CellRecord:
    grid_id: str
    cell_id: int
    low_price: Decimal
    high_price: Decimal
    entry_side: Side
    generation: int
    state: str
    blocker: Optional[str]
    reserved_slots: int
    queued_at_ms: Optional[int]

    def spec(self) -> CellSpec:
        return CellSpec(self.cell_id, self.low_price, self.high_price, self.entry_side)


@dataclass(frozen=True)
class CycleRecord:
    grid_id: str
    cell_id: int
    generation: int
    entry_side: Side
    entry_price: Decimal
    tp_price: Decimal
    planned_amount: Decimal
    config_revision: int
    entry_filled: Decimal
    exit_filled: Decimal
    dust: Decimal
    state: str
    late_evidence: int
    opened_at_ms: int
    closed_at_ms: Optional[int]
    close_reason: Optional[str]

    @property
    def tp_side(self) -> Side:
        return Side.SELL if self.entry_side == Side.BUY else Side.BUY


@dataclass(frozen=True)
class LegRecord:
    cid: int
    grid_id: str
    cell_id: int
    generation: int
    role: LegRole
    revision: int
    side: Side
    price: Decimal
    amount: Decimal
    order_type: OrderTypePolicy
    reduce_only: bool
    expiry_ms: Optional[int]
    state: OrderState
    filled: Decimal
    created_at_ms: int
    updated_at_ms: int
    # aggregate TP only: generation -> exact share of ``amount`` (same cell; WS-A ``Leg.allocation``)
    allocation: Optional[Dict[int, Decimal]] = None

    @property
    def identity(self) -> LegIdentity:
        return LegIdentity(self.grid_id, self.cell_id, self.generation, self.role, self.revision)

    @property
    def remaining(self) -> Decimal:
        return self.amount - self.filled

    @property
    def final(self) -> bool:
        return self.state in FINAL_ORDER_STATES

    def submit_request(self) -> SubmitRequest:
        return SubmitRequest(client_order_id=self.cid, side=self.side, price=self.price, amount=self.amount,
                             order_type=self.order_type, reduce_only=self.reduce_only, expiry_ms=self.expiry_ms)


@dataclass(frozen=True)
class OrderRecord:
    cid: int
    exchange_order_id: Optional[str]
    order_index: Optional[str]
    nonce: Optional[str]
    submission_state: str
    cancel_state: str
    venue_status: Optional[str]
    venue_filled: Optional[Decimal]
    venue_remaining: Optional[Decimal]
    venue_final: bool
    last_evidence_ms: Optional[int]


@dataclass(frozen=True)
class ReservationRecord:
    cid: int
    side: Side
    amount: Decimal
    slots: int
    state: str
    created_at_ms: int
    released_at_ms: Optional[int]
    release_reason: Optional[str]


@dataclass(frozen=True)
class OutboxRecord:
    id: int
    kind: str
    cid: int
    request: Dict[str, Any]
    status: str
    attempts: int
    prev_leg_state: Optional[str]
    outcome: Optional[TransportOutcome]
    outcome_detail: Optional[str]
    created_at_ms: int
    dispatched_at_ms: Optional[int]
    result_at_ms: Optional[int]

    @property
    def transport_possibly_invoked(self) -> bool:
        """False only for PENDING rows: the protocol forbids calling transport before ``mark_dispatching``
        commits, so a PENDING row is proof the venue never saw this request."""
        return self.status != "PENDING"


@dataclass(frozen=True)
class IntentRecord:
    cid: int
    outbox_id: int
    request: SubmitRequest
    leg: LegRecord
    created: bool


@dataclass(frozen=True)
class FillRecord:
    dedupe_key: str
    trade_id_str: str
    own_side: Side
    own_exchange_order_id: Optional[str]
    cid: int
    grid_id: str
    cell_id: int
    generation: int
    role: LegRole
    size: Decimal
    price: Decimal
    timestamp_ms: int
    inbox_id: int
    late: bool


@dataclass(frozen=True)
class InboxRecord:
    id: int
    stream: str
    dedupe_key: str
    payload_hash: str
    payload: Dict[str, Any]
    raw_json: str
    status: str
    cid: Optional[int]
    detail: Optional[str]
    received_at_ms: int
    resolved_at_ms: Optional[int]
    resolution: Optional[str]


@dataclass(frozen=True)
class HistoryConflictRecord:
    id: int
    kind: str
    inbox_id: Optional[int]
    cid: Optional[int]
    detail: str
    created_at_ms: int
    resolved_at_ms: Optional[int]
    resolution: Optional[str]
    new: bool = False


@dataclass
class HistoryBatchResult:
    applied: List[InboxRecord] = field(default_factory=list)
    duplicates: int = 0
    unmatched: List[InboxRecord] = field(default_factory=list)
    pre_cut: List[InboxRecord] = field(default_factory=list)
    conflicts: List[HistoryConflictRecord] = field(default_factory=list)
    new_fills: List[FillRecord] = field(default_factory=list)
    late_fills: List[FillRecord] = field(default_factory=list)
    touched_cids: Set[int] = field(default_factory=set)

    @property
    def has_conflicts(self) -> bool:
        return bool(self.conflicts)


@dataclass(frozen=True)
class CursorRecord:
    stream: str
    cursor: Optional[str]
    high_water: Optional[str]
    high_water_ts_ms: Optional[int]
    required_boundary_ts_ms: Optional[int]
    required_boundary_marker: Optional[str]
    oldest_available_ts_ms: Optional[int]
    complete: bool
    incomplete_reason: Optional[str]
    last_full_scan_ms: Optional[int]
    revision: int
    updated_at_ms: int
    retention_gap_open: bool = False


@dataclass(frozen=True)
class CommandRecord:
    id: int
    idempotency_key: str
    kind: str
    expected_config_revision: int
    expected_engine_revision: int
    payload: Dict[str, Any]
    status: CommandStatus
    result: Optional[Dict[str, Any]]
    created_at_ms: int
    claimed_at_ms: Optional[int]
    claim_count: int
    applied_at_ms: Optional[int]
    duplicate: bool = False
    request_mismatch: bool = False


@dataclass(frozen=True)
class StoredSnapshot:
    snapshot_version: int
    config_revision: int
    engine_revision: int
    committed_at_ms: int
    engine_state: EngineState
    snapshot_json: str

    @property
    def payload(self) -> Dict[str, Any]:
        return _loads(self.snapshot_json)

    def to_contract(self) -> Snapshot:
        document = self.payload
        reasons = list(document.pop("reasons", []))
        for key in ("snapshot_version", "config_revision", "engine_revision", "committed_at", "committed_at_ms",
                    "engine_state"):
            document.pop(key, None)
        return Snapshot(snapshot_version=self.snapshot_version, config_revision=self.config_revision,
                        engine_revision=self.engine_revision, committed_at=self.committed_at_ms / 1000,
                        engine_state=self.engine_state, reasons=reasons, payload=document)


@dataclass(frozen=True)
class AuditEvent:
    id: int
    at_ms: int
    kind: str
    actor: str
    engine_revision: Optional[int]
    payload: Dict[str, Any]


@dataclass(frozen=True)
class OrderMatch:
    """Drill-down hit of :meth:`NeutralGridStore.find_orders_by_id`; ``matched_on`` names the exact columns."""
    leg: LegRecord
    order: OrderRecord
    matched_on: Tuple[str, ...]


@dataclass(frozen=True)
class PositionLedger:
    baseline: Decimal
    confirmed_buys: Decimal
    confirmed_sells: Decimal

    @property
    def net(self) -> Decimal:
        return self.baseline + self.confirmed_buys - self.confirmed_sells


@dataclass(frozen=True)
class LedgerState:
    """Everything the engine needs to rebuild its in-memory model after restart (NG-DB-003 step 2)."""
    engine: EngineRecord
    grid: Optional[GridRecord]
    cells: List[CellRecord]
    open_cycles: List[CycleRecord]
    legs: List[LegRecord]
    orders: Dict[int, OrderRecord]
    unresolved_outbox: List[OutboxRecord]
    active_reservations: List[ReservationRecord]
    cursors: Dict[str, CursorRecord]
    open_conflicts: List[HistoryConflictRecord]
    unmatched: List[InboxRecord]
    position: Optional[PositionLedger]
    entry_blockers: List[str]
    queued_commands: int
    late_cycles: List[CycleRecord] = field(default_factory=list)  # released cycles with a late obligation


# --------------------------------------------------------------------------------------------------------------
# Transaction handle
# --------------------------------------------------------------------------------------------------------------


class Transaction:
    """Handle of the one open ``BEGIN IMMEDIATE`` transaction. Pass it to store methods."""

    def __init__(self, store: "NeutralGridStore"):
        self._store = store
        self._after_commit: List[str] = []
        self._failed: Optional[BaseException] = None
        self.committed = False

    def after_commit(self, point: str) -> None:
        """Hit fault point ``point`` right after this transaction commits."""
        self._after_commit.append(point)


# --------------------------------------------------------------------------------------------------------------
# The store
# --------------------------------------------------------------------------------------------------------------


_WRITER = "writer"
_READONLY = "readonly"
_COMMANDS = "command_client"


class NeutralGridStore:
    """See module docstring. Create with :meth:`open`, :meth:`open_readonly` or :meth:`open_command_client`."""

    def __init__(self, *, conn: _Connection, path: Path, mode: str, hooks: FaultHooks,
                 clock_ms: Callable[[], int], identity: Optional[EngineIdentity] = None,
                 locks: Sequence[_HostLock] = ()):
        self._conn = conn
        self.path = path
        self._mode = mode
        self.fault_hooks = hooks
        self._clock_ms = clock_ms
        self.identity = identity
        self._locks = list(locks)
        self._rlock = threading.RLock()
        self._tx: Optional[Transaction] = None
        self._owner_token: Optional[str] = None
        self.degraded_reason: Optional[str] = None
        self.closed = False
        self.crashed = False
        self._broken: Optional[str] = None
        self._pending_marker_repair: Optional[List[str]] = None

    # ---------------------------------------------------------------------------------------------- opening

    @classmethod
    def open(cls, path: Optional[Union[str, Path]], engine_identity: EngineIdentity,
             create_if_missing: bool = False,
             prior_run_markers: Optional[Callable[[EngineIdentity, Path], Any]] = None, *,
             lock_dir: Optional[Union[str, Path]] = None,
             config_fingerprint: Optional[str] = None,
             fault_hooks: Optional[FaultHooks] = None,
             clock_ms: Optional[Callable[[], int]] = None,
             manual_recovery: Optional[ManualRecovery] = None,
             takeover_foreign_host: bool = False,
             integrity_check: str = "quick",
             busy_timeout_ms: int = 5000,
             migrations: Sequence[Migration] = MIGRATIONS) -> "NeutralGridStore":
        """Open (or create) the engine's database as its single writer.

        Fails closed with :class:`PriorRunEvidenceError` when the database is missing/empty but a marker,
        leftover WAL/SHM file or ``prior_run_markers(identity, path)`` says a previous engine ran (AC-54), and
        with :class:`StoreCorruptError` for a file that is not an intact neutral grid database. A fresh
        database is only created with ``create_if_missing=True`` and no such evidence, or with an explicit
        :class:`ManualRecovery` (new CID epoch, manual reconciliation required, audited).
        """
        if not isinstance(engine_identity, EngineIdentity):
            raise TypeError("engine_identity must be EngineIdentity")
        if sqlite3.sqlite_version_info < MIN_SQLITE_VERSION:
            raise PersistenceError(f"SQLite {sqlite3.sqlite_version} is too old; >= 3.37 (STRICT) required")
        db_path = Path(path) if path is not None else default_db_path(engine_identity)
        db_path = db_path.absolute()
        locks_dir = Path(lock_dir) if lock_dir is not None else default_lock_dir()
        hooks = fault_hooks or FaultHooks()
        clock = clock_ms or _now_ms
        locks = [_HostLock(locks_dir / f"{engine_identity.lock_key}.lock"),
                 _HostLock(db_path.with_name(db_path.name + ".lock"))]
        acquired: List[_HostLock] = []
        conn: Optional[_Connection] = None
        try:
            for lock in locks:
                lock.acquire()
                acquired.append(lock)
            markers = cls._marker_paths(db_path, engine_identity, locks_dir)
            evidence = cls._prior_run_evidence(db_path, engine_identity, markers, prior_run_markers)
            exists = db_path.exists() and db_path.stat().st_size > 0
            fresh_leftover = False
            if exists:
                cls._check_header(db_path, evidence)
                conn = cls._connect_writer(db_path, hooks, busy_timeout_ms)
                if cls._is_uninitialised(conn):
                    if evidence:
                        raise StoreCorruptError(
                            f"{db_path} has no neutral grid schema but a previous run left evidence", evidence)
                    fresh_leftover = True
                elif manual_recovery is not None:
                    raise ValueError("manual_recovery only applies when the database is missing")
            if not exists or fresh_leftover:
                if evidence and manual_recovery is None:
                    raise PriorRunEvidenceError(
                        f"database {db_path} is missing but a previous engine run left evidence; restore the "
                        f"backup or perform an explicit ManualRecovery (never a fresh bootstrap)", evidence)
                if not evidence and manual_recovery is not None:
                    raise ValueError("manual_recovery requires evidence of a lost database")
                if not evidence and not create_if_missing:
                    raise StoreMissingError(f"database {db_path} does not exist")
                db_path.parent.mkdir(parents=True, exist_ok=True)
                if conn is None:
                    conn = cls._connect_writer(db_path, hooks, busy_timeout_ms)
                store = cls(conn=conn, path=db_path, mode=_WRITER, hooks=hooks, clock_ms=clock,
                            identity=engine_identity, locks=acquired)
                store._initialise(migrations, markers, evidence, manual_recovery)
            else:
                store = cls(conn=conn, path=db_path, mode=_WRITER, hooks=hooks, clock_ms=clock,
                            identity=engine_identity, locks=acquired)
                store._open_existing(migrations, markers, evidence, integrity_check, config_fingerprint)
            store._acquire_owner(takeover_foreign_host)
            return store
        except BaseException:
            if conn is not None:
                with contextlib.suppress(Exception):
                    conn.close()
            for lock in acquired:
                with contextlib.suppress(Exception):
                    lock.release()
            raise

    @classmethod
    def open_readonly(cls, path: Union[str, Path], *, integrity_check: str = "none",
                      migrations: Sequence[Migration] = MIGRATIONS,
                      clock_ms: Optional[Callable[[], int]] = None) -> "NeutralGridStore":
        """Read-only handle for UI/CLI processes (``mode=ro`` + ``query_only``); takes no lock."""
        db_path = Path(path).absolute()
        if not db_path.exists() or db_path.stat().st_size == 0:
            raise StoreMissingError(f"database {db_path} does not exist")
        cls._check_header(db_path, [])
        hooks = FaultHooks()
        try:
            raw = sqlite3.connect(db_path.as_uri() + "?mode=ro", uri=True, isolation_level=None,
                                  check_same_thread=False, timeout=5.0)
            raw.row_factory = sqlite3.Row
            raw.execute("PRAGMA query_only = 1")
        except sqlite3.Error as exc:
            raise PersistenceError(f"cannot open {db_path} read-only: {exc}") from exc
        conn = _Connection(raw, hooks)
        store = cls(conn=conn, path=db_path, mode=_READONLY, hooks=hooks, clock_ms=clock_ms or _now_ms)
        try:
            store._verify_existing_schema(migrations, allow_upgrade=False, integrity_check=integrity_check)
        except BaseException:
            store._close_quietly()
            raise
        return store

    @classmethod
    def open_command_client(cls, path: Union[str, Path], *, busy_timeout_ms: int = 5000,
                            migrations: Sequence[Migration] = MIGRATIONS,
                            fault_hooks: Optional[FaultHooks] = None,
                            clock_ms: Optional[Callable[[], int]] = None) -> "NeutralGridStore":
        """Handle for the local web backend: reads snapshots/commands and may only INSERT into ``commands``
        (enforced by an SQLite authorizer). Takes no engine lock; it is not a ledger writer."""
        db_path = Path(path).absolute()
        if not db_path.exists() or db_path.stat().st_size == 0:
            raise StoreMissingError(f"database {db_path} does not exist")
        cls._check_header(db_path, [])
        hooks = fault_hooks or FaultHooks()
        try:
            raw = sqlite3.connect(str(db_path), isolation_level=None, check_same_thread=False,
                                  timeout=busy_timeout_ms / 1000)
            raw.row_factory = sqlite3.Row
            raw.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
            raw.execute("PRAGMA synchronous = FULL")
            raw.execute("PRAGMA fullfsync = ON")
            raw.execute("PRAGMA foreign_keys = ON")
        except sqlite3.Error as exc:
            raise PersistenceError(f"cannot open {db_path}: {exc}") from exc
        conn = _Connection(raw, hooks)
        store = cls(conn=conn, path=db_path, mode=_COMMANDS, hooks=hooks, clock_ms=clock_ms or _now_ms)
        try:
            store._verify_existing_schema(migrations, allow_upgrade=False, integrity_check="none")
            if raw.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal":
                raise PersistenceError("database is not in WAL mode")
            raw.set_authorizer(_command_client_authorizer)
        except BaseException:
            store._close_quietly()
            raise
        return store

    @staticmethod
    def _marker_paths(db_path: Path, identity: EngineIdentity, lock_dir: Path) -> List[Path]:
        # the host marker lives next to the lock directory, never inside the database's data directory
        return [db_path.with_name(db_path.name + ".marker.json"),
                lock_dir.parent / "markers" / f"{identity.lock_key}.marker.json"]

    @staticmethod
    def _prior_run_evidence(db_path: Path, identity: EngineIdentity, markers: Sequence[Path],
                            callback: Optional[Callable[[EngineIdentity, Path], Any]]) -> List[str]:
        evidence = [f"prior-run marker {marker}" for marker in markers if marker.exists()]
        for suffix in ("-wal", "-shm", "-journal"):
            leftover = Path(str(db_path) + suffix)
            if leftover.exists() and leftover.stat().st_size > 0:
                evidence.append(f"leftover {leftover}")
        if callback is not None:
            try:
                result = callback(identity, db_path)
            except Exception as exc:
                raise PriorRunEvidenceError(f"prior-run evidence callback failed: {exc}",
                                            evidence + ["callback failed"]) from exc
            if result is True:
                evidence.append("caller-provided prior-run evidence")
            elif result not in (None, False):
                evidence.extend(str(item) for item in result)
        return evidence

    @staticmethod
    def _check_header(db_path: Path, evidence: Sequence[str]) -> None:
        try:
            with db_path.open("rb") as handle:
                header = handle.read(100)
        except OSError as exc:
            raise PersistenceError(f"cannot read {db_path}: {exc}") from exc
        if len(header) < 100 or not header.startswith(SQLITE_HEADER):
            raise StoreCorruptError(f"{db_path} is not a SQLite database (bad header)",
                                    list(evidence) + [f"corrupt file {db_path}"])

    @staticmethod
    def _connect_writer(db_path: Path, hooks: FaultHooks, busy_timeout_ms: int) -> _Connection:
        try:
            raw = sqlite3.connect(str(db_path), isolation_level=None, check_same_thread=False,
                                  timeout=busy_timeout_ms / 1000)
            raw.row_factory = sqlite3.Row
            raw.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
            mode = raw.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                raise PersistenceError(f"cannot enable WAL on {db_path} (got {mode}); read-only storage?")
            raw.execute("PRAGMA synchronous = FULL")
            raw.execute("PRAGMA fullfsync = ON")
            raw.execute("PRAGMA checkpoint_fullfsync = ON")
            raw.execute("PRAGMA foreign_keys = ON")
            raw.execute("PRAGMA cell_size_check = ON")
            if raw.execute("PRAGMA synchronous").fetchone()[0] != 2:
                raise PersistenceError("synchronous=FULL was not accepted")
        except sqlite3.DatabaseError as exc:
            if "not a database" in str(exc) or "malformed" in str(exc):
                raise StoreCorruptError(f"{db_path} is corrupt: {exc}", [f"corrupt file {db_path}"]) from exc
            raise PersistenceError(f"cannot open {db_path}: {exc}") from exc
        return _Connection(raw, hooks)

    @staticmethod
    def _is_uninitialised(conn: _Connection) -> bool:
        try:
            app_id = conn.execute("PRAGMA application_id").fetchone()[0]
            tables = conn.execute("SELECT count(*) FROM sqlite_master").fetchone()[0]
        except sqlite3.DatabaseError as exc:
            raise StoreCorruptError(f"database is corrupt: {exc}", ["corrupt database"]) from exc
        return app_id == 0 and tables == 0

    def _initialise(self, migrations: Sequence[Migration], markers: Sequence[Path], evidence: Sequence[str],
                    recovery: Optional[ManualRecovery]) -> None:
        identity = self.identity
        cid_epoch = 1
        prior_markers = [m for m in (_read_marker(p) for p in markers if p.exists()) if m]
        if recovery is not None:
            _require_text(recovery.actor, "actor")
            _require_text(recovery.reason, "reason", 2000)
            if not isinstance(recovery.evidence, Mapping) or not recovery.evidence:
                raise ValueError("manual recovery requires non-empty evidence")
            epochs = [int(m["cid_epoch"]) for m in prior_markers if isinstance(m.get("cid_epoch"), int)]
            if epochs:
                cid_epoch = max(epochs) + 1
            elif recovery.min_cid_epoch is None:
                raise ValueError("no readable marker: ManualRecovery.min_cid_epoch is required to avoid CID reuse")
            if recovery.min_cid_epoch is not None:
                cid_epoch = max(cid_epoch, _require_int(recovery.min_cid_epoch, "min_cid_epoch", 2))
            if cid_epoch > CID_MAX_EPOCH:
                raise CidExhaustedError("CID epochs exhausted")
        now = self._clock_ms()
        db_uuid = str(uuid.uuid4())
        with self._raw_tx():
            for migration in migrations:
                for statement in _split_sql(migration.sql):
                    self._x(statement)
                self._x("INSERT INTO schema_migrations(version, name, checksum, applied_at_ms) VALUES (?, ?, ?, ?)",
                        (migration.version, migration.name, migration.checksum, now))
            self._x(f"PRAGMA application_id = {APPLICATION_ID}")
            self._x(f"PRAGMA user_version = {int(migrations[-1].version)}")
            self._x("""INSERT INTO engine(id, db_uuid, engine_id, connector_name, connector_domain, account_index,
                       trading_pair, cid_epoch, created_at_ms, recovered_from_loss, manual_reconcile_required,
                       manual_reconcile_reason, updated_at_ms) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (db_uuid, identity.engine_id, identity.connector_name, identity.connector_domain,
                     identity.account_index, identity.trading_pair, cid_epoch, now, int(recovery is not None),
                     int(recovery is not None),
                     "database recreated after loss: manual reconciliation required" if recovery else None, now))
            self._x("INSERT INTO cid_state(id, cid_epoch, next_seq) VALUES (1, ?, 1)", (cid_epoch,))
            self._x("INSERT INTO snapshot_state(id, last_version) VALUES (1, 0)")
            for stream in STREAMS:
                self._x("INSERT INTO cursors(stream, updated_at_ms) VALUES (?, ?)", (stream, now))
            self._audit("store_created", "store", {"db_uuid": db_uuid, "schema_version": migrations[-1].version,
                                                   "engine_id": identity.engine_id, "cid_epoch": cid_epoch})
            if recovery is not None:
                self._audit("manual_recovery", recovery.actor, {
                    "reason": recovery.reason, "evidence": dict(recovery.evidence),
                    "prior_run_evidence": list(evidence), "prior_markers": prior_markers,
                    "new_cid_epoch": cid_epoch, "baseline": "not adopted; explicit bootstrap confirmation required"})
        try:
            _fsync_dir(self.path.parent)
        except OSError as exc:
            raise PersistenceError(f"cannot fsync {self.path.parent}: {exc}") from exc
        self._write_markers(markers, db_uuid, cid_epoch)

    def _write_markers(self, markers: Sequence[Path], db_uuid: str, cid_epoch: int) -> None:
        content = {"format": 1, "db_uuid": db_uuid, "engine_id": self.identity.engine_id,
                   "db_path": str(self.path), "cid_epoch": cid_epoch, "written_at_ms": self._clock_ms()}
        for marker in markers:
            _write_marker(marker, content)

    def _open_existing(self, migrations: Sequence[Migration], markers: Sequence[Path], evidence: Sequence[str],
                       integrity_check: str, config_fingerprint: Optional[str]) -> None:
        # every refusal happens before any write (migrations included): identity columns and the grid
        # fingerprint exist since schema v1 and are immutable, so they are read with narrow queries
        pending = self._verify_existing_schema(migrations, allow_upgrade=True, integrity_check=integrity_check,
                                               evidence=evidence)
        row = self._x("""SELECT e.db_uuid, e.engine_id, e.connector_name, e.connector_domain, e.account_index,
                         e.trading_pair, e.cid_epoch, g.fingerprint FROM engine e
                         LEFT JOIN grids g ON g.grid_id = e.current_grid_id WHERE e.id = 1""").fetchone()
        if row is None:
            raise StoreCorruptError(f"{self.path} has no engine row", list(evidence) + ["engine row missing"])
        identity = self.identity
        stored = (row["engine_id"], row["connector_name"], row["connector_domain"], row["account_index"],
                  row["trading_pair"])
        wanted = (identity.engine_id, identity.connector_name, identity.connector_domain, identity.account_index,
                  identity.trading_pair)
        if stored != wanted:
            raise IdentityMismatchError(f"database belongs to {stored}, opener is {wanted}")
        missing = []
        for marker in markers:
            if not marker.exists():
                missing.append(marker)
                continue
            content = _read_marker(marker)
            if content is None or content.get("db_uuid") != row["db_uuid"]:
                raise PriorRunEvidenceError(
                    f"prior-run marker {marker} does not match database {row['db_uuid']}: the database file was "
                    f"replaced or another database exists for this account/market", list(evidence))
        if config_fingerprint is not None and row["fingerprint"] is not None \
                and row["fingerprint"] != config_fingerprint:
            raise ConfigMutationError(
                f"grid dimensions/Q changed (stored fingerprint {row['fingerprint']}, config "
                f"{config_fingerprint}); a running grid is immutable -- use migrate_grid() once it is quiescent")
        for migration in pending:
            self._apply_migration(migration)
        if missing:
            self._write_markers(missing, row["db_uuid"], row["cid_epoch"])
            self._pending_marker_repair = [str(m) for m in missing]

    def _verify_existing_schema(self, migrations: Sequence[Migration], *, allow_upgrade: bool, integrity_check: str,
                                evidence: Sequence[str] = ()) -> List[Migration]:
        """Fail closed on foreign/corrupt/unknown/newer/edited schemas; returns the pending (older) migrations."""
        conn = self._conn
        try:
            app_id = conn.execute("PRAGMA application_id").fetchone()[0]
            if app_id != APPLICATION_ID:
                raise StoreCorruptError(f"{self.path} is not a neutral grid store (application_id={app_id})",
                                        list(evidence) + [f"foreign database {self.path}"])
            if integrity_check in ("quick", "full"):
                pragma = "quick_check" if integrity_check == "quick" else "integrity_check"
                result = [row[0] for row in conn.execute(f"PRAGMA {pragma}").fetchall()]
                if result != ["ok"]:
                    raise StoreCorruptError(f"{self.path} failed {pragma}: {result[:5]}",
                                            list(evidence) + [f"corrupt database {self.path}"])
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if "schema_migrations" not in tables:
                raise SchemaVersionError("schema_migrations table missing")
            rows = conn.execute("SELECT version, name, checksum FROM schema_migrations ORDER BY version").fetchall()
            user_version = conn.execute("PRAGMA user_version").fetchone()[0]
        except sqlite3.DatabaseError as exc:
            raise StoreCorruptError(f"{self.path} is corrupt: {exc}",
                                    list(evidence) + [f"corrupt database {self.path}"]) from exc
        known = {m.version: m for m in migrations}
        latest = migrations[-1].version
        for expected, row in enumerate(rows, start=1):
            version, name, checksum = row[0], row[1], row[2]
            if version != expected:
                raise SchemaVersionError(f"schema history has a gap at version {version}")
            migration = known.get(version)
            if migration is None:
                raise SchemaVersionError(f"database schema version {version} is newer/unknown to this code "
                                         f"(latest known {latest}); refusing to open")
            if migration.checksum != checksum or migration.name != name:
                raise SchemaVersionError(f"schema migration {version} ({name}) differs from this code's migration")
        current = rows[-1][0] if rows else 0
        if current == 0 or user_version != current:
            raise SchemaVersionError(f"inconsistent schema version (user_version={user_version}, "
                                     f"migrations={current})")
        if current < latest and not allow_upgrade:
            raise SchemaVersionError(f"database schema {current} is older than {latest}; the engine must "
                                     f"migrate it first")
        return list(migrations[current:])

    def _apply_migration(self, migration: Migration) -> None:
        now = self._clock_ms()
        with self._raw_tx():
            for statement in _split_sql(migration.sql):
                self._x(statement)
            self._x("INSERT INTO schema_migrations(version, name, checksum, applied_at_ms) VALUES (?, ?, ?, ?)",
                    (migration.version, migration.name, migration.checksum, now))
            self._x(f"PRAGMA user_version = {int(migration.version)}")
            self._audit("migration", "store", {"version": migration.version, "name": migration.name,
                                               "checksum": migration.checksum})

    def _acquire_owner(self, takeover_foreign_host: bool) -> None:
        host = socket.gethostname()
        token = str(uuid.uuid4())
        lock_key = self.identity.lock_key
        with self._raw_tx():
            row = self._x("SELECT * FROM engine_owner WHERE id = 1").fetchone()
            previous = dict(row) if row is not None else None
            if previous and previous["released_at_ms"] is None and previous["hostname"] != host:
                if not takeover_foreign_host:
                    raise StoreLockedError(
                        f"database owner row names host {previous['hostname']!r} (pid {previous['pid']}); another "
                        f"host may run this engine. The host lock is NOT distributed; take over only after "
                        f"verifying that engine is stopped (takeover_foreign_host=True, audited)")
            self._x("""INSERT INTO engine_owner(id, owner_token, hostname, pid, lock_key, acquired_at_ms,
                       released_at_ms) VALUES (1, ?, ?, ?, ?, ?, NULL)
                       ON CONFLICT(id) DO UPDATE SET owner_token=excluded.owner_token, hostname=excluded.hostname,
                       pid=excluded.pid, lock_key=excluded.lock_key, acquired_at_ms=excluded.acquired_at_ms,
                       released_at_ms=NULL""", (token, host, os.getpid(), lock_key, self._clock_ms()))
            stale = previous is not None and previous["released_at_ms"] is None
            self._audit("owner_acquired", "store", {
                "pid": os.getpid(), "hostname": host, "previous_owner": previous,
                "previous_owner_unreleased": stale, "foreign_takeover": bool(stale and previous["hostname"] != host)})
            if self._pending_marker_repair:
                self._audit("marker_repaired", "store", {"markers": self._pending_marker_repair})
        self._owner_token = token

    # ---------------------------------------------------------------------------------------------- lifecycle

    def close(self) -> None:
        with self._rlock:
            if self.closed:
                return
            if self._tx is not None:
                raise StoreError("close() inside an open transaction")
            if self._mode == _WRITER and self._owner_token is not None and self._broken is None:
                try:
                    with self._raw_tx():
                        self._x("UPDATE engine_owner SET released_at_ms = ? WHERE id = 1 AND owner_token = ?",
                                (self._clock_ms(), self._owner_token))
                except (StoreError, sqlite3.Error) as exc:
                    logger.warning("could not release neutral grid owner row: %s", exc)
            self._close_quietly()

    def _close_quietly(self) -> None:
        self.closed = True
        with contextlib.suppress(Exception):
            self._conn.close()
        for lock in self._locks:
            with contextlib.suppress(Exception):
                lock.release()
        self._locks = []

    def _die(self) -> None:
        """Simulated process death: drop the connection without commit and release OS locks."""
        self.crashed = True
        self._tx = None
        self._close_quietly()

    def __enter__(self) -> "NeutralGridStore":
        return self

    def __exit__(self, *exc_info) -> None:
        if not self.crashed:
            self.close()

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def is_degraded(self) -> bool:
        return self.degraded_reason is not None

    def fault_point(self, point: str) -> None:
        """Hit a named fault point (engine code may use this around transport calls)."""
        try:
            self.fault_hooks.hit(point)
        except SimulatedCrash:
            self._die()
            raise

    # ---------------------------------------------------------------------------------------------- transactions

    def _require_open(self) -> None:
        if self.closed:
            raise StoreClosedError("store is closed" + (" (simulated crash)" if self.crashed else ""))
        if self._broken is not None:
            raise PersistenceError(f"store connection is unusable: {self._broken}; reopen the store")

    def _require_writer(self) -> None:
        if self._mode != _WRITER:
            raise ReadOnlyStoreError(f"{self._mode} store cannot modify the ledger")

    def _persistence_failure(self, where: str, exc: BaseException) -> PersistenceError:
        code = getattr(exc, "sqlite_errorname", type(exc).__name__)
        reason = f"{where}: {code}: {exc}"
        self.degraded_reason = reason
        cls = StoreIntegrityError if isinstance(exc, sqlite3.IntegrityError) else PersistenceError
        return cls(reason)

    def _x(self, sql: str, params: Any = ()) -> sqlite3.Cursor:
        try:
            return self._conn.execute(sql, params)
        except sqlite3.Error as exc:
            error = self._persistence_failure(sql.strip().split("\n")[0][:80], exc)
            if self._tx is not None:
                self._tx._failed = error
            raise error from exc

    def _rollback_quietly(self) -> None:
        if not self._conn.in_transaction:
            return
        try:
            self._conn.raw.execute("ROLLBACK")
        except sqlite3.Error as exc:
            self._broken = f"rollback failed: {exc}"
            self.degraded_reason = self.degraded_reason or self._broken
            with contextlib.suppress(Exception):
                self._conn.close()

    @contextlib.contextmanager
    def _raw_tx(self) -> Iterator[None]:
        """Internal transaction used during open/close (no owner fencing, no fault points)."""
        with self._rlock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
            except sqlite3.Error as exc:
                raise self._persistence_failure("begin", exc) from exc
            try:
                yield
                self._conn.commit()
            except sqlite3.Error as exc:
                self._rollback_quietly()
                raise self._persistence_failure("commit", exc) from exc
            except BaseException:
                self._rollback_quietly()
                raise

    @contextlib.contextmanager
    def transaction(self) -> Iterator[Transaction]:
        """``BEGIN IMMEDIATE`` ... ``COMMIT``. All-or-nothing; SQLite errors become :class:`PersistenceError`."""
        with self._rlock:
            self._require_open()
            if self._mode == _READONLY:
                raise ReadOnlyStoreError("read-only store has no transactions")
            if self._tx is not None:
                raise StoreError("nested transactions are not supported")
            try:
                self._conn.execute("BEGIN IMMEDIATE")
            except sqlite3.Error as exc:
                raise self._persistence_failure("begin", exc) from exc
            tx = Transaction(self)
            self._tx = tx
            try:
                if self._mode == _WRITER:
                    self._verify_owner()
                yield tx
                if tx._failed is not None:
                    raise PersistenceError(f"transaction had a failed statement or a store call that failed after "
                                           f"writing; refusing to commit: {tx._failed!r}") from tx._failed
                self.fault_hooks.hit("before_commit")
                try:
                    self._conn.commit()
                except sqlite3.Error as exc:
                    raise self._persistence_failure("commit", exc) from exc
            except SimulatedCrash:
                self._die()
                raise
            except BaseException:
                self._tx = None
                self._rollback_quietly()
                raise
            finally:
                self._tx = None
            tx.committed = True
            try:
                for point in tx._after_commit:
                    self.fault_hooks.hit(point)
                self.fault_hooks.hit("after_commit")
            except SimulatedCrash:
                self._die()
                raise

    def _verify_owner(self) -> None:
        row = self._x("SELECT owner_token FROM engine_owner WHERE id = 1").fetchone()
        if row is None or row[0] != self._owner_token:
            self._broken = "ownership lost"
            raise OwnershipLostError("engine_owner row no longer names this store; another opener took over")

    def _check_tx(self, tx: Optional[Transaction]) -> Transaction:
        if tx is None or tx is not self._tx or tx._store is not self:
            raise StoreError("method requires the store's currently open transaction")
        return tx

    @contextlib.contextmanager
    def _scope(self, tx: Optional[Transaction]) -> Iterator[Transaction]:
        """Run in the caller's transaction (or an own one). If a store method raises after it has written rows,
        the caller's transaction is poisoned: even if the caller catches the error, COMMIT is refused, so a guard
        error can never leave half-applied state behind (AC-55, NG-DB-002)."""
        if tx is None:
            with self.transaction() as own:
                yield own
            return
        scope = self._check_tx(tx)
        before = self._conn.raw.total_changes
        try:
            yield scope
        except BaseException as exc:
            if scope._failed is None and self._tx is scope and not self.closed \
                    and self._conn.raw.total_changes != before:
                scope._failed = exc
            raise

    def clear_degraded(self, actor: str, reason: str) -> None:
        """Operator/engine acknowledgement after storage recovered; succeeds only if an audit row commits."""
        _require_text(actor, "actor")
        _require_text(reason, "reason", 2000)
        previous = self.degraded_reason
        with self.transaction():
            self._audit("degraded_cleared", actor, {"previous_reason": previous, "reason": reason})
        self.degraded_reason = None

    # ---------------------------------------------------------------------------------------------- helpers

    def _audit(self, kind: str, actor: str, payload: Mapping[str, Any]) -> int:
        revision = self._x("SELECT engine_revision FROM engine WHERE id = 1").fetchone()
        cursor = self._x("INSERT INTO audit_events(at_ms, kind, actor, engine_revision, payload_json) "
                         "VALUES (?, ?, ?, ?, ?)",
                         (self._clock_ms(), kind, actor, revision[0] if revision else None,
                          canonical_json(payload)))
        return cursor.lastrowid

    def _transition(self, entity: str, key: str, from_state: Optional[str], to_state: str, reason: str) -> None:
        self._x("INSERT INTO state_transitions(at_ms, entity, entity_key, from_state, to_state, reason) "
                "VALUES (?, ?, ?, ?, ?, ?)", (self._clock_ms(), entity, key, from_state, to_state, reason or None))

    def record_audit(self, tx: Optional[Transaction], kind: str, actor: str, payload: Mapping[str, Any]) -> int:
        _require_text(kind, "kind", 64)
        _require_text(actor, "actor")
        with self._scope(tx):
            self._require_writer()
            return self._audit(kind, actor, payload)

    # ---------------------------------------------------------------------------------------------- engine / bootstrap

    def engine(self) -> EngineRecord:
        with self._rlock:
            self._require_open()
            row = self._x("SELECT * FROM engine WHERE id = 1").fetchone()
            if row is None:
                raise StoreIntegrityError("engine row missing")
            adjustments = self._x("SELECT new_baseline FROM baseline_adjustments ORDER BY id DESC LIMIT 1").fetchone()
        initial = parse_decimal(row["initial_baseline"])
        effective = parse_decimal(adjustments[0]) if adjustments is not None else initial
        return EngineRecord(
            db_uuid=row["db_uuid"], engine_id=row["engine_id"], connector_name=row["connector_name"],
            connector_domain=row["connector_domain"], account_index=row["account_index"],
            trading_pair=row["trading_pair"], market_id=row["market_id"], cid_epoch=row["cid_epoch"],
            created_at_ms=row["created_at_ms"], recovered_from_loss=bool(row["recovered_from_loss"]),
            bootstrapped_at_ms=row["bootstrapped_at_ms"], initial_grid_id=row["initial_grid_id"],
            initial_baseline=initial, effective_baseline=effective,
            bootstrap_trades_cut=row["bootstrap_trades_cut"], bootstrap_orders_cut=row["bootstrap_orders_cut"],
            bootstrap_cut_ts_ms=row["bootstrap_cut_ts_ms"], bootstrap_actor=row["bootstrap_actor"],
            current_grid_id=row["current_grid_id"], config_revision=row["config_revision"],
            engine_revision=row["engine_revision"], reconciliation_revision=row["reconciliation_revision"],
            engine_state=EngineState(row["engine_state"]), state_reason=row["state_reason"],
            pause_reason=row["pause_reason"], stop_reason=row["stop_reason"],
            manual_reconcile_required=bool(row["manual_reconcile_required"]),
            manual_reconcile_reason=row["manual_reconcile_reason"], updated_at_ms=row["updated_at_ms"])

    def _validate_grid(self, grid_id: str, lower: Decimal, upper: Decimal, amount: Decimal,
                       prices: Sequence[Decimal], cells: Sequence[CellSpec], anchor: Decimal) -> List[str]:
        _require_text(grid_id, "grid_id", 128)
        lower_d = Decimal(canonical_decimal(lower, "lower_price"))
        upper_d = Decimal(canonical_decimal(upper, "upper_price"))
        _require_positive(amount, "order_amount_base")
        price_list = [Decimal(canonical_decimal(p, "price")) for p in prices]
        if len(price_list) < 2 or len(price_list) != len(cells) + 1:
            raise ValueError("prices must have exactly len(cells) + 1 entries")
        if price_list[0] != lower_d or price_list[-1] != upper_d:
            raise ValueError("first/last price must equal lower/upper bound")
        if any(b <= a for a, b in zip(price_list, price_list[1:])) or price_list[0] <= 0:
            raise ValueError("prices must be positive and strictly increasing")
        for index, cell in enumerate(cells):
            if not isinstance(cell, CellSpec) or cell.cell_id != index:
                raise ValueError("cells must be CellSpec with cell_id 0..N-1 in order")
            if cell.low_price != price_list[index] or cell.high_price != price_list[index + 1]:
                raise ValueError(f"cell {index} prices do not match the grid prices")
        anchor_d = Decimal(canonical_decimal(anchor, "anchor"))
        if not lower_d <= anchor_d <= upper_d:
            raise ValueError("anchor must lie within [lower_price, upper_price]")
        for cell in cells:  # NG-GRID-002: P[i] < anchor -> BUY entry, otherwise SELL entry
            expected = Side.BUY if cell.low_price < anchor_d else Side.SELL
            if _enum(Side, cell.entry_side, "entry_side") != expected:
                raise ValueError(f"cell {cell.cell_id} entry side must be {expected.value} for anchor {anchor_d}")
        return [canonical_decimal(p) for p in price_list]

    def _insert_grid(self, grid_id: str, fingerprint: str, config_revision: int, lower: Decimal, upper: Decimal,
                     amount: Decimal, prices_text: List[str], cells: Sequence[CellSpec], anchor: Decimal,
                     migrated_from: Optional[str]) -> None:
        now = self._clock_ms()
        self._x("""INSERT INTO grids(grid_id, fingerprint, config_revision, lower_price, upper_price, cell_count,
                   order_amount_base, prices_json, anchor, status, migrated_from, created_at_ms)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)""",
                (grid_id, fingerprint, config_revision, canonical_decimal(lower), canonical_decimal(upper), len(cells),
                 canonical_decimal(amount), canonical_json(prices_text), canonical_decimal(anchor), migrated_from, now))
        for cell in cells:
            self._x("""INSERT INTO cells(grid_id, cell_id, low_price, high_price, entry_side, updated_at_ms)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (grid_id, cell.cell_id, canonical_decimal(cell.low_price), canonical_decimal(cell.high_price),
                     Side(cell.entry_side).value, now))

    def bootstrap(self, tx: Optional[Transaction], record: BootstrapRecord) -> EngineRecord:
        """Store engine identity data, fixed grid, anchor, confirmed baseline and bootstrap cut exactly once.

        A second call raises :class:`BootstrapError`: the baseline is never recaptured (NG-RISK-001, AC-45).
        """
        _require_text(record.config_fingerprint, "config_fingerprint")
        _require_text(record.actor, "actor")
        _require_text(record.confirmation, "confirmation", 2000)
        baseline = canonical_decimal(record.baseline, "baseline")
        market_id = _require_int(record.market_id, "market_id", 0)
        cut_ts = _require_int(record.bootstrap_cut_ts_ms, "bootstrap_cut_ts_ms", 0)
        prices_text = self._validate_grid(record.grid_id, record.lower_price, record.upper_price,
                                          record.order_amount_base, record.prices, record.cells, record.anchor)
        config_json = canonical_json(record.config)
        with self._scope(tx):
            self._require_writer()
            engine = self.engine()
            if engine.bootstrapped:
                raise BootstrapError("engine already bootstrapped; the baseline and cut are immutable and are never "
                                     "recaptured on restart")
            now = self._clock_ms()
            self._x("INSERT INTO config_revisions(revision, grid_id, fingerprint, config_json, reason, actor, "
                    "created_at_ms) VALUES (1, ?, ?, ?, 'bootstrap', ?, ?)",
                    (record.grid_id, record.config_fingerprint, config_json, record.actor, now))
            self._insert_grid(record.grid_id, record.config_fingerprint, 1, record.lower_price, record.upper_price,
                              record.order_amount_base, prices_text, record.cells, record.anchor, None)
            self._x("""UPDATE engine SET market_id = ?, bootstrapped_at_ms = ?, initial_grid_id = ?,
                       initial_baseline = ?, bootstrap_trades_cut = ?, bootstrap_orders_cut = ?,
                       bootstrap_cut_ts_ms = ?, bootstrap_actor = ?, bootstrap_confirmation = ?,
                       current_grid_id = ?, config_revision = 1, engine_revision = engine_revision + 1,
                       updated_at_ms = ? WHERE id = 1""",
                    (market_id, now, record.grid_id, baseline, record.trades_cut, record.orders_cut, cut_ts,
                     record.actor, record.confirmation, record.grid_id, now))
            for stream, cut in ((STREAM_TRADES, record.trades_cut), (STREAM_INACTIVE_ORDERS, record.orders_cut)):
                self._x("UPDATE cursors SET high_water = ?, high_water_ts_ms = ?, revision = revision + 1, "
                        "updated_at_ms = ? WHERE stream = ?", (cut, cut_ts, now, stream))
            self._audit("bootstrap", record.actor, {
                "grid_id": record.grid_id, "fingerprint": record.config_fingerprint, "anchor": record.anchor,
                "cell_count": len(record.cells), "market_id": market_id, "bootstrap_cut_ts_ms": cut_ts,
                "trades_cut": record.trades_cut, "orders_cut": record.orders_cut})
            self._audit("baseline_confirmed", record.actor, {"baseline": baseline,
                                                             "confirmation": record.confirmation})
            self._transition("ENGINE", "engine", None, "BOOTSTRAPPED", "bootstrap")
        return self.engine()

    def grid(self, grid_id: Optional[str] = None) -> GridRecord:
        with self._rlock:
            self._require_open()
            if grid_id is None:
                grid_id = self._x("SELECT current_grid_id FROM engine WHERE id = 1").fetchone()[0]
                if grid_id is None:
                    raise BootstrapError("engine is not bootstrapped")
            row = self._x("SELECT * FROM grids WHERE grid_id = ?", (grid_id,)).fetchone()
        if row is None:
            raise KeyError(f"unknown grid {grid_id}")
        return GridRecord(
            grid_id=row["grid_id"], fingerprint=row["fingerprint"], config_revision=row["config_revision"],
            lower_price=parse_decimal(row["lower_price"]), upper_price=parse_decimal(row["upper_price"]),
            cell_count=row["cell_count"], order_amount_base=parse_decimal(row["order_amount_base"]),
            prices=tuple(parse_decimal(p) for p in json.loads(row["prices_json"])),
            anchor=parse_decimal(row["anchor"]), status=row["status"], migrated_from=row["migrated_from"],
            created_at_ms=row["created_at_ms"])

    def record_config_revision(self, tx: Optional[Transaction], config: Any, config_fingerprint: str, actor: str,
                               reason: str) -> int:
        """New config revision for non-dimension settings. Dimensions/Q (fingerprint) may not change here."""
        _require_text(actor, "actor")
        _require_text(reason, "reason", 2000)
        config_json = canonical_json(config)
        with self._scope(tx):
            self._require_writer()
            grid = self.grid()
            if config_fingerprint != grid.fingerprint:
                raise ConfigMutationError("grid dimensions/Q are immutable for a running grid; use migrate_grid()")
            revision = self.engine().config_revision + 1
            self._x("INSERT INTO config_revisions(revision, grid_id, fingerprint, config_json, reason, actor, "
                    "created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (revision, grid.grid_id, config_fingerprint, config_json, reason, actor, self._clock_ms()))
            self._x("UPDATE engine SET config_revision = ?, updated_at_ms = ? WHERE id = 1",
                    (revision, self._clock_ms()))
            self._audit("config_revision", actor, {"revision": revision, "reason": reason})
            return revision

    def grid_mutation_blockers(self, grid_id: Optional[str] = None) -> List[str]:
        """Why the grid's dimensions cannot be replaced now (open orders, reservations, obligations, dust...)."""
        grid = self.grid(grid_id)
        blockers: List[str] = []
        with self._rlock:
            for row in self._x("SELECT cid, state FROM legs WHERE grid_id = ?", (grid.grid_id,)).fetchall():
                if OrderState(row["state"]) not in FINAL_ORDER_STATES:
                    blockers.append(f"leg {row['cid']} is {row['state']}")
            for row in self._x("SELECT r.cid FROM reservations r JOIN legs l ON l.cid = r.cid "
                               "WHERE l.grid_id = ? AND r.state = 'ACTIVE'", (grid.grid_id,)).fetchall():
                blockers.append(f"reservation {row['cid']} active")
            for row in self._x("SELECT o.id, o.cid FROM outbox o JOIN legs l ON l.cid = o.cid "
                               "WHERE l.grid_id = ? AND o.status != 'DONE'", (grid.grid_id,)).fetchall():
                blockers.append(f"outbox {row['id']} (cid {row['cid']}) unresolved")
            for cycle in self._cycles("grid_id = ?", (grid.grid_id,)):  # OPEN and released (late evidence)
                if cycle.entry_filled != cycle.exit_filled:
                    blockers.append(f"cell {cycle.cell_id} gen {cycle.generation} obligation "
                                    f"E={cycle.entry_filled} X={cycle.exit_filled}")
                if cycle.dust != 0:
                    blockers.append(f"cell {cycle.cell_id} gen {cycle.generation} dust {cycle.dust}")
                if cycle.late_evidence == 1:
                    blockers.append(f"cell {cycle.cell_id} gen {cycle.generation} late evidence not acknowledged")
            unallocated = self._x("SELECT count(*) FROM fills f JOIN legs l ON l.cid = f.cid WHERE l.grid_id = ? "
                                  "AND EXISTS (SELECT 1 FROM allocations a WHERE a.cid = f.cid) AND NOT EXISTS "
                                  "(SELECT 1 FROM fill_allocations fa WHERE fa.dedupe_key = f.dedupe_key)",
                                  (grid.grid_id,)).fetchone()[0]
            conflicts = self._x("SELECT count(*) FROM history_conflicts WHERE resolved_at_ms IS NULL").fetchone()[0]
        if unallocated:
            blockers.append(f"{unallocated} aggregated fill(s) not yet allocated")
        if conflicts:
            blockers.append(f"{conflicts} unresolved history conflict(s)")
        return blockers

    def migrate_grid(self, tx: Optional[Transaction], migration: GridMigration) -> GridRecord:
        """Audited replacement of grid dimensions/Q (AC-52). Old grid, cells and cycles are preserved
        (RETIRED); refused while the old grid has open orders, reservations, obligations or dust.
        There is deliberately no reset/delete API."""
        _require_text(migration.actor, "actor")
        _require_text(migration.reason, "reason", 2000)
        _require_text(migration.config_fingerprint, "config_fingerprint")
        prices_text = self._validate_grid(migration.new_grid_id, migration.lower_price, migration.upper_price,
                                          migration.order_amount_base, migration.prices, migration.cells,
                                          migration.anchor)
        config_json = canonical_json(migration.config)
        with self._scope(tx):
            self._require_writer()
            old = self.grid()
            blockers = self.grid_mutation_blockers(old.grid_id)
            if blockers:
                raise ConfigMutationError("grid has open orders/obligations; dimensions/Q cannot change", blockers)
            if self._x("SELECT 1 FROM grids WHERE grid_id = ?", (migration.new_grid_id,)).fetchone():
                raise ConfigMutationError(f"grid id {migration.new_grid_id} already exists")
            revision = self.engine().config_revision + 1
            now = self._clock_ms()
            self._x("INSERT INTO config_revisions(revision, grid_id, fingerprint, config_json, reason, actor, "
                    "created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (revision, migration.new_grid_id, migration.config_fingerprint, config_json,
                     f"grid migration: {migration.reason}", migration.actor, now))
            self._x("UPDATE grids SET status = 'RETIRED', retired_at_ms = ? WHERE grid_id = ?", (now, old.grid_id))
            self._insert_grid(migration.new_grid_id, migration.config_fingerprint, revision, migration.lower_price,
                              migration.upper_price, migration.order_amount_base, prices_text, migration.cells,
                              migration.anchor, old.grid_id)
            self._x("UPDATE engine SET current_grid_id = ?, config_revision = ?, engine_revision = engine_revision + 1,"
                    " updated_at_ms = ? WHERE id = 1", (migration.new_grid_id, revision, now))
            preserved = self._x("SELECT count(*) FROM cycles WHERE grid_id = ?", (old.grid_id,)).fetchone()[0]
            self._audit("grid_migration", migration.actor, {
                "reason": migration.reason, "old_grid_id": old.grid_id, "old_fingerprint": old.fingerprint,
                "new_grid_id": migration.new_grid_id, "new_fingerprint": migration.config_fingerprint,
                "config_revision": revision, "preserved_cycles": preserved})
        return self.grid(migration.new_grid_id)

    def set_engine_state(self, tx: Optional[Transaction], state: EngineState, reason: Optional[str] = None, *,
                         pause_reason: Any = _KEEP, stop_reason: Any = _KEEP) -> EngineRecord:
        """Persist engine state. ``engine_revision`` is bumped only when the state itself changes, so that
        commands carrying the revision of a displayed snapshot do not conflict on every tick."""
        state = _enum(EngineState, state, "engine state")
        with self._scope(tx):
            self._require_writer()
            current = self.engine()
            sets = ["engine_state = ?", "state_reason = ?", "updated_at_ms = ?"]
            params: List[Any] = [state.value, reason, self._clock_ms()]
            if pause_reason is not _KEEP:
                sets.append("pause_reason = ?")
                params.append(pause_reason)
            if stop_reason is not _KEEP:
                sets.append("stop_reason = ?")
                params.append(stop_reason)
            if state != current.engine_state:
                sets.append("engine_revision = engine_revision + 1")
                self._transition("ENGINE", "engine", current.engine_state.value, state.value, reason or "")
            self._x(f"UPDATE engine SET {', '.join(sets)} WHERE id = 1", params)
        return self.engine()

    def bump_engine_revision(self, tx: Optional[Transaction], reason: str = "") -> int:
        with self._scope(tx):
            self._require_writer()
            self._x("UPDATE engine SET engine_revision = engine_revision + 1, updated_at_ms = ? WHERE id = 1",
                    (self._clock_ms(),))
            return self.engine().engine_revision

    def bump_reconciliation_revision(self, tx: Optional[Transaction]) -> int:
        with self._scope(tx):
            self._require_writer()
            self._x("UPDATE engine SET reconciliation_revision = reconciliation_revision + 1, updated_at_ms = ? "
                    "WHERE id = 1", (self._clock_ms(),))
            return self.engine().reconciliation_revision

    def kv_set(self, tx: Optional[Transaction], key: str, value: Any) -> None:
        """Engine-owned auxiliary durable state (JSON, no floats)."""
        _require_text(key, "key", 200)
        text = canonical_json(value)
        with self._scope(tx):
            self._require_writer()
            self._x("INSERT INTO engine_kv(key, value_json, updated_at_ms) VALUES (?, ?, ?) ON CONFLICT(key) "
                    "DO UPDATE SET value_json = excluded.value_json, updated_at_ms = excluded.updated_at_ms",
                    (key, text, self._clock_ms()))

    def kv_get(self, key: str, default: Any = None) -> Any:
        with self._rlock:
            self._require_open()
            row = self._x("SELECT value_json FROM engine_kv WHERE key = ?", (key,)).fetchone()
        return default if row is None else _loads(row[0])

    # ---------------------------------------------------------------------------------------------- cells / cycles

    @staticmethod
    def _cell(row: sqlite3.Row) -> CellRecord:
        return CellRecord(grid_id=row["grid_id"], cell_id=row["cell_id"], low_price=parse_decimal(row["low_price"]),
                          high_price=parse_decimal(row["high_price"]), entry_side=Side(row["entry_side"]),
                          generation=row["generation"], state=row["state"], blocker=row["blocker"],
                          reserved_slots=row["reserved_slots"], queued_at_ms=row["queued_at_ms"])

    def cells(self, grid_id: Optional[str] = None) -> List[CellRecord]:
        grid_id = grid_id or self.grid().grid_id
        with self._rlock:
            self._require_open()
            rows = self._x("SELECT * FROM cells WHERE grid_id = ? ORDER BY cell_id", (grid_id,)).fetchall()
        return [self._cell(row) for row in rows]

    def cell(self, grid_id: str, cell_id: int) -> CellRecord:
        with self._rlock:
            self._require_open()
            row = self._x("SELECT * FROM cells WHERE grid_id = ? AND cell_id = ?", (grid_id, cell_id)).fetchone()
        if row is None:
            raise KeyError(f"unknown cell {grid_id}/{cell_id}")
        return self._cell(row)

    def set_cell_state(self, tx: Optional[Transaction], grid_id: str, cell_id: int, state: CellState, *,
                       blocker: Any = _KEEP, reserved_slots: Optional[int] = None, queued_at_ms: Any = _KEEP,
                       reason: str = "") -> CellRecord:
        """Persist the derived aggregate cell view (the legs remain the source of truth)."""
        state = _enum(CellState, state, "cell state")
        with self._scope(tx):
            self._require_writer()
            current = self.cell(grid_id, cell_id)
            sets = ["state = ?", "updated_at_ms = ?"]
            params: List[Any] = [state.value, self._clock_ms()]
            if blocker is not _KEEP:
                sets.append("blocker = ?")
                params.append(blocker)
            if reserved_slots is not None:
                sets.append("reserved_slots = ?")
                params.append(_require_int(reserved_slots, "reserved_slots", 0))
            if queued_at_ms is not _KEEP:
                sets.append("queued_at_ms = ?")
                params.append(queued_at_ms)
            self._x(f"UPDATE cells SET {', '.join(sets)} WHERE grid_id = ? AND cell_id = ?",
                    params + [grid_id, cell_id])
            if current.state != state.value:
                self._transition("CELL", f"{grid_id}/{cell_id}", current.state, state.value, reason)
            return self.cell(grid_id, cell_id)

    @staticmethod
    def _cycle(row: sqlite3.Row) -> CycleRecord:
        return CycleRecord(
            grid_id=row["grid_id"], cell_id=row["cell_id"], generation=row["generation"],
            entry_side=Side(row["entry_side"]), entry_price=parse_decimal(row["entry_price"]),
            tp_price=parse_decimal(row["tp_price"]), planned_amount=parse_decimal(row["planned_amount"]),
            config_revision=row["config_revision"], entry_filled=parse_decimal(row["entry_filled"]),
            exit_filled=parse_decimal(row["exit_filled"]), dust=parse_decimal(row["dust"]), state=row["state"],
            late_evidence=row["late_evidence"], opened_at_ms=row["opened_at_ms"], closed_at_ms=row["closed_at_ms"],
            close_reason=row["close_reason"])

    def _cycles(self, where: str, params: Sequence[Any]) -> List[CycleRecord]:
        rows = self._x(f"SELECT * FROM cycles WHERE {where} ORDER BY grid_id, cell_id, generation", params).fetchall()
        return [self._cycle(row) for row in rows]

    def cycle(self, grid_id: str, cell_id: int, generation: int) -> CycleRecord:
        with self._rlock:
            self._require_open()
            found = self._cycles("grid_id = ? AND cell_id = ? AND generation = ?", (grid_id, cell_id, generation))
        if not found:
            raise KeyError(f"unknown cycle {grid_id}/{cell_id}/{generation}")
        return found[0]

    def current_cycle(self, grid_id: str, cell_id: int) -> Optional[CycleRecord]:
        cell = self.cell(grid_id, cell_id)
        return None if cell.generation == 0 else self.cycle(grid_id, cell_id, cell.generation)

    def open_cycles(self, grid_id: Optional[str] = None) -> List[CycleRecord]:
        with self._rlock:
            self._require_open()
            if grid_id is None:
                return self._cycles("state = 'OPEN'", ())
            return self._cycles("grid_id = ? AND state = 'OPEN'", (grid_id,))

    def late_obligation_cycles(self, grid_id: Optional[str] = None) -> List[CycleRecord]:
        """Released (COMPLETE) cycles that late history reopened: E != X or late evidence not yet acknowledged.
        Their obligation still needs a TP (allowed once the late evidence is audited, see ``record_intent``)."""
        where = "state = 'COMPLETE' AND (entry_filled != exit_filled OR late_evidence = 1)"
        with self._rlock:
            self._require_open()
            if grid_id is None:
                return self._cycles(where, ())
            return self._cycles(f"grid_id = ? AND {where}", (grid_id,))

    def open_cycle(self, tx: Optional[Transaction], grid_id: str, cell_id: int,
                   planned_amount: Optional[Decimal] = None) -> CycleRecord:
        """Start the next cycle (generation + 1) of a cell with its fixed entry side and prices. Refused while
        the previous cycle is not COMPLETE (whole-cell lock, NG-CELL-001), while any released cycle of the cell
        still has a late obligation (E != X) or unacknowledged late evidence, and while any history conflict is
        unresolved (market freeze, NG-HIST-002)."""
        with self._scope(tx):
            self._require_writer()
            grid = self.grid(grid_id)
            if grid.status != "ACTIVE" or grid.grid_id != self.engine().current_grid_id:
                raise InvalidTransitionError(f"grid {grid_id} is not the active grid")
            cell = self.cell(grid_id, cell_id)
            if cell.generation > 0:
                previous = self.cycle(grid_id, cell_id, cell.generation)
                if previous.state != "COMPLETE":
                    raise InvalidTransitionError(f"cell {cell_id} cycle {cell.generation} is still open")
            late = [c for c in self.late_obligation_cycles(grid_id) if c.cell_id == cell_id]
            if late:
                raise InvalidTransitionError(
                    f"cell {cell_id} has a late obligation on released cycle(s) "
                    f"{[(c.generation, str(c.entry_filled), str(c.exit_filled)) for c in late]}; cover it first")
            conflicts = self._x("SELECT count(*) FROM history_conflicts WHERE resolved_at_ms IS NULL").fetchone()[0]
            if conflicts:
                raise EntryBlockedError(f"{conflicts} unresolved history conflict(s): market frozen, no new cycle",
                                        [f"{conflicts} unresolved history conflict(s)"])
            if planned_amount is None:
                amount = grid.order_amount_base
            else:
                amount = _require_positive(planned_amount, "planned_amount")
            spec = cell.spec()
            generation = cell.generation + 1
            now = self._clock_ms()
            self._x("""INSERT INTO cycles(grid_id, cell_id, generation, entry_side, entry_price, tp_price,
                       planned_amount, config_revision, state, opened_at_ms)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)""",
                    (grid_id, cell_id, generation, spec.entry_side.value, canonical_decimal(spec.entry_price),
                     canonical_decimal(spec.tp_price), canonical_decimal(amount), self.engine().config_revision, now))
            self._x("UPDATE cells SET generation = ?, updated_at_ms = ? WHERE grid_id = ? AND cell_id = ?",
                    (generation, now, grid_id, cell_id))
            self._transition("CYCLE", f"{grid_id}/{cell_id}/{generation}", None, "OPEN", "open_cycle")
            return self.cycle(grid_id, cell_id, generation)

    def update_cycle(self, tx: Optional[Transaction], grid_id: str, cell_id: int, generation: int, *,
                     dust: Optional[Decimal] = None) -> CycleRecord:
        with self._scope(tx):
            self._require_writer()
            self.cycle(grid_id, cell_id, generation)
            if dust is not None:
                text = canonical_decimal(dust, "dust")
                if Decimal(text) < 0:
                    raise ValueError("dust must be >= 0")
                self._x("UPDATE cycles SET dust = ? WHERE grid_id = ? AND cell_id = ? AND generation = ?",
                        (text, grid_id, cell_id, generation))
            return self.cycle(grid_id, cell_id, generation)

    def cycle_release_blockers(self, grid_id: str, cell_id: int, generation: int) -> List[str]:
        """Persistence-derivable part of NG-CELL-001: all legs final, nothing reserved/unresolved, E == X,
        no dust, no unallocated fills, no unacknowledged late evidence."""
        cycle = self.cycle(grid_id, cell_id, generation)
        blockers: List[str] = []
        with self._rlock:
            for leg in self.legs(grid_id=grid_id, cell_id=cell_id, generation=generation):
                if not leg.final:
                    blockers.append(f"leg {leg.cid} ({leg.role.value}) is {leg.state.value}")
                reservation = self._x("SELECT state FROM reservations WHERE cid = ?", (leg.cid,)).fetchone()
                if reservation is not None and reservation[0] == "ACTIVE":
                    blockers.append(f"reservation of leg {leg.cid} is active")
                if self._x("SELECT 1 FROM outbox WHERE cid = ? AND status != 'DONE'", (leg.cid,)).fetchone():
                    blockers.append(f"outbox of leg {leg.cid} unresolved")
            unallocated = self._x(
                """SELECT count(*) FROM fills f JOIN allocations a ON a.cid = f.cid
                   WHERE a.grid_id = ? AND a.cell_id = ? AND a.generation = ?
                   AND NOT EXISTS (SELECT 1 FROM fill_allocations fa WHERE fa.dedupe_key = f.dedupe_key)""",
                (grid_id, cell_id, generation)).fetchone()[0]
        if unallocated:
            blockers.append(f"{unallocated} aggregated fill(s) not yet allocated")
        if cycle.entry_filled != cycle.exit_filled:
            blockers.append(f"obligation open: E={cycle.entry_filled} X={cycle.exit_filled}")
        if cycle.dust != 0:
            blockers.append(f"dust {cycle.dust}")
        if cycle.late_evidence == 1:
            blockers.append("late evidence not acknowledged")
        return blockers

    def close_cycle(self, tx: Optional[Transaction], grid_id: str, cell_id: int, generation: int,
                    reason: str) -> CycleRecord:
        _require_text(reason, "reason", 2000)
        with self._scope(tx):
            self._require_writer()
            cycle = self.cycle(grid_id, cell_id, generation)
            if cycle.state == "COMPLETE":
                return cycle
            blockers = self.cycle_release_blockers(grid_id, cell_id, generation)
            if blockers:
                raise InvalidTransitionError(f"cycle {grid_id}/{cell_id}/{generation} cannot be released: "
                                             + "; ".join(blockers))
            self._x("UPDATE cycles SET state = 'COMPLETE', closed_at_ms = ?, close_reason = ? "
                    "WHERE grid_id = ? AND cell_id = ? AND generation = ?",
                    (self._clock_ms(), reason, grid_id, cell_id, generation))
            self._transition("CYCLE", f"{grid_id}/{cell_id}/{generation}", "OPEN", "COMPLETE", reason)
            return self.cycle(grid_id, cell_id, generation)

    # ---------------------------------------------------------------------------------------------- CIDs

    def cid_for(self, leg: LegIdentity) -> Optional[int]:
        with self._rlock:
            self._require_open()
            row = self._x("SELECT cid FROM cid_map WHERE grid_id = ? AND cell_id = ? AND generation = ? AND role = ? "
                          "AND revision = ?", (leg.grid_id, leg.cell_id, leg.generation, LegRole(leg.role).value,
                                               leg.revision)).fetchone()
        return None if row is None else row[0]

    def identity_for_cid(self, cid: int) -> Optional[LegIdentity]:
        with self._rlock:
            self._require_open()
            row = self._x("SELECT * FROM cid_map WHERE cid = ?", (_require_int(cid, "cid"),)).fetchone()
        if row is None:
            return None
        return LegIdentity(row["grid_id"], row["cell_id"], row["generation"], LegRole(row["role"]), row["revision"])

    def allocate_cid(self, tx: Optional[Transaction], leg: LegIdentity, *,
                     is_foreign_cid: Optional[Callable[[int], bool]] = None) -> int:
        """Durable 48-bit CID for a full leg identity (NG-HIST-004, AC-43).

        The same identity always gets the same CID; a CID is never reused (``cid_map`` is append-only). A
        candidate already used by this map, seen on the account as a foreign CID, or rejected by
        ``is_foreign_cid`` raises :class:`CidCollisionError` (fail closed; nothing is skipped silently --
        an operator may :meth:`retire_cid` it). Exhausting the epoch raises :class:`CidExhaustedError`.
        """
        if not isinstance(leg, LegIdentity):
            raise TypeError("leg must be LegIdentity")
        role = _enum(LegRole, leg.role, "role")
        _require_int(leg.generation, "generation", 1)
        _require_int(leg.revision, "revision", 0)
        _require_int(leg.cell_id, "cell_id", 0)
        with self._scope(tx):
            self._require_writer()
            existing = self.cid_for(leg)
            if existing is not None:
                return existing
            cycle = self.cycle(leg.grid_id, leg.cell_id, leg.generation)
            if not self._cycle_accepts(cycle, role):
                raise InvalidTransitionError(f"cycle {leg.grid_id}/{leg.cell_id}/{leg.generation} is not open")
            epoch, seq = self._x("SELECT cid_epoch, next_seq FROM cid_state WHERE id = 1").fetchone()
            while True:
                if seq > CID_MAX_SEQ:
                    raise CidExhaustedError(f"CID space of epoch {epoch} exhausted")
                candidate = (epoch << CID_EPOCH_SHIFT) | seq
                if candidate <= 0 or candidate > MAX_CLIENT_ORDER_ID:
                    raise CidExhaustedError(f"CID {candidate} outside the 48-bit client order id range")
                if self._x("SELECT 1 FROM retired_cids WHERE cid = ?", (candidate,)).fetchone() is None:
                    break
                seq += 1
            if self._x("SELECT 1 FROM cid_map WHERE cid = ?", (candidate,)).fetchone() is not None:
                raise CidCollisionError(f"CID {candidate} is already mapped (ledger integrity violation)", candidate)
            foreign = self._x("SELECT source FROM foreign_cids WHERE cid = ?", (candidate,)).fetchone()
            if foreign is not None or (is_foreign_cid is not None and is_foreign_cid(candidate)):
                raise CidCollisionError(f"CID {candidate} is already used on the account "
                                        f"({foreign[0] if foreign else 'caller check'}); retire_cid() it explicitly",
                                        candidate)
            self._x("INSERT INTO cid_map(cid, grid_id, cell_id, generation, role, revision, allocated_at_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (candidate, leg.grid_id, leg.cell_id, leg.generation, role.value, leg.revision, self._clock_ms()))
            self._x("UPDATE cid_state SET next_seq = ? WHERE id = 1", (seq + 1,))
            self.fault_hooks.hit("after_cid_allocated")
            return candidate

    def retire_cid(self, tx: Optional[Transaction], cid: int, actor: str, reason: str) -> None:
        """Audited: never allocate ``cid`` (e.g. after a :class:`CidCollisionError` with a foreign order)."""
        _require_int(cid, "cid", 1)
        _require_text(actor, "actor")
        _require_text(reason, "reason", 2000)
        with self._scope(tx):
            self._require_writer()
            if self._x("SELECT 1 FROM cid_map WHERE cid = ?", (cid,)).fetchone():
                raise InvalidTransitionError("an allocated CID cannot be retired")
            self._x("INSERT OR IGNORE INTO retired_cids(cid, reason, actor, at_ms) VALUES (?, ?, ?, ?)",
                    (cid, reason, actor, self._clock_ms()))
            self._audit("cid_retired", actor, {"cid": cid, "reason": reason})

    def note_foreign_cids(self, tx: Optional[Transaction], cids: Iterable[int], source: str) -> None:
        """Remember client ids seen on the account that are not ours (collision evidence for allocate_cid)."""
        with self._scope(tx):
            self._require_writer()
            for cid in cids:
                self._note_foreign(_require_int(cid, "cid"), source)

    def _note_foreign(self, cid: int, source: str) -> None:
        if self._x("SELECT 1 FROM cid_map WHERE cid = ?", (cid,)).fetchone() is None:
            self._x("INSERT OR IGNORE INTO foreign_cids(cid, source, first_seen_ms) VALUES (?, ?, ?)",
                    (cid, source, self._clock_ms()))

    # ---------------------------------------------------------------------------------------------- legs / orders

    @staticmethod
    def _leg(row: sqlite3.Row) -> LegRecord:
        return LegRecord(
            cid=row["cid"], grid_id=row["grid_id"], cell_id=row["cell_id"], generation=row["generation"],
            role=LegRole(row["role"]), revision=row["revision"], side=Side(row["side"]),
            price=parse_decimal(row["price"]), amount=parse_decimal(row["amount"]),
            order_type=OrderTypePolicy(row["order_type"]), reduce_only=bool(row["reduce_only"]),
            expiry_ms=row["expiry_ms"], state=OrderState(row["state"]), filled=parse_decimal(row["filled"]),
            created_at_ms=row["created_at_ms"], updated_at_ms=row["updated_at_ms"])

    def _with_allocation(self, leg: LegRecord) -> LegRecord:
        shares = self.leg_allocation(leg.cid)
        return leg if shares is None else dataclasses.replace(leg, allocation=shares)

    def leg_allocation(self, cid: int) -> Optional[Dict[int, Decimal]]:
        """``{generation: share}`` of an aggregate TP leg (same shape as WS-A ``Leg.allocation``), else None."""
        with self._rlock:
            self._require_open()
            rows = self._x("SELECT generation, amount FROM allocations WHERE cid = ? ORDER BY generation",
                           (cid,)).fetchall()
        return {row[0]: parse_decimal(row[1]) for row in rows} or None

    def leg(self, cid: int) -> Optional[LegRecord]:
        with self._rlock:
            self._require_open()
            row = self._x("SELECT * FROM legs WHERE cid = ?", (_require_int(cid, "cid"),)).fetchone()
        return None if row is None else self._with_allocation(self._leg(row))

    def legs(self, *, grid_id: Optional[str] = None, cell_id: Optional[int] = None,
             generation: Optional[int] = None, non_final_only: bool = False) -> List[LegRecord]:
        clauses, params = [], []
        for column, value in (("grid_id", grid_id), ("cell_id", cell_id), ("generation", generation)):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        if non_final_only:
            clauses.append("state NOT IN (?, ?, ?)")
            params.extend(state.value for state in sorted(FINAL_ORDER_STATES, key=lambda s: s.value))
        where = " AND ".join(clauses) or "1 = 1"
        with self._rlock:
            self._require_open()
            rows = self._x(f"SELECT * FROM legs WHERE {where} ORDER BY cid", params).fetchall()
        return [self._with_allocation(self._leg(row)) for row in rows]

    @staticmethod
    def _order(row: sqlite3.Row) -> OrderRecord:
        return OrderRecord(cid=row["cid"], exchange_order_id=row["exchange_order_id"], order_index=row["order_index"],
                           nonce=row["nonce"], submission_state=row["submission_state"],
                           cancel_state=row["cancel_state"], venue_status=row["venue_status"],
                           venue_filled=parse_decimal(row["venue_filled"]),
                           venue_remaining=parse_decimal(row["venue_remaining"]), venue_final=bool(row["venue_final"]),
                           last_evidence_ms=row["last_evidence_ms"])

    def order(self, cid: int) -> Optional[OrderRecord]:
        with self._rlock:
            self._require_open()
            row = self._x("SELECT * FROM orders WHERE cid = ?", (_require_int(cid, "cid"),)).fetchone()
        return None if row is None else self._order(row)

    def _set_leg_state_raw(self, leg: LegRecord, to_state: OrderState, reason: str) -> None:
        if leg.state == to_state:
            return
        self._x("UPDATE legs SET state = ?, updated_at_ms = ? WHERE cid = ?", (to_state.value, self._clock_ms(), leg.cid))
        self._transition("LEG", str(leg.cid), leg.state.value, to_state.value, reason)
        if to_state in FINAL_ORDER_STATES:
            self._release_reservation(leg.cid, f"leg {to_state.value}: {reason}")

    def set_leg_state(self, tx: Optional[Transaction], cid: int, to_state: OrderState, *, reason: str = "",
                      expected_from: Optional[Iterable[OrderState]] = None) -> LegRecord:
        """Evidence-driven leg transition (LIVE / TERMINAL_UNKNOWN / TERMINAL / REJECTED_ZERO_FILL).

        Final states are final. An undispatched INTENT cannot be moved by evidence. LIVE needs evidence (transport
        ACCEPTED, a recorded venue order row or a history fill). TERMINAL requires the exact final venue order row
        with cumulative filled equal to the leg's history fills (NG-HIST-002); REJECTED_ZERO_FILL requires a final
        venue row with zero fill and no history fills (use record_transport_result for a documented definitive
        venue reject). Reaching a final state releases the leg's reservation.
        """
        to_state = _enum(OrderState, to_state, "order state")
        with self._scope(tx):
            self._require_writer()
            leg = self.leg(cid)
            if leg is None:
                raise KeyError(f"unknown leg {cid}")
            if leg.state == to_state:
                return leg
            if expected_from is not None and leg.state not in {OrderState(s) for s in expected_from}:
                raise InvalidTransitionError(f"leg {cid} is {leg.state.value}, expected one of {list(expected_from)}")
            if leg.final:
                raise InvalidTransitionError(f"leg {cid} is final ({leg.state.value})")
            if to_state not in _EVIDENCE_TARGETS:
                raise InvalidTransitionError(f"{to_state.value} is only reachable through the outbox protocol")
            if leg.state == OrderState.INTENT:
                raise InvalidTransitionError(f"leg {cid} was never dispatched; evidence cannot move it "
                                             f"(possible CID collision)")
            self._check_evidence_for(leg, to_state)
            self._set_leg_state_raw(leg, to_state, reason)
            return self.leg(cid)

    def _check_evidence_for(self, leg: LegRecord, to_state: OrderState) -> None:
        cid = leg.cid
        order = self.order(cid)
        if to_state in _REJECTED_STATES:
            if leg.filled != 0 or self._fill_count(cid):
                raise InvalidTransitionError(f"leg {cid} has fills; it cannot be a zero-fill rejection")
            if order is None or not order.venue_final or order.venue_filled != 0:
                raise InvalidTransitionError(f"leg {cid}: no final zero-fill venue row; rejection unproven "
                                             f"(timeouts/not-found stay UNKNOWN)")
        elif to_state == OrderState.TERMINAL:
            unallocated = self._unallocated_quantity(cid)
            if unallocated:
                raise InvalidTransitionError(f"leg {cid}: {unallocated} of its fills are unallocated; allocate them "
                                             f"to cycles before TERMINAL (else they would vanish from X and reserve)")
            if order is None or not order.venue_final or order.venue_filled is None:
                raise InvalidTransitionError(f"leg {cid}: no exact final venue order row; TERMINAL unproven")
            if order.venue_filled != leg.filled:
                raise InvalidTransitionError(f"leg {cid}: history fills {leg.filled} != terminal cumulative "
                                             f"{order.venue_filled}; TERMINAL unproven")
        elif to_state == OrderState.LIVE:
            evidenced = order is not None and (order.submission_state == "ACCEPTED" or
                                               order.last_evidence_ms is not None)
            if not evidenced and not self._fill_count(cid):
                raise InvalidTransitionError(f"leg {cid}: no acceptance, venue row or fill; LIVE unproven")

    def _fill_count(self, cid: int) -> int:
        return self._x("SELECT count(*) FROM fills WHERE cid = ?", (cid,)).fetchone()[0]

    def _unallocated_quantity(self, cid: int) -> Decimal:
        """Fill quantity of an aggregated (allocated) leg not yet split to cycles; 0 for ordinary legs."""
        rows = self._x("""SELECT f.size FROM fills f WHERE f.cid = ?
                          AND EXISTS (SELECT 1 FROM allocations a WHERE a.cid = f.cid)
                          AND NOT EXISTS (SELECT 1 FROM fill_allocations fa WHERE fa.dedupe_key = f.dedupe_key)""",
                       (cid,)).fetchall()
        return sum((parse_decimal(r[0]) for r in rows), Decimal(0))

    def record_order_evidence(self, tx: Optional[Transaction], row: ExchangeOrderRow, *,
                              final: bool = False) -> Optional[int]:
        """Record an active-orders (or other non-history) row for an owned order; returns its CID or None.

        Unknown rows are not adopted (their client id is remembered as foreign). Such a row is never terminal proof:
        ``final=True`` is refused -- terminal rows only come from ``accountInactiveOrders`` through
        :meth:`apply_history_batch` (NG-HIST-001: WS/cancel events and active-list rows are not proof)."""
        if not isinstance(row, ExchangeOrderRow):
            raise TypeError("row must be ExchangeOrderRow")
        if final:
            raise ValueError("final order rows are accepted only from history (apply_history_batch), never from "
                             "active orders or WS events")
        with self._scope(tx):
            self._require_writer()
            cid = self._attribute_order(row)
            if cid is None:
                client = _row_client_id(row)
                if client is not None:
                    self._note_foreign(client, "active_orders")
                return None
            problems = self._order_row_problems(cid, row)
            if problems:
                raise InvalidTransitionError(f"order evidence for {cid} conflicts: {'; '.join(problems)}")
            self._update_order_from_row(cid, row, final=False)
            return cid

    # ---------------------------------------------------------------------------------------------- intents

    def entry_blockers(self) -> List[str]:
        """Persistence-level reasons that forbid new ENTRY intents (new exposure)."""
        blockers: List[str] = []
        engine = self.engine()
        if engine.manual_reconcile_required:
            blockers.append(f"manual reconciliation required: {engine.manual_reconcile_reason}")
        with self._rlock:
            conflicts = self._x("SELECT count(*) FROM history_conflicts WHERE resolved_at_ms IS NULL").fetchone()[0]
            unmatched = self._x("SELECT count(*) FROM history_inbox WHERE stream = 'TRADES' AND status = 'UNMATCHED' "
                                "AND resolved_at_ms IS NULL").fetchone()[0]
            gaps = self._x("SELECT stream, required_boundary_ts_ms, oldest_available_ts_ms FROM cursors "
                           "WHERE retention_gap_open = 1 ORDER BY stream").fetchall()
        for gap in gaps:
            blockers.append(f"retention gap on {gap[0]}: required boundary {gap[1]} older than available history "
                            f"{gap[2]}; audited manual reconciliation naming this stream required")
        if conflicts:
            blockers.append(f"{conflicts} unresolved history conflict(s)")
        if unmatched:
            blockers.append(f"{unmatched} unmatched account trade(s) require baseline audit")
        if self.degraded_reason is not None:
            blockers.append(f"persistence degraded: {self.degraded_reason}")
        return blockers

    def record_intent(self, tx: Optional[Transaction], leg_identity: LegIdentity, submit_request: SubmitRequest,
                      reservation: Optional[Reservation] = None, *, cell_state: Optional[CellState] = None,
                      cell_blocker: Any = _KEEP, allocations: Optional[Sequence[Tuple[str, int, int, Decimal]]] = None,
                      reason: str = "") -> IntentRecord:
        """One transaction: leg INTENT + exact outbox SUBMIT request + CID binding + ACTIVE reservation (+ cell
        state) -- NG-DB-002. Transport may only be called after this commits AND :meth:`mark_dispatching`
        commits. Re-recording the identical request is idempotent; a different request for the same CID is
        refused (never replace an intent; use a new revision).
        """
        if not isinstance(submit_request, SubmitRequest):
            raise TypeError("submit_request must be SubmitRequest")
        cid = _require_int(submit_request.client_order_id, "client_order_id", 1)
        if cid > MAX_CLIENT_ORDER_ID:
            raise CidExhaustedError("client order id exceeds 48 bits")
        side = _enum(Side, submit_request.side, "side")
        order_type = _enum(OrderTypePolicy, submit_request.order_type, "order type")
        price = _require_positive(submit_request.price, "price")
        amount = _require_positive(submit_request.amount, "amount")
        if not isinstance(submit_request.reduce_only, bool):
            raise TypeError("reduce_only must be bool")
        if submit_request.expiry_ms is not None:
            _require_int(submit_request.expiry_ms, "expiry_ms", 0)
        reservation = reservation or Reservation(side=side, amount=amount, slots=1)
        res_amount = _require_positive(reservation.amount, "reservation amount")
        if _enum(Side, reservation.side, "reservation side") != side or res_amount < amount:
            raise ValueError("reservation must cover the full submitted amount on the same side")
        _require_int(reservation.slots, "reservation slots", 0)
        request_json = canonical_json(submit_request)
        with self._scope(tx) as scope:
            self._require_writer()
            if self.degraded_reason is not None:
                raise PersistenceError(f"store is degraded ({self.degraded_reason}); no new submit intents until "
                                       f"clear_degraded()")
            mapped = self.identity_for_cid(cid)
            if mapped != LegIdentity(leg_identity.grid_id, leg_identity.cell_id, leg_identity.generation,
                                     LegRole(leg_identity.role), leg_identity.revision):
                raise InvalidTransitionError(f"CID {cid} is not allocated to {leg_identity} (allocate_cid first)")
            existing = self.leg(cid)
            if existing is not None:
                outbox = self._x("SELECT id, request_json FROM outbox WHERE cid = ? AND kind = 'SUBMIT'",
                                 (cid,)).fetchone()
                if outbox is not None and outbox["request_json"] == request_json:
                    return IntentRecord(cid=cid, outbox_id=outbox["id"], request=submit_request, leg=existing,
                                        created=False)
                raise InvalidTransitionError(f"CID {cid} already has a different intent; use a new revision")
            role = LegRole(leg_identity.role)
            parts = self._intent_preconditions(leg_identity, side, price, amount, allocations, cid)
            now = self._clock_ms()
            self._x("""INSERT INTO legs(cid, grid_id, cell_id, generation, role, revision, side, price, amount,
                       order_type, reduce_only, expiry_ms, state, filled, created_at_ms, updated_at_ms)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'INTENT', '0', ?, ?)""",
                    (cid, leg_identity.grid_id, leg_identity.cell_id, leg_identity.generation, role.value,
                     leg_identity.revision, side.value, canonical_decimal(price), canonical_decimal(amount),
                     order_type.value, int(submit_request.reduce_only), submit_request.expiry_ms, now, now))
            self._x("INSERT INTO orders(cid, submission_state, updated_at_ms) VALUES (?, 'INTENT', ?)", (cid, now))
            outbox_id = self._x("INSERT INTO outbox(kind, cid, request_json, status, created_at_ms) "
                                "VALUES ('SUBMIT', ?, ?, 'PENDING', ?)", (cid, request_json, now)).lastrowid
            self._x("INSERT INTO reservations(cid, side, amount, slots, state, created_at_ms) "
                    "VALUES (?, ?, ?, ?, 'ACTIVE', ?)",
                    (cid, side.value, canonical_decimal(res_amount), reservation.slots, now))
            for grid_id, cell_id, generation, part in parts:
                self._x("INSERT INTO allocations(cid, grid_id, cell_id, generation, amount, created_at_ms) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (cid, grid_id, cell_id, generation, canonical_decimal(part), now))
            self._transition("LEG", str(cid), None, OrderState.INTENT.value, reason or "record_intent")
            if cell_state is not None:
                self.set_cell_state(scope, leg_identity.grid_id, leg_identity.cell_id, cell_state,
                                    blocker=cell_blocker, reason=reason or f"{role.value} intent")
            self.fault_hooks.hit("before_intent_commit")
            scope.after_commit("after_intent_commit")
            return IntentRecord(cid=cid, outbox_id=outbox_id, request=submit_request, leg=self.leg(cid), created=True)

    def _check_single_physical_entry(self, cycle: CycleRecord) -> None:
        """MVP: one physical entry per cycle. A new ENTRY revision is allowed only after every earlier entry of the
        cycle is final with zero fill (e.g. pre-send rejection or documented zero-fill reject, NG-DB-005)."""
        for leg in self.legs(grid_id=cycle.grid_id, cell_id=cycle.cell_id, generation=cycle.generation):
            if leg.role != LegRole.ENTRY:
                continue
            if not leg.final:
                raise InvalidTransitionError(f"cycle already has entry leg {leg.cid} in {leg.state.value}")
            if leg.filled != 0:
                raise InvalidTransitionError(f"cycle entry leg {leg.cid} executed {leg.filled}; the cycle closes that "
                                             f"quantity and a new cycle starts with the full amount")

    def _tp_reserved(self, cycle: CycleRecord) -> Decimal:
        """Unfilled remainder of non-final TP legs obligated to ``cycle`` (own legs and allocations)."""
        reserved = Decimal(0)
        own = self._x("""SELECT l.cid, l.amount, l.filled FROM legs l WHERE l.grid_id = ? AND l.cell_id = ?
                         AND l.generation = ? AND l.role = 'TP' AND l.state NOT IN (?, ?, ?)
                         AND NOT EXISTS (SELECT 1 FROM allocations a WHERE a.cid = l.cid)""",
                      [cycle.grid_id, cycle.cell_id, cycle.generation] +
                      [s.value for s in sorted(FINAL_ORDER_STATES, key=lambda s: s.value)]).fetchall()
        for row in own:
            reserved += parse_decimal(row["amount"]) - parse_decimal(row["filled"])
        allocated = self._x("""SELECT a.cid, a.amount, l.state FROM allocations a JOIN legs l ON l.cid = a.cid
                               WHERE a.grid_id = ? AND a.cell_id = ? AND a.generation = ? AND l.role = 'TP'""",
                            [cycle.grid_id, cycle.cell_id, cycle.generation]).fetchall()
        for row in allocated:
            if OrderState(row["state"]) in FINAL_ORDER_STATES:
                # a final aggregate leg can still hold executed-but-unallocated quantity (e.g. a late fill): it is
                # neither in X nor releasable, so it conservatively counts against every cycle it may belong to
                reserved += self._unallocated_quantity(row["cid"])
                continue
            used = sum((parse_decimal(r[0]) for r in self._x(
                "SELECT fa.amount FROM fill_allocations fa JOIN fills f ON f.dedupe_key = fa.dedupe_key "
                "WHERE f.cid = ? AND fa.grid_id = ? AND fa.cell_id = ? AND fa.generation = ?",
                (row["cid"], cycle.grid_id, cycle.cell_id, cycle.generation))), Decimal(0))
            reserved += parse_decimal(row["amount"]) - used
        return reserved

    def _check_tp_headroom(self, cycle: CycleRecord, cid: int, amount: Decimal,
                           allocations: Optional[Sequence[Tuple[str, int, int, Decimal]]]) -> None:
        """NG-CELL-002 invariant ``confirmed_exit + reserved_TP_unfilled <= confirmed_entry`` per cycle: a TP can
        only close history-confirmed entry quantity, never more (no duplicate TP on status lag)."""
        parts = [(cycle.grid_id, cycle.cell_id, cycle.generation, amount)] if not allocations else allocations
        for grid_id, cell_id, generation, part in parts:
            target = self.cycle(grid_id, cell_id, generation)
            reserved = self._tp_reserved(target)
            if target.exit_filled + reserved + Decimal(canonical_decimal(part)) > target.entry_filled:
                raise InvalidTransitionError(
                    f"TP {cid} for {grid_id}/{cell_id}/{generation} exceeds confirmed entry: X={target.exit_filled} "
                    f"+ reserved={reserved} + new={part} > E={target.entry_filled}")

    def prepare_submit(self, tx: Optional[Transaction], leg_identity: LegIdentity, *, side: Side, price: Decimal,
                       amount: Decimal, order_type: OrderTypePolicy, reduce_only: bool = False,
                       expiry_ms: Optional[int] = None, reservation: Optional[Reservation] = None,
                       cell_state: Optional[CellState] = None, reason: str = "") -> IntentRecord:
        """``allocate_cid`` + ``record_intent`` in one transaction. Every guard runs before the CID is allocated, so a
        refused intent (e.g. :class:`EntryBlockedError`) writes nothing."""
        with self._scope(tx) as scope:
            self._require_writer()
            if self.degraded_reason is not None:
                raise PersistenceError(f"store is degraded ({self.degraded_reason}); no new submit intents until "
                                       f"clear_degraded()")
            if self.cid_for(leg_identity) is None:
                self._intent_preconditions(leg_identity, _enum(Side, side, "side"), _require_positive(price, "price"),
                                           _require_positive(amount, "amount"), None, None)
            cid = self.allocate_cid(scope, leg_identity)
            request = SubmitRequest(client_order_id=cid, side=side, price=price, amount=amount, order_type=order_type,
                                    reduce_only=reduce_only, expiry_ms=expiry_ms)
            return self.record_intent(scope, leg_identity, request, reservation, cell_state=cell_state, reason=reason)

    def _intent_preconditions(self, leg_identity: LegIdentity, side: Side, price: Decimal, amount: Decimal,
                              allocations: Optional[Sequence[Tuple[str, int, int, Decimal]]],
                              cid: Optional[int]) -> List[Tuple[str, int, int, Decimal]]:
        """Every read-only guard of a new intent (runs before any write). Returns validated allocation parts."""
        cycle = self.cycle(leg_identity.grid_id, leg_identity.cell_id, leg_identity.generation)
        role = LegRole(leg_identity.role)
        if not self._cycle_accepts(cycle, role):
            raise InvalidTransitionError("intent for a closed cycle (only an audited late obligation of a released "
                                         "cycle may still get a TP)")
        expected_side = cycle.entry_side if role == LegRole.ENTRY else cycle.tp_side
        expected_price = cycle.entry_price if role == LegRole.ENTRY else cycle.tp_price
        if side != expected_side or price != expected_price:
            raise InvalidTransitionError(f"{role.value} must be {expected_side.value} @ {expected_price} "
                                         f"(fixed cell prices; no recenter/clamp)")
        parts = self._validated_allocations(role, leg_identity, side, price, amount, allocations)
        if role == LegRole.ENTRY:
            if amount != cycle.planned_amount:
                raise InvalidTransitionError(f"ENTRY amount {amount} must equal the cycle's planned amount "
                                             f"{cycle.planned_amount} (one physical entry, never glued or resized)")
            blockers = self.entry_blockers()
            if blockers:
                raise EntryBlockedError("new entry exposure is blocked: " + "; ".join(blockers), blockers)
            self._check_single_physical_entry(cycle)
        else:
            self._check_tp_headroom(cycle, cid, amount, parts or None)
        return parts

    def _cycle_accepts(self, cycle: CycleRecord, role: LegRole) -> bool:
        """OPEN cycles accept legs; a released cycle accepts only a TP for its audited late obligation (E > X)."""
        if cycle.state == "OPEN":
            return True
        return role == LegRole.TP and cycle.late_evidence == 2 and cycle.entry_filled > cycle.exit_filled

    @staticmethod
    def _allocation_pairs(leg_identity: LegIdentity, allocations: Any) -> List[Tuple[str, int, int, Any]]:
        """Accept WS-A's shapes -- ``{generation: share}`` (also ``Leg.to_record`` string keys) or ``(generation,
        share)`` pairs (``TpDispatchItem.allocation``) -- and the explicit ``(grid_id, cell_id, generation, share)``
        form. Two-element forms refer to the leg's own cell."""
        items = allocations.items() if isinstance(allocations, Mapping) else allocations
        pairs = []
        for item in items:
            item = tuple(item)
            if len(item) == 2:
                generation, share = item
                if isinstance(generation, str) and generation.isdigit():
                    generation = int(generation)
                if isinstance(share, str):
                    share = parse_decimal(share)
                pairs.append((leg_identity.grid_id, leg_identity.cell_id, generation, share))
            elif len(item) == 4:
                pairs.append(item)
            else:
                raise ValueError(f"allocation item {item!r} is neither (generation, share) nor (grid, cell, gen, share)")
        return pairs

    def _validated_allocations(self, role: LegRole, leg_identity: LegIdentity, side: Side, price: Decimal,
                               amount: Decimal, allocations: Any) -> List[Tuple[str, int, int, Decimal]]:
        """WS-A aggregate TP model (AC-33/AC-39): one TP leg of ONE cell over several of its accepting cycles
        (``{generation: share}``), same TP side and fixed TP price by construction, shares positive and summing to
        the order amount, hosted by the newest allocated generation. Per-cycle headroom is checked by the caller."""
        if not allocations:
            return []
        if role != LegRole.TP:
            raise InvalidTransitionError("ENTRY intents cannot carry allocations (no glued physical entries)")
        parts, seen, total = [], set(), Decimal(0)
        for grid_id, cell_id, generation, part in self._allocation_pairs(leg_identity, allocations):
            _require_int(generation, "allocation generation", 1)
            part_d = _require_positive(part, "allocation amount")
            if (grid_id, cell_id) != (leg_identity.grid_id, leg_identity.cell_id):
                target = self.cycle(grid_id, cell_id, generation)
                raise InvalidTransitionError(
                    f"allocation target {grid_id}/{cell_id}/{generation} must be a cycle of the leg's own cell "
                    f"{leg_identity.grid_id}/{leg_identity.cell_id} (same TP side and price required: "
                    f"{target.tp_side.value} @ {target.tp_price} vs {side.value} @ {price})")
            if generation in seen:
                raise ValueError(f"duplicate allocation generation {generation}")
            seen.add(generation)
            target = self.cycle(grid_id, cell_id, generation)
            if not self._cycle_accepts(target, LegRole.TP):
                raise InvalidTransitionError(f"allocation to closed cycle {grid_id}/{cell_id}/{generation}")
            if target.tp_side != side or target.tp_price != price:
                raise InvalidTransitionError(f"allocation target {grid_id}/{cell_id}/{generation} must have the same "
                                             f"TP side and price")
            total += part_d
            parts.append((grid_id, cell_id, generation, part_d))
        if total != amount:
            raise ValueError(f"allocations sum {total} != order amount {amount}")
        if max(seen) != leg_identity.generation:
            raise InvalidTransitionError(f"an aggregate TP is hosted by its newest allocated generation {max(seen)}, "
                                         f"not {leg_identity.generation}")
        return sorted(parts, key=lambda part: part[2])

    @staticmethod
    def _outbox(row: sqlite3.Row) -> OutboxRecord:
        return OutboxRecord(id=row["id"], kind=row["kind"], cid=row["cid"], request=_loads(row["request_json"]),
                            status=row["status"], attempts=row["attempts"], prev_leg_state=row["prev_leg_state"],
                            outcome=TransportOutcome(row["outcome"]) if row["outcome"] else None,
                            outcome_detail=row["outcome_detail"], created_at_ms=row["created_at_ms"],
                            dispatched_at_ms=row["dispatched_at_ms"], result_at_ms=row["result_at_ms"])

    def outbox_entry(self, outbox_id: int) -> OutboxRecord:
        with self._rlock:
            self._require_open()
            row = self._x("SELECT * FROM outbox WHERE id = ?", (outbox_id,)).fetchone()
        if row is None:
            raise KeyError(f"unknown outbox row {outbox_id}")
        return self._outbox(row)

    def outbox_for_cid(self, cid: int) -> List[OutboxRecord]:
        with self._rlock:
            self._require_open()
            rows = self._x("SELECT * FROM outbox WHERE cid = ? ORDER BY id", (cid,)).fetchall()
        return [self._outbox(row) for row in rows]

    def unresolved_outbox(self) -> List[OutboxRecord]:
        """PENDING (provably never sent), DISPATCHED (maybe sent, no result) and DONE/UNKNOWN rows whose leg is
        not final -- the durable recovery questions after a restart (NG-DB-002)."""
        with self._rlock:
            self._require_open()
            rows = self._x("""SELECT o.* FROM outbox o JOIN legs l ON l.cid = o.cid
                              WHERE o.status != 'DONE' OR (o.outcome = 'UNKNOWN' AND l.state NOT IN (?, ?, ?))
                              ORDER BY o.id""",
                           [s.value for s in sorted(FINAL_ORDER_STATES, key=lambda s: s.value)]).fetchall()
        return [self._outbox(row) for row in rows]

    def mark_dispatching(self, tx: Optional[Transaction], outbox_id: int, *,
                         resend_same_cid: bool = False) -> OutboxRecord:
        """Commit "transport is about to be called" before calling it. After this the outcome is unknown until
        a result or history evidence: the leg becomes SUBMIT_UNKNOWN (cancel: CANCEL_UNKNOWN).

        Only a PENDING row may be dispatched without ``resend_same_cid``. A DISPATCHED row (transport possibly
        called, e.g. by a process that died) and a DONE/UNKNOWN row are re-dispatched only with
        ``resend_same_cid=True`` -- the SAME CID, allowed only when the engine relies on documented venue idempotence
        -- and every dispatch increments ``attempts`` and appends an ``outbox_attempts`` row. A degraded store refuses
        every submit dispatch. A new CID is never issued here.
        """
        with self._scope(tx) as scope:
            self._require_writer()
            entry = self.outbox_entry(outbox_id)
            if entry.kind == KIND_SUBMIT and self.degraded_reason is not None:
                raise PersistenceError(f"store is degraded ({self.degraded_reason}); refusing to dispatch submit")
            if entry.status == "DISPATCHED" and not resend_same_cid:
                raise InvalidTransitionError(f"outbox {outbox_id} was already dispatched (outcome unknown); only an "
                                             f"explicit resend_same_cid may call transport again")
            if entry.status == "DONE":
                if not (resend_same_cid and entry.outcome == TransportOutcome.UNKNOWN):
                    raise InvalidTransitionError(f"outbox {outbox_id} already has result {entry.outcome}")
            leg = self.leg(entry.cid)
            if leg.final:
                raise InvalidTransitionError(f"leg {leg.cid} is final ({leg.state.value})")
            now = self._clock_ms()
            self._x("UPDATE outbox SET status = 'DISPATCHED', attempts = attempts + 1, dispatched_at_ms = ?, "
                    "outcome = NULL, outcome_detail = NULL, result_at_ms = NULL WHERE id = ?", (now, outbox_id))
            self._x("INSERT INTO outbox_attempts(outbox_id, attempt, dispatched_at_ms) VALUES (?, ?, ?)",
                    (outbox_id, entry.attempts + 1, now))
            if entry.kind == KIND_SUBMIT:
                self._x("UPDATE orders SET submission_state = 'DISPATCHED', updated_at_ms = ? WHERE cid = ?",
                        (now, leg.cid))
                if leg.state == OrderState.INTENT:
                    self._set_leg_state_raw(leg, OrderState.SUBMIT_UNKNOWN, "dispatching submit")
            else:
                self._x("UPDATE orders SET cancel_state = 'DISPATCHED', updated_at_ms = ? WHERE cid = ?",
                        (now, leg.cid))
                if leg.state == OrderState.CANCEL_PENDING:
                    self._set_leg_state_raw(leg, OrderState.CANCEL_UNKNOWN, "dispatching cancel")
            self.fault_hooks.hit("before_dispatch_commit")
            scope.after_commit("after_dispatch_commit")
            return self.outbox_entry(outbox_id)

    def record_cancel_intent(self, tx: Optional[Transaction], cid: int, reason: str = "") -> OutboxRecord:
        """Durable cancel intent (outbox CANCEL + leg CANCEL_PENDING). The reservation of the full remainder is
        kept until terminal proof (AC-21). Idempotent while a cancel is unresolved."""
        with self._scope(tx) as scope:
            self._require_writer()
            leg = self.leg(cid)
            if leg is None:
                raise KeyError(f"unknown leg {cid}")
            if leg.final:
                raise InvalidTransitionError(f"leg {cid} is final ({leg.state.value})")
            if leg.state == OrderState.INTENT:
                raise InvalidTransitionError(f"leg {cid} was never dispatched; record NOT_SENT instead of cancel")
            open_cancel = self._x("SELECT * FROM outbox WHERE cid = ? AND kind = 'CANCEL' AND status != 'DONE'",
                                  (cid,)).fetchone()
            if open_cancel is not None:
                return self._outbox(open_cancel)
            order = self.order(cid)
            now = self._clock_ms()
            request = canonical_json({"client_order_id": cid, "exchange_order_id": order.exchange_order_id})
            outbox_id = self._x("INSERT INTO outbox(kind, cid, request_json, status, prev_leg_state, created_at_ms) "
                                "VALUES ('CANCEL', ?, ?, 'PENDING', ?, ?)",
                                (cid, request, leg.state.value, now)).lastrowid
            self._x("UPDATE orders SET cancel_state = 'INTENT', updated_at_ms = ? WHERE cid = ?", (now, cid))
            if leg.state not in (OrderState.CANCEL_PENDING, OrderState.CANCEL_UNKNOWN):
                self._set_leg_state_raw(leg, OrderState.CANCEL_PENDING, reason or "cancel intent")
            self.fault_hooks.hit("before_cancel_intent_commit")
            scope.after_commit("after_cancel_intent_commit")
            return self.outbox_entry(outbox_id)

    def record_transport_result(self, tx: Optional[Transaction], cid: int, result: TransportResult, *,
                                kind: str = KIND_SUBMIT, leg_state: Optional[OrderState] = None) -> OutboxRecord:
        """Durably record what transport returned (NG-DB-005, AC-56).

        Every dispatched attempt keeps its own outcome in ``outbox_attempts``. For a SUBMIT, NOT_SENT (proven: no
        transport call) and DEFINITIVE_REJECT_ZERO_FILL release the intent only when the CID cannot be at the venue:
        the first attempt of a row that was never resent, no earlier UNKNOWN, no fills and no venue evidence
        (acceptance, exchange order id, a recorded venue row, a LIVE leg). In every other case the CID may already be
        resting on the venue, so the leg stays SUBMIT_UNKNOWN (or its evidenced state), the reservation is kept and
        the row's effective outcome is UNKNOWN. UNKNOWN never releases anything.
        """
        self.fault_point("after_transport_before_result_commit")
        if not isinstance(result, TransportResult):
            raise TypeError("result must be TransportResult")
        outcome = _enum(TransportOutcome, result.outcome, "transport outcome")
        if kind not in (KIND_SUBMIT, KIND_CANCEL):
            raise ValueError("kind must be SUBMIT or CANCEL")
        detail = result.detail or ""
        with self._scope(tx) as scope:
            self._require_writer()
            row = self._x("SELECT * FROM outbox WHERE cid = ? AND kind = ? AND status != 'DONE' ORDER BY id DESC "
                          "LIMIT 1", (cid, kind)).fetchone()
            if row is None:
                done = self._x("SELECT * FROM outbox WHERE cid = ? AND kind = ? ORDER BY id DESC LIMIT 1",
                               (cid, kind)).fetchone()
                if done is not None and self._same_last_result(self._outbox(done), outcome, detail):
                    if leg_state is not None:  # replayed result + refinement: still evidence-checked
                        self.set_leg_state(scope, cid, leg_state, reason=f"{kind} transport {outcome.value}")
                    return self._outbox(done)
                raise InvalidTransitionError(f"no unresolved {kind} outbox row for CID {cid}")
            entry = self._outbox(row)
            if entry.status == "PENDING" and outcome != TransportOutcome.NOT_SENT:
                raise InvalidTransitionError(f"{kind} for CID {cid} was never marked dispatching; transport must not "
                                             f"have been called")
            leg = self.leg(cid)
            order = self.order(cid)
            if result.exchange_order_id and order.exchange_order_id and \
                    order.exchange_order_id != result.exchange_order_id:
                raise InvalidTransitionError(f"CID {cid}: exchange order id {result.exchange_order_id} differs "
                                             f"from recorded {order.exchange_order_id}")
            releasing = outcome in (TransportOutcome.NOT_SENT, TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL)
            now = self._clock_ms()
            effective, effective_detail = outcome, detail
            target: Optional[OrderState] = None
            if kind == KIND_SUBMIT:
                if releasing and (leg.filled != 0 or self._fill_count(cid)):
                    raise InvalidTransitionError(f"CID {cid} has fills; {outcome.value} contradicts history")
                if releasing:
                    blocker = self._release_blocker(entry, leg, order)
                    if blocker is not None:
                        effective = TransportOutcome.UNKNOWN
                        effective_detail = f"{outcome.value} on attempt {entry.attempts} not accepted as release " \
                                           f"({blocker}): {detail}"
                if effective == TransportOutcome.NOT_SENT:
                    target, submission = OrderState.REJECTED_UNSENT, "REJECTED_UNSENT"
                elif effective == TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL:
                    target, submission = OrderState.REJECTED_ZERO_FILL, "REJECTED_ZERO_FILL"
                elif effective == TransportOutcome.ACCEPTED:
                    target = OrderState.LIVE if leg.state in (OrderState.INTENT, OrderState.SUBMIT_UNKNOWN) else None
                    submission = "ACCEPTED"
                else:
                    target = OrderState.SUBMIT_UNKNOWN if leg.state == OrderState.INTENT else None
                    submission = "ACCEPTED" if order.submission_state == "ACCEPTED" else "UNKNOWN"
                self._x("UPDATE orders SET submission_state = ?, updated_at_ms = ? WHERE cid = ?",
                        (submission, now, cid))
            else:
                cancel_state = {TransportOutcome.NOT_SENT: "NOT_SENT", TransportOutcome.ACCEPTED: "ACKED",
                                TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL: "REJECTED",
                                TransportOutcome.UNKNOWN: "UNKNOWN"}[outcome]
                cancelling = (OrderState.CANCEL_PENDING, OrderState.CANCEL_UNKNOWN)
                if releasing:
                    target = OrderState(entry.prev_leg_state) if leg.state in cancelling and entry.prev_leg_state \
                        else None
                elif outcome == TransportOutcome.ACCEPTED:
                    # a cancel ack is not terminal proof
                    target = OrderState.TERMINAL_UNKNOWN if leg.state in cancelling else None
                else:
                    target = OrderState.CANCEL_UNKNOWN if leg.state == OrderState.CANCEL_PENDING else None
                self._x("UPDATE orders SET cancel_state = ?, updated_at_ms = ? WHERE cid = ?",
                        (cancel_state, now, cid))
            if result.exchange_order_id and not order.exchange_order_id:
                self._x("UPDATE orders SET exchange_order_id = ? WHERE cid = ?", (result.exchange_order_id, cid))
            if target is not None and not leg.final:
                self._set_leg_state_raw(leg, target, f"{kind} transport {effective.value}")
            if leg_state is not None:  # engine refinement: same evidence rules as set_leg_state
                self.set_leg_state(scope, cid, leg_state, reason=f"{kind} transport {effective.value}")
            if entry.attempts >= 1:
                self._x("UPDATE outbox_attempts SET outcome = ?, outcome_detail = ?, result_at_ms = ? "
                        "WHERE outbox_id = ? AND attempt = ?", (outcome.value, detail or None, now, entry.id,
                                                                entry.attempts))
            self._x("UPDATE outbox SET status = 'DONE', outcome = ?, outcome_detail = ?, result_at_ms = ? WHERE id = ?",
                    (effective.value, effective_detail or None, now, entry.id))
            self.fault_hooks.hit("before_result_commit")
            scope.after_commit("after_result_commit")
            return self.outbox_entry(entry.id)

    def _release_blocker(self, entry: OutboxRecord, leg: LegRecord, order: OrderRecord) -> Optional[str]:
        """Why a NOT_SENT / zero-fill reject may NOT finalise this submit (None = it may)."""
        if entry.attempts > 1:
            return "the CID was resent after an unknown outcome"
        earlier = self._x("SELECT count(*) FROM outbox_attempts WHERE outbox_id = ? AND attempt < ? "
                          "AND (outcome IS NULL OR outcome != 'NOT_SENT')", (entry.id, max(entry.attempts, 1))
                          ).fetchone()[0]
        if earlier:
            return "an earlier attempt may have reached the venue"
        if leg.state not in (OrderState.INTENT, OrderState.SUBMIT_UNKNOWN):
            return f"leg is {leg.state.value}"
        if order.submission_state == "ACCEPTED" or order.exchange_order_id or order.last_evidence_ms is not None:
            return "the venue already reported this CID"
        return None

    def _same_last_result(self, entry: OutboxRecord, outcome: TransportOutcome, detail: str) -> bool:
        if entry.attempts >= 1:
            attempt = self._x("SELECT outcome, outcome_detail FROM outbox_attempts WHERE outbox_id = ? AND attempt = ?",
                              (entry.id, entry.attempts)).fetchone()
            if attempt is not None:
                return attempt[0] == outcome.value and (attempt[1] or "") == detail
        return entry.outcome == outcome and (entry.outcome_detail or "") == detail

    def outbox_attempts(self, outbox_id: int) -> List[Dict[str, Any]]:
        """Every transport attempt of an outbox row with its own outcome (a resend never erases the earlier one)."""
        with self._rlock:
            self._require_open()
            rows = self._x("SELECT * FROM outbox_attempts WHERE outbox_id = ? ORDER BY attempt", (outbox_id,)).fetchall()
        return [dict(row) for row in rows]

    # ---------------------------------------------------------------------------------------------- reservations

    def _release_reservation(self, cid: int, reason: str) -> None:
        self._x("UPDATE reservations SET state = 'RELEASED', released_at_ms = ?, release_reason = ? "
                "WHERE cid = ? AND state = 'ACTIVE'", (self._clock_ms(), reason, cid))

    def release_reservation(self, tx: Optional[Transaction], cid: int, reason: str) -> None:
        """Only for final legs (the engine may not free exposure of an unproven order)."""
        _require_text(reason, "reason", 2000)
        with self._scope(tx):
            self._require_writer()
            leg = self.leg(cid)
            if leg is None or not leg.final:
                raise InvalidTransitionError(f"leg {cid} is not final; its reservation stays")
            self._release_reservation(cid, reason)

    @staticmethod
    def _reservation(row: sqlite3.Row) -> ReservationRecord:
        return ReservationRecord(cid=row["cid"], side=Side(row["side"]), amount=parse_decimal(row["amount"]),
                                 slots=row["slots"], state=row["state"], created_at_ms=row["created_at_ms"],
                                 released_at_ms=row["released_at_ms"], release_reason=row["release_reason"])

    def reservations(self, active_only: bool = True) -> List[ReservationRecord]:
        with self._rlock:
            self._require_open()
            rows = self._x("SELECT * FROM reservations" + (" WHERE state = 'ACTIVE'" if active_only else "")
                           + " ORDER BY cid").fetchall()
        return [self._reservation(row) for row in rows]

    # ---------------------------------------------------------------------------------------------- history

    def _attribute_order(self, row: ExchangeOrderRow) -> Optional[int]:
        client = _row_client_id(row)
        if client is not None and self._x("SELECT 1 FROM cid_map WHERE cid = ?", (client,)).fetchone():
            return client
        exchange_id = row.order_id or row.order_index
        if exchange_id:
            found = self._x("SELECT cid FROM orders WHERE exchange_order_id = ?", (exchange_id,)).fetchall()
            if len(found) == 1:
                return found[0][0]
        return None

    def _order_row_problems(self, cid: int, row: ExchangeOrderRow) -> List[str]:
        leg = self.leg(cid)
        order = self.order(cid)
        problems = []
        if leg is None:
            return [f"CID {cid} is allocated but has no intent"]
        if leg.state in (OrderState.INTENT, OrderState.REJECTED_UNSENT):
            problems.append("venue evidence for an intent that was never dispatched (CID collision?)")
        client = _row_client_id(row)
        if client is not None and client != cid:
            problems.append(f"client id {client} != {cid}")
        if Side(row.side) != leg.side:
            problems.append(f"side {Side(row.side).value} != {leg.side.value}")
        if row.price != leg.price:
            problems.append(f"price {row.price} != {leg.price}")
        if row.initial_base_amount != leg.amount:
            problems.append(f"amount {row.initial_base_amount} != {leg.amount}")
        if row.order_id and order.exchange_order_id and row.order_id != order.exchange_order_id:
            problems.append(f"exchange order id {row.order_id} != {order.exchange_order_id}")
        if order.venue_filled is not None and row.filled_base_amount < order.venue_filled:
            problems.append(f"cumulative filled regressed {order.venue_filled} -> {row.filled_base_amount}")
        if row.filled_base_amount > leg.amount:
            problems.append(f"filled {row.filled_base_amount} exceeds amount {leg.amount}")
        if order.venue_final and not self._same_final_row(order, row):
            problems.append("final order row changed")
        return problems

    @staticmethod
    def _same_final_row(order: OrderRecord, row: ExchangeOrderRow) -> bool:
        return order.venue_filled == row.filled_base_amount and order.venue_status == row.status

    def _update_order_from_row(self, cid: int, row: ExchangeOrderRow, *, final: bool) -> None:
        self._x("""UPDATE orders SET exchange_order_id = COALESCE(exchange_order_id, ?), order_index = ?,
                   nonce = COALESCE(?, nonce), venue_status = ?, venue_filled = ?, venue_remaining = ?,
                   venue_final = MAX(venue_final, ?), venue_row_json = ?, last_evidence_ms = ?, updated_at_ms = ?
                   WHERE cid = ?""",
                (row.order_id or row.order_index, row.order_index, row.nonce, row.status,
                 canonical_decimal(row.filled_base_amount),
                 canonical_decimal(row.remaining_base_amount), int(final), row.raw_json, row.timestamp_ms,
                 self._clock_ms(), cid))

    def _conflict(self, result: HistoryBatchResult, kind: str, inbox_id: Optional[int], cid: Optional[int],
                  detail: str, discriminator: str = "") -> None:
        conflict_key = f"{kind}:{inbox_id}:{cid}" + (f":{discriminator}" if discriminator else "")
        cursor = self._x("INSERT OR IGNORE INTO history_conflicts(conflict_key, kind, inbox_id, cid, detail, "
                         "created_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
                         (conflict_key, kind, inbox_id, cid, detail, self._clock_ms()))
        new = cursor.rowcount == 1
        row = self._x("SELECT * FROM history_conflicts WHERE conflict_key = ?", (conflict_key,)).fetchone()
        record = self._conflict_record(row, new=new)
        if all(existing.id != record.id for existing in result.conflicts):
            result.conflicts.append(record)
        if new:
            self._audit("history_conflict", "store", {"kind": kind, "inbox_id": inbox_id, "cid": cid,
                                                      "detail": detail})

    @staticmethod
    def _conflict_record(row: sqlite3.Row, new: bool = False) -> HistoryConflictRecord:
        return HistoryConflictRecord(id=row["id"], kind=row["kind"], inbox_id=row["inbox_id"], cid=row["cid"],
                                     detail=row["detail"], created_at_ms=row["created_at_ms"],
                                     resolved_at_ms=row["resolved_at_ms"], resolution=row["resolution"], new=new)

    @staticmethod
    def _inbox(row: sqlite3.Row) -> InboxRecord:
        return InboxRecord(id=row["id"], stream=row["stream"], dedupe_key=row["dedupe_key"],
                           payload_hash=row["payload_hash"], payload=_loads(row["payload_json"]),
                           raw_json=row["raw_json"], status=row["status"], cid=row["cid"], detail=row["detail"],
                           received_at_ms=row["received_at_ms"], resolved_at_ms=row["resolved_at_ms"],
                           resolution=row["resolution"])

    def _inbox_by_id(self, inbox_id: int) -> InboxRecord:
        return self._inbox(self._x("SELECT * FROM history_inbox WHERE id = ?", (inbox_id,)).fetchone())

    def apply_history_batch(self, tx: Optional[Transaction], rows: Sequence[Union[ExchangeTradeRow, ExchangeOrderRow]],
                            cursor_updates: Sequence[CursorUpdate] = (),
                            ledger_transitions: Union[None, Sequence[LedgerTransition],
                                                      Callable[[Transaction, HistoryBatchResult], None]] = None, *,
                            dedupe_keys: Optional[Sequence[Any]] = None,
                            batch_id: Optional[str] = None) -> HistoryBatchResult:
        """ONE transaction: normalized inbox rows + dedupe keys + exact-ID fill attribution + ledger transitions +
        cursor/high-water updates (NG-HIST-002). A crash anywhere rolls all of it back; replaying the same batch
        is idempotent. Same dedupe key with identical payload is a no-op; with a different payload it becomes a
        durable history conflict (returned, blocks new entries until manual reconciliation).

        Trades are ``ExchangeTradeRow`` (history ``/trades``); order rows are terminal rows from
        ``accountInactiveOrders``. Ownership is decided only by exact CID / exchange order id, never by
        price/size/time. Unowned rows after the bootstrap cut are UNMATCHED (manual trade evidence); unowned rows
        at or before the cut are PRE_CUT and never count (AC-45).

        ``ledger_transitions`` is either a list of transition records or ``callable(tx, result)`` invoked after the
        rows are classified, so the engine only derives transitions from genuinely new facts.

        The store derives the canonical dedupe key of every row itself (``trade_dedupe_key``/``order_dedupe_key``).
        ``dedupe_keys`` (keyword, optional, aligned with ``rows``) lets a caller assert that its keys -- the
        ``ExchangeTradeRow.dedupe_key(domain)`` tuple or the canonical string -- are the same; any difference raises.
        """
        cursor_updates = list(cursor_updates)
        for update in cursor_updates:
            if not isinstance(update, CursorUpdate):
                raise TypeError("cursor_updates must be CursorUpdate records (pass dedupe_keys by keyword)")
        if dedupe_keys is not None and len(dedupe_keys) != len(rows):
            raise ValueError("dedupe_keys must be aligned with rows")
        with self._scope(tx) as scope:
            self._require_writer()
            engine = self.engine()
            if not engine.bootstrapped:
                raise BootstrapError("history can only be applied after bootstrap (the cut defines pre-bootstrap rows)")
            if dedupe_keys is not None:
                for row, given in zip(rows, dedupe_keys):
                    self._check_caller_key(engine.connector_domain, row, given)
            result = HistoryBatchResult()
            touched_orders: Set[int] = set()
            midpoint = len(rows) // 2
            for index, row in enumerate(rows):
                if index == midpoint and len(rows) > 1:
                    self.fault_hooks.hit("mid_history_batch")
                self._apply_history_row(engine, row, batch_id, result, touched_orders)
            for cid in sorted(result.touched_cids | touched_orders):
                self._check_cumulative(cid, result)
            if ledger_transitions is not None:
                if callable(ledger_transitions):
                    ledger_transitions(scope, result)
                else:
                    for transition in ledger_transitions:
                        self._apply_transition(scope, transition)
            self.fault_hooks.hit("before_cursor_commit")
            for update in cursor_updates:
                self._apply_cursor_update(update)
            scope.after_commit("after_history_commit")
            return result

    @staticmethod
    def _check_caller_key(domain: str, row: Any, given: Any) -> None:
        if isinstance(row, ExchangeTradeRow):
            canonical, native = trade_dedupe_key(domain, row), tuple(row.dedupe_key(domain))
        elif isinstance(row, ExchangeOrderRow):
            canonical = order_dedupe_key(domain, row)
            native = (domain, row.account_index, row.market_id, _row_exchange_id(row))
        else:
            raise TypeError(f"unsupported history row {type(row).__name__}")
        if given != canonical and (native is None or tuple(given) != native):
            raise ValueError(f"caller dedupe key {given!r} differs from the canonical key {canonical}")

    def _apply_history_row(self, engine: EngineRecord, row: Any, batch_id: Optional[str],
                           result: HistoryBatchResult, touched_orders: Set[int]) -> None:
        domain = engine.connector_domain
        if isinstance(row, ExchangeTradeRow):
            stream, key, payload = STREAM_TRADES, trade_dedupe_key(domain, row), _trade_payload(row)
        elif isinstance(row, ExchangeOrderRow):
            stream, key, payload = STREAM_INACTIVE_ORDERS, order_dedupe_key(domain, row), _order_payload(row)
        else:
            raise TypeError(f"unsupported history row {type(row).__name__}")
        _require_text(row.raw_json, "raw_json", 1_000_000)
        payload_json = canonical_json(payload)
        payload_hash = _sha256(payload_json)
        known = self._x("SELECT payload_hash, first_inbox_id FROM dedupe_keys WHERE dedupe_key = ?", (key,)).fetchone()
        now = self._clock_ms()
        if known is not None:
            if known["payload_hash"] == payload_hash:
                result.duplicates += 1
                first = self._inbox_by_id(known["first_inbox_id"])
                if first.status == "CONFLICT" and first.resolved_at_ms is None:
                    for conflict in self._x("SELECT * FROM history_conflicts WHERE inbox_id = ?", (first.id,)):
                        if all(c.id != conflict["id"] for c in result.conflicts):
                            result.conflicts.append(self._conflict_record(conflict))
                return
            inserted = self._x("INSERT OR IGNORE INTO history_inbox(stream, dedupe_key, payload_hash, payload_json, "
                               "raw_json, status, detail, batch_id, received_at_ms) "
                               "VALUES (?, ?, ?, ?, ?, 'CONFLICT', ?, ?, ?)",
                               (stream, key, payload_hash, payload_json, row.raw_json,
                                "same dedupe key, different payload", batch_id, now))
            inbox_id = inserted.lastrowid if inserted.rowcount == 1 else self._x(
                "SELECT id FROM history_inbox WHERE dedupe_key = ? AND payload_hash = ?", (key, payload_hash)
            ).fetchone()[0]
            self._conflict(result, "PAYLOAD_MISMATCH", inbox_id, None,
                           f"{stream} key {key}: payload differs from first seen row {known['first_inbox_id']}")
            return
        inbox_id = self._x("INSERT INTO history_inbox(stream, dedupe_key, payload_hash, payload_json, raw_json, status, "
                           "batch_id, received_at_ms) VALUES (?, ?, ?, ?, ?, 'UNMATCHED', ?, ?)",
                           (stream, key, payload_hash, payload_json, row.raw_json, batch_id, now)).lastrowid
        self._x("INSERT INTO dedupe_keys(dedupe_key, stream, payload_hash, first_inbox_id, first_seen_ms) "
                "VALUES (?, ?, ?, ?, ?)", (key, stream, payload_hash, inbox_id, now))
        if row.account_index != engine.account_index or row.market_id != engine.market_id:
            self._finish_inbox(inbox_id, "CONFLICT", None, "row outside this engine's account/market")
            self._conflict(result, "FOREIGN_SCOPE", inbox_id, None,
                           f"row for account {row.account_index} market {row.market_id}")
            return
        if stream == STREAM_TRADES:
            self._apply_trade(engine, row, key, inbox_id, result)
        else:
            self._apply_order_row(engine, row, inbox_id, result, touched_orders)

    def _finish_inbox(self, inbox_id: int, status: str, cid: Optional[int], detail: Optional[str]) -> None:
        self._x("UPDATE history_inbox SET status = ?, cid = ?, detail = ? WHERE id = ?", (status, cid, detail, inbox_id))

    def _classify_unowned(self, engine: EngineRecord, timestamp_ms: int, inbox_id: int, client: Optional[int],
                          source: str, result: HistoryBatchResult) -> None:
        if client is not None:
            self._note_foreign(client, source)
        if engine.bootstrap_cut_ts_ms is not None and timestamp_ms <= engine.bootstrap_cut_ts_ms:
            self._finish_inbox(inbox_id, "PRE_CUT", None, "before bootstrap cut; included in baseline")
            result.pre_cut.append(self._inbox_by_id(inbox_id))
        else:
            self._finish_inbox(inbox_id, "UNMATCHED", None, "no owned CID/exchange order id")
            result.unmatched.append(self._inbox_by_id(inbox_id))

    def _apply_trade(self, engine: EngineRecord, row: ExchangeTradeRow, key: str, inbox_id: int,
                     result: HistoryBatchResult) -> None:
        cid = None
        client = row.own_client_order_id
        if client is not None and self._x("SELECT 1 FROM cid_map WHERE cid = ?", (client,)).fetchone():
            cid = client
        elif row.own_exchange_order_id:
            found = self._x("SELECT cid FROM orders WHERE exchange_order_id = ?", (row.own_exchange_order_id,)).fetchall()
            if len(found) == 1:
                cid = found[0][0]
        if cid is None:
            self._classify_unowned(engine, row.timestamp_ms, inbox_id, client, "trades", result)
            return
        leg = self.leg(cid)
        order = self.order(cid)
        problems = []
        if leg is None:
            problems.append(("EVIDENCE_FOR_UNDISPATCHED_INTENT", f"CID {cid} allocated without intent"))
        else:
            if leg.state in (OrderState.INTENT, OrderState.REJECTED_UNSENT):
                problems.append(("EVIDENCE_FOR_UNDISPATCHED_INTENT",
                                 f"fill for CID {cid} that was never sent (collision?)"))
            if Side(row.own_side) != leg.side:
                problems.append(("SIDE_MISMATCH", f"trade side {Side(row.own_side).value} != leg {leg.side.value}"))
            if client is not None and client != cid:
                problems.append(("CID_MISMATCH", f"trade client id {client} != {cid}"))
            if row.own_exchange_order_id and order.exchange_order_id and \
                    row.own_exchange_order_id != order.exchange_order_id:
                problems.append(("EXCHANGE_ID_MISMATCH",
                                 f"trade order id {row.own_exchange_order_id} != {order.exchange_order_id}"))
            if not isinstance(row.size, Decimal) or row.size <= 0:
                problems.append(("MALFORMED", f"trade size {row.size!r}"))
            elif leg.filled + row.size > leg.amount:
                problems.append(("OVERFILL", f"fills {leg.filled}+{row.size} exceed amount {leg.amount}"))
        if problems:
            self._finish_inbox(inbox_id, "CONFLICT", cid, "; ".join(detail for _, detail in problems))
            for kind, detail in problems:
                self._conflict(result, kind, inbox_id, cid, detail)
            return
        now = self._clock_ms()
        cycle = self.cycle(leg.grid_id, leg.cell_id, leg.generation)
        # a non-final TP on a released cycle is the audited cover of its late obligation, not late evidence
        late = leg.final or (cycle.state == "COMPLETE" and leg.role == LegRole.ENTRY)
        if row.own_exchange_order_id and not order.exchange_order_id:
            self._x("UPDATE orders SET exchange_order_id = ? WHERE cid = ?", (row.own_exchange_order_id, cid))
        self._x("""INSERT INTO fills(dedupe_key, domain, account_index, market_id, trade_id_str, own_side,
                   own_exchange_order_id, cid, grid_id, cell_id, generation, role, size, price, is_maker, timestamp_ms,
                   inbox_id, late, applied_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (key, engine.connector_domain, row.account_index, row.market_id, row.trade_id_str,
                 Side(row.own_side).value, row.own_exchange_order_id, cid, leg.grid_id, leg.cell_id, leg.generation,
                 leg.role.value, canonical_decimal(row.size), canonical_decimal(row.price),
                 None if row.is_maker is None else int(row.is_maker), row.timestamp_ms, inbox_id, int(late), now))
        self._x("UPDATE legs SET filled = ?, updated_at_ms = ? WHERE cid = ?",
                (canonical_decimal(leg.filled + row.size), now, cid))
        shares = self.leg_allocation(cid)
        if shares is None:
            self._credit_cycle(leg.grid_id, leg.cell_id, leg.generation, leg.role, row.size)
        else:  # aggregate TP: split by water-filling the cumulative fill in generation order (WS-A model)
            before, after = _water_fill(shares, leg.filled), _water_fill(shares, leg.filled + row.size)
            for generation in sorted(shares):
                delta = after[generation] - before[generation]
                if delta > 0:
                    self._x("INSERT INTO fill_allocations(dedupe_key, grid_id, cell_id, generation, amount) "
                            "VALUES (?, ?, ?, ?, ?)", (key, leg.grid_id, leg.cell_id, generation,
                                                       canonical_decimal(delta)))
                    self._credit_cycle(leg.grid_id, leg.cell_id, generation, leg.role, delta)
        if late:
            self._x("UPDATE cycles SET late_evidence = 1 WHERE grid_id = ? AND cell_id = ? AND generation = ?",
                    (leg.grid_id, leg.cell_id, leg.generation))
        self._finish_inbox(inbox_id, "APPLIED", cid, "late fill" if late else None)
        fill = self._fill_by_key(key)
        result.applied.append(self._inbox_by_id(inbox_id))
        result.new_fills.append(fill)
        result.touched_cids.add(cid)
        if late:
            result.late_fills.append(fill)
            self._conflict(result, "LATE_FILL", inbox_id, cid,
                           f"fill {row.trade_id_str} after leg {leg.state.value}/cycle {cycle.state}: market freeze")

    def _credit_cycle(self, grid_id: str, cell_id: int, generation: int, role: LegRole, size: Decimal) -> None:
        column = "entry_filled" if role == LegRole.ENTRY else "exit_filled"
        cycle = self.cycle(grid_id, cell_id, generation)
        current = cycle.entry_filled if role == LegRole.ENTRY else cycle.exit_filled
        self._x(f"UPDATE cycles SET {column} = ? WHERE grid_id = ? AND cell_id = ? AND generation = ?",
                (canonical_decimal(current + size), grid_id, cell_id, generation))

    def _apply_order_row(self, engine: EngineRecord, row: ExchangeOrderRow, inbox_id: int,
                         result: HistoryBatchResult, touched_orders: Set[int]) -> None:
        cid = self._attribute_order(row)
        if cid is None:
            self._classify_unowned(engine, row.timestamp_ms, inbox_id, _row_client_id(row), "inactive_orders",
                                   result)
            return
        problems = self._order_row_problems(cid, row)
        if problems:
            kind = "EVIDENCE_FOR_UNDISPATCHED_INTENT" if "never dispatched" in problems[0] else "ORDER_MISMATCH"
            self._finish_inbox(inbox_id, "CONFLICT", cid, "; ".join(problems))
            self._conflict(result, kind, inbox_id, cid, "; ".join(problems))
            return
        self._update_order_from_row(cid, row, final=True)
        self._finish_inbox(inbox_id, "APPLIED", cid, None)
        result.applied.append(self._inbox_by_id(inbox_id))
        touched_orders.add(cid)

    def _check_cumulative(self, cid: int, result: HistoryBatchResult) -> None:
        order = self.order(cid)
        leg = self.leg(cid)
        if order is None or leg is None or not order.venue_final or order.venue_filled is None:
            return
        if leg.filled > order.venue_filled:
            inbox = self._x("SELECT id FROM history_inbox WHERE stream = 'INACTIVE_ORDERS' AND cid = ? "
                            "AND status = 'APPLIED' ORDER BY id DESC LIMIT 1", (cid,)).fetchone()
            # keyed by the excess cumulative: a further excess after an audited resolution is flagged again
            self._conflict(result, "CUMULATIVE_EXCEEDS_ORDER", inbox[0] if inbox else None, cid,
                           f"trade cumulative {leg.filled} > terminal order cumulative {order.venue_filled}",
                           discriminator=canonical_decimal(leg.filled))

    def _apply_cursor_update(self, update: CursorUpdate) -> None:
        if update.stream not in STREAMS:
            raise ValueError(f"unknown cursor stream {update.stream!r}")
        current = self.cursor(update.stream)
        sets, params = ["revision = revision + 1", "updated_at_ms = ?"], [self._clock_ms()]
        if update.clear_cursor:
            sets.append("cursor = NULL")
        elif update.cursor is not None:
            sets.append("cursor = ?")
            params.append(_require_text(update.cursor, "cursor", 4096))
        for column, value, monotonic in (("high_water_ts_ms", update.high_water_ts_ms, True),
                                         ("last_full_scan_ms", update.last_full_scan_ms, True),
                                         ("required_boundary_ts_ms", update.required_boundary_ts_ms, False),
                                         ("oldest_available_ts_ms", update.oldest_available_ts_ms, False)):
            if value is None:
                continue
            _require_int(value, column, 0)
            previous = getattr(current, column)
            if monotonic and previous is not None and value < previous:
                raise InvalidTransitionError(f"{update.stream} {column} would move backwards {previous} -> {value}")
            sets.append(f"{column} = ?")
            params.append(value)
        for column, value in (("high_water", update.high_water),
                              ("required_boundary_marker", update.required_boundary_marker)):
            if value is not None:
                sets.append(f"{column} = ?")
                params.append(_require_text(value, column, 4096))
        required = update.required_boundary_ts_ms if update.required_boundary_ts_ms is not None \
            else current.required_boundary_ts_ms
        oldest = update.oldest_available_ts_ms if update.oldest_available_ts_ms is not None \
            else current.oldest_available_ts_ms
        gap = current.retention_gap_open
        if (update.required_boundary_ts_ms is not None or update.oldest_available_ts_ms is not None) \
                and required is not None and oldest is not None and required < oldest:
            gap = True  # AC-53: the required overlap boundary is older than the retained history
            sets.append("retention_gap_open = 1")
        if update.complete is True and gap:
            raise InvalidTransitionError(f"{update.stream} has a retention gap (required boundary {required} < "
                                         f"oldest available {oldest}); it cannot be complete until an audited "
                                         f"manual reconciliation names this stream")
        if update.complete is True:
            sets.extend(["complete = 1", "incomplete_reason = NULL"])
        elif update.complete is False:
            sets.extend(["complete = 0", "incomplete_reason = ?"])
            params.append(_require_text(update.incomplete_reason or "", "incomplete_reason", 2000))
        self._x(f"UPDATE cursors SET {', '.join(sets)} WHERE stream = ?", params + [update.stream])

    def update_cursors(self, tx: Optional[Transaction], updates: Sequence[CursorUpdate]) -> None:
        with self._scope(tx):
            self._require_writer()
            for update in updates:
                self._apply_cursor_update(update)

    def _apply_transition(self, tx: Transaction, transition: Any) -> None:
        if isinstance(transition, LegStateChange):
            self.set_leg_state(tx, transition.cid, transition.to_state, reason=transition.reason,
                               expected_from=transition.expected_from)
        elif isinstance(transition, CellStateChange):
            self.set_cell_state(tx, transition.grid_id, transition.cell_id, transition.to_state,
                                blocker=transition.blocker, reason=transition.reason)
        elif isinstance(transition, CycleChange):
            self.update_cycle(tx, transition.grid_id, transition.cell_id, transition.generation, dust=transition.dust)
        elif isinstance(transition, EngineStateChange):
            self.set_engine_state(tx, transition.state, transition.reason)
        elif isinstance(transition, ReservationRelease):
            self.release_reservation(tx, transition.cid, transition.reason)
        elif isinstance(transition, FillAllocation):
            self.allocate_fill(tx, transition.dedupe_key, transition.parts)
        else:
            raise TypeError(f"unsupported ledger transition {type(transition).__name__}")

    def allocate_fill(self, tx: Optional[Transaction], dedupe_key: str,
                      parts: Sequence[Tuple[str, int, int, Decimal]]) -> None:
        """Assert the split of one aggregate-TP fill. Since the WS-A alignment, fills are split automatically at
        history time by water-filling the leg's cumulative fill over ``{generation: share}``; this call is an
        idempotent check (identical split -> no-op, anything else -> InvalidTransitionError). Only a fill that has
        no split at all (pre-alignment rows) is split here explicitly."""
        with self._scope(tx):
            self._require_writer()
            fill = self._fill_by_key(dedupe_key)
            if fill is None:
                raise KeyError(f"unknown fill {dedupe_key}")
            allowed = {(r["grid_id"], r["cell_id"], r["generation"]): parse_decimal(r["amount"])
                       for r in self._x("SELECT * FROM allocations WHERE cid = ?", (fill.cid,)).fetchall()}
            if not allowed:
                raise InvalidTransitionError(f"CID {fill.cid} has no allocations")
            normalized = sorted((g, c, n, Decimal(canonical_decimal(a))) for g, c, n, a in parts)
            existing = sorted((r["grid_id"], r["cell_id"], r["generation"], parse_decimal(r["amount"]))
                              for r in self._x("SELECT * FROM fill_allocations WHERE dedupe_key = ?", (dedupe_key,)))
            if existing:
                if existing == normalized:
                    return
                raise InvalidTransitionError(f"fill {dedupe_key} already allocated differently")
            if sum((a for *_, a in normalized), Decimal(0)) != fill.size or any(a <= 0 for *_, a in normalized):
                raise ValueError("allocation parts must be positive and sum to the fill size")
            if len({(g, c, n) for g, c, n, _ in normalized}) != len(normalized):
                raise ValueError("allocation parts must name distinct cycles")
            for grid_id, cell_id, generation, amount in normalized:  # validate every part before the first write
                cap = allowed.get((grid_id, cell_id, generation))
                if cap is None:
                    raise InvalidTransitionError(f"cycle {grid_id}/{cell_id}/{generation} is not allocated to CID "
                                                 f"{fill.cid}")
                used = sum((parse_decimal(r[0]) for r in self._x(
                    "SELECT fa.amount FROM fill_allocations fa JOIN fills f ON f.dedupe_key = fa.dedupe_key "
                    "WHERE f.cid = ? AND fa.grid_id = ? AND fa.cell_id = ? AND fa.generation = ?",
                    (fill.cid, grid_id, cell_id, generation))), Decimal(0))
                if used + amount > cap:
                    raise InvalidTransitionError(f"allocation to {grid_id}/{cell_id}/{generation} exceeds {cap}")
            for grid_id, cell_id, generation, amount in normalized:
                self._x("INSERT INTO fill_allocations(dedupe_key, grid_id, cell_id, generation, amount) "
                        "VALUES (?, ?, ?, ?, ?)", (dedupe_key, grid_id, cell_id, generation, canonical_decimal(amount)))
                self._credit_cycle(grid_id, cell_id, generation, fill.role, amount)

    @staticmethod
    def _fill(row: sqlite3.Row) -> FillRecord:
        return FillRecord(dedupe_key=row["dedupe_key"], trade_id_str=row["trade_id_str"], own_side=Side(row["own_side"]),
                          own_exchange_order_id=row["own_exchange_order_id"], cid=row["cid"], grid_id=row["grid_id"],
                          cell_id=row["cell_id"], generation=row["generation"], role=LegRole(row["role"]),
                          size=parse_decimal(row["size"]), price=parse_decimal(row["price"]),
                          timestamp_ms=row["timestamp_ms"], inbox_id=row["inbox_id"], late=bool(row["late"]))

    def _fill_by_key(self, key: str) -> Optional[FillRecord]:
        row = self._x("SELECT * FROM fills WHERE dedupe_key = ?", (key,)).fetchone()
        return None if row is None else self._fill(row)

    def fills(self, cid: Optional[int] = None) -> List[FillRecord]:
        with self._rlock:
            self._require_open()
            if cid is None:
                rows = self._x("SELECT * FROM fills ORDER BY timestamp_ms, dedupe_key").fetchall()
            else:
                rows = self._x("SELECT * FROM fills WHERE cid = ? ORDER BY timestamp_ms, dedupe_key", (cid,)).fetchall()
        return [self._fill(row) for row in rows]

    def unallocated_fills(self) -> List[FillRecord]:
        with self._rlock:
            self._require_open()
            rows = self._x("""SELECT f.* FROM fills f WHERE EXISTS (SELECT 1 FROM allocations a WHERE a.cid = f.cid)
                              AND NOT EXISTS (SELECT 1 FROM fill_allocations fa WHERE fa.dedupe_key = f.dedupe_key)
                              ORDER BY f.timestamp_ms""").fetchall()
        return [self._fill(row) for row in rows]

    @staticmethod
    def _cursor(row: sqlite3.Row) -> CursorRecord:
        return CursorRecord(stream=row["stream"], cursor=row["cursor"], high_water=row["high_water"],
                            high_water_ts_ms=row["high_water_ts_ms"],
                            required_boundary_ts_ms=row["required_boundary_ts_ms"],
                            required_boundary_marker=row["required_boundary_marker"],
                            oldest_available_ts_ms=row["oldest_available_ts_ms"], complete=bool(row["complete"]),
                            incomplete_reason=row["incomplete_reason"], last_full_scan_ms=row["last_full_scan_ms"],
                            revision=row["revision"], updated_at_ms=row["updated_at_ms"],
                            retention_gap_open=bool(row["retention_gap_open"]))

    def cursor(self, stream: str) -> CursorRecord:
        with self._rlock:
            self._require_open()
            row = self._x("SELECT * FROM cursors WHERE stream = ?", (stream,)).fetchone()
        if row is None:
            raise KeyError(f"unknown stream {stream}")
        return self._cursor(row)

    def cursors(self) -> Dict[str, CursorRecord]:
        return {stream: self.cursor(stream) for stream in STREAMS}

    def open_conflicts(self) -> List[HistoryConflictRecord]:
        with self._rlock:
            self._require_open()
            rows = self._x("SELECT * FROM history_conflicts WHERE resolved_at_ms IS NULL ORDER BY id").fetchall()
        return [self._conflict_record(row) for row in rows]

    def unmatched_evidence(self, include_resolved: bool = False) -> List[InboxRecord]:
        with self._rlock:
            self._require_open()
            rows = self._x("SELECT * FROM history_inbox WHERE status = 'UNMATCHED'"
                           + ("" if include_resolved else " AND resolved_at_ms IS NULL") + " ORDER BY id").fetchall()
        return [self._inbox(row) for row in rows]

    def inbox(self, *, status: Optional[str] = None, limit: int = 1000) -> List[InboxRecord]:
        with self._rlock:
            self._require_open()
            if status is None:
                rows = self._x("SELECT * FROM history_inbox ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            else:
                rows = self._x("SELECT * FROM history_inbox WHERE status = ? ORDER BY id DESC LIMIT ?",
                               (status, limit)).fetchall()
        return [self._inbox(row) for row in rows]

    def position_ledger(self) -> PositionLedger:
        """``P = B + confirmed buys - confirmed sells`` over all owned history fills (NG-RISK-002)."""
        engine = self.engine()
        if engine.effective_baseline is None:
            raise BootstrapError("engine is not bootstrapped")
        buys, sells = Decimal(0), Decimal(0)
        for fill in self.fills():
            if fill.own_side == Side.BUY:
                buys += fill.size
            else:
                sells += fill.size
        return PositionLedger(baseline=engine.effective_baseline, confirmed_buys=buys, confirmed_sells=sells)

    def verify_ledger(self) -> List[str]:
        """Recompute leg/cycle quantities from the append-only fills table; returns inconsistencies."""
        problems: List[str] = []
        credited: Dict[Tuple[str, int, int, str], Decimal] = {}
        for leg in self.legs():
            fills = self.fills(leg.cid)
            total = sum((f.size for f in fills), Decimal(0))
            if total != leg.filled:
                problems.append(f"leg {leg.cid}: filled {leg.filled} != sum(fills) {total}")
            with self._rlock:
                allocated = self._x("SELECT 1 FROM allocations WHERE cid = ?", (leg.cid,)).fetchone() is not None
            if not allocated:
                key = (leg.grid_id, leg.cell_id, leg.generation, leg.role.value)
                credited[key] = credited.get(key, Decimal(0)) + total
        with self._rlock:
            for row in self._x("SELECT fa.grid_id, fa.cell_id, fa.generation, f.role, fa.amount FROM fill_allocations "
                               "fa JOIN fills f ON f.dedupe_key = fa.dedupe_key").fetchall():
                key = (row[0], row[1], row[2], row[3])
                credited[key] = credited.get(key, Decimal(0)) + parse_decimal(row[4])
            cycles = self._cycles("1 = 1", ())
        for cycle in cycles:
            entry = credited.get((cycle.grid_id, cycle.cell_id, cycle.generation, "ENTRY"), Decimal(0))
            exit_ = credited.get((cycle.grid_id, cycle.cell_id, cycle.generation, "TP"), Decimal(0))
            if entry != cycle.entry_filled or exit_ != cycle.exit_filled:
                problems.append(f"cycle {cycle.grid_id}/{cycle.cell_id}/{cycle.generation}: stored E/X "
                                f"{cycle.entry_filled}/{cycle.exit_filled} != fills {entry}/{exit_}")
        return problems

    # ---------------------------------------------------------------------------------------------- operator audits

    def mark_manual_reconcile_required(self, tx: Optional[Transaction], reason: str, *, actor: str = "engine",
                                       evidence: Optional[Mapping[str, Any]] = None) -> None:
        _require_text(reason, "reason", 2000)
        with self._scope(tx):
            self._require_writer()
            self._x("UPDATE engine SET manual_reconcile_required = 1, manual_reconcile_reason = ?, updated_at_ms = ? "
                    "WHERE id = 1", (reason, self._clock_ms()))
            self._audit("manual_reconcile_required", actor, {"reason": reason, "evidence": dict(evidence or {})})

    def mark_retention_gap(self, tx: Optional[Transaction], stream: str, required_boundary_ts_ms: int,
                           oldest_available_ts_ms: int, *, actor: str = "engine") -> None:
        """The needed overlap boundary is older than the venue's retained history: no automatic proof exists
        (NG-DB-004, AC-53). Blocks new entries until an audited manual reconciliation."""
        _require_int(required_boundary_ts_ms, "required_boundary_ts_ms", 0)
        _require_int(oldest_available_ts_ms, "oldest_available_ts_ms", 0)
        with self._scope(tx) as scope:
            self._require_writer()
            self._apply_cursor_update(CursorUpdate(
                stream=stream, required_boundary_ts_ms=required_boundary_ts_ms,
                oldest_available_ts_ms=oldest_available_ts_ms, complete=False,
                incomplete_reason="retention gap: required boundary older than available history"))
            self.mark_manual_reconcile_required(
                scope, f"retention gap on {stream}: required boundary {required_boundary_ts_ms} < oldest available "
                       f"{oldest_available_ts_ms}", actor=actor,
                evidence={"stream": stream, "required_boundary_ts_ms": required_boundary_ts_ms,
                          "oldest_available_ts_ms": oldest_available_ts_ms})

    def record_manual_reconciliation(self, tx: Optional[Transaction], actor: str, reason: str,
                                     evidence: Mapping[str, Any], *,
                                     resolved_conflict_ids: Sequence[int] = (),
                                     resolved_inbox_ids: Sequence[int] = (),
                                     leg_resolutions: Optional[Mapping[int, OrderState]] = None,
                                     resolved_retention_gaps: Sequence[str] = (),
                                     clear_manual_reconcile: bool = True) -> int:
        """Audited operator reconciliation (NG-DB-004, AC-53/54). Resolves the listed conflicts/unmatched rows,
        leg states and retention gaps with the operator's evidence. A retention gap is cleared ONLY when its stream
        is named in ``resolved_retention_gaps`` (clearing the generic manual-reconcile flag does not lift it). It
        never resets the ledger, never changes the baseline, cells or fill quantities, and keeps every row it
        resolves. Every argument is validated before anything is written."""
        _require_text(actor, "actor")
        _require_text(reason, "reason", 2000)
        if not isinstance(evidence, Mapping) or not evidence:
            raise ValueError("manual reconciliation requires non-empty evidence")
        resolutions = {int(cid): _enum(OrderState, state, "leg resolution")
                       for cid, state in (leg_resolutions or {}).items()}
        gaps = list(resolved_retention_gaps)
        with self._scope(tx):
            self._require_writer()
            conflicts = []
            for conflict_id in resolved_conflict_ids:
                row = self._x("SELECT * FROM history_conflicts WHERE id = ?", (conflict_id,)).fetchone()
                if row is None:
                    raise KeyError(f"unknown conflict {conflict_id}")
                conflicts.append(row)
            for inbox_id in resolved_inbox_ids:
                if self._x("SELECT 1 FROM history_inbox WHERE id = ? AND resolved_at_ms IS NULL",
                           (inbox_id,)).fetchone() is None:
                    raise KeyError(f"inbox row {inbox_id} unknown or already resolved")
            legs = {}
            for cid, state in resolutions.items():
                leg = self.leg(cid)
                if leg is None:
                    raise KeyError(f"unknown leg {cid}")
                if leg.final and leg.state != state:
                    raise InvalidTransitionError(f"leg {cid} is final ({leg.state.value})")
                if state in _REJECTED_STATES and (leg.filled != 0 or self._fill_count(cid)):
                    raise InvalidTransitionError(f"leg {cid} has fills; cannot resolve as zero-fill")
                if state in FINAL_ORDER_STATES and self._unallocated_quantity(cid):
                    raise InvalidTransitionError(f"leg {cid} has unallocated aggregate fills; allocate them first")
                legs[cid] = leg
            for stream in gaps:
                if stream not in STREAMS:
                    raise ValueError(f"unknown stream {stream!r}")
                if not self.cursor(stream).retention_gap_open:
                    raise InvalidTransitionError(f"{stream} has no open retention gap")
            audit_id = self._audit("manual_reconcile", actor, {
                "reason": reason, "evidence": dict(evidence), "resolved_conflict_ids": list(resolved_conflict_ids),
                "resolved_inbox_ids": list(resolved_inbox_ids),
                "leg_resolutions": {str(k): v.value for k, v in resolutions.items()},
                "resolved_retention_gaps": gaps})
            now = self._clock_ms()
            tag = f"manual_reconcile:{audit_id}"
            for row in conflicts:
                self._x("UPDATE history_conflicts SET resolved_at_ms = ?, resolution = ? WHERE id = ? "
                        "AND resolved_at_ms IS NULL", (now, tag, row["id"]))
                if row["kind"] == "LATE_FILL" and row["cid"] is not None:
                    leg = self.leg(row["cid"])
                    self._x("UPDATE cycles SET late_evidence = 2 WHERE grid_id = ? AND cell_id = ? AND generation = ?",
                            (leg.grid_id, leg.cell_id, leg.generation))
            for inbox_id in resolved_inbox_ids:
                self._x("UPDATE history_inbox SET resolved_at_ms = ?, resolution = ? WHERE id = ?", (now, tag, inbox_id))
            for cid, state in resolutions.items():
                self._set_leg_state_raw(legs[cid], state, tag)
                self._x("UPDATE outbox SET status = 'DONE', outcome = COALESCE(outcome, 'UNKNOWN'), "
                        "outcome_detail = ?, result_at_ms = COALESCE(result_at_ms, ?) WHERE cid = ? AND status != 'DONE'",
                        (tag, now, cid))
            for stream in gaps:
                self._x("UPDATE cursors SET retention_gap_open = 0, revision = revision + 1, updated_at_ms = ? "
                        "WHERE stream = ?", (now, stream))
            if clear_manual_reconcile:
                self._x("UPDATE engine SET manual_reconcile_required = 0, manual_reconcile_reason = NULL, "
                        "reconciliation_revision = reconciliation_revision + 1, updated_at_ms = ? WHERE id = 1", (now,))
            return audit_id

    def record_baseline_audit(self, tx: Optional[Transaction], actor: str, reason: str, observed_position: Decimal, *,
                              new_baseline: Optional[Decimal] = None,
                              resolved_inbox_ids: Sequence[int] = ()) -> int:
        """Operator baseline audit after drift/manual trades (NG-RISK-003). The original bootstrap baseline is
        immutable; an explicit re-base is appended to ``baseline_adjustments``. Cell obligations are untouched and
        unmatched rows stay in the inbox (resolved with a reference to this audit)."""
        _require_text(actor, "actor")
        _require_text(reason, "reason", 2000)
        observed = canonical_decimal(observed_position, "observed_position")
        with self._scope(tx):
            self._require_writer()
            for inbox_id in resolved_inbox_ids:
                if self._x("SELECT 1 FROM history_inbox WHERE id = ? AND status = 'UNMATCHED' "
                           "AND resolved_at_ms IS NULL", (inbox_id,)).fetchone() is None:
                    raise KeyError(f"inbox row {inbox_id} is not an unresolved unmatched row")
            new_text = None if new_baseline is None else canonical_decimal(new_baseline, "new_baseline")
            ledger = self.position_ledger()
            payload = {"reason": reason, "observed_position": observed, "baseline": ledger.baseline,
                       "confirmed_buys": ledger.confirmed_buys, "confirmed_sells": ledger.confirmed_sells,
                       "ledger_net": ledger.net, "new_baseline": new_baseline,
                       "resolved_inbox_ids": list(resolved_inbox_ids)}
            audit_id = self._audit("baseline_audit", actor, payload)
            if new_text is not None:
                if Decimal(new_text) != ledger.baseline:
                    self._x("INSERT INTO baseline_adjustments(at_ms, actor, reason, old_baseline, new_baseline, "
                            "observed_position, audit_event_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (self._clock_ms(), actor, reason, canonical_decimal(ledger.baseline), new_text, observed,
                             audit_id))
            now = self._clock_ms()
            for inbox_id in resolved_inbox_ids:
                self._x("UPDATE history_inbox SET resolved_at_ms = ?, resolution = ? WHERE id = ?",
                        (now, f"baseline_audit:{audit_id}", inbox_id))
            self._x("UPDATE engine SET engine_revision = engine_revision + 1, updated_at_ms = ? WHERE id = 1", (now,))
            return audit_id

    @staticmethod
    def _audit_event(row: sqlite3.Row) -> AuditEvent:
        return AuditEvent(id=row["id"], at_ms=row["at_ms"], kind=row["kind"], actor=row["actor"],
                          engine_revision=row["engine_revision"], payload=_loads(row["payload_json"]))

    def audit_events(self, kind: Optional[str] = None, limit: int = 200) -> List[AuditEvent]:
        with self._rlock:
            self._require_open()
            if kind is None:
                rows = self._x("SELECT * FROM audit_events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            else:
                rows = self._x("SELECT * FROM audit_events WHERE kind = ? ORDER BY id DESC LIMIT ?",
                               (kind, limit)).fetchall()
        return [self._audit_event(row) for row in rows]

    # ---------------------------------------------------------------------------------------------- UI drill-down
    # Read-only and indexed; available on the writer, the command client and the read-only store. Ids are matched
    # as exact TEXT (never parsed through float); pages are keyset pages (``id < before_id`` ORDER BY id DESC), so
    # every row is reachable no matter how many exist.

    _MAX_PAGE = 1000

    @staticmethod
    def _id_text(id_str: Any, name: str) -> str:
        if not isinstance(id_str, str) or not id_str or id_str != id_str.strip() or len(id_str) > 128:
            raise TypeError(f"{name} must be a non-empty exact id string (never a number)")
        return id_str

    def _page_args(self, before_id: Optional[int], limit: int) -> Tuple[int, int]:
        _require_int(limit, "limit", 1)
        if limit > self._MAX_PAGE:
            raise ValueError(f"limit must be <= {self._MAX_PAGE}")
        if before_id is None:
            return (1 << 62), limit
        return _require_int(before_id, "before_id", 1), limit

    def find_orders_by_id(self, id_str: str) -> List[OrderMatch]:
        """Owned orders whose exchange order id, venue order index or client order id equals ``id_str`` exactly."""
        text = self._id_text(id_str, "id_str")
        hits: Dict[int, List[str]] = {}
        with self._rlock:
            self._require_open()
            for column, sql in (("exchange_order_id", "SELECT cid FROM orders WHERE exchange_order_id = ?"),
                                ("order_index", "SELECT cid FROM orders WHERE order_index = ?")):
                for row in self._x(sql, (text,)).fetchall():
                    hits.setdefault(row[0], []).append(column)
            if text.isdigit() and text == str(int(text)) and 0 < int(text) <= MAX_CLIENT_ORDER_ID:
                for row in self._x("SELECT cid FROM orders WHERE cid = ?", (int(text),)).fetchall():
                    hits.setdefault(row[0], []).append("client_order_id")
        return [OrderMatch(leg=self.leg(cid), order=self.order(cid), matched_on=tuple(columns))
                for cid, columns in sorted(hits.items())]

    def find_fills_by_trade_id(self, trade_id_str: str) -> List[FillRecord]:
        """Fills with exactly this trade id (a self-trade has two own legs, so up to two rows)."""
        text = self._id_text(trade_id_str, "trade_id_str")
        with self._rlock:
            self._require_open()
            rows = self._x("SELECT * FROM fills WHERE trade_id_str = ? ORDER BY own_side, dedupe_key",
                           (text,)).fetchall()
        return [self._fill(row) for row in rows]

    def commands_page(self, before_id: Optional[int] = None, limit: int = 100) -> List[CommandRecord]:
        """Commands newest first with ``id < before_id``; pass the last id of a page to get the next one."""
        before, limit = self._page_args(before_id, limit)
        with self._rlock:
            self._require_open()
            rows = self._x("SELECT * FROM commands WHERE id < ? ORDER BY id DESC LIMIT ?", (before, limit)).fetchall()
        return [self._command(row) for row in rows]

    def audit_page(self, before_id: Optional[int] = None, limit: int = 100) -> List[AuditEvent]:
        """Audit events newest first with ``id < before_id`` (keyset pagination)."""
        before, limit = self._page_args(before_id, limit)
        with self._rlock:
            self._require_open()
            rows = self._x("SELECT * FROM audit_events WHERE id < ? ORDER BY id DESC LIMIT ?",
                           (before, limit)).fetchall()
        return [self._audit_event(row) for row in rows]

    def state_transitions(self, entity: Optional[str] = None, key: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._rlock:
            self._require_open()
            clauses, params = [], []
            if entity is not None:
                clauses.append("entity = ?")
                params.append(entity)
            if key is not None:
                clauses.append("entity_key = ?")
                params.append(key)
            rows = self._x("SELECT * FROM state_transitions" + (" WHERE " + " AND ".join(clauses) if clauses else "")
                           + " ORDER BY id", params).fetchall()
        return [dict(row) for row in rows]

    # ---------------------------------------------------------------------------------------------- commands

    @staticmethod
    def _command(row: sqlite3.Row, duplicate: bool = False, mismatch: bool = False) -> CommandRecord:
        return CommandRecord(id=row["id"], idempotency_key=row["idempotency_key"], kind=row["kind"],
                             expected_config_revision=row["expected_config_revision"],
                             expected_engine_revision=row["expected_engine_revision"],
                             payload=_loads(row["payload_json"]), status=CommandStatus(row["status"]),
                             result=_loads(row["result_json"]), created_at_ms=row["created_at_ms"],
                             claimed_at_ms=row["claimed_at_ms"], claim_count=row["claim_count"],
                             applied_at_ms=row["applied_at_ms"], duplicate=duplicate, request_mismatch=mismatch)

    def enqueue_command(self, idempotency_key: str, kind: Union[CommandKind, str], expected_config_revision: int,
                        expected_engine_revision: int, payload: Optional[Mapping[str, Any]] = None, *,
                        tx: Optional[Transaction] = None) -> CommandRecord:
        """Durable, idempotent command (NG-UI-001/003). A repeated key returns the original row unchanged
        (``duplicate=True``; ``request_mismatch`` if the retry differs). Stale expected revisions produce a
        CONFLICT row -- never applied automatically. A second START while one is queued is a CONFLICT."""
        _require_text(idempotency_key, "idempotency_key", 200)
        kind_value = _enum(CommandKind, kind, "command kind").value
        _require_int(expected_config_revision, "expected_config_revision", 0)
        _require_int(expected_engine_revision, "expected_engine_revision", 0)
        payload_json = canonical_json(dict(payload or {}))
        with self._rlock:
            self._require_open()
            if self._mode == _READONLY:
                raise ReadOnlyStoreError("read-only store cannot enqueue commands")
            with self._scope(tx):
                row = self._x("SELECT * FROM commands WHERE idempotency_key = ?", (idempotency_key,)).fetchone()
                if row is not None:
                    mismatch = (row["kind"], row["expected_config_revision"], row["expected_engine_revision"],
                                row["payload_json"]) != (kind_value, expected_config_revision,
                                                         expected_engine_revision, payload_json)
                    return self._command(row, duplicate=True, mismatch=mismatch)
                engine = self._x("SELECT config_revision, engine_revision FROM engine WHERE id = 1").fetchone()
                status, result = CommandStatus.QUEUED.value, None
                if (expected_config_revision, expected_engine_revision) != (engine[0], engine[1]):
                    status = CommandStatus.CONFLICT.value
                    result = canonical_json(self._stale_result("stale_revision", engine))
                elif kind_value == CommandKind.START.value:
                    queued = self._x("SELECT id FROM commands WHERE kind = ? AND status = 'QUEUED' ORDER BY id LIMIT 1",
                                     (kind_value,)).fetchone()
                    if queued is not None:
                        status = CommandStatus.CONFLICT.value
                        result = canonical_json({"reason": "start_already_queued", "command_id": queued[0]})
                command_id = self._x("INSERT INTO commands(idempotency_key, kind, expected_config_revision, "
                                     "expected_engine_revision, payload_json, status, result_json, created_at_ms) "
                                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                     (idempotency_key, kind_value, expected_config_revision, expected_engine_revision,
                                      payload_json, status, result, self._clock_ms())).lastrowid
                self.fault_hooks.hit("before_command_commit")
                return self._command(self._x("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone())

    def _stale_result(self, reason: str, engine: sqlite3.Row) -> Dict[str, Any]:
        latest = self._x("SELECT last_version FROM snapshot_state WHERE id = 1").fetchone()
        return {"reason": reason, "current_config_revision": engine[0], "current_engine_revision": engine[1],
                "latest_snapshot_version": latest[0] if latest else None}

    def claim_next_command(self, tx: Transaction) -> Optional[CommandRecord]:
        """Oldest QUEUED command whose expected revisions still match; stale ones become CONFLICT here. Apply the
        command's effects and :meth:`complete_command` in the same transaction for exactly-once application."""
        self._check_tx(tx)
        self._require_writer()
        while True:
            row = self._x("SELECT * FROM commands WHERE status = 'QUEUED' ORDER BY id LIMIT 1").fetchone()
            if row is None:
                return None
            engine = self._x("SELECT config_revision, engine_revision FROM engine WHERE id = 1").fetchone()
            if (row["expected_config_revision"], row["expected_engine_revision"]) != (engine[0], engine[1]):
                self._x("UPDATE commands SET status = 'CONFLICT', result_json = ?, applied_at_ms = ? WHERE id = ?",
                        (canonical_json(self._stale_result("stale_at_apply", engine)), self._clock_ms(), row["id"]))
                continue
            self._x("UPDATE commands SET claimed_at_ms = ?, claim_count = claim_count + 1 WHERE id = ?",
                    (self._clock_ms(), row["id"]))
            return self._command(self._x("SELECT * FROM commands WHERE id = ?", (row["id"],)).fetchone())

    def complete_command(self, tx: Transaction, command_id: int, status: CommandStatus,
                         result: Optional[Mapping[str, Any]] = None) -> CommandRecord:
        self._check_tx(tx)
        self._require_writer()
        status = _enum(CommandStatus, status, "command status")
        if status == CommandStatus.QUEUED:
            raise ValueError("a command cannot be completed as QUEUED")
        result_json = canonical_json(dict(result or {}))
        row = self._x("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone()
        if row is None:
            raise KeyError(f"unknown command {command_id}")
        if row["status"] != CommandStatus.QUEUED.value:
            if row["status"] == status.value and row["result_json"] == result_json:
                return self._command(row)
            raise InvalidTransitionError(f"command {command_id} already {row['status']}")
        self._x("UPDATE commands SET status = ?, result_json = ?, applied_at_ms = ? WHERE id = ?",
                (status.value, result_json, self._clock_ms(), command_id))
        return self._command(self._x("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone())

    def get_command(self, command_id: Optional[int] = None, idempotency_key: Optional[str] = None) -> Optional[CommandRecord]:
        with self._rlock:
            self._require_open()
            if command_id is not None:
                row = self._x("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone()
            else:
                row = self._x("SELECT * FROM commands WHERE idempotency_key = ?", (idempotency_key,)).fetchone()
        return None if row is None else self._command(row)

    def list_commands(self, limit: int = 50, status: Optional[CommandStatus] = None) -> List[CommandRecord]:
        with self._rlock:
            self._require_open()
            if status is None:
                rows = self._x("SELECT * FROM commands ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            else:
                rows = self._x("SELECT * FROM commands WHERE status = ? ORDER BY id DESC LIMIT ?",
                               (CommandStatus(status).value, limit)).fetchall()
        return [self._command(row) for row in rows]

    # ---------------------------------------------------------------------------------------------- snapshots

    _SNAPSHOT_HEADER = ("snapshot_version", "config_revision", "engine_revision", "committed_at", "committed_at_ms",
                        "engine_state", "reasons")

    def write_snapshot(self, tx: Optional[Transaction], snapshot: Union[Snapshot, Mapping[str, Any], str], *,
                       keep_last: int = 500) -> StoredSnapshot:
        """Commit a versioned snapshot for UI/CLI readers. ``snapshot_version`` is assigned here and strictly
        increases (also across pruning). Revisions must equal the committed engine row; Decimals and ids are
        serialized as strings (ints beyond 2**53 too). Accepts a ``Snapshot``, a mapping or its JSON text."""
        if isinstance(snapshot, str):
            snapshot = json.loads(snapshot, parse_float=Decimal)
            if not isinstance(snapshot, Mapping):
                raise TypeError("snapshot JSON must be an object")
        if isinstance(snapshot, Snapshot):
            body = dict(snapshot.payload)
            state, reasons = snapshot.engine_state, list(snapshot.reasons)
            config_rev, engine_rev = snapshot.config_revision, snapshot.engine_revision
        elif isinstance(snapshot, Mapping):
            body = dict(snapshot)
            if "engine_state" not in body:
                raise ValueError("snapshot requires engine_state")
            state, reasons = body.pop("engine_state"), list(body.pop("reasons", []))
            config_rev, engine_rev = body.pop("config_revision", None), body.pop("engine_revision", None)
            for key in ("snapshot_version", "committed_at", "committed_at_ms"):
                body.pop(key, None)
        else:
            raise TypeError("snapshot must be Snapshot or mapping")
        state = _enum(EngineState, state, "engine state")
        reserved = set(self._SNAPSHOT_HEADER) & set(body)
        if reserved:
            raise ValueError(f"snapshot payload may not override header keys {sorted(reserved)}")
        _require_int(keep_last, "keep_last", 1)
        with self._scope(tx):
            self._require_writer()
            engine = self._x("SELECT config_revision, engine_revision FROM engine WHERE id = 1").fetchone()
            if (config_rev is not None and config_rev != engine[0]) or \
                    (engine_rev is not None and engine_rev != engine[1]):
                raise InvalidTransitionError(f"snapshot revisions ({config_rev}, {engine_rev}) differ from the "
                                             f"committed engine ({engine[0]}, {engine[1]})")
            version = self._x("SELECT last_version FROM snapshot_state WHERE id = 1").fetchone()[0] + 1
            now = self._clock_ms()
            document = {"snapshot_version": version, "config_revision": engine[0], "engine_revision": engine[1],
                        "committed_at": now / 1000, "committed_at_ms": now, "engine_state": state.value,
                        "reasons": reasons}
            document.update(body)
            text = canonical_json(document, allow_float=True)
            self._x("INSERT INTO snapshots(snapshot_version, config_revision, engine_revision, committed_at_ms, "
                    "engine_state, snapshot_json) VALUES (?, ?, ?, ?, ?, ?)",
                    (version, engine[0], engine[1], now, state.value, text))
            self._x("UPDATE snapshot_state SET last_version = ? WHERE id = 1", (version,))
            self._x("DELETE FROM snapshots WHERE snapshot_version <= ?", (version - keep_last,))
            self.fault_hooks.hit("before_snapshot_commit")
            return StoredSnapshot(snapshot_version=version, config_revision=engine[0], engine_revision=engine[1],
                                  committed_at_ms=now, engine_state=state, snapshot_json=text)

    @staticmethod
    def _snapshot(row: sqlite3.Row) -> StoredSnapshot:
        return StoredSnapshot(snapshot_version=row["snapshot_version"], config_revision=row["config_revision"],
                              engine_revision=row["engine_revision"], committed_at_ms=row["committed_at_ms"],
                              engine_state=EngineState(row["engine_state"]), snapshot_json=row["snapshot_json"])

    def latest_snapshot(self) -> Optional[StoredSnapshot]:
        """Latest committed snapshot (any mode; readers never see uncommitted state)."""
        with self._rlock:
            self._require_open()
            row = self._x("SELECT * FROM snapshots ORDER BY snapshot_version DESC LIMIT 1").fetchone()
        return None if row is None else self._snapshot(row)

    def snapshot(self, version: int) -> Optional[StoredSnapshot]:
        with self._rlock:
            self._require_open()
            row = self._x("SELECT * FROM snapshots WHERE snapshot_version = ?", (version,)).fetchone()
        return None if row is None else self._snapshot(row)

    # ---------------------------------------------------------------------------------------------- restart view

    def load_state(self) -> LedgerState:
        """Committed ledger view for restart (NG-DB-003 step 2). Rebuild cells/legs only from this and history."""
        engine = self.engine()
        grid = self.grid() if engine.current_grid_id else None
        cells = self.cells(grid.grid_id) if grid else []
        open_cycles = self.open_cycles()
        late_cycles = self.late_obligation_cycles()
        legs = [leg for cycle in open_cycles + late_cycles
                for leg in self.legs(grid_id=cycle.grid_id, cell_id=cycle.cell_id, generation=cycle.generation)]
        known = {leg.cid for leg in legs}
        legs.extend(leg for leg in self.legs(non_final_only=True) if leg.cid not in known)
        orders = {leg.cid: self.order(leg.cid) for leg in legs}
        with self._rlock:
            queued = self._x("SELECT count(*) FROM commands WHERE status = 'QUEUED'").fetchone()[0]
        return LedgerState(engine=engine, grid=grid, cells=cells, open_cycles=open_cycles, legs=legs, orders=orders,
                           unresolved_outbox=self.unresolved_outbox(), active_reservations=self.reservations(),
                           cursors=self.cursors(), open_conflicts=self.open_conflicts(),
                           unmatched=self.unmatched_evidence(),
                           position=self.position_ledger() if engine.bootstrapped else None,
                           entry_blockers=self.entry_blockers(), queued_commands=queued, late_cycles=late_cycles)


def _command_client_authorizer(action: int, arg1: Optional[str], arg2: Optional[str], db_name: Optional[str],
                               source: Optional[str]) -> int:
    if action == sqlite3.SQLITE_INSERT:
        return sqlite3.SQLITE_OK if arg1 in ("commands", "sqlite_sequence") else sqlite3.SQLITE_DENY
    if action == sqlite3.SQLITE_UPDATE:
        return sqlite3.SQLITE_OK if arg1 == "sqlite_sequence" else sqlite3.SQLITE_DENY
    if action in (sqlite3.SQLITE_DELETE, sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_DROP_TABLE,
                  sqlite3.SQLITE_ALTER_TABLE, sqlite3.SQLITE_CREATE_INDEX, sqlite3.SQLITE_DROP_INDEX,
                  sqlite3.SQLITE_CREATE_TRIGGER, sqlite3.SQLITE_DROP_TRIGGER, sqlite3.SQLITE_ATTACH,
                  sqlite3.SQLITE_DETACH, sqlite3.SQLITE_PRAGMA):
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def _split_sql(script: str) -> List[str]:
    """Split a migration script into statements (``sqlite3.complete_statement`` aware, handles triggers)."""
    statements, buffer = [], ""
    for line in script.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            if buffer.strip():
                statements.append(buffer.strip())
            buffer = ""
    if buffer.strip():
        raise SchemaVersionError("migration script has an incomplete trailing statement")
    return statements
