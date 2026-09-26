"""Persistent neutral fixed-cell grid engine (spec docs/superpowers/specs/2026-09-24-neutral-grid-spec.md).

One engine owns every cell of one grid on one account/market and runs inside ONE asyncio task (the
executor control loop, AC-04). A tick is strictly ordered:

1. drain the durable command queue (claim + effects + completion in ONE store transaction);
2. read account facts (book, rules, position, active orders) under a weight budget;
3. one bounded, resumable ``HistoryScanner`` step (cursor pagination of trades + inactive orders);
4. apply new history rows: inbox + dedupe keys + fill attribution + leg transitions + cursor/high-water in ONE
   store transaction (``NeutralGridStore.apply_history_batch``);
5. reconcile the active-order list (acceptance evidence only; disappearance is never terminal proof);
6. settle orders (exact terminal row + complete scan + cumulative equality + delay + repeat scans), refresh
   dust and release cells (NG-CELL-001);
7. drift detection and honest engine state/reasons;
8. plan (admission + router) and dispatch: intent+CID+reservation commit -> ``mark_dispatching`` commit ->
   transport -> result commit (NG-DB-002);
9. commit a versioned snapshot (the only thing UI and CLI read).

Durable truth is the WS-B store (legs, fills, cycles, outbox, cursors, commands). The WS-A ``CellLedger``
objects are an in-memory projection rebuilt from the store on load and mirrored after every store write.
Nothing here uses float for quantities or ids. WebSocket events only wake the scanner.
"""
from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import logging
import os
import re
import time
from collections import deque
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Sequence, Set, Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor import admission, grid, risk, router
from hummingbot.strategy_v2.executors.neutral_grid_executor.cells import (
    FINAL_STATES,
    CellLedger,
    Cycle,
    FillRecord as LedgerFill,
    LedgerError,
    Leg,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.commands import (
    AUDIT_ACTION_BASELINE,
    ALL_AUDIT_ACTIONS,
    CommandOutcome,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CellSpec,
    CellState,
    CommandKind,
    CommandStatus,
    EngineState,
    ExchangeOrderRow,
    ExchangePort,
    ExchangeTradeRow,
    GridConfig,
    LegRole,
    OrderState,
    OrderTypePolicy,
    PositionSnapshot,
    Side,
    SubmitRequest,
    TradingRules,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import (
    EngineMeta,
    EngineOptions,
    OrderMeta,
    grid_config_to_json,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.history import (
    ENDPOINT_ACCOUNT,
    ENDPOINT_ACTIVE_ORDERS,
    ENDPOINT_INACTIVE_ORDERS,
    ENDPOINT_TRADES,
    ENDPOINT_TRADING_RULES,
    STREAM_ORDERS,
    STREAM_TRADES,
    REASON_CONFLICT,
    AuditedResolution,
    HighWaterMark,
    HistoryScanner,
    ScanRecord,
    canonical_decimal,
    evaluate_terminal_release,
    order_dedupe_key as c_order_key,
    order_payload_fingerprint,
    trade_payload_fingerprint,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    STREAM_INACTIVE_ORDERS as B_ORDERS,
    STREAM_TRADES as B_TRADES,
    BootstrapRecord,
    GridExtension,
    GridMigration,
    CidAllocationError,
    CidCollisionError,
    CursorUpdate,
    EngineIdentity as StoreIdentity,
    EntryBlockedError,
    ExternalEntryEvidence,
    ExternalEntryRequest,
    ExternalSettlementCycle,
    ExternalSettlementEvidence,
    ExternalSettlementRequest,
    InvalidTransitionError,
    NeutralGridStore,
    PersistenceError,
    Reservation,
    StoreClosedError,
    StoreError,
    StoreIntegrityError,
    order_dedupe_key as b_order_key,
)

ZERO = Decimal("0")
LOGGER = logging.getLogger(__name__)
ACTOR = "engine"

# Engine-level operator-cleared blockers (the store keeps its own durable ones: conflicts, unmatched trades,
# manual_reconcile_required; both are shown).
FREEZE_RISK_BLOCKED = "RISK_BLOCKED"
FREEZE_INVARIANT = "LEDGER_INVARIANT"
FREEZE_CID = "CID_ALLOCATION"
FREEZE_CONFIG_MISMATCH = "CONFIG_MISMATCH"
# Engine-wide TP blockers. History conflicts and ledger-invariant freezes are scoped to the affected cells
# (``tp_blocked_cells``): TPs of exact cells and risk-reducing cancels keep flowing, new entries stop globally.
_FREEZES_BLOCKING_TP = {FREEZE_CID, FREEZE_CONFIG_MISMATCH}

CANCELLABLE = {OrderState.LIVE}
_LOOKBACK_STATES = {OrderState.SUBMIT_UNKNOWN, OrderState.TERMINAL_UNKNOWN, OrderState.CANCEL_PENDING,
                    OrderState.CANCEL_UNKNOWN}
_PREVIEW_ID = re.compile(r"[0-9a-f]{24}")
STOPPED_OUTCOMES = {EngineState.STOPPED.value, EngineState.STOPPED_WITH_INVENTORY.value}
_C_TO_B = {STREAM_TRADES: B_TRADES, STREAM_ORDERS: B_ORDERS}


def _ms(ts: float) -> int:
    return int(round(ts * 1000))


def _retry_only(cycle: Cycle) -> bool:
    """Every entry revision was proven unsent / definitively rejected with zero fill: the same cycle may retry
    with a new revision (NG-DB-005); it is neither an open exposure nor a completed cycle."""
    return (bool(cycle.entries) and not cycle.tps and cycle.E == 0
            and all(e.state in (OrderState.REJECTED_UNSENT, OrderState.REJECTED_ZERO_FILL) for e in cycle.entries))


# ------------------------------------------------------------------------------------------ row json helpers
def order_row_to_json(row: ExchangeOrderRow) -> Dict[str, Any]:
    return {
        "client_order_id": None if row.client_order_id is None else str(row.client_order_id),
        "client_order_id_str": row.client_order_id_str, "order_id": row.order_id, "order_index": row.order_index,
        "nonce": row.nonce, "account_index": row.account_index, "market_id": row.market_id,
        "side": row.side.value, "price": str(row.price), "initial_base_amount": str(row.initial_base_amount),
        "filled_base_amount": str(row.filled_base_amount), "remaining_base_amount": str(row.remaining_base_amount),
        "status": row.status, "reduce_only": row.reduce_only, "timestamp_ms": row.timestamp_ms,
        "raw_json": row.raw_json,
    }


def order_row_from_json(d: Dict[str, Any]) -> ExchangeOrderRow:
    return ExchangeOrderRow(
        client_order_id=None if d["client_order_id"] is None else int(d["client_order_id"]),
        client_order_id_str=d["client_order_id_str"], order_id=d["order_id"], order_index=d["order_index"],
        nonce=d["nonce"], account_index=int(d["account_index"]), market_id=int(d["market_id"]),
        side=Side(d["side"]), price=Decimal(str(d["price"])),
        initial_base_amount=Decimal(str(d["initial_base_amount"])),
        filled_base_amount=Decimal(str(d["filled_base_amount"])),
        remaining_base_amount=Decimal(str(d["remaining_base_amount"])), status=d["status"],
        reduce_only=bool(d["reduce_only"]), timestamp_ms=int(d["timestamp_ms"]), raw_json=d["raw_json"],
    )


_URL_QUERY = re.compile(r"(https?://[^\s'\"<>?]+)\?[^\s'\"<>]*", re.IGNORECASE)
_SECRET_VALUE = re.compile(
    r"(?i)\b(auth|token|access[_-]?token|api[_-]?key|x-api-key|secret|signature|sig|password|passphrase|"
    r"authorization|private[_-]?key)(\s*[=:]\s*)(bearer\s+)?[^\s&'\",;)}\]]+")
_LONG_HEX = re.compile(r"\b(0x)?[0-9a-fA-F]{40,}\b")


def redact(text: Any, limit: int = 500) -> str:
    """Operator-visible text never carries credentials (spec section 11, NG-UI-001): URL query strings are cut,
    auth/token/signature/key-like values and long hex secrets are replaced, the length is capped."""
    out = _URL_QUERY.sub(r"\1?<redacted>", str(text))
    out = _SECRET_VALUE.sub(lambda m: f"{m.group(1)}{m.group(2)}<redacted>", out)
    out = _LONG_HEX.sub("<hex redacted>", out)
    return out[:limit]


def _key_label(key: Tuple) -> str:
    """Exact, JSON-safe encoding of a history dedupe key (ints stay ints, however large)."""
    return json.dumps([["i", str(p)] if isinstance(p, int) and not isinstance(p, bool) else ["s", str(p)]
                       for p in key], separators=(",", ":"))


def _key_from_label(label: str) -> Tuple:
    return tuple(int(v) if t == "i" else v for t, v in json.loads(label))


def _history_row_summary(stream: str, row: Any) -> Dict[str, str]:
    """Operator-readable, string-only view of one history row version (M1 conflict set)."""
    def text(value: Any) -> str:
        if value is None:
            return ""
        return canonical_decimal(value) if isinstance(value, Decimal) else str(value)
    if stream == STREAM_TRADES:
        return {"trade_id": text(row.trade_id_str), "side": text(row.own_side.value), "size": text(row.size),
                "price": text(row.price), "exchange_order_id": text(row.own_exchange_order_id),
                "client_order_id": text(row.own_client_order_id), "is_maker": text(row.is_maker),
                "timestamp_ms": text(row.timestamp_ms)}
    return {"order_index": text(row.order_index), "order_id": text(row.order_id),
            "client_order_id": text(row.client_order_id), "status": text(row.status), "side": text(row.side.value),
            "price": text(row.price), "initial": text(row.initial_base_amount),
            "filled": text(row.filled_base_amount), "remaining": text(row.remaining_base_amount),
            "nonce": text(row.nonce), "timestamp_ms": text(row.timestamp_ms)}


_PUBLIC_PART = re.compile(r"[A-Za-z0-9_.\-]*\Z")


def public_conflict_key(stream: str, key: Tuple) -> str:
    """Canonical, web-safe key of a contradicted history row (M1 parity with WS-E, ``[A-Za-z0-9_:.-]{1,160}``):
    ``trade:<trade_id>:<own_side>:<own_exchange_order_id>`` / ``order:<exchange_order_id>``. Domain, account and
    market are constant for one engine (the scanner rejects rows outside them), so they are omitted. A part outside
    ``[A-Za-z0-9_.-]`` or an over-long key falls back to ``<kind>:sha:<32 hex of the exact typed key>``."""
    kind, parts = ("trade", key[3:]) if stream == STREAM_TRADES else ("order", key[3:])
    text = ":".join([kind] + [str(p) for p in parts])
    if len(text) <= 160 and all(_PUBLIC_PART.match(str(p)) for p in parts):
        return text
    return f"{kind}:sha:{_digest(_key_label(key))}"


def public_fingerprint(raw: str) -> str:
    """Web-safe version fingerprint: the first 32 hex of sha256 over the scanner's canonical payload fingerprint
    (``history.trade/order_payload_fingerprint``, UTF-8); the engine maps it back to the raw one."""
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def _digest(value: Any, n: int = 32) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
                          ).hexdigest()[:n]


def _atomic_write_json(path: str, data: Dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + f".tmp{os.getpid()}")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, target)
    try:
        dir_fd = os.open(str(target.parent), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


# ------------------------------------------------------------------------------------------ store opening
def transport_cids_of(store: NeutralGridStore) -> List[int]:
    """Durable CIDs that may have reached transport (see ``NeutralGridEngine.transport_cids``); works on a
    read-only handle too."""
    return sorted(lr.cid for lr in store.legs() if lr.state not in (OrderState.INTENT, OrderState.REJECTED_UNSENT))


def engine_store_identity(connector_name: str, trading_pair: str, port: ExchangePort) -> StoreIdentity:
    return StoreIdentity(connector_name=connector_name, connector_domain=port.domain,
                         account_index=port.account_index, trading_pair=trading_pair)


def open_engine_store(db_path: Optional[str], config: Any, port: ExchangePort, *, create_if_missing: bool = True,
                      lock_dir: Optional[str] = None, fault_hooks: Any = None,
                      clock: Callable[[], float] = time.time,
                      prior_run_markers: Optional[Callable] = None,
                      allow_grid_migration: bool = False) -> NeutralGridStore:
    """Open the single-writer store (fails closed on missing/corrupt DB with prior-run evidence, AC-54).

    A changed grid (dimensions/Q) is refused (``ConfigMutationError``, AC-52) unless the operator explicitly
    confirmed a migration: then the store opens without the fingerprint check, the engine stays frozen
    (CONFIG_MISMATCH) and only the audited ``migrate_grid`` command can replace the quiescent grid."""
    grid_config = config if isinstance(config, GridConfig) else config.to_grid_config(port.account_index)
    identity = engine_store_identity(grid_config.connector_name, grid_config.trading_pair, port)
    return NeutralGridStore.open(
        Path(db_path) if db_path else None, identity, create_if_missing=create_if_missing,
        prior_run_markers=prior_run_markers, lock_dir=lock_dir,
        config_fingerprint=None if allow_grid_migration else grid.config_fingerprint(grid_config),
        fault_hooks=fault_hooks, clock_ms=lambda: _ms(clock()))


def open_engine(config: GridConfig, db_path: Optional[str], port: ExchangePort, *,
                clock: Callable[[], float] = time.time, options: Optional[EngineOptions] = None,
                lock_dir: Optional[str] = None, fault_hooks: Any = None, create_if_missing: bool = True,
                offline_demo: bool = False, prior_run_markers: Optional[Callable] = None,
                allow_grid_migration: bool = False) -> "NeutralGridEngine":
    """Open store + engine. Any store refusal yields a fail-closed engine (never a fresh bootstrap)."""
    try:
        store = open_engine_store(db_path, config, port, create_if_missing=create_if_missing, lock_dir=lock_dir,
                                  fault_hooks=fault_hooks, clock=clock, prior_run_markers=prior_run_markers,
                                  allow_grid_migration=allow_grid_migration)
    except (StoreError, PersistenceError, OSError) as exc:
        return NeutralGridEngine(config, None, port, clock, options=options, offline_demo=offline_demo,
                                 fatal_reason=f"STORE_OPEN_REFUSED:{type(exc).__name__}: {exc}",
                                 health_path=_health_path(db_path, config, port))
    return NeutralGridEngine(config, store, port, clock, options=options, offline_demo=offline_demo)


def _health_path(db_path: Optional[str], config: GridConfig, port: ExchangePort) -> Optional[str]:
    """``<db_path>.health.json`` (the default database path when none is configured)."""
    try:
        if db_path:
            return str(db_path) + ".health.json"
        from hummingbot.strategy_v2.executors.neutral_grid_executor.store import default_db_path
        return str(default_db_path(engine_store_identity(config.connector_name, config.trading_pair, port))) \
            + ".health.json"
    except Exception:  # noqa: BLE001 - no sidecar rather than no engine
        return None


class _CursorView:
    """``history.HistoryCursorView`` over the engine's committed history caches."""

    def __init__(self, engine: "NeutralGridEngine"):
        self._engine = engine

    def high_water(self, stream: str) -> Optional[str]:
        if stream in self._engine.meta.history_reset:
            return None
        return self._engine.high_water.get(stream)

    def committed_payload(self, stream: str, key: Tuple) -> Optional[str]:
        return self._engine.committed.get(stream, {}).get(tuple(key))

    def oldest_unresolved_ms(self) -> Optional[int]:
        return self._engine.oldest_unresolved_ms()

    def bootstrap_floor_ms(self, stream: str) -> Optional[int]:
        meta = self._engine.meta
        return meta.history_reset.get(stream, meta.bootstrap_floor_ms)

    def audited_resolutions(self) -> List[AuditedResolution]:
        """Operator-audited payload verdicts (``ack_history_conflict``, persisted in engine_meta): the committed
        payload is accepted, the other versions seen at the audit are audited noise (C3)."""
        out = []
        for stream, entries in self._engine.meta.audited_payloads.items():
            for label, entry in entries.items():
                out.append(AuditedResolution(stream=stream, key=_key_from_label(label),
                                             accepted_fingerprint=entry.get("accepted"),
                                             rejected_fingerprints=frozenset(entry.get("noise", []))))
        return out


class NeutralGridEngine:
    """Single-task engine for all cells of one fixed grid (AC-04)."""

    def __init__(self, config: GridConfig, store: Optional[NeutralGridStore], port: ExchangePort,
                 clock: Callable[[], float] = time.time, scanner: Optional[HistoryScanner] = None, *,
                 options: Optional[EngineOptions] = None, offline_demo: bool = False,
                 fatal_reason: Optional[str] = None, health_path: Optional[str] = None):
        self.config = config
        self.port = port
        self.clock = clock
        self.options = options or EngineOptions()
        self.offline_demo = offline_demo
        self.store = store
        if type(config.directional_gross_limits) is not bool:
            raise ValueError("directional_gross_limits must be a bool")
        self.directional_gross_limits_active = False
        self.risk_policy_review_required = False
        self.max_abs_net_position_active = config.max_abs_net_position
        self.max_gross_position_active = config.max_gross_position
        self.limits = risk.RiskLimits(config.max_abs_net_position, config.max_gross_position, False)
        self.fingerprint = grid.config_fingerprint(config)
        self.fatal_reason = fatal_reason if (fatal_reason or store is not None) else "NO_STORE"
        # ---------------- durable model (projection of the store) ----------------
        self.meta = EngineMeta()
        self.b_engine = None
        self.grid_record = None
        self.cells: Dict[int, CellLedger] = {}
        self.order_meta: Dict[int, OrderMeta] = {}
        self.reservations: Dict[int, int] = {}
        self.persisted_cell_state: Dict[int, Tuple[str, Optional[str], int]] = {}
        self.high_water: Dict[str, Optional[str]] = {STREAM_TRADES: None, STREAM_ORDERS: None}
        self.cursor_ts: Dict[str, int] = {}
        self.committed: Dict[str, Dict[Tuple, str]] = {STREAM_TRADES: {}, STREAM_ORDERS: {}}
        self.trades_by_cid: Dict[int, List[ExchangeTradeRow]] = {}
        self.terminal_rows: Dict[int, ExchangeOrderRow] = {}
        self.store_entry_blockers: List[str] = []
        self.open_conflicts: List[Any] = []
        self.unmatched: List[Any] = []
        # ---------------- runtime (never trusted across restarts) ----------------
        self.rules: Optional[TradingRules] = None
        self.rules_at: Optional[float] = None
        self.bid: Optional[Decimal] = None
        self.ask: Optional[Decimal] = None
        self.position: Optional[PositionSnapshot] = None
        self.position_at: Optional[float] = None
        self.active_rows: Optional[List[ExchangeOrderRow]] = None
        self.active_at: Optional[float] = None
        self.errors: Deque[Dict[str, Any]] = deque(maxlen=50)
        self.recent_commands: Deque[Dict[str, Any]] = deque(maxlen=20)
        self.persistence_error: Optional[str] = None
        self.reload_needed = False
        self.history_complete = False
        self.history_incomplete_reason: Optional[str] = "no complete scan yet"
        self.last_complete_scan_at: Optional[float] = None
        self.last_history_commit_at: Optional[float] = None
        self.last_scan_pages: Dict[str, int] = {}
        # Complete walks (the scanner's own plus walks whose only conflicts were operator-audited and carry no new
        # evidence): input of the settlement predicate.
        self.complete_scans: Deque[ScanRecord] = deque(maxlen=64)
        self._walk_started_at: Optional[float] = None
        self._phase = "idle"
        self._walk_start_seq = 0
        self._last_walk_start_seq = 0                          # causal start of the latest complete walk
        self.weight_log: Deque[Tuple[float, int]] = deque()
        self.position_gap_since: Optional[float] = None
        self.unknown_active: List[ExchangeOrderRow] = []
        self.ws_pending: Dict[str, float] = {}
        self._ws_label_cid: Dict[str, Optional[int]] = {}
        self.last_ws_event_at: Optional[float] = None
        self.tp_latencies: Deque[Tuple[int, float]] = deque(maxlen=200)
        self.cell_blockers: Dict[int, str] = {}
        self.entry_blockers: List[str] = []
        self.tp_blockers: List[str] = []
        self.admission_plan: Optional[admission.AdmissionPlan] = None
        self.router_plan: Optional[router.RouterPlan] = None
        self.endpoints: Optional[risk.RiskEndpoints] = None
        self.margin_warning: Optional[str] = None
        self.margin_required: Optional[Decimal] = None
        self.position_reconciled = False
        self.startup_reconciled = False
        self.normal_since_start = False       # per process: an unknown order at (re)start blocks everything
        self.bootstrap_probe: Optional[Dict[str, Any]] = None
        self.bootstrap_rows_seen = 0
        self.engine_state = EngineState.BOOTSTRAPPING
        self.reasons: List[str] = []
        self.last_tick_at: Optional[float] = None
        self.ticks = 0
        self.transport_calls = 0
        self.last_snapshot: Optional[Dict[str, Any]] = None
        self._last_account_poll_at: Optional[float] = None
        self._active_reconciled_at: Optional[float] = None
        self._submits_this_tick = 0
        self._last_degraded_probe: Optional[float] = None
        self._active_evidence_cache: Dict[int, Tuple[Optional[str], Decimal]] = {}
        # Connector CID ownership: called once per CID after its leg is proven final (stop tracking only).
        self.leg_final_hook: Optional[Callable[[int], None]] = None
        self._released_cids: Set[int] = set()
        # Causal order of reads and commits inside and across ticks (REST reads are not atomic with each other):
        # a position/active list is only compared with a ledger whose fills were committed BEFORE it was requested.
        self._seq = 0
        self.position_seq = 0
        self.active_seq = 0
        self.fills_commit_seq = 0
        self.terminal_commit_seq: Dict[int, int] = {}
        self._cell_fill_commit_ms: Dict[int, int] = {}        # cell -> latest history commit with its new fills
        self._committed_trade_ids: Set[str] = set()
        self._rules_failures = 0
        self._rules_retry_at: Optional[float] = None
        self._rules_weight_log: Deque[Tuple[float, int]] = deque()
        self._hold_normal_until_tick = 0                       # anti-flap after DEGRADED (W4)
        self._sticky_freezes: Dict[str, str] = {}              # freezes to persist across a reload
        self._sticky_meta: Dict[str, Any] = {}                 # meta fields to persist across a reload
        self._invariant_cells_cache: Optional[Dict[int, str]] = None
        self.tp_blocked_cells: Dict[int, str] = {}
        self.unattributed_conflict: Optional[str] = None
        self.health_path: Optional[str] = health_path or (
            str(store.path) + ".health.json" if store is not None and getattr(store, "path", None) else None)
        self._health_written: Optional[Tuple[Optional[str], Optional[str]]] = None
        self._health_written_at: Optional[float] = None       # heartbeat: an older sidecar means "unknown" (W3)
        self.full_fingerprint = hashlib.sha256(json.dumps(
            {k: v for k, v in grid_config_to_json(config).items() if k != "enabled"}, sort_keys=True,
            default=str).encode()).hexdigest()[:32]           # everything a START acknowledges (E-09)
        self._position_reads: Deque[Tuple[int, float, Decimal]] = deque(maxlen=32)
        self.evidence_conflicts: Dict[int, str] = {}          # cell -> refused active-row evidence (D1-02)
        self.history_rows_commit_seq = 0                       # latest history commit with ANY new row (M4)
        # Restart with only cell-attributed history problems: exits and risk-reducing cancels of the unaffected
        # cells proceed, entries stay blocked globally, the affected cells stay blocked (M3).
        self.startup_scoped = False
        self.history_refused_cells: Dict[int, str] = {}       # cell -> its rows are in a refused history batch
        self.history_refused_unattributed: Optional[str] = None
        if self.fatal_reason is None:
            try:
                self._load()
            except (StoreError, PersistenceError) as exc:
                self.fatal_reason = f"STORE_LOAD_FAILED:{type(exc).__name__}: {exc}"
        self.scanner = scanner or HistoryScanner(
            port, _CursorView(self), overlap_s=config.history_overlap_s,
            weight_budget=self.options.scan_weight_budget, clock=clock,
            max_pages_per_call=self.options.max_scan_pages_per_tick,
            poll_interval_s=config.poll_interval_s,
            min_wake_interval_s=Decimal(str(self.options.min_wake_interval_s)),
            retention_horizon_s=(None if self.options.history_retention_horizon_s is None
                                 else Decimal(str(self.options.history_retention_horizon_s))),
        )
        if self.fatal_reason is not None:
            self.engine_state = EngineState.DEGRADED
            self.reasons = [self.fatal_reason]

    # ================================================================================== loading
    @property
    def grid_id(self) -> str:
        return self.grid_record.grid_id if self.grid_record is not None else self.config.grid_id

    @property
    def bootstrapped(self) -> bool:
        return self.b_engine is not None and self.b_engine.bootstrapped

    def _now_ms(self) -> int:
        return _ms(self.clock())

    def _load(self) -> None:
        s = self.store
        self.b_engine = s.engine()
        data = s.kv_get("engine_meta")
        if data is None:
            self.meta = EngineMeta(bootstrap_floor_ms=self._now_ms() - int(self.config.history_overlap_s * 1000))
            with s.transaction() as tx:
                s.kv_set(tx, "engine_meta", self.meta.to_json())
        else:
            self.meta = EngineMeta.from_json(data)
        self.cells, self.order_meta, self.trades_by_cid, self.terminal_rows = {}, {}, {}, {}
        self.reservations, self.persisted_cell_state = {}, {}
        self.committed = {STREAM_TRADES: {}, STREAM_ORDERS: {}}
        self.high_water = {STREAM_TRADES: None, STREAM_ORDERS: None}
        self.grid_record = None
        if not self.bootstrapped:
            return
        self.grid_record = s.grid()
        persisted_config = s.config_payload()
        persisted_policy = persisted_config.get("directional_gross_limits", False)
        if type(persisted_policy) is not bool:
            raise StoreIntegrityError("persisted directional_gross_limits must be a bool")
        persisted_limits = []
        for name in ("max_abs_net_position", "max_gross_position"):
            raw = persisted_config.get(name)
            if isinstance(raw, (bool, float)):
                raise StoreIntegrityError(f"persisted {name} must be an exact positive decimal")
            try:
                value = Decimal(str(raw))
            except ArithmeticError:
                raise StoreIntegrityError(f"persisted {name} must be an exact positive decimal") from None
            if not value.is_finite() or value <= ZERO:
                raise StoreIntegrityError(f"persisted {name} must be an exact positive decimal")
            persisted_limits.append(value)
        self.directional_gross_limits_active = persisted_policy
        self.max_abs_net_position_active, self.max_gross_position_active = persisted_limits
        self.risk_policy_review_required = persisted_policy != self.config.directional_gross_limits or \
            self.max_abs_net_position_active != self.config.max_abs_net_position or \
            self.max_gross_position_active != self.config.max_gross_position
        self.limits = risk.RiskLimits(self.max_abs_net_position_active, self.max_gross_position_active,
                                      persisted_policy)
        if self.grid_record.fingerprint != self.fingerprint:
            self.meta.freezes[FREEZE_CONFIG_MISMATCH] = "running grid dimensions/Q differ from config (AC-52)"
        self._rebuild_ledgers()
        cursors = s.cursors()
        for c_stream, b_stream in _C_TO_B.items():
            self.high_water[c_stream] = cursors[b_stream].high_water
            self.cursor_ts[c_stream] = cursors[b_stream].high_water_ts_ms or 0
        self._load_committed()
        self._refresh_store_facts()

    def _rebuild_ledgers(self) -> None:
        s = self.store
        grid_id = self.grid_record.grid_id
        q = self.grid_record.order_amount_base
        legs = s.legs(grid_id=grid_id)
        fills_by_cid: Dict[int, List[Any]] = {}
        for f in s.fills():
            fills_by_cid.setdefault(f.cid, []).append(f)
        for lr in legs:
            data = s.kv_get(f"om:{lr.cid}")
            self.order_meta[lr.cid] = OrderMeta.from_json(data) if data else OrderMeta(
                cid=lr.cid, cell_id=lr.cell_id, generation=lr.generation, role=lr.role.value,
                intent_ms=lr.created_at_ms)
        for cr in s.cells(grid_id):
            spec = cr.spec()
            ledger = CellLedger(grid_id, spec, q)
            # Genesis sentinel: the store numbers cycles from 1 (cells.generation 0 == no cycle yet).
            ledger.cycles = [Cycle(generation=0, entry_side=spec.entry_side, planned_qty=q, closed=True)]
            cell_legs = [lr for lr in legs if lr.cell_id == cr.cell_id]
            gens = sorted({lr.generation for lr in cell_legs} | ({cr.generation} if cr.generation > 0 else set()))
            for g in gens:
                cy = s.cycle(grid_id, cr.cell_id, g)
                cycle = Cycle(generation=g, entry_side=cy.entry_side, planned_qty=cy.planned_amount, dust=cy.dust,
                              closed=cy.state == "COMPLETE", late_evidence=cy.late_evidence == 1,
                              external_settled=cy.external_settled, external_entered=cy.external_entered)
                for lr in sorted((x for x in cell_legs if x.generation == g),
                                 key=lambda x: (x.role.value, x.revision)):
                    leg = self._leg_from_store(lr, fills_by_cid.get(lr.cid, []))
                    (cycle.entries if lr.role == LegRole.ENTRY else cycle.tps).append(leg)
                ledger.cycles.append(cycle)
            ledger._link()          # aggregate TPs: every cycle sees the shares other cycles' TP legs owe it (AC-39)
            for lr in cell_legs:
                for f in fills_by_cid.get(lr.cid, []):
                    ledger.fills[f.dedupe_key] = LedgerFill(identity=lr.identity, qty=f.size, price=f.price,
                                                            side=f.own_side)
            self.cells[cr.cell_id] = ledger
            self.reservations[cr.cell_id] = cr.reserved_slots
            self.persisted_cell_state[cr.cell_id] = (cr.state, cr.blocker, cr.reserved_slots)
        for cid, fs in fills_by_cid.items():
            self.trades_by_cid[cid] = [self._trade_from_fill(f) for f in fs]
        for cid, meta in self.order_meta.items():
            if meta.terminal_row:
                self.terminal_rows[cid] = order_row_from_json(meta.terminal_row)

    def _leg_from_store(self, lr, fills: Sequence[Any]) -> Leg:
        order = self.store.order(lr.cid)
        meta = self.order_meta.get(lr.cid)
        leg = Leg(identity=lr.identity, side=lr.side, price=lr.price, requested=lr.amount, state=lr.state,
                  cid=lr.cid, exchange_order_id=order.exchange_order_id if order else None, filled=lr.filled,
                  order_type=lr.order_type, expiry_ms=lr.expiry_ms, seq=meta.seq if meta else 0,
                  allocation=dict(lr.allocation) if lr.allocation else None)
        self._project_terminal(leg, order, meta, late=any(f.late for f in fills))
        return leg

    @staticmethod
    def _project_terminal(leg: Leg, order, meta: Optional[OrderMeta], *, late: bool) -> None:
        """Terminal cumulative and late-evidence flag of a leg. An operator audit of late evidence
        (``ack_late_evidence``) persists the audited cumulative: it supersedes a stale venue order row, a "rejected"
        leg that did execute is projected as TERMINAL with it (``CellLedger.acknowledge_late_evidence``), and only
        executions beyond it are unacknowledged late evidence."""
        audited = None if meta is None or meta.audited_cumulative is None else Decimal(meta.audited_cumulative)
        if audited is not None and leg.state in (OrderState.REJECTED_ZERO_FILL, OrderState.REJECTED_UNSENT):
            leg.state = OrderState.TERMINAL
        if leg.state == OrderState.TERMINAL:
            leg.terminal_cumulative = audited if audited is not None else (order.venue_filled if order else None)
        leg.late_evidence = late and (audited is None or leg.filled != audited)

    def _trade_from_fill(self, f) -> ExchangeTradeRow:
        market_id = self.b_engine.market_id if self.b_engine.market_id is not None else self.port.market_id
        return ExchangeTradeRow(
            trade_id_str=f.trade_id_str, account_index=self.b_engine.account_index, market_id=market_id,
            own_side=f.own_side, own_exchange_order_id=f.own_exchange_order_id, own_client_order_id=f.cid,
            size=f.size, price=f.price, is_maker=None, timestamp_ms=f.timestamp_ms, raw_json="{}")

    def _load_committed(self) -> None:
        """Recent committed inbox rows -> scanner keys/fingerprints (older rows are deduped by the store)."""
        domain = self.port.domain
        for rec in self.store.inbox(limit=self.options.committed_cache_rows):
            try:
                p = dict(rec.payload)
                if rec.stream == B_TRADES:
                    row = ExchangeTradeRow(
                        trade_id_str=str(p["trade_id_str"]), account_index=int(p["account_index"]),
                        market_id=int(p["market_id"]), own_side=Side(p["own_side"]),
                        own_exchange_order_id=p["own_exchange_order_id"],
                        own_client_order_id=None if p["own_client_order_id"] is None
                        else int(p["own_client_order_id"]),
                        size=Decimal(str(p["size"])), price=Decimal(str(p["price"])), is_maker=p["is_maker"],
                        timestamp_ms=int(p["timestamp_ms"]), raw_json=rec.raw_json)
                    self.committed[STREAM_TRADES][row.dedupe_key(domain)] = trade_payload_fingerprint(row)
                    self._committed_trade_ids.add(row.trade_id_str)
                else:
                    p["raw_json"] = rec.raw_json
                    p["client_order_id"] = None if p["client_order_id"] is None else str(p["client_order_id"])
                    row = order_row_from_json(p)
                    self.committed[STREAM_ORDERS][c_order_key(domain, row)] = order_payload_fingerprint(row)
            except (KeyError, TypeError, ValueError, ArithmeticError):
                continue

    def _refresh_store_facts(self) -> None:
        s = self.store
        self.b_engine = s.engine()
        self.store_entry_blockers = s.entry_blockers()
        self.open_conflicts = s.open_conflicts()
        self.unmatched = s.unmatched_evidence()

    def _reload(self) -> None:
        """After a failed transaction memory may be ahead of disk: rebuild everything from the store. A freeze raised
        by the failure itself (LEDGER_INVARIANT) is re-applied and persisted: only an operator ack clears it."""
        self._load()
        self.reload_needed = False
        self._active_evidence_cache = {}
        self._active_reconciled_at = None
        self._invariant_cells_cache = None
        if self._sticky_freezes or self._sticky_meta:
            for code, detail in self._sticky_freezes.items():
                self.meta.freezes.setdefault(code, detail)
            for name, value in self._sticky_meta.items():
                setattr(self.meta, name, value)
            with self.store.transaction() as tx:
                self.store.kv_set(tx, "engine_meta", self.meta.to_json())
            self._sticky_freezes = {}
            self._sticky_meta = {}

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    # ================================================================================== lookups / mirror
    def leg_by_cid(self, cid: int) -> Optional[Leg]:
        meta = self.order_meta.get(cid)
        if meta is None:
            return None
        ledger = self.cells.get(meta.cell_id)
        if ledger is None:
            return None
        for cycle in ledger.cycles:
            if cycle.generation != meta.generation:
                continue
            for leg in cycle.legs:
                if leg.cid == cid:
                    return leg
        return None

    def _mirror(self, cid: int) -> None:
        """Project the store's durable leg/order/cycle rows onto the in-memory ledger (store is authoritative)."""
        leg = self.leg_by_cid(cid)
        lr = self.store.leg(cid)
        if leg is None or lr is None:
            return
        order = self.store.order(cid)
        leg.state = lr.state
        leg.filled = lr.filled
        if order is not None and order.exchange_order_id:
            leg.exchange_order_id = order.exchange_order_id
        self._project_terminal(leg, order, self.order_meta.get(cid), late=any(f.late for f in self.store.fills(cid)))
        self._mirror_cycle(leg.identity.cell_id, leg.identity.generation)
        self._notify_final(leg)

    def grid_mutation_blockers(self) -> List[str]:
        """Why the current grid cannot be migrated now (published for the web's migrate gate; the engine re-checks
        at apply time). Only a durably stopped engine or one opened for a confirmed migration queries the store;
        a running grid is never quiescent."""
        if self.store is None or self.store.closed or not self.bootstrapped:
            return ["NOT_BOOTSTRAPPED"]
        if not self.is_stopped and FREEZE_CONFIG_MISMATCH not in self.meta.freezes:
            return ["ENGINE_NOT_STOPPED: stop the grid (STOPPED / STOPPED_WITH_INVENTORY) first"]
        try:
            return [redact(b, 300) for b in self.store.grid_mutation_blockers()]
        except (StoreError, PersistenceError) as exc:
            return [redact(f"UNKNOWN: {type(exc).__name__}", 300)]

    def active_cell_ids(self) -> List[int]:
        """Effective price order. Historical ledgers stay loaded even when a future window excludes them."""
        if self.store is None or self.store.closed or not self.bootstrapped:
            return sorted(self.cells)
        return [cell.cell_id for cell in self.store.active_cells(self.grid_id)]

    def transport_cids(self) -> List[int]:
        """Every durable CID whose submit may have reached transport: all legs except a never-dispatched INTENT
        (PENDING outbox, dispatched later with the same CID) and a proven-unsent REJECTED_UNSENT. The connector
        must treat them as engine-owned (``register_history_reconciled_order``) before its polling starts."""
        if self.store is None or self.store.closed:
            return []
        return transport_cids_of(self.store)

    def release_final_cids(self) -> None:
        """Stop connector tracking of every leg already proven final (after a restart)."""
        for leg in self.all_legs():
            self._notify_final(leg)

    def _notify_final(self, leg: Leg) -> None:
        if self.leg_final_hook is None or leg.cid is None or not leg.is_final or leg.cid in self._released_cids:
            return
        self._released_cids.add(leg.cid)
        try:
            self.leg_final_hook(leg.cid)
        except Exception:  # noqa: BLE001 - tracking hygiene only, never a ledger fact
            LOGGER.exception("neutral grid: releasing connector tracking of CID %s failed", leg.cid)

    def _mirror_cycle(self, cell_id: int, generation: int) -> None:
        cy = self.store.cycle(self.grid_id, cell_id, generation)
        for cycle in self.cells[cell_id].cycles:
            if cycle.generation == generation:
                cycle.dust = cy.dust
                cycle.closed = cy.state == "COMPLETE"
                cycle.late_evidence = cy.late_evidence == 1
                return

    def all_legs(self) -> List[Leg]:
        return [leg for ledger in self.cells.values() for leg in ledger.legs()]

    def non_final_legs(self) -> List[Leg]:
        return [leg for ledger in self.cells.values() for leg in ledger.non_final_legs()]

    def oldest_unresolved_ms(self) -> Optional[int]:
        """History lookback for legs that can have old-dated evidence still missing (unknown submit/terminal,
        cancel in flight, terminal row awaiting settlement). A proven-LIVE resting order produces only new-dated
        rows, which the overlap from the high-water covers, so it never forces a walk back to its intent."""
        times = [self.order_meta[leg.cid].intent_ms for leg in self.non_final_legs()
                 if leg.cid in self.order_meta and (leg.state in _LOOKBACK_STATES or leg.cid in self.terminal_rows)]
        return min(times) if times else None

    @property
    def baseline(self) -> Optional[Decimal]:
        return None if self.b_engine is None else self.b_engine.initial_baseline

    @property
    def effective_baseline(self) -> Optional[Decimal]:
        return None if self.b_engine is None else self.b_engine.effective_baseline

    @property
    def mid(self) -> Optional[Decimal]:
        if self.bid is None or self.ask is None:
            return None
        return (self.bid + self.ask) / 2

    @property
    def state(self) -> EngineState:
        return self.engine_state

    @property
    def is_stopped(self) -> bool:
        return self.meta.stop_outcome in STOPPED_OUTCOMES

    # ================================================================================== public API
    def wake(self, event: Optional[Dict[str, Any]] = None) -> None:
        """Private WS signal: scan history soon (coalesced). Never a fill/terminal proof (NG-HIST-001).

        Only executions and terminal/cancel events need authoritative history; an acceptance ack of our own
        order does not, so it does not spend request weight (NG-HIST-003).
        """
        now = self.clock()
        event = event or {}
        status = str(event.get("status") or "")
        if event.get("type") == "order" and status in ("open", "pending", "in-progress"):
            return
        self.last_ws_event_at = now
        self.scanner.wake()
        if event.get("type") == "trade":
            trade_id = event.get("trade_id")
            if trade_id is not None and str(trade_id) in self._committed_trade_ids:
                return                                  # already in committed history: no lag (AC-13)
            cid = self._event_cid(event.get("client_order_id"))
            if cid is not None and self._leg_settled(cid):
                return                                  # replay for an order whose cumulative is already proven
            label = trade_id if trade_id is not None else event.get("client_order_id")
            if label is not None:
                self.ws_pending.setdefault(str(label), now)
                self._ws_label_cid[str(label)] = cid

    def _event_cid(self, value: Any) -> Optional[int]:
        text = str(value) if value is not None else ""
        return int(text) if text.isdigit() and int(text) in self.order_meta else None

    def _leg_settled(self, cid: int) -> bool:
        """Final with a proven cumulative. A zero-fill resolution (venue reject or an audited "never landed") is final
        too, but any execution signal for it is new evidence, never a replay: its label stays pending until history
        commits it (H1)."""
        leg = self.leg_by_cid(cid)
        return leg is not None and leg.state in FINAL_STATES and leg.state != OrderState.REJECTED_ZERO_FILL

    def history_lag_s(self, now: Optional[float] = None) -> float:
        """Age of the oldest WS trade signal not yet reflected in committed history (AC-13)."""
        now = self.clock() if now is None else now
        if not self.ws_pending:
            return 0.0
        return max(0.0, now - min(self.ws_pending.values()))

    async def run_forever(self, stop_event: Optional[asyncio.Event] = None) -> None:
        """Offline demo / standalone loop. Inside Hummingbot the executor control loop calls ``tick``."""
        while stop_event is None or not stop_event.is_set():
            await self.tick()
            await asyncio.sleep(self.options.tick_interval_s)

    async def tick(self) -> None:
        now = self.clock()
        self.ticks += 1
        self._submits_this_tick = 0
        self.last_tick_at = now
        try:
            await self._tick_body(now)
        finally:
            self._write_health()

    async def _tick_body(self, now: float) -> None:
        if self.fatal_reason is not None:
            self._evaluate_state(now)
            return
        try:
            if self.store.is_degraded or self.persistence_error is not None:
                if not self._probe_persistence(now):
                    await self._refresh_account(now)
                    self._evaluate_state(now)
                    return
            self._phase = "reload"
            if self.reload_needed:
                self._reload()
            self._phase = "commands"
            self._process_commands(now)
            self._phase = "reads"
            await self._refresh_account(now)
            await self._scan_history(now)
            await self._reread_position_for_release(now)
            self._reconcile_active(now)
            self._settle(now)
            self._detect_drift(now)
            self._evaluate_state(now)
            self._phase = "act"
            await self._act(now)
            self._phase = "finish"
            self._finish_stop(now)
            self._evaluate_state(now)
            self._commit_snapshot(now)
        except PersistenceError as exc:
            self._persistence_failed(exc, now)
        except StoreClosedError:
            raise
        except (StoreError, LedgerError) as exc:
            LOGGER.exception("neutral grid store/ledger refusal")
            detail = redact(f"{type(exc).__name__}: {exc}", 1000)
            self._error("STORE_REFUSED", detail, now)
            self.meta.freezes.setdefault(FREEZE_INVARIANT, detail)
            self._sticky_freezes.setdefault(FREEZE_INVARIANT, detail)   # survives the reload, persisted there
            self._persist_freeze(FREEZE_INVARIANT, detail)             # ... and now, in its own transaction
            self.reload_needed = True
            self._evaluate_state(now)
            await self._recover_after_refusal(now, self._phase)

    async def _recover_after_refusal(self, now: float, phase: str) -> None:
        """A refusal that recurs every tick must not withhold every exit or risk-reducing cancel, nor leave the UI on
        the last committed NORMAL (D1-02/D2-04): rebuild from the store, keep acting (TPs of exact cells, cancels;
        new entries stay blocked by the freeze) unless the refusal came from acting itself, and commit the FROZEN
        state and snapshot in their own transactions."""
        try:
            self._reload()
            self._evaluate_state(now)
            if phase not in ("act", "finish"):
                await self._act(now)
            self._finish_stop(now)
            self._evaluate_state(now)
            self._commit_snapshot(now)
        except PersistenceError as exc:
            self._persistence_failed(exc, now)
        except StoreClosedError:
            raise
        except (StoreError, LedgerError) as exc:
            LOGGER.exception("neutral grid: recovery after a store/ledger refusal failed")
            self._error("STORE_REFUSED", redact(f"{type(exc).__name__}: {exc}"), now)
            self.reload_needed = True
            self._evaluate_state(now)
            try:
                with self.store.transaction() as tx:
                    self.store.set_engine_state(tx, self.engine_state, redact("; ".join(self.reasons), 1900) or None)
            except (StoreError, PersistenceError):
                LOGGER.exception("neutral grid: could not commit the engine state after a refusal")

    def _persist_freeze(self, code: str, detail: str) -> None:
        """Durably add one freeze to the stored engine meta in its own transaction (the in-memory meta may be ahead
        of the rolled-back work, so only this field is merged). A failing store keeps it sticky for the reload."""
        try:
            with self.store.transaction() as tx:
                stored = EngineMeta.from_json(self.store.kv_get("engine_meta") or {})
                stored.freezes.setdefault(code, detail)
                self.store.kv_set(tx, "engine_meta", stored.to_json())
        except (StoreError, PersistenceError, OSError):
            LOGGER.exception("neutral grid: could not persist the %s freeze now; kept for the reload", code)

    # ================================================================================== health sidecar (W3)
    def health(self) -> Dict[str, Any]:
        """In-process health for a host (same content as the ``<db_path>.health.json`` sidecar)."""
        return {"persistence_error": self.persistence_error, "fatal_reason": self.fatal_reason,
                "at": str(self.clock()),
                "engine_revision": self.b_engine.engine_revision if self.b_engine is not None else 0}

    def _write_health(self) -> None:
        """Atomic write+fsync+replace of the health sidecar whenever the persistence/fatal status changes: the UI
        cannot otherwise see a failing store, because no snapshot can commit (AC-55)."""
        status = (self.persistence_error, self.fatal_reason)
        now = self.clock()
        if self.health_path is None or (status == self._health_written and self._health_written_at is not None
                                        and now - self._health_written_at < self.options.health_heartbeat_s):
            return
        try:
            _atomic_write_json(self.health_path, self.health())
            self._health_written = status
            self._health_written_at = now
        except OSError:
            LOGGER.exception("neutral grid: health sidecar %s could not be written", self.health_path)

    def _probe_persistence(self, now: float) -> bool:
        """Degraded store: try an audited ``clear_degraded`` at most every 5 s; reload on success."""
        if self._last_degraded_probe is not None and now - self._last_degraded_probe < 5:
            return False
        self._last_degraded_probe = now
        try:
            self.store.clear_degraded(ACTOR, "storage probe: audit row committed")
        except (PersistenceError, StoreError) as exc:
            self.persistence_error = redact(exc)
            return False
        self.persistence_error = None
        self.reload_needed = True
        return True

    def _persistence_failed(self, exc: Exception, now: float) -> None:
        """NG-DB-004 / AC-55: nothing is sent without a committed intent; memory may be ahead of disk."""
        self.persistence_error = redact(exc)
        self.reload_needed = True
        self._error("PERSISTENCE_FAILURE", str(exc), now)
        self._evaluate_state(now)

    def _error(self, code: str, message: str, now: float) -> None:
        self.errors.append({"at": now, "code": code, "message": redact(message)})

    # ================================================================================== weights
    def _charge(self, now: float, weight: int) -> None:
        self.weight_log.append((now, weight))

    def weight_used(self, now: float) -> int:
        while self.weight_log and self.weight_log[0][0] <= now - 60:
            self.weight_log.popleft()
        return sum(w for _, w in self.weight_log)

    def _can_spend(self, now: float, weight: int) -> bool:
        return self.weight_used(now) + weight <= self.options.weight_budget_per_min

    # ================================================================================== commands
    def _process_commands(self, now: float) -> None:
        """Claim + effects + completion in ONE transaction. A store/ledger refusal inside a command rolls the whole
        transaction back (a store call that raised after writing poisons it: never catch-and-commit); the command
        is then completed as REJECTED in a fresh transaction and memory is rebuilt from the store."""
        s = self.store
        for _ in range(50):
            cmd = None
            try:
                with s.transaction() as tx:
                    cmd = s.claim_next_command(tx)
                    if cmd is None:
                        break
                    outcome = self._apply_command(cmd, now, tx)
                    s.complete_command(tx, cmd.id, outcome.status, outcome.result)
                    if outcome.status == CommandStatus.APPLIED:
                        s.bump_engine_revision(tx, f"command {cmd.kind}")
                    s.kv_set(tx, "engine_meta", self.meta.to_json())
                    if outcome.audit is not None:
                        s.record_audit(tx, outcome.audit[0], ACTOR, outcome.audit[1])
            except (PersistenceError, StoreClosedError):
                raise
            except (StoreError, LedgerError, ValueError, KeyError) as exc:
                if cmd is None:
                    raise
                self._reload()                   # memory (meta, ledgers) may be ahead of the rolled-back tx
                outcome = CommandOutcome(CommandStatus.REJECTED,
                                         {"error": "COMMAND_FAILED", "detail": f"{type(exc).__name__}: {exc}"})
                with s.transaction() as tx:
                    again = s.claim_next_command(tx)
                    if again is None or again.id != cmd.id:
                        raise StoreError(f"command {cmd.id} vanished after rollback") from exc
                    s.complete_command(tx, cmd.id, outcome.status, outcome.result)
            if outcome.reload:
                self._reload()
            self._refresh_store_facts()
            self.recent_commands.append({"id": cmd.id, "kind": cmd.kind, "status": outcome.status.value,
                                         "result": outcome.result})

    def _apply_command(self, cmd, now: float, tx) -> CommandOutcome:
        kind = cmd.kind
        payload = cmd.payload or {}
        if kind == CommandKind.CONFIRM_BASELINE.value:
            return self._cmd_confirm_baseline(payload, now, tx)
        if kind == CommandKind.START.value:
            if not (self.config.enabled or self.offline_demo):
                return CommandOutcome(CommandStatus.REJECTED, {"error": "CONFIG_DISABLED",
                                                               "detail": "enabled=false refuses live start"})
            preview_id = payload.get("preview_id")
            if payload.get("risk_acknowledged") is not True or not isinstance(preview_id, str) \
                    or not _PREVIEW_ID.fullmatch(preview_id):
                return CommandOutcome(CommandStatus.REJECTED, {
                    "error": "START_NOT_ACKNOWLEDGED",
                    "detail": "START needs risk_acknowledged=true and the preview_id of the viewed preview (AC-47)"})
            if "expected_initial_position" in payload:
                try:
                    typed = Decimal(str(payload["expected_initial_position"]))
                except ArithmeticError:
                    typed = None
                if typed is None or typed != self.config.expected_initial_position:
                    return CommandOutcome(CommandStatus.REJECTED, {
                        "error": "BASELINE_CONFIG_MISMATCH", "config": str(self.config.expected_initial_position),
                        "command": str(payload["expected_initial_position"])})
            latest_stop = self.meta.last_stop_applied_ms or self.meta.stop_requested_ms
            if self.meta.stop_requested_ms is not None and payload.get("source") == "launcher" \
                    and payload.get("resume_stop_ms") != latest_stop:
                # An automatic launcher START (every process start, new random key) never overrides a durable
                # STOP / STOPPING / STOP_UNCERTAIN: only an explicit operator START (web: key + expected revisions
                # of a viewed snapshot) or a launcher resume naming the LATEST applied STOP may (NG-OPS-003, AC-36).
                # A STOP applied after the operator typed the resume phrase (e.g. the Hummingbot stop while the old
                # drain is still STOPPING, which keeps stop_requested_ms) supersedes that phrase (H2).
                return CommandOutcome(CommandStatus.REJECTED, {
                    "error": "DURABLE_STOP_ACTIVE", "stop_requested_ms": self.meta.stop_requested_ms,
                    "latest_stop_ms": latest_stop, "stop_outcome": self.meta.stop_outcome,
                    "detail": "an explicit operator START or a launcher resume naming the latest stop is required"})
            policy_revision = None
            if self.risk_policy_review_required:
                if payload.get("source") == "launcher":
                    return CommandOutcome(CommandStatus.REJECTED, {
                        "error": "RISK_POLICY_REVIEW_REQUIRED",
                        "detail": "directional gross policy changes require an explicit reviewed operator START"})
                source_config = dict(self.store.config_payload())
                target_config = grid_config_to_json(self.config)
                source_compare, target_compare = dict(source_config), dict(target_config)
                source_compare.setdefault("directional_gross_limits", False)
                target_compare.setdefault("directional_gross_limits", False)
                changes = {name for name in set(source_compare) | set(target_compare)
                           if source_compare.get(name) != target_compare.get(name)}
                if changes != {"directional_gross_limits"}:
                    return CommandOutcome(CommandStatus.REJECTED, {
                        "error": "RISK_POLICY_CONFIG_CHANGE_FORBIDDEN", "changes": sorted(changes)})
                policy_revision = self.store.record_config_revision(
                    tx, target_config, self.fingerprint, "operator-reviewed-start",
                    "reviewed directional gross risk policy change")
            rebind = False
            if self.meta.started and self.meta.stop_requested_ms is None:
                if (self.bootstrapped and not self.risk_policy_review_required) or \
                        (not self.bootstrapped and self.meta.start_config_fingerprint == self.full_fingerprint):
                    return CommandOutcome(CommandStatus.APPLIED, {"already_started": True, "grid_id": self.grid_id,
                                                                  "engine_state": self.engine_state.value})
                # Before bootstrap a START for a changed config re-binds the acknowledgement to it (audited); the
                # baseline is then confirmable for exactly the config this START acknowledged (E-09, L1).
                rebind = True
            resumed = {"resumed_stop_ms": self.meta.stop_requested_ms,
                       "stop_outcome": self.meta.stop_outcome} if self.meta.stop_requested_ms is not None else {}
            if rebind:
                resumed["rebound_from"] = self.meta.start_config_fingerprint
            self.meta.started = True
            self.meta.start_preview_id = preview_id
            self.meta.start_config_fingerprint = self.full_fingerprint
            material = payload.get("material_id")
            self.meta.start_material_id = str(material)[:128] if material not in (None, "") else None
            self.meta.stop_requested_ms = None
            self.meta.stop_outcome = None
            self.meta.stop_reason = None
            result = {"started": True, "grid_id": self.grid_id}
            if policy_revision is not None:
                result["risk_policy_config_revision"] = policy_revision
            if rebind:
                result["rebound"] = True
            return CommandOutcome(CommandStatus.APPLIED, result,
                                  audit=("start", dict({"grid_id": self.grid_id, "preview_id": preview_id,
                                                        "risk_acknowledged": True,
                                                        "config_fingerprint": self.full_fingerprint,
                                                        "material_id": self.meta.start_material_id,
                                                        "source": payload.get("source") or "operator"},
                                                       **resumed)), reload=policy_revision is not None)
        if kind == CommandKind.PAUSE.value:
            self.meta.operator_paused = True
            self.meta.pause_reason = str(payload.get("reason") or "operator pause")
            return CommandOutcome(CommandStatus.APPLIED, {"paused": True},
                                  audit=("pause", {"reason": self.meta.pause_reason}))
        if kind == CommandKind.RESUME.value:
            return self._cmd_resume(now)
        if kind == CommandKind.STOP.value:
            if self.meta.stop_requested_ms is None or self.meta.stop_outcome is not None:
                self.meta.stop_requested_ms = _ms(now)
                self.meta.stop_outcome = None
                self.meta.stop_reason = str(payload.get("reason") or "operator stop")
            # A STOP while STOPPING keeps the drain's stop time, but it is the newest stop intent: a resume must name
            # it (a resume phrase typed before it is stale, H2).
            self.meta.last_stop_applied_ms = max(_ms(now), self.meta.stop_requested_ms)
            return CommandOutcome(CommandStatus.APPLIED, {"stopping": True},
                                  audit=("stop", {"reason": self.meta.stop_reason}))
        if kind == CommandKind.BASELINE_AUDIT.value:
            action = payload.get("action", AUDIT_ACTION_BASELINE)
            if action == AUDIT_ACTION_BASELINE:
                return self._cmd_baseline_audit(payload, now, tx)
            return self._cmd_reconcile(action, payload, now, tx)
        return CommandOutcome(CommandStatus.REJECTED, {"error": "UNKNOWN_COMMAND", "kind": kind})

    def _cmd_resume(self, now: float) -> CommandOutcome:
        gate = []
        if self.meta.stop_requested_ms is not None:
            gate.append("STOPPING")
        if not self.history_complete or self._history_stale(now):
            gate.append("HISTORY_NOT_FRESH")
        if self._position_stale(now):
            gate.append("POSITION_NOT_FRESH")
        if self.persistence_error:
            gate.append("PERSISTENCE_FAILURE")
        gate.extend(f"FROZEN:{code}" for code in sorted(self.meta.freezes))
        gate.extend(f"STORE:{b}" for b in self.store_entry_blockers)
        if self.endpoints is not None:
            gate.extend(risk.cap_violations(self.endpoints, self.limits))
        if gate:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "RESUME_GATE", "blockers": gate})
        self.meta.operator_paused = False
        self.meta.pause_reason = None
        return CommandOutcome(CommandStatus.APPLIED, {"resumed": True}, audit=("resume", {}))

    def bootstrap_ready(self, now: float) -> Tuple[bool, str]:
        """Stable snapshot + full history cut (NG-RISK-001, AC-45)."""
        if self.bootstrapped:
            return False, "already bootstrapped"
        if self.position is None or self._position_stale(now):
            return False, "position unknown or stale"
        if not self.history_complete or self._history_stale(now):
            return False, "history cut incomplete"
        if self.unknown_active:
            return False, "unknown active orders on market"
        if grid.rules_blockers(self.rules):
            return False, "trading rules unknown"
        if self.mid is None:
            return False, "order book unknown"
        probe = self.bootstrap_probe
        if probe is None or probe["scans"] < 2:
            return False, "waiting for two identical complete scans"
        if now - probe["since"] < float(self.config.settlement_delay_s):
            return False, "waiting for stability window"
        return True, "ready"

    def _cmd_confirm_baseline(self, payload: Dict[str, Any], now: float, tx) -> CommandOutcome:
        if not (self.config.enabled or self.offline_demo):
            return CommandOutcome(CommandStatus.REJECTED, {"error": "CONFIG_DISABLED",
                                                           "detail": "enabled=false refuses live start"})
        if not self.meta.started:
            return CommandOutcome(CommandStatus.REJECTED, {
                "error": "START_REQUIRED",
                "detail": "an acknowledged START (risk + preview) must be applied before the baseline confirmation"})
        if not self.bootstrapped and self.meta.start_config_fingerprint != self.full_fingerprint:
            # The config changed after the START that acknowledged it (e.g. restart with another Q/caps): the new
            # grid was never acknowledged, so a new START (preview + risk ack) is required (NG-UI-002, E-09).
            self.meta.started = False
            return CommandOutcome(CommandStatus.REJECTED, {
                "error": "START_CONFIG_CHANGED", "acknowledged": self.meta.start_config_fingerprint,
                "current": self.full_fingerprint,
                "detail": "the configuration changed after START; send a new START for the current preview"})
        if self.bootstrapped:
            return CommandOutcome(CommandStatus.REJECTED, {
                "error": "BASELINE_ALREADY_CONFIRMED", "baseline": str(self.baseline),
                "detail": "baseline is loaded from the ledger and never recaptured (NG-RISK-001)"})
        try:
            expected = Decimal(str(payload["expected_initial_position"]))
        except Exception:  # noqa: BLE001
            return CommandOutcome(CommandStatus.REJECTED, {"error": "BASELINE_PAYLOAD_INVALID"})
        if not expected.is_finite():
            return CommandOutcome(CommandStatus.REJECTED, {"error": "BASELINE_PAYLOAD_INVALID"})
        if self.config.expected_initial_position is None or expected != self.config.expected_initial_position:
            return CommandOutcome(CommandStatus.REJECTED, {
                "error": "BASELINE_CONFIG_MISMATCH", "config": str(self.config.expected_initial_position),
                "command": str(expected)})
        ready, why = self.bootstrap_ready(now)
        if not ready:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "BOOTSTRAP_NOT_READY", "detail": why})
        observed = self.position.net_base
        if observed != expected:
            return CommandOutcome(CommandStatus.REJECTED, {
                "error": "BASELINE_MISMATCH", "observed_position": str(observed), "expected": str(expected),
                "detail": "the bot never adopts the current position as baseline automatically"})
        errors = grid.validate_config(self.config, self.rules, self.mid, bootstrap=True)
        if errors:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "CONFIG_INVALID", "errors": errors})
        prices = grid.build_grid(self.config.lower_price, self.config.upper_price, self.config.cell_count, self.rules)
        anchor = grid.compute_anchor(self.mid, self.config.lower_price, self.config.upper_price)
        specs = grid.assign_cells(prices, anchor)
        trades_hw = self.high_water.get(STREAM_TRADES)
        orders_hw = self.high_water.get(STREAM_ORDERS)
        marks = [HighWaterMark.decode(h) for h in (trades_hw, orders_hw) if h is not None]
        cut_ts = max([m.timestamp_ms for m in marks] + [int(self.bootstrap_probe.get("scan_ms", 0))])
        record = BootstrapRecord(
            grid_id=self.config.grid_id, config_fingerprint=self.fingerprint,
            config=grid_config_to_json(self.config), lower_price=self.config.lower_price,
            upper_price=self.config.upper_price, order_amount_base=self.config.order_amount_base, prices=prices,
            cells=specs, anchor=anchor, baseline=expected, market_id=self.port.market_id, bootstrap_cut_ts_ms=cut_ts,
            actor=str(payload.get("actor") or "operator"),
            confirmation=f"operator confirmed signed baseline {expected} == observed {observed}",
            trades_cut=trades_hw, orders_cut=orders_hw)
        self.store.bootstrap(tx, record)
        buy = sum(1 for c in specs if c.entry_side == Side.BUY)
        return CommandOutcome(
            CommandStatus.APPLIED,
            {"baseline": str(expected), "anchor": str(anchor), "cells": len(specs), "buy_cells": buy,
             "sell_cells": len(specs) - buy, "cut_ts_ms": cut_ts}, reload=True)

    def _confirmed_fills(self) -> Tuple[Decimal, Decimal]:
        # The in-memory projection contains only the current grid after migration. Baseline audits reconcile the
        # whole account history, so use the durable all-grid ledger (owned fills plus real external executions).
        ledger = self.store.position_ledger()
        return ledger.confirmed_buys, ledger.confirmed_sells

    def external_close_candidate(self, now: Optional[float] = None) -> Dict[str, Any]:
        """Build the bounded one-cycle/one-order manual-close proof from committed facts only.

        The proof intentionally excludes polling and scan timestamps. Those are execution gates and audit
        evidence, so an unchanged semantic candidate keeps the same proof while the operator reviews it.
        """
        now = self.clock() if now is None else now
        blockers: List[str] = []
        if not self.bootstrapped:
            blockers.append("NOT_BOOTSTRAPPED")
        if not self.is_stopped or self.meta.stop_outcome not in ("STOPPED", "STOPPED_WITH_INVENTORY"):
            blockers.append("ENGINE_NOT_CLEANLY_STOPPED")
        if self.position is None or self._position_stale(now):
            blockers.append("POSITION_NOT_FRESH")
        elif self.position.net_base != ZERO:
            blockers.append(f"POSITION_NOT_FLAT:{self.position.net_base}")
        if self.active_rows is None or self.active_at is None \
                or now - self.active_at > float(self.config.history_freshness_s):
            blockers.append("ACTIVE_ORDERS_NOT_FRESH")
        elif self.active_seq < self.position_seq:
            blockers.append("ACTIVE_ORDERS_PREDATE_POSITION")
        elif self.active_rows:
            blockers.append(f"ACTIVE_ORDERS_PRESENT:{len(self.active_rows)}")
        if not self.history_complete or self._history_stale(now):
            blockers.append("HISTORY_NOT_FRESH_COMPLETE")
        if not self._position_settled_for_audit():
            blockers.append("STABLE_CUT_NOT_PROVEN")
        blockers.extend("COHERENT_CUT:" + reason for reason in self._audit_evidence_unsettled(now))
        if self.ws_pending:
            blockers.append(f"WS_EXECUTIONS_PENDING:{len(self.ws_pending)}")
        if self.open_conflicts:
            blockers.append(f"HISTORY_CONFLICTS:{len(self.open_conflicts)}")
        blockers.extend(f"FROZEN:{code}" for code in sorted(self.meta.freezes))
        if self.store is not None and not self.store.closed:
            if self.store.unresolved_outbox():
                blockers.append("OUTBOX_NOT_DONE")
            if self.store.reservations():
                blockers.append("ACTIVE_RESERVATIONS")
            gaps = [name for name, cursor in self.store.cursors().items() if cursor.retention_gap_open]
            if gaps:
                blockers.append("RETENTION_GAPS:" + ",".join(sorted(gaps)))
        if self.b_engine is not None and self.b_engine.manual_reconcile_required \
                and "drift" not in (self.b_engine.manual_reconcile_reason or ""):
            blockers.append("MANUAL_RECONCILIATION_BLOCKED")
        if any(not leg.is_final for leg in self.all_legs()):
            blockers.append("OWNED_LEGS_NOT_FINAL")
        if any(c.late_evidence for ledger in self.cells.values() for c in ledger.cycles):
            blockers.append("LATE_EVIDENCE")

        cycles = sorted(((ledger, cycle) for ledger in self.cells.values() for cycle in ledger.cycles
                         if cycle.generation > 0 and cycle.open_obligation > ZERO),
                        key=lambda item: (self.grid_id, item[0].cell_id, item[1].generation))
        if not cycles:
            blockers.append("OPEN_CYCLES_REQUIRED:0")
        expected_sides = {Side.SELL if cycle.entry_side == Side.BUY else Side.BUY for _, cycle in cycles}
        if len(expected_sides) > 1:
            blockers.append("MIXED_CYCLE_CLOSE_SIDES")
        if self.store is not None and not self.store.closed:
            for ledger, cycle in cycles:
                blockers.extend(f"CYCLE:{ledger.cell_id}/{cycle.generation}:" + b
                                for b in self.store.cycle_release_blockers(
                                    self.grid_id, ledger.cell_id, cycle.generation)
                                if not b.startswith("obligation open:"))

        trades = [r for r in self.unmatched if r.stream == B_TRADES]
        orders = [r for r in self.unmatched if r.stream == B_ORDERS]
        order_keys = {(str(r.payload.get("own_exchange_order_id") or ""),
                       str(r.payload.get("own_client_order_id") or "")) for r in trades}
        if not trades or len(order_keys) != 1:
            blockers.append(f"EXACTLY_ONE_MANUAL_ORDER_REQUIRED:{len(order_keys)}")
        side_values = {str(r.payload.get("own_side")) for r in trades}
        if len(side_values) != 1:
            blockers.append("MIXED_TRADE_SIDES")
        settlement_side = next(iter(side_values), "")
        expected_side = next(iter(expected_sides)).value if len(expected_sides) == 1 else None
        if expected_side is not None and settlement_side != expected_side:
            blockers.append(f"WRONG_SETTLEMENT_SIDE:{settlement_side}")

        matching_orders = []
        if len(order_keys) == 1:
            exchange_id, client_id = next(iter(order_keys))
            matching_orders = [r for r in orders
                               if str(r.payload.get("order_index") or r.payload.get("order_id") or "") == exchange_id
                               and str(r.payload.get("client_order_id") or "") == client_id]
        if len(matching_orders) != 1 or len(orders) != 1:
            blockers.append(f"EXACTLY_ONE_TERMINAL_ORDER_REQUIRED:{len(matching_orders)}")
        terminal = matching_orders[0] if len(matching_orders) == 1 else None
        quantity = sum((Decimal(str(r.payload.get("size", "0"))) for r in trades), ZERO)
        if terminal is not None:
            p = terminal.payload
            final = Decimal(str(p.get("remaining_base_amount", "-1"))) == ZERO
            if not p.get("reduce_only") or not final:
                blockers.append("ORDER_NOT_FINAL_REDUCE_ONLY")
            if str(p.get("side")) != settlement_side:
                blockers.append("ORDER_SIDE_MISMATCH")
            if Decimal(str(p.get("filled_base_amount", "-1"))) != quantity:
                blockers.append("ORDER_TRADE_QUANTITY_MISMATCH")
        total_obligation = sum((cycle.open_obligation for _, cycle in cycles), ZERO)
        if quantity != total_obligation:
            blockers.append(f"SETTLEMENT_QUANTITY_MISMATCH:{quantity}:{total_obligation}")

        trade_view = [{"inbox_id": str(r.id), "payload_hash": r.payload_hash,
                       "trade_id": str(r.payload.get("trade_id_str")), "side": str(r.payload.get("own_side")),
                       "quantity": str(r.payload.get("size")), "price": str(r.payload.get("price")),
                       "exchange_order_id": None if r.payload.get("own_exchange_order_id") is None
                       else str(r.payload.get("own_exchange_order_id"))}
                      for r in sorted(trades, key=lambda x: x.id)]
        cycle_views = [{
            "grid_id": self.grid_id, "cell_id": str(ledger.cell_id), "generation": str(cycle.generation),
            "entry_side": cycle.entry_side.value, "E": str(cycle.E), "X": str(cycle.X),
            "external_settled": canonical_decimal(cycle.external_settled),
            "proposed_settlement": canonical_decimal(cycle.open_obligation), "open_after": "0",
        } for ledger, cycle in cycles]
        cycle_view = cycle_views[0] if len(cycle_views) == 1 else None
        order_view = None if terminal is None else {
            "inbox_id": str(terminal.id), "payload_hash": terminal.payload_hash,
            "exchange_order_id": str(terminal.payload.get("order_index") or terminal.payload.get("order_id")),
            "client_order_id": str(terminal.payload.get("client_order_id")),
            "side": str(terminal.payload.get("side")), "reduce_only": bool(terminal.payload.get("reduce_only")),
            "final": Decimal(str(terminal.payload.get("remaining_base_amount", "-1"))) == ZERO,
            "filled": str(terminal.payload.get("filled_base_amount")),
        }
        semantic = {
            "account_index": str(self.b_engine.account_index) if self.b_engine else None,
            "domain": self.port.domain, "market_id": str(self.port.market_id),
            "config_revision": self.b_engine.config_revision if self.b_engine else 0,
            "engine_revision": self.b_engine.engine_revision if self.b_engine else 0,
            "cycles": cycle_views, "total_quantity": canonical_decimal(total_obligation),
            "trades": trade_view, "terminal_order": order_view,
            "observed_position": None if self.position is None else str(self.position.net_base),
            "active_order_fingerprint": [],
        }
        proof_id = hashlib.sha256(json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        scan = self.complete_scans[-1] if self.complete_scans else None
        return {
            "proof_id": proof_id, "blockers": sorted(set(blockers)), "cycle": cycle_view,
            "cycles": cycle_views, "total_quantity": canonical_decimal(total_obligation),
            "settlement_side": settlement_side or None, "trades": trade_view, "terminal_order": order_view,
            "observed_position": None if self.position is None else str(self.position.net_base),
            "position_observed_at_ms": None if self.position_at is None else _ms(self.position_at),
            "active_observed_at_ms": None if self.active_at is None else _ms(self.active_at),
            "history_scan_started_at_ms": None if scan is None else _ms(scan.started_at),
            "history_scan_completed_at_ms": None if scan is None else _ms(scan.completed_at),
            "trades_high_water": self.high_water.get(STREAM_TRADES),
            "orders_high_water": self.high_water.get(STREAM_ORDERS),
        }

    def _cmd_baseline_audit(self, payload: Dict[str, Any], now: float, tx) -> CommandOutcome:
        """AC-30: explicit operator audit/rebase; never changes cell obligations or hides unknown fills."""
        if not self.bootstrapped:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "NOT_BOOTSTRAPPED"})
        if not self.history_complete or self._history_stale(now) or self._position_stale(now):
            return CommandOutcome(CommandStatus.REJECTED, {"error": "AUDIT_NEEDS_FRESH_COMPLETE_HISTORY"})
        try:
            observed = Decimal(str(payload["observed_position"]))
        except Exception:  # noqa: BLE001
            return CommandOutcome(CommandStatus.REJECTED, {"error": "AUDIT_PAYLOAD_INVALID"})
        venue = self.position.net_base
        if observed != venue:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "AUDIT_STALE_OBSERVATION",
                                                           "observed_position": str(venue)})
        unsettled = self._audit_evidence_unsettled(now)
        if unsettled:
            # Retryable: a rebase on a position that may contain an own fill history has not delivered would put
            # that fill into B and count it twice (NG-RISK-003, AC-30).
            return CommandOutcome(CommandStatus.REJECTED, {"error": "AUDIT_EVIDENCE_NOT_SETTLED",
                                                           "reasons": unsettled, "retryable": True})
        unresolved = [leg.cid for leg in self.non_final_legs() if leg.state != OrderState.LIVE]
        if unresolved:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "AUDIT_UNRESOLVED_ORDERS",
                                                           "cids": [str(c) for c in unresolved]})
        buys, sells = self._confirmed_fills()
        new_baseline = venue - buys + sells
        inbox_ids = [rec.id for rec in self.unmatched if rec.stream == B_TRADES]
        old = self.effective_baseline
        self.store.record_baseline_audit(tx, str(payload.get("actor") or "operator"),
                                         str(payload.get("note") or "baseline audit"), venue,
                                         new_baseline=new_baseline, resolved_inbox_ids=inbox_ids)
        if self.b_engine.manual_reconcile_required and "drift" in (self.b_engine.manual_reconcile_reason or ""):
            self.store.record_manual_reconciliation(tx, str(payload.get("actor") or "operator"),
                                                    "baseline audit resolved position drift",
                                                    {"observed_position": str(venue),
                                                     "new_baseline": str(new_baseline)},
                                                    clear_manual_reconcile=True)
        self.position_gap_since = None
        return CommandOutcome(CommandStatus.APPLIED, {
            "observed_position": str(venue), "old_effective_baseline": str(old),
            "new_effective_baseline": str(new_baseline), "acknowledged_unmatched": inbox_ids}, reload=True)

    def _audit_evidence_unsettled(self, now: float) -> List[str]:
        """Why the current venue position cannot yet be compared with the ledger for a baseline rebase."""
        reasons = []
        if self.position_seq < self.fills_commit_seq:
            reasons.append("position read predates the latest committed fill")
        if self.position_seq < self.history_rows_commit_seq:
            # any row (own fill, unmatched manual trade, order row): an audit rebased on this read would
            # acknowledge an inbox row the read does not include (D2-07, M4)
            reasons.append("position read predates a committed history row")
        if self.active_rows is None or self.active_at is None or self.active_seq < self.position_seq:
            # the "active ahead" check below needs a list at least as new as the position (D2-18, M4)
            reasons.append("active orders unknown or older than the position read")
        if self.ws_pending:
            reasons.append(f"{len(self.ws_pending)} WS trade signal(s) not yet in committed history")
        if self.history_lag_s(now) > 0:
            reasons.append(f"history lag {self.history_lag_s(now):.1f} s")
        if self._last_walk_start_seq < self.position_seq:
            reasons.append("no complete history walk started after the position read")
        live = [leg.cid for leg in self.non_final_legs() if leg.state != OrderState.LIVE]
        if live:
            reasons.append(f"unresolved own orders {[str(c) for c in live[:5]]}")
        # A dropped WS event must not let a live order's unrecorded execution into B: the venue's own active rows
        # may not show more execution than history proves.
        rows = {r.client_order_id: r for r in (self.active_rows or [])}
        ahead = [str(leg.cid) for leg in self.non_final_legs() if leg.state == OrderState.LIVE and leg.cid in rows
                 and rows[leg.cid].filled_base_amount > leg.filled]
        if ahead:
            reasons.append(f"active orders show executions history has not delivered {ahead[:5]}")
        if not self._position_settled_for_audit():
            reasons.append("position not yet stable for the settlement delay before a complete history walk")
        return reasons

    def _position_settled_for_audit(self) -> bool:
        """The current venue position has been observed unchanged, with no fill committed since, from a read at
        least ``settlement_delay_s`` before a complete history walk started (history had the delay to publish
        every execution that read included)."""
        if self.position is None or not self.complete_scans:
            return False
        value = self.position.net_base
        stable_since = None
        for seq, at, net in reversed(self._position_reads):
            if net != value or seq <= self.fills_commit_seq:
                break
            stable_since = at
        if stable_since is None:
            return False
        return self.complete_scans[-1].started_at >= stable_since + float(self.config.settlement_delay_s)

    def _acknowledge_scan_conflicts(self, now: float) -> None:
        """The operator audited the current scanner conflicts: they are not re-flagged, and the walk boundary is
        rebased (a walk containing a contradicted row never completes, so the old high-water could not advance).
        The contradicted rows stay withheld; nothing is reset (no rebaseline, cells untouched)."""
        last = self.scanner.last_result
        current = list(last.conflicts) if last is not None else []
        self.meta.acknowledged_conflicts = (self.meta.acknowledged_conflicts + [
            c for c in current if c not in self.meta.acknowledged_conflicts])[-200:]
        if current:
            floor = _ms(now) - int(self.config.history_overlap_s * 1000)
            self.meta.history_reset = {STREAM_TRADES: floor, STREAM_ORDERS: floor}

    def _withheld_late_trades(self) -> List[ExchangeTradeRow]:
        """Exact own executions the scanner withheld because they exceed an already *settled* order's terminal
        cumulative (a stale/contradicted order row, AC-42). They are never applied automatically (AC-40); an
        operator ``ack_late_evidence`` may accept them. A key seen with several payloads, a row that is not
        exactly attributed to a final own leg, or one contradicting that leg's side/exchange id is never a
        candidate (it stays a history conflict)."""
        domain = self.port.domain
        versions: Dict[Tuple, List[ExchangeTradeRow]] = {}
        for row in self.scanner.last_conflicted_rows.get(STREAM_TRADES, []):
            versions.setdefault(row.dedupe_key(domain), []).append(row)
        out: List[ExchangeTradeRow] = []
        for key, rows in versions.items():
            if len(rows) != 1 or key in self.committed[STREAM_TRADES]:
                continue
            row = rows[0]
            cid = row.own_client_order_id if row.own_client_order_id in self.order_meta else None
            leg = self.leg_by_cid(cid) if cid is not None else None
            if leg is None or leg.state not in (OrderState.TERMINAL, OrderState.REJECTED_ZERO_FILL) \
                    or row.own_side != leg.side:
                continue
            if row.own_exchange_order_id and leg.exchange_order_id \
                    and row.own_exchange_order_id != leg.exchange_order_id:
                continue
            out.append(row)
        return out

    def _cmd_ack_late_evidence(self, actor: str, note: str, evidence: Dict[str, Any], now: float,
                               tx) -> CommandOutcome:
        """Operator audit of late evidence (NG-HIST-002, AC-42) -- ONE store transaction:

        1. withheld executions of settled own orders are committed to their OLD cycles (only now, with the audit);
        2. WS-A ``CellLedger.acknowledge_late_evidence(gen)`` for every cycle carrying late evidence;
        3. the audited cumulative of every corrected leg is persisted with the leg (it supersedes the stale venue
           order row; a "rejected" leg that did execute is projected as TERMINAL with it);
        4. ``record_manual_reconciliation(resolved_conflict_ids=LATE_FILL + CUMULATIVE_EXCEEDS_ORDER)`` with the
           corrected legs and audited cumulatives as evidence; the store then accepts an ordinary TP for the old
           cycle's obligation (``late_evidence == 2``).

        The obligation is never reset or re-based: it is closed by ordinary TP legs at the fixed target."""
        s = self.store
        accepted = self._withheld_late_trades()
        if accepted:
            res = s.apply_history_batch(tx, accepted, (), None, batch_id=f"late-audit-{_ms(now)}")
            self.fills_commit_seq = self.history_rows_commit_seq = self._next_seq()
            for fill in res.new_fills:
                ledger = self.cells.get(fill.cell_id)
                identity = s.identity_for_cid(fill.cid)
                if ledger is not None and identity is not None:
                    ledger.fills[fill.dedupe_key] = LedgerFill(identity=identity, qty=fill.size, price=fill.price,
                                                               side=fill.own_side)
            for cid in sorted(res.touched_cids):
                self._mirror(cid)
        conflicts = s.open_conflicts()
        late_cids = {c.cid for c in conflicts if c.kind == "LATE_FILL" and c.cid is not None}
        late_cids |= {leg.cid for leg in self.all_legs() if leg.late_evidence and leg.cid is not None}
        ids = [c.id for c in conflicts
               if c.kind == "LATE_FILL" or (c.kind == "CUMULATIVE_EXCEEDS_ORDER" and c.cid in late_cids)]
        if not ids and not late_cids:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "NO_LATE_EVIDENCE"})
        corrected: List[Dict[str, str]] = []
        cycles: List[Dict[str, str]] = []
        for cell_id, gen in sorted({(self.order_meta[c].cell_id, self.order_meta[c].generation)
                                    for c in late_cids if c in self.order_meta}):
            ledger = self.cells[cell_id]
            for identity in ledger.acknowledge_late_evidence(gen):
                _, leg = ledger.find_leg(identity)
                meta = self.order_meta[leg.cid]
                order = s.order(leg.cid)
                meta.audited_cumulative = str(leg.filled)
                s.kv_set(tx, f"om:{leg.cid}", meta.to_json())
                corrected.append({"cid": str(leg.cid), "cell_id": str(cell_id), "generation": str(gen),
                                  "role": leg.identity.role.value, "state": leg.state.value,
                                  "audited_cumulative": str(leg.filled),
                                  "venue_terminal_filled": None if order is None or order.venue_filled is None
                                  else str(order.venue_filled)})
            cycle = next(c for c in ledger.cycles if c.generation == gen)
            cycles.append({"cell_id": str(cell_id), "generation": str(gen), "E": str(cycle.E), "X": str(cycle.X)})
        scan_conflicts = [c for c in (self.scanner.last_result.conflicts if self.scanner.last_result else [])]
        explained = all(c.startswith("conflict:trades_exceed_order_cumulative") for c in scan_conflicts)
        reason = self.b_engine.manual_reconcile_reason or ""
        remaining = [c for c in conflicts if c.id not in ids]
        clear = explained and not remaining and reason.startswith("history conflict")
        s.record_manual_reconciliation(
            tx, actor, note, dict(evidence, accepted_trades=[
                {"trade_id": r.trade_id_str, "cid": str(r.own_client_order_id), "side": r.own_side.value,
                 "size": str(r.size), "price": str(r.price)} for r in accepted],
                corrected_legs=corrected, audited_cycles=cycles),
            resolved_conflict_ids=ids, clear_manual_reconcile=clear)
        if scan_conflicts and explained:
            self._acknowledge_scan_conflicts(now)     # any other contradiction needs ack_history_conflict
        return CommandOutcome(CommandStatus.APPLIED, {
            "resolved_conflicts": ids, "accepted_trades": [r.trade_id_str for r in accepted],
            "corrected_legs": corrected, "audited_cycles": cycles}, reload=True)

    def _cmd_migrate_grid(self, actor: str, note: str, now: float, tx) -> CommandOutcome:
        """AC-52 audited grid replacement: only when the configured dimensions differ from the stored grid, the old
        grid is quiescent (``grid_mutation_blockers() == []``) and fresh market data allows a valid new grid. Old
        grid, cells and cycles are preserved (RETIRED); baseline, fills and cursors are untouched (no reset)."""
        s = self.store
        if self.grid_record is None or self.grid_record.fingerprint == self.fingerprint:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "NOTHING_TO_MIGRATE"})
        blockers = s.grid_mutation_blockers()
        if blockers:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "GRID_NOT_QUIESCENT", "blockers": blockers[:20]})
        if self.config.grid_id == self.grid_record.grid_id:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "NEW_GRID_ID_REQUIRED",
                                                           "detail": "a migrated grid needs a new grid_id"})
        if self.rules is None or grid.rules_blockers(self.rules) or self.mid is None or self._rules_stale(now):
            return CommandOutcome(CommandStatus.REJECTED, {"error": "MARKET_DATA_NOT_READY"})
        errors = grid.validate_config(self.config, self.rules, self.mid, bootstrap=True)
        if errors:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "CONFIG_INVALID", "errors": errors})
        prices = grid.build_grid(self.config.lower_price, self.config.upper_price, self.config.cell_count, self.rules)
        anchor = grid.compute_anchor(self.mid, self.config.lower_price, self.config.upper_price)
        specs = grid.assign_cells(prices, anchor)
        old = self.grid_record
        s.migrate_grid(tx, GridMigration(
            new_grid_id=self.config.grid_id, config_fingerprint=self.fingerprint,
            config=grid_config_to_json(self.config), lower_price=self.config.lower_price,
            upper_price=self.config.upper_price, order_amount_base=self.config.order_amount_base, prices=prices,
            cells=specs, anchor=anchor, actor=actor, reason=note))
        self.meta.freezes.pop(FREEZE_CONFIG_MISMATCH, None)
        self.meta.obligations = {}
        self.meta.reject_latches = {}
        return CommandOutcome(CommandStatus.APPLIED, {
            "old_grid_id": old.grid_id, "new_grid_id": self.config.grid_id, "anchor": str(anchor),
            "cells": len(specs)}, reload=True)

    def grid_extension_candidate(self, now: Optional[float] = None) -> Dict[str, Any]:
        """Proof-bound add-only extension derived from the stored window and this process's config."""
        now = self.clock() if now is None else now
        blockers: List[str] = []
        source = self.grid_record
        if source is None:
            return {"proof_id": hashlib.sha256(b"not-bootstrapped").hexdigest(),
                    "blockers": ["NOT_BOOTSTRAPPED"], "source": None, "target": None,
                    "added_cells": [], "retained_obligations": [], "observed_position": None}

        def window_payload(fingerprint: str, lower: Decimal, upper: Decimal, count: int,
                           amount: Decimal, anchor: Decimal, prices: Sequence[Decimal]) -> Dict[str, Any]:
            return {"grid_id": source.grid_id, "fingerprint": fingerprint,
                    "lower_price": canonical_decimal(lower), "upper_price": canonical_decimal(upper),
                    "cell_count": count, "order_amount_base": canonical_decimal(amount),
                    "anchor": canonical_decimal(anchor), "prices": [canonical_decimal(p) for p in prices]}

        source_payload = window_payload(source.fingerprint, source.lower_price, source.upper_price,
                                        source.cell_count, source.order_amount_base, source.anchor, source.prices)
        source_payload["config"] = self.store.config_payload()
        target_prices: List[Decimal] = []
        if self.config.grid_id != source.grid_id:
            blockers.append("GRID_ID_CHANGED")
        if self.config.order_amount_base != source.order_amount_base:
            blockers.append("ORDER_AMOUNT_CHANGED")
        if self.config.lower_price > source.lower_price or self.config.upper_price < source.upper_price:
            blockers.append("WINDOW_CONTRACTION")
        if self.config.lower_price == source.lower_price and self.config.upper_price == source.upper_price:
            blockers.append("NOTHING_TO_EXTEND")
        if not self.config.lower_price <= source.anchor <= self.config.upper_price:
            blockers.append("ANCHOR_OUTSIDE_TARGET")
        if self.rules is None or grid.rules_blockers(self.rules) or self._rules_stale(now):
            blockers.append("RULES_NOT_READY")
        else:
            try:
                target_prices = grid.build_grid(self.config.lower_price, self.config.upper_price,
                                                self.config.cell_count, self.rules)
            except (ArithmeticError, grid.GridValidationError) as exc:
                blockers.append(f"TARGET_GRID_INVALID:{type(exc).__name__}")
        if target_prices:
            starts = [i for i in range(len(target_prices) - len(source.prices) + 1)
                      if tuple(target_prices[i:i + len(source.prices)]) == source.prices]
            old_steps = {b - a for a, b in zip(source.prices, source.prices[1:])}
            new_steps = {b - a for a, b in zip(target_prices, target_prices[1:])}
            if len(starts) != 1:
                blockers.append("OLD_BOUNDARIES_NOT_RETAINED")
            if len(old_steps) != 1 or new_steps != old_steps:
                blockers.append("STEP_CHANGED")
        target_payload = window_payload(self.fingerprint, self.config.lower_price, self.config.upper_price,
                                        self.config.cell_count, self.config.order_amount_base, source.anchor,
                                        target_prices)
        target_payload["config"] = grid_config_to_json(self.config)
        allowed_config_changes = {"lower_price", "upper_price", "cell_count", "max_active_orders",
                                  "directional_outside_bounds_entries"}
        for name in sorted(set(source_payload["config"]) | set(target_payload["config"])):
            if source_payload["config"].get(name) != target_payload["config"].get(name) \
                    and name not in allowed_config_changes:
                blockers.append(f"CONFIG_CHANGE_FORBIDDEN:{name}")

        existing = {(ledger.spec.low_price, ledger.spec.high_price): ledger for ledger in self.cells.values()}
        next_id = max(self.cells, default=-1) + 1
        added = []
        for low, high in zip(target_prices, target_prices[1:]):
            if (low, high) not in existing:
                side = Side.BUY if low < source.anchor else Side.SELL
                added.append({"cell_id": str(next_id), "low_price": canonical_decimal(low),
                              "high_price": canonical_decimal(high),
                              "entry_side": side.value})
                next_id += 1
        retained = []
        for cell_id, ledger in sorted(self.cells.items()):
            for cycle in ledger.cycles:
                if cycle.generation > 0 and cycle.open_obligation > ZERO:
                    retained.append({"cell_id": str(cell_id), "generation": str(cycle.generation),
                                     "quantity": canonical_decimal(cycle.open_obligation),
                                     "tp_price": canonical_decimal(ledger.spec.tp_price),
                                     "side": ledger.spec.tp_side.value})

        if not self.is_stopped or self.meta.stop_outcome not in STOPPED_OUTCOMES:
            blockers.append("ENGINE_NOT_CLEANLY_STOPPED")
        if not self._position_fresh_for_ledger(now):
            blockers.append("POSITION_NOT_FRESH")
        elif self.endpoints is None or self.position.net_base != self.endpoints.P:
            blockers.append("POSITION_DRIFT")
        if self.active_rows is None or self.active_at is None \
                or now - self.active_at > float(self.config.history_freshness_s):
            blockers.append("ACTIVE_ORDERS_NOT_FRESH")
        elif self.active_rows:
            blockers.append(f"ACTIVE_ORDERS_PRESENT:{len(self.active_rows)}")
        if not self.history_complete or self._history_stale(now):
            blockers.append("HISTORY_NOT_FRESH_COMPLETE")
        if self.ws_pending:
            blockers.append(f"WS_EXECUTIONS_PENDING:{len(self.ws_pending)}")
        blockers.extend(f"COHERENT_CUT:{reason}" for reason in self._audit_evidence_unsettled(now))
        for code in sorted(self.meta.freezes):
            if code != FREEZE_CONFIG_MISMATCH:
                blockers.append(f"FROZEN:{code}")
        if self.store is not None and not self.store.closed:
            blockers.extend(self.store.grid_extension_blockers())
        semantic = {"source": source_payload, "target": target_payload, "added_cells": added,
                    "retained_obligations": retained}
        proof_id = hashlib.sha256(json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return dict(semantic, proof_id=proof_id, blockers=sorted(set(blockers)),
                    observed_position=None if self.position is None else canonical_decimal(self.position.net_base))

    def _cmd_extend_grid(self, actor: str, note: str, payload: Dict[str, Any], now: float, tx) -> CommandOutcome:
        candidate = self.grid_extension_candidate(now)
        if candidate["blockers"]:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "GRID_EXTENSION_NOT_ELIGIBLE",
                                                           "blockers": candidate["blockers"]})
        if payload.get("acknowledge") is not True:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "GRID_EXTENSION_NOT_ACKNOWLEDGED"})
        if payload.get("proof_id") != candidate["proof_id"]:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "GRID_EXTENSION_PROOF_CHANGED",
                                                           "proof_id": candidate["proof_id"]})
        added = tuple(CellSpec(int(cell["cell_id"]), Decimal(cell["low_price"]), Decimal(cell["high_price"]),
                               Side(cell["entry_side"])) for cell in candidate["added_cells"])
        target = candidate["target"]
        self.store.extend_grid(tx, GridExtension(
            config_fingerprint=self.fingerprint, config=grid_config_to_json(self.config),
            lower_price=Decimal(target["lower_price"]), upper_price=Decimal(target["upper_price"]),
            order_amount_base=Decimal(target["order_amount_base"]),
            prices=tuple(Decimal(value) for value in target["prices"]), added_cells=added,
            anchor=Decimal(target["anchor"]), actor=actor, reason=note, proof_id=candidate["proof_id"]))
        self.meta.freezes.pop(FREEZE_CONFIG_MISMATCH, None)
        return CommandOutcome(CommandStatus.APPLIED, {"proof_id": candidate["proof_id"],
                                                      "added_cells": candidate["added_cells"],
                                                      "retained_obligations": candidate["retained_obligations"]},
                              reload=True)

    def grid_external_entry_candidate(self, now: Optional[float] = None) -> Dict[str, Any]:
        """Proof for one real manual entry coupled to the sole cell added by an extension."""
        now = self.clock() if now is None else now
        base = self.grid_extension_candidate(now)
        blockers = [value for value in base["blockers"] if value != "POSITION_DRIFT" and
                    "unmatched account trade" not in value and
                    not (value.startswith("STORE:manual reconciliation required:") and "drift" in value)]
        added = base.get("added_cells") or []
        if len(added) != 1:
            blockers.append(f"EXACTLY_ONE_ADDED_CELL_REQUIRED:{len(added)}")
        cell = added[0] if len(added) == 1 else None
        side = None if cell is None else Side(cell["entry_side"])
        quantity = self.config.order_amount_base
        tp_price = None if cell is None else (Decimal(cell["high_price"]) if side == Side.BUY
                                              else Decimal(cell["low_price"]))
        trades = [row for row in self.unmatched if row.stream == B_TRADES]
        orders = [row for row in self.unmatched if row.stream == B_ORDERS]
        order_ids = {str(row.payload.get("own_exchange_order_id") or "") for row in trades}
        client_ids = {"" if row.payload.get("own_client_order_id") is None
                      else str(row.payload.get("own_client_order_id")) for row in trades}
        if not trades or len(order_ids) != 1 or "" in order_ids or len(client_ids) != 1 or "" in client_ids:
            blockers.append("EXACTLY_ONE_MANUAL_ENTRY_ORDER_REQUIRED")
        matching = []
        if len(order_ids) == 1:
            order_id = next(iter(order_ids))
            matching = [row for row in orders
                        if str(row.payload.get("order_index") or row.payload.get("order_id") or "") == order_id]
        if len(matching) != 1 or len(orders) != 1:
            blockers.append(f"EXACTLY_ONE_TERMINAL_ORDER_REQUIRED:{len(matching)}")
        terminal = matching[0] if len(matching) == 1 else None
        trade_quantity = sum((Decimal(str(row.payload.get("size", "0"))) for row in trades), ZERO)
        if side is not None and any(row.payload.get("own_side") != side.value for row in trades):
            blockers.append("WRONG_ENTRY_SIDE")
        if trade_quantity != quantity:
            blockers.append(f"ENTRY_QUANTITY_MISMATCH:{trade_quantity}:{quantity}")
        if cell is not None and trades:
            entry_price = Decimal(cell["low_price"] if side == Side.BUY else cell["high_price"])
            if any((Decimal(str(row.payload.get("price"))) > entry_price if side == Side.BUY else
                    Decimal(str(row.payload.get("price"))) < entry_price) for row in trades):
                blockers.append("ENTRY_EXECUTION_WORSE_THAN_CELL")
        if terminal is not None:
            order = terminal.payload
            final = str(order.get("status", "")).lower() in {"filled", "closed", "complete", "completed"}
            terminal_clients = {str(v) for v in (order.get("client_order_id"), order.get("client_order_id_str"))
                                if v not in (None, "")}
            if not final or order.get("reduce_only") or side is None or order.get("side") != side.value or \
                    Decimal(str(order.get("filled_base_amount", "-1"))) != quantity or \
                    Decimal(str(order.get("initial_base_amount", "-1"))) != quantity or \
                    Decimal(str(order.get("remaining_base_amount", "-1"))) != ZERO:
                blockers.append("MANUAL_ENTRY_ORDER_NOT_EXACT_FINAL")
            if terminal_clients != client_ids:
                blockers.append("MANUAL_ENTRY_CLIENT_ID_MISMATCH")
        old_p = None if self.endpoints is None else self.endpoints.P
        expected_position = None if old_p is None or side is None else old_p + (quantity if side == Side.BUY else -quantity)
        if self.position is None or expected_position is None or self.position.net_base != expected_position:
            blockers.append("POSITION_NOT_EXPLAINED_BY_EXTERNAL_ENTRY")
        scan = self.complete_scans[-1] if self.complete_scans else None
        trade_view = [{"inbox_id": str(row.id), "payload_hash": row.payload_hash,
                       "trade_id": str(row.payload.get("trade_id_str")),
                       "exchange_order_id": str(row.payload.get("own_exchange_order_id")),
                       "client_order_id": str(row.payload.get("own_client_order_id")),
                       "side": str(row.payload.get("own_side")), "quantity": str(row.payload.get("size")),
                       "price": str(row.payload.get("price"))} for row in sorted(trades, key=lambda value: value.id)]
        order_view = None if terminal is None else {
            "inbox_id": str(terminal.id), "payload_hash": terminal.payload_hash,
            "exchange_order_id": str(terminal.payload.get("order_index") or terminal.payload.get("order_id")),
            "client_order_id": str(terminal.payload.get("client_order_id")),
            "side": str(terminal.payload.get("side")), "reduce_only": bool(terminal.payload.get("reduce_only")),
            "status": str(terminal.payload.get("status")), "filled": str(terminal.payload.get("filled_base_amount"))}
        proposed = None if cell is None else {
            "grid_id": self.config.grid_id, "cell_id": cell["cell_id"], "generation": "1",
            "entry_side": cell["entry_side"], "quantity": canonical_decimal(quantity),
            "tp_price": canonical_decimal(tp_price), "entry_origin": "EXTERNAL", "fee_pnl_complete": False}
        base_asset = self.config.trading_pair.split("-")[0]
        confirmation = None if proposed is None else (
            f"ADOPT EXTERNAL {side.value} {canonical_decimal(quantity)} "
            f"{base_asset} INTO {self.config.grid_id} CELL {cell['cell_id']} TP {canonical_decimal(tp_price)}")
        semantic = {"source": base.get("source"), "target": base.get("target"), "added_cell": cell,
                    "proposed_cycle": proposed, "retained_obligations": base.get("retained_obligations", []),
                    "old_ledger_position": None if old_p is None else canonical_decimal(old_p),
                    "observed_position": None if self.position is None else canonical_decimal(self.position.net_base),
                    "manual_order": order_view, "trades": trade_view,
                    "config_revision": None if self.b_engine is None else self.b_engine.config_revision,
                    "engine_revision": None if self.b_engine is None else self.b_engine.engine_revision,
                    "position_observed_at_ms": None if self.position_at is None else _ms(self.position_at),
                    "active_observed_at_ms": None if self.active_at is None else _ms(self.active_at),
                    "history_scan_started_at_ms": None if scan is None else _ms(scan.started_at),
                    "history_scan_completed_at_ms": None if scan is None else _ms(scan.completed_at),
                    "trades_high_water": self.high_water.get(STREAM_TRADES),
                    "orders_high_water": self.high_water.get(STREAM_ORDERS),
                    "risk_preview": {"P": None if expected_position is None else canonical_decimal(expected_position),
                                     "gross": canonical_decimal(sum(
                                         (abs(c.open_obligation) for ledger in self.cells.values()
                                          for c in ledger.cycles), ZERO) + quantity)}}
        proof_id = hashlib.sha256(json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return dict(semantic, proof_id=proof_id, blockers=sorted(set(blockers)), confirmation=confirmation)

    def _cmd_extend_grid_with_external_entry(self, actor: str, note: str, payload: Dict[str, Any], now: float,
                                             tx) -> CommandOutcome:
        candidate = self.grid_external_entry_candidate(now)
        if candidate["blockers"]:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "EXTERNAL_ENTRY_NOT_ELIGIBLE",
                                                           "blockers": candidate["blockers"]})
        if payload.get("acknowledge") is not True or payload.get("confirmation") != candidate["confirmation"]:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "EXTERNAL_ENTRY_NOT_ACKNOWLEDGED",
                                                           "confirmation": candidate["confirmation"]})
        if payload.get("proof_id") != candidate["proof_id"]:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "EXTERNAL_ENTRY_PROOF_CHANGED",
                                                           "proof_id": candidate["proof_id"]})
        cell = candidate["added_cell"]
        added = (CellSpec(int(cell["cell_id"]), Decimal(cell["low_price"]), Decimal(cell["high_price"]),
                          Side(cell["entry_side"])),)
        target = candidate["target"]
        extension = GridExtension(self.fingerprint, grid_config_to_json(self.config),
                                  Decimal(target["lower_price"]), Decimal(target["upper_price"]),
                                  Decimal(target["order_amount_base"]),
                                  tuple(Decimal(value) for value in target["prices"]), added,
                                  Decimal(target["anchor"]), actor, note, candidate["proof_id"])
        terminal = candidate["manual_order"]
        evidence = tuple(ExternalEntryEvidence(int(row["inbox_id"]), "TRADE", Decimal(row["quantity"]))
                         for row in candidate["trades"]) + (
            ExternalEntryEvidence(int(terminal["inbox_id"]), "TERMINAL_ORDER"),)
        adoption_id = self.store.extend_grid_with_external_entry(tx, ExternalEntryRequest(
            extension, candidate["proof_id"], int(cell["cell_id"]), Side(cell["entry_side"]),
            Decimal(candidate["proposed_cycle"]["quantity"]), Decimal(candidate["observed_position"]), actor, note,
            int(candidate["config_revision"]), int(candidate["engine_revision"]),
            int(candidate["position_observed_at_ms"]), int(candidate["active_observed_at_ms"]),
            int(candidate["history_scan_started_at_ms"]), int(candidate["history_scan_completed_at_ms"]),
            candidate["trades_high_water"], candidate["orders_high_water"], evidence))
        self.meta.freezes.pop(FREEZE_CONFIG_MISMATCH, None)
        return CommandOutcome(CommandStatus.APPLIED, {
            "adoption_id": str(adoption_id), "proof_id": candidate["proof_id"],
            "proposed_cycle": candidate["proposed_cycle"],
            "retained_obligations": candidate["retained_obligations"]}, reload=True)

    def _cmd_reconcile(self, action: str, payload: Dict[str, Any], now: float, tx) -> CommandOutcome:
        if action not in ALL_AUDIT_ACTIONS:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "UNKNOWN_AUDIT_ACTION", "action": action})
        s = self.store
        actor = str(payload.get("actor") or "operator")
        note = str(payload.get("note") or action)
        evidence = {"action": action, "note": note, "engine_state": self.engine_state.value}
        if action == "ack_late_evidence":
            return self._cmd_ack_late_evidence(actor, note, evidence, now, tx)
        if action == "ack_history_conflict":
            return self._cmd_ack_history_conflict(actor, note, evidence, payload, now, tx)
        if action == "ack_retention_gap":
            # The unreachable old boundary is replaced by the currently available history (audited); baseline,
            # cells and fills are untouched (no reset, no rebaseline). Drift checks still apply afterwards. The
            # store lifts a durable gap only for the streams named here.
            gaps = [b for b, cur in s.cursors().items() if cur.retention_gap_open]
            s.record_manual_reconciliation(tx, actor, note, dict(evidence, retention_gaps=gaps),
                                           resolved_retention_gaps=gaps, clear_manual_reconcile=True)
            floor = _ms(now) - int(self.config.history_overlap_s * 1000)
            self.meta.history_reset = {STREAM_TRADES: floor, STREAM_ORDERS: floor}
            return CommandOutcome(CommandStatus.APPLIED, {"history_reset_floor_ms": floor,
                                                          "resolved_retention_gaps": gaps}, reload=True)
        if action == "retire_colliding_cid":
            if FREEZE_CID not in self.meta.freezes or self.meta.colliding_cid is None:
                return CommandOutcome(CommandStatus.REJECTED, {"error": "NO_CID_COLLISION"})
            cid = int(payload.get("cid", self.meta.colliding_cid))
            if cid != int(self.meta.colliding_cid):
                # only the CID the engine itself recorded as colliding may be retired (never an arbitrary one)
                return CommandOutcome(CommandStatus.REJECTED, {"error": "NOT_THE_COLLIDING_CID",
                                                               "colliding_cid": str(self.meta.colliding_cid)})
            s.retire_cid(tx, cid, actor, note)                 # audited; never allocated from now on (AC-43)
            detail = self.meta.freezes.pop(FREEZE_CID, None)
            self.meta.colliding_cid = None
            return CommandOutcome(CommandStatus.APPLIED, {"retired_cid": str(cid), "cleared": FREEZE_CID,
                                                          "detail": detail}, reload=True)
        if action == "migrate_grid":
            return self._cmd_migrate_grid(actor, note, now, tx)
        if action == "extend_grid":
            return self._cmd_extend_grid(actor, note, payload, now, tx)
        if action == "extend_grid_with_external_entry":
            return self._cmd_extend_grid_with_external_entry(actor, note, payload, now, tx)
        if action == "settle_external_close":
            expected_confirmation = f"SETTLE EXTERNAL CLOSE {self.grid_id} AT FLAT 0"
            if payload.get("acknowledge") is not True or payload.get("confirmation") != expected_confirmation:
                return CommandOutcome(CommandStatus.REJECTED, {
                    "error": "EXTERNAL_CLOSE_NOT_ACKNOWLEDGED", "confirmation": expected_confirmation})
            candidate = self.external_close_candidate(now)
            if candidate["blockers"]:
                return CommandOutcome(CommandStatus.REJECTED, {
                    "error": "EXTERNAL_CLOSE_NOT_ELIGIBLE", "blockers": candidate["blockers"]})
            if payload.get("proof_id") != candidate["proof_id"]:
                return CommandOutcome(CommandStatus.REJECTED, {
                    "error": "EXTERNAL_CLOSE_PROOF_CHANGED", "proof_id": candidate["proof_id"]})
            cycles = candidate["cycles"]
            terminal = candidate["terminal_order"]
            request = ExternalSettlementRequest(
                proof_id=candidate["proof_id"], grid_id=self.grid_id,
                settlement_side=Side(candidate["settlement_side"]), observed_position=ZERO,
                actor=actor, reason=note,
                expected_config_revision=self.b_engine.config_revision,
                expected_engine_revision=self.b_engine.engine_revision,
                position_observed_at_ms=int(candidate["position_observed_at_ms"]),
                active_observed_at_ms=int(candidate["active_observed_at_ms"]),
                history_scan_started_at_ms=int(candidate["history_scan_started_at_ms"]),
                history_scan_completed_at_ms=int(candidate["history_scan_completed_at_ms"]),
                trades_high_water=candidate["trades_high_water"],
                orders_high_water=candidate["orders_high_water"],
                cycles=tuple(
                    ExternalSettlementCycle(
                        grid_id=cycle["grid_id"], cell_id=int(cycle["cell_id"]),
                        generation=int(cycle["generation"]), quantity=Decimal(cycle["proposed_settlement"]))
                    for cycle in cycles),
                evidence=tuple(ExternalSettlementEvidence(
                    inbox_id=int(t["inbox_id"]), evidence_role="TRADE",
                    allocated_quantity=Decimal(t["quantity"])) for t in candidate["trades"])
                + (ExternalSettlementEvidence(inbox_id=int(terminal["inbox_id"]),
                                              evidence_role="TERMINAL_ORDER"),),
            )
            settlement_id = s.record_external_settlement(tx, request)
            self.position_gap_since = None
            result = {
                "settlement_id": str(settlement_id), "proof_id": candidate["proof_id"],
                "grid_id": self.grid_id, "cycles": cycles,
                "total_quantity": candidate["total_quantity"], "stopped": True,
            }
            if len(cycles) == 1:
                cycle = cycles[0]
                result.update({"cell_id": cycle["cell_id"], "generation": cycle["generation"],
                               "E": cycle["E"], "X": cycle["X"],
                               "external_settled": cycle["proposed_settlement"], "open": cycle["open_after"]})
            return CommandOutcome(CommandStatus.APPLIED, result, reload=True)
        if action == "ack_risk_blocked":
            detail = self.meta.freezes.pop(FREEZE_RISK_BLOCKED, None)
            self.meta.risk_blocked = {}
            return CommandOutcome(CommandStatus.APPLIED, {"cleared": FREEZE_RISK_BLOCKED, "detail": detail},
                                  audit=("ack_risk_blocked", {"detail": detail, "note": note}))
        if action == "resolve_unknown_submit":
            cid = int(payload["cid"])
            leg = self.leg_by_cid(cid)
            if leg is None or leg.state != OrderState.SUBMIT_UNKNOWN or leg.filled != 0:
                return CommandOutcome(CommandStatus.REJECTED, {"error": "NOT_AN_UNRESOLVED_ZERO_FILL_SUBMIT"})
            if not self.history_complete or self._history_stale(now) or cid in self.terminal_rows:
                return CommandOutcome(CommandStatus.REJECTED, {"error": "EVIDENCE_EXISTS_OR_HISTORY_INCOMPLETE"})
            not_fresh = self._absence_not_proven(cid, now)
            if not_fresh:
                return CommandOutcome(CommandStatus.REJECTED, {"error": "ABSENCE_NOT_PROVEN", "detail": not_fresh})
            if any(r.client_order_id == cid for r in self.active_rows):
                return CommandOutcome(CommandStatus.REJECTED, {"error": "ORDER_IS_ACTIVE_ON_VENUE"})
            s.record_manual_reconciliation(tx, actor, note, dict(evidence, cid=str(cid)),
                                           leg_resolutions={cid: OrderState.REJECTED_ZERO_FILL},
                                           clear_manual_reconcile=False)
            return CommandOutcome(CommandStatus.APPLIED, {"cid": str(cid), "resolution": "not_landed"}, reload=True)
        return CommandOutcome(CommandStatus.REJECTED, {"error": "UNKNOWN_AUDIT_ACTION", "action": action})

    def unknown_resolution_delay_s(self) -> float:
        return max(float(self.options.unknown_resolution_delay_s), float(self.config.settlement_delay_s))

    def history_conflict_set(self) -> Tuple[List[Dict[str, Any]], str]:
        """Everything an ``ack_history_conflict`` audits, as published in the snapshot (M1): the durable payload
        contradictions with every version seen (``committed`` marks the ledger's), the open store conflicts, the
        manual-reconcile reason, a ledger-invariant freeze and refused active-row evidence. ``conflict_set_id`` binds
        an ack to exactly this set: anything that appears after the operator's review changes it."""
        items: List[Dict[str, Any]] = []
        for _, entry in sorted(self.meta.history_conflicts.items()):
            cell = self._cell_of_ids(entry.get("client"), entry.get("exchange"))
            items.append({"stream": entry["stream"], "key": self._public_key_of(entry),
                          "cell_id": None if cell is None else str(cell),
                          "versions": sorted(({"fingerprint": public_fingerprint(fp), "summary": dict(summary),
                                               "committed": fp == entry.get("committed")}
                                              for fp, summary in entry["versions"].items()),
                                             key=lambda v: v["fingerprint"])})

        def single(stream: str, key: str, summary: Dict[str, str], cell: Optional[int] = None) -> None:
            items.append({"stream": stream, "key": f"{stream}:{key}", "cell_id": None if cell is None else str(cell),
                          "versions": [{"fingerprint": _digest([stream, key, summary]), "summary": summary,
                                        "committed": True}]})
        for c in self.open_conflicts:
            if c.kind != "LATE_FILL":
                cell = self.order_meta[c.cid].cell_id if c.cid in self.order_meta else None
                single("store_conflict", str(c.id), {"kind": str(c.kind), "cid": "" if c.cid is None else str(c.cid),
                                                     "detail": redact(c.detail or "")}, cell)
        if self.b_engine is not None and self.b_engine.manual_reconcile_required:
            single("manual_reconcile", "reason", {"reason": redact(self.b_engine.manual_reconcile_reason or "")})
        if FREEZE_INVARIANT in self.meta.freezes:
            single("freeze", FREEZE_INVARIANT, {"detail": redact(self.meta.freezes[FREEZE_INVARIANT])})
        for cell_id, why in sorted(self.evidence_conflicts.items()):
            single("active_evidence", str(cell_id), {"detail": redact(why)}, cell_id)
        set_id = _digest([[i["stream"], i["key"], [v["fingerprint"] for v in i["versions"]]] for i in items])
        return items, set_id

    @staticmethod
    def _public_key_of(entry: Dict[str, Any]) -> str:
        return public_conflict_key(entry["stream"], _key_from_label(entry["key"]))

    def _cmd_ack_history_conflict(self, actor: str, note: str, evidence: Dict[str, Any], payload: Dict[str, Any],
                                  now: float, tx) -> CommandOutcome:
        """Operator audit bound to the reviewed ``conflict_set_id`` (M1). Only the versions in that set are audited:
        the ledger's committed payload is accepted (never a quantity change, R6); a key that was never committed
        needs the operator's ``accepted`` choice (then the scanner commits it as ordinary evidence); every other
        version of the key seen by any walk is audited noise, merged with earlier noise so a flapping key converges
        (CR-3). Nothing audited => REJECTED."""
        items, set_id = self.history_conflict_set()
        given = payload.get("conflict_set_id")
        if not isinstance(given, str) or not given:
            return CommandOutcome(CommandStatus.REJECTED, {
                "error": "CONFLICT_SET_ID_REQUIRED", "conflict_set_id": set_id,
                "detail": "ack the conflict set you reviewed (snapshot summary.conflict_set_id)"})
        if given != set_id:
            return CommandOutcome(CommandStatus.REJECTED, {
                "error": "CONFLICT_SET_CHANGED", "conflict_set_id": set_id, "acknowledged": given,
                "detail": "the conflict set changed after your review; review the current snapshot again"})
        if not items:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "NOTHING_TO_AUDIT"})
        choices = payload.get("accepted") or {}
        if not isinstance(choices, dict) or not all(isinstance(k, str) and isinstance(v, str)
                                                    for k, v in choices.items()):
            return CommandOutcome(CommandStatus.REJECTED, {"error": "ACCEPTED_INVALID",
                                                           "detail": "accepted must map key -> fingerprint"})
        # The published (web-safe) key/fingerprints map back to the exact typed key and raw payload fingerprints.
        entries = sorted(self.meta.history_conflicts.values(), key=lambda e: (e["stream"], e["key"]))
        need, invalid, audited, internal = [], [], [], []
        known_keys = {self._public_key_of(e) for e in entries}
        invalid.extend({"key": k, "error": "NOT_IN_CONFLICT_SET"} for k in choices if k not in known_keys)
        for entry in entries:
            public = self._public_key_of(entry)
            raw = {public_fingerprint(fp): fp for fp in entry["versions"]}
            committed, choice = entry.get("committed"), choices.get(public)
            if committed is not None:
                if choice is not None and raw.get(choice) != committed:
                    invalid.append({"key": public, "error": "LEDGER_CORRECTION_NOT_SUPPORTED",
                                    "committed": public_fingerprint(committed)})
                accept = committed
            elif choice is None:
                need.append(public)
                continue
            elif choice not in raw:
                invalid.append({"key": public, "error": "NOT_A_SEEN_VERSION"})
                continue
            else:
                accept = raw[choice]
            noise = sorted(set(entry["versions"]) - {accept})
            internal.append((entry["stream"], entry["key"], accept, noise))
            audited.append({"stream": entry["stream"], "key": public, "accepted": public_fingerprint(accept),
                            "noise": sorted(public_fingerprint(fp) for fp in noise)})
        if invalid:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "ACCEPTED_INVALID", "keys": invalid[:20]})
        if need:
            return CommandOutcome(CommandStatus.REJECTED, {
                "error": "ACCEPTED_CHOICE_REQUIRED", "keys": need[:20],
                "detail": "no committed payload exists for these keys: choose the version the venue export proves"})
        for stream, label, accept, noise_now in internal:
            previous = self.meta.audited_payloads.get(stream, {}).get(label) or {}
            noise = set(noise_now) | (set(previous.get("noise", [])) if previous.get("accepted") == accept else set())
            self.meta.audited_payloads.setdefault(stream, {})[label] = {"accepted": accept,
                                                                        "noise": sorted(noise - {accept})}
        ids = [int(i["key"].split(":", 1)[1]) for i in items if i["stream"] == "store_conflict"]
        s = self.store
        s.record_manual_reconciliation(tx, actor, note, dict(evidence, conflict_set_id=set_id, audited_payloads=audited,
                                                             audited_set=items[:50]),
                                       resolved_conflict_ids=ids, clear_manual_reconcile=True)
        self.meta.history_conflicts = {}
        self.evidence_conflicts = {}
        self.meta.freezes.pop(FREEZE_INVARIANT, None)
        self._acknowledge_scan_conflicts(now)
        return CommandOutcome(CommandStatus.APPLIED, {"conflict_set_id": set_id, "resolved_conflicts": ids,
                                                      "audited_payloads": audited}, reload=True)

    def _absence_not_proven(self, cid: int, now: float) -> Optional[str]:
        """An unknown submit may be audited as "never landed" only against evidence taken long AFTER it was
        dispatched: no WS execution signal for the CID (filed under its trade id or the CID) still pending, and, only
        after ``unknown_resolution_delay_s`` (>= settlement_delay_s), a fresh active-orders list and
        ``settlement_scans`` complete history walks that started after that delay. Absence is never provable: an
        active list lagging longer than the delay stays an operator-audit residual risk (D2-15, H1)."""
        dispatched = [o.dispatched_at_ms for o in self.store.outbox_for_cid(cid)
                      if o.kind == "SUBMIT" and o.dispatched_at_ms is not None]
        if not dispatched:
            return "no dispatch recorded"
        if any(label == str(cid) or self._ws_label_cid.get(label) == cid for label in self.ws_pending):
            return "a WS execution signal for this CID is not yet in committed history"
        delay = self.unknown_resolution_delay_s()
        resolve_ms = max(dispatched) + int(delay * 1000)
        if _ms(now) < resolve_ms:
            return f"unknown-resolution delay ({delay:g} s) since the dispatch has not elapsed"
        if self.active_rows is None or self.active_at is None:
            return "active orders unknown"
        if _ms(self.active_at) <= resolve_ms:
            return "no active orders list read after the dispatch + unknown-resolution delay"
        if now - self.active_at > float(self.config.history_freshness_s):
            return "active orders list is stale"
        walks = [w for w in self.complete_scans if _ms(w.started_at) > resolve_ms]
        if len(walks) < self.config.settlement_scans:
            return (f"{len(walks)}/{self.config.settlement_scans} complete history walks started after the "
                    f"dispatch + unknown-resolution delay")
        return None

    # ================================================================================== account reads
    def _history_stale(self, now: float) -> bool:
        return (self.last_complete_scan_at is None
                or now - self.last_complete_scan_at > float(self.config.history_freshness_s))

    def _position_stale(self, now: float) -> bool:
        return self.position_at is None or now - self.position_at > float(self.config.history_freshness_s)

    def _rules_stale(self, now: float) -> bool:
        return self.rules_at is None or now - self.rules_at > self.options.rules_max_age_s

    async def _refresh_account(self, now: float) -> None:
        try:
            self.bid, self.ask = await self.port.best_bid_ask()
        except Exception as exc:  # noqa: BLE001
            self.bid = self.ask = None
            self._error("BOOK_UNAVAILABLE", f"{type(exc).__name__}: {exc}", now)
        if (self.rules is None or now - (self.rules_at or 0) >= self.options.rules_refresh_s) \
                and (self._rules_retry_at is None or now >= self._rules_retry_at):
            weight = self.port.request_weight(ENDPOINT_TRADING_RULES)
            # Lower priority than history and account reads: it only runs while one page of each history stream
            # and one account poll still fit the budget (a failing rules endpoint never starves proofs).
            reserve = (self.port.request_weight(ENDPOINT_TRADES) + self.port.request_weight(ENDPOINT_INACTIVE_ORDERS)
                       + self.port.request_weight(ENDPOINT_ACCOUNT) + self.port.request_weight(ENDPOINT_ACTIVE_ORDERS))
            while self._rules_weight_log and self._rules_weight_log[0][0] <= now - 60:
                self._rules_weight_log.popleft()
            rules_used = sum(w for _, w in self._rules_weight_log)
            if self._can_spend(now, weight + reserve) and rules_used + weight <= self._rules_allowance(weight):
                self._charge(now, weight)
                self._rules_weight_log.append((now, weight))
                try:
                    self.rules = await self.port.trading_rules()
                    self.rules_at = self.clock()
                    self._rules_failures = 0
                    self._rules_retry_at = None
                except Exception as exc:  # noqa: BLE001
                    self._rules_failures += 1
                    delay = min(self.options.rules_retry_initial_s * 2 ** (self._rules_failures - 1),
                                self.options.rules_retry_max_s)
                    self._rules_retry_at = now + delay
                    self._error("RULES_UNAVAILABLE", f"{type(exc).__name__}: {exc} (retry in {delay:g} s)", now)
        due = (self._last_account_poll_at is None
               or now - self._last_account_poll_at >= float(self.config.poll_interval_s)
               or (not self.startup_reconciled
                   and now - self._last_account_poll_at >= self.options.min_wake_interval_s))
        if not due:
            return
        weight = self.port.request_weight(ENDPOINT_ACCOUNT) + self.port.request_weight(ENDPOINT_ACTIVE_ORDERS)
        if not self._can_spend(now, weight):
            self._error("WEIGHT_BUDGET", "account poll delayed by weight budget", now)
            return
        self._charge(now, weight)
        self._last_account_poll_at = now
        await self._read_position(now)
        issued, seq = self.clock(), self._next_seq()
        try:
            self.active_rows = await self.port.active_orders()
            self.active_at, self.active_seq = issued, seq
        except Exception as exc:  # noqa: BLE001
            self.active_rows = None
            self._error("ACTIVE_ORDERS_UNAVAILABLE", f"{type(exc).__name__}: {exc}", now)

    def _rules_allowance(self, rules_weight: int) -> float:
        """Per-minute weight the rules refresh may use: what the account-poll and history-scan cadence leaves of
        the budget, but at least one read per minute (rules are lower priority than proofs, NG-HIST-003)."""
        per_poll = sum(self.port.request_weight(e) for e in (ENDPOINT_ACCOUNT, ENDPOINT_ACTIVE_ORDERS,
                                                             ENDPOINT_TRADES, ENDPOINT_INACTIVE_ORDERS))
        need = 60.0 / max(float(self.config.poll_interval_s), 1.0) * per_poll
        return max(float(rules_weight), self.options.weight_budget_per_min - need)

    async def _read_position(self, now: float) -> None:
        """The read is stamped with the time and causal sequence at which it was *requested*: it reflects at least
        every fill committed before that point."""
        issued, seq = self.clock(), self._next_seq()
        try:
            self.position = await self.port.position()
            self.position_at, self.position_seq = issued, seq
            self._position_reads.append((seq, issued, self.position.net_base))
        except Exception as exc:  # noqa: BLE001
            self._error("POSITION_UNAVAILABLE", f"{type(exc).__name__}: {exc}", now)

    def _position_fresh_for_ledger(self, now: float) -> bool:
        """A known position requested after the latest committed fill (and within the freshness bound)."""
        return (self.position is not None and not self._position_stale(now)
                and self.position_seq > self.fills_commit_seq)

    def _position_reconciled_now(self, now: float) -> bool:
        """NG-CELL-001 (5): account position == ledger P, judged on a read that postdates the ledger's fills."""
        if not self.bootstrapped or not self._position_fresh_for_ledger(now):
            return False
        ep = risk.endpoints_from_ledgers(self.effective_baseline, list(self.cells.values()))
        return self.position.net_base == ep.P and not self.store_entry_blockers

    async def _reread_position_for_release(self, now: float) -> None:
        """A history commit with new fills makes the tick's earlier position read unusable for release; when a
        cell waits only for condition (5), read the position again now (bounded by the weight budget) instead of
        starving releases while fills keep arriving."""
        if not self.bootstrapped or self.position_seq > self.fills_commit_seq:
            return
        waiting = any(ledger.current is not None and not _retry_only(ledger.current)
                      and ledger.can_release(True).ok for ledger in self.cells.values())
        weight = self.port.request_weight(ENDPOINT_ACCOUNT)
        if waiting and self._can_spend(now, weight):
            self._charge(now, weight)
            await self._read_position(now)

    # ================================================================================== history
    async def _scan_history(self, now: float) -> None:
        if not self.scanner.should_scan(now):
            return
        one_page_each = self.port.request_weight(ENDPOINT_TRADES) + self.port.request_weight(ENDPOINT_INACTIVE_ORDERS)
        if not self._can_spend(now, one_page_each):
            # Exhaustion delays new exposure (history goes stale), it never skips a proof (NG-HIST-003).
            self._error("WEIGHT_BUDGET", "history scan delayed by weight budget", now)
            return
        if self._walk_started_at is None:
            self._walk_started_at = now
            self._walk_start_seq = self._next_seq()
        result = await self.scanner.scan()
        self._charge(now, result.weight_used)
        for stream, pages in result.pages_read.items():
            self.last_scan_pages[stream] = pages
        in_progress = not result.complete and result.incomplete_reason in ("in_progress", "backoff")
        walk_started = self._walk_started_at
        if not in_progress:
            self._walk_started_at = None
        if self.bootstrapped and not in_progress and not result.complete and self._only_audited_conflicts(result):
            # Every contradiction of this walk was audited by the operator and every withheld row is already in
            # the ledger with the identical payload: the walk carries no unexplained evidence. It counts as
            # complete; without a high-water the walk floor moves to this walk's start (overlap covers lag).
            result = dataclasses.replace(result, complete=True, incomplete_reason=None)
            floor = _ms(walk_started) if walk_started is not None else _ms(now)
            self.meta.history_reset = {STREAM_TRADES: floor, STREAM_ORDERS: floor}
        if result.complete:
            started = walk_started if walk_started is not None else now
            self.complete_scans.append(ScanRecord(started_at=started, completed_at=now))
            self._last_walk_start_seq = self._walk_start_seq
            # A client-id-only WS signal cannot be matched to one trade row: a complete walk that started after it
            # covered it (a trade-id signal stays until its row is committed, so real lag stays visible, AC-13).
            # A trade-id signal that is not tied to one of our live orders (unknown CID, another market, a replay of
            # a settled order) and that a complete walk started >= settlement_delay_s after it did not find expires,
            # so lag never grows forever. A signal for our live order stays until committed: real lag stays
            # visible (AC-13).
            settle = float(self.config.settlement_delay_s)
            self.ws_pending = {label: at for label, at in self.ws_pending.items()
                               if at >= started or (self._is_trade_label(label) and (
                                   at + settle > started or self._own_live_label(label)))}
            self._ws_label_cid = {k: v for k, v in self._ws_label_cid.items() if k in self.ws_pending}
        if not in_progress:
            self._record_history_conflicts()
        if self.bootstrapped:
            try:
                self._apply_history(result, now, in_progress)
            except (StoreError, LedgerError):
                if not in_progress:
                    self._note_refused_batch(result)
                raise
        else:
            self._apply_prebootstrap(result)
        if result.complete:
            self.history_complete = True
            self.history_incomplete_reason = None
            self.last_complete_scan_at = now
            if not self.startup_reconciled and self.active_rows is not None and self.position is not None:
                self.startup_reconciled = True
            self._update_bootstrap_probe(now)
        elif not in_progress:
            self.history_complete = False
            self.history_incomplete_reason = None if result.incomplete_reason is None else redact(result.incomplete_reason)
            self.bootstrap_probe = None
            self._error("HISTORY_INCOMPLETE", f"{result.incomplete_reason}: {result.conflicts[:3]}", now)
            if self._walk_problems_attributed(result):
                self._scope_startup()

    def _row_ids(self, stream: str, row: Any) -> Tuple[Optional[str], Optional[str]]:
        if stream == STREAM_TRADES:
            client, exchange = row.own_client_order_id, row.own_exchange_order_id
        else:
            client, exchange = row.client_order_id, (row.order_index or row.order_id)
        return (None if client is None else str(client)), (str(exchange) if exchange else None)

    def _cell_of_ids(self, client: Optional[str], exchange: Optional[str]) -> Optional[int]:
        """Own cell of a history row by exact client order id, else exact exchange order id (never guessed)."""
        cid = int(client) if client is not None and client.isdigit() else None
        if cid not in self.order_meta and exchange:
            cid = next((leg.cid for leg in self.all_legs() if leg.exchange_order_id == exchange), None)
        return self.order_meta[cid].cell_id if cid in self.order_meta else None

    def _scope_startup(self) -> None:
        if not self.startup_reconciled and self.bootstrapped and self.active_rows is not None \
                and self.position is not None:
            self.startup_scoped = True

    _SCOPED_CONFLICTS = ("duplicate_key_payload_mismatch", "committed_payload_mismatch",
                         "trades_exceed_order_cumulative", "inactive_order_not_terminal")

    def _walk_problems_attributed(self, result) -> bool:
        """A finished walk whose only problems are row contradictions attributed to own cells (every other row of
        the walk was delivered): startup may proceed for the unaffected cells (M3). Boundary, retention, schema or
        unattributable problems keep the global block."""
        if not self.bootstrapped or result.incomplete_reason != REASON_CONFLICT or not result.conflicts:
            return False
        if any(not c.startswith(tuple(f"{REASON_CONFLICT}:{k}:" for k in self._SCOPED_CONFLICTS))
               for c in result.conflicts):
            return False
        blocked, unattributed = self._withheld_conflict_cells()
        return bool(blocked) and unattributed is None

    def _note_refused_batch(self, result) -> None:
        """A history batch the store refused (recurring): the cells of its rows wait (their evidence is not in the
        ledger); a row that is not exactly attributable blocks every TP. Only-attributed refusals let startup proceed
        for the other cells (M3)."""
        cells: Dict[int, str] = {}
        unattributed = None
        for stream, rows in ((STREAM_TRADES, result.new_trades), (STREAM_ORDERS, result.new_orders)):
            for row in rows:
                cell = self._cell_of_ids(*self._row_ids(stream, row))
                if cell is None:
                    unattributed = f"HISTORY_REFUSED_UNATTRIBUTED:{stream}"
                else:
                    cells.setdefault(cell, f"HISTORY_REFUSED:{stream} batch refused by the store")
        self.history_refused_cells, self.history_refused_unattributed = cells, unattributed
        if cells and unattributed is None and (result.complete or self._walk_problems_attributed(result)):
            self._scope_startup()

    def _only_audited_conflicts(self, result) -> bool:
        """A conflicted walk whose contradictions were all audited (``ack_late_evidence``) and whose withheld rows
        are all committed with identical payloads (e.g. a stale settled order row + the audited late executions).
        Any new or different row keeps the walk incomplete (AC-40)."""
        if result.incomplete_reason != REASON_CONFLICT or not result.conflicts:
            return False
        if any(c not in self.meta.acknowledged_conflicts
               or not c.startswith(f"{REASON_CONFLICT}:trades_exceed_order_cumulative:") for c in result.conflicts):
            return False
        domain = self.port.domain
        for row in self.scanner.last_conflicted_rows.get(STREAM_TRADES, []):
            if self.committed[STREAM_TRADES].get(row.dedupe_key(domain)) != trade_payload_fingerprint(row):
                return False
        for row in self.scanner.last_conflicted_rows.get(STREAM_ORDERS, []):
            if self.committed[STREAM_ORDERS].get(c_order_key(domain, row)) != order_payload_fingerprint(row):
                return False
        return True

    def _own_live_label(self, label: str) -> bool:
        cid = self._ws_label_cid.get(label)
        return cid is not None and not self._leg_settled(cid)

    def _is_trade_label(self, label: str) -> bool:
        """A ws_pending label is a trade id unless it names one of our CIDs."""
        return not (label.isdigit() and int(label) in self.order_meta)

    def _remember_committed(self, result) -> None:
        domain = self.port.domain
        for row in result.new_trades:
            self.committed[STREAM_TRADES][row.dedupe_key(domain)] = trade_payload_fingerprint(row)
            self._committed_trade_ids.add(row.trade_id_str)
            self.ws_pending.pop(row.trade_id_str, None)
            if row.own_client_order_id is not None:
                self.ws_pending.pop(str(row.own_client_order_id), None)
        for row in result.new_orders:
            self.committed[STREAM_ORDERS][c_order_key(domain, row)] = order_payload_fingerprint(row)

    def _apply_prebootstrap(self, result) -> None:
        """Before bootstrap every row is pre-cut; only the cut (high-water) matters and it stays in memory."""
        self.bootstrap_rows_seen += len(result.new_trades) + len(result.new_orders)
        self._remember_committed(result)
        if result.complete:
            if result.trades_high_water is not None:
                self.high_water[STREAM_TRADES] = result.trades_high_water
            if result.orders_high_water is not None:
                self.high_water[STREAM_ORDERS] = result.orders_high_water

    def _update_bootstrap_probe(self, now: float) -> None:
        if self.bootstrapped or self.position is None:
            return
        key = (str(self.position.net_base), self.high_water.get(STREAM_TRADES), self.high_water.get(STREAM_ORDERS))
        probe = self.bootstrap_probe
        if probe is not None and probe["key"] == key:
            probe["scans"] += 1
        else:
            self.bootstrap_probe = {"key": key, "scans": 1, "since": now, "scan_ms": _ms(now)}

    def _retention_bounds(self, c_stream: str, result) -> Tuple[int, int]:
        """(required boundary, oldest available) for a detected retention gap: the committed high-water row (or
        bootstrap floor) must be served, but the venue's retained history starts after it."""
        required = self.cursor_ts.get(c_stream) or self.meta.bootstrap_floor_ms or 0
        rows = result.new_trades if c_stream == STREAM_TRADES else result.new_orders
        oldest = min((r.timestamp_ms for r in rows), default=required + 1)
        return required, max(oldest, required + 1)

    def _apply_history(self, result, now: float, in_progress: bool) -> None:
        """Inbox + dedupe + attribution + leg transitions + cursor: ONE store transaction (NG-HIST-002)."""
        s = self.store
        now_ms = _ms(now)
        domain = self.b_engine.connector_domain
        rows: List[Any] = list(result.new_orders) + list(result.new_trades)
        order_rows = {b_order_key(domain, row): row for row in result.new_orders}
        conflicts = list(result.conflicts)
        touched_terminal: Dict[int, ExchangeOrderRow] = {}
        cursor_updates: List[CursorUpdate] = []
        if result.complete:
            for c_stream, hw in ((STREAM_TRADES, result.trades_high_water), (STREAM_ORDERS, result.orders_high_water)):
                if s.cursor(_C_TO_B[c_stream]).retention_gap_open:
                    # Only an audited reconciliation lifts a durable gap; until then the stream is not complete.
                    cursor_updates.append(CursorUpdate(stream=_C_TO_B[c_stream], complete=False,
                                                       incomplete_reason="retention gap awaiting audit"))
                    continue
                ts = self.cursor_ts.get(c_stream, 0)
                if hw is not None:
                    ts = max(ts, HighWaterMark.decode(hw).timestamp_ms)
                cursor_updates.append(CursorUpdate(stream=_C_TO_B[c_stream], high_water=hw, high_water_ts_ms=ts,
                                                   complete=True, last_full_scan_ms=now_ms))
        elif not in_progress:
            for b_stream in _C_TO_B.values():
                cursor_updates.append(CursorUpdate(stream=b_stream, complete=False,
                                                   incomplete_reason=redact(result.incomplete_reason)))
        if not rows and not cursor_updates and not conflicts:
            return

        def transitions(tx, res) -> None:
            for rec in res.applied:
                if rec.stream != B_ORDERS or rec.cid is None:
                    continue
                row = order_rows.get(rec.dedupe_key)
                meta = self.order_meta.get(rec.cid)
                if row is None or meta is None:
                    continue
                if meta.first_terminal_seen_ms is None:
                    meta.first_terminal_seen_ms = now_ms
                meta.terminal_row = order_row_to_json(row)
                s.kv_set(tx, f"om:{rec.cid}", meta.to_json())
                lr = s.leg(rec.cid)
                if lr is not None and not lr.final and lr.state not in (OrderState.TERMINAL_UNKNOWN,
                                                                        OrderState.INTENT):
                    s.set_leg_state(tx, rec.cid, OrderState.TERMINAL_UNKNOWN, reason="exact terminal row seen")
                touched_terminal[rec.cid] = row
            gaps = [c for c in conflicts if c.startswith("retention_gap")]
            others = [c for c in conflicts if not c.startswith("retention_gap")
                      and c not in self.meta.acknowledged_conflicts]
            for c_stream in sorted({g.split(":")[1] for g in gaps if g.count(":") >= 2} & set(_C_TO_B)):
                b_stream = _C_TO_B[c_stream]
                if s.cursor(b_stream).retention_gap_open:
                    continue
                # Durable per-stream gap (AC-53): blocks entries until an audited reconciliation names the stream.
                required, oldest = self._retention_bounds(c_stream, result)
                s.mark_retention_gap(tx, b_stream, required, oldest, actor=ACTOR)
            already = self.b_engine.manual_reconcile_required or bool(gaps)
            if others and not already:
                s.mark_manual_reconcile_required(tx, redact(f"history conflict: {others[0]}", 1900),
                                                 evidence={"conflicts": others[:10]})
            s.kv_set(tx, "engine_meta", self.meta.to_json())

        res = s.apply_history_batch(None, rows, cursor_updates, transitions, batch_id=f"scan-{now_ms}")
        # Projection only after the commit succeeded.
        committed_at = self.clock()
        self.history_refused_cells, self.history_refused_unattributed = {}, None
        if len(rows) > res.duplicates:                          # a row not committed before (any kind, M4)
            self.history_rows_commit_seq = self._next_seq()
        if res.new_fills:
            self.fills_commit_seq = self._next_seq()           # the ledger P moved: earlier position reads are stale
            for fill in res.new_fills:
                self._cell_fill_commit_ms[fill.cell_id] = _ms(committed_at)
        if touched_terminal:
            seq = self._next_seq()
            for cid in touched_terminal:
                self.terminal_commit_seq[cid] = seq
        late_cids = {f.cid for f in res.late_fills}
        for fill in res.new_fills:
            ledger = self.cells.get(fill.cell_id)
            identity = s.identity_for_cid(fill.cid)
            if ledger is not None and identity is not None:
                ledger.fills[fill.dedupe_key] = LedgerFill(identity=identity, qty=fill.size, price=fill.price,
                                                           side=fill.own_side)
            self.trades_by_cid.setdefault(fill.cid, []).append(self._trade_from_fill(fill))
        for cid in sorted(res.touched_cids | set(touched_terminal)):
            self._mirror(cid)
            if cid in late_cids:
                leg = self.leg_by_cid(cid)
                if leg is not None:
                    leg.late_evidence = True
        self.terminal_rows.update(touched_terminal)
        self._remember_committed(result)
        if result.complete:
            for c_stream, hw in ((STREAM_TRADES, result.trades_high_water), (STREAM_ORDERS, result.orders_high_water)):
                if hw is not None:
                    self.high_water[c_stream] = hw
                    self.cursor_ts[c_stream] = max(self.cursor_ts.get(c_stream, 0),
                                                   HighWaterMark.decode(hw).timestamp_ms)
            cleared = [c for c, hw in ((STREAM_TRADES, result.trades_high_water),
                                       (STREAM_ORDERS, result.orders_high_water))
                       if hw is not None and c in self.meta.history_reset]
            if cleared:
                for c_stream in cleared:
                    del self.meta.history_reset[c_stream]
                with s.transaction() as tx:
                    s.kv_set(tx, "engine_meta", self.meta.to_json())
        self.last_history_commit_at = committed_at
        self._refresh_store_facts()

    # ================================================================================== active list
    def _reconcile_active(self, now: float) -> None:
        """Active list proves acceptance; disappearance is never a terminal proof (TERMINAL_UNKNOWN)."""
        if self.active_rows is None or self.active_at is None or self.active_at == self._active_reconciled_at:
            return
        self._active_reconciled_at = self.active_at
        polled_ms = _ms(self.active_at)
        owned: Dict[int, ExchangeOrderRow] = {}
        unknown: List[ExchangeOrderRow] = []
        by_exchange = {leg.exchange_order_id: leg.cid for leg in self.all_legs() if leg.exchange_order_id}
        for row in self.active_rows:
            cid = row.client_order_id if row.client_order_id in self.order_meta else None
            if cid is None and (row.order_index or row.order_id) in by_exchange:
                cid = by_exchange[row.order_index or row.order_id]
            if cid is None:
                unknown.append(row)
            else:
                owned[cid] = row
        self.unknown_active = unknown
        if not self.bootstrapped:
            return
        s = self.store
        changed: Set[int] = set()
        evidence: List[Tuple[int, ExchangeOrderRow, Tuple, bool]] = []
        with s.transaction() as tx:
            ids = [r.client_order_id for r in unknown if r.client_order_id is not None]
            if ids:
                s.note_foreign_cids(tx, ids, "active_orders")
            for leg in self.all_legs():
                if leg.cid is None or leg.state == OrderState.INTENT:
                    continue
                row = owned.get(leg.cid)
                if row is not None:
                    if leg.cid in self.terminal_rows:
                        # The exact terminal row is committed: an active row requested before that commit is stale
                        # (REST reads are not atomic) and is never recorded as live evidence against it.
                        if self.active_seq < self.terminal_commit_seq.get(leg.cid, 0):
                            continue
                        if leg.state in FINAL_STATES:
                            s.mark_manual_reconcile_required(
                                tx, f"final order {leg.cid} ({leg.state.value}) is still active on the venue")
                        continue
                    if leg.state in FINAL_STATES:
                        s.mark_manual_reconcile_required(tx, f"final order {leg.cid} ({leg.state.value}) is still "
                                                             f"active on the venue")
                        continue
                    key = (row.order_index or row.order_id, row.filled_base_amount)
                    set_live = leg.state in (OrderState.SUBMIT_UNKNOWN, OrderState.TERMINAL_UNKNOWN)
                    if self._active_evidence_cache.get(leg.cid) != key or set_live:
                        evidence.append((leg.cid, row, key, set_live))
                elif leg.state in (OrderState.LIVE, OrderState.CANCEL_PENDING, OrderState.CANCEL_UNKNOWN) \
                        and self._sent_before(leg.cid, polled_ms):
                    s.set_leg_state(tx, leg.cid, OrderState.TERMINAL_UNKNOWN,
                                    reason="absent from active orders (not a terminal proof)")
                    changed.add(leg.cid)
        refused = False
        for cid, row, key, set_live in evidence:
            # One transaction per order: a row the store refuses (it contradicts the leg) is isolated to its cell
            # instead of aborting the tick every poll (NG-OPS-001/002, D1-02).
            try:
                with s.transaction() as tx:
                    if self._active_evidence_cache.get(cid) != key:
                        s.record_order_evidence(tx, row, final=False)
                    if set_live:
                        s.set_leg_state(tx, cid, OrderState.LIVE, reason="seen in active orders")
            except InvalidTransitionError as exc:
                refused = True
                self._evidence_refused(cid, exc, now)
                continue
            self._active_evidence_cache[cid] = key
            changed.add(cid)
        for cid in changed:
            self._mirror(cid)
        if unknown or changed or refused:
            self._refresh_store_facts()

    def _evidence_refused(self, cid: int, exc: Exception, now: float) -> None:
        meta = self.order_meta.get(cid)
        cell = meta.cell_id if meta is not None else None
        reason = redact(f"ACTIVE_EVIDENCE_REFUSED:cid {cid}: {exc}", 300)
        self._error("ACTIVE_EVIDENCE_REFUSED", reason, now)
        if cell is None or cell in self.evidence_conflicts:
            return
        self.evidence_conflicts[cell] = reason
        with self.store.transaction() as tx:
            self.store.mark_manual_reconcile_required(
                tx, redact(f"history conflict: active orders contradict own order {cid}: {exc}", 1900),
                evidence={"cid": str(cid), "cell_id": cell})

    def _sent_before(self, cid: int, polled_ms: int) -> bool:
        """Only an active list fetched after the order was sent can say anything about its absence."""
        meta = self.order_meta.get(cid)
        return meta is not None and meta.intent_ms < polled_ms

    # ================================================================================== settlement
    def _settle(self, now: float) -> None:
        if not self.bootstrapped:
            return
        s = self.store
        conflicts: List[str] = []
        terminal: List[int] = []
        for leg in self.non_final_legs():
            if leg.cid is None or leg.cid not in self.terminal_rows:
                continue
            meta = self.order_meta[leg.cid]
            decision = evaluate_terminal_release(
                terminal_row=self.terminal_rows[leg.cid],
                owned_trades=self.trades_by_cid.get(leg.cid, []),
                history_complete=self.history_complete,
                first_terminal_seen_at=(None if meta.first_terminal_seen_ms is None
                                        else meta.first_terminal_seen_ms / 1000),
                complete_scans=list(self.complete_scans),
                now=now,
                settlement_delay_s=self.config.settlement_delay_s,
                settlement_scans=self.config.settlement_scans,
            )
            if decision.conflict:
                conflicts.append(f"cid {leg.cid}: {decision.reason}")
            elif decision.release:
                terminal.append(leg.cid)
        if terminal or conflicts:
            with s.transaction() as tx:
                for cid in terminal:
                    s.set_leg_state(tx, cid, OrderState.TERMINAL,
                                    reason="terminal row + complete scan + cumulative equality + settlement")
                    self._close_open_outbox(tx, cid)
                for c in conflicts:
                    s.mark_manual_reconcile_required(tx, f"settlement conflict {c}"[:1900])
            for cid in terminal:
                self._mirror(cid)
            if conflicts:
                self._refresh_store_facts()
        dust_changes: List[Tuple[int, int, Decimal]] = []
        releases: List[int] = []
        rules_ok = not grid.rules_blockers(self.rules)
        # NG-CELL-001 (5) judged now, on a position read that postdates every committed fill (not last tick's flag)
        reconciled = self._position_reconciled_now(now)
        self.position_reconciled = reconciled
        for ledger in self.cells.values():
            if rules_ok:
                for gen, dust in ledger.refresh_dust(self.rules).items():
                    dust_changes.append((ledger.cell_id, gen, dust))
            if ledger.current is not None and not _retry_only(ledger.current) \
                    and ledger.can_release(reconciled).ok:
                releases.append(ledger.cell_id)
        if not dust_changes and not releases:
            return
        released: List[Tuple[int, int]] = []
        with s.transaction() as tx:
            for cell_id, gen, dust in dust_changes:
                s.update_cycle(tx, self.grid_id, cell_id, gen, dust=dust)
            for cell_id in releases:
                gen = self.cells[cell_id].current.generation
                s.close_cycle(tx, self.grid_id, cell_id, gen, reason="NG-CELL-001 release proven")
                s.set_cell_state(tx, self.grid_id, cell_id, CellState.IDLE, blocker=None, reserved_slots=0)
                self.meta.obligations.pop(f"{cell_id}:{gen}", None)
                released.append((cell_id, gen))
            s.kv_set(tx, "engine_meta", self.meta.to_json())
        for cell_id, gen in released:
            self.cells[cell_id].release(reconciled)
            self.reservations[cell_id] = 0
            self.persisted_cell_state[cell_id] = (CellState.IDLE.value, None, 0)

    def _close_open_outbox(self, tx, cid: int) -> None:
        """A proven-final leg may still have an unresolved outbox row (crash between dispatch and result): record
        what is known so the cycle can be released (NOT_SENT only for a never-dispatched PENDING row)."""
        s = self.store
        for row in s.outbox_for_cid(cid):
            if row.status == "DONE":
                continue
            if row.status == "PENDING":
                result = TransportResult(TransportOutcome.NOT_SENT, "leg proven terminal; request never dispatched")
            else:
                result = TransportResult(TransportOutcome.UNKNOWN, "leg proven terminal by history; transport "
                                                                   "outcome of this request never recorded")
            s.record_transport_result(tx, cid, result, kind=row.kind)

    # ================================================================================== drift
    def _detect_drift(self, now: float) -> None:
        if not self.bootstrapped:
            self.endpoints = None
            self.position_reconciled = False
            return
        self.endpoints = risk.endpoints_from_ledgers(self.effective_baseline, list(self.cells.values()))
        if not self._position_fresh_for_ledger(now):
            # A position requested before the latest fill commit cannot be compared with the ledger (it may predate
            # a committed fill); the next read decides (NG-RISK-003).
            self.position_reconciled = False
            return
        venue = self.position.net_base
        ep = self.endpoints
        if self.history_complete and not self._history_stale(now) and not (ep.P_min <= venue <= ep.P_max):
            if not self.b_engine.manual_reconcile_required:
                with self.store.transaction() as tx:
                    self.store.mark_manual_reconcile_required(
                        tx, f"position drift: venue {venue} outside ledger reachable [{ep.P_min}, {ep.P_max}]; "
                            f"baseline audit required", evidence={"venue": str(venue), "P": str(ep.P)})
                self._refresh_store_facts()
        if venue == ep.P:
            self.position_gap_since = None
        elif self.position_gap_since is None:
            self.position_gap_since = now
        self.position_reconciled = self._position_reconciled_now(now)

    # ================================================================================== state
    def _outside_bounds(self) -> bool:
        if self.bid is None or self.ask is None:
            return False
        return self.bid > self.config.upper_price or self.ask < self.config.lower_price

    def _entry_side_allowed_by_bounds(self, side: Side) -> bool:
        if self.bid is None or self.ask is None or self.bid > self.ask:
            return False
        if not self.config.directional_outside_bounds_entries:
            return not self._outside_bounds()
        if self.ask < self.config.lower_price:
            return side == Side.SELL
        if self.bid > self.config.upper_price:
            return side == Side.BUY
        return True

    def _evaluate_state(self, now: float) -> None:
        if self.fatal_reason is not None:
            # Fail closed: the store refused to open/load (missing/corrupt DB with prior-run evidence, config
            # mutation, foreign lock...). Nothing is derived or sent.
            self.entry_blockers = self.tp_blockers = [self.fatal_reason]
            self.engine_state = EngineState.DEGRADED
            self.reasons = [self.fatal_reason]
            return
        entry: List[str] = []
        tp: List[str] = []
        if self.persistence_error is not None:
            entry.append("PERSISTENCE_FAILURE")
            tp.append("PERSISTENCE_FAILURE")
        for code in sorted(self.meta.freezes):
            entry.append(f"FROZEN:{code}")
            if code in _FREEZES_BLOCKING_TP:
                tp.append(f"FROZEN:{code}")
        entry.extend(f"STORE:{b}" for b in self.store_entry_blockers)
        self.tp_blocked_cells = self._scoped_tp_blocks()
        for blocker in (self.unattributed_conflict, self.history_refused_unattributed):
            if blocker:
                entry.append(blocker)
                tp.append(blocker)
        if not self.bootstrapped:
            entry.append("BASELINE_NOT_CONFIRMED")
            tp.append("BASELINE_NOT_CONFIRMED")
        if not self.meta.started:
            entry.append("AWAITING_START")
        if self.risk_policy_review_required:
            entry.append("RISK_POLICY_REVIEW_REQUIRED")
        if not self.startup_reconciled:
            entry.append("RECONCILING")
            if not self.startup_scoped:
                tp.append("RECONCILING")
        if self.unknown_active:
            entry.append("UNKNOWN_ACTIVE_ORDER")
            if not self.normal_since_start:
                tp.append("UNKNOWN_ACTIVE_ORDER_AT_STARTUP")
        if not self.history_complete:
            entry.append(f"HISTORY_INCOMPLETE:{self.history_incomplete_reason}")
        elif self._history_stale(now):
            entry.append("HISTORY_STALE")
        rule_errors = grid.rules_blockers(self.rules)
        if rule_errors:
            tp.extend(rule_errors)
        elif self._rules_stale(now):
            entry.append("RULES_STALE")
        if self.rules is not None:
            # The connector reports supports_limit/post_only=False unless the market is tradable: no new exposure,
            # and a TP that cannot be placed is visible instead of retried blindly.
            ordinary_limit_blocker = self.rules.ordinary_limit_blocker
            if ordinary_limit_blocker is not None:
                entry.append(ordinary_limit_blocker)
                if self.config.tp_order_type == OrderTypePolicy.LIMIT:
                    tp.append(ordinary_limit_blocker)
            elif not self.rules.supports_limit:
                entry.append("MARKET_NOT_TRADABLE")
                tp.append("MARKET_NOT_TRADABLE")
            if self.config.tp_order_type == OrderTypePolicy.LIMIT_MAKER and not self.rules.supports_post_only:
                tp.append("POST_ONLY_UNSUPPORTED")
            if self.config.entry_order_type == OrderTypePolicy.LIMIT_MAKER and not self.rules.supports_post_only:
                entry.append("POST_ONLY_UNSUPPORTED")
        if self.mid is None:
            entry.append("BOOK_UNKNOWN")
        self.margin_warning = None
        self.margin_required = None
        if self.endpoints is not None:
            self.margin_required = risk.required_margin_estimate(
                self.endpoints.P_min, self.endpoints.P_max, self.mid, self.config.leverage)
            entry.extend(risk.cap_violations(self.endpoints, self.limits))
        position = self.position
        fresh = position is not None and not self._position_stale(now)
        exposure, self.margin_warning = risk.exposure_blockers(risk.ExposureInputs(
            position_known=True if fresh else None,
            leverage_ok=None if position is None or position.leverage is None
            else position.leverage == self.config.leverage,
            position_mode_ok=None if position is None or position.margin_mode is None else True,
            market_active=None if self.mid is None or self.rules is None else bool(
                self.rules.supports_limit or self.rules.supports_post_only),
            rules=self.rules,
            history_complete=self.history_complete and not self._history_stale(now),
            account_identity_ok=self.port.account_index == self.config.account_index,
            data_age_s=None if self.position_at is None else Decimal(str(round(now - self.position_at, 3))),
            freshness_limit_s=self.config.history_freshness_s,
            margin_available=None if position is None else position.available_collateral,
            margin_required=self.margin_required if self.margin_required is not None else Decimal("0")))
        entry.extend(exposure)
        if self.position_gap_since is not None and now - self.position_gap_since > self.options.position_gap_grace_s:
            entry.append("POSITION_UNRECONCILED")
        if self._outside_bounds() and not self.config.directional_outside_bounds_entries:
            entry.append("OUTSIDE_BOUNDS")
        if self.meta.operator_paused:
            entry.append("OPERATOR_PAUSE")
        stopping = self.meta.stop_requested_ms is not None
        if stopping:
            entry.append("STOP_REQUESTED")
            tp.append("STOP_REQUESTED")
        self.entry_blockers = entry
        self.tp_blockers = tp

        late = any(c.kind == "LATE_FILL" for c in self.open_conflicts)
        conflict = any(c.kind != "LATE_FILL" for c in self.open_conflicts)
        if stopping:
            state = EngineState(self.meta.stop_outcome) if self.meta.stop_outcome else EngineState.STOPPING
        elif self.fatal_reason is not None or self.persistence_error is not None or FREEZE_CID in self.meta.freezes:
            # A latched CID allocation failure (collision/exhaustion) is a system fault, not a risk limit.
            state = EngineState.DEGRADED
        elif late or conflict or FREEZE_INVARIANT in self.meta.freezes or self.meta.history_conflicts or (
                self.b_engine is not None and self.b_engine.manual_reconcile_required
                and "conflict" in (self.b_engine.manual_reconcile_reason or "")):
            state = EngineState.FROZEN
        elif self.meta.freezes or self.store_entry_blockers or (self.unknown_active and not self.normal_since_start) \
                or any(e.startswith(("NET_CAP", "GROSS_CAP", "MARGIN_UNKNOWN", "ACCOUNT_IDENTITY")) for e in entry):
            state = EngineState.RISK_BLOCKED
        elif not self.bootstrapped:
            state = EngineState.BOOTSTRAPPING
        elif not self.startup_reconciled or not self.history_complete:
            state = EngineState.RECONCILING
        elif any(e in ("BOOK_UNKNOWN", "RULES_STALE", "HISTORY_STALE", "MARKET_NOT_TRADABLE", "POST_ONLY_UNSUPPORTED")
                 or e.startswith(("RULES_", "POSITION_KNOWN", "LEVERAGE_OK", "POSITION_MODE", "MARKET_ACTIVE",
                                  "FRESHNESS")) for e in entry + tp):
            state = EngineState.DEGRADED
        elif entry:
            state = EngineState.PAUSED
        else:
            state = EngineState.NORMAL
        # Anti-flap (W4): after DEGRADED the engine returns to NORMAL only after N consecutive clean ticks; until
        # then it stays DEGRADED (STABILIZING) and opens no new exposure, so engine_revision does not churn.
        if state == EngineState.DEGRADED:
            self._hold_normal_until_tick = self.ticks + self.options.normal_hysteresis_ticks
        elif state == EngineState.NORMAL and self.ticks < self._hold_normal_until_tick:
            state = EngineState.DEGRADED
            entry.append("STABILIZING")
        if state == EngineState.NORMAL:
            self.meta.ever_normal = True
            self.normal_since_start = True
        self.engine_state = state
        self.reasons = sorted(set(entry) | set(tp) | {f"CELL_TP_BLOCKED:{c}:{why}"
                                                      for c, why in sorted(self.tp_blocked_cells.items())})

    def _scoped_tp_blocks(self) -> Dict[int, str]:
        """Cells whose TPs must wait: a store conflict naming one of their CIDs, or (under a ledger-invariant
        freeze) a cell whose own ledger or durable quantities are inconsistent. A conflict without an own CID and
        an unattributable invariant freeze block only new entries (FROZEN); the store still refuses any TP above
        the history-confirmed obligation."""
        blocked: Dict[int, str] = {}
        for c in self.open_conflicts:
            if c.kind == "LATE_FILL" or c.cid not in self.order_meta:
                continue
            blocked.setdefault(self.order_meta[c.cid].cell_id, f"HISTORY_CONFLICT:{c.kind}")
        if FREEZE_INVARIANT in self.meta.freezes:
            for cell_id, why in self._invariant_cells().items():
                blocked.setdefault(cell_id, f"FROZEN:LEDGER_INVARIANT:{why}")
        for cell_id, why in list(self.evidence_conflicts.items()) + list(self.history_refused_cells.items()):
            blocked.setdefault(cell_id, why)
        withheld, self.unattributed_conflict = self._withheld_conflict_cells()
        for cell_id, why in withheld.items():
            blocked.setdefault(cell_id, why)
        return blocked

    def _withheld_conflict_cells(self) -> Tuple[Dict[int, str], Optional[str]]:
        """Rows the scanner withheld as contradicted (payload / committed-payload / cumulative conflicts) never
        reach the store, so no store conflict names their cell: map them to cells by own CID or exchange order id
        (their TPs wait, AC-40). A row that cannot be attributed blocks every new TP (fail closed). Rows whose
        conflict the operator audited are not blocking."""
        blocked: Dict[int, str] = {}
        unattributed: Optional[str] = None
        for entry in self.meta.history_conflicts.values():            # durable until audited (M2)
            cell = self._cell_of_ids(entry.get("client"), entry.get("exchange"))
            if cell is None:
                unattributed = f"HISTORY_CONFLICT_UNATTRIBUTED:{entry['stream']}"
            else:
                blocked.setdefault(cell, f"HISTORY_CONFLICT:unaudited {entry['stream']} contradiction")
        rows = self.scanner.last_conflicted_rows
        if not any(rows.values()):
            return blocked, unattributed
        last = self.scanner.last_result
        if last is not None and last.incomplete_reason == REASON_CONFLICT and self._only_audited_conflicts(last):
            return blocked, unattributed
        by_exchange = {leg.exchange_order_id: leg.cid for leg in self.all_legs() if leg.exchange_order_id}
        for stream, stream_rows in rows.items():
            for row in stream_rows:
                if self._audited_row(stream, row):
                    continue
                if stream == STREAM_TRADES:
                    client, exchange = row.own_client_order_id, row.own_exchange_order_id
                else:
                    client, exchange = row.client_order_id, (row.order_index or row.order_id)
                cid = client if client in self.order_meta else by_exchange.get(exchange)
                if cid in self.order_meta:
                    blocked.setdefault(self.order_meta[cid].cell_id, f"HISTORY_CONFLICT:withheld {stream} row")
                else:
                    unattributed = f"HISTORY_CONFLICT_UNATTRIBUTED:{stream}"
        return blocked, unattributed

    def _row_key_fp(self, stream: str, row: Any) -> Tuple[Tuple, str]:
        if stream == STREAM_TRADES:
            return row.dedupe_key(self.port.domain), trade_payload_fingerprint(row)
        return c_order_key(self.port.domain, row), order_payload_fingerprint(row)

    def _audited_row(self, stream: str, row: Any) -> bool:
        """The operator audited this key (``ack_history_conflict``): its accepted (committed) payload is
        authoritative and the other versions seen at audit time are audited noise (C3). A version never seen at
        the audit is new evidence and stays a conflict."""
        key, fp = self._row_key_fp(stream, row)
        audited = self.meta.audited_payloads.get(stream, {}).get(_key_label(key))
        return audited is not None and (fp == audited.get("accepted") or fp in audited.get("noise", []))

    def _committed_row(self, stream: str, key: Tuple) -> Optional[Any]:
        """The ledger's own copy of a committed row (for the operator summary of the committed version)."""
        domain = self.port.domain
        if stream == STREAM_TRADES:
            return next((r for rows in self.trades_by_cid.values() for r in rows if r.dedupe_key(domain) == key), None)
        return next((r for r in self.terminal_rows.values() if c_order_key(domain, r) == key), None)

    def _record_history_conflicts(self) -> None:
        """Durably record every unaudited payload contradiction of the finished walk (a key served with a payload
        other than the committed one, or with several payloads) with all versions seen, until an operator
        ``ack_history_conflict`` audits exactly that set. A contradiction the venue serves only once keeps its cell
        blocked and the engine FROZEN (M2, CR-2)."""
        by_key: Dict[Tuple[str, Tuple], Dict[str, Any]] = {}
        for stream, rows in self.scanner.last_conflicted_rows.items():
            for row in rows:
                if self._audited_row(stream, row):
                    continue
                key, fp = self._row_key_fp(stream, row)
                by_key.setdefault((stream, tuple(key)), {}).setdefault(fp, row)
        changed = False
        for (stream, key), versions in sorted(by_key.items(), key=lambda kv: (kv[0][0], _key_label(kv[0][1]))):
            committed = self.committed[stream].get(key)
            if len(versions) < 2 and (committed is None or committed in versions):
                continue                                   # not a payload contradiction (e.g. late evidence)
            label = _key_label(key)
            record_id = f"{stream}|{label}"
            entry = self.meta.history_conflicts.get(record_id)
            if entry is None:
                client, exchange = self._row_ids(stream, next(iter(versions.values())))
                entry = {"stream": stream, "key": label, "client": client, "exchange": exchange,
                         "committed": committed, "versions": {}}
                self.meta.history_conflicts[record_id] = entry
                changed = True
            if committed is not None and committed not in entry["versions"]:
                row = versions.get(committed) or self._committed_row(stream, key)
                entry["versions"][committed] = (_history_row_summary(stream, row) if row is not None
                                                else {"note": "committed ledger payload"})
                changed = True
            for fp, row in versions.items():
                if fp not in entry["versions"]:
                    entry["versions"][fp] = _history_row_summary(stream, row)
                    changed = True
        if changed:
            self._persist_meta_field("history_conflicts", self.meta.history_conflicts)

    def _persist_meta_field(self, name: str, value: Any) -> None:
        """Merge one engine-meta field into the stored meta in its own transaction (a later refusal and reload must
        not lose it)."""
        with self.store.transaction() as tx:
            stored = EngineMeta.from_json(self.store.kv_get("engine_meta") or {})
            setattr(stored, name, value)
            self.store.kv_set(tx, "engine_meta", stored.to_json())

    def _invariant_cells(self) -> Dict[int, str]:
        if self._invariant_cells_cache is None:
            bad: Dict[int, str] = {}
            for cell_id, ledger in self.cells.items():
                errors = ledger.check_invariants()
                if errors:
                    bad[cell_id] = errors[0][:200]
            for problem in (self.store.verify_ledger() if self.store is not None else []):
                m = re.match(r"leg (\d+):", problem) or None
                if m and int(m.group(1)) in self.order_meta:
                    bad.setdefault(self.order_meta[int(m.group(1))].cell_id, problem[:200])
                m = re.match(r"cycle [^/]+/(\d+)/\d+:", problem)
                if m:
                    bad.setdefault(int(m.group(1)), problem[:200])
            self._invariant_cells_cache = bad
        return self._invariant_cells_cache

    # ================================================================================== planning
    def _slot_admission(self) -> Optional[admission.AdmissionPlan]:
        if grid.rules_blockers(self.rules) or not self.cells:
            return None
        cells = []
        for cell_id in self.active_cell_ids():
            ledger = self.cells[cell_id]
            cur = ledger.current
            open_cycle = (cur is not None and not _retry_only(cur)) or any(
                c.has_open_obligation_or_orders() for c in ledger.cycles if c is not cur)
            eligible, why = True, None
            if not open_cycle:
                eligible, why = self._idle_eligibility(ledger)
            entry_live = open_cycle and any(not e.is_final for c in ledger.cycles for e in c.entries)
            cells.append(admission.CellAdmission(
                cell=ledger.spec, open_cycle=open_cycle, actual_orders=len(ledger.non_final_legs()),
                reserved_slots=self.reservations.get(cell_id, 0) if open_cycle else 0,
                eligible=eligible, blocker=why,
                slot_need=admission.slot_need_from_ledger(ledger, self.rules) if open_cycle else None,
                entry_live=entry_live))
        return admission.plan(cells, cap=self.config.max_active_orders, mid=self.mid, rules=self.rules,
                              order_amount_base=self.config.order_amount_base,
                              entries_allowed=not self.entry_blockers,
                              entries_blocker=self.entry_blockers[0] if self.entry_blockers else None)

    def _idle_eligibility(self, ledger: CellLedger) -> Tuple[bool, Optional[str]]:
        spec = ledger.spec
        if not self._entry_side_allowed_by_bounds(spec.entry_side):
            return False, "OUTSIDE_BOUNDS_SIDE"
        try:
            ledger.next_entry_identity()
        except LedgerError as exc:
            return False, f"LOCKED:{exc}"
        # (the full-Q quantity check is admission's FULL_Q_INVALID, also before any intent)
        why = self._latched(ledger.cell_id, LegRole.ENTRY) or self._off_tick(spec.entry_price)
        if why:
            self.cell_blockers[ledger.cell_id] = why
            return False, why
        if spec.entry_side == Side.BUY and (self.ask is None or spec.entry_price >= self.ask):
            return False, "ENTRY_WOULD_CROSS"
        if spec.entry_side == Side.SELL and (self.bid is None or spec.entry_price <= self.bid):
            return False, "ENTRY_WOULD_CROSS"
        return True, None

    def _qty_blocker(self, qty: Decimal, price: Decimal) -> Optional[str]:
        """The pre-send quantity check, run in planning before any intent/CID (never discovered after commit)."""
        if self.rules is None or grid.rules_blockers(self.rules):
            return None
        why = grid.order_qty_blocker(qty, price, self.rules)
        return None if why is None else f"PRESEND_QTY:{why}"

    def _off_tick(self, price: Decimal) -> Optional[str]:
        """A fixed grid price that is not on the venue's current tick can never be placed: blocked in planning,
        never re-issued as a dead intent every tick (NG-DB-005, AC-34)."""
        if self.rules is None or grid.rules_blockers(self.rules):
            return None
        try:
            grid.to_ticks(price, self.rules.tick_size)
        except grid.GridValidationError:
            return f"PRICE_NOT_ON_TICK:{price}@{self.rules.tick_size}"
        return None

    # ------------------------------------------------------------------ reject latches (NG-DB-005)
    def _rules_fingerprint(self) -> str:
        r = self.rules
        if r is None:
            return f"none|{self.fingerprint}"
        return "|".join(str(x) for x in (r.tick_size, r.size_step, r.min_base, r.min_notional, r.max_base,
                                         r.supports_limit, r.supports_post_only, self.fingerprint))

    def _latch(self, cid: int, reason: str) -> None:
        """A pre-send rejection or a definitive venue reject latches the cell/role: no new revision until the
        trading rules/config change or an exponential backoff elapses (never a new dead intent every tick)."""
        meta = self.order_meta.get(cid)
        if meta is None:
            return
        key = f"{meta.cell_id}:{meta.role}"
        previous = self.meta.reject_latches.get(key) or {}
        failures = int(previous.get("failures", 0)) + 1
        venue = reason.startswith("VENUE_REJECT")
        # A first definitive venue reject may be transient (e.g. post-only would cross): one immediate new revision
        # (AC-56); repeated rejects and deterministic pre-send refusals back off exponentially.
        exponent = failures - 2 if venue else failures - 1
        backoff = 0.0 if exponent < 0 else min(self.options.reject_backoff_initial_s * 2 ** exponent,
                                               self.options.reject_backoff_max_s)
        self.meta.reject_latches[key] = {"reason": reason[:300], "fingerprint": self._rules_fingerprint(),
                                         "retry_at_ms": self._now_ms() + int(backoff * 1000), "failures": failures}

    def _unlatch(self, cid: int) -> None:
        meta = self.order_meta.get(cid)
        if meta is not None:
            self.meta.reject_latches.pop(f"{meta.cell_id}:{meta.role}", None)

    def _latched(self, cell_id: int, role: LegRole) -> Optional[str]:
        key = f"{cell_id}:{role.value}"
        latch = self.meta.reject_latches.get(key)
        if latch is None:
            return None
        if latch.get("fingerprint") != self._rules_fingerprint():
            del self.meta.reject_latches[key]               # rules/config changed: try again
            return None
        if str(latch.get("reason", "")).startswith("PRESEND"):
            # deterministic pre-send refusal: no new revision until the rules/config fingerprint changes
            return f"{latch.get('reason')} (until the trading rules or config change)"
        wait_ms = int(latch.get("retry_at_ms", 0)) - self._now_ms()
        if wait_ms <= 0:
            return None                                     # backoff elapsed: one more attempt (failures kept)
        return f"{latch.get('reason')} (retry in {wait_ms / 1000:g} s)"

    def _arming_min(self, entry: Leg) -> Optional[Decimal]:
        meta = self.order_meta.get(entry.cid) if entry.cid is not None else None
        return None if meta is None or meta.arming_min_tp is None else Decimal(meta.arming_min_tp)

    def _tp_candidates(self, now_ms: int) -> List[Tuple[router.RouterIntent, int, Any]]:
        """TP dispatch items (an aggregate item carries its exact per-generation ``allocation``, AC-39).

        The router's TP FIFO sequence / SLO clock is the moment the obligation became dispatchable: the history
        commit of the fill that made it dispatchable (not the first below-minimum fill). While the cell's entry is
        live, a TP smaller than the minimum valid at arming time accumulates inside the cell's reserved slots
        (a runtime minimum decrease must not create more concurrent TPs than were reserved, nor emergency-cancel
        other cells' entries); once the entry is final everything is dispatched."""
        out = []
        for cell_id, ledger in sorted(self.cells.items()):
            plan = ledger.tp_obligation_to_dispatch(self.rules)
            if plan.blocker:
                self.cell_blockers[cell_id] = plan.blocker
            if cell_id in self.tp_blocked_cells:
                self.cell_blockers[cell_id] = self.tp_blocked_cells[cell_id]
                continue
            if not plan.items:
                continue
            why = (self._latched(cell_id, LegRole.TP)
                   or self._off_tick(ledger.spec.tp_price))
            if why:
                # durable, visible blocker; the obligation keeps its queue age (NG-CELL-002, AC-34)
                self.cell_blockers[cell_id] = why
                for item in plan.items:
                    for g in ([g for g, _ in item.allocation] if item.allocation else [item.generation]):
                        self.meta.obligations.setdefault(f"{cell_id}:{g}",
                                                         self._cell_fill_commit_ms.get(cell_id, now_ms))
                continue
            for index, item in enumerate(plan.items):
                qty_why = self._qty_blocker(item.qty, ledger.spec.tp_price)
                if qty_why:
                    self.cell_blockers[cell_id] = qty_why
                    continue
                if not item.allocation:
                    cycle = next(c for c in ledger.cycles if c.generation == item.generation)
                    live_entry = next((e for e in cycle.entries if not e.is_final), None)
                    arming = self._arming_min(live_entry) if live_entry is not None else None
                    if arming is not None and item.qty < arming:
                        self.cell_blockers[cell_id] = f"TP_ACCUMULATING:{item.qty}<{arming} while entry live"
                        continue
                gens = [g for g, _ in item.allocation] if item.allocation else [item.generation]
                start = self._cell_fill_commit_ms.get(cell_id, now_ms)
                since = min(self.meta.obligations.setdefault(f"{cell_id}:{g}", start) for g in gens)
                intent = router.RouterIntent(key=f"tp:{cell_id}:{item.generation}:{index}",
                                             side=ledger.spec.tp_side, price=ledger.spec.tp_price, qty=item.qty,
                                             role=LegRole.TP, cell_id=cell_id, seq=since)
                out.append((intent, cell_id, item))
        return out

    def _pending_cancel_outbox(self, cid: int) -> bool:
        return any(o.kind == "CANCEL" and o.status == "PENDING" for o in self.store.outbox_for_cid(cid))

    def _risk_reducing_cancels(self, cancels: Dict[int, str], now: float) -> None:
        """Cancels that only reduce exposure: allowed while frozen/blocked (never while persistence fails or before
        startup reconciliation): outside-bounds entries, due cancel retries, committed-but-unsent cancels."""
        outside = self._outside_bounds()
        for leg in self.non_final_legs():
            if leg.cid is None:
                continue
            if outside and leg.identity.role == LegRole.ENTRY and leg.state in CANCELLABLE and \
                    not self._entry_side_allowed_by_bounds(leg.side):
                cancels.setdefault(leg.cid, "OUTSIDE_BOUNDS")
            elif self._cancel_retry_due(leg, now):
                cancels.setdefault(leg.cid, "CANCEL_RETRY")
            elif leg.state == OrderState.CANCEL_PENDING and self._pending_cancel_outbox(leg.cid):
                cancels.setdefault(leg.cid, "RESUME_PENDING_CANCEL")

    async def _act(self, now: float) -> None:
        self.cell_blockers = {}
        self.router_plan = None
        if not self.bootstrapped:
            self.admission_plan = None
            return
        cancels: Dict[int, str] = {}
        if self.meta.stop_requested_ms is not None:
            await self._withdraw_pending_intents("STOP")
            for leg in self.non_final_legs():
                if leg.state in CANCELLABLE or self._cancel_retry_due(leg, now) or (
                        leg.state == OrderState.CANCEL_PENDING and self._pending_cancel_outbox(leg.cid)):
                    cancels[leg.cid] = "STOP"
            await self._dispatch_cancels(cancels, now)
            self.admission_plan = self._slot_admission()
            return
        if self.tp_blockers:
            self.admission_plan = self._slot_admission()
            if "PERSISTENCE_FAILURE" not in self.tp_blockers and (self.startup_reconciled or self.startup_scoped):
                self._risk_reducing_cancels(cancels, now)
                await self._dispatch_cancels(cancels, now)
            return
        self._risk_reducing_cancels(cancels, now)
        plan = self._slot_admission()
        self.admission_plan = plan
        if plan is None:
            await self._resume_pending_intents(now)
            await self._dispatch_cancels(cancels, now)
            return
        self._persist_reservations(plan)
        now_ms = self._now_ms()
        tp_items = self._tp_candidates(now_ms)
        entry_items = []
        for rank, cell_id in enumerate(plan.newly_armed):
            spec = self.cells[cell_id].spec
            if not self._entry_side_allowed_by_bounds(spec.entry_side):
                self.cell_blockers[cell_id] = "OUTSIDE_BOUNDS_SIDE"
                continue
            entry_items.append(router.RouterIntent(key=f"entry:{cell_id}", side=spec.entry_side,
                                                   price=spec.entry_price, qty=self.config.order_amount_base,
                                                   role=LegRole.ENTRY, cell_id=cell_id, seq=now_ms * 1000 + rank))
        owned = [router.RouterOrder.from_leg(leg) for leg in self.non_final_legs()]
        ep = risk.endpoints_from_ledgers(self.effective_baseline, list(self.cells.values()))
        # headroom = effective cap - actual orders is a hard ceiling whatever the reservations say (NG-RISK-005,
        # AC-44): past it a TP obtains a real slot only through the router's emergency entry-cancel path.
        budget = router.SlotBudget.from_plan(plan)
        rplan = router.plan_submits([i for i, *_ in tp_items] + entry_items, owned, endpoints=ep,
                                    limits=self.limits, slots=budget, mid=self.mid,
                                    entries_allowed=not self.entry_blockers,
                                    entries_blocker=self.entry_blockers[0] if self.entry_blockers
                                    else "ENTRIES_BLOCKED",
                                    owed=risk.obligation_totals(list(self.cells.values())))
        self.router_plan = rplan
        blocked_now: Dict[str, str] = {}
        for action in rplan.actions:
            if action.kind == router.ActionKind.CANCEL:
                cancels.setdefault(int(action.key), action.reason)
            elif action.kind == router.ActionKind.BLOCKED:
                self.cell_blockers[int(action.key.split(":")[1])] = action.reason
                blocked_now[action.key] = action.reason
            elif action.kind == router.ActionKind.WAIT:
                self.cell_blockers.setdefault(int(action.key.split(":")[1]), action.reason)
        if blocked_now:
            # Operator-required freeze, detailed per obligation (TP key -> reason), cleared by ack_risk_blocked.
            self.meta.risk_blocked.update(blocked_now)
            self.meta.freezes[FREEZE_RISK_BLOCKED] = "; ".join(
                f"{k}={v}" for k, v in sorted(self.meta.risk_blocked.items()))[:1900]
        submits = set(rplan.submits)
        # 1. Every router-approved TP intent is committed durably BEFORE any transport is awaited (NG-CELL-002 SLO).
        ready: List[Tuple[int, int, SubmitRequest]] = []
        for intent, cell_id, item in tp_items:
            if intent.key not in submits:
                continue
            if self._submits_this_tick >= self.options.max_submits_per_tick or self.persistence_error:
                self.cell_blockers.setdefault(cell_id, "TP_WAITS_THROTTLE")
                continue
            if len(self.non_final_legs()) >= plan.slots.cap:
                self.cell_blockers.setdefault(cell_id, "TP_WAITS_VENUE_CAP")
                continue
            committed = self._commit_intent(self.cells[cell_id], LegRole.TP, item.generation, item.qty, now,
                                            since_ms=intent.seq, allocation=item.allocation)
            if committed is not None:
                ready.append(committed)
        # 2. Transports: exits first, then withdrawals, resumed intents, cancels and finally new entries.
        for cid, outbox_id, req in ready:
            await self._dispatch_intent(cid, outbox_id, req)
        for key in rplan.withdraws:
            await self._withdraw(int(key), "ROUTER_WITHDRAW")
        await self._resume_pending_intents(now)
        await self._dispatch_cancels(cancels, now)
        for intent in entry_items:
            if intent.key not in submits:
                continue
            if self._submits_this_tick >= self.options.max_submits_per_tick or self.persistence_error \
                    or len(self.non_final_legs()) >= plan.slots.cap:
                break
            await self._submit(self.cells[intent.cell_id], LegRole.ENTRY, None, intent.qty, now,
                               reserved_slots=plan.reservations.get(intent.cell_id, 0))

    def _persist_reservations(self, plan: admission.AdmissionPlan) -> None:
        changed = {cell_id: slots for cell_id, slots in plan.reservations.items()
                   if self.reservations.get(cell_id, 0) != slots and self.cells[cell_id].current is not None}
        if not changed:
            return
        with self.store.transaction() as tx:
            for cell_id, slots in changed.items():
                self.store.set_cell_state(tx, self.grid_id, cell_id, self.cells[cell_id].primary_state(),
                                          reserved_slots=slots)
        for cell_id, slots in changed.items():
            self.reservations[cell_id] = slots

    def _cancel_retry_due(self, leg: Leg, now: float) -> bool:
        if leg.state != OrderState.CANCEL_UNKNOWN or leg.cid is None:
            return False
        meta = self.order_meta.get(leg.cid)
        if meta is None or meta.cancel_sent_ms is None:
            return True
        still_active = any(r.client_order_id == leg.cid for r in (self.active_rows or []))
        return still_active and now - meta.cancel_sent_ms / 1000 >= self.options.cancel_retry_s

    # ================================================================================== dispatch
    async def _call(self, coro) -> TransportResult:
        self.transport_calls += 1
        try:
            result = await asyncio.wait_for(coro, timeout=self.options.transport_timeout_s)
        except asyncio.TimeoutError:
            return TransportResult(TransportOutcome.UNKNOWN, "transport timeout")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - an ordinary exception is NOT a rejection (NG-DB-005)
            return TransportResult(TransportOutcome.UNKNOWN, redact(f"transport exception {type(exc).__name__}: {exc}"))
        if not isinstance(result, TransportResult):
            return TransportResult(TransportOutcome.UNKNOWN, "malformed transport result")
        if result.detail:
            result = dataclasses.replace(result, detail=redact(result.detail))
        return result

    def _pre_send_blocker(self, req: SubmitRequest) -> Optional[str]:
        if req.reduce_only:
            return "REDUCE_ONLY_FORBIDDEN"
        if req.order_type not in (OrderTypePolicy.LIMIT_MAKER, OrderTypePolicy.LIMIT):
            return "ORDER_TYPE_FORBIDDEN"
        rules = self.rules
        if rules is None or grid.rules_blockers(rules):
            return "RULES_UNKNOWN"
        ordinary_limit_blocker = rules.ordinary_limit_blocker
        meta = self.order_meta.get(req.client_order_id)
        if ordinary_limit_blocker is not None \
                and (meta is None or meta.role == LegRole.ENTRY.value or req.order_type == OrderTypePolicy.LIMIT):
            return ordinary_limit_blocker
        if ordinary_limit_blocker is None and not rules.supports_limit:
            return "MARKET_NOT_TRADABLE"
        if req.order_type == OrderTypePolicy.LIMIT_MAKER and not rules.supports_post_only:
            return "POST_ONLY_UNSUPPORTED"
        try:
            grid.to_ticks(req.price, rules.tick_size)
        except grid.GridValidationError as exc:
            return f"PRICE_NOT_ON_TICK:{exc}"
        return grid.order_qty_blocker(req.amount, req.price, rules)

    def _is_foreign_cid(self, cid: int) -> bool:
        return any(r.client_order_id == cid for r in (self.active_rows or []))

    async def _submit(self, ledger: CellLedger, role: LegRole, generation: Optional[int], qty: Decimal, now: float,
                      since_ms: Optional[int] = None, reserved_slots: int = 0,
                      allocation: Optional[Tuple[Tuple[int, Decimal], ...]] = None) -> bool:
        committed = self._commit_intent(ledger, role, generation, qty, now, since_ms=since_ms,
                                        reserved_slots=reserved_slots, allocation=allocation)
        if committed is None:
            return False
        await self._dispatch_intent(*committed)
        return True

    def _commit_intent(self, ledger: CellLedger, role: LegRole, generation: Optional[int], qty: Decimal, now: float,
                       since_ms: Optional[int] = None, reserved_slots: int = 0,
                       allocation: Optional[Tuple[Tuple[int, Decimal], ...]] = None
                       ) -> Optional[Tuple[int, int, SubmitRequest]]:
        """Intent + CID + reservation committed BEFORE transport; the result is committed after (NG-DB-002).
        Returns ``(cid, outbox_id, request)`` to dispatch, or None when the store refused the intent.

        An aggregate TP (``allocation``) is hosted by its newest generation; the exact per-generation shares are
        stored durably with the intent (``allocations`` rows) and mirrored on the WS-A leg (AC-39)."""
        s = self.store
        rules = self.rules
        spec = ledger.spec
        now_ms = self._now_ms()
        if role == LegRole.ENTRY:
            order_type = self.config.entry_order_type
            side, price, expiry_ms = spec.entry_side, spec.entry_price, None
        else:
            order_type = self.config.tp_order_type
            side, price = spec.tp_side, spec.tp_price
            expiry_ms = now_ms + self.config.tp_gtt_seconds * 1000
        self.meta.seq += 1
        seq = self.meta.seq
        try:
            with s.transaction() as tx:
                if role == LegRole.ENTRY:
                    if ledger.current is None:
                        s.open_cycle(tx, self.grid_id, ledger.cell_id, planned_amount=self.config.order_amount_base)
                    identity = ledger.next_entry_identity()
                else:
                    identity = ledger.next_tp_identity(generation)
                cid = s.allocate_cid(tx, identity, is_foreign_cid=self._is_foreign_cid)
                req = SubmitRequest(client_order_id=cid, side=side, price=price, amount=qty, order_type=order_type,
                                    reduce_only=False, expiry_ms=expiry_ms)
                intent = s.record_intent(tx, identity, req, Reservation(side=side, amount=qty, slots=1),
                                         reason=f"{role.value} intent", allocations=allocation)
                if role == LegRole.ENTRY:
                    leg = ledger.begin_entry(cid, rules, order_type=order_type, seq=seq)
                    s.set_cell_state(tx, self.grid_id, ledger.cell_id, CellState.ENTRY_INTENT, blocker=None,
                                     reserved_slots=max(reserved_slots, 1))
                else:
                    leg = ledger.add_tp_intent(qty, cid, rules, generation=generation, order_type=order_type,
                                               expiry_ms=expiry_ms, seq=seq, allocation=allocation)
                if leg.identity != identity:
                    raise LedgerError(f"identity drift {leg.identity} != {identity}")
                meta = OrderMeta(cid=cid, cell_id=ledger.cell_id, generation=identity.generation, role=role.value,
                                 intent_ms=now_ms, seq=seq, obligation_ms=since_ms)
                if role == LegRole.ENTRY:
                    meta.arming_min_tp = str(grid.min_valid_order_qty(rules, spec.tp_price))
                if role == LegRole.TP:
                    remaining = ledger.tp_obligation_to_dispatch(rules)
                    still_owed = {g for i in remaining.items
                                  for g in ([g for g, _ in i.allocation] if i.allocation else [i.generation])}
                    for gen in ([g for g, _ in allocation] if allocation else [identity.generation]):
                        if gen not in still_owed:
                            self.meta.obligations.pop(f"{ledger.cell_id}:{gen}", None)
                s.kv_set(tx, f"om:{cid}", meta.to_json())
                s.kv_set(tx, "engine_meta", self.meta.to_json())
        except EntryBlockedError as exc:
            self.reload_needed = True
            self.cell_blockers[ledger.cell_id] = f"STORE_ENTRY_BLOCKED:{exc}"
            return None
        except InvalidTransitionError as exc:
            # The durable ledger refuses this intent (e.g. a TP for a released cycle re-opened by late evidence):
            # nothing was sent; keep the obligation visible on the cell instead of freezing every cell.
            self.reload_needed = True
            self.cell_blockers[ledger.cell_id] = f"STORE_REFUSED_INTENT:{exc}"
            self._error("STORE_REFUSED_INTENT", f"cell {ledger.cell_id}: {exc}", now)
            return None
        except CidAllocationError as exc:
            self.reload_needed = True
            self.meta.freezes[FREEZE_CID] = f"{type(exc).__name__}: {exc}"
            self._sticky_freezes[FREEZE_CID] = self.meta.freezes[FREEZE_CID]
            if isinstance(exc, CidCollisionError):
                self.meta.colliding_cid = exc.cid
                self._sticky_meta["colliding_cid"] = exc.cid
            self._error("CID_ALLOCATION", str(exc), now)
            return None
        except (StoreError, LedgerError):
            self.reload_needed = True
            raise
        self.order_meta[cid] = meta
        if role == LegRole.ENTRY:
            self.reservations[ledger.cell_id] = max(reserved_slots, 1)
            self.persisted_cell_state[ledger.cell_id] = (CellState.ENTRY_INTENT.value, None,
                                                         self.reservations[ledger.cell_id])
        self._submits_this_tick += 1
        if role == LegRole.TP and since_ms is not None:
            # measured at the actual durable commit, against the moment the obligation became dispatchable
            self.tp_latencies.append((cid, max(0, self._now_ms() - since_ms) / 1000))
        return cid, intent.outbox_id, req

    async def _dispatch_intent(self, cid: int, outbox_id: int, req: SubmitRequest) -> None:
        s = self.store
        blocker = self._pre_send_blocker(req)
        if blocker is not None:
            # Proven no transport call: the intent is released (NG-DB-005, AC-56) and the cell/role latched.
            s.record_transport_result(None, cid, TransportResult(TransportOutcome.NOT_SENT,
                                                                 f"pre-send validation: {blocker}"), kind="SUBMIT")
            self._latch(cid, f"PRESEND:{blocker}")
            self._mirror(cid)
            return
        with s.transaction() as tx:
            s.mark_dispatching(tx, outbox_id)
        self._mirror(cid)
        s.fault_point("before_transport")
        result = await self._call(self.port.submit(req))
        s.fault_point("after_transport")
        s.record_transport_result(None, cid, result, kind="SUBMIT")
        meta = self.order_meta.get(cid)
        if meta is not None and result.detail:
            meta.transport_detail = result.detail[:500]
        if result.outcome == TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL:
            self._latch(cid, f"VENUE_REJECT:{result.detail or 'definitive zero-fill reject'}")
        elif result.outcome == TransportOutcome.ACCEPTED:
            self._unlatch(cid)
        self._mirror(cid)

    async def _withdraw(self, cid: int, reason: str) -> None:
        leg = self.leg_by_cid(cid)
        if leg is None or leg.state != OrderState.INTENT:
            return
        self.store.record_transport_result(None, cid, TransportResult(TransportOutcome.NOT_SENT, reason),
                                           kind="SUBMIT")
        self._mirror(cid)

    def _pending_submit_outbox(self, cid: int) -> Optional[Any]:
        outbox = [o for o in self.store.outbox_for_cid(cid) if o.kind == "SUBMIT"]
        return outbox[-1] if outbox and outbox[-1].status == "PENDING" else None

    async def _withdraw_pending_intents(self, reason: str) -> None:
        for leg in self.non_final_legs():
            if leg.state == OrderState.INTENT and leg.cid is not None and self._pending_submit_outbox(leg.cid):
                await self._withdraw(leg.cid, f"{reason}: proven unsent intent withdrawn")

    async def _resume_pending_intents(self, now: float) -> None:
        """Restart after a crash between intent and dispatch: the PENDING outbox row proves the venue never saw
        the request, so it is dispatched with the SAME CID (never a new one, AC-16) or withdrawn if no longer
        allowed."""
        for leg in self.non_final_legs():
            if leg.state != OrderState.INTENT or leg.cid is None:
                continue
            pending = self._pending_submit_outbox(leg.cid)
            if pending is None:
                continue
            if leg.identity.role == LegRole.ENTRY and (
                    self.entry_blockers or not self._entry_side_allowed_by_bounds(leg.side)):
                await self._withdraw(leg.cid, "entries blocked after restart")
                continue
            await self._dispatch_intent(leg.cid, pending.id, self.store.leg(leg.cid).submit_request())

    async def _dispatch_cancels(self, cancels: Dict[int, str], now: float) -> None:
        s = self.store
        for cid, reason in sorted(cancels.items()):
            if self.persistence_error:
                return
            leg = self.leg_by_cid(cid)
            if leg is None or leg.state in FINAL_STATES or leg.state == OrderState.INTENT:
                continue
            meta = self.order_meta[cid]
            with s.transaction() as tx:
                outbox = s.record_cancel_intent(tx, cid, reason)
                meta.cancel_reason = reason
                meta.cancel_requested_ms = _ms(now)
                s.kv_set(tx, f"om:{cid}", meta.to_json())
            self._mirror(cid)
            with s.transaction() as tx:
                # An unresolved cancel row that was already DISPATCHED (a process died after the dispatch mark)
                # is re-sent for the SAME order: cancelling a known order is idempotent at the venue.
                s.mark_dispatching(tx, outbox.id, resend_same_cid=outbox.status == "DISPATCHED")
            self._mirror(cid)
            meta.cancel_sent_ms = self._now_ms()
            s.fault_point("before_transport")
            result = await self._call(self.port.cancel(cid, leg.exchange_order_id))
            s.fault_point("after_transport")
            s.record_transport_result(None, cid, result, kind="CANCEL")
            with s.transaction() as tx:
                s.kv_set(tx, f"om:{cid}", meta.to_json())
            self._mirror(cid)

    # ================================================================================== stop
    def _finish_stop(self, now: float) -> None:
        if self.meta.stop_requested_ms is None:
            return
        open_legs = self.non_final_legs()
        previous = self.meta.stop_outcome
        if not open_legs and self.history_complete and self._position_fresh_for_ledger(now):
            # STOPPED / STOPPED_WITH_INVENTORY only on a fresh known position; the baseline B counts as inventory.
            inventory = any(c.open_obligation != 0 or c.dust > 0
                            for ledger in self.cells.values() for c in ledger.cycles)
            if self.position.net_base != 0 or (self.effective_baseline or ZERO) != 0:
                inventory = True
            outcome: Optional[EngineState] = (EngineState.STOPPED_WITH_INVENTORY if inventory
                                              else EngineState.STOPPED)
        elif any(leg.state in (OrderState.CANCEL_UNKNOWN, OrderState.SUBMIT_UNKNOWN) for leg in open_legs) or \
                now - self.meta.stop_requested_ms / 1000 > self.options.stop_uncertain_after_s:
            outcome = EngineState.STOP_UNCERTAIN
        else:
            outcome = None
        new_value = outcome.value if outcome is not None else None
        if new_value != previous:
            self.meta.stop_outcome = new_value
            with self.store.transaction() as tx:
                self.store.kv_set(tx, "engine_meta", self.meta.to_json())
                self.store.record_audit(tx, "stop_outcome", ACTOR, {"outcome": new_value,
                                                                    "open_orders": len(open_legs)})

    # ================================================================================== snapshot
    def preview(self) -> Dict[str, Any]:
        from hummingbot.strategy_v2.executors.neutral_grid_executor.snapshot import build_preview
        return build_preview(self)

    def _commit_snapshot(self, now: float) -> None:
        from hummingbot.strategy_v2.executors.neutral_grid_executor.snapshot import build_snapshot
        s = self.store
        reason = "; ".join(self.reasons)[:1900] or None
        with s.transaction() as tx:
            s.set_engine_state(tx, self.engine_state, reason, pause_reason=self.meta.pause_reason,
                               stop_reason=self.meta.stop_reason)
            s.kv_set(tx, "engine_meta", self.meta.to_json())
            if self.bootstrapped:
                for cell_id, ledger in self.cells.items():
                    state = ledger.primary_state()
                    blocker = self.cell_blockers.get(cell_id)
                    persisted = (state.value, blocker, self.reservations.get(cell_id, 0))
                    if self.persisted_cell_state.get(cell_id) != persisted:
                        s.set_cell_state(tx, self.grid_id, cell_id, state, blocker=blocker,
                                         reserved_slots=self.reservations.get(cell_id, 0))
                        self.persisted_cell_state[cell_id] = persisted
            self.b_engine = s.engine()
            stored = s.write_snapshot(tx, build_snapshot(self, now))
        self.last_snapshot = stored.payload

    def current_snapshot(self) -> Dict[str, Any]:
        from hummingbot.strategy_v2.executors.neutral_grid_executor.snapshot import build_snapshot
        return build_snapshot(self, self.clock())

    def latest_committed_snapshot(self) -> Optional[Dict[str, Any]]:
        if self.store is None or self.store.closed:
            return self.last_snapshot
        stored = self.store.latest_snapshot()
        return None if stored is None else stored.payload

    def enqueue(self, kind: CommandKind, payload: Optional[Dict[str, Any]] = None, *, key: str,
                expected: Optional[Tuple[int, int]] = None):
        """Enqueue with the revisions of the committed engine row (UI/CLI semantics: stale => CONFLICT)."""
        eng = self.store.engine()
        config_rev, engine_rev = expected if expected is not None else (eng.config_revision, eng.engine_revision)
        return self.store.enqueue_command(key, kind, config_rev, engine_rev, payload or {})

    def cells_view(self) -> Sequence[CellLedger]:
        return [self.cells[i] for i in sorted(self.cells)]
