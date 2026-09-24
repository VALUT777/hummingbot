"""``NeutralGridExecutor``: the single V2 executor hosting the neutral grid engine (NG-ARCH-001).

One executor, one control-loop task, one engine for all cells (AC-04). Connector order events are only
WebSocket-style wake-up signals for the history poller; they never prove a fill or a terminal state.
The executor never cancels or places orders itself: every side effect goes through the engine's durable
outbox (intent before transport). A forced shutdown therefore does NOT run a best-effort cancel.
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from hummingbot.logger import HummingbotLogger
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.neutral_grid_executor.commands import new_idempotency_key
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CommandKind, CommandStatus, EngineState
from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import EngineOptions, NeutralGridExecutorConfig
from hummingbot.strategy_v2.executors.neutral_grid_executor.snapshot import format_status
from hummingbot.strategy_v2.models.executors import CloseType

EXECUTOR_TYPE = "neutral_grid_executor"
_TRANSIENT_BASELINE_ERRORS = {"BOOTSTRAP_NOT_READY", None}


def launcher_start_payload(config: NeutralGridExecutorConfig) -> Dict[str, Any]:
    """START as the launcher sends it after the operator typed the exact confirmation phrase (grid id + signed B):
    the same acknowledgement fields as the web START (AC-47). The launcher has no web preview; its preview id is a
    stable digest of what the operator confirmed."""
    digest = hashlib.sha256(f"{config.grid_id}|{config.expected_initial_position}|{config.id}".encode()).hexdigest()
    payload = {"source": "launcher", "risk_acknowledged": True, "baseline_acknowledged": True,
               "expected_initial_position": str(config.expected_initial_position), "preview_id": digest[:24]}
    if config.operator_resume_stop_ms is not None:
        # the operator typed the resume phrase naming exactly this durable stop (NG-OPS-003, AC-36)
        payload["resume_stop_ms"] = int(config.operator_resume_stop_ms)
    return payload


def engine_db_path(db_path: Optional[str], connector_name: str, trading_pair: str, connector: Any) -> str:
    """The engine ledger: an explicit path, else the store's default per account/market (the same identity as the
    host lock and prior-run marker, so a new grid there is an audited migration, never a second database)."""
    if db_path:
        return str(db_path)
    from hummingbot.strategy_v2.executors.neutral_grid_executor.store import EngineIdentity, default_db_path
    return str(default_db_path(EngineIdentity(connector_name=connector_name, connector_domain=connector.domain,
                                              account_index=connector.account_index, trading_pair=trading_pair)))


def open_ledger_readonly(db_path: str):
    """Read-only handle for pre-start reads. A ledger written by an earlier build (older schema, e.g. v2 before
    ``m0003``) is read with the migrations it already has: the executor's writer upgrades it on start, but the
    launcher must see its durable stop and CIDs BEFORE that (C6). Anything else fails loudly."""
    import sqlite3

    from hummingbot.strategy_v2.executors.neutral_grid_executor.migrations import MIGRATIONS
    from hummingbot.strategy_v2.executors.neutral_grid_executor.store import NeutralGridStore, SchemaVersionError
    try:
        return NeutralGridStore.open_readonly(db_path)
    except SchemaVersionError as exc:
        if "older than" not in str(exc):
            raise
        raw = sqlite3.connect(Path(db_path).absolute().as_uri() + "?mode=ro", uri=True)
        try:
            version = int(raw.execute("PRAGMA user_version").fetchone()[0])
        finally:
            raw.close()
        if not 1 <= version < len(MIGRATIONS):
            raise
        return NeutralGridStore.open_readonly(db_path, migrations=MIGRATIONS[:version])


def durable_stop_ms(db_path: Optional[str]) -> Optional[int]:
    """``stop_requested_ms`` of a durable STOP in an existing ledger (read-only, no lock), else None."""
    if not db_path or not Path(db_path).exists():
        return None
    store = open_ledger_readonly(db_path)
    try:
        meta = store.kv_get("engine_meta") or {}
    finally:
        store.close()
    value = meta.get("stop_requested_ms")
    return int(value) if value is not None else None


def has_cid_ownership(owner: Any) -> bool:
    return owner is not None and callable(getattr(owner, "register_history_reconciled_order", None)) \
        and callable(getattr(owner, "release_history_reconciled_order", None))


def register_durable_cids(owner: Any, db_path: Optional[str]) -> List[int]:
    """Before the connector's polling loops start: mark every durable CID that may have reached transport as
    engine-owned (``register_history_reconciled_order``) so Hummingbot's generic stop/exit/lost-order paths never
    cancel, poll or re-send it. Reads the ledger read-only (takes no lock); a missing database registers nothing
    (the engine itself fails closed on a missing DB with prior-run evidence)."""
    if not has_cid_ownership(owner) or not db_path or not Path(db_path).exists():
        return []
    from hummingbot.strategy_v2.executors.neutral_grid_executor.engine import transport_cids_of
    store = open_ledger_readonly(db_path)
    try:
        cids = transport_cids_of(store)
    finally:
        store.close()
    for cid in cids:
        owner.register_history_reconciled_order(cid)
    return cids


def register_executor_type() -> None:
    """Make the V2 orchestrator able to create this executor (idempotent, no global rewrite).

    Native registration in ``executor_orchestrator._executor_mapping`` / ``executors_info.AnyExecutorConfig``
    is requested from the integrator (see docs/neutral-grid/trace/ws-d-engine.md); until then this adds the
    class mapping at import time and ``executor_info`` below falls back to an unvalidated model.
    """
    from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
    ExecutorOrchestrator._executor_mapping.setdefault(EXECUTOR_TYPE, NeutralGridExecutor)


class NeutralGridExecutor(ExecutorBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, strategy, config: NeutralGridExecutorConfig, update_interval: float = 1.0,
                 max_retries: int = 10, *, port: Any = None, store: Any = None,
                 clock: Optional[Callable[[], float]] = None, options: Optional[EngineOptions] = None,
                 offline_demo: bool = False, cid_owner: Any = None):
        super().__init__(strategy=strategy, connectors=[config.connector_name], config=config,
                         update_interval=update_interval, max_retries=max_retries)
        self.config: NeutralGridExecutorConfig = config
        self._port = port
        self._store = store
        self._clock = clock
        self._options = options
        self._offline_demo = offline_demo
        self._cid_owner = cid_owner
        self.registered_cids: List[int] = []
        self.engine = None
        self.start_error: Optional[str] = None
        self._stop_requested = False        # Hummingbot asked this executor to stop (operator's own CLI intent)
        self._stop_command_sent = False     # ... and a STOP row was APPLIED by the engine (only then durable)
        self._stop_keys: List[str] = []
        self._start_key: Optional[str] = None
        self._start_attempts = 0
        self._start_refused = False
        self._migration_key: Optional[str] = None
        self._migration_attempts = 0
        self._migration_refused = False
        self._bootstrap_attempts = 0
        self._baseline_key: Optional[str] = None
        self._baseline_refused = False
        self._wakeup_callback = None
        self._wakeup_unsubscribe = None

    # ------------------------------------------------------------------ construction
    def _build_port(self):
        if self._port is not None:
            return self._port
        from hummingbot.strategy_v2.executors.neutral_grid_executor.lighter_port import LighterExchangePort
        connector = self.connectors[self.config.connector_name]
        return LighterExchangePort(connector, self.config.trading_pair)

    async def on_start(self):
        """Build the engine; never checks/uses budget-based sizing (no ``validate_sufficient_balance``)."""
        if not (self.config.enabled or self._offline_demo):
            self.start_error = "enabled=false: live start refused"
            self.logger().error(self.start_error)
            self.close_type = CloseType.FAILED
            self.stop()
            return
        from hummingbot.strategy_v2.executors.neutral_grid_executor.engine import NeutralGridEngine, open_engine
        port = self._build_port()
        clock = self._clock or time.time
        grid_config = self.config.to_grid_config(port.account_index)
        if self._store is not None:
            self.engine = NeutralGridEngine(grid_config, self._store, port, clock, options=self._options,
                                            offline_demo=self._offline_demo)
        else:
            # A refused store (missing/corrupt DB with prior-run evidence, lock held, config mutation) yields
            # a fail-closed DEGRADED engine: never a fresh bootstrap (AC-54).
            self.engine = open_engine(grid_config, self.config.db_path, port, clock=clock, options=self._options,
                                      offline_demo=self._offline_demo,
                                      allow_grid_migration=self.config.operator_confirmed_migration)
        if self.engine.fatal_reason is not None:
            self.start_error = self.engine.fatal_reason
            self.logger().error(f"Neutral grid engine is fail-closed: {self.start_error}")
            return
        self._take_cid_ownership(port)
        self._subscribe_wakeups(port)
        if self.config.operator_confirmed_start:
            self._start_key = f"launcher-start-{self.config.id}"
            try:
                self._enqueue(CommandKind.START, launcher_start_payload(self.config), key=self._start_key)
            except Exception as exc:  # noqa: BLE001 - _track_start re-sends a START whose row does not exist
                self.logger().warning(f"Neutral grid START could not be enqueued ({type(exc).__name__}); retrying")

    def _resolve_cid_owner(self, port) -> Any:
        if self._cid_owner is not None:
            return self._cid_owner
        candidates = [port]
        connectors = getattr(self, "connectors", None) or {}
        if isinstance(connectors, dict):
            candidates.append(connectors.get(self.config.connector_name))
        return next((c for c in candidates if has_cid_ownership(c)), None)

    def _take_cid_ownership(self, port) -> None:
        """CID orders are engine-owned: every durable CID that may have reached transport is registered with the
        connector (idempotent; the launcher already did it before polling started) and connector tracking of a
        CID stops once the engine proves its leg final (``release_history_reconciled_order``)."""
        owner = self._resolve_cid_owner(port)
        if owner is None:
            return
        self.registered_cids = self.engine.transport_cids()
        for cid in self.registered_cids:
            owner.register_history_reconciled_order(cid)
        self.engine.leg_final_hook = owner.release_history_reconciled_order
        self.engine.release_final_cids()

    def _subscribe_wakeups(self, port) -> None:
        """Private-stream activity only hints the history poller (coalesced); it is never evidence."""
        subscribe = getattr(port, "subscribe_history_wakeups", None)        # LighterExchangePort
        if callable(subscribe):
            self._wakeup_callback = lambda: self.engine.wake({"type": "private_stream"})
            subscribe(self._wakeup_callback)
            self._wakeup_unsubscribe = lambda: port.unsubscribe_history_wakeups(self._wakeup_callback)
            return
        add_listener = getattr(port, "add_ws_listener", None)                # FakeExchange (offline)
        if callable(add_listener):
            add_listener(self.engine.wake)
            self._wakeup_unsubscribe = lambda: port.remove_ws_listener(self.engine.wake)

    def on_stop(self):
        if self._wakeup_unsubscribe is not None:
            self._wakeup_unsubscribe()
            self._wakeup_unsubscribe = None
        store = self.engine.store if self.engine is not None else None
        if store is not None and not store.closed:
            # Releases the single-writer host lock and marks the owner row released; the ledger stays on disk.
            store.close()
        super().on_stop()

    async def validate_sufficient_balance(self):  # pragma: no cover - never used by this executor
        return None

    def _enqueue(self, kind: CommandKind, payload: Dict[str, Any], key: Optional[str] = None):
        if self.engine is None or self.engine.store is None:
            return None
        return self.engine.enqueue(kind, payload, key=key or new_idempotency_key(kind.value))

    # ------------------------------------------------------------------ control loop
    async def control_task(self):
        if self.engine is None:
            return
        if self._stop_requested and self.engine.fatal_reason is not None:
            self.close_type = CloseType.EARLY_STOP
            self.stop()
            return
        await self.engine.tick()
        self._track_stop()
        self._track_start()
        self._maybe_migrate()
        self._maybe_confirm_baseline()
        if self.engine.is_stopped and self._stop_command_sent:
            self.close_type = CloseType.EARLY_STOP
            self.stop()

    def _track_stop(self) -> None:
        """The Hummingbot stop is the operator's own CLI intent: it is re-sent with fresh revisions and a NEW key
        until a STOP row is APPLIED (a PAUSE/RESUME/state change in between makes the queued row CONFLICT; web
        commands keep the strict 409). Only an APPLIED row is durable (NG-OPS-003, AC-35/36)."""
        engine = self.engine
        if not self._stop_requested or self._stop_command_sent or engine is None or engine.store is None \
                or engine.store.closed or not self._stop_keys:
            return
        record = engine.store.get_command(idempotency_key=self._stop_keys[-1])
        if record is None:
            # The STOP insert itself failed (SQLITE_BUSY past the busy timeout, disk full, I/O): no row exists, so
            # the stop is re-sent instead of being lost (D2-01).
            self._send_stop()
            return
        if record.status == CommandStatus.QUEUED:
            return
        if record.status == CommandStatus.APPLIED:
            self._stop_command_sent = True
            return
        self.logger().warning(f"Neutral grid STOP was {record.status.value} ({record.result}); re-sending it")
        self._send_stop()

    def _send_stop(self) -> None:
        key = f"executor-stop-{self.config.id}-{os.getpid()}-{len(self._stop_keys) + 1}"
        self._stop_keys.append(key)
        try:
            self._enqueue(CommandKind.STOP, {"reason": "hummingbot stop (executor early_stop)", "keep_position": True},
                          key=key)
        except Exception as exc:  # noqa: BLE001 - retried by _track_stop until a STOP row is APPLIED
            self.logger().warning(f"Neutral grid STOP could not be enqueued ({type(exc).__name__}); retrying")

    def _track_start(self) -> None:
        """The launcher START (incl. a confirmed resume) is the operator's own intent: a CONFLICT (another command
        applied first, state changed) is re-sent with fresh revisions and a new key; a REJECTED is surfaced."""
        engine = self.engine
        if self._start_key is None or self._start_refused or engine.store is None or engine.store.closed:
            return
        record = engine.store.get_command(idempotency_key=self._start_key)
        if record is None or record.status == CommandStatus.CONFLICT:
            self._start_attempts += 1
            self._start_key = f"launcher-start-{self.config.id}-{self._start_attempts}"
            try:
                self._enqueue(CommandKind.START, launcher_start_payload(self.config), key=self._start_key)
            except Exception as exc:  # noqa: BLE001 - retried next tick
                self.logger().warning(f"Neutral grid START could not be enqueued ({type(exc).__name__}); retrying")
            return
        if record.status == CommandStatus.APPLIED:
            self._start_key = None
            return
        if record.status == CommandStatus.REJECTED:
            self._start_refused = True
            error = (record.result or {}).get("error")
            self.start_error = f"launcher START refused: {error}"
            self.logger().error(f"Neutral grid: {self.start_error} ({record.result})")

    def _maybe_migrate(self) -> None:
        """Launcher-confirmed audited grid migration (AC-52): sent once the engine has fresh market data; the
        engine re-verifies quiescence and rules and refuses otherwise."""
        engine = self.engine
        if not self.config.operator_confirmed_migration or self._migration_refused or engine.store is None \
                or engine.store.closed or "CONFIG_MISMATCH" not in engine.meta.freezes:
            return
        if self._migration_key is not None:
            record = engine.store.get_command(idempotency_key=self._migration_key)
            if record is None or record.status == CommandStatus.QUEUED:
                return
            error = (record.result or {}).get("error")
            if record.status == CommandStatus.REJECTED and error != "MARKET_DATA_NOT_READY":
                self._migration_refused = True
                self.start_error = f"grid migration refused: {error}"
                self.logger().error(f"Neutral grid: {self.start_error} ({record.result})")
                return
        if engine.rules is None or engine.mid is None:
            return
        self._migration_attempts += 1
        self._migration_key = f"launcher-migrate-{self.config.id}-{self._migration_attempts}"
        self._enqueue(CommandKind.BASELINE_AUDIT, {"action": "migrate_grid", "actor": "launcher-confirmed-operator",
                                                   "note": f"launcher-confirmed migration to {self.config.grid_id}"},
                      key=self._migration_key)

    def _maybe_confirm_baseline(self) -> None:
        """Launcher-confirmed B is sent only once the engine reports a stable snapshot + history cut."""
        engine = self.engine
        if (not self.config.operator_confirmed_baseline or engine.bootstrapped or engine.store is None
                or self.config.expected_initial_position is None):
            return
        if self._baseline_key is not None:
            record = engine.store.get_command(idempotency_key=self._baseline_key)
            if record is None or record.status == CommandStatus.QUEUED:
                return
            error = (record.result or {}).get("error")
            if record.status == CommandStatus.REJECTED and error not in _TRANSIENT_BASELINE_ERRORS:
                if not self._baseline_refused:
                    self._baseline_refused = True
                    self.start_error = f"baseline confirmation refused: {error}"
                    self.logger().error(f"Neutral grid: {self.start_error} ({record.result})")
                return
        ready, _ = engine.bootstrap_ready(engine.clock())
        if not ready:
            return
        # A transient refusal (cut moved between enqueue and apply, stale revision) may be retried with a new key
        # once the engine again reports a stable cut; the engine re-verifies everything at apply time.
        self._bootstrap_attempts += 1
        self._baseline_key = f"launcher-baseline-{self.config.id}-{self._bootstrap_attempts}"
        self._enqueue(CommandKind.CONFIRM_BASELINE,
                      {"expected_initial_position": str(self.config.expected_initial_position),
                       "actor": "launcher-confirmed-operator"},
                      key=self._baseline_key)

    def early_stop(self, keep_position: bool = False):
        """NG-OPS-003: draining stop via the durable queue; never flattens, keep_position is implied."""
        if self.engine is None or self.engine.store is None:
            self.close_type = CloseType.EARLY_STOP
            self.stop()
            return
        if not self._stop_requested:
            self._stop_requested = True
            self._send_stop()

    def _collect_held_position_orders(self) -> List[Dict]:
        # Inventory stays in the engine ledger; the orchestrator must not build a PositionHold from it.
        return []

    def _cancel_outstanding_orders(self):
        # A synchronous best-effort cancel would bypass intent-before-side-effect: never do it here.
        return None

    # ------------------------------------------------------------------ WS wake-ups (not proof)
    def _wake(self, event_type: str, status: str, event: Any) -> None:
        """Only this market's grid orders are hints: ExecutorBase listens on the whole connector (other pairs,
        other controllers, manual orders), and a foreign trade id would never clear from the lag bookkeeping."""
        if self.engine is None:
            return
        pair = getattr(event, "trading_pair", None)
        if pair is not None and pair != self.config.trading_pair:
            return
        order_id = str(getattr(event, "order_id", "") or "")
        if not order_id.isdigit() or int(order_id) not in self.engine.order_meta:
            return
        self.engine.wake({"type": event_type, "status": status, "client_order_id": getattr(event, "order_id", None),
                          "trade_id": getattr(event, "exchange_trade_id", None)})

    def process_order_filled_event(self, event_tag, market, event):
        self._wake("trade", "filled", event)

    def process_order_canceled_event(self, event_tag, market, event):
        self._wake("order", "canceled", event)

    def process_order_completed_event(self, event_tag, market, event):
        self._wake("order", "filled", event)

    def process_order_created_event(self, event_tag, market, event):
        # An acceptance ack needs no authoritative history scan (and must not spend request weight).
        return None

    def process_order_failed_event(self, event_tag, market, event):
        self._wake("order", "failed", event)

    # ------------------------------------------------------------------ reporting
    def get_net_pnl_quote(self) -> Decimal:
        return Decimal("0")

    def get_net_pnl_pct(self) -> Decimal:
        return Decimal("0")

    def get_cum_fees_quote(self) -> Decimal:
        return Decimal("0")

    @property
    def is_trading(self) -> bool:
        return self.engine is not None and self.engine.state == EngineState.NORMAL

    def get_custom_info(self) -> Dict:
        if self.engine is None:
            return {"engine_state": None, "start_error": self.start_error}
        snap = self.engine.last_snapshot
        return {
            "engine_state": self.engine.engine_state.value,
            "reasons": list(self.engine.reasons),
            "snapshot_version": None if snap is None else snap.get("snapshot_version"),
            "grid_id": self.config.grid_id,
            "held_position_orders": [],
            # The last *committed* snapshot (same JSON the web UI reads); contains no secrets.
            "snapshot": snap,
        }

    def status_text(self) -> str:
        if self.engine is None:
            return f"Neutral grid not running ({self.start_error or 'not started'})"
        snap = self.engine.latest_committed_snapshot()
        return format_status(snap, now=self.engine.clock(),
                             local_error=self.engine.persistence_error or self.engine.fatal_reason)

    @property
    def executor_info(self):
        try:
            return super().executor_info
        except Exception:  # noqa: BLE001 - union without our type (see register_executor_type)
            from hummingbot.strategy_v2.models.executors_info import ExecutorInfo
            return ExecutorInfo.model_construct(
                id=self.config.id, timestamp=self.config.timestamp, type=self.config.type, status=self.status,
                close_type=self.close_type, close_timestamp=self.close_timestamp, config=self.config,
                net_pnl_pct=Decimal("0"), net_pnl_quote=Decimal("0"), cum_fees_quote=Decimal("0"),
                filled_amount_quote=Decimal("0"), is_active=self.is_active, is_trading=self.is_trading,
                custom_info=self.get_custom_info(), controller_id=self.config.controller_id,
            )
