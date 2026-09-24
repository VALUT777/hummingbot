"""``NeutralGridExecutor``: the single V2 executor hosting the neutral grid engine (NG-ARCH-001).

One executor, one control-loop task, one engine for all cells (AC-04). Connector order events are only
WebSocket-style wake-up signals for the history poller; they never prove a fill or a terminal state.
The executor never cancels or places orders itself: every side effect goes through the engine's durable
outbox (intent before transport). A forced shutdown therefore does NOT run a best-effort cancel.
"""
from __future__ import annotations

import logging
import os
import time
from decimal import Decimal
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
                 offline_demo: bool = False):
        super().__init__(strategy=strategy, connectors=[config.connector_name], config=config,
                         update_interval=update_interval, max_retries=max_retries)
        self.config: NeutralGridExecutorConfig = config
        self._port = port
        self._store = store
        self._clock = clock
        self._options = options
        self._offline_demo = offline_demo
        self.engine = None
        self.start_error: Optional[str] = None
        self._stop_command_sent = False
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
                                      offline_demo=self._offline_demo)
        if self.engine.fatal_reason is not None:
            self.start_error = self.engine.fatal_reason
            self.logger().error(f"Neutral grid engine is fail-closed: {self.start_error}")
            return
        self._subscribe_wakeups(port)
        if self.config.operator_confirmed_start:
            self._enqueue(CommandKind.START, {"source": "launcher"}, key=f"launcher-start-{self.config.id}")

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
        if self._stop_command_sent and self.engine.fatal_reason is not None:
            self.close_type = CloseType.EARLY_STOP
            self.stop()
            return
        await self.engine.tick()
        self._maybe_confirm_baseline()
        if self.engine.is_stopped and self._stop_command_sent:
            self.close_type = CloseType.EARLY_STOP
            self.stop()

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
        if not self._stop_command_sent:
            self._stop_command_sent = True
            self._enqueue(CommandKind.STOP, {"reason": "executor early_stop", "keep_position": True},
                          key=f"executor-stop-{self.config.id}-{os.getpid()}")

    def _collect_held_position_orders(self) -> List[Dict]:
        # Inventory stays in the engine ledger; the orchestrator must not build a PositionHold from it.
        return []

    def _cancel_outstanding_orders(self):
        # A synchronous best-effort cancel would bypass intent-before-side-effect: never do it here.
        return None

    # ------------------------------------------------------------------ WS wake-ups (not proof)
    def _wake(self, event_type: str, status: str, event: Any) -> None:
        if self.engine is None:
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
