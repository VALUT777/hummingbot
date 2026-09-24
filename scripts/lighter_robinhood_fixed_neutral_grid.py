"""Thin Robinhood Lighter launcher for the generic fixed-cell neutral grid (NG-ARCH-001/003).

It pins the Robinhood profile (``lighter_perpetual_robinhood``, ``LIT-USDG``, ONEWAY, 5x; 1000 LIT default caps) on top
of the generic ``controllers/generic/neutral_grid.py`` controller, which runs exactly ONE
``NeutralGridExecutor`` hosting the engine. Credentials come only from Hummingbot's existing encrypted connector
config; the API private key is validated with the existing setup validator (80 hex chars, optional ``0x``,
never a 64-hex wallet key) and never logged.

Live start needs ``enabled: true`` in the controller config AND an explicit confirmation phrase that binds the
grid id and the signed initial position; ``enabled: false`` (the example default) refuses to start. Stopping
never flattens: the engine drains its own orders and reports STOPPED / STOPPED_WITH_INVENTORY / STOP_UNCERTAIN.

CID orders are engine-owned: at construction (before the connector's polling loops start) every durable CID that
may have reached transport is registered with the connector, so Hummingbot's generic stop/exit ``cancel_all`` and
lost-order paths never cancel them. The only stop path for those orders is the engine drain (``on_stop``).
"""
import asyncio
import os
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Dict, List, Optional

from pydantic import model_validator

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import PositionMode
from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import EngineOptions
from hummingbot.strategy_v2.executors.neutral_grid_executor.executor import (
    durable_stop_ms,
    engine_db_path,
    register_durable_cids,
)
from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction
from scripts.v2_with_controllers import V2WithControllers, V2WithControllersConfig

CONNECTOR_NAME = "lighter_perpetual_robinhood"
TRADING_PAIR = "LIT-USDG"
CONTROLLER_CONFIG_NAME = "lighter_robinhood_fixed_neutral_grid.yml"
ROBINHOOD_PROFILE = {
    "controller_name": "neutral_grid",
    "connector_name": CONNECTOR_NAME,
    "trading_pair": TRADING_PAIR,
    "leverage": Decimal("5"),
    "position_mode": PositionMode.ONEWAY,
}
DEFAULT_CAPS = {"max_abs_net_position": Decimal("1000"), "max_gross_position": Decimal("1000")}
EXECUTOR_TYPE = "neutral_grid_executor"
# The drain waits at least until the engine itself would declare STOP_UNCERTAIN (plus a margin).
STOP_DRAIN_TIMEOUT_S = EngineOptions().stop_uncertain_after_s + 30.0


def confirmation_phrase(grid_id: str, expected_initial_position: Optional[Decimal]) -> str:
    """The exact phrase an operator must type to authorize a live start with this signed baseline."""
    return f"START {grid_id} ON {CONNECTOR_NAME} {TRADING_PAIR} WITH B={expected_initial_position}"


def resume_phrase(grid_id: str, stop_ms: int) -> str:
    """The exact phrase that resumes trading after the durable stop recorded at ``stop_ms``."""
    return f"RESUME {grid_id} AFTER STOP {stop_ms}"


def migration_phrase(grid_id: str) -> str:
    """The exact phrase that authorizes an audited migration of the quiescent grid on this market to ``grid_id``."""
    return f"MIGRATE {CONNECTOR_NAME} {TRADING_PAIR} TO {grid_id}"


def validate_profile(controller) -> List[str]:
    """Robinhood policy on top of the generic controller validation (never silently fixed). The caps are the
    operator's own positive limits (the profile values are only defaults, NG-ARCH-003)."""
    errors = []
    for field, value in ROBINHOOD_PROFILE.items():
        if getattr(controller, field) != value:
            errors.append(f"{field} must be {getattr(value, 'name', value)}")
    for field in DEFAULT_CAPS:
        current = getattr(controller, field)
        if not isinstance(current, Decimal) or not current.is_finite() or current <= 0:
            errors.append(f"{field} must be a finite positive LIT limit")
    if controller.db_path is not None and "://" in str(controller.db_path):
        errors.append("db_path must be a local file path")
    return errors


def profile_warnings(controller) -> List[str]:
    return [f"{field} {getattr(controller, field)} differs from the profile default {value} LIT"
            for field, value in DEFAULT_CAPS.items() if getattr(controller, field) != value]


@dataclass
class LaunchDecision:
    refusal: Optional[str] = None
    resume_stop_ms: Optional[int] = None
    migrate: bool = False
    notice: Optional[str] = None


def evaluate_launch(ccfg, config, connector, durable_stop_ms: Optional[int]) -> LaunchDecision:
    """Every launcher gate in one place: profile, enabled, the explicit start phrase, the API key, and the
    explicit phrases that alone may resume a durable stop or migrate a quiescent grid."""
    decision = LaunchDecision()
    errors = validate_profile(ccfg)
    if errors:
        decision.refusal = "; ".join(errors)
    elif not ccfg.enabled:
        decision.refusal = "enabled=false: live start refused (example config)"
    elif config.live_start_confirmation != confirmation_phrase(ccfg.grid_id, ccfg.expected_initial_position):
        decision.refusal = ("explicit confirmation missing: set live_start_confirmation to exactly "
                            f"'{confirmation_phrase(ccfg.grid_id, ccfg.expected_initial_position)}'")
    else:
        decision.refusal = validate_api_credentials(connector) if connector is not None else \
            "Robinhood connector is not configured"
    if decision.refusal is not None:
        return decision
    if durable_stop_ms is not None:
        wanted = resume_phrase(ccfg.grid_id, durable_stop_ms)
        if config.resume_after_stop_confirmation == wanted:
            decision.resume_stop_ms = durable_stop_ms
        else:
            decision.notice = (f"a durable STOP is recorded: the engine stays stopped; to resume trading set "
                               f"resume_after_stop_confirmation to exactly '{wanted}'")
    decision.migrate = config.migrate_grid_confirmation == migration_phrase(ccfg.grid_id)
    return decision


def validate_api_credentials(connector: ConnectorBase) -> Optional[str]:
    """Reuse the setup wizard validator on the key Hummingbot decrypted into the connector (never logged)."""
    from bin.lighter_robinhood_setup import normalize_api_private_key
    key = getattr(connector, "_api_private_key", None)
    if key is None:
        return "connector has no API private key (configure it with the encrypted Robinhood setup)"
    raw = key.get_secret_value() if hasattr(key, "get_secret_value") else str(key)
    stripped = raw[2:] if raw.startswith(("0x", "0X")) else raw
    if len(stripped) == 64:
        return "configured key looks like a 64-hex wallet key; an 80-hex Lighter API private key is required"
    try:
        normalize_api_private_key(raw)
    except ValueError:
        return "API private key must be exactly 80 hexadecimal characters (optional 0x)"
    return None


class LighterRobinhoodFixedNeutralGridConfig(V2WithControllersConfig):
    script_file_name: str = os.path.basename(__file__)
    controllers_config: List[str] = [CONTROLLER_CONFIG_NAME]
    live_start_confirmation: Optional[str] = None
    resume_after_stop_confirmation: Optional[str] = None      # "RESUME <grid_id> AFTER STOP <stop_ms>"
    migrate_grid_confirmation: Optional[str] = None           # "MIGRATE lighter_perpetual_robinhood LIT-USDG TO <id>"

    @model_validator(mode="after")
    def validate_runner(self):
        if self.script_file_name != os.path.basename(__file__):
            raise ValueError("script_file_name must select the fixed neutral grid runner")
        if self.controllers_config != [CONTROLLER_CONFIG_NAME]:
            raise ValueError(f"controllers_config must contain exactly {CONTROLLER_CONFIG_NAME}")
        if self.max_global_drawdown_quote is not None or self.max_controller_drawdown_quote is not None:
            raise ValueError("drawdown stops are disabled: stopping never flattens the position")
        return self


class LighterRobinhoodFixedNeutralGrid(V2WithControllers):
    def __init__(self, connectors: Dict[str, ConnectorBase], config: LighterRobinhoodFixedNeutralGridConfig):
        super().__init__(connectors, config)
        self.config = config
        self.refusal: Optional[str] = None
        self.registered_cids: List[int] = []
        self.last_drain_outcome: Optional[str] = None
        connector = connectors.get(CONNECTOR_NAME)
        for controller in self.controllers.values():
            ccfg = controller.config
            db_path = None
            stop_ms = None
            if connector is not None:
                try:
                    db_path = engine_db_path(ccfg.db_path, ccfg.connector_name, ccfg.trading_pair, connector)
                    # Before the connector's polling loops start: engine-owned CIDs of any previous run (even a
                    # refused start protects them from Hummingbot's generic cancel paths).
                    self.registered_cids += register_durable_cids(connector, db_path)
                    stop_ms = durable_stop_ms(db_path)
                except Exception as exc:  # noqa: BLE001 - the engine itself fails closed on an unreadable ledger
                    self.logger().error(f"Neutral grid: could not read the durable ledger: {type(exc).__name__}: {exc}")
            decision = evaluate_launch(ccfg, config, connector, stop_ms)
            for warning in profile_warnings(ccfg):
                self.logger().warning(f"Neutral grid profile: {warning}")
            if decision.notice:
                self.logger().warning(f"Neutral grid: {decision.notice}")
            if decision.refusal is None:
                ccfg.mark_operator_confirmed(start=True, baseline=True, migration=decision.migrate,
                                             resume_stop_ms=decision.resume_stop_ms)
            else:
                self.refusal = decision.refusal
                ccfg.enabled = False                   # the controller then creates no executor at all
        if self.refusal:
            self.logger().error(f"Fixed neutral grid refused to start: {self.refusal}")

    def apply_initial_setting(self):
        if self.refusal is None:
            for controller in self.controllers.values():
                self.connectors[CONNECTOR_NAME].set_position_mode(PositionMode.ONEWAY)
                self.connectors[CONNECTOR_NAME].set_leverage(TRADING_PAIR, int(controller.config.leverage))

    def neutral_executors(self):
        """Live executor objects (not the cached controller reports, which go stale during on_stop)."""
        return [e for executors in self.executor_orchestrator.active_executors.values() for e in executors
                if e.config.type == EXECUTOR_TYPE]

    async def on_stop(self):
        """NG-OPS-003: durable STOP (cancel own orders, keep position), then leave the engine ledger authoritative.

        The connector no longer cancels engine-owned CID orders on stop/exit, so this engine drain is their only
        stop path. The executor is detached before the V2 orchestrator persists executors: its durable state lives
        in the engine's SQLite ledger (and its config type is not part of the V2 executor registry, see trace).
        """
        await self.drain_neutral_executors()
        await super().on_stop()

    async def drain_neutral_executors(self, timeout_s: float = STOP_DRAIN_TIMEOUT_S, poll_s: float = 1.0,
                                      clock: Callable[[], float] = time.time) -> bool:
        """Send the durable STOP through the orchestrator (-> executor.early_stop -> engine STOP command), wait until
        the engine proves the drain (executor closes after STOPPED / STOPPED_WITH_INVENTORY) and detach the
        executors. Returns True when every neutral executor closed within ``timeout_s``."""
        for controller in self.controllers.values():
            controller.stop()
        actions = [StopExecutorAction(executor_id=e.config.id, controller_id=e.config.controller_id,
                                      keep_position=True)
                   for e in self.neutral_executors() if e.is_active]
        if actions:
            self.executor_orchestrator.execute_actions(actions)
        deadline = clock() + timeout_s
        while clock() < deadline and any(e.is_active for e in self.neutral_executors()):
            await asyncio.sleep(poll_s)
        clean = True
        for controller_id, executors in list(self.executor_orchestrator.active_executors.items()):
            for executor in list(executors):
                if executor.config.type == EXECUTOR_TYPE:
                    self.last_drain_outcome = self._durable_stop_outcome(executor)
                    if not executor.is_closed:
                        clean = False
                        self.logger().warning(
                            f"Neutral grid did not prove a clean stop in time; durable engine state: "
                            f"{self.last_drain_outcome}. Engine-owned orders stay under engine control and are "
                            f"reconciled on the next start (a durable STOP is never resumed automatically).")
                        executor.stop()
                    executors.remove(executor)
        return clean

    @staticmethod
    def _durable_stop_outcome(executor) -> str:
        """The stop state the engine actually committed (never a claim about a STOP that was not applied)."""
        engine = getattr(executor, "engine", None)
        if engine is None:
            return "NO_ENGINE"
        meta = engine.meta
        if meta.stop_requested_ms is None:
            return "STOP_NOT_APPLIED"
        return meta.stop_outcome or "STOPPING"

    def format_status(self) -> str:
        lines = []
        if self.refusal:
            lines.append(f"Fixed neutral grid refused: {self.refusal}")
        for controller in self.controllers.values():
            lines.extend(controller.to_format_status())
        return "\n".join(lines) if lines else super().format_status()
