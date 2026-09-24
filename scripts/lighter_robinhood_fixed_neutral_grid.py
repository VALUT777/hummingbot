"""Thin Robinhood Lighter launcher for the generic fixed-cell neutral grid (NG-ARCH-001/003).

It pins the Robinhood profile (``lighter_perpetual_robinhood``, ``LIT-USDG``, ONEWAY, 5x, 1000 LIT caps) on top
of the generic ``controllers/generic/neutral_grid.py`` controller, which runs exactly ONE
``NeutralGridExecutor`` hosting the engine. Credentials come only from Hummingbot's existing encrypted connector
config; the API private key is validated with the existing setup validator (80 hex chars, optional ``0x``,
never a 64-hex wallet key) and never logged.

Live start needs ``enabled: true`` in the controller config AND an explicit confirmation phrase that binds the
grid id and the signed initial position; ``enabled: false`` (the example default) refuses to start. Stopping
never flattens: the engine drains its own orders and reports STOPPED / STOPPED_WITH_INVENTORY / STOP_UNCERTAIN.
"""
import asyncio
import os
import time
from decimal import Decimal
from typing import Dict, List, Optional

from pydantic import model_validator

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import PositionMode
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
STOP_DRAIN_TIMEOUT_S = 90.0


def confirmation_phrase(grid_id: str, expected_initial_position: Optional[Decimal]) -> str:
    """The exact phrase an operator must type to authorize a live start with this signed baseline."""
    return f"START {grid_id} ON {CONNECTOR_NAME} {TRADING_PAIR} WITH B={expected_initial_position}"


def validate_profile(controller) -> List[str]:
    """Robinhood policy on top of the generic controller validation (never silently fixed)."""
    errors = []
    for field, value in ROBINHOOD_PROFILE.items():
        if getattr(controller, field) != value:
            errors.append(f"{field} must be {getattr(value, 'name', value)}")
    for field, value in DEFAULT_CAPS.items():
        current = getattr(controller, field)
        if current > value:
            errors.append(f"{field} {current} exceeds the Robinhood profile cap {value} LIT")
    if controller.db_path is not None and "://" in str(controller.db_path):
        errors.append("db_path must be a local file path")
    return errors


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
        for controller in self.controllers.values():
            ccfg = controller.config
            errors = validate_profile(ccfg)
            if errors:
                self.refusal = "; ".join(errors)
            elif not ccfg.enabled:
                self.refusal = "enabled=false: live start refused (example config)"
            elif config.live_start_confirmation != confirmation_phrase(ccfg.grid_id, ccfg.expected_initial_position):
                self.refusal = ("explicit confirmation missing: set live_start_confirmation to exactly "
                                f"'{confirmation_phrase(ccfg.grid_id, ccfg.expected_initial_position)}'")
            else:
                error = validate_api_credentials(connectors[CONNECTOR_NAME]) if CONNECTOR_NAME in connectors else \
                    "Robinhood connector is not configured"
                if error:
                    self.refusal = error
            if self.refusal is None:
                ccfg.operator_confirmed_start = True
                ccfg.operator_confirmed_baseline = True
            else:
                ccfg.enabled = False                   # the controller then creates no executor at all
        if self.refusal:
            self.logger().error(f"Fixed neutral grid refused to start: {self.refusal}")

    def apply_initial_setting(self):
        if self.refusal is None:
            for controller in self.controllers.values():
                self.connectors[CONNECTOR_NAME].set_position_mode(PositionMode.ONEWAY)
                self.connectors[CONNECTOR_NAME].set_leverage(TRADING_PAIR, int(controller.config.leverage))

    def neutral_executors(self):
        return [e for e in self.get_all_executors() if e.type == EXECUTOR_TYPE]

    async def on_stop(self):
        """NG-OPS-003: durable STOP (cancel own orders, keep position), then leave the engine ledger authoritative.

        The executor is detached before the V2 orchestrator persists executors: its durable state lives in the
        engine's SQLite ledger (and its config type is not part of the V2 executor registry, see trace).
        """
        for controller in self.controllers.values():
            controller.stop()
        actions = [StopExecutorAction(executor_id=e.id, controller_id=e.controller_id, keep_position=True)
                   for e in self.neutral_executors() if e.is_active]
        if actions:
            self.executor_orchestrator.execute_actions(actions)
        deadline = time.time() + STOP_DRAIN_TIMEOUT_S
        while time.time() < deadline and any(e.is_active for e in self.neutral_executors()):
            await asyncio.sleep(1.0)
        for controller_id, executors in list(self.executor_orchestrator.active_executors.items()):
            for executor in list(executors):
                if executor.config.type == EXECUTOR_TYPE:
                    if not executor.is_closed:
                        self.logger().warning("Neutral grid did not prove a clean stop in time; its durable state "
                                              "(STOP_UNCERTAIN) will be reconciled on the next start.")
                        executor.stop()
                    executors.remove(executor)
        await super().on_stop()

    def format_status(self) -> str:
        lines = []
        if self.refusal:
            lines.append(f"Fixed neutral grid refused: {self.refusal}")
        for controller in self.controllers.values():
            lines.extend(controller.to_format_status())
        return "\n".join(lines) if lines else super().format_status()
