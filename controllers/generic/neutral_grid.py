"""Generic V2 controller for the persistent neutral fixed-cell grid (NG-ARCH-001/003).

The controller only validates configuration, builds the fixed grid preview inputs and manages the
lifecycle of exactly ONE ``NeutralGridExecutor`` (never 55 executors, never a GridExecutor rewrite).
All trading decisions live in the engine hosted by that executor.
"""
from decimal import Decimal
from pathlib import Path
from typing import List, Optional

from pydantic import Field, model_validator

from hummingbot.core.data_type.common import MarketDict, PositionMode
from hummingbot.strategy_v2.controllers import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import OrderTypePolicy
from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import NeutralGridExecutorConfig
from hummingbot.strategy_v2.executors.neutral_grid_executor.executor import EXECUTOR_TYPE, register_executor_type
from hummingbot.strategy_v2.executors.neutral_grid_executor.snapshot import format_status
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

_ALLOWED_ORDER_TYPES = (OrderTypePolicy.LIMIT_MAKER, OrderTypePolicy.LIMIT)


def default_db_path(grid_id: str) -> str:
    from hummingbot import data_path
    return str(Path(data_path()) / f"neutral_grid_{grid_id}.sqlite")


class NeutralGridConfig(ControllerConfigBase):
    """Fixed-cell neutral grid. Values are a profile, not universal constants (NG-ARCH-003)."""
    controller_type: str = "generic"
    controller_name: str = "neutral_grid"
    connector_name: str = "lighter_perpetual_robinhood"
    trading_pair: str = "LIT-USDG"
    grid_id: str = "lit-neutral-fixed-v1"
    enabled: bool = False
    lower_price: Optional[Decimal] = None
    upper_price: Optional[Decimal] = None
    cell_count: int = 55
    order_amount_base: Decimal = Decimal("10")
    leverage: Decimal = Decimal("5")
    position_mode: PositionMode = PositionMode.ONEWAY
    expected_initial_position: Optional[Decimal] = None
    max_abs_net_position: Decimal = Decimal("1000")
    max_gross_position: Decimal = Decimal("1000")
    max_active_orders: int = 120
    history_freshness_s: Decimal = Decimal("10")
    settlement_delay_s: Decimal = Decimal("5")
    settlement_scans: int = 2
    history_overlap_s: Decimal = Decimal("60")
    poll_interval_s: Decimal = Decimal("5")
    entry_order_type: OrderTypePolicy = OrderTypePolicy.LIMIT_MAKER
    tp_order_type: OrderTypePolicy = OrderTypePolicy.LIMIT
    tp_gtt_seconds: int = 28 * 24 * 3600
    db_path: Optional[str] = None
    # Set only by the launcher after explicit interactive confirmation; never read from YAML as true.
    operator_confirmed_start: bool = Field(default=False, json_schema_extra={"is_updatable": False})
    operator_confirmed_baseline: bool = Field(default=False, json_schema_extra={"is_updatable": False})

    @model_validator(mode="after")
    def validate_neutral_grid(self):
        for name in ("order_amount_base", "leverage", "max_abs_net_position", "max_gross_position",
                     "history_freshness_s", "poll_interval_s", "history_overlap_s"):
            value = getattr(self, name)
            if not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be a finite positive number")
        if not self.settlement_delay_s.is_finite() or self.settlement_delay_s < 0:
            raise ValueError("settlement_delay_s must be finite and non-negative")
        if self.settlement_scans < 1 or self.cell_count < 1 or self.max_active_orders < 1 or self.tp_gtt_seconds < 1:
            raise ValueError("settlement_scans, cell_count, max_active_orders and tp_gtt_seconds must be positive")
        if self.entry_order_type not in _ALLOWED_ORDER_TYPES or self.tp_order_type not in _ALLOWED_ORDER_TYPES:
            raise ValueError("entry/TP order types must be LIMIT_MAKER or LIMIT; MARKET is forbidden")
        if self.position_mode != PositionMode.ONEWAY:
            raise ValueError("neutral grid requires ONEWAY position mode (virtual cells, one venue net position)")
        if self.lower_price is not None and self.upper_price is not None:
            if not (self.lower_price.is_finite() and self.upper_price.is_finite()) or \
                    not (0 < self.lower_price < self.upper_price):
                raise ValueError("bounds must be finite with 0 < lower_price < upper_price")
        if self.expected_initial_position is not None and not self.expected_initial_position.is_finite():
            raise ValueError("expected_initial_position must be a finite signed decimal")
        if self.enabled:
            if self.lower_price is None or self.upper_price is None:
                raise ValueError("enabled grid requires explicit lower_price and upper_price")
            if self.expected_initial_position is None:
                raise ValueError("enabled grid requires a signed expected_initial_position (first bootstrap)")
        return self

    def update_markets(self, markets: MarketDict) -> MarketDict:
        return markets.add_or_update(self.connector_name, self.trading_pair)

    def resolved_db_path(self) -> str:
        return self.db_path or default_db_path(self.grid_id)

    def executor_config(self, timestamp: Optional[float] = None) -> NeutralGridExecutorConfig:
        return NeutralGridExecutorConfig(
            timestamp=timestamp, controller_id=self.id, connector_name=self.connector_name,
            trading_pair=self.trading_pair, grid_id=self.grid_id, lower_price=self.lower_price,
            upper_price=self.upper_price, cell_count=self.cell_count, order_amount_base=self.order_amount_base,
            leverage=self.leverage, expected_initial_position=self.expected_initial_position,
            max_abs_net_position=self.max_abs_net_position, max_gross_position=self.max_gross_position,
            max_active_orders=self.max_active_orders, history_freshness_s=self.history_freshness_s,
            settlement_delay_s=self.settlement_delay_s, settlement_scans=self.settlement_scans,
            history_overlap_s=self.history_overlap_s, poll_interval_s=self.poll_interval_s,
            entry_order_type=self.entry_order_type, tp_order_type=self.tp_order_type,
            tp_gtt_seconds=self.tp_gtt_seconds, enabled=self.enabled, db_path=self.resolved_db_path(),
            operator_confirmed_start=self.operator_confirmed_start,
            operator_confirmed_baseline=self.operator_confirmed_baseline,
        )


class NeutralGrid(ControllerBase):
    """Lifecycle of the single neutral grid executor."""

    def __init__(self, config: NeutralGridConfig, *args, **kwargs):
        register_executor_type()
        super().__init__(config, *args, **kwargs)
        self.config: NeutralGridConfig = config
        self._executor_requested = False

    def neutral_executors(self) -> List[ExecutorInfo]:
        return [e for e in self.executors_info if e.type == EXECUTOR_TYPE]

    def determine_executor_actions(self) -> List[ExecutorAction]:
        """Exactly one executor per controller run; a finished one is not silently recreated."""
        if not self.config.enabled or self._executor_requested or self.neutral_executors():
            return []
        self._executor_requested = True
        return [CreateExecutorAction(controller_id=self.config.id,
                                     executor_config=self.config.executor_config(self.market_data_provider.time()))]

    async def update_processed_data(self):
        return None

    def latest_snapshot(self) -> Optional[dict]:
        for info in self.neutral_executors():
            snap = (info.custom_info or {}).get("snapshot")
            if snap is not None:
                return snap
        return None

    def to_format_status(self) -> List[str]:
        if not self.config.enabled:
            return ["Neutral grid disabled (enabled=false): live start refused."]
        snap = self.latest_snapshot()
        return format_status(snap, now=self.market_data_provider.time()).split("\n")
