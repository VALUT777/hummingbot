from decimal import Decimal, InvalidOperation, ROUND_CEILING
from typing import Any, Dict, List, Optional, Set

from pydantic import model_validator

from controllers.generic.multi_grid_strike import MultiGridStrike, MultiGridStrikeConfig
from hummingbot.core.data_type.common import OrderType, PositionMode, PriceType, TradeType
from hummingbot.strategy_v2.executors.position_executor.data_types import TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction


ROBINHOOD_CONNECTOR = "lighter_perpetual_robinhood"
ROBINHOOD_PAIR = "LIT-USDG"


class LighterRobinhoodMultiGridStrikeConfig(MultiGridStrikeConfig):
    controller_name: str = "lighter_robinhood_multi_grid_strike"
    connector_name: str = ROBINHOOD_CONNECTOR
    trading_pair: str = ROBINHOOD_PAIR
    leverage: int = 5
    position_mode: PositionMode = PositionMode.ONEWAY
    total_amount_quote: Decimal = Decimal("3000")
    min_spread_between_orders: Optional[Decimal] = Decimal("0.018")
    min_order_amount_quote: Optional[Decimal] = Decimal("250")
    max_open_orders: int = 2
    max_orders_per_batch: Optional[int] = 1
    order_frequency: int = 3
    activation_bounds: Optional[Decimal] = None
    keep_position: bool = True
    triple_barrier_config: TripleBarrierConfig = TripleBarrierConfig(
        take_profit=Decimal("0.018"),
        open_order_type=OrderType.LIMIT_MAKER,
        take_profit_order_type=OrderType.LIMIT_MAKER,
    )
    max_abs_net_position: Decimal = Decimal("1000")
    max_data_age_seconds: Decimal = Decimal("10")
    snapshot_poll_seconds: Decimal = Decimal("10")

    @model_validator(mode="after")
    def validate_robinhood_profile(self):
        if self.connector_name != ROBINHOOD_CONNECTOR or self.trading_pair != ROBINHOOD_PAIR:
            raise ValueError("Robinhood Multi Grid Strike only supports lighter_perpetual_robinhood LIT-USDG")
        if self.leverage != 5 or self.position_mode is not PositionMode.ONEWAY:
            raise ValueError("Robinhood Multi Grid Strike requires ONEWAY mode and leverage 5")
        if not self.keep_position:
            raise ValueError("Robinhood Multi Grid Strike must preserve positions")
        barrier = self.triple_barrier_config
        preserving_barrier = (
            barrier.take_profit is not None
            and barrier.take_profit.is_finite()
            and barrier.take_profit > 0
            and barrier.open_order_type is OrderType.LIMIT_MAKER
            and barrier.take_profit_order_type is OrderType.LIMIT_MAKER
            and barrier.stop_loss is None
            and barrier.time_limit is None
            and barrier.trailing_stop is None
        )
        tuning_valid = (
            self.min_spread_between_orders is not None
            and self.min_spread_between_orders.is_finite()
            and self.min_spread_between_orders > 0
            and self.min_order_amount_quote is not None
            and self.min_order_amount_quote.is_finite()
            and self.min_order_amount_quote > 0
            and not isinstance(self.max_open_orders, bool)
            and self.max_open_orders >= 1
            and not isinstance(self.max_orders_per_batch, bool)
            and self.max_orders_per_batch is not None
            and self.max_orders_per_batch >= 1
            and not isinstance(self.order_frequency, bool)
            and self.order_frequency >= 0
            and (
                self.activation_bounds is None
                or (self.activation_bounds.is_finite() and self.activation_bounds > 0)
            )
        )
        if not tuning_valid or not preserving_barrier:
            raise ValueError("grid tuning must retain maker entry/TP and position-preserving barriers")
        decimals = (
            self.total_amount_quote, self.max_abs_net_position,
            self.max_data_age_seconds, self.snapshot_poll_seconds,
        )
        if any(not value.is_finite() or value <= 0 for value in decimals):
            raise ValueError("budget, cap, and freshness intervals must be finite and positive")
        if self.max_abs_net_position != Decimal("1000"):
            raise ValueError("maximum absolute LIT position must be exactly 1000")
        ids: Set[str] = set()
        allocation = Decimal("0")
        worst_base = Decimal("0")
        for grid in self.grids:
            if not grid.grid_id or grid.grid_id in ids:
                raise ValueError("grid ids must be present and unique")
            ids.add(grid.grid_id)
            values = (grid.start_price, grid.end_price, grid.limit_price, grid.amount_quote_pct)
            if any(not value.is_finite() for value in values):
                raise ValueError("grid prices and allocations must be finite")
            if grid.side is not TradeType.BUY:
                raise ValueError("Robinhood grids must be LONG/BUY")
            if not (Decimal("0") < grid.limit_price < grid.start_price < grid.end_price):
                raise ValueError("LONG grids require 0 < limit_price < start_price < end_price")
            if grid.amount_quote_pct <= 0 or grid.amount_quote_pct > 1:
                raise ValueError("grid allocation must be in (0, 1]")
            if grid.enabled:
                if self.total_amount_quote * grid.amount_quote_pct < self.min_order_amount_quote * Decimal("1.05"):
                    raise ValueError("each enabled grid allocation must fund one conservative native grid level")
                allocation += grid.amount_quote_pct
                worst_base += self.total_amount_quote * grid.amount_quote_pct / grid.limit_price
        if allocation > 1:
            raise ValueError("enabled grid allocations cannot exceed 1")
        if worst_base > self.max_abs_net_position:
            raise ValueError("configured grid budget can exceed the 1000 LIT position cap")
        return self


class LighterRobinhoodMultiGridStrike(MultiGridStrike):
    PRIVATE_STREAM_MAX_AGE_SECONDS = Decimal("65")

    def __init__(self, config: LighterRobinhoodMultiGridStrikeConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config
        self._snapshot_ready = False
        self._snapshot_position = Decimal("NaN")
        self._snapshot_active_ids: Set[str] = set()
        self._snapshot_pending = True
        self._last_snapshot_poll = float("-inf")
        self._generation_fingerprint: Optional[str] = None
        self._generation_grid_ids: Set[str] = set()
        self._issued_grid_ids: Set[str] = set()
        self._flat_reset_revision: Optional[Any] = None
        self._flat_reset_observations = 0
        self._seen_issued_grid_ids: Set[str] = set()
        self._safety_gate_reason = "waiting for first authoritative snapshot"

    def _config_fingerprint(self) -> str:
        return repr((
            self.config.total_amount_quote,
            self.config.min_spread_between_orders,
            self.config.min_order_amount_quote,
            self.config.max_open_orders,
            self.config.max_orders_per_batch,
            self.config.order_frequency,
            self.config.activation_bounds,
            self.config.triple_barrier_config,
            tuple(
                (grid.grid_id, grid.start_price, grid.end_price, grid.limit_price,
                 grid.side, grid.amount_quote_pct, grid.enabled)
                for grid in self.config.grids
            ),
        ))

    async def update_processed_data(self):
        await super().update_processed_data()
        reported_ids = {
            executor.config.level_id
            for executor in self.executors_info
            if getattr(executor.config, "level_id", None)
        }
        self._seen_issued_grid_ids.update(self._issued_grid_ids & reported_ids)
        poll_started = self.market_data_provider.time()
        if poll_started - self._last_snapshot_poll < float(self.config.snapshot_poll_seconds):
            return
        self._last_snapshot_poll = poll_started
        connector = self.market_data_provider.connectors[self.config.connector_name]
        try:
            tracked_ids = {str(order_id) for order_id in connector.in_flight_orders}
            account = await connector.get_grid_account_snapshot(
                self.config.trading_pair, client_order_ids=list(tracked_ids), force_refresh=True
            )
            now = self.market_data_provider.time()
            self._apply_account_snapshot(account, Decimal(str(now)), tracked_ids)
        except Exception:
            self._snapshot_ready = False
            self._safety_gate_reason = "authoritative account refresh failed"

    def _apply_account_snapshot(self, account: Dict[str, Any], now: Decimal, tracked_ids: Set[str]) -> None:
        try:
            request_started = Decimal(str(account["request_started_at"]))
            fetched_at = Decimal(str(account["fetched_at"]))
            public_at = Decimal(str(account["public_data_last_recv_time"]))
            private_at = Decimal(str(account["private_stream_last_recv_time"]))
            position = Decimal(str(account["net_position"]))
            available_margin = Decimal(str(account["available_margin"]))
            leverage = Decimal(str(account["leverage"]))
            account_index = account["account_index"]
            if isinstance(account_index, bool) or not isinstance(account_index, int):
                raise ValueError("invalid account index")
            active_ids = self._active_order_ids(account["active_orders"])
        except (InvalidOperation, KeyError, TypeError, ValueError):
            self._snapshot_ready = False
            self._safety_gate_reason = "authoritative account snapshot is malformed or incomplete"
            return
        finite = all(
            value.is_finite()
            for value in (now, request_started, fetched_at, public_at, private_at, position, available_margin)
        )
        fresh = (
            finite
            and Decimal("0") <= fetched_at - request_started <= self.config.max_data_age_seconds
            and Decimal("0") <= now - request_started <= self.config.max_data_age_seconds
            and Decimal("0") <= now - fetched_at <= self.config.max_data_age_seconds
            and Decimal("0") <= now - public_at <= self.config.max_data_age_seconds
            and Decimal("0") <= now - private_at <= self.PRIVATE_STREAM_MAX_AGE_SECONDS
        )
        authoritative = (
            account.get("net_position_known") is True
            and account.get("available_margin_known") is True
            and available_margin >= 0
            and account.get("pending_submissions_unknown") is False
            and account.get("private_stream_connected") is True
            and account.get("market_state_known") is True
            and account.get("market_tradable") is True
            and account.get("force_reduce_only") is False
            and account.get("position_mode") in {"ONEWAY", PositionMode.ONEWAY}
            and account.get("leverage_confirmed") is True
            and leverage == Decimal("5")
            and account.get("collateral_token") == "USDG"
            and abs(position) <= self.config.max_abs_net_position
        )
        active_executors = self.active_executors()
        same_generation_owned = (
            self._generation_fingerprint == self._config_fingerprint()
            and bool(active_executors)
            and active_ids <= tracked_ids
        )
        initial_flat = position == 0 and not active_ids
        self._snapshot_ready = bool(fresh and authoritative and (initial_flat or same_generation_owned))
        self._snapshot_position = position
        self._snapshot_active_ids = active_ids
        self._snapshot_pending = account.get("pending_submissions_unknown") is not False
        if self._snapshot_ready:
            self._safety_gate_reason = "authoritative account state is ready"
            self._observe_generation_reset(account.get("revision", fetched_at), active_executors)
        else:
            self._flat_reset_revision = None
            self._flat_reset_observations = 0
            if not fresh:
                self._safety_gate_reason = "account, public, or private data is stale or disconnected"
            elif account.get("pending_submissions_unknown") is not False:
                self._safety_gate_reason = "pending submission outcome is unknown"
            elif not authoritative:
                self._safety_gate_reason = "market, margin, position mode, leverage, or account state is unconfirmed"
            elif active_ids:
                self._safety_gate_reason = f"{len(active_ids)} active order(s) are not owned by this generation"
            elif position != 0:
                self._safety_gate_reason = "non-flat held position requires operator reconciliation"
            else:
                self._safety_gate_reason = "account state is not eligible for this generation"

    @staticmethod
    def _active_order_ids(active_orders: Any) -> Set[str]:
        if not isinstance(active_orders, list):
            raise ValueError("active orders must be a list")
        result = set()
        for order in active_orders:
            if not isinstance(order, dict):
                raise ValueError("invalid active order")
            value = order.get("client_order_id_str", order.get("client_order_id"))
            if value is None or isinstance(value, bool) or not str(value):
                raise ValueError("active order lacks a client id")
            result.add(str(value))
        return result

    def _observe_generation_reset(self, revision: Any, active_executors: List[Any]) -> None:
        if self._generation_fingerprint is None:
            return
        if not self._issued_grid_ids <= self._seen_issued_grid_ids:
            self._flat_reset_revision = None
            self._flat_reset_observations = 0
            self._safety_gate_reason = "issued create action is awaiting executor evidence"
            return
        flat = (
            not active_executors and self._snapshot_position == 0
            and not self._snapshot_active_ids and not self._snapshot_pending
        )
        if not flat:
            self._flat_reset_revision = None
            self._flat_reset_observations = 0
            return
        if revision == self._flat_reset_revision:
            return
        self._flat_reset_revision = revision
        self._flat_reset_observations += 1
        if self._flat_reset_observations >= 2:
            for grid_id in self._generation_grid_ids:
                self._grid_executor_mapping.pop(grid_id, None)
            self._generation_fingerprint = None
            self._generation_grid_ids.clear()
            self._issued_grid_ids.clear()
            self._seen_issued_grid_ids.clear()
            self._flat_reset_revision = None
            self._flat_reset_observations = 0

    def determine_executor_actions(self) -> List[ExecutorAction]:
        native_actions = super().determine_executor_actions()
        actions: List[ExecutorAction] = []
        current_fingerprint = self._config_fingerprint()
        for action in native_actions:
            if isinstance(action, StopExecutorAction):
                actions.append(action.model_copy(update={"keep_position": True}))
                continue
            if not isinstance(action, CreateExecutorAction) or not self._snapshot_ready:
                continue
            grid_id = action.executor_config.level_id
            if self._generation_fingerprint is None:
                if self._snapshot_position != 0 or self._snapshot_active_ids or self._snapshot_pending:
                    continue
                self._generation_fingerprint = current_fingerprint
                self._generation_grid_ids = {grid.grid_id for grid in self.config.grids if grid.enabled}
            if current_fingerprint != self._generation_fingerprint:
                self._safety_gate_reason = "configuration changes are deferred until the generation resets flat"
                continue
            if grid_id not in self._generation_grid_ids or grid_id in self._issued_grid_ids:
                continue
            if not self._native_minimum_is_funded(action):
                self._safety_gate_reason = "grid allocation is below the current market minimum level"
                continue
            remaining_base = sum(
                self.config.total_amount_quote * grid.amount_quote_pct / grid.limit_price
                for grid in self.config.grids
                if grid.grid_id in self._generation_grid_ids and grid.grid_id not in self._issued_grid_ids
            )
            if max(Decimal("0"), self._snapshot_position) + remaining_base > self.config.max_abs_net_position:
                self._safety_gate_reason = "remaining generation budget could exceed the LIT position cap"
                continue
            self._issued_grid_ids.add(grid_id)
            actions.append(action)
        return actions

    def _native_minimum_is_funded(self, action: CreateExecutorAction) -> bool:
        connector = self.market_data_provider.connectors[self.config.connector_name]
        rule = connector.trading_rules.get(self.config.trading_pair)
        if rule is None:
            return False
        try:
            price = Decimal(str(self.market_data_provider.get_price_by_type(
                self.config.connector_name, self.config.trading_pair, PriceType.MidPrice
            )))
            increment = Decimal(str(rule.min_base_amount_increment))
            minimum_base = Decimal(str(rule.min_order_size))
            minimum = max(action.executor_config.min_order_amount_quote, Decimal(str(rule.min_notional_size)))
            min_base = max(
                minimum * Decimal("1.05") / price,
                increment * (minimum / (increment * price)).to_integral_value(rounding=ROUND_CEILING),
            )
            quantized_base = (min_base / increment).to_integral_value(rounding=ROUND_CEILING) * increment
            venue_minimum_base = (
                minimum_base / increment
            ).to_integral_value(rounding=ROUND_CEILING) * increment
            return (
                quantized_base >= venue_minimum_base
                and action.executor_config.total_amount_quote >= quantized_base * price
            )
        except (AttributeError, InvalidOperation, TypeError, ValueError, ZeroDivisionError):
            return False

    def to_format_status(self) -> List[str]:
        status = super().to_format_status()
        state = (
            "READY"
            if self._snapshot_ready and self._safety_gate_reason == "authoritative account state is ready"
            else "BLOCKED"
        )
        status.append(f"Robinhood safety gate: {state} — {self._safety_gate_reason}")
        return status
