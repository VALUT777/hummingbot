import asyncio
import math
import os
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import StrictInt, model_validator

from hummingbot import data_path
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import MarketDict, OrderType, PositionAction, PositionMode
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
)
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from scripts.lighter_robinhood_grid_risk import (
    ExposureEpoch,
    IntentJournal,
    JournalError,
    ReservationError,
)


CONNECTOR_NAME = "lighter_perpetual_robinhood"
TRADING_PAIR = "LIT-USDG"


class GridState(Enum):
    DISABLED = "DISABLED"
    RECONCILING = "RECONCILING"
    QUOTING = "QUOTING"
    DRAINING = "DRAINING"
    PAUSED = "PAUSED"


class LighterRobinhoodNeutralGridConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    controllers_config: List[str] = []
    enabled: bool = False
    connector_name: str = CONNECTOR_NAME
    trading_pair: str = TRADING_PAIR
    lower_price: Optional[Decimal] = None
    upper_price: Optional[Decimal] = None
    grid_levels: StrictInt = 21
    order_amount_base: Decimal = Decimal("10")
    max_abs_net_position: Decimal = Decimal("1000")
    leverage: int = 5
    max_open_orders: int = 2
    refresh_seconds: float = 30.0
    max_data_age_seconds: float = 10.0
    margin_reserve_usdg: Optional[Decimal] = None

    @model_validator(mode="after")
    def validate_safety_contract(self):
        if self.controllers_config:
            raise ValueError("this dedicated script does not permit controllers_config")
        if (not self.order_amount_base.is_finite() or self.order_amount_base <= 0
                or not self.max_abs_net_position.is_finite() or self.max_abs_net_position <= 0):
            raise ValueError("order amount and position cap must be finite and positive")
        if self.max_abs_net_position > Decimal("1000"):
            raise ValueError("max_abs_net_position cannot exceed 1000 LIT")
        if self.grid_levels < 2:
            raise ValueError("grid_levels must be an exact integer of at least 2")
        if self.order_amount_base > self.max_abs_net_position:
            raise ValueError("order_amount_base cannot exceed max_abs_net_position")
        if self.max_open_orders != 2 or self.leverage != 5:
            raise ValueError("this script requires two orders and 5x leverage")
        if (not math.isfinite(self.refresh_seconds) or not math.isfinite(self.max_data_age_seconds)
                or self.refresh_seconds <= 0 or self.max_data_age_seconds <= 0):
            raise ValueError("refresh and freshness intervals must be positive")
        if self.enabled:
            if self.connector_name != CONNECTOR_NAME or self.trading_pair != TRADING_PAIR:
                raise ValueError("enabled mode is restricted to Robinhood Lighter LIT-USDG")
            if self.lower_price is None or self.upper_price is None:
                raise ValueError("enabled mode requires explicit lower_price and upper_price")
            if (not self.lower_price.is_finite() or not self.upper_price.is_finite()
                    or self.lower_price <= 0 or self.lower_price >= self.upper_price):
                raise ValueError("bounds must be finite, positive, and lower than upper")
            if (self.margin_reserve_usdg is None or not self.margin_reserve_usdg.is_finite()
                    or self.margin_reserve_usdg < 0):
                raise ValueError("enabled mode requires an explicit finite nonnegative margin reserve")
        return self

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets[self.connector_name] = markets.get(self.connector_name, set()) | {self.trading_pair}
        return markets


def eligible_grid_prices(
    lower: Decimal,
    upper: Decimal,
    count: int,
    price_increment: Decimal,
    best_bid: Decimal,
    best_ask: Decimal,
) -> Tuple[Optional[Decimal], Optional[Decimal], List[Decimal]]:
    if count < 2 or price_increment <= 0:
        raise ValueError("grid needs at least two levels and a positive price increment")
    first_tick = (lower / price_increment).to_integral_value(rounding=ROUND_CEILING)
    last_tick = (upper / price_increment).to_integral_value(rounding=ROUND_FLOOR)
    distinct_ticks = max(0, int(last_tick - first_tick + 1))
    if count > distinct_ticks:
        raise ValueError("grid_levels exceeds distinct exchange price ticks within bounds")
    step = (upper - lower) / Decimal(count - 1)
    levels = sorted(level for level in {
        ((lower + step * index) / price_increment).to_integral_value(rounding=ROUND_DOWN) * price_increment
        for index in range(count)
    } if lower <= level <= upper)
    if len(levels) != count:
        raise ValueError("grid levels collapse after runtime price quantization")
    midpoint = (best_bid + best_ask) / Decimal("2")
    bids = [level for level in levels if level > 0 and level < best_ask and level < midpoint]
    asks = [level for level in levels if level > best_bid and level > midpoint]
    bid = max(bids) if bids else None
    ask = min(asks) if asks else None
    if bid is not None and ask is not None and bid >= ask:
        return None, None, levels
    return bid, ask, levels


class LighterRobinhoodNeutralGrid(StrategyV2Base):
    """Fail-closed neutral grid requiring exclusive use of the enabled account."""

    SNAPSHOT_POLL_SECONDS = 10.0
    PRIVATE_STREAM_MAX_AGE_SECONDS = 65.0
    INTENT_JOURNAL_FILENAME = "lighter_robinhood_neutral_grid_intents.json"

    def __init__(self, connectors: Dict[str, ConnectorBase], config: LighterRobinhoodNeutralGridConfig):
        super().__init__(connectors, config)
        self.config = config
        self.state = GridState.DISABLED if not config.enabled else GridState.RECONCILING
        self.epoch: Optional[ExposureEpoch] = None
        self._snapshot_task: Optional[asyncio.Task] = None
        self._snapshot_poll_seconds = self.SNAPSHOT_POLL_SECONDS
        self._next_snapshot_at = 0.0
        self._snapshot_started_at: Optional[float] = None
        self._owned_order_ids: Set[str] = set()
        self._cancel_requested: Set[str] = set()
        self._last_cancel_retry_revision: Optional[Any] = None
        self._started_enabled = False
        self._next_refresh_at = float("inf")
        self._now = 0.0
        self._submission_sequence = 0
        self._pause_reason = ""
        self._journal: Optional[IntentJournal] = None
        self._journal_error: Optional[str] = None
        try:
            self._journal = self._create_intent_journal()
        except JournalError as exc:
            self._journal_error = str(exc)
            if config.enabled:
                self.state = GridState.PAUSED
                self._pause_reason = self._journal_error
        entries = {} if self._journal is None else self._journal.entries
        self._unidentified_recovery = any(entry["client_order_id"] is None for entry in entries.values())
        bound_entries = [entry for entry in entries.values() if entry["client_order_id"] is not None]
        self._recovery_entries_by_id = {
            entry["client_order_id"]: entry
            for entry in bound_entries
        }
        self._recovery_order_ids = set(self._recovery_entries_by_id)
        self._tracked_recovery_order_ids: Set[str] = set()
        self._recovery_epoch: Optional[ExposureEpoch] = None
        self._account_index: Optional[int] = None
        if len(self._recovery_entries_by_id) != len(bound_entries):
            self._journal_error = "durable intent journal contains duplicate client order ids"
            self.state = GridState.PAUSED
            self._pause_reason = self._journal_error
        elif self._recovery_entries_by_id:
            baselines = {entry["baseline"] for entry in self._recovery_entries_by_id.values()}
            identities = {
                (entry.get("connector_domain"), entry.get("trading_pair"), entry.get("account_index"))
                for entry in self._recovery_entries_by_id.values()
            }
            if len(baselines) != 1 or len(identities) != 1:
                self._journal_error = "durable intent journal has inconsistent epoch authority"
                self.state = GridState.PAUSED
                self._pause_reason = self._journal_error

    def _create_intent_journal(self) -> IntentJournal:
        return IntentJournal(Path(data_path()) / self.INTENT_JOURNAL_FILENAME)

    def start(self, clock, timestamp: float):
        self._now = timestamp
        if not self.config.enabled:
            self.state = GridState.DISABLED
            return
        if self._journal_error is not None:
            self.state = GridState.PAUSED
            return
        connector = self.connectors[self.config.connector_name]
        try:
            tracked = connector.in_flight_orders
        except (AttributeError, NotImplementedError):
            self._pause("connector cannot expose restored in-flight orders", drain=False)
            return
        tracked_ids = {str(order_id) for order_id in tracked}
        self._tracked_recovery_order_ids = tracked_ids & self._recovery_order_ids
        if tracked_ids - self._recovery_order_ids:
            self._unidentified_recovery = True
        connector.set_position_mode(PositionMode.ONEWAY)
        connector.set_leverage(self.config.trading_pair, self.config.leverage)
        self._started_enabled = True
        self.state = GridState.RECONCILING

    def tick(self, timestamp: float):
        self._now = timestamp
        if not self.config.enabled:
            self.state = GridState.DISABLED
            return
        super().tick(timestamp)

    def on_tick(self):
        if not self._started_enabled or self.state is GridState.DISABLED:
            return
        if self.state is GridState.QUOTING and self._now >= self._next_refresh_at:
            self._begin_drain("refresh interval elapsed")
        if self.state is GridState.DRAINING:
            self._cancel_owned_orders()
        self._poll_account_snapshot()

    async def on_stop(self):
        if self._snapshot_task is not None and not self._snapshot_task.done():
            self._snapshot_task.cancel()
        if self.config.enabled:
            self._begin_drain("strategy stopped")
            self._cancel_owned_orders()
            retained = self.epoch.baseline + self.epoch.signed_fills if self.epoch is not None else None
            self.logger().warning(f"Stopped without liquidation; retained LIT net position estimate: {retained}")
        listener = getattr(self, "listen_to_executor_actions_task", None)
        if listener is not None:
            listener.cancel()

    def _poll_account_snapshot(self):
        if self._snapshot_task is not None:
            if not self._snapshot_task.done():
                if (self._snapshot_started_at is not None
                        and self._now - self._snapshot_started_at > self.config.max_data_age_seconds):
                    self._snapshot_task.cancel()
                    self._snapshot_task = None
                    self._snapshot_started_at = None
                    self._pause("authoritative account refresh exceeded freshness limit", drain=True)
                return
            task = self._snapshot_task
            self._snapshot_task = None
            self._snapshot_started_at = None
            try:
                account_snapshot = task.result()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._pause(f"authoritative account refresh failed: {exc}", drain=True)
                return
            self._process_snapshot(account_snapshot)
        if (self.state is not GridState.DISABLED and self._snapshot_task is None
                and self._now >= self._next_snapshot_at):
            connector = self.connectors[self.config.connector_name]
            method = getattr(connector, "get_grid_account_snapshot", None)
            if method is None:
                self._pause("connector lacks authoritative grid snapshot API", drain=True)
                return
            self._snapshot_task = asyncio.create_task(method(
                self.config.trading_pair,
                client_order_ids=list(self._owned_order_ids | self._recovery_order_ids),
                force_refresh=True,
            ))
            self._snapshot_started_at = self._now
            self._next_snapshot_at = self._now + self._snapshot_poll_seconds

    def _process_snapshot(self, account_snapshot: Dict[str, Any]):
        if (account_snapshot.get("net_position_known") is not True
                or account_snapshot.get("available_margin_known") is not True):
            self._pause("position or available margin is not authoritative", drain=True)
            return
        try:
            fetched_at = Decimal(str(account_snapshot["fetched_at"]))
            request_started_at = Decimal(str(account_snapshot["request_started_at"]))
            public_at = Decimal(str(account_snapshot["public_data_last_recv_time"]))
            net_position = Decimal(str(account_snapshot["net_position"]))
            margin = Decimal(str(account_snapshot["available_margin"]))
            private_at = Decimal(str(account_snapshot["private_stream_last_recv_time"]))
            account_index = account_snapshot["account_index"]
            if isinstance(account_index, bool) or not isinstance(account_index, int):
                raise ValueError("invalid account index")
            revision = account_snapshot.get("revision", account_snapshot.get("fetched_at"))
            active_ids = self._active_order_ids(account_snapshot["active_orders"])
        except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
            self._pause(f"invalid account snapshot: {exc}", drain=True)
            return
        now = Decimal(str(self._now))
        max_age = Decimal(str(self.config.max_data_age_seconds))
        private_max_age = Decimal(str(self.PRIVATE_STREAM_MAX_AGE_SECONDS))
        if (not fetched_at.is_finite() or not request_started_at.is_finite()
                or not public_at.is_finite() or not private_at.is_finite()
                or request_started_at <= 0 or fetched_at < request_started_at or now < fetched_at
                or now - request_started_at > max_age or fetched_at - request_started_at > max_age
                or now - fetched_at > max_age or now - public_at > max_age
                or now - private_at > private_max_age
                or account_snapshot.get("private_stream_connected") is not True):
            self._pause("stale or disconnected public/private data", drain=True)
            return
        known_ids = self._owned_order_ids | self._recovery_order_ids
        if self.state is GridState.DRAINING and revision != self._last_cancel_retry_revision:
            # A connector cancel is scheduled asynchronously and can fail without
            # producing an event. A fresh authoritative snapshot that still shows
            # our order active is the signal to retry, even while the safety
            # condition that initiated draining remains present.
            self._cancel_requested.difference_update(active_ids & known_ids)
            self._cancel_owned_orders()
            self._last_cancel_retry_revision = revision
        if (account_snapshot.get("market_state_known") is not True
                or account_snapshot.get("market_tradable") is not True
                or account_snapshot.get("force_reduce_only") is not False):
            self._pause("market is closed, reduce-only, or unknown", drain=True)
            return
        try:
            confirmed_leverage = Decimal(str(account_snapshot["leverage"]))
        except (InvalidOperation, KeyError, ValueError):
            confirmed_leverage = Decimal("NaN")
        if (account_snapshot.get("position_mode") not in {"ONEWAY", PositionMode.ONEWAY}
                or account_snapshot.get("leverage_confirmed") is not True
                or not confirmed_leverage.is_finite()
                or confirmed_leverage != Decimal(self.config.leverage)):
            self._pause("position mode or leverage is not authoritatively confirmed", drain=True)
            return
        if account_snapshot.get("collateral_token") != "USDG":
            self._pause("unexpected collateral token", drain=True)
            return
        if not net_position.is_finite() or abs(net_position) > self.config.max_abs_net_position:
            self._pause("position is invalid or above the configured cap", drain=True)
            return
        if not margin.is_finite() or margin < self._required_margin():
            self._pause("insufficient available USDG margin", drain=True)
            return
        if self._owned_order_ids and not self._live_book_is_in_range():
            self._pause("market is outside configured bounds", drain=True)
            return
        foreign_ids = active_ids - known_ids
        if foreign_ids:
            self._pause("external/manual active orders detected", drain=True)
            return
        if self.state is GridState.QUOTING and self.epoch is not None and self.epoch.orders:
            expected_position = self.epoch.baseline + self.epoch.signed_fills
            position_matches_fills = (
                net_position.quantize(self.epoch.amount_increment)
                == expected_position.quantize(self.epoch.amount_increment)
            )
            current_capacity_safe = (
                net_position + self.epoch.reserved_buy <= self.epoch.cap
                and net_position - self.epoch.reserved_sell >= -self.epoch.cap
            )
            if not position_matches_fills or not current_capacity_safe:
                self._pause("account position drift invalidates reserved exposure capacity", drain=True)
                return
        pending = account_snapshot.get("pending_submissions_unknown") is not False
        self._account_index = account_index
        if self._unidentified_recovery:
            self._pause("unidentified durable submission intent requires operator reconciliation", drain=True)
            return
        if self._recovery_order_ids:
            identities = {
                (entry["connector_domain"], entry["trading_pair"], entry["account_index"])
                for entry in self._recovery_entries_by_id.values()
            }
            if identities != {(self.config.connector_name, self.config.trading_pair, account_index)}:
                self._pause("durable intent journal belongs to a different account", drain=True)
                return
            uncancelable_active = (
                active_ids & (self._recovery_order_ids - self._tracked_recovery_order_ids)
            )
            if uncancelable_active:
                self._pause("journal order is active but missing restored connector tracking", drain=False)
                return
            recovered = self._reconcile_recovery(account_snapshot, revision, net_position, active_ids, pending)
            if self._recovery_epoch is not None and self._recovery_epoch.invalid_reason is not None:
                self._pause(self._recovery_epoch.invalid_reason, drain=True)
                return
            if recovered is None:
                self._pause("restored orders are not authoritatively terminal", drain=True)
                return
            if not recovered:
                if self.state not in {GridState.DRAINING, GridState.PAUSED}:
                    self.state = GridState.RECONCILING
                return
            try:
                self.epoch = ExposureEpoch(
                    net_position, self.config.max_abs_net_position, self._amount_increment()
                )
                self._journal.clear()
            except (ValueError, JournalError) as exc:
                self._pause(str(exc), drain=False)
                return
            self._recovery_order_ids.clear()
            self._tracked_recovery_order_ids.clear()
            self._recovery_entries_by_id.clear()
            self._recovery_epoch = None
            self.state = GridState.RECONCILING
            self._quote_if_safe(margin)
            return
        if self.epoch is None:
            if active_ids or pending:
                self._pause("startup account state contains unresolved orders", drain=False)
                return
            try:
                self.epoch = ExposureEpoch(net_position, self.config.max_abs_net_position, self._amount_increment())
            except ValueError as exc:
                self._pause(str(exc), drain=False)
                return
        elif self.epoch.orders and not self._authoritative_order_evidence(account_snapshot):
            return
        reconciled = self.epoch.observe(revision, net_position, active_ids, pending)
        if self.epoch.invalid_reason is not None:
            self._pause(self.epoch.invalid_reason, drain=True)
            return
        if not reconciled:
            if self.state not in {GridState.DRAINING, GridState.PAUSED}:
                self.state = GridState.RECONCILING
            return
        try:
            self.epoch = ExposureEpoch(net_position, self.config.max_abs_net_position, self._amount_increment())
        except ValueError as exc:
            self._pause(str(exc), drain=False)
            return
        self._owned_order_ids.clear()
        self._cancel_requested.clear()
        self._recovery_order_ids.clear()
        self._tracked_recovery_order_ids.clear()
        try:
            self._journal.clear()
        except JournalError as exc:
            self._pause(str(exc), drain=False)
            return
        self.state = GridState.RECONCILING
        self._quote_if_safe(margin)

    def _quote_if_safe(self, available_margin: Decimal):
        connector = self.connectors[self.config.connector_name]
        try:
            best_bid = Decimal(connector.get_price(self.config.trading_pair, False))
            best_ask = Decimal(connector.get_price(self.config.trading_pair, True))
        except Exception as exc:
            self._pause(f"book unavailable: {exc}", drain=True)
            return
        if (not best_bid.is_finite() or not best_ask.is_finite() or best_bid <= 0 or best_ask <= best_bid
                or best_ask < self.config.lower_price or best_bid > self.config.upper_price):
            self._pause("market is invalid or outside configured bounds", drain=True)
            return
        try:
            price_increment = self._price_increment(best_ask)
            bid, ask, _ = eligible_grid_prices(
                self.config.lower_price, self.config.upper_price, self.config.grid_levels,
                price_increment, best_bid, best_ask,
            )
        except ValueError as exc:
            self._pause(str(exc), drain=True)
            return
        side_capacity = {
            "BUY": self.epoch.cap - self.epoch.baseline - self.epoch.reserved_buy,
            "SELL": self.epoch.cap + self.epoch.baseline - self.epoch.reserved_sell,
        }
        amounts = {
            side: Decimal(connector.quantize_order_amount(
                self.config.trading_pair,
                min(self.config.order_amount_base, max(Decimal("0"), capacity)),
            ))
            for side, capacity in side_capacity.items()
        }
        bid = (
            bid if bid is not None and amounts["BUY"].is_finite() and amounts["BUY"] > 0
            and self._passes_runtime_minimums(amounts["BUY"], [bid]) else None
        )
        ask = (
            ask if ask is not None and amounts["SELL"].is_finite() and amounts["SELL"] > 0
            and self._passes_runtime_minimums(amounts["SELL"], [ask]) else None
        )
        prices = [price for price in (bid, ask) if price is not None]
        if not prices:
            self._pause("grid order is dust under runtime market metadata", drain=False)
            return
        required_margin = self._required_margin()
        if not available_margin.is_finite() or available_margin < required_margin:
            self._pause("insufficient available USDG margin", drain=False)
            return
        for side, price in (("BUY", bid), ("SELL", ask)):
            if price is not None:
                self._reserve_then_submit(side, amounts[side], price)
            if self.state is GridState.PAUSED:
                return
        self.state = GridState.QUOTING
        self._next_refresh_at = self._now + self.config.refresh_seconds

    def _reserve_then_submit(self, side: str, amount: Decimal, price: Decimal):
        self._submission_sequence += 1
        provisional_id = f"pending-{self._submission_sequence}"
        try:
            reserved = self.epoch.reserve(provisional_id, side, amount)
        except ReservationError:
            return
        try:
            if self._account_index is None:
                raise JournalError("cannot persist an intent before account identity is authoritative")
            self._journal.reserve(
                provisional_id,
                side,
                reserved,
                self.epoch.baseline,
                self.config.connector_name,
                self.config.trading_pair,
                self._account_index,
            )
        except JournalError as exc:
            self._pause(str(exc), drain=True)
            return
        try:
            method = self.buy if side == "BUY" else self.sell
            client_order_id = method(
                self.config.connector_name, self.config.trading_pair, reserved,
                OrderType.LIMIT_MAKER, price, position_action=PositionAction.OPEN,
            )
            if not client_order_id:
                raise RuntimeError("submission returned no client order id")
            client_order_id = str(client_order_id)
            # Once the connector returns an ID, retain it for cancellation before
            # any fallible in-memory rename or durable journal update.
            self._owned_order_ids.add(client_order_id)
            self.epoch.rename_order(provisional_id, client_order_id)
            self._journal.bind_client_order_id(provisional_id, client_order_id)
        except Exception as exc:
            self._pause(f"submission outcome unknown: {exc}", drain=True)

    def _begin_drain(self, reason: str):
        if self.state is not GridState.DISABLED:
            self.state = GridState.DRAINING
            self._pause_reason = reason

    def _pause(self, reason: str, drain: bool):
        self._pause_reason = reason
        if drain and (self._owned_order_ids or self._tracked_recovery_order_ids):
            self.state = GridState.DRAINING
            self._cancel_owned_orders()
        else:
            self.state = GridState.PAUSED

    def _cancel_owned_orders(self):
        cancelable_ids = self._owned_order_ids | self._tracked_recovery_order_ids
        for order_id in cancelable_ids - self._cancel_requested:
            self._cancel_requested.add(order_id)
            self.cancel(self.config.connector_name, self.config.trading_pair, order_id)

    def did_fill_order(self, event: OrderFilledEvent):
        if not self.config.enabled or self.epoch is None:
            return
        if not self.epoch.record_fill(event.order_id, event.exchange_trade_id, event.amount):
            self._pause(self.epoch.invalid_reason or "invalid fill", drain=True)
            return
        self._begin_drain("fill received")

    def did_cancel_order(self, event: OrderCancelledEvent):
        self._mark_local_terminal(event.order_id)

    def did_fail_order(self, event: MarketOrderFailureEvent):
        self._mark_local_terminal(event.order_id)

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        self._mark_local_terminal(event.order_id)

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        self._mark_local_terminal(event.order_id)

    def _mark_local_terminal(self, order_id: str):
        if not self.config.enabled or self.epoch is None:
            return
        if not self.epoch.mark_terminal(order_id):
            self._pause(self.epoch.invalid_reason or "unknown terminal order", drain=True)

    def _authoritative_order_evidence(self, account_snapshot: Dict[str, Any]) -> bool:
        evidence = account_snapshot.get("orders_by_client_id")
        if not isinstance(evidence, dict):
            return False
        for order_id, order in self.epoch.orders.items():
            item = evidence.get(order_id)
            if not isinstance(item, dict) or item.get("terminal") is not True:
                return False
            if item.get("cumulative_fill_known") is not True:
                return False
            try:
                cumulative = Decimal(str(item["cumulative_fill_base"]))
            except (InvalidOperation, KeyError, ValueError):
                return False
            if not self.epoch.set_authoritative_cumulative(order_id, cumulative):
                self._pause(self.epoch.invalid_reason or "invalid authoritative fill", drain=True)
                return False
            order.terminal = True
        return True

    def _reconcile_recovery(
        self,
        account_snapshot: Dict[str, Any],
        revision: Any,
        net_position: Decimal,
        active_ids: Set[str],
        pending: bool,
    ) -> Optional[bool]:
        evidence = account_snapshot.get("orders_by_client_id")
        if not isinstance(evidence, dict):
            return None
        if self._recovery_epoch is None:
            try:
                baseline = Decimal(next(iter(self._recovery_entries_by_id.values()))["baseline"])
                epoch = ExposureEpoch(
                    baseline, self.config.max_abs_net_position, self._amount_increment()
                )
                for order_id, entry in self._recovery_entries_by_id.items():
                    amount = Decimal(entry["amount"])
                    if epoch.reserve(order_id, entry["side"], amount) != amount:
                        raise ValueError("journal amount is not aligned to runtime size precision")
                self._recovery_epoch = epoch
            except (InvalidOperation, KeyError, ReservationError, StopIteration, ValueError) as exc:
                self._pause(f"invalid durable recovery epoch: {exc}", drain=True)
                return None
        for order_id in self._recovery_order_ids:
            item = evidence.get(order_id)
            if (not isinstance(item, dict) or item.get("terminal") is not True
                    or item.get("cumulative_fill_known") is not True):
                return None
            try:
                cumulative = Decimal(str(item["cumulative_fill_base"]))
            except (InvalidOperation, KeyError, ValueError):
                return None
            if not self._recovery_epoch.set_authoritative_cumulative(order_id, cumulative):
                return False
            self._recovery_epoch.orders[order_id].terminal = True
        return self._recovery_epoch.observe(revision, net_position, active_ids, pending)

    @staticmethod
    def _active_order_ids(active_orders: List[Any]) -> Set[str]:
        result = set()
        for order in active_orders:
            if isinstance(order, dict):
                order_id = order.get("client_order_id_str", order.get("client_order_id"))
            else:
                order_id = getattr(order, "client_order_id", None)
            if order_id is None or str(order_id) == "":
                raise ValueError("active account order lacks client order id")
            result.add(str(order_id))
        return result

    def _amount_increment(self) -> Decimal:
        connector = self.connectors[self.config.connector_name]
        rule = getattr(connector, "trading_rules", {}).get(self.config.trading_pair)
        if rule is None:
            raise ValueError("runtime trading rule is unavailable")
        increment = Decimal(rule.min_base_amount_increment)
        if not increment.is_finite() or increment <= 0:
            raise ValueError("runtime amount increment is invalid")
        return increment

    def _price_increment(self, price: Decimal) -> Decimal:
        connector = self.connectors[self.config.connector_name]
        rule = getattr(connector, "trading_rules", {}).get(self.config.trading_pair)
        if rule is None:
            raise ValueError("runtime trading rule is unavailable")
        increment = Decimal(rule.min_price_increment)
        if not increment.is_finite() or increment <= 0:
            raise ValueError("runtime price increment is invalid")
        return increment

    def _passes_runtime_minimums(self, amount: Decimal, prices: List[Decimal]) -> bool:
        rule = getattr(self.connectors[self.config.connector_name], "trading_rules", {}).get(self.config.trading_pair)
        if rule is None:
            return False
        min_size = Decimal(getattr(rule, "min_order_size", Decimal("NaN")))
        max_size = Decimal(getattr(rule, "max_order_size", Decimal("NaN")))
        min_notional = max(
            Decimal(getattr(rule, "min_notional_size", Decimal("NaN"))),
            Decimal(getattr(rule, "min_order_value", Decimal("0"))),
        )
        if (not getattr(rule, "supports_limit_orders", False)
                or not min_size.is_finite() or min_size < 0
                or not max_size.is_finite() or max_size <= 0
                or not min_notional.is_finite() or min_notional < 0):
            return False
        return min_size <= amount <= max_size and all(amount * price >= min_notional for price in prices)

    def _live_book_is_in_range(self) -> bool:
        connector = self.connectors[self.config.connector_name]
        try:
            best_bid = Decimal(connector.get_price(self.config.trading_pair, False))
            best_ask = Decimal(connector.get_price(self.config.trading_pair, True))
        except Exception:
            return False
        return (
            best_bid.is_finite()
            and best_ask.is_finite()
            and best_bid > 0
            and best_ask > best_bid
            and best_ask >= self.config.lower_price
            and best_bid <= self.config.upper_price
        )

    def _required_margin(self) -> Decimal:
        return (
            self.config.upper_price * self.config.max_abs_net_position / Decimal(self.config.leverage)
            + self.config.margin_reserve_usdg
        )

    def format_status(self) -> str:
        baseline = self.epoch.baseline if self.epoch is not None else "unreconciled"
        return f"Robinhood neutral grid: {self.state.value}; baseline={baseline}; reason={self._pause_reason or 'none'}"
