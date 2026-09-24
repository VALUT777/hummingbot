import asyncio
import math
import time
from contextlib import AsyncExitStack
from decimal import Decimal
from typing import Any, Callable, Collection, Dict, List, Optional, Set, Tuple

from lighter import AccountApi, SignerClient

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.lighter_perpetual import (
    lighter_perpetual_constants as CONSTANTS,
    lighter_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_api_order_book_data_source import (
    LighterPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_api_utils import (
    LighterHistoryPage,
    LighterTransportOutcome,
    LighterTransportResult,
    account_index_from_account,
    decimal_to_exchange_int,
    exact_int,
    exact_scaled_int,
    extract_account_snapshot,
    history_page_from_response,
    leverage_from_account_margin_percentage,
    markets_by_exchange_symbol,
    markets_by_id,
    markets_by_trading_pair,
    normalize_timestamp_to_seconds,
    order_state_from_order_data,
    own_trade_details,
    perpetual_markets_from_exchange_info,
    strict_decimal,
    strict_market_position,
    trading_pair_symbol_map,
    validate_history_page_request,
)
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_auth import LighterAuth
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_user_stream_data_source import (
    LighterPerpetualUserStreamDataSource,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_numeric_client_order_id
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.utils.estimate_fee import build_perpetual_trade_fee
from hummingbot.core.utils.tracking_nonce import NonceCreator
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class LighterPerpetualDerivative(PerpetualDerivativePyBase):
    web_utils = web_utils

    SHORT_POLL_INTERVAL = 5.0
    LONG_POLL_INTERVAL = 60.0

    def __init__(
        self,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        lighter_perpetual_l1_address: str = None,
        lighter_perpetual_account_index: int = None,
        lighter_perpetual_api_key_index: int = None,
        lighter_perpetual_api_public_key: str = None,
        lighter_perpetual_api_private_key: str = None,
        lighter_perpetual_account_limit: str = "Standard",
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DOMAIN,
    ):
        self._domain_settings = CONSTANTS.get_domain_settings(domain)
        self._l1_address = lighter_perpetual_l1_address
        self._account_index = (
            int(lighter_perpetual_account_index)
            if lighter_perpetual_account_index not in (None, "")
            else None
        )
        self._api_key_index = (
            int(lighter_perpetual_api_key_index)
            if lighter_perpetual_api_key_index not in (None, "")
            else None
        )
        self._api_public_key = lighter_perpetual_api_public_key
        self._api_private_key = lighter_perpetual_api_private_key
        self._api_account_limit = lighter_perpetual_account_limit
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        self._domain = domain
        self._nonce_creator = NonceCreator.for_milliseconds()
        self._markets_by_id = {}
        self._markets_by_trading_pair = {}
        self._markets_by_exchange_symbol = {}
        # Populated only from authoritative account REST readback. A successful sendTx
        # response merely means the leverage update was accepted for processing.
        self._confirmed_leverage_by_trading_pair: Dict[str, Decimal] = {}
        self._tx_lock = asyncio.Lock()
        self._account_ready_lock = asyncio.Lock()
        # Single-flight task for WS-triggered balance refresh: Lighter's account_all_assets
        # event lacks `available_balance`, so we use the event as a trigger to refresh from REST.
        self._balance_refresh_task: Optional[asyncio.Task] = None
        # Neutral grid: payload-free "poll authoritative history soon" signals (never proof).
        self._history_wakeup_listeners: List[Callable[[], None]] = []
        # Every client id sent through `submit_with_client_id`, restored from tracking state or
        # registered by the engine, for the whole process lifetime (never removed). Such orders are
        # tracked (events/WS updates) but reconciled by the neutral-grid engine from paginated
        # authoritative history: no legacy per-order polls, lost-order handling, generic cancel or
        # re-send of the same client id.
        self._history_reconciled_client_order_ids: Set[str] = set()
        # CIDs currently waiting at the pre-send gates. This closes the single-process race before
        # an order is registered with the tracker; ownership is released if the signer is never run.
        self._pending_cid_submissions: Set[str] = set()
        self._real_time_balance_update = False
        self._signer_client = self._create_signer_client() if trading_required and self._account_index is not None else None
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @property
    def account_index(self) -> int:
        return self._account_index

    @property
    def api_key_index(self) -> int:
        return self._api_key_index

    @property
    def name(self) -> str:
        return self._domain

    @property
    def authenticator(self) -> Optional[LighterAuth]:
        if self._trading_required and self._signer_client is not None:
            return LighterAuth(self._signer_client, api_key_index=self._api_key_index, api_public_key=self._api_public_key)
        return None

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return CONSTANTS.generate_account_limit(self._api_account_limit)

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.BROKER_ID

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return 120

    async def _make_network_check_request(self):
        await self._api_get(path_url=self.check_network_request_path)

    async def start_network(self):
        if self.is_trading_required:
            await self._ensure_account_ready()
        await super().start_network()

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    def supported_position_modes(self):
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.sell_order_collateral_token

    def buy(
        self,
        trading_pair: str,
        amount: Decimal,
        order_type=OrderType.LIMIT,
        price: Decimal = s_decimal_NaN,
        **kwargs,
    ) -> str:
        order_id = self._new_client_order_id()
        position_action = kwargs.pop("position_action", PositionAction.OPEN)
        safe_ensure_future(
            self._create_order(
                trade_type=TradeType.BUY,
                order_id=order_id,
                trading_pair=trading_pair,
                amount=amount,
                order_type=order_type,
                price=price,
                position_action=position_action,
                **kwargs,
            )
        )
        return order_id

    def sell(
        self,
        trading_pair: str,
        amount: Decimal,
        order_type: OrderType = OrderType.LIMIT,
        price: Decimal = s_decimal_NaN,
        **kwargs,
    ) -> str:
        order_id = self._new_client_order_id()
        position_action = kwargs.pop("position_action", PositionAction.OPEN)
        safe_ensure_future(
            self._create_order(
                trade_type=TradeType.SELL,
                order_id=order_id,
                trading_pair=trading_pair,
                amount=amount,
                order_type=order_type,
                price=price,
                position_action=position_action,
                **kwargs,
            )
        )
        return order_id

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        exchange_info = await self._api_get(
            path_url=CONSTANTS.EXCHANGE_INFO_PATH_URL,
            params={"filter": "all"},
        )
        prices = []
        for market in perpetual_markets_from_exchange_info(exchange_info, domain=self.domain):
            prices.append({"symbol": market.exchange_symbol, "price": str(market.raw_info["last_trade_price"])})
        return prices

    async def fetch_maker_only_api_key_indexes(self) -> frozenset[int]:
        """Read the account's maker-only API key set without changing key settings.

        The endpoint is private and typed by lighter-sdk. Its response is still checked explicitly because generated
        models can be constructed without validation by ``from_dict``. Unknown or malformed capability must never be
        interpreted as ordinary LIMIT support by a caller.
        """
        if self._account_index is None or type(self._api_key_index) is not int \
                or not 0 <= self._api_key_index <= CONSTANTS.MAX_API_KEY_INDEX \
                or self._signer_client is None or self._auth is None:
            raise IOError("Lighter maker-only capability requires a resolved trading account and API key.")
        account_api = AccountApi(self._signer_client.api_client)
        async with self._throttler.execute_task(limit_id=CONSTANTS.GET_MAKER_ONLY_API_KEYS_PATH_URL):
            authorization = await self._auth._get_auth_token()
            response = await account_api.get_maker_only_api_keys(
                authorization=authorization, account_index=self._account_index)
        code = getattr(response, "code", None)
        if type(code) is not int:
            raise IOError("Lighter maker-only capability response has an invalid code.")
        if code != 200:
            raise IOError(f"Lighter maker-only capability response failed with code {code}.")
        indexes = getattr(response, "api_key_indexes", None)
        if not isinstance(indexes, list) or any(
                type(index) is not int or not 0 <= index <= CONSTANTS.MAX_API_KEY_INDEX for index in indexes):
            raise IOError("Lighter maker-only capability response has invalid API key indexes.")
        return frozenset(indexes)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return CONSTANTS.UNKNOWN_ORDER_MESSAGE in str(cancelation_exception)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(throttler=self._throttler, auth=self._auth)

    async def _make_trading_rules_request(self) -> Any:
        info = await self._api_get(
            path_url=self.trading_rules_request_path,
            params={"filter": "all"},
        )
        return info

    async def _make_trading_pairs_request(self) -> Any:
        return await self._make_trading_rules_request()

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return LighterPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return LighterPerpetualUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    async def _status_polling_loop_fetch_updates(self):
        await self._ensure_account_ready()
        await safe_gather(
            self._update_trade_history(),
            self._update_orders(),
            self._update_balances(),
            self._update_positions(),
        )

    async def _update_order_status(self):
        await self._update_orders()

    async def _update_orders(self):
        # Orders submitted with a pre-persisted client id are reconciled by the neutral-grid engine
        # from paginated authoritative history. Polling each of them here would cost 300-400 request
        # weight per order per poll and starve the shared 18000/min pool; legacy orders are polled
        # exactly as before.
        orders = [
            order for order in self.in_flight_orders.copy().values()
            if order.client_order_id not in self._history_reconciled_client_order_ids
        ]
        await self._update_orders_with_error_handler(
            orders=orders, error_handler=self._handle_update_error_for_active_order
        )

    async def _update_lost_orders(self):
        orders = [
            order for client_order_id, order in self._order_tracker.lost_orders.copy().items()
            if client_order_id not in self._history_reconciled_client_order_ids
        ]
        await self._update_orders_with_error_handler(
            orders=orders, error_handler=self._handle_update_error_for_lost_order
        )

    async def _cancel_lost_orders(self):
        # History-reconciled orders are cancelled only by the engine (durable intent first).
        for client_order_id, lost_order in list(self._order_tracker.lost_orders.items()):
            if client_order_id in self._history_reconciled_client_order_ids:
                continue
            await self._execute_order_cancel(order=lost_order)

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            order.to_limit_order() for order in self.in_flight_orders.values()
            if order.client_order_id not in self._history_reconciled_client_order_ids
        ]

    @property
    def tracking_states(self) -> Dict[str, Any]:
        states = super().tracking_states
        for client_order_id, state in states.items():
            if client_order_id in self._history_reconciled_client_order_ids and isinstance(state, dict):
                state[CONSTANTS.HISTORY_RECONCILED_STATE_MARKER] = True
        return states

    def restore_tracking_states(self, saved_states: Dict[str, Any]):
        for client_order_id, state in saved_states.items():
            if isinstance(state, dict) and state.get(CONSTANTS.HISTORY_RECONCILED_STATE_MARKER) is True:
                self._history_reconciled_client_order_ids.add(str(client_order_id))
        super().restore_tracking_states(saved_states)

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        # Hummingbot stop/exit must not cancel neutral-grid orders behind the engine's back: they are
        # skipped by `_execute_cancel` and omitted from the result (the engine owns their cancel).
        results = await super().cancel_all(timeout_seconds)
        return [result for result in results if result.order_id not in self._history_reconciled_client_order_ids]

    async def _execute_cancel(self, trading_pair: str, order_id: str) -> str:
        if order_id in self._history_reconciled_client_order_ids:
            self.logger().warning(
                f"Order {order_id} is owned by the neutral grid engine; it is cancelled only through "
                f"cancel_with_client_id after a durable cancel intent."
            )
            return None
        return await super()._execute_cancel(trading_pair, order_id)

    async def _update_lost_orders_status(self):
        await self._update_lost_orders()

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ) -> Tuple[str, float]:
        await self._ensure_account_ready()
        market = self.market_info_for_trading_pair(trading_pair)
        price = self._effective_order_price(
            trading_pair=trading_pair,
            trade_type=trade_type,
            order_type=order_type,
            price=price,
        )
        base_amount = decimal_to_exchange_int(amount, market.size_decimals)
        price_int = decimal_to_exchange_int(price, market.price_decimals)
        client_order_index = int(order_id)
        reduce_only = position_action == PositionAction.CLOSE

        async with self._tx_lock:
            if order_type is OrderType.MARKET:
                _, tx_response, error = await self._signer_client.create_market_order(
                    market_index=market.market_id,
                    client_order_index=client_order_index,
                    base_amount=base_amount,
                    avg_execution_price=price_int,
                    is_ask=trade_type is TradeType.SELL,
                    reduce_only=reduce_only,
                )
            else:
                tif = self._signer_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
                if order_type is OrderType.LIMIT_MAKER:
                    tif = self._signer_client.ORDER_TIME_IN_FORCE_POST_ONLY
                _, tx_response, error = await self._signer_client.create_order(
                    market_index=market.market_id,
                    client_order_index=client_order_index,
                    base_amount=base_amount,
                    price=price_int,
                    is_ask=trade_type is TradeType.SELL,
                    order_type=self._signer_client.ORDER_TYPE_LIMIT,
                    time_in_force=tif,
                    reduce_only=reduce_only,
                )

        if error is not None:
            raise IOError(f"Error submitting Lighter order {order_id}: {error}")
        if not self._is_tx_response_success(tx_response):
            raise IOError(f"Error submitting Lighter order {order_id}: {tx_response}")
        return order_id, self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        await self._ensure_account_ready()
        market = self.market_info_for_trading_pair(tracked_order.trading_pair)
        order_data = await self._find_order(tracked_order=tracked_order, include_inactive=True)
        if order_data is None:
            raise IOError(f"{CONSTANTS.ORDER_NOT_EXIST_MESSAGE}: {order_id}")

        async with self._tx_lock:
            _, tx_response, error = await self._signer_client.cancel_order(
                market_index=market.market_id,
                order_index=int(order_data["order_id"]),
            )
        if error is not None:
            raise IOError(f"Error cancelling Lighter order {order_id}: {error}")
        if not self._is_tx_response_success(tx_response):
            raise IOError(f"Error cancelling Lighter order {order_id}: {tx_response}")
        return True

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        position_action: PositionAction,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        return build_perpetual_trade_fee(
            exchange=self.name,
            is_maker=is_maker if is_maker is not None else order_type is OrderType.LIMIT_MAKER,
            position_action=position_action,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )

    async def _update_trading_fees(self):
        return

    async def _update_trade_history(self):
        fillable_orders = [
            order for order in self._order_tracker.all_fillable_orders.values()
            if order.client_order_id not in self._history_reconciled_client_order_ids
        ]
        if not fillable_orders:
            return
        if self._markets_by_exchange_symbol == {}:
            await self._update_trading_rules()

        market_ids = {
            self.market_info_for_trading_pair(order.trading_pair).market_id
            for order in fillable_orders
        }
        for market_id in market_ids:
            response = await self._api_get(
                path_url=CONSTANTS.RECENT_TRADES_PATH_URL,
                params={
                    "market_id": market_id,
                    "limit": 100,
                },
            )
            for trade in response.get("trades", []):
                trade_update = self._trade_update_from_trade(trade)
                if trade_update is not None:
                    self._order_tracker.process_trade_update(trade_update)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        return []

    @staticmethod
    def _order_misc_updates(order_data: Dict[str, Any], state: OrderState) -> Optional[Dict[str, Any]]:
        if state != OrderState.FAILED:
            return None

        status = str(order_data.get("status", ""))
        # Kept both fields populated for compatibility with existing failure-event logging.
        return {
            "error_type": status,
            "error_message": f"Exchange order status: {status}",
        }

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        order_data = await self._find_order(tracked_order=tracked_order, include_inactive=True)
        if order_data is None:
            # Lighter's REST endpoints may not have indexed a just-submitted order yet.
            # Within the grace window, report it as still open rather than not-found, so the
            # order tracker doesn't escalate a live order to "lost".
            age = self.current_timestamp - tracked_order.creation_timestamp
            if age < CONSTANTS.ORDER_NOT_FOUND_GRACE_PERIOD:
                return OrderUpdate(
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=self.current_timestamp,
                    new_state=tracked_order.current_state,
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=tracked_order.exchange_order_id,
                )
            raise IOError(f"{CONSTANTS.ORDER_NOT_EXIST_MESSAGE}: {tracked_order.client_order_id}")
        new_state = order_state_from_order_data(order_data)
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=normalize_timestamp_to_seconds(
                order_data.get("updated_at", order_data.get("transaction_time"))
            ),
            new_state=new_state,
            client_order_id=str(order_data["client_order_id"]),
            exchange_order_id=str(order_data["order_id"]),
            misc_updates=self._order_misc_updates(order_data=order_data, state=new_state),
        )

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        account_response = await self._api_get(
            path_url=CONSTANTS.BALANCE_PATH_URL,
            params=self._account_lookup_params(),
        )
        try:
            account = extract_account_snapshot(
                account_response, account_index=self._account_index, l1_address=self._l1_address
            )
        except ValueError as exc:
            raise IOError("Lighter account snapshot contains an invalid account index.") from exc
        self._set_account_index_from_account(account)
        available = self._safe_decimal(account.get("available_balance", "0"))
        assets = account.get("assets")
        if not isinstance(assets, list):
            raise IOError("Lighter account response is missing assets data.")
        if self._domain == CONSTANTS.ROBINHOOD_DOMAIN:
            symbol_rows = [
                asset for asset in assets
                if isinstance(asset, dict)
                and str(asset.get("symbol", "")).upper() == self._domain_settings.collateral_token
            ]
            try:
                collateral_asset_id = exact_int(symbol_rows[0].get("asset_id"), "asset_id")
            except (IndexError, ValueError):
                collateral_asset_id = None
            if (
                len(symbol_rows) != 1
                or collateral_asset_id != CONSTANTS.ROBINHOOD_COLLATERAL_ASSET_ID
            ):
                raise IOError("Lighter account response must contain exactly one valid USDG asset 3 row.")

        for asset in assets:
            asset_name = str(asset["symbol"]).upper()
            # spot_balance = self._safe_decimal(asset.get("balance", "0"))
            locked_balance = self._safe_decimal(asset.get("locked_balance", "0"))
            total_balance = self._safe_decimal(asset.get("margin_balance", "0"))
            self._account_balances[asset_name] = total_balance
            unlocked_balance = max(total_balance - locked_balance, Decimal("0"))
            self._account_available_balances[asset_name] = (
                min(available, unlocked_balance)
                if asset_name == self._domain_settings.collateral_token
                else unlocked_balance
            )
            remote_asset_names.add(asset_name)

        for asset_name in local_asset_names.difference(remote_asset_names):
            del self._account_balances[asset_name]
            del self._account_available_balances[asset_name]

    def _schedule_balance_refresh(self):
        # Trigger an out-of-band `_update_balances` REST call. Single-flight: if a
        # refresh is already in progress, this is a no-op (the running task will pick
        # up the latest state).
        if self._balance_refresh_task is None or self._balance_refresh_task.done():
            self._balance_refresh_task = safe_ensure_future(self._safe_update_balances())

    async def _safe_update_balances(self):
        try:
            await self._update_balances()
        except Exception:
            self.logger().exception("WS-triggered balance refresh failed.")

    async def _update_positions(self):
        account_response = await self._api_get(
            path_url=CONSTANTS.BALANCE_PATH_URL,
            params=self._account_lookup_params(),
        )
        try:
            account = extract_account_snapshot(
                account_response, account_index=self._account_index, l1_address=self._l1_address
            )
        except ValueError as exc:
            raise IOError("Lighter account snapshot contains an invalid account index.") from exc
        self._set_account_index_from_account(account)

        active_position_keys = set()
        for raw_position in account.get("positions", []):
            position = self._parse_position(raw_position)
            if position is None:
                continue
            pos_key = self._perpetual_trading.position_key(position.trading_pair, position.position_side)
            active_position_keys.add(pos_key)
            self._perpetual_trading.set_position(pos_key, position)

        for position_key in list(self._perpetual_trading.account_positions.keys()):
            if position_key not in active_position_keys:
                self._perpetual_trading.remove_position(position_key)

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        if mode is PositionMode.ONEWAY:
            return True, ""
        return False, "Lighter only supports ONEWAY position mode."

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        await self._ensure_account_ready()
        if self._signer_client is None:
            return False, "Connector is not configured for trading."

        if not hasattr(self._signer_client, "update_leverage"):
            return False, "This lighter-sdk version does not support leverage updates."

        market = self.market_info_for_trading_pair(trading_pair)
        margin_mode = getattr(self._signer_client, "CROSS_MARGIN_MODE", 0)
        try:
            _, tx_response, error = await self._signer_client.update_leverage(
                market_index=market.market_id,
                margin_mode=margin_mode,
                leverage=int(leverage),
            )
        except Exception as e:
            return False, str(e)

        if error is not None:
            return False, f"Error updating leverage: {error}"

        if not self._is_tx_response_success(tx_response):
            return False, f"Unexpected leverage response: {tx_response}"

        return True, ""

    def confirmed_leverage(self, trading_pair: str) -> Optional[Decimal]:
        """Leverage most recently confirmed by an authoritative account readback."""
        return self._confirmed_leverage_by_trading_pair.get(trading_pair)

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[float, Decimal, Decimal]:
        if self._markets_by_exchange_symbol == {}:
            await self._update_trading_rules()
        market = self.market_info_for_trading_pair(trading_pair)
        response = await self._api_get(
            path_url=CONSTANTS.POSITION_FUNDING_PATH_URL,
            params={
                "account_index": self._account_index,
                "market_id": market.market_id,
                "limit": 1,
            },
            is_auth_required=True,
        )

        entries = response.get("position_fundings", response.get("fundings", []))
        if len(entries) == 0:
            return 0, Decimal("-1"), Decimal("-1")

        latest = entries[0]
        payment = self._safe_decimal(latest.get("change", "0"))
        if payment == Decimal("0"):
            return 0, Decimal("-1"), Decimal("-1")

        rate = self._safe_decimal(latest.get("rate", "-1"))
        timestamp_ms = int(latest.get("timestamp", 0))
        return timestamp_ms * 1e-3, rate, payment

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        if trading_pair not in self._markets_by_trading_pair:
            await self._update_trading_rules()
        if trading_pair not in self._markets_by_trading_pair:
            raise ValueError(f"Market info not available for {trading_pair}")
        market = self.market_info_for_trading_pair(trading_pair)
        response = await self._api_get(
            path_url=CONSTANTS.EXCHANGE_INFO_PATH_URL,
            params={"market_id": market.market_id},
        )
        refreshed_markets = perpetual_markets_from_exchange_info(response, domain=self.domain)
        if len(refreshed_markets) == 0:
            return float(self._safe_decimal(market.raw_info.get("last_trade_price", "0")))
        return float(self._safe_decimal(refreshed_markets[0].raw_info["last_trade_price"]))

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                channel = str(event_message.get("channel", ""))
                if channel.startswith(f"{CONSTANTS.ACCOUNT_ALL_ORDERS_CHANNEL}:"):
                    self._signal_history_wakeup()
                    self._process_order_events(event_message.get("orders", {}))
                elif channel.startswith(f"{CONSTANTS.ACCOUNT_ALL_TRADES_CHANNEL}:"):
                    self._signal_history_wakeup()
                    self._process_trade_events(event_message.get("trades", {}))
                elif channel.startswith(f"{CONSTANTS.ACCOUNT_ALL_ASSETS_CHANNEL}:"):
                    # Lighter's assets event has no `available_balance` — use it as a
                    # signal to fetch the authoritative balance snapshot from REST.
                    self._schedule_balance_refresh()
                elif channel.startswith(f"{CONSTANTS.ACCOUNT_ALL_POSITIONS_CHANNEL}:"):
                    self._signal_history_wakeup()
                    self._process_position_events(event_message.get("positions", {}))
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        markets = perpetual_markets_from_exchange_info(exchange_info_dict, domain=self.domain)
        self._markets_by_id = markets_by_id(markets)
        self._markets_by_trading_pair = markets_by_trading_pair(markets)
        self._markets_by_exchange_symbol = markets_by_exchange_symbol(markets)
        return [
            market.trading_rule(collateral_token=self._domain_settings.collateral_token)
            for market in markets
        ]

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        markets = perpetual_markets_from_exchange_info(exchange_info, domain=self.domain)
        self._markets_by_id = markets_by_id(markets)
        self._markets_by_trading_pair = markets_by_trading_pair(markets)
        self._markets_by_exchange_symbol = markets_by_exchange_symbol(markets)
        self._set_trading_pair_symbol_map(trading_pair_symbol_map(markets))

    def market_info_for_trading_pair(self, trading_pair: str):
        return self._markets_by_trading_pair[trading_pair]

    def market_info_for_market_id(self, market_id: int):
        return self._markets_by_id[int(market_id)]

    def _new_client_order_id(self) -> str:
        return str(
            get_new_numeric_client_order_id(
                nonce_creator=self._nonce_creator,
                max_id_bit_count=CONSTANTS.MAX_CLIENT_ORDER_ID_BIT_COUNT,
            )
        )

    def _effective_order_price(
        self,
        trading_pair: str,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
    ) -> Decimal:
        if order_type is not OrderType.MARKET:
            return price
        reference_price = self.get_mid_price(trading_pair) if price.is_nan() else price
        multiplier = Decimal("1") + CONSTANTS.MARKET_ORDER_SLIPPAGE
        if trade_type is TradeType.SELL:
            multiplier = Decimal("1") - CONSTANTS.MARKET_ORDER_SLIPPAGE
        return self.quantize_order_price(trading_pair, reference_price * multiplier)

    def _create_signer_client(self):
        if self._account_index is None or self._api_key_index is None or self._api_private_key is None:
            raise ValueError(
                "Lighter trading requires an L1 address or account index, plus API key index and API private key."
            )
        client = None
        try:
            client = SignerClient(
                url=web_utils.public_rest_url(domain=self._domain),
                account_index=self._account_index,
                api_private_keys={self._api_key_index: self._api_private_key},
                chain_id=self._domain_settings.chain_id,
            )
        except Exception as e:
            raise IOError(f"Error creating Lighter signer client: {e}")
        return client

    async def _find_order(self, tracked_order: InFlightOrder, include_inactive: bool) -> Optional[Dict[str, Any]]:
        await self._ensure_account_ready()
        market = self.market_info_for_trading_pair(tracked_order.trading_pair)
        active_orders = await self._api_get(
            path_url=CONSTANTS.ACCOUNT_ACTIVE_ORDERS_PATH_URL,
            params={
                "account_index": self._account_index,
                "market_id": market.market_id,
            },
            is_auth_required=True,
        )
        order = self._match_order(tracked_order=tracked_order, orders=active_orders.get("orders", []))
        if order is not None or not include_inactive:
            return order

        inactive_orders = await self._api_get(
            path_url=CONSTANTS.ACCOUNT_INACTIVE_ORDERS_PATH_URL,
            params={
                "account_index": self._account_index,
                "market_id": market.market_id,
                "limit": 100,
            },
            is_auth_required=True,
        )
        return self._match_order(tracked_order=tracked_order, orders=inactive_orders.get("orders", []))

    def _account_lookup_params(self) -> Dict[str, Any]:
        if self._account_index is not None:
            return {"by": CONSTANTS.ACCOUNT_LOOKUP_BY_INDEX, "value": self._account_index, "active_only": "true"}
        if self._l1_address is not None:
            return {"by": CONSTANTS.ACCOUNT_LOOKUP_BY_L1_ADDRESS, "value": self._l1_address, "active_only": "true"}
        raise ValueError("Lighter requires an L1 address or account index to look up account balances.")

    def _set_account_index_from_account(self, account: Dict[str, Any]):
        if self._account_index is None:
            self._account_index = account_index_from_account(account)

    async def _ensure_account_ready(self):
        if not self.is_trading_required:
            return
        async with self._account_ready_lock:
            if self._markets_by_exchange_symbol == {}:
                await self._update_trading_rules()
            if self._account_index is None:
                account_response = await self._api_get(
                    path_url=CONSTANTS.BALANCE_PATH_URL,
                    params=self._account_lookup_params(),
                )
                account = extract_account_snapshot(account_response, l1_address=self._l1_address)
                self._set_account_index_from_account(account)
            if self._signer_client is None:
                self._signer_client = self._create_signer_client()
                self._auth = self.authenticator
                self._web_assistants_factory = self._create_web_assistants_factory()
                self._user_stream_tracker = self._create_user_stream_tracker()

    async def get_grid_account_snapshot(
        self,
        trading_pair: str,
        client_order_ids: Optional[Collection[str]] = None,
        force_refresh: bool = True,
    ) -> Dict[str, Any]:
        """Return a fresh, read-only reconciliation snapshot for bounded strategies.

        This intentionally reads all account orders for the selected market. Requested
        client IDs are also looked up in inactive orders and reconciled with account trades,
        so disappearance from the active set is never treated as final by itself.
        """
        del force_refresh  # Every call is authoritative; retained for a stable strategy API.
        await self._ensure_account_ready()
        market = self.market_info_for_trading_pair(trading_pair)
        account_params = self._account_lookup_params()
        # `active_only=false` retains zero-position rows containing per-market leverage
        # configuration, allowing flat accounts to confirm leverage authoritatively.
        account_params["active_only"] = "false"
        request_started_at = time.time()
        account_response, active_response, inactive_response, trades_response, markets_response = await safe_gather(
            self._api_get(path_url=CONSTANTS.BALANCE_PATH_URL, params=account_params),
            self._api_get(
                path_url=CONSTANTS.ACCOUNT_ACTIVE_ORDERS_PATH_URL,
                params={"account_index": self._account_index, "market_id": market.market_id},
                is_auth_required=True,
            ),
            self._api_get(
                path_url=CONSTANTS.ACCOUNT_INACTIVE_ORDERS_PATH_URL,
                params={"account_index": self._account_index, "market_id": market.market_id, "limit": 100},
                is_auth_required=True,
            ),
            self._api_get(
                path_url=CONSTANTS.TRADES_PATH_URL,
                params={
                    "account_index": self._account_index,
                    "market_id": market.market_id,
                    "sort_by": "trade_id",
                    "sort_dir": "desc",
                    "limit": 100,
                },
                is_auth_required=True,
            ),
            self._api_get(path_url=CONSTANTS.ORDER_BOOK_DETAILS_PATH_URL),
        )

        def require_response(payload: Any, field: str, accepted_types: tuple) -> Any:
            if not isinstance(payload, dict):
                raise IOError(f"Lighter snapshot response for {field} is not an object.")
            if "code" in payload:
                try:
                    code = exact_int(payload["code"], "code")
                except (TypeError, ValueError):
                    raise IOError(f"Lighter snapshot response for {field} has an invalid code.")
                if code != 200:
                    raise IOError(f"Lighter snapshot response for {field} failed with code {code}.")
            value = payload.get(field)
            if not isinstance(value, accepted_types):
                raise IOError(f"Lighter snapshot response is missing valid {field} data.")
            return value

        require_response(
            account_response,
            "accounts" if "accounts" in account_response else "sub_accounts",
            (list,),
        )
        active_orders = list(require_response(active_response, "orders", (list,)))
        inactive_orders = list(require_response(inactive_response, "orders", (list,)))
        raw_trades = require_response(trades_response, "trades", (list, dict))
        raw_markets = require_response(markets_response, "order_book_details", (list,))
        try:
            account = extract_account_snapshot(
                account_response, account_index=self._account_index, l1_address=self._l1_address
            )
        except ValueError as exc:
            raise IOError("Lighter account snapshot contains an invalid account index.") from exc

        def strict_non_negative_decimal(value: Any) -> Optional[Decimal]:
            try:
                parsed = Decimal(str(value))
            except Exception:
                return None
            if not parsed.is_finite() or parsed < 0:
                return None
            return parsed

        all_orders = active_orders + inactive_orders
        tracked_ids = {
            order.client_order_id
            for order in self._order_tracker.all_updatable_orders.values()
            if order.trading_pair == trading_pair
        }
        requested_ids = {str(client_id) for client_id in (client_order_ids or [])} | tracked_ids
        orders_by_client_id: Dict[str, Dict[str, Any]] = {}
        for order in all_orders:
            if not isinstance(order, dict):
                raise IOError("Lighter snapshot orders data contains a malformed order.")
            client_id = str(order.get("client_order_id_str", order.get("client_order_id", "")))
            if client_id == "" or (requested_ids and client_id not in requested_ids):
                continue
            status = str(order.get("status", ""))
            cumulative_fill = strict_non_negative_decimal(order.get("filled_base_amount"))
            orders_by_client_id[client_id] = {
                "client_order_id": client_id,
                "exchange_order_id": str(order.get("order_id", "")),
                "status": status,
                "terminal": status in CONSTANTS.CANCELED_ORDER_STATES
                or status in CONSTANTS.FAILED_ORDER_STATES
                or status == "filled",
                "cumulative_fill_base": cumulative_fill,
                "cumulative_fill_known": cumulative_fill is not None,
                "observed_trade_fill_base": Decimal("0"),
                "trade_ids": [],
            }

        trade_totals: Dict[str, Decimal] = {}
        trade_ids: Dict[str, set] = {}
        trade_sizes_by_id: Dict[str, Dict[str, Decimal]] = {}
        invalid_trade_clients = set()
        unattributed_trade_evidence = False

        def iter_trades(payload: Any):
            if isinstance(payload, list):
                for item in payload:
                    yield from iter_trades(item)
            elif isinstance(payload, dict):
                if "trade_id" in payload or "ask_account_id" in payload or "bid_account_id" in payload:
                    yield payload
                else:
                    for value in payload.values():
                        yield from iter_trades(value)

        for trade in iter_trades(raw_trades):
            try:
                details = own_trade_details(trade, account_index=self._account_index)
            except ValueError as exc:
                raise IOError("Lighter snapshot trades data contains an invalid account id.") from exc
            if details is None:
                continue
            _, client_id, _, _ = details
            if client_id == "":
                unattributed_trade_evidence = True
                continue
            if requested_ids and client_id not in requested_ids:
                continue
            trade_id = str(trade.get("trade_id", ""))
            trade_size = strict_non_negative_decimal(trade.get("size"))
            if trade_id == "" or trade_size is None:
                invalid_trade_clients.add(client_id)
                continue
            seen_sizes = trade_sizes_by_id.setdefault(client_id, {})
            if trade_id in seen_sizes:
                if seen_sizes[trade_id] != trade_size:
                    invalid_trade_clients.add(client_id)
                continue
            seen_sizes[trade_id] = trade_size
            trade_ids.setdefault(client_id, set()).add(trade_id)
            trade_totals[client_id] = trade_totals.get(client_id, Decimal("0")) + trade_size

        for client_id, total in trade_totals.items():
            detail = orders_by_client_id.setdefault(
                client_id,
                {
                    "client_order_id": client_id,
                    "exchange_order_id": "",
                    "status": "unknown",
                    "terminal": False,
                    "cumulative_fill_base": None,
                    "cumulative_fill_known": False,
                    "observed_trade_fill_base": Decimal("0"),
                    "trade_ids": [],
                },
            )
            detail["observed_trade_fill_base"] = total
            detail["trade_ids"] = sorted(trade_ids[client_id])

        for client_id in requested_ids:
            orders_by_client_id.setdefault(
                client_id,
                {
                    "client_order_id": client_id,
                    "exchange_order_id": "",
                    "status": "unknown",
                    "terminal": False,
                    "cumulative_fill_base": None,
                    "cumulative_fill_known": False,
                    "observed_trade_fill_base": Decimal("0"),
                    "trade_ids": [],
                },
            )

        for client_id, detail in orders_by_client_id.items():
            observed_total = trade_totals.get(client_id, Decimal("0"))
            evidence_conflicts = (
                client_id in invalid_trade_clients
                or (
                    detail["cumulative_fill_known"]
                    and observed_total > detail["cumulative_fill_base"]
                )
            )
            if evidence_conflicts:
                detail["cumulative_fill_base"] = None
                detail["cumulative_fill_known"] = False

        raw_positions = account.get("positions")
        net_position_known = isinstance(raw_positions, list)
        net_position = Decimal("0") if net_position_known else None
        leverage = None
        leverage_confirmed = False
        for raw_position in raw_positions or []:
            if not isinstance(raw_position, dict):
                net_position_known = False
                net_position = None
                break
            try:
                position_market_id = exact_int(raw_position["market_id"], "market_id")
            except (KeyError, TypeError, ValueError) as exc:
                raise IOError("Lighter account snapshot contains an invalid position market id.") from exc
            if position_market_id != market.market_id:
                continue
            size = strict_non_negative_decimal(raw_position.get("position"))
            try:
                sign = Decimal(exact_int(raw_position.get("sign"), "sign"))
            except (TypeError, ValueError):
                sign = None
            valid_sign = sign in (Decimal("-1"), Decimal("1")) or (size == 0 and sign == 0)
            if size is None or sign is None or not sign.is_finite() or not valid_sign:
                raise IOError("Lighter account snapshot contains an invalid position size or sign.")
            net_position += size * sign
            account_leverage = leverage_from_account_margin_percentage(
                raw_position.get("initial_margin_fraction")
            )
            if account_leverage is not None:
                leverage = account_leverage
                leverage_confirmed = True

        if leverage_confirmed:
            self._confirmed_leverage_by_trading_pair[trading_pair] = leverage
        else:
            self._confirmed_leverage_by_trading_pair.pop(trading_pair, None)

        aggregate_available = strict_non_negative_decimal(account.get("available_balance"))
        assets = account.get("assets")
        if not isinstance(assets, list):
            raise IOError("Lighter account snapshot is missing assets data.")
        collateral_rows = [
            asset for asset in assets
            if isinstance(asset, dict)
            and str(asset.get("symbol", "")).upper() == self._domain_settings.collateral_token
        ]
        if self._domain == CONSTANTS.ROBINHOOD_DOMAIN:
            try:
                collateral_asset_id = exact_int(collateral_rows[0].get("asset_id"), "asset_id")
            except (IndexError, ValueError):
                collateral_asset_id = None
            if (
                len(collateral_rows) != 1
                or collateral_asset_id != CONSTANTS.ROBINHOOD_COLLATERAL_ASSET_ID
            ):
                collateral_rows = []
        if len(collateral_rows) != 1:
            raise IOError(
                f"Lighter account snapshot must contain exactly one valid "
                f"{self._domain_settings.collateral_token} collateral row."
            )
        collateral_total = strict_non_negative_decimal(collateral_rows[0].get("margin_balance"))
        collateral_locked = strict_non_negative_decimal(collateral_rows[0].get("locked_balance"))
        available_margin = None
        if aggregate_available is not None and collateral_total is not None and collateral_locked is not None:
            available_margin = min(aggregate_available, max(collateral_total - collateral_locked, Decimal("0")))

        matching_markets = []
        for raw_market in raw_markets:
            if not isinstance(raw_market, dict):
                raise IOError("Lighter market metadata contains a malformed market.")
            try:
                if exact_int(raw_market.get("market_id"), "market_id") == market.market_id:
                    matching_markets.append(raw_market)
            except (TypeError, ValueError):
                raise IOError("Lighter market metadata contains an invalid market id.")
        if len(matching_markets) != 1:
            raise IOError(f"Lighter market metadata did not uniquely identify market {market.market_id}.")
        raw_market = matching_markets[0]
        market_config = raw_market.get("market_config")
        if (
            not isinstance(raw_market.get("status"), str)
            or not isinstance(market_config, dict)
            or not isinstance(market_config.get("hidden"), bool)
            or not isinstance(market_config.get("force_reduce_only"), bool)
        ):
            raise IOError("Lighter market metadata is missing required safety state.")
        market_status = raw_market["status"]
        force_reduce_only = market_config["force_reduce_only"]
        market_tradable = market_status == "active" and not market_config["hidden"] and not force_reduce_only

        user_stream_tracker = getattr(self, "_user_stream_tracker", None)
        user_stream_last_recv_time = getattr(user_stream_tracker, "last_recv_time", 0)
        if hasattr(user_stream_tracker, "_user_stream_tracking_task"):
            user_stream_task = user_stream_tracker._user_stream_tracking_task
            private_stream_connected = (
                user_stream_last_recv_time > 0
                and user_stream_task is not None
                and not user_stream_task.done()
            )
        else:
            private_stream_connected = user_stream_last_recv_time > 0
        order_book_data_source = getattr(getattr(self, "order_book_tracker", None), "data_source", None)
        public_ws = getattr(order_book_data_source, "_ws_assistant", None)
        public_last_recv_time = getattr(public_ws, "last_recv_time", 0)
        return {
            "request_started_at": request_started_at,
            "fetched_at": time.time(),
            "account_index": self._account_index,
            "net_position": net_position,
            "net_position_known": net_position_known,
            "active_orders": active_orders,
            "orders_by_client_id": orders_by_client_id,
            "pending_submissions_unknown": unattributed_trade_evidence or not net_position_known or any(
                detail["status"] == "unknown" or not detail["cumulative_fill_known"]
                for detail in orders_by_client_id.values()
            ),
            "available_margin": available_margin,
            "available_margin_known": available_margin is not None,
            "collateral_token": self._domain_settings.collateral_token,
            "position_mode": "ONEWAY",
            "leverage": leverage,
            "leverage_confirmed": leverage_confirmed,
            "market_state_known": True,
            "market_status": market_status,
            "market_tradable": market_tradable,
            "force_reduce_only": force_reduce_only,
            "private_stream_last_recv_time": user_stream_last_recv_time,
            "private_stream_connected": private_stream_connected,
            "public_data_last_recv_time": public_last_recv_time,
        }

    @staticmethod
    def _match_order(tracked_order: InFlightOrder, orders: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        for order in orders:
            if str(order.get("client_order_id", "")) == tracked_order.client_order_id:
                return order
            if (
                tracked_order.exchange_order_id is not None
                and str(order.get("order_id", "")) == tracked_order.exchange_order_id
            ):
                return order
        return None

    def _process_order_events(self, order_payload: Any):
        def iter_orders(payload: Any):
            if isinstance(payload, list):
                for item in payload:
                    yield from iter_orders(item)
            elif isinstance(payload, dict):
                if "client_order_id" in payload or "order_id" in payload:
                    yield payload
                    return
                for value in payload.values():
                    yield from iter_orders(value)

        for order in iter_orders(order_payload):
            client_order_id = str(order.get("client_order_id", ""))
            if client_order_id == "":
                continue
            tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
            if tracked_order is None:
                continue
            new_state = order_state_from_order_data(order)
            order_update = OrderUpdate(
                trading_pair=tracked_order.trading_pair,
                update_timestamp=normalize_timestamp_to_seconds(
                    order.get("updated_at", order.get("transaction_time"))
                ),
                new_state=new_state,
                client_order_id=client_order_id,
                exchange_order_id=str(order.get("order_id")),
                misc_updates=self._order_misc_updates(order_data=order, state=new_state),
            )
            self._order_tracker.process_order_update(order_update)

    def _process_trade_events(self, trade_payload: Any):
        if isinstance(trade_payload, dict):
            groups = trade_payload.values()
        else:
            groups = [trade_payload]
        for trades in groups:
            if isinstance(trades, dict):
                trades = [trades]
            if not isinstance(trades, list):
                continue
            for trade in trades:
                trade_update = self._trade_update_from_trade(trade)
                if trade_update is not None:
                    self._order_tracker.process_trade_update(trade_update)

    def _process_position_events(self, position_payload: Any):
        # WS sends delta updates — only add/update positions present in the event.
        # Removing a position requires either size=0 in the event, or REST reconciliation
        # via _update_positions (which sees all positions and prunes closed ones).
        if isinstance(position_payload, dict):
            raw_positions = position_payload.values()
        elif isinstance(position_payload, list):
            raw_positions = position_payload
        else:
            return

        for raw_position in raw_positions:
            market_id = raw_position.get("market_id")
            size = self._safe_decimal(raw_position.get("position", "0"))
            position = self._parse_position(raw_position)
            if position is None:
                if market_id is not None and size == Decimal("0"):
                    market = self._markets_by_id.get(int(market_id))
                    if market is not None:
                        for side in (PositionSide.LONG, PositionSide.SHORT):
                            pos_key = self._perpetual_trading.position_key(market.trading_pair, side)
                            if pos_key in self._perpetual_trading.account_positions:
                                self._perpetual_trading.remove_position(pos_key)
                continue
            pos_key = self._perpetual_trading.position_key(position.trading_pair, position.position_side)
            self._perpetual_trading.set_position(pos_key, position)

    def _trade_update_from_trade(self, trade: Dict[str, Any]) -> Optional[TradeUpdate]:
        details = own_trade_details(trade, account_index=self._account_index)
        if details is None:
            return None

        trade_type, client_order_id, exchange_order_id, is_maker = details
        tracked_order = self._order_tracker.all_fillable_orders.get(client_order_id)
        if tracked_order is None and exchange_order_id:
            tracked_order = self._order_tracker.all_fillable_orders_by_exchange_order_id.get(exchange_order_id)
        if tracked_order is None:
            return None

        market = self.market_info_for_market_id(int(trade["market_id"]))
        position_action = (
            tracked_order.position
            if tracked_order.position in [PositionAction.OPEN, PositionAction.CLOSE]
            else PositionAction.OPEN
        )
        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent=market.maker_fee if is_maker else market.taker_fee,
        )
        price = self._safe_decimal(trade["price"])
        size = self._safe_decimal(trade["size"])
        return TradeUpdate(
            trade_id=str(trade["trade_id"]),
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            fill_timestamp=normalize_timestamp_to_seconds(trade.get("transaction_time")),
            fill_price=price,
            fill_base_amount=size,
            fill_quote_amount=price * size,
            fee=fee,
            is_taker=not is_maker,
        )

    def _parse_position(self, raw_position: Dict[str, Any]) -> Optional[Position]:
        market_id = raw_position.get("market_id")
        symbol = str(raw_position.get("symbol", "")).upper()

        market = None
        if market_id is not None:
            market = self._markets_by_id.get(int(market_id))
        if market is None and symbol:
            market = self._markets_by_exchange_symbol.get(symbol)
        if market is None:
            return None

        sign = self._safe_decimal(raw_position.get("sign", "1"))
        size = self._safe_decimal(raw_position.get("position", "0"))
        amount = size * sign if sign in (Decimal("-1"), Decimal("1")) and size >= 0 else size

        if amount == Decimal("0"):
            return None

        position_side = PositionSide.LONG if amount > 0 else PositionSide.SHORT
        unrealized_pnl = self._safe_decimal(raw_position.get("unrealized_pnl", "0"))
        entry_price = self._safe_decimal(raw_position.get("avg_entry_price", "0"))

        leverage = Decimal("1")
        initial_margin_fraction = raw_position.get("initial_margin_fraction")
        if initial_margin_fraction not in (None, "", "0", 0):
            leverage = leverage_from_account_margin_percentage(initial_margin_fraction) or Decimal("1")

        return Position(
            trading_pair=market.trading_pair,
            position_side=position_side,
            unrealized_pnl=unrealized_pnl,
            entry_price=entry_price,
            amount=amount,
            leverage=leverage,
        )

    @staticmethod
    def _safe_decimal(value: Any) -> Decimal:
        if value is None or value == "":
            return Decimal("0")
        try:
            result = Decimal(str(value))
        except Exception:
            return Decimal("0")
        if result.is_nan() or result.is_infinite():
            return Decimal("0")
        return result

    @staticmethod
    def _extract_tx_code(tx_response: Any) -> Optional[int]:
        if tx_response is None:
            return None
        if isinstance(tx_response, dict):
            code = tx_response.get("code")
            if code is not None:
                try:
                    return exact_int(code, "code")
                except (TypeError, ValueError):
                    return None
        if hasattr(tx_response, "code"):
            try:
                return exact_int(getattr(tx_response, "code"), "code")
            except (TypeError, ValueError):
                return None
        return None

    def _is_tx_response_success(self, tx_response: Any) -> bool:
        code = self._extract_tx_code(tx_response)
        return code == 200

    @staticmethod
    def _signer_error_category(error: Any) -> str:
        """Return an allowlisted diagnostic category without exposing SDK error text."""
        normalized = str(error).casefold() if error is not None else ""
        if "invalid nonce" in normalized:
            return "invalid_nonce"
        if "order not found" in normalized:
            return "order_not_found"
        if "too many requests" in normalized or "rate limit" in normalized or "http 429" in normalized:
            return "rate_limited"
        return "unclassified"

    @staticmethod
    def _transport_exception_category(exc: BaseException) -> str:
        """Classify an exception without publishing its message or arbitrary class name."""
        if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
            return "timeout"
        if isinstance(exc, ConnectionError):
            return "connection"
        return "unclassified"

    # ── Neutral grid: paginated authoritative history (NG-HIST-001/002) ─────────────────────────
    # Additive API. The legacy one-page readers (`_find_order`, `get_grid_account_snapshot`,
    # `_update_trade_history`) are intentionally left unchanged.

    def add_history_wakeup_listener(self, listener: Callable[[], None]) -> None:
        """Call ``listener()`` whenever the private stream reports own order/trade/position activity.

        This is only a hint to poll the authoritative REST history sooner (NG-HIST-001); it carries
        no payload and must never be treated as evidence of fills or terminal state.
        """
        if listener not in self._history_wakeup_listeners:
            self._history_wakeup_listeners.append(listener)

    def remove_history_wakeup_listener(self, listener: Callable[[], None]) -> None:
        if listener in self._history_wakeup_listeners:
            self._history_wakeup_listeners.remove(listener)

    def _signal_history_wakeup(self) -> None:
        for listener in list(self._history_wakeup_listeners):
            try:
                listener()
            except Exception:
                self.logger().error("History wakeup listener failed.", exc_info=True)

    def _require_history_account_index(self) -> int:
        if self._account_index is None:
            raise IOError("Lighter account index is not resolved; history cannot be scoped.")
        return self._account_index

    async def fetch_inactive_orders_page(
        self,
        trading_pair: str,
        cursor: Optional[str] = None,
        limit: int = CONSTANTS.HISTORY_PAGE_LIMIT_MAX,
    ) -> LighterHistoryPage:
        """One page of ``GET /api/v1/accountInactiveOrders``.

        Parameter names are those of lighter-sdk 1.1.4 ``OrderApi.account_inactive_orders``:
        header ``authorization`` (added by ``LighterAuth``), required ``account_index`` and
        ``limit`` (1..100), optional ``market_id`` and ``cursor``. The cursor is sent verbatim and
        the response ``next_cursor`` is returned verbatim. Throttled as weight 100.
        """
        validate_history_page_request(cursor, limit)
        await self._ensure_account_ready()
        market = self.market_info_for_trading_pair(trading_pair)
        params: Dict[str, Any] = {
            "account_index": self._require_history_account_index(),
            "market_id": market.market_id,
            "limit": limit,
        }
        if cursor is not None:
            params["cursor"] = cursor
        response = await self._api_get(
            path_url=CONSTANTS.ACCOUNT_INACTIVE_ORDERS_PATH_URL,
            params=params,
            is_auth_required=True,
            limit_id=CONSTANTS.ACCOUNT_INACTIVE_ORDERS_PATH_URL,
        )
        return history_page_from_response(response, "orders", cursor_sent=cursor, limit=limit)

    async def fetch_trades_page(
        self,
        trading_pair: str,
        cursor: Optional[str] = None,
        limit: int = CONSTANTS.HISTORY_PAGE_LIMIT_MAX,
        order_index: Optional[int] = None,
    ) -> LighterHistoryPage:
        """One page of ``GET /api/v1/trades``, newest first.

        Parameter names are those of lighter-sdk 1.1.4 ``OrderApi.trades``: required ``sort_by``
        (``trade_id``) and ``limit`` (1..100); ``authorization`` header, ``account_index``,
        ``market_id``, ``sort_dir`` (``desc``), optional exact ``order_index`` filter and the opaque
        ``cursor`` sent verbatim. The response ``next_cursor`` is returned verbatim. Throttled as
        weight 600.
        """
        validate_history_page_request(cursor, limit)
        if order_index is not None and (type(order_index) is not int or order_index < 0):
            raise ValueError("order_index filter must be a non-negative int.")
        await self._ensure_account_ready()
        market = self.market_info_for_trading_pair(trading_pair)
        params: Dict[str, Any] = {
            "account_index": self._require_history_account_index(),
            "market_id": market.market_id,
            "sort_by": CONSTANTS.TRADES_SORT_BY_TRADE_ID,
            "sort_dir": CONSTANTS.TRADES_SORT_DIR_DESC,
            "limit": limit,
        }
        if cursor is not None:
            params["cursor"] = cursor
        if order_index is not None:
            params["order_index"] = order_index
        response = await self._api_get(
            path_url=CONSTANTS.TRADES_PATH_URL,
            params=params,
            is_auth_required=True,
            limit_id=CONSTANTS.TRADES_PATH_URL,
        )
        return history_page_from_response(response, "trades", cursor_sent=cursor, limit=limit)

    async def fetch_active_orders(self, trading_pair: str) -> LighterHistoryPage:
        """All active orders of the account on one market (``GET /api/v1/accountActiveOrders``).

        SDK 1.1.4 ``account_active_orders`` has no cursor parameter; a non-empty ``next_cursor`` in
        the response therefore means a truncated list, which callers must treat as unknown.
        """
        await self._ensure_account_ready()
        market = self.market_info_for_trading_pair(trading_pair)
        response = await self._api_get(
            path_url=CONSTANTS.ACCOUNT_ACTIVE_ORDERS_PATH_URL,
            params={"account_index": self._require_history_account_index(), "market_id": market.market_id},
            is_auth_required=True,
            limit_id=CONSTANTS.ACCOUNT_ACTIVE_ORDERS_PATH_URL,
        )
        return history_page_from_response(response, "orders", cursor_sent=None, limit=None)

    async def fetch_account_position(self, trading_pair: str) -> Dict[str, Any]:
        """Authoritative signed net position for one market from a single account read (weight 300).

        Quantities are parsed from exact string/int sources only; malformed data raises.
        ``available_collateral`` is advisory and ``None`` when it cannot be established exactly.
        """
        await self._ensure_account_ready()
        market = self.market_info_for_trading_pair(trading_pair)
        account_params = self._account_lookup_params()
        account_params["active_only"] = "false"
        response = await self._api_get(path_url=CONSTANTS.BALANCE_PATH_URL, params=account_params)
        if not isinstance(response, dict):
            raise IOError("Lighter account response is not an object.")
        try:
            account = extract_account_snapshot(
                response, account_index=self._account_index, l1_address=self._l1_address
            )
        except ValueError as exc:
            raise IOError("Lighter account snapshot contains an invalid account index.") from exc
        net_position, leverage, margin_mode = strict_market_position(account, market.market_id)
        available_collateral = None
        assets = account.get("assets")
        aggregate_available = strict_decimal(account.get("available_balance"))
        if isinstance(assets, list):
            rows = [
                asset for asset in assets
                if isinstance(asset, dict)
                and str(asset.get("symbol", "")).upper() == self._domain_settings.collateral_token
            ]
            if self._domain == CONSTANTS.ROBINHOOD_DOMAIN:
                try:
                    valid_asset = exact_int(rows[0].get("asset_id"), "asset_id") == CONSTANTS.ROBINHOOD_COLLATERAL_ASSET_ID
                except (IndexError, ValueError):
                    valid_asset = False
                if not valid_asset:
                    rows = []
            if len(rows) == 1 and aggregate_available is not None and aggregate_available >= 0:
                total = strict_decimal(rows[0].get("margin_balance"))
                locked = strict_decimal(rows[0].get("locked_balance"))
                if total is not None and locked is not None and total >= 0 and locked >= 0:
                    available_collateral = min(aggregate_available, max(total - locked, Decimal("0")))
        return {
            "account_index": self._account_index,
            "market_id": market.market_id,
            "net_position": net_position,
            "leverage": leverage,
            "margin_mode": margin_mode,
            "available_collateral": available_collateral,
            "fetched_at": time.time(),
        }

    # ── Neutral grid: pre-persisted client-id submission (NG-HIST-004, NG-DB-005) ───────────────
    # Outcome classification:
    # * NOT_SENT  - returned only from pre-send validation, i.e. before the signer is invoked, so
    #               the transport provably was not called for this request;
    # * ACCEPTED  - signer returned no error and a RespSendTx with code 200 (tx accepted for
    #               processing; NOT proof of order acceptance, fills or terminal state);
    # * UNKNOWN   - everything else: exceptions, timeouts, error strings, non-200 codes.
    # * DEFINITIVE_REJECT_ZERO_FILL is never emitted: lighter-sdk 1.1.4 documents no definitive
    #   venue rejection codes. RespSendTx only carries code/message/tx_hash (models/resp_send_tx.py),
    #   and `process_api_key_and_nonce` (signer_client.py:211-243) collapses local signing errors
    #   and HTTP 400 BadRequestException bodies into the same `(None, None, error_str)` tuple, so a
    #   returned error does not even prove whether the tx reached the venue. Zero-fill terminal
    #   proof must come from authoritative history (exact inactive order row + full scan).
    # Nonce: the SDK allocates the tx nonce inside the async signer call under its own per-key lock
    # (`nonce_manager.async_next_nonce`, signer_client.py:211-243), so no pre-send nonce exists. The
    # order row `nonce` is a secondary corroborating field only; the primary identity is the
    # pre-persisted client order id within the exact account/market.

    @staticmethod
    def _client_order_id_problem(client_order_id: Any) -> Optional[str]:
        if type(client_order_id) is not int:
            return "client_order_id must be an int"
        if not 1 <= client_order_id < (1 << CONSTANTS.MAX_CLIENT_ORDER_ID_BIT_COUNT):
            return f"client_order_id outside [1, 2**{CONSTANTS.MAX_CLIENT_ORDER_ID_BIT_COUNT})"
        return None

    def _order_update_timestamp(self) -> float:
        timestamp = self.current_timestamp
        if timestamp is None or timestamp != timestamp:  # NaN before the clock starts
            timestamp = self._time()
        return timestamp

    async def submit_with_client_id(
        self,
        client_order_id: int,
        trading_pair: str,
        trade_type: TradeType,
        price: Decimal,
        amount: Decimal,
        order_type: OrderType,
        order_expiry_ms: Optional[int] = None,
        *,
        pre_send_timeout_s: float = 5.0,
    ) -> LighterTransportResult:
        """Submit a LIMIT (GoodTillTime) or LIMIT_MAKER (post-only) order with a pre-persisted CID.

        The numeric client order id is used unchanged as ``client_order_index`` (never replaced by
        ``_new_client_order_id``); ``reduce_only`` is always explicitly ``False`` (virtual neutral
        cells, NG-ORD-002). The order is registered with the Hummingbot order tracker before the
        signer call and the signer runs under the connector's existing tx lock. The pre-send gate
        timeout is intentionally shorter than the neutral-grid engine's default 10-second outer
        timeout: expiring while waiting for the throttler/tx lock proves the signer was not invoked.
        """
        not_sent = LighterTransportOutcome.NOT_SENT
        problem = self._client_order_id_problem(client_order_id)
        if problem is not None:
            return LighterTransportResult(not_sent, f"pre_send_validation: {problem}")
        if trade_type not in (TradeType.BUY, TradeType.SELL):
            return LighterTransportResult(not_sent, "pre_send_validation: trade_type must be BUY or SELL")
        if order_type not in (OrderType.LIMIT, OrderType.LIMIT_MAKER):
            return LighterTransportResult(not_sent, "pre_send_validation: only LIMIT and LIMIT_MAKER are allowed")
        if order_expiry_ms is not None and (type(order_expiry_ms) is not int or order_expiry_ms <= 0):
            return LighterTransportResult(not_sent, "pre_send_validation: order_expiry_ms must be a positive int")
        if (type(pre_send_timeout_s) not in (int, float) or not math.isfinite(pre_send_timeout_s)
                or pre_send_timeout_s <= 0):
            return LighterTransportResult(not_sent, "pre_send_validation: pre_send_timeout_s must be finite and positive")
        try:
            await self._ensure_account_ready()
            market = self.market_info_for_trading_pair(trading_pair)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return LighterTransportResult(not_sent, f"pre_send_validation: market unavailable ({type(exc).__name__})")
        base_amount = exact_scaled_int(amount, market.size_decimals)
        price_int = exact_scaled_int(price, market.price_decimals)
        if base_amount is None or price_int is None:
            return LighterTransportResult(
                not_sent, "pre_send_validation: price/amount must be positive and exact at venue precision"
            )
        if self._signer_client is None:
            return LighterTransportResult(not_sent, "pre_send_validation: signer client unavailable")
        order_id = str(client_order_id)
        if (
            order_id in self._history_reconciled_client_order_ids
            or order_id in self._pending_cid_submissions
            or self._order_tracker.fetch_order(client_order_id=order_id) is not None
        ):
            # A previous submission with this CID governs; re-sending it is not proven idempotent.
            return LighterTransportResult(LighterTransportOutcome.UNKNOWN, "duplicate_client_order_id_in_flight")

        tif = (
            self._signer_client.ORDER_TIME_IN_FORCE_POST_ONLY
            if order_type is OrderType.LIMIT_MAKER
            else self._signer_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
        )
        order_kwargs: Dict[str, Any] = dict(
            market_index=market.market_id,
            client_order_index=client_order_id,
            base_amount=base_amount,
            price=price_int,
            is_ask=trade_type is TradeType.SELL,
            order_type=self._signer_client.ORDER_TYPE_LIMIT,
            time_in_force=tif,
            reduce_only=False,
        )
        if order_expiry_ms is not None:
            order_kwargs["order_expiry"] = order_expiry_ms
        # No await separates the duplicate check from this claim, so only one coroutine can own a
        # CID while it waits for the throttler and tx lock.
        self._pending_cid_submissions.add(order_id)
        gates = AsyncExitStack()
        try:
            try:
                async with asyncio.timeout(pre_send_timeout_s):
                    await gates.enter_async_context(
                        self._throttler.execute_task(limit_id=CONSTANTS.SEND_TX_LIMIT)
                    )
                    await gates.enter_async_context(self._tx_lock)
            except TimeoutError:
                if (
                    order_id in self._history_reconciled_client_order_ids
                    or self._order_tracker.fetch_order(client_order_id=order_id) is not None
                ):
                    return LighterTransportResult(
                        LighterTransportOutcome.UNKNOWN, "duplicate_client_order_id_in_flight"
                    )
                return LighterTransportResult(
                    not_sent, "pre_send_validation: gate timeout before signer invocation"
                )
            # Durable registration may change while this owner awaits the shared gates. Recheck
            # under the tx lock before registering or invoking the signer.
            if (
                order_id in self._history_reconciled_client_order_ids
                or self._order_tracker.fetch_order(client_order_id=order_id) is not None
            ):
                return LighterTransportResult(
                    LighterTransportOutcome.UNKNOWN, "duplicate_client_order_id_in_flight"
                )
            self.start_tracking_order(
                order_id=order_id,
                exchange_order_id=None,
                trading_pair=trading_pair,
                trade_type=trade_type,
                price=price,
                amount=amount,
                order_type=order_type,
                position_action=PositionAction.NIL,
            )
            self._history_reconciled_client_order_ids.add(order_id)
            _, tx_response, error = await self._signer_client.create_order(**order_kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return LighterTransportResult(
                LighterTransportOutcome.UNKNOWN,
                f"transport_exception:{self._transport_exception_category(exc)}",
            )
        finally:
            try:
                await gates.aclose()
            finally:
                self._pending_cid_submissions.discard(order_id)
        if error is not None or not self._is_tx_response_success(tx_response):
            code = self._extract_tx_code(tx_response)
            return LighterTransportResult(
                LighterTransportOutcome.UNKNOWN,
                (f"transport_error: code={code}" if error is None else
                 f"transport_error: signer_error:{self._signer_error_category(error)}"),
            )
        self._order_tracker.process_order_update(OrderUpdate(
            client_order_id=order_id,
            trading_pair=trading_pair,
            update_timestamp=self._order_update_timestamp(),
            new_state=OrderState.OPEN,
        ))
        tx_hash = tx_response.get("tx_hash") if isinstance(tx_response, dict) else getattr(tx_response, "tx_hash", None)
        return LighterTransportResult(
            LighterTransportOutcome.ACCEPTED,
            "send_tx_accepted",
            tx_hash=tx_hash if isinstance(tx_hash, str) else None,
        )

    def register_history_reconciled_order(self, client_order_id: int) -> None:
        """Mark a durable engine client id as history-reconciled (call on startup, before polling).

        The engine should register every client id whose outbox intent may have reached transport.
        The id is then never polled per order, never cancelled by generic/lost-order paths and never
        re-sent by ``submit_with_client_id`` in this process. Orders restored from Hummingbot
        tracking state are re-registered automatically via a marker in the saved state.
        """
        problem = self._client_order_id_problem(client_order_id)
        if problem is not None:
            raise ValueError(problem)
        self._history_reconciled_client_order_ids.add(str(client_order_id))

    def release_history_reconciled_order(self, client_order_id: int) -> None:
        """Stop tracking a CID-submitted order once the engine has proven it terminal from history.

        Emits no events and sends nothing. The client id stays history-reconciled for the process
        lifetime: it is still refused by ``submit_with_client_id`` after the tracker cache expires and
        still excluded from legacy polling. Across restarts the engine's durable CID ledger is the
        guard (see ``register_history_reconciled_order``).
        """
        self._order_tracker.stop_tracking_order(str(client_order_id))

    async def cancel_with_client_id(
        self,
        trading_pair: str,
        client_order_id: int,
        exchange_order_index: Optional[str] = None,
    ) -> LighterTransportResult:
        """Cancel an own order by exact exchange order index (or look it up by exact CID).

        ACCEPTED only means the cancel tx was accepted for processing; it is never terminal proof.
        NOT_SENT means no cancel tx was sent (validation/lookup failed) and says nothing about the
        order itself.
        """
        not_sent = LighterTransportOutcome.NOT_SENT
        problem = self._client_order_id_problem(client_order_id)
        if problem is not None:
            return LighterTransportResult(not_sent, f"pre_send_validation: {problem}")
        try:
            await self._ensure_account_ready()
            market = self.market_info_for_trading_pair(trading_pair)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return LighterTransportResult(not_sent, f"pre_send_validation: market unavailable ({type(exc).__name__})")
        if exchange_order_index is None:
            try:
                active = await self.fetch_active_orders(trading_pair)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                return LighterTransportResult(not_sent, f"order_lookup_failed: {type(exc).__name__}")
            matches = []
            for row in active.rows:
                client_index = row.get("client_order_index", row.get("client_order_id"))
                try:
                    if exact_int(client_index, "client_order_index") == client_order_id:
                        matches.append(row)
                except (TypeError, ValueError):
                    continue
            if len(matches) != 1:
                return LighterTransportResult(not_sent, f"order_lookup: {len(matches)} active matches for client id")
            exchange_order_index = matches[0].get("order_index", matches[0].get("order_id"))
        try:
            order_index = exact_int(exchange_order_index, "order_index")
        except (TypeError, ValueError):
            return LighterTransportResult(not_sent, "pre_send_validation: exchange order index is not an exact int")
        if order_index < 0:
            return LighterTransportResult(not_sent, "pre_send_validation: exchange order index is negative")
        if self._signer_client is None:
            return LighterTransportResult(not_sent, "pre_send_validation: signer client unavailable")
        try:
            async with self._throttler.execute_task(limit_id=CONSTANTS.SEND_TX_LIMIT):
                async with self._tx_lock:
                    _, tx_response, error = await self._signer_client.cancel_order(
                        market_index=market.market_id,
                        order_index=order_index,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return LighterTransportResult(LighterTransportOutcome.UNKNOWN, f"transport_exception: {type(exc).__name__}")
        if error is not None or not self._is_tx_response_success(tx_response):
            code = self._extract_tx_code(tx_response)
            return LighterTransportResult(
                LighterTransportOutcome.UNKNOWN,
                f"transport_error: code={code}" if error is None else "transport_error: signer returned an error",
                exchange_order_id=str(order_index),
            )
        tracked = self._order_tracker.fetch_order(client_order_id=str(client_order_id))
        if tracked is not None and not tracked.is_done:
            self._order_tracker.process_order_update(OrderUpdate(
                client_order_id=tracked.client_order_id,
                trading_pair=tracked.trading_pair,
                update_timestamp=self._order_update_timestamp(),
                new_state=OrderState.PENDING_CANCEL,
            ))
        return LighterTransportResult(
            LighterTransportOutcome.ACCEPTED, "cancel_tx_accepted_not_terminal", exchange_order_id=str(order_index)
        )
