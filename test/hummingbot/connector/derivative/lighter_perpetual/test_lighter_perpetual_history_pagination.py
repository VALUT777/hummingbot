"""Connector tests: paginated authoritative history and pre-persisted CID submission (neutral grid).

No network: HTTP is mocked with aioresponses, the lighter-sdk signer is a mock carrying the real
SDK 1.1.4 constants. Covers AC-10/11/12/22 (connector part), AC-56 (venue mapping), NG-HIST-001..004.
"""
import asyncio
import json
import re
import unittest
from decimal import Decimal
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import CallbackResult, aioresponses
from lighter.signer_client import SignerClient as SdkSignerClient

from hummingbot.connector.derivative.lighter_perpetual import lighter_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_api_utils import (
    LighterHistoryResponseError,
    LighterMarketInfo,
    LighterTransportOutcome,
)
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_derivative import LighterPerpetualDerivative
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.strategy_v2.executors.neutral_grid_executor.history import HistoryScanner, InMemoryHistoryCursorView
from hummingbot.strategy_v2.executors.neutral_grid_executor.lighter_port import LighterExchangePort

ACCOUNT = 724450
MARKET = 5
PAIR = "LIT-USDG"
AUTH_TOKEN = "test-auth-token-not-a-secret"
PRIVATE_KEY = "unused-test-private-key"
BASE_URL = CONSTANTS.ROBINHOOD_BASE_URL
INACTIVE_RE = re.compile(r"^" + re.escape(BASE_URL + CONSTANTS.ACCOUNT_INACTIVE_ORDERS_PATH_URL))
TRADES_RE = re.compile(r"^" + re.escape(BASE_URL + CONSTANTS.TRADES_PATH_URL))
ACTIVE_RE = re.compile(r"^" + re.escape(BASE_URL + CONSTANTS.ACCOUNT_ACTIVE_ORDERS_PATH_URL))
ACCOUNT_RE = re.compile(r"^" + re.escape(BASE_URL + CONSTANTS.ACCOUNT_PATH_URL) + r"(\?|$)")
BIG = 1 << 53
MAX_CID = (1 << 48) - 1
BASE_TS = 1_790_000_000_000


def raw_order(i: int, ts_ms: int, status: str = "canceled", filled: str = "0", cid: Optional[int] = None) -> Dict:
    cid = 1000 + i if cid is None else cid
    return {
        "order_index": BIG + i, "client_order_index": cid, "order_id": str(BIG + i), "client_order_id": str(cid),
        "market_index": MARKET, "owner_account_index": ACCOUNT, "initial_base_amount": "10.00", "price": "5.4000",
        "nonce": 10_000 + i, "remaining_base_amount": "10.00", "is_ask": False, "filled_base_amount": filled,
        "filled_quote_amount": "0", "type": "limit", "time_in_force": "post-only", "reduce_only": False,
        "status": status, "timestamp": ts_ms, "created_at": ts_ms, "updated_at": ts_ms,
    }


def raw_trade(i: int, ts_ms: int, size: str = "1.00", order_index: Optional[int] = None) -> Dict:
    order_index = BIG + 900_000 + i if order_index is None else order_index
    trade_id = (1 << 60) - i * 10  # newest first: ids decrease with age
    return {
        "trade_id": trade_id, "trade_id_str": str(trade_id), "type": "trade", "market_id": MARKET,
        "size": size, "price": "5.4000", "ask_account_id": 1, "bid_account_id": ACCOUNT,
        "ask_id": 77, "bid_id": order_index, "ask_id_str": "77", "bid_id_str": str(order_index),
        "ask_client_id": 0, "bid_client_id": 5000 + i, "ask_client_id_str": "0", "bid_client_id_str": str(5000 + i),
        "is_maker_ask": False, "timestamp": ts_ms,
    }


class PagedEndpoint:
    """aioresponses callback serving newest->oldest pages keyed by an opaque cursor."""

    def __init__(self, rows_field: str, rows: List[Dict], page_size: int = 100):
        self.rows_field = rows_field
        self.pages = [rows[i:i + page_size] for i in range(0, max(len(rows), 1), page_size)] or [[]]
        self.cursors = [None] + [f"cur+{n}/==" for n in range(1, len(self.pages))]
        self.requests: List[Dict[str, Any]] = []
        self.overrides: Dict[int, Dict[str, Any]] = {}

    def __call__(self, url, **kwargs):
        params = dict(kwargs.get("params") or {})
        self.requests.append({"params": params, "headers": dict(kwargs.get("headers") or {})})
        index = self.cursors.index(params.get("cursor"))
        body: Dict[str, Any] = {"code": 200, self.rows_field: self.pages[index]}
        if index + 1 < len(self.pages):
            body["next_cursor"] = self.cursors[index + 1]
        body.update(self.overrides.get(index, {}))
        return CallbackResult(status=200, body=json.dumps(body), content_type="application/json")


def market_info() -> LighterMarketInfo:
    return LighterMarketInfo(
        market_id=MARKET, exchange_symbol="LIT", trading_pair=PAIR, base_asset="LIT", quote_asset="USDG",
        market_type="perp", min_base_amount=Decimal("5"), min_quote_amount=Decimal("10"), size_decimals=2,
        price_decimals=4, maker_fee=Decimal("0"), taker_fee=Decimal("0"),
        raw_info={"min_initial_margin_fraction": "2000", "last_trade_price": "5.4"},
    )


class LighterHistoryPaginationTest(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.signer = MagicMock()
        self.signer.create_auth_token_with_expiry.return_value = (AUTH_TOKEN, None)
        self.signer.ORDER_TYPE_LIMIT = SdkSignerClient.ORDER_TYPE_LIMIT
        self.signer.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = SdkSignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
        self.signer.ORDER_TIME_IN_FORCE_POST_ONLY = SdkSignerClient.ORDER_TIME_IN_FORCE_POST_ONLY
        self.lock_states: List[bool] = []

        async def create_order(**kwargs):
            self.lock_states.append(self.connector._tx_lock.locked())
            return None, {"code": 200, "tx_hash": "0xfeed"}, None

        async def cancel_order(**kwargs):
            self.lock_states.append(self.connector._tx_lock.locked())
            return None, {"code": 200, "tx_hash": "0xbeef"}, None

        self.signer.create_order = AsyncMock(side_effect=create_order)
        self.signer.cancel_order = AsyncMock(side_effect=cancel_order)
        with patch(
            "hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_derivative.SignerClient",
            return_value=self.signer,
        ):
            self.connector = LighterPerpetualDerivative(
                lighter_perpetual_account_index=ACCOUNT,
                lighter_perpetual_api_key_index=1,
                lighter_perpetual_api_private_key=PRIVATE_KEY,
                trading_pairs=[PAIR],
                trading_required=True,
                domain=CONSTANTS.ROBINHOOD_DOMAIN,
            )
        market = market_info()
        self.connector._markets_by_trading_pair = {PAIR: market}
        self.connector._markets_by_id = {MARKET: market}
        self.connector._markets_by_exchange_symbol = {"LIT": market}
        # prove the pre-persisted CID path never allocates a fresh client id
        self.connector._new_client_order_id = MagicMock(side_effect=AssertionError("new CID allocated"))

    def pool_weights(self) -> List[int]:
        return [log.weight for log in self.connector._throttler._task_logs
                if log.rate_limit.limit_id == CONSTANTS.ALL_ENDPOINTS_LIMIT]

    # ------------------------------------------------------------------------ history pages
    async def test_inactive_orders_page_exact_sdk_params_cursor_verbatim_weight_100(self):
        endpoint = PagedEndpoint("orders", [raw_order(i, BASE_TS - i) for i in range(150)])
        with aioresponses() as mocked:
            mocked.get(INACTIVE_RE, callback=endpoint, repeat=True)
            first = await self.connector.fetch_inactive_orders_page(PAIR)
            second = await self.connector.fetch_inactive_orders_page(PAIR, cursor=first.next_cursor)

        self.assertEqual(100, len(first.rows))
        self.assertEqual("cur+1/==", first.next_cursor)
        self.assertIsNone(first.cursor_sent)
        self.assertEqual(50, len(second.rows))
        self.assertIsNone(second.next_cursor)
        self.assertEqual("cur+1/==", second.cursor_sent)
        first_params, second_params = endpoint.requests[0]["params"], endpoint.requests[1]["params"]
        self.assertEqual({"account_index": ACCOUNT, "market_id": MARKET, "limit": 100},
                         {k: first_params[k] for k in ("account_index", "market_id", "limit")})
        self.assertNotIn("cursor", first_params)
        self.assertEqual("cur+1/==", second_params["cursor"])  # opaque cursor verbatim
        self.assertEqual(AUTH_TOKEN, endpoint.requests[0]["headers"]["authorization"])
        self.assertEqual(BIG + 1, first.rows[1]["order_index"])  # exact int, never float
        self.assertEqual([CONSTANTS.WEIGHT_INACTIVE_ORDERS] * 2, self.pool_weights())

    async def test_trades_page_exact_sdk_params_weight_600_and_order_index_filter(self):
        endpoint = PagedEndpoint("trades", [raw_trade(i, BASE_TS - i) for i in range(3)])
        endpoint.cursors = ["opaque=="]  # serve the single page for a caller-supplied opaque cursor
        with aioresponses() as mocked:
            mocked.get(TRADES_RE, callback=endpoint, repeat=True)
            page = await self.connector.fetch_trades_page(PAIR, cursor="opaque==", limit=50, order_index=BIG + 7)
        params = endpoint.requests[0]["params"]
        self.assertEqual(
            {"account_index": ACCOUNT, "market_id": MARKET, "sort_by": "trade_id", "sort_dir": "desc",
             "limit": 50, "cursor": "opaque==", "order_index": BIG + 7},
            {k: params[k] for k in ("account_index", "market_id", "sort_by", "sort_dir", "limit", "cursor",
                                    "order_index")})
        self.assertEqual(AUTH_TOKEN, endpoint.requests[0]["headers"]["authorization"])
        self.assertEqual(3, len(page.rows))
        self.assertEqual([CONSTANTS.WEIGHT_TRADES], self.pool_weights())

    async def test_page_request_validation_happens_before_any_request(self):
        with aioresponses() as mocked:
            for kwargs in ({"limit": 101}, {"limit": 0}, {"limit": True}, {"limit": "100"}, {"cursor": 5}):
                with self.subTest(kwargs=kwargs):
                    with self.assertRaises(ValueError):
                        await self.connector.fetch_inactive_orders_page(PAIR, **kwargs)
                    with self.assertRaises(ValueError):
                        await self.connector.fetch_trades_page(PAIR, **kwargs)
            with self.assertRaises(ValueError):
                await self.connector.fetch_trades_page(PAIR, order_index=-1)
            self.assertEqual({}, dict(mocked.requests))
        self.assertEqual([], self.pool_weights())

    async def test_response_shape_errors_fail_closed_and_next_cursor_is_verbatim(self):
        cases = {
            "error_code": {"code": 29500, "message": "internal", "orders": []},
            "missing_rows": {"code": 200},
            "rows_not_list": {"code": 200, "orders": {"1": []}},
            "row_not_object": {"code": 200, "orders": [1]},
            "oversized": {"code": 200, "orders": [raw_order(i, BASE_TS) for i in range(101)]},
        }
        for name, body in cases.items():
            with self.subTest(name), aioresponses() as mocked:
                mocked.get(INACTIVE_RE, body=json.dumps(body), content_type="application/json")
                with self.assertRaises(LighterHistoryResponseError):
                    await self.connector.fetch_inactive_orders_page(PAIR)
        with aioresponses() as mocked:
            mocked.get(INACTIVE_RE, body=json.dumps({"code": 200, "orders": [], "next_cursor": 12345}),
                       content_type="application/json")
            page = await self.connector.fetch_inactive_orders_page(PAIR)
        self.assertEqual(12345, page.next_cursor)  # passed through; the scanner classifies it as malformed

    def test_rate_limits_standard_account_weights(self):
        limits = {limit.limit_id: limit for limit in CONSTANTS.generate_account_limit("Standard")}
        self.assertEqual(18_000, limits[CONSTANTS.ALL_ENDPOINTS_LIMIT].limit)
        linked = {path: {pair.limit_id: pair.weight for pair in limits[path].linked_limits}
                  for path in (CONSTANTS.TRADES_PATH_URL, CONSTANTS.ACCOUNT_INACTIVE_ORDERS_PATH_URL,
                               CONSTANTS.ACCOUNT_ACTIVE_ORDERS_PATH_URL, CONSTANTS.ACCOUNT_PATH_URL,
                               CONSTANTS.ORDER_BOOK_DETAILS_PATH_URL)}
        self.assertEqual(600, linked[CONSTANTS.TRADES_PATH_URL][CONSTANTS.ALL_ENDPOINTS_LIMIT])
        self.assertEqual(100, linked[CONSTANTS.ACCOUNT_INACTIVE_ORDERS_PATH_URL][CONSTANTS.ALL_ENDPOINTS_LIMIT])
        for path in (CONSTANTS.ACCOUNT_ACTIVE_ORDERS_PATH_URL, CONSTANTS.ACCOUNT_PATH_URL,
                     CONSTANTS.ORDER_BOOK_DETAILS_PATH_URL):
            self.assertEqual(300, linked[path][CONSTANTS.ALL_ENDPOINTS_LIMIT])
        self.assertEqual(100, CONSTANTS.HISTORY_PAGE_LIMIT_MAX)

    async def test_legacy_one_page_reader_is_unchanged(self):
        calls = []

        async def api_get(path_url, **kwargs):
            calls.append((path_url, kwargs.get("params")))
            return {"orders": []}

        self.connector._api_get = AsyncMock(side_effect=api_get)
        tracked = MagicMock(client_order_id="11", exchange_order_id=None, trading_pair=PAIR)
        self.assertIsNone(await self.connector._find_order(tracked_order=tracked, include_inactive=True))
        self.assertEqual((CONSTANTS.ACCOUNT_INACTIVE_ORDERS_PATH_URL,
                          {"account_index": ACCOUNT, "market_id": MARKET, "limit": 100}), calls[1])

    # ------------------------------------------------------------------------ scanner over connector
    async def test_ac10_ac11_scanner_over_connector_reads_every_page_and_dedupes_boundary(self):
        orders = [raw_order(i, BASE_TS - i * 1000) for i in range(250)]
        target_cid = orders[230]["client_order_index"]  # on the third page
        trades = [raw_trade(i, BASE_TS - i * 1000, size="0.25") for i in range(150)]
        orders_endpoint = PagedEndpoint("orders", orders)
        trades_endpoint = PagedEndpoint("trades", trades)
        trades_endpoint.pages[1].insert(0, trades[99])  # boundary duplicate repeated on the next page
        port = LighterExchangePort(self.connector, PAIR)
        view = InMemoryHistoryCursorView(CONSTANTS.ROBINHOOD_DOMAIN)
        scanner = HistoryScanner(port, view, Decimal("60"), 700, clock=lambda: BASE_TS / 1000)
        results = []
        with aioresponses() as mocked:
            mocked.get(INACTIVE_RE, callback=orders_endpoint, repeat=True)
            mocked.get(TRADES_RE, callback=trades_endpoint, repeat=True)
            for _ in range(10):
                results.append(await scanner.scan())
                if results[-1].incomplete_reason != "in_progress":
                    break
        self.assertFalse(results[0].complete)  # the first page is never the complete history
        final = results[-1]
        self.assertTrue(final.complete, final.incomplete_reason)
        self.assertEqual(3, len(orders_endpoint.requests))
        self.assertEqual(2, len(trades_endpoint.requests))
        self.assertIn(target_cid, {row.client_order_id for row in final.new_orders})
        self.assertEqual(250, len(final.new_orders))
        self.assertEqual(150, len(final.new_trades))
        self.assertEqual(Decimal("37.50"), sum((row.size for row in final.new_trades), Decimal("0")))
        self.assertEqual([None, "cur+1/==", "cur+2/=="],
                         [request["params"].get("cursor") for request in orders_endpoint.requests])
        self.assertTrue(all(w <= 700 for w in (r.weight_used for r in results)))

    async def test_ac12_malformed_cursor_from_venue_makes_scan_incomplete(self):
        orders_endpoint = PagedEndpoint("orders", [raw_order(i, BASE_TS - i) for i in range(3)])
        trades_endpoint = PagedEndpoint("trades", [raw_trade(i, BASE_TS - i) for i in range(3)])
        trades_endpoint.overrides[0] = {"next_cursor": 12345}
        scanner = HistoryScanner(LighterExchangePort(self.connector, PAIR),
                                 InMemoryHistoryCursorView(CONSTANTS.ROBINHOOD_DOMAIN), Decimal("60"), 700)
        with aioresponses() as mocked:
            mocked.get(INACTIVE_RE, callback=orders_endpoint, repeat=True)
            mocked.get(TRADES_RE, callback=trades_endpoint, repeat=True)
            result = await scanner.scan()
        self.assertFalse(result.complete)
        self.assertTrue(result.incomplete_reason.startswith("malformed_cursor"), result.incomplete_reason)

    async def test_ac22_http_body_ids_above_2_pow_53_stay_exact(self):
        trade_id = (1 << 63) - 25
        body = ('{"code":200,"trades":[{"trade_id":%d,"trade_id_str":"%d","market_id":5,"size":"10.00",'
                '"price":"5.4000","ask_account_id":%d,"bid_account_id":%d,"ask_id":%d,"bid_id":%d,'
                '"ask_client_id":%d,"bid_client_id":%d,"ask_client_id_str":"%d","bid_client_id_str":"%d",'
                '"is_maker_ask":true,"timestamp":%d}],"next_cursor":null}'
                % (trade_id, trade_id, ACCOUNT, ACCOUNT, BIG + 1, BIG + 3, MAX_CID, MAX_CID - 1, MAX_CID,
                   MAX_CID - 1, BASE_TS))
        port = LighterExchangePort(self.connector, PAIR)
        with aioresponses() as mocked:
            mocked.get(TRADES_RE, body=body, content_type="application/json")
            page = await port.trades_page(None)
        sell, buy = page.rows  # a self-trade: both own legs are kept
        self.assertEqual(str(trade_id), sell.trade_id_str)
        self.assertEqual(str(trade_id), buy.trade_id_str)
        self.assertEqual(str(BIG + 1), sell.own_exchange_order_id)
        self.assertEqual(str(BIG + 3), buy.own_exchange_order_id)
        self.assertEqual(MAX_CID, sell.own_client_order_id)
        self.assertEqual(MAX_CID - 1, buy.own_client_order_id)
        self.assertNotEqual(str(int(float(BIG + 1))), sell.own_exchange_order_id)

    async def test_fetch_account_position_is_strict(self):
        account = {"accounts": [{
            "account_index": ACCOUNT, "available_balance": "80",
            "assets": [{"asset_id": 3, "symbol": "USDG", "margin_balance": "100", "locked_balance": "20"}],
            "positions": [{"market_id": MARKET, "position": "12.50", "sign": -1, "initial_margin_fraction": "20.00",
                           "margin_mode": 0},
                          {"market_id": 9, "position": "1", "sign": 1}],
        }]}
        with aioresponses() as mocked:
            mocked.get(ACCOUNT_RE, body=json.dumps(account), content_type="application/json")
            snapshot = await self.connector.fetch_account_position(PAIR)
        self.assertEqual(Decimal("-12.50"), snapshot["net_position"])
        self.assertEqual(Decimal("5"), snapshot["leverage"])
        self.assertEqual("0", snapshot["margin_mode"])
        self.assertEqual(Decimal("80"), snapshot["available_collateral"])
        account["accounts"][0]["positions"][0]["position"] = 12.5  # float quantity -> fail closed
        with aioresponses() as mocked:
            mocked.get(ACCOUNT_RE, body=json.dumps(account), content_type="application/json")
            with self.assertRaises(IOError):
                await self.connector.fetch_account_position(PAIR)

    # ------------------------------------------------------------------------ CID submission
    async def test_submit_uses_pre_persisted_cid_unchanged_gtt_tp_reduce_only_false(self):
        result = await self.connector.submit_with_client_id(
            client_order_id=MAX_CID, trading_pair=PAIR, trade_type=TradeType.SELL, price=Decimal("6.0000"),
            amount=Decimal("10.00"), order_type=OrderType.LIMIT, order_expiry_ms=BASE_TS + 86_400_000)
        self.assertEqual(LighterTransportOutcome.ACCEPTED, result.outcome)
        self.assertEqual("0xfeed", result.tx_hash)
        self.signer.create_order.assert_awaited_once_with(
            market_index=MARKET, client_order_index=MAX_CID, base_amount=1000, price=60000, is_ask=True,
            order_type=SdkSignerClient.ORDER_TYPE_LIMIT,
            time_in_force=SdkSignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            reduce_only=False, order_expiry=BASE_TS + 86_400_000)
        self.assertEqual([True], self.lock_states)  # signed/sent under the connector tx lock
        await asyncio.sleep(0)
        tracked = self.connector._order_tracker.fetch_order(client_order_id=str(MAX_CID))
        self.assertIsNotNone(tracked)
        self.assertEqual(OrderState.OPEN, tracked.current_state)
        self.assertEqual(TradeType.SELL, tracked.trade_type)
        self.assertIn(CONSTANTS.SEND_TX_LIMIT, [log.rate_limit.limit_id for log in self.connector._throttler._task_logs])

    async def test_submit_entry_is_post_only_with_sdk_default_expiry(self):
        result = await self.connector.submit_with_client_id(
            client_order_id=1, trading_pair=PAIR, trade_type=TradeType.BUY, price=Decimal("5.0000"),
            amount=Decimal("10"), order_type=OrderType.LIMIT_MAKER)
        self.assertEqual(LighterTransportOutcome.ACCEPTED, result.outcome)
        kwargs = self.signer.create_order.await_args.kwargs
        self.assertEqual(SdkSignerClient.ORDER_TIME_IN_FORCE_POST_ONLY, kwargs["time_in_force"])
        self.assertIs(False, kwargs["reduce_only"])
        self.assertFalse(kwargs["is_ask"])
        self.assertNotIn("order_expiry", kwargs)

    async def test_pre_send_validation_is_not_sent_and_never_touches_signer_or_tracker(self):
        base = dict(client_order_id=7, trading_pair=PAIR, trade_type=TradeType.BUY, price=Decimal("5.0000"),
                    amount=Decimal("10"), order_type=OrderType.LIMIT_MAKER)
        cases = {
            "cid_zero": dict(client_order_id=0),
            "cid_negative": dict(client_order_id=-5),
            "cid_49_bits": dict(client_order_id=1 << 48),
            "cid_bool": dict(client_order_id=True),
            "cid_str": dict(client_order_id="7"),
            "cid_float": dict(client_order_id=7.0),
            "market_order": dict(order_type=OrderType.MARKET),
            "price_would_round": dict(price=Decimal("5.00001")),
            "amount_would_round": dict(amount=Decimal("10.001")),
            "amount_zero": dict(amount=Decimal("0")),
            "price_nan": dict(price=Decimal("NaN")),
            "bad_expiry": dict(order_expiry_ms=-1),
            "float_expiry": dict(order_expiry_ms=1.5),
            "unknown_pair": dict(trading_pair="ETH-USDG"),
            "bad_side": dict(trade_type=TradeType.RANGE),
        }
        for name, overrides in cases.items():
            with self.subTest(name):
                result = await self.connector.submit_with_client_id(**dict(base, **overrides))
                self.assertEqual(LighterTransportOutcome.NOT_SENT, result.outcome, result.detail)
                self.assertTrue(result.detail.startswith("pre_send_validation"), result.detail)
        self.signer.create_order.assert_not_awaited()
        self.assertEqual({}, dict(self.connector._order_tracker.all_orders))
        self.connector._signer_client = None
        result = await self.connector.submit_with_client_id(**base)
        self.assertEqual(LighterTransportOutcome.NOT_SENT, result.outcome)

    async def test_ac56_timeout_not_found_and_errors_are_unknown_and_never_retry_with_new_cid(self):
        outcomes = {
            "timeout": asyncio.TimeoutError(),
            "connection": ConnectionResetError("peer reset"),
            "secretish_exception": RuntimeError(f"{PRIVATE_KEY} {AUTH_TOKEN}"),
            "http_400_or_signing_error": (None, None, "invalid nonce"),
            "not_found": (None, None, "order not found"),
            "non_200": (None, {"code": 21120, "message": "rejected"}, None),
            "missing_code": (None, {}, None),
        }
        for cid, (name, behaviour) in enumerate(outcomes.items(), start=100):
            with self.subTest(name):
                self.signer.create_order.reset_mock()
                if isinstance(behaviour, BaseException):
                    self.signer.create_order.side_effect = behaviour
                else:
                    self.signer.create_order.side_effect = None
                    self.signer.create_order.return_value = behaviour
                result = await self.connector.submit_with_client_id(
                    client_order_id=cid, trading_pair=PAIR, trade_type=TradeType.BUY, price=Decimal("5.0000"),
                    amount=Decimal("10"), order_type=OrderType.LIMIT_MAKER)
                self.assertEqual(LighterTransportOutcome.UNKNOWN, result.outcome)
                self.assertNotEqual(LighterTransportOutcome.DEFINITIVE_REJECT_ZERO_FILL, result.outcome)
                self.assertNotIn(PRIVATE_KEY, result.detail)
                self.assertNotIn(AUTH_TOKEN, result.detail)
                self.signer.create_order.assert_awaited_once()
                self.assertEqual(cid, self.signer.create_order.await_args.kwargs["client_order_index"])
                await asyncio.sleep(0)
                tracked = self.connector._order_tracker.fetch_order(client_order_id=str(cid))
                self.assertIsNotNone(tracked)  # still reserved/tracked, not marked failed
                self.assertFalse(tracked.is_failure)
                # the same CID is never re-sent without documented idempotence
                again = await self.connector.submit_with_client_id(
                    client_order_id=cid, trading_pair=PAIR, trade_type=TradeType.BUY, price=Decimal("5.0000"),
                    amount=Decimal("10"), order_type=OrderType.LIMIT_MAKER)
                self.assertEqual(LighterTransportOutcome.UNKNOWN, again.outcome)
                self.assertEqual("duplicate_client_order_id_in_flight", again.detail)
                self.signer.create_order.assert_awaited_once()

    # ------------------------------------------------------------------------ cancel
    async def test_cancel_by_exchange_index_is_not_terminal(self):
        await self.connector.submit_with_client_id(
            client_order_id=55, trading_pair=PAIR, trade_type=TradeType.BUY, price=Decimal("5.0000"),
            amount=Decimal("10"), order_type=OrderType.LIMIT_MAKER)
        await asyncio.sleep(0)
        result = await self.connector.cancel_with_client_id(PAIR, 55, str(BIG + 55))
        self.assertEqual(LighterTransportOutcome.ACCEPTED, result.outcome)
        self.assertEqual("cancel_tx_accepted_not_terminal", result.detail)
        self.signer.cancel_order.assert_awaited_once_with(market_index=MARKET, order_index=BIG + 55)
        self.assertEqual([True, True], self.lock_states)
        await asyncio.sleep(0)
        tracked = self.connector._order_tracker.fetch_order(client_order_id="55")
        self.assertEqual(OrderState.PENDING_CANCEL, tracked.current_state)

    async def test_cancel_looks_up_index_by_exact_cid_or_is_not_sent(self):
        active = {"code": 200, "orders": [raw_order(1, BASE_TS, status="open", cid=77),
                                          raw_order(2, BASE_TS, status="open", cid=78)]}
        with aioresponses() as mocked:
            mocked.get(ACTIVE_RE, body=json.dumps(active), content_type="application/json", repeat=True)
            found = await self.connector.cancel_with_client_id(PAIR, 78)
            missing = await self.connector.cancel_with_client_id(PAIR, 79)
        self.assertEqual(LighterTransportOutcome.ACCEPTED, found.outcome)
        self.assertEqual(str(BIG + 2), found.exchange_order_id)
        self.assertEqual(LighterTransportOutcome.NOT_SENT, missing.outcome)
        self.signer.cancel_order.assert_awaited_once_with(market_index=MARKET, order_index=BIG + 2)
        for bad in ("12a", "-1", 1.5):
            with self.subTest(bad=bad):
                self.assertEqual(LighterTransportOutcome.NOT_SENT,
                                 (await self.connector.cancel_with_client_id(PAIR, 78, bad)).outcome)

    async def test_cancel_transport_failures_are_unknown(self):
        for behaviour in (asyncio.TimeoutError(), (None, None, "order not found"), (None, {"code": 500}, None)):
            with self.subTest(behaviour=repr(behaviour)):
                if isinstance(behaviour, BaseException):
                    self.signer.cancel_order.side_effect = behaviour
                else:
                    self.signer.cancel_order.side_effect = None
                    self.signer.cancel_order.return_value = behaviour
                result = await self.connector.cancel_with_client_id(PAIR, 90, str(BIG + 90))
                self.assertEqual(LighterTransportOutcome.UNKNOWN, result.outcome)


if __name__ == "__main__":
    unittest.main()
