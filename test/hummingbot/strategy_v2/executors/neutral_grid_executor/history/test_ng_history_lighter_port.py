"""LighterExchangePort field mapping and transport classification (AC-22, AC-56, NG-HIST-001, NG-ORD-002)."""
import json
import traceback
import unittest
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.derivative.lighter_perpetual import lighter_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_api_utils import (
    LighterHistoryPage,
    LighterHistoryResponseError,
    LighterTransportOutcome,
    LighterTransportResult,
)
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    OrderTypePolicy,
    Side,
    SubmitRequest,
    TransportOutcome,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.history import (
    HistorySchemaError,
    HistoryScanner,
    InMemoryHistoryCursorView,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.lighter_port import (
    LighterExchangePort,
    order_row_from_raw,
    timestamp_to_ms,
    trade_rows_from_raw,
)

ACCOUNT = 724450
OTHER = 99
MARKET = 5
TRADE_ID = (1 << 60) + 7          # > 2**53: a float round-trip would corrupt it
ASK_ORDER = (1 << 53) + 1          # 9007199254740993 == float(2**53) + 1 is not representable
BID_ORDER = (1 << 53) + 3
MAX_CID = (1 << 48) - 1


def raw_trade(**overrides) -> Dict[str, Any]:
    raw = {
        "trade_id": TRADE_ID, "trade_id_str": str(TRADE_ID), "tx_hash": "0xabc", "type": "trade",
        "market_id": MARKET, "size": "10.00", "price": "5.4000", "usd_amount": "54",
        "ask_id": ASK_ORDER, "bid_id": BID_ORDER, "ask_id_str": str(ASK_ORDER), "bid_id_str": str(BID_ORDER),
        "ask_account_id": ACCOUNT, "bid_account_id": OTHER, "is_maker_ask": True,
        "ask_client_id": MAX_CID, "bid_client_id": 5, "ask_client_id_str": str(MAX_CID), "bid_client_id_str": "5",
        "timestamp": 1_790_000_000_123, "transaction_time": 1_790_000_000_123_456,
    }
    raw.update(overrides)
    return raw


def raw_order(**overrides) -> Dict[str, Any]:
    raw = {
        "order_index": ASK_ORDER, "client_order_index": MAX_CID, "order_id": str(ASK_ORDER),
        "client_order_id": str(MAX_CID), "market_index": MARKET, "owner_account_index": ACCOUNT,
        "initial_base_amount": "10.00", "price": "5.4000", "nonce": 123456789,
        "remaining_base_amount": "0.00", "is_ask": True, "filled_base_amount": "10.00",
        "filled_quote_amount": "54.00", "type": "limit", "time_in_force": "good-till-time",
        "reduce_only": False, "order_expiry": 1_792_000_000_000, "status": "filled",
        "timestamp": 1_790_000_000_500, "created_at": 1_790_000_000_000, "updated_at": 1_790_000_000_500,
    }
    raw.update(overrides)
    return raw


class FieldMappingTest(unittest.TestCase):

    def test_ac22_ids_above_2_pow_53_survive_json_wire_and_mapping_exactly(self):
        wire = json.dumps({"trades": [raw_trade()]})  # JSON numbers on the wire
        decoded = json.loads(wire)["trades"][0]
        wire_str_only = json.loads(json.dumps({"t": raw_trade(trade_id=None, ask_id=None, ask_client_id=None)}))["t"]
        for raw in (decoded, wire_str_only):
            with self.subTest(keys=sorted(k for k, v in raw.items() if v is not None)[:3]):
                (row,) = trade_rows_from_raw(raw, account_index=ACCOUNT)
                self.assertEqual(str(TRADE_ID), row.trade_id_str)
                self.assertEqual(str(ASK_ORDER), row.own_exchange_order_id)
                self.assertEqual(MAX_CID, row.own_client_order_id)
                self.assertNotEqual(str(int(float(TRADE_ID))), row.trade_id_str)  # a float path would differ
                self.assertEqual(Side.SELL, row.own_side)
                self.assertTrue(row.is_maker)
                self.assertEqual(Decimal("10.00"), row.size)
                self.assertEqual(Decimal("5.4000"), row.price)
                self.assertEqual(1_790_000_000_123, row.timestamp_ms)
                self.assertIn(str(TRADE_ID), row.raw_json)

        order = order_row_from_raw(json.loads(json.dumps(raw_order())))
        self.assertEqual(str(ASK_ORDER), order.order_id)
        self.assertEqual(str(ASK_ORDER), order.order_index)
        self.assertEqual(MAX_CID, order.client_order_id)
        self.assertEqual(str(MAX_CID), order.client_order_id_str)
        self.assertEqual("123456789", order.nonce)

    def test_ac22_float_or_bool_ids_and_quantities_are_rejected(self):
        bad = {
            "float_trade_id": raw_trade(trade_id=float(TRADE_ID), trade_id_str=None),
            "bool_market": raw_trade(market_id=True),
            "float_size": raw_trade(size=10.0),
            "float_order_id": raw_trade(ask_id=float(ASK_ORDER), ask_id_str=None),
            "nan_size": raw_trade(size="NaN"),
            "zero_size": raw_trade(size="0"),
            "float_timestamp": raw_trade(timestamp=1.79e12),
        }
        for name, raw in bad.items():
            with self.subTest(name), self.assertRaises(HistorySchemaError):
                trade_rows_from_raw(raw, account_index=ACCOUNT)
        with self.assertRaises(HistorySchemaError):
            order_row_from_raw(raw_order(filled_base_amount=10.0))
        with self.assertRaises(HistorySchemaError):
            order_row_from_raw(raw_order(reduce_only="false"))

    def test_disagreeing_id_pairs_fail_closed(self):
        cases = {
            "trade_id": raw_trade(trade_id_str=str(TRADE_ID + 1)),
            "ask_id": raw_trade(ask_id_str=str(ASK_ORDER + 1)),
            "ask_client_id": raw_trade(ask_client_id=MAX_CID - 1),
        }
        for name, raw in cases.items():
            with self.subTest(name), self.assertRaises(HistorySchemaError):
                trade_rows_from_raw(raw, account_index=ACCOUNT)
        for name, raw in {
            "order_id_vs_index": raw_order(order_id=str(ASK_ORDER + 1)),
            "client_ids": raw_order(client_order_id=str(MAX_CID - 1)),
            "client_id_str": raw_order(client_order_id_str="7"),
            "missing_exchange_id": raw_order(order_id=None, order_index=None),
        }.items():
            with self.subTest(name), self.assertRaises(HistorySchemaError):
                order_row_from_raw(raw)

    def test_self_trade_yields_two_own_legs(self):
        rows = trade_rows_from_raw(raw_trade(bid_account_id=ACCOUNT, bid_client_id=6, bid_client_id_str="6"),
                                   account_index=ACCOUNT)
        self.assertEqual([Side.SELL, Side.BUY], [row.own_side for row in rows])
        self.assertEqual([str(ASK_ORDER), str(BID_ORDER)], [row.own_exchange_order_id for row in rows])
        self.assertEqual([MAX_CID, 6], [row.own_client_order_id for row in rows])
        self.assertEqual([True, False], [row.is_maker for row in rows])
        self.assertEqual(len({row.dedupe_key("d") for row in rows}), 2)

    def test_bid_leg_and_foreign_trade(self):
        (row,) = trade_rows_from_raw(raw_trade(ask_account_id=OTHER, bid_account_id=ACCOUNT), account_index=ACCOUNT)
        self.assertEqual(Side.BUY, row.own_side)
        self.assertEqual(str(BID_ORDER), row.own_exchange_order_id)
        self.assertEqual(5, row.own_client_order_id)
        self.assertFalse(row.is_maker)
        with self.assertRaises(HistorySchemaError):
            trade_rows_from_raw(raw_trade(ask_account_id=OTHER), account_index=ACCOUNT)

    def test_missing_client_id_is_none_not_guessed(self):
        (row,) = trade_rows_from_raw(raw_trade(ask_client_id=None, ask_client_id_str=""), account_index=ACCOUNT)
        self.assertIsNone(row.own_client_order_id)
        (unknown_maker,) = trade_rows_from_raw(raw_trade(is_maker_ask=None), account_index=ACCOUNT)
        self.assertIsNone(unknown_maker.is_maker)

    def test_order_row_mapping(self):
        row = order_row_from_raw(raw_order(is_ask=False, status="canceled-post-only", filled_base_amount="0",
                                           remaining_base_amount="10.00"))
        self.assertEqual(Side.BUY, row.side)
        self.assertEqual("canceled-post-only", row.status)
        self.assertEqual(Decimal("0"), row.filled_base_amount)
        self.assertEqual(Decimal("10.00"), row.remaining_base_amount)
        self.assertEqual(Decimal("10.00"), row.initial_base_amount)
        self.assertFalse(row.reduce_only)
        self.assertEqual(ACCOUNT, row.account_index)
        self.assertEqual(MARKET, row.market_id)
        self.assertEqual(1_790_000_000_500, row.timestamp_ms)
        created = order_row_from_raw(raw_order(), timestamp_field="created_at")
        self.assertEqual(1_790_000_000_000, created.timestamp_ms)

    def test_timestamp_units_normalized_with_integers_only(self):
        self.assertEqual(1_790_000_000_000, timestamp_to_ms(1_790_000_000))
        self.assertEqual(1_790_000_000_123, timestamp_to_ms(1_790_000_000_123))
        self.assertEqual(1_790_000_000_123, timestamp_to_ms(1_790_000_000_123_456))
        self.assertEqual(1_790_000_000_123, timestamp_to_ms(1_790_000_000_123_456_789))
        self.assertEqual(1_790_000_000_123, timestamp_to_ms("1790000000123"))
        for bad in (0, -1, 1.5, True, "12a", None):
            with self.subTest(bad=bad), self.assertRaises(HistorySchemaError):
                timestamp_to_ms(bad)


class FakeConnector:
    domain = CONSTANTS.ROBINHOOD_DOMAIN
    account_index = ACCOUNT

    def __init__(self):
        self.market = SimpleNamespace(market_id=MARKET)
        self.trades_pages: List[LighterHistoryPage] = []
        self.orders_pages: List[LighterHistoryPage] = []
        self.submit_with_client_id = AsyncMock(
            return_value=LighterTransportResult(LighterTransportOutcome.ACCEPTED, "send_tx_accepted"))
        self.cancel_with_client_id = AsyncMock(
            return_value=LighterTransportResult(LighterTransportOutcome.ACCEPTED, "cancel_tx_accepted_not_terminal",
                                                exchange_order_id=str(ASK_ORDER)))
        self.fetch_active_orders = AsyncMock()
        self.cursors: List[Optional[str]] = []

    def market_info_for_trading_pair(self, trading_pair):
        return self.market

    async def fetch_trades_page(self, trading_pair, cursor=None, limit=100):
        self.cursors.append(cursor)
        return self.trades_pages.pop(0)

    async def fetch_inactive_orders_page(self, trading_pair, cursor=None, limit=100):
        self.cursors.append(cursor)
        page = self.orders_pages.pop(0)
        if isinstance(page, Exception):
            raise page
        return page


class LighterPortTest(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.connector = FakeConnector()
        self.port = LighterExchangePort(self.connector, "LIT-USDG")

    async def test_pages_pass_cursor_and_next_cursor_verbatim(self):
        opaque = "eyJ0IjoxfQ==/+x"
        self.connector.trades_pages.append(
            LighterHistoryPage(rows=[raw_trade()], next_cursor="next/+=", cursor_sent=opaque, limit=100))
        page = await self.port.trades_page(opaque)
        self.assertEqual([opaque], self.connector.cursors)
        self.assertEqual("next/+=", page.next_cursor)
        self.assertEqual(opaque, page.raw_cursor_sent)
        self.assertEqual(str(TRADE_ID), page.rows[0].trade_id_str)

        self.connector.orders_pages.append(
            LighterHistoryPage(rows=[raw_order()], next_cursor=None, cursor_sent=None, limit=100))
        orders = await self.port.inactive_orders_page(None)
        self.assertIsNone(orders.next_cursor)
        self.assertEqual(MAX_CID, orders.rows[0].client_order_id)

    async def test_schema_errors_surface_as_history_schema_error(self):
        self.connector.orders_pages.append(LighterHistoryResponseError("bad"))
        with self.assertRaises(HistorySchemaError):
            await self.port.inactive_orders_page(None)
        self.connector.orders_pages.append(
            LighterHistoryPage(rows=[raw_order(market_index=MARKET + 1)], next_cursor=None, cursor_sent=None,
                               limit=100))
        with self.assertRaises(HistorySchemaError):
            await self.port.inactive_orders_page(None)

    async def test_active_orders_truncation_fails_closed(self):
        self.connector.fetch_active_orders.return_value = LighterHistoryPage(
            rows=[raw_order(status="open")], next_cursor="more", cursor_sent=None, limit=None)
        with self.assertRaises(HistorySchemaError):
            await self.port.active_orders()
        self.connector.fetch_active_orders.return_value = LighterHistoryPage(
            rows=[raw_order(status="open")], next_cursor=None, cursor_sent=None, limit=None)
        (row,) = await self.port.active_orders()
        self.assertEqual("open", row.status)

    def test_request_weights_match_standard_account_table(self):
        self.assertEqual(600, self.port.request_weight("trades"))
        self.assertEqual(100, self.port.request_weight("inactive_orders"))
        self.assertEqual(300, self.port.request_weight("active_orders"))
        self.assertEqual(300, self.port.request_weight("account"))
        self.assertEqual(300, self.port.request_weight("anything-else"))

    async def test_submit_passes_cid_unchanged_and_never_reduce_only(self):
        req = SubmitRequest(client_order_id=MAX_CID, side=Side.SELL, price=Decimal("6.0000"),
                            amount=Decimal("10.00"), order_type=OrderTypePolicy.LIMIT, expiry_ms=1_792_000_000_000)
        result = await self.port.submit(req)
        self.assertEqual(TransportOutcome.ACCEPTED, result.outcome)
        self.connector.submit_with_client_id.assert_awaited_once_with(
            client_order_id=MAX_CID, trading_pair="LIT-USDG", trade_type=TradeType.SELL, price=Decimal("6.0000"),
            amount=Decimal("10.00"), order_type=OrderType.LIMIT, order_expiry_ms=1_792_000_000_000)
        entry = SubmitRequest(client_order_id=1, side=Side.BUY, price=Decimal("5"), amount=Decimal("10"),
                              order_type=OrderTypePolicy.LIMIT_MAKER)
        await self.port.submit(entry)
        self.assertEqual(OrderType.LIMIT_MAKER, self.connector.submit_with_client_id.await_args.kwargs["order_type"])

    async def test_ac56_classification_at_port(self):
        reduce_only = SubmitRequest(client_order_id=1, side=Side.BUY, price=Decimal("5"), amount=Decimal("10"),
                                    order_type=OrderTypePolicy.LIMIT, reduce_only=True)
        result = await self.port.submit(reduce_only)
        self.assertEqual(TransportOutcome.NOT_SENT, result.outcome)
        market = SubmitRequest(client_order_id=1, side=Side.BUY, price=Decimal("5"), amount=Decimal("10"),
                               order_type="MARKET")
        self.assertEqual(TransportOutcome.NOT_SENT, (await self.port.submit(market)).outcome)
        self.connector.submit_with_client_id.assert_not_awaited()

        ok = SubmitRequest(client_order_id=42, side=Side.BUY, price=Decimal("5"), amount=Decimal("10"),
                           order_type=OrderTypePolicy.LIMIT_MAKER)
        for connector_result, expected in (
            (TimeoutError(), TransportOutcome.UNKNOWN),
            (RuntimeError("boom"), TransportOutcome.UNKNOWN),
            (LighterTransportResult(LighterTransportOutcome.UNKNOWN, "transport_error: code=None"),
             TransportOutcome.UNKNOWN),
            (LighterTransportResult(LighterTransportOutcome.NOT_SENT, "pre_send_validation: x"),
             TransportOutcome.NOT_SENT),
            (SimpleNamespace(outcome="SOMETHING_NEW", detail=""), TransportOutcome.UNKNOWN),
        ):
            with self.subTest(result=repr(connector_result)):
                self.connector.submit_with_client_id.reset_mock()
                if isinstance(connector_result, Exception):
                    self.connector.submit_with_client_id.side_effect = connector_result
                else:
                    self.connector.submit_with_client_id.side_effect = None
                    self.connector.submit_with_client_id.return_value = connector_result
                result = await self.port.submit(ok)
                self.assertEqual(expected, result.outcome)
                # never a new CID: the adapter forwards exactly the persisted id, once
                self.connector.submit_with_client_id.assert_awaited_once()
                self.assertEqual(42, self.connector.submit_with_client_id.await_args.kwargs["client_order_id"])

    async def test_cancel_mapping(self):
        result = await self.port.cancel(7, str(ASK_ORDER))
        self.assertEqual(TransportOutcome.ACCEPTED, result.outcome)
        self.assertEqual(str(ASK_ORDER), result.exchange_order_id)
        self.connector.cancel_with_client_id.assert_awaited_once_with(
            trading_pair="LIT-USDG", client_order_id=7, exchange_order_index=str(ASK_ORDER))
        self.connector.cancel_with_client_id.side_effect = TimeoutError()
        self.assertEqual(TransportOutcome.UNKNOWN, (await self.port.cancel(7, None)).outcome)

    async def test_trading_rules_reflect_market_state_and_fail_closed(self):
        """Review finding 7: limit/post-only availability comes from fresh market state."""
        def market(raw_info):
            return SimpleNamespace(
                market_id=MARKET, min_price_increment=Decimal("0.0001"), min_base_increment=Decimal("0.01"),
                min_base_amount=Decimal("5"), min_quote_amount=Decimal("10"), max_leverage=Decimal("5"),
                raw_info=raw_info)

        tradable = {"status": "active", "market_config": {"hidden": False, "force_reduce_only": False}}
        cases = {
            "tradable": (tradable, True),
            "force_reduce_only": ({"status": "active", "market_config": {"hidden": False, "force_reduce_only": True}},
                                  False),
            "hidden": ({"status": "active", "market_config": {"hidden": True, "force_reduce_only": False}}, False),
            "inactive": ({"status": "inactive", "market_config": {"hidden": False, "force_reduce_only": False}},
                         False),
            "missing_market_config": ({"status": "active"}, False),
            "missing_force_reduce_only": ({"status": "active", "market_config": {"hidden": False}}, False),
            "non_bool_flag": ({"status": "active", "market_config": {"hidden": False, "force_reduce_only": "false"}},
                              False),
        }
        self.connector._update_trading_rules = AsyncMock()
        for name, (raw_info, expected) in cases.items():
            with self.subTest(name):
                self.connector.market = market(raw_info)
                rules = await self.port.trading_rules()
                self.assertIs(expected, rules.supports_limit)
                self.assertIs(expected, rules.supports_post_only)
                self.assertEqual(Decimal("0.0001"), rules.tick_size)
        self.assertEqual(len(cases), self.connector._update_trading_rules.await_count)  # always refreshed

    @staticmethod
    def leaky_content_type_error():
        """The real aiohttp error whose text carries the request URL, including ``auth=`` (critic C1)."""
        import aiohttp
        import yarl
        from multidict import CIMultiDict, CIMultiDictProxy
        url = yarl.URL("https://api.rh.lighter.xyz/api/v1/accountActiveOrders?account_index=1&auth=tok-SECRET-123")
        info = aiohttp.RequestInfo(url, "GET", CIMultiDictProxy(CIMultiDict()), url)
        return aiohttp.ContentTypeError(info, (), message="Attempt to decode JSON with unexpected mimetype: text/html")

    async def test_port_errors_carry_only_the_exception_type_never_its_text(self):
        leak = self.leaky_content_type_error()
        self.assertIn("SECRET", str(leak))
        self.connector.fetch_active_orders = AsyncMock(side_effect=leak)
        self.connector.fetch_account_position = AsyncMock(side_effect=leak)
        self.connector.fetch_inactive_orders_page = AsyncMock(side_effect=leak)
        self.connector.fetch_trades_page = AsyncMock(side_effect=leak)
        self.connector._update_trading_rules = AsyncMock(side_effect=leak)
        calls = {
            "active_orders": self.port.active_orders,
            "position": self.port.position,
            "inactive_orders_page": lambda: self.port.inactive_orders_page(None),
            "trades_page": lambda: self.port.trades_page(None),
            "trading_rules": self.port.trading_rules,
        }
        for name, call in calls.items():
            with self.subTest(name):
                with self.assertRaises(Exception) as ctx:
                    await call()
                error = ctx.exception
                rendered = "".join(traceback.format_exception(type(error), error, error.__traceback__))
                self.assertNotIn("SECRET", rendered)
                self.assertNotIn("auth=", rendered)
                self.assertNotIn("SECRET", repr(error))
                self.assertIsNone(error.__cause__)
                self.assertIsNone(error.__context__)
                self.assertIn("ContentTypeError", str(error))
                self.assertEqual("ContentTypeError", getattr(error, "original_type", None))

    async def test_scanner_reason_keeps_only_the_original_error_type(self):
        self.connector.fetch_inactive_orders_page = AsyncMock(side_effect=self.leaky_content_type_error())
        scanner = HistoryScanner(self.port, InMemoryHistoryCursorView(self.port.domain), Decimal("60"), 700)
        result = await scanner.scan()
        self.assertEqual("page_fetch_error:inactive_orders:ContentTypeError", result.incomplete_reason)

    async def test_position_scope_is_checked(self):
        self.connector.fetch_account_position = AsyncMock(return_value={
            "account_index": ACCOUNT, "market_id": MARKET, "net_position": Decimal("-12.5"),
            "leverage": Decimal("5"), "margin_mode": "0", "available_collateral": Decimal("80"), "fetched_at": 1.0})
        position = await self.port.position()
        self.assertEqual(Decimal("-12.5"), position.net_base)
        self.assertEqual(Decimal("5"), position.leverage)
        self.connector.fetch_account_position.return_value = dict(
            self.connector.fetch_account_position.return_value, market_id=MARKET + 1)
        with self.assertRaises(HistorySchemaError):
            await self.port.position()

    async def test_book_prices_are_reference_only_and_fail_soft(self):
        book = MagicMock()
        book.get_price.side_effect = lambda is_buy: 5.41 if is_buy else 5.39
        self.connector.get_order_book = MagicMock(return_value=book)
        bid, ask = await self.port.best_bid_ask()
        self.assertEqual((Decimal("5.39"), Decimal("5.41")), (bid, ask))
        self.assertEqual(Decimal("5.40"), await self.port.mid_price())
        self.connector.get_order_book.side_effect = ValueError("no book")
        self.assertEqual((None, None), await self.port.best_bid_ask())
        self.assertIsNone(await self.port.mid_price())


if __name__ == "__main__":
    unittest.main()
