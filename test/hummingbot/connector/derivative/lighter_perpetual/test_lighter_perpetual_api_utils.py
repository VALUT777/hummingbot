import json
from decimal import Decimal
from pathlib import Path
from unittest import TestCase

from hummingbot.connector.derivative.lighter_perpetual import (
    lighter_perpetual_api_utils as utils,
    lighter_perpetual_constants as CONSTANTS,
    lighter_perpetual_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState


class LighterPerpetualApiUtilsTests(TestCase):
    def test_domain_settings_route_all_supported_domains_exactly(self):
        expected = {
            CONSTANTS.DOMAIN: ("https://mainnet.zklighter.elliot.ai", "wss://mainnet.zklighter.elliot.ai/stream", 304, "USDC"),
            CONSTANTS.TESTNET_DOMAIN: ("https://testnet.zklighter.elliot.ai", "wss://testnet.zklighter.elliot.ai/stream", 300, "USDC"),
            CONSTANTS.ROBINHOOD_DOMAIN: ("https://api.rh.lighter.xyz", "wss://api.rh.lighter.xyz/stream", 466324, "USDG"),
        }
        for domain, values in expected.items():
            settings = CONSTANTS.get_domain_settings(domain)
            self.assertEqual(values, (settings.rest_url, settings.ws_url, settings.chain_id, settings.quote_token))
            self.assertEqual(values[0], web_utils.rest_url(domain=domain))
            self.assertEqual(values[1], web_utils.wss_url(domain=domain))

    def test_unknown_domain_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unsupported Lighter perpetual domain"):
            CONSTANTS.get_domain_settings("lighter_perpetual_typo")
        with self.assertRaises(ValueError):
            web_utils.rest_url(domain="lighter_perpetual_typo")

    def test_perpetual_markets_from_exchange_info(self):
        exchange_info = {
            "order_book_details": [
                {
                    "symbol": "ETH",
                    "market_id": 5,
                    "status": "active",
                    "market_config": {"hidden": False},
                    "min_base_amount": "0.01",
                    "min_quote_amount": "10",
                    "supported_size_decimals": 3,
                    "supported_price_decimals": 2,
                    "maker_fee": "0.0001",
                    "taker_fee": "0.0004",
                }
            ]
        }

        markets = utils.perpetual_markets_from_exchange_info(exchange_info)

        self.assertEqual(1, len(markets))
        self.assertEqual("ETH", markets[0].exchange_symbol)
        self.assertEqual(f"ETH-{CONSTANTS.PERPETUAL_QUOTE_TOKEN}", markets[0].trading_pair)
        self.assertEqual(Decimal("0.0001"), markets[0].maker_fee)

    def test_robinhood_perpetual_market_uses_usdg_and_filters_non_perpetual_markets(self):
        exchange_info = {
            "order_book_details": [
                {
                    "symbol": "LIT", "market_id": 5, "market_type": "perp", "status": "active",
                    "market_config": {"hidden": False}, "min_base_amount": "5", "min_quote_amount": "10",
                    "supported_size_decimals": 2, "supported_price_decimals": 4,
                    "maker_fee": "0", "taker_fee": "0", "min_initial_margin_fraction": 2000,
                },
                {
                    "symbol": "USDG", "market_id": 3, "market_type": "spot", "status": "active",
                    "market_config": {"hidden": False}, "min_base_amount": "1", "min_quote_amount": "1",
                    "supported_size_decimals": 6, "supported_price_decimals": 6,
                    "maker_fee": "0", "taker_fee": "0",
                },
            ]
        }

        markets = utils.perpetual_markets_from_exchange_info(
            exchange_info, domain=CONSTANTS.ROBINHOOD_DOMAIN
        )

        self.assertEqual(1, len(markets))
        self.assertEqual("LIT-USDG", markets[0].trading_pair)
        self.assertEqual(Decimal("5"), markets[0].min_base_amount)
        self.assertEqual(Decimal("0.01"), markets[0].min_base_increment)
        self.assertEqual(Decimal("0.0001"), markets[0].min_price_increment)
        self.assertEqual(Decimal("5"), markets[0].max_leverage)

    def test_dated_robinhood_fixture_keeps_synthetic_quote_id_separate_from_usdg_asset(self):
        fixture_path = Path(__file__).parent / "fixtures" / "robinhood_mainnet_2026-09-23.json"
        fixture = json.loads(fixture_path.read_text())

        market = utils.perpetual_markets_from_exchange_info(
            fixture, domain=CONSTANTS.ROBINHOOD_DOMAIN
        )[0]
        usd_g = fixture["asset_details"][0]

        self.assertEqual(0, market.raw_info["quote_asset_id"])
        self.assertEqual((3, "USDG", 6, "enabled"), (
            usd_g["asset_id"], usd_g["symbol"], usd_g["decimals"], usd_g["margin_mode"]
        ))
        self.assertEqual("USDG", market.quote_asset)

    def test_order_state_from_order_data_partial_fill(self):
        order_data = {
            "status": "open",
            "filled_base_amount": "0.5",
        }

        order_state = utils.order_state_from_order_data(order_data)

        self.assertEqual(OrderState.PARTIALLY_FILLED, order_state)

    def test_own_trade_details_for_ask_and_bid(self):
        trade = {
            "ask_account_id": 10,
            "bid_account_id": 11,
            "ask_client_id_str": "a1",
            "ask_id_str": "o1",
            "bid_client_id_str": "b1",
            "bid_id_str": "o2",
            "is_maker_ask": True,
        }

        ask_details = utils.own_trade_details(trade=trade, account_index=10)
        bid_details = utils.own_trade_details(trade=trade, account_index=11)
        none_details = utils.own_trade_details(trade=trade, account_index=12)

        self.assertEqual((TradeType.SELL, "a1", "o1", True), ask_details)
        self.assertEqual((TradeType.BUY, "b1", "o2", False), bid_details)
        self.assertIsNone(none_details)

    def test_normalize_timestamp_to_seconds_infers_unit_from_magnitude(self):
        # Lighter mixes units: wall-clock fields are ms, transaction_time is us (live-API verified).
        self.assertAlmostEqual(1781056278.158, utils.normalize_timestamp_to_seconds("1781056278158"))       # ms
        self.assertAlmostEqual(1781056278.158263, utils.normalize_timestamp_to_seconds("1781056278158263"))  # us
        self.assertAlmostEqual(1781056278.0, utils.normalize_timestamp_to_seconds(1781056278))               # s
        self.assertEqual(0.0, utils.normalize_timestamp_to_seconds(None))

    def test_normalize_timestamp_milliseconds_not_parsed_as_1970(self):
        # Regression: an order's millisecond updated_at must not be read as microseconds (~1970).
        self.assertGreater(utils.normalize_timestamp_to_seconds("1640780000000"), 1_600_000_000)
