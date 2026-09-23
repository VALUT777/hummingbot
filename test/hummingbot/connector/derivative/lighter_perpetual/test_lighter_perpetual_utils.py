from unittest import TestCase

from pydantic import ValidationError

from hummingbot.client.settings import AllConnectorSettings, ConnectorSetting, ConnectorType
from hummingbot.connector.derivative.lighter_perpetual import (
    lighter_perpetual_api_utils as api_utils,
    lighter_perpetual_utils as utils,
)


class LighterPerpetualUtilsTests(TestCase):
    def test_validate_non_negative_int(self):
        self.assertEqual(7, utils.validate_non_negative_int("7"))
        self.assertIsNone(utils.validate_non_negative_int(""))

    def test_validate_non_negative_int_raises_for_negative(self):
        with self.assertRaises(ValueError):
            utils.validate_non_negative_int(-1)

    def test_mainnet_config_map_accepts_explicit_account_index(self):
        config = utils.LighterPerpetualConfigMap(
            lighter_perpetual_l1_address="0xabc",
            lighter_perpetual_account_index=0,
            lighter_perpetual_api_key_index="8",
            lighter_perpetual_api_private_key="0xabc",
            lighter_perpetual_account_limit="Starter",
        )

        self.assertEqual("0xabc", config.lighter_perpetual_l1_address)
        self.assertEqual(0, config.lighter_perpetual_account_index)
        self.assertEqual(8, config.lighter_perpetual_api_key_index)

    def test_testnet_config_map_accepts_explicit_account_index(self):
        config = utils.LighterPerpetualTestnetConfigMap(
            lighter_perpetual_testnet_l1_address="0xabc",
            lighter_perpetual_testnet_account_index=0,
            lighter_perpetual_testnet_api_key_index="1",
            lighter_perpetual_testnet_api_private_key="0xdef",
            lighter_perpetual_testnet_account_limit="Starter",
        )

        self.assertEqual("0xabc", config.lighter_perpetual_testnet_l1_address)
        self.assertEqual(0, config.lighter_perpetual_testnet_account_index)
        self.assertEqual(1, config.lighter_perpetual_testnet_api_key_index)

    def test_other_domains_settings_include_testnet(self):
        self.assertIn("lighter_perpetual_testnet", utils.OTHER_DOMAINS)
        self.assertIn("lighter_perpetual_testnet", utils.OTHER_DOMAINS_DEFAULT_FEES)

    def test_robinhood_config_is_secure_and_defaults_to_standard(self):
        config = utils.LighterPerpetualRobinhoodConfigMap(
            lighter_perpetual_robinhood_l1_address="0xabc",
            lighter_perpetual_robinhood_account_index=0,
            lighter_perpetual_robinhood_api_key_index=7,
            lighter_perpetual_robinhood_api_private_key="secret",
        )

        self.assertEqual("lighter_perpetual_robinhood", config.connector)
        self.assertEqual(0, config.lighter_perpetual_robinhood_account_index)
        self.assertEqual("Standard", config.lighter_perpetual_robinhood_account_limit)
        self.assertTrue(
            type(config).model_fields["lighter_perpetual_robinhood_api_private_key"]
            .json_schema_extra["is_secure"]
        )
        self.assertNotIn("wallet_private_key", type(config).model_fields)

    def test_robinhood_account_limit_rejects_non_standard_tier(self):
        with self.assertRaises(ValidationError):
            utils.LighterPerpetualRobinhoodConfigMap(
                lighter_perpetual_robinhood_l1_address="0xabc",
                lighter_perpetual_robinhood_account_index=0,
                lighter_perpetual_robinhood_api_key_index=7,
                lighter_perpetual_robinhood_api_private_key="secret",
                lighter_perpetual_robinhood_account_limit="Premium",
            )

    def test_robinhood_config_requires_wallet_or_account_index(self):
        with self.assertRaisesRegex(ValidationError, "L1 address or account index"):
            utils.LighterPerpetualRobinhoodConfigMap(
                lighter_perpetual_robinhood_api_key_index=7,
                lighter_perpetual_robinhood_api_private_key="secret",
            )

    def test_robinhood_other_domain_registration_and_constructor_remapping(self):
        domain = "lighter_perpetual_robinhood"
        self.assertIn(domain, utils.OTHER_DOMAINS)
        self.assertEqual(domain, utils.OTHER_DOMAINS_PARAMETER[domain])
        self.assertEqual("LIT-USDG", utils.OTHER_DOMAINS_EXAMPLE_PAIR[domain])
        self.assertEqual(domain, utils.OTHER_DOMAINS_KEYS[domain].connector)

    def test_connector_setting_remaps_robinhood_credentials_to_parent_constructor(self):
        domain = "lighter_perpetual_robinhood"
        setting = ConnectorSetting(
            name=domain,
            type=ConnectorType.Derivative,
            example_pair="LIT-USDG",
            centralised=False,
            use_ethereum_wallet=False,
            trade_fee_schema=utils.DEFAULT_FEES,
            config_keys=utils.OTHER_DOMAINS_KEYS[domain],
            is_sub_domain=True,
            parent_name="lighter_perpetual",
            domain_parameter=domain,
            use_eth_gas_lookup=False,
        )
        params = setting.conn_init_parameters(
            api_keys={
                "lighter_perpetual_robinhood_account_index": 0,
                "lighter_perpetual_robinhood_api_key_index": 7,
                "lighter_perpetual_robinhood_api_private_key": "secret",
            }
        )

        self.assertEqual(0, params["lighter_perpetual_account_index"])
        self.assertEqual(7, params["lighter_perpetual_api_key_index"])
        self.assertEqual("secret", params["lighter_perpetual_api_private_key"])
        self.assertEqual(domain, params["domain"])

    def test_connector_discovery_includes_robinhood_domain(self):
        setting = AllConnectorSettings.get_connector_settings()["lighter_perpetual_robinhood"]

        self.assertTrue(setting.is_sub_domain)
        self.assertEqual("lighter_perpetual", setting.parent_name)
        self.assertEqual("lighter_perpetual_robinhood", setting.domain_parameter)

    def test_extract_account_snapshot_by_l1_address_from_sub_accounts_response(self):
        response = {
            "code": 200,
            "l1_address": "0xe34167D92340c95A7775495d78bcc3Dc21cf11c0",
            "sub_accounts": [
                {
                    "code": 0,
                    "account_type": 0,
                    "index": 724450,
                    "l1_address": "0xe34167D92340c95A7775495d78bcc3Dc21cf11c0",
                    "available_balance": "",
                    "collateral": "50.000000",
                }
            ],
        }

        account = api_utils.extract_account_snapshot(
            response, l1_address="0xe34167D92340c95A7775495d78bcc3Dc21cf11c0"
        )

        self.assertEqual(724450, api_utils.account_index_from_account(account))

    def test_extract_account_snapshot_rejects_ambiguous_l1_accounts(self):
        response = {
            "accounts": [
                {"account_index": 0, "l1_address": "0xabc"},
                {"account_index": 1, "l1_address": "0xabc"},
            ]
        }

        with self.assertRaisesRegex(IOError, "multiple"):
            api_utils.extract_account_snapshot(response, l1_address="0xabc")
