from decimal import Decimal
from typing import Literal, Optional

from pydantic import ConfigDict, Field, SecretStr, field_validator, model_validator

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0"),
    buy_percent_fee_deducted_from_returns=False,
)

CENTRALIZED = False
EXAMPLE_PAIR = "ETH-USD"


def validate_non_negative_int(value) -> int:
    if value in (None, ""):
        return None
    ivalue = int(value)
    if ivalue < 0:
        raise ValueError("Value must be non-negative.")
    return ivalue


class LighterPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "lighter_perpetual"
    lighter_perpetual_l1_address: str = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter Perpetual L1 wallet address",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_account_index: Optional[int] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter your Lighter Perpetual account index (required for wallets with multiple accounts)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": False,
        },
    )
    lighter_perpetual_api_key_index: int = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter Perpetual API key index",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_api_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter Perpetual API private key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_account_limit: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter Perpetual account limit(Standard/Premium/Plus/Builder)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
            "default_value": "Standard",
        },
    )
    model_config = ConfigDict(title="lighter_perpetual")

    @field_validator("lighter_perpetual_account_index", "lighter_perpetual_api_key_index", mode="before")
    @classmethod
    def validate_indexes(cls, value):
        return validate_non_negative_int(value)


KEYS = LighterPerpetualConfigMap.model_construct()

OTHER_DOMAINS = ["lighter_perpetual_testnet", "lighter_perpetual_robinhood"]
OTHER_DOMAINS_PARAMETER = {
    "lighter_perpetual_testnet": "lighter_perpetual_testnet",
    "lighter_perpetual_robinhood": "lighter_perpetual_robinhood",
}
OTHER_DOMAINS_EXAMPLE_PAIR = {
    "lighter_perpetual_testnet": EXAMPLE_PAIR,
    "lighter_perpetual_robinhood": "LIT-USDG",
}
OTHER_DOMAINS_DEFAULT_FEES = {
    "lighter_perpetual_testnet": DEFAULT_FEES,
    "lighter_perpetual_robinhood": DEFAULT_FEES,
}


class LighterPerpetualTestnetConfigMap(BaseConnectorConfigMap):
    connector: str = "lighter_perpetual_testnet"
    lighter_perpetual_testnet_l1_address: str = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter Perpetual Testnet L1 wallet address",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_testnet_account_index: Optional[int] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter your Lighter Perpetual Testnet account index (required for wallets with multiple accounts)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": False,
        },
    )
    lighter_perpetual_testnet_api_key_index: int = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter Perpetual Testnet API key index",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_testnet_api_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter Perpetual Testnet API private key",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_testnet_account_limit: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter Perpetual Testnet account limit(Standard)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="lighter_perpetual_testnet")

    @field_validator(
        "lighter_perpetual_testnet_api_key_index",
        "lighter_perpetual_testnet_account_index",
        mode="before",
    )
    @classmethod
    def validate_indexes(cls, value):
        return validate_non_negative_int(value)


class LighterPerpetualRobinhoodConfigMap(BaseConnectorConfigMap):
    connector: str = "lighter_perpetual_robinhood"
    lighter_perpetual_robinhood_l1_address: Optional[str] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter your Robinhood Lighter L1 wallet address (optional with account index)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_robinhood_account_index: Optional[int] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter your Robinhood Lighter account index (required for wallets with multiple accounts)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_robinhood_api_key_index: int = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Robinhood Lighter API key index",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_robinhood_api_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Robinhood Lighter API private key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_robinhood_account_limit: Literal["Standard"] = Field(
        default="Standard",
        json_schema_extra={
            "prompt": "Robinhood Lighter account limit (Standard)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": False,
        },
    )
    model_config = ConfigDict(title="lighter_perpetual_robinhood")

    @field_validator(
        "lighter_perpetual_robinhood_account_index",
        "lighter_perpetual_robinhood_api_key_index",
        mode="before",
    )
    @classmethod
    def validate_indexes(cls, value):
        return validate_non_negative_int(value)

    @model_validator(mode="after")
    def validate_account_lookup(self):
        if not self.lighter_perpetual_robinhood_l1_address and self.lighter_perpetual_robinhood_account_index is None:
            raise ValueError("Robinhood Lighter requires an L1 address or account index.")
        return self


OTHER_DOMAINS_KEYS = {
    "lighter_perpetual_testnet": LighterPerpetualTestnetConfigMap.model_construct(),
    "lighter_perpetual_robinhood": LighterPerpetualRobinhoodConfigMap.model_construct(),
}
