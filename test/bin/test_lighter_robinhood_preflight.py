import asyncio
import time
from copy import deepcopy
from decimal import Decimal

import pytest

from bin.lighter_robinhood_preflight import (
    AuthenticatedReadClient,
    PreflightCredentials,
    PreflightError,
    PrivateSnapshot,
    _exact_integer,
    evaluate_private_snapshot,
    redact,
    run_public_preflight,
)


MARKET = {
    "symbol": "LIT",
    "market_id": 5,
    "market_type": "perp",
    "base_asset_id": 0,
    "quote_asset_id": 0,
    "status": "active",
    "min_base_amount": "5.00",
    "min_quote_amount": "10.000000",
    "supported_size_decimals": 2,
    "supported_price_decimals": 4,
    "min_initial_margin_fraction": 2000,
    "market_config": {"hidden": False, "force_reduce_only": False},
}
USDG = {
    "asset_id": 3,
    "symbol": "USDG",
    "decimals": 6,
    "margin_mode": "enabled",
}
CONFIG = {
    "enabled": False,
    "connector_name": "lighter_perpetual_robinhood",
    "trading_pair": "LIT-USDG",
    "lower_price": "4.5000",
    "upper_price": "6.5000",
    "grid_levels": 21,
    "order_amount_base": "10",
    "max_abs_net_position": "1000",
    "leverage": 5,
    "max_open_orders": 2,
    "refresh_seconds": 30,
    "max_data_age_seconds": 10,
    "margin_reserve_usdg": "100",
}


class FakePublicClient:
    def __init__(self, market=None, asset=None, ws_age=1):
        self.market = deepcopy(market or MARKET)
        self.asset = deepcopy(asset or USDG)
        self.ws_age = ws_age

    async def get_json(self, path):
        if path.endswith("orderBookDetails"):
            return {"code": 200, "order_book_details": [self.market]}
        if path.endswith("assetDetails"):
            return {"code": 200, "asset_details": [self.asset]}
        raise AssertionError(f"unexpected read path: {path}")

    async def public_order_book_event(self, market_id, timeout):
        return {
            "channel": f"order_book:{market_id}",
            "type": "subscribed/order_book",
            "timestamp": int((time.time() - self.ws_age) * 1000),
            "order_book": {"asks": [{"price": "5.4"}], "bids": [{"price": "5.3"}]},
        }


class SuccessfulSdkProbe:
    def probe(self):
        return {"version": "1.1.4", "native": True, "chain_id_supported": True}


def run(coro):
    return asyncio.run(coro)


def test_public_success_discovers_market_and_remains_not_live_ready():
    report = run(run_public_preflight(FakePublicClient(), SuccessfulSdkProbe(), config=CONFIG))

    assert report.exit_code(public_only=True) == 0
    assert report.public_ready is True
    assert report.live_ready is False
    assert report.market.market_id == 5
    assert report.market.max_leverage == Decimal("5")
    assert "private account checks were not run" in report.summary().lower()


@pytest.mark.parametrize(
    "mutation, expected",
    [
        (lambda market: market.update(status="closed"), "active LIT perpetual"),
        (lambda market: market.update(market_type="spot"), "active LIT perpetual"),
        (lambda market: market["market_config"].update(hidden=True), "active LIT perpetual"),
    ],
)
def test_closed_or_malformed_market_fails(mutation, expected):
    market = deepcopy(MARKET)
    mutation(market)

    report = run(run_public_preflight(FakePublicClient(market=market), SuccessfulSdkProbe()))

    assert report.exit_code(public_only=True) == 1
    assert expected in report.summary()


def test_wrong_asset_or_domain_fails_closed():
    asset = deepcopy(USDG)
    asset["symbol"] = "USDC"

    report = run(run_public_preflight(FakePublicClient(asset=asset), SuccessfulSdkProbe()))

    assert report.exit_code(public_only=True) == 1
    assert "USDG asset 3" in report.summary()
    assert "quote_asset_id=0 is synthetic" in report.summary()


@pytest.mark.parametrize("value", [True, 5.0, 5.5, float("nan"), float("inf"), "05", "+5", "5.0", " 5"])
def test_exact_integer_rejects_coercible_or_noncanonical_values(value):
    with pytest.raises(PreflightError, match="exact integer"):
        _exact_integer(value, "identifier")


@pytest.mark.parametrize("value, expected", [(5, 5), ("5", 5), (-1, -1), ("-1", -1)])
def test_exact_integer_accepts_only_integers_and_canonical_integer_strings(value, expected):
    assert _exact_integer(value, "identifier") == expected


@pytest.mark.parametrize(
    "path, value",
    [
        (("market_id",), 5.5),
        (("supported_size_decimals",), 2.5),
        (("supported_price_decimals",), "04"),
        (("market_config", "hidden"), 0),
        (("market_config", "force_reduce_only"), "false"),
    ],
)
def test_public_market_rejects_permissive_numeric_and_boolean_coercions(path, value):
    market = deepcopy(MARKET)
    target = market
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value

    report = run(run_public_preflight(FakePublicClient(market=market), SuccessfulSdkProbe()))

    assert report.exit_code(public_only=True) == 1


@pytest.mark.parametrize("field, value", [("asset_id", 3.5), ("decimals", 6.5), ("asset_id", True)])
def test_public_asset_rejects_permissive_integer_coercions(field, value):
    asset = deepcopy(USDG)
    asset[field] = value

    report = run(run_public_preflight(FakePublicClient(asset=asset), SuccessfulSdkProbe()))

    assert report.exit_code(public_only=True) == 1


def test_public_websocket_channel_requires_exact_market_identity():
    class PrefixCollisionClient(FakePublicClient):
        async def public_order_book_event(self, market_id, timeout):
            event = await super().public_order_book_event(market_id, timeout)
            event["channel"] = f"order_book:{market_id}0"
            return event

    report = run(run_public_preflight(PrefixCollisionClient(), SuccessfulSdkProbe()))

    assert report.exit_code(public_only=True) == 1
    assert "wrong market" in report.summary().lower()


def test_public_response_code_rejects_fractional_success_value():
    class FractionalCodeClient(FakePublicClient):
        async def get_json(self, path):
            payload = await super().get_json(path)
            payload["code"] = 200.5
            return payload

    report = run(run_public_preflight(FractionalCodeClient(), SuccessfulSdkProbe()))

    assert report.exit_code(public_only=True) == 1
    assert "success code" in report.summary().lower()


def test_public_response_requires_application_success_code():
    class MissingCodeClient(FakePublicClient):
        async def get_json(self, path):
            payload = await super().get_json(path)
            payload.pop("code")
            return payload

    report = run(run_public_preflight(MissingCodeClient(), SuccessfulSdkProbe()))

    assert report.exit_code(public_only=True) == 1
    assert "success code" in report.summary().lower()


def test_stale_websocket_book_fails():
    report = run(run_public_preflight(FakePublicClient(ws_age=11), SuccessfulSdkProbe(), max_data_age=10))

    assert report.exit_code(public_only=True) == 1
    assert "stale" in report.summary().lower()


def test_missing_native_sdk_fails_before_connectivity_can_be_ready():
    class MissingSdk:
        def probe(self):
            return {"version": "1.1.4", "native": False, "chain_id_supported": False}

    report = run(run_public_preflight(FakePublicClient(), MissingSdk()))

    assert report.exit_code(public_only=True) == 1
    assert "native signer" in report.summary().lower()


@pytest.mark.parametrize(
    "changes, expected",
    [
        ({"lower_price": "6", "upper_price": "5"}, "lower_price"),
        ({"order_amount_base": "4.99"}, "minimum size"),
        ({"order_amount_base": "5", "lower_price": "1.5", "upper_price": "1.6"}, "minimum notional"),
        ({"lower_price": "5.00001"}, "price increment"),
        ({"leverage": 6}, "maximum leverage"),
        ({"max_abs_net_position": "1000.01"}, "1,000 LIT cap"),
    ],
)
def test_config_bounds_quantization_mins_and_cap_fail_closed(changes, expected):
    config = {**CONFIG, **changes}

    report = run(run_public_preflight(FakePublicClient(), SuccessfulSdkProbe(), config=config))

    assert report.exit_code(public_only=True) == 1
    assert expected in report.summary()


def test_nondefault_order_size_and_grid_levels_pass_public_config_validation():
    config = {**CONFIG, "order_amount_base": "25", "grid_levels": 9}

    report = run(run_public_preflight(FakePublicClient(), SuccessfulSdkProbe(), config=config))

    assert report.exit_code(public_only=True) == 0
    assert not any(check.status == "FAIL" and check.name.startswith("config") for check in report.checks)


@pytest.mark.parametrize(
    "changes, expected",
    [
        ({"grid_levels": 1}, "at least 2"),
        ({"grid_levels": 20002}, "distinct exchange price ticks"),
        ({"order_amount_base": "1000.01"}, "position cap"),
    ],
)
def test_dynamic_grid_parameters_respect_technical_and_exposure_bounds(changes, expected):
    report = run(run_public_preflight(
        FakePublicClient(), SuccessfulSdkProbe(), config={**CONFIG, **changes}
    ))

    assert report.exit_code(public_only=True) == 1
    assert expected in report.summary()


@pytest.mark.parametrize(
    "field, value",
    [
        ("grid_levels", 21.5),
        ("grid_levels", True),
        ("grid_levels", "21"),
        ("leverage", 5.5),
        ("max_open_orders", 2.0),
    ],
)
def test_config_integer_fields_reject_fractional_float_and_boolean_values(field, value):
    report = run(run_public_preflight(
        FakePublicClient(), SuccessfulSdkProbe(), config={**CONFIG, field: value}
    ))

    assert report.exit_code(public_only=True) == 1
    assert "exact integer" in report.summary()


def test_missing_keys_and_disabled_config_are_incomplete_not_public_failure():
    config = {**CONFIG, "lower_price": None, "upper_price": None}

    report = run(run_public_preflight(FakePublicClient(), SuccessfulSdkProbe(), config=config))

    assert report.exit_code(public_only=True) == 0
    assert report.public_ready is True
    assert report.live_ready is False
    assert "bounds are incomplete" in report.summary().lower()
    assert "enabled=false" in report.summary().lower()


def test_enabled_config_requires_an_explicit_margin_reserve():
    config = {**CONFIG, "enabled": True, "margin_reserve_usdg": None}

    report = run(run_public_preflight(FakePublicClient(), SuccessfulSdkProbe(), config=config))

    assert report.exit_code(public_only=True) == 0
    assert report.live_ready is False
    assert "margin reserve" in report.summary().lower()


def test_authenticated_read_only_snapshot_can_be_live_ready():
    credentials = PreflightCredentials(account_index=42, api_key_index=2, api_private_key="secret")
    snapshot = PrivateSnapshot(
        account_index=42,
        key_association_valid=True,
        private_subscription_valid=True,
        available_usdg=Decimal("1400"),
        net_position_lit=Decimal("250"),
        active_orders=[],
    )

    report = evaluate_private_snapshot(
        public_report=run(run_public_preflight(FakePublicClient(), SuccessfulSdkProbe(), config={**CONFIG, "enabled": True})),
        credentials=credentials,
        snapshot=snapshot,
        margin_reserve_usdg=Decimal("100"),
    )

    assert report.live_ready is True
    assert report.required_margin_usdg == Decimal("1400")
    assert "secret" not in report.summary()


def test_known_margin_shortfall_is_advisory_and_live_ready():
    config = {
        **CONFIG,
        "enabled": True,
        "lower_price": "5",
        "upper_price": "5.5",
        "grid_levels": 25,
        "margin_reserve_usdg": "1000",
    }
    credentials = PreflightCredentials(account_index=42, api_key_index=4, api_private_key="secret")
    snapshot = PrivateSnapshot(
        account_index=42,
        key_association_valid=True,
        private_subscription_valid=True,
        available_usdg=Decimal("1000"),
        net_position_lit=Decimal("0"),
        active_orders=[],
    )

    report = evaluate_private_snapshot(
        run(run_public_preflight(FakePublicClient(), SuccessfulSdkProbe(), config=config)),
        credentials,
        snapshot,
        Decimal("1000"),
    )

    margin = next(check for check in report.checks if check.name == "private.margin")
    assert margin.status == "WARN"
    assert "available=1000" in margin.detail
    assert "base=1100.0" in margin.detail
    assert "reserve=1000" in margin.detail
    assert "total=2100.0" in margin.detail
    assert report.live_ready is True
    assert report.exit_code(public_only=False) == 0
    assert "LIVE READY WITH MARGIN WARNING" in report.summary()


def test_margin_warning_does_not_override_active_orders_failure():
    config = {**CONFIG, "enabled": True, "margin_reserve_usdg": "1000"}
    credentials = PreflightCredentials(account_index=42, api_key_index=4, api_private_key="secret")
    snapshot = PrivateSnapshot(42, True, True, Decimal("1"), Decimal("0"), [{"order_id": 99}])

    report = evaluate_private_snapshot(
        run(run_public_preflight(FakePublicClient(), SuccessfulSdkProbe(), config=config)),
        credentials,
        snapshot,
        Decimal("1000"),
    )

    assert next(check for check in report.checks if check.name == "private.margin").status == "WARN"
    assert next(check for check in report.checks if check.name == "private.orders").status == "FAIL"
    assert report.live_ready is False


def test_arbitrary_warning_and_invalid_margin_data_still_block_readiness():
    report = run(run_public_preflight(
        FakePublicClient(), SuccessfulSdkProbe(), config={**CONFIG, "enabled": True}
    ))
    report.private_checked = True
    report.checks = [check for check in report.checks if not check.name.startswith("private.")]
    report.add("WARN", "private.position", "unexpected advisory")
    assert report.live_ready is False

    credentials = PreflightCredentials(account_index=42, api_key_index=4, api_private_key="secret")
    snapshot = PrivateSnapshot(42, True, True, Decimal("NaN"), Decimal("0"), [])
    malformed = evaluate_private_snapshot(report, credentials, snapshot, Decimal("100"))
    assert next(check for check in malformed.checks if check.name == "private.margin").status == "FAIL"
    assert malformed.live_ready is False


def test_authenticated_exit_code_requires_full_live_readiness():
    public_only = run(run_public_preflight(FakePublicClient(), SuccessfulSdkProbe(), config=CONFIG))

    assert public_only.exit_code(public_only=True) == 0
    assert public_only.exit_code(public_only=False) == 1


def test_authenticated_client_performs_reads_and_never_calls_mutations():
    class FakeResponse:
        def __init__(self, payload):
            self.payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def raise_for_status(self):
            return None

        async def json(self):
            return self.payload

    class FakeMessage:
        type = 1  # aiohttp.WSMsgType.TEXT

        def json(self):
            return {"channel": "account_all_orders:42", "type": "subscribed/account_all_orders"}

    class FakeWebSocket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def send_json(self, payload):
            return None

        async def receive(self, timeout):
            return FakeMessage()

    class FakeSession:
        def get(self, url, params, headers, timeout):
            if url.endswith("accountActiveOrders"):
                return FakeResponse({"code": 200, "orders": []})
            return FakeResponse({
                "code": 200,
                "accounts": [{
                    "index": 42,
                    "available_balance": "1400",
                    "assets": [{
                        "asset_id": 3,
                        "symbol": "USDG",
                        "margin_balance": "1500",
                        "locked_balance": "100",
                    }],
                    "positions": [{"market_id": 5, "position": "25", "sign": "-1"}],
                }]
            })

        def ws_connect(self, url, heartbeat, timeout):
            return FakeWebSocket()

    class ReadOnlySigner:
        def __init__(self, **kwargs):
            assert kwargs["chain_id"] == 466324

        def check_client(self):
            return None

        def create_auth_token_with_expiry(self, deadline, api_key_index):
            return "short-lived-token", None

        async def close(self):
            return None

        def create_order(self, *args, **kwargs):
            raise AssertionError("mutation called")

        def cancel_order(self, *args, **kwargs):
            raise AssertionError("mutation called")

        def update_leverage(self, *args, **kwargs):
            raise AssertionError("mutation called")

        def transfer(self, *args, **kwargs):
            raise AssertionError("mutation called")

    credentials = PreflightCredentials(account_index=42, api_key_index=2, api_private_key="secret")

    snapshot = run(AuthenticatedReadClient(FakeSession(), credentials, ReadOnlySigner).snapshot(5))

    assert snapshot.key_association_valid is True
    assert snapshot.private_subscription_valid is True
    assert snapshot.available_usdg == Decimal("1400")
    assert snapshot.net_position_lit == Decimal("-25")
    assert snapshot.active_orders == []


@pytest.mark.parametrize(
    "account_changes, orders_payload, expected",
    [
        ({"positions": None}, {"code": 200, "orders": []}, "positions"),
        ({"positions": {}}, {"code": 200, "orders": []}, "positions"),
        ({}, {"code": 200}, "orders"),
        ({}, {"code": 503, "orders": []}, "success code"),
        ({"positions": [{"market_id": 99, "position": "bad", "sign": "1"}]},
         {"code": 200, "orders": []}, "position"),
        ({"assets": [{
            "asset_id": 4, "symbol": "USDG", "margin_balance": "100", "locked_balance": "0"
        }]}, {"code": 200, "orders": []}, "USDG asset 3"),
        ({"assets": [{
            "asset_id": 3, "symbol": "USDG", "margin_balance": "100", "locked_balance": "101"
        }]}, {"code": 200, "orders": []}, "exceeds"),
    ],
)
def test_authenticated_client_rejects_http_200_missing_or_malformed_account_state(
    account_changes, orders_payload, expected
):
    class Response:
        def __init__(self, payload):
            self.payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def raise_for_status(self):
            return None

        async def json(self):
            return self.payload

    account = {
        "index": 42,
        "available_balance": "100",
        "assets": [{
            "asset_id": 3, "symbol": "USDG", "margin_balance": "100", "locked_balance": "0"
        }],
        "positions": [],
    }
    account.update(account_changes)

    class Session:
        def get(self, url, params, headers, timeout):
            payload = orders_payload if url.endswith("accountActiveOrders") else {
                "code": 200, "accounts": [account]
            }
            return Response(payload)

        def ws_connect(self, url, heartbeat, timeout):
            raise AssertionError("malformed REST state must fail before WebSocket")

    class Signer:
        def __init__(self, **kwargs):
            pass

        def check_client(self):
            return None

        def create_auth_token_with_expiry(self, deadline, api_key_index):
            return "token", None

        async def close(self):
            return None

    credentials = PreflightCredentials(account_index=42, api_key_index=2, api_private_key="secret")

    with pytest.raises(PreflightError, match=expected):
        run(AuthenticatedReadClient(Session(), credentials, Signer).snapshot(5))


def test_authenticated_client_accepts_flat_sign_zero_and_caps_margin_to_unlocked_usdg():
    class Response:
        def __init__(self, payload):
            self.payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def raise_for_status(self):
            return None

        async def json(self):
            return self.payload

    class Message:
        type = 1

        def json(self):
            return {"channel": "account_all_orders:42"}

    class WebSocket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def send_json(self, payload):
            return None

        async def receive(self, timeout):
            return Message()

    class Session:
        def get(self, url, params, headers, timeout):
            if url.endswith("accountActiveOrders"):
                return Response({"code": 200, "orders": []})
            return Response({"code": 200, "accounts": [{
                "index": 42,
                "available_balance": "900",
                "assets": [{
                    "asset_id": 3,
                    "symbol": "USDG",
                    "margin_balance": "600",
                    "locked_balance": "100",
                }],
                "positions": [{"market_id": 5, "position": "0", "sign": "0"}],
            }]})

        def ws_connect(self, url, heartbeat, timeout):
            return WebSocket()

    class Signer:
        def __init__(self, **kwargs):
            pass

        def check_client(self):
            return None

        def create_auth_token_with_expiry(self, deadline, api_key_index):
            return "token", None

        async def close(self):
            return None

    credentials = PreflightCredentials(account_index=42, api_key_index=2, api_private_key="secret")

    snapshot = run(AuthenticatedReadClient(Session(), credentials, Signer).snapshot(5))

    assert snapshot.net_position_lit == Decimal("0")
    assert snapshot.available_usdg == Decimal("500")


def test_native_signer_error_redacts_literal_private_key():
    literal_key = "literal-private-key"

    class FailingSigner:
        def __init__(self, **kwargs):
            raise RuntimeError(f"native signer rejected {literal_key}")

    credentials = PreflightCredentials(account_index=42, api_key_index=2, api_private_key=literal_key)

    with pytest.raises(PreflightError) as error:
        run(AuthenticatedReadClient(object(), credentials, FailingSigner).snapshot(5))

    assert literal_key not in str(error.value)
    assert "[REDACTED]" in str(error.value)


def _direct_authenticated_client(account_index=1):
    credentials = PreflightCredentials(account_index=account_index, api_key_index=2, api_private_key="secret")
    return AuthenticatedReadClient(object(), credentials, object())


@pytest.mark.parametrize("bad_index", [1.5, True, "01"])
def test_private_account_binding_rejects_non_exact_account_indexes(bad_index):
    client = _direct_authenticated_client(account_index=1)
    with pytest.raises(PreflightError, match="account_index"):
        client._select_account({"accounts": [{"account_index": bad_index}]})


@pytest.mark.parametrize(
    "position, expected",
    [
        ({"market_id": 5.5, "position": "0", "sign": 0}, "market_id"),
        ({"market_id": 5, "position": "1", "sign": 1.5}, "sign"),
        ({"market_id": 5, "position": "1", "sign": True}, "sign"),
    ],
)
def test_private_positions_reject_permissive_integer_coercions(position, expected):
    with pytest.raises(PreflightError, match=expected):
        _direct_authenticated_client()._net_position({"positions": [position]}, market_id=5)


@pytest.mark.parametrize("bad_asset_id", [3.5, True, "03"])
def test_private_usdg_binding_rejects_non_exact_asset_id(bad_asset_id):
    account = {
        "available_balance": "100",
        "assets": [{
            "asset_id": bad_asset_id,
            "symbol": "USDG",
            "margin_balance": "100",
            "locked_balance": "0",
        }],
    }
    with pytest.raises(PreflightError, match="asset_id"):
        _direct_authenticated_client()._available_usdg(account)


def test_authenticated_response_code_rejects_fractional_success_value():
    class Response:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def raise_for_status(self):
            return None

        async def json(self):
            return {"code": 200.5}

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    client = AuthenticatedReadClient(
        Session(), PreflightCredentials(account_index=1, api_key_index=2, api_private_key="secret"), object()
    )
    with pytest.raises(PreflightError, match="success code"):
        run(client._read_json("/read", {}, {}))


def test_private_websocket_channel_requires_exact_account_identity():
    class Message:
        def __init__(self, message_type, payload=None):
            self.type = message_type
            self._payload = payload

        def json(self):
            return self._payload

    class WebSocket:
        def __init__(self):
            self.messages = [
                Message(1, {"channel": "account_all_orders:10"}),
                Message(257),  # aiohttp.WSMsgType.CLOSED
            ]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def send_json(self, payload):
            return None

        async def receive(self, timeout):
            return self.messages.pop(0)

    class Session:
        def ws_connect(self, *args, **kwargs):
            return WebSocket()

    credentials = PreflightCredentials(account_index=1, api_key_index=2, api_private_key="secret")
    client = AuthenticatedReadClient(Session(), credentials, object())

    assert run(client._private_subscription("token", 1)) is False


def test_authenticated_snapshot_rejects_unknown_orders_and_position_breach():
    public = run(run_public_preflight(FakePublicClient(), SuccessfulSdkProbe(), config={**CONFIG, "enabled": True}))
    credentials = PreflightCredentials(account_index=42, api_key_index=2, api_private_key="secret")
    snapshot = PrivateSnapshot(
        account_index=42,
        key_association_valid=True,
        private_subscription_valid=True,
        available_usdg=Decimal("99999"),
        net_position_lit=Decimal("1000.01"),
        active_orders=[{"order_id": 99}],
    )

    report = evaluate_private_snapshot(public, credentials, snapshot, Decimal("0"))

    assert report.live_ready is False
    assert "position exceeds" in report.summary().lower()
    assert "active orders" in report.summary().lower()


def test_redaction_removes_secret_tokens_from_errors_and_urls():
    text = "authorization=Bearer abc.def&auth=token123 api_private_key=0xdeadbeef"

    redacted = redact(text, secrets=["token123", "0xdeadbeef"])

    assert "abc.def" not in redacted
    assert "token123" not in redacted
    assert "0xdeadbeef" not in redacted
    assert "[REDACTED]" in redacted


def test_preflight_surface_has_no_mutation_methods():
    from bin import lighter_robinhood_preflight as preflight

    forbidden = {"send_tx", "cancel_order", "create_order", "update_leverage", "transfer", "change_pub_key"}
    public_names = {name.lower() for name in dir(preflight) if not name.startswith("_")}

    assert forbidden.isdisjoint(public_names)
    with pytest.raises(PreflightError, match="read-only"):
        PreflightCredentials(account_index=1, api_key_index=1, api_private_key="").validate()
