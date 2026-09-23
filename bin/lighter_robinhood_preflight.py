#!/usr/bin/env python
"""Read-only readiness checks for the Robinhood Lighter neutral grid.

This command deliberately has no transaction, cancellation, leverage, transfer, or
key-management capability. Public checks need no credentials. Authenticated checks
use an API private key only to create a short-lived auth token and perform reads.
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import inspect
import os
import platform
import re
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Protocol

import aiohttp
import yaml


DOMAIN = "lighter_perpetual_robinhood"
REST_URL = "https://api.rh.lighter.xyz"
WS_URL = "wss://api.rh.lighter.xyz/stream"
SIGNING_CHAIN_ID = 466324
SDK_MINIMUM = (1, 1, 4)
PAIR = "LIT-USDG"
USDG_ASSET_ID = 3
USDG_DECIMALS = 6
POSITION_CAP = Decimal("1000")
LEVERAGE = 5
ORDER_BOOK_DETAILS = "/api/v1/orderBookDetails"
ASSET_DETAILS = "/api/v1/assetDetails"
ACCOUNT = "/api/v1/account"
ACTIVE_ORDERS = "/api/v1/accountActiveOrders"


class PreflightError(RuntimeError):
    pass


@dataclass(frozen=True)
class Check:
    status: str
    name: str
    detail: str


@dataclass(frozen=True)
class Market:
    market_id: int
    min_base_amount: Decimal
    min_quote_amount: Decimal
    size_increment: Decimal
    price_increment: Decimal
    max_leverage: Decimal
    mark_price: Decimal


@dataclass
class PreflightCredentials:
    account_index: int
    api_key_index: int
    api_private_key: str = field(repr=False)
    l1_address: Optional[str] = None

    def validate(self) -> None:
        account_index = _exact_integer(self.account_index, "account_index")
        api_key_index = _exact_integer(self.api_key_index, "api_key_index")
        if account_index < 0 or api_key_index < 0:
            raise PreflightError("Preflight account and API key indexes must be nonnegative.")
        if not self.api_private_key.strip():
            raise PreflightError("Authenticated preflight is read-only but still requires an API private key.")


@dataclass(frozen=True)
class PrivateSnapshot:
    account_index: int
    key_association_valid: bool
    private_subscription_valid: bool
    available_usdg: Decimal
    net_position_lit: Decimal
    active_orders: List[Dict[str, Any]]


@dataclass
class PreflightReport:
    checks: List[Check] = field(default_factory=list)
    market: Optional[Market] = None
    config: Optional[Dict[str, Any]] = None
    private_checked: bool = False
    required_margin_usdg: Optional[Decimal] = None

    def add(self, status: str, name: str, detail: str) -> None:
        self.checks.append(Check(status=status, name=name, detail=detail))

    @property
    def public_ready(self) -> bool:
        public = [check for check in self.checks if not check.name.startswith("private.")]
        return bool(public) and all(check.status != "FAIL" for check in public)

    @property
    def live_ready(self) -> bool:
        return self.private_checked and bool(self.checks) and all(check.status == "PASS" for check in self.checks)

    def exit_code(self, public_only: bool = False) -> int:
        if not public_only:
            return 0 if self.live_ready else 1
        relevant = [check for check in self.checks if not check.name.startswith("private.")]
        return 1 if any(check.status == "FAIL" for check in relevant) else 0

    def summary(self) -> str:
        lines = [f"[{check.status}] {check.name}: {check.detail}" for check in self.checks]
        lines.append("PUBLIC CHECKS PASSED" if self.public_ready else "PUBLIC CHECKS FAILED")
        if self.live_ready:
            lines.append("LIVE READY: authenticated, configured, and read-only checks passed.")
        elif not self.private_checked:
            lines.append("NOT LIVE-READY: private account checks were not run.")
        else:
            lines.append("NOT LIVE-READY: one or more required checks are failed or incomplete.")
        return "\n".join(lines)


class PublicClient(Protocol):
    async def get_json(self, path: str) -> Dict[str, Any]:
        ...

    async def public_order_book_event(self, market_id: int, timeout: float) -> Dict[str, Any]:
        ...


def _decimal(value: Any, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise PreflightError(f"{label} is not a decimal") from exc
    if not result.is_finite():
        raise PreflightError(f"{label} must be finite")
    return result


_CANONICAL_INTEGER = re.compile(r"(?:0|-?[1-9][0-9]*)\Z")


def _exact_integer(value: Any, label: str) -> int:
    """Parse an external integer without accepting lossy Python coercions."""
    if isinstance(value, bool):
        raise PreflightError(f"{label} must be an exact integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and _CANONICAL_INTEGER.fullmatch(value):
        return int(value)
    raise PreflightError(f"{label} must be an exact integer")


def _success_code(payload: Dict[str, Any], label: str, required: bool = False) -> None:
    if "code" not in payload:
        if required:
            raise PreflightError(f"{label} lacks a valid success code")
        return
    try:
        code = _exact_integer(payload["code"], f"{label} success code")
    except PreflightError as exc:
        raise PreflightError(f"{label} lacks a valid success code") from exc
    if code != 200:
        raise PreflightError(f"{label} did not return success code 200")


def _sdk_tuple(value: str) -> tuple[int, ...]:
    parts = re.findall(r"\d+", value)
    return tuple(int(part) for part in parts[:3])


class SdkProbe:
    def probe(self) -> Dict[str, Any]:
        try:
            sdk_version = version("lighter-sdk")
            import lighter
            from lighter.signer_client import get_signer

            native = get_signer() is not None
            signature = inspect.signature(lighter.SignerClient)
            chain_id_supported = "chain_id" in signature.parameters
            return {
                "version": sdk_version,
                "native": native,
                "chain_id_supported": chain_id_supported,
                "platform": f"{platform.system()}/{platform.machine()}",
            }
        except (PackageNotFoundError, ImportError, OSError, AttributeError) as exc:
            return {"version": "missing", "native": False, "chain_id_supported": False, "error": str(exc)}


class AiohttpPublicClient:
    def __init__(self, session: aiohttp.ClientSession):
        self._session = session

    async def get_json(self, path: str) -> Dict[str, Any]:
        async with self._session.get(f"{REST_URL}{path}", timeout=aiohttp.ClientTimeout(total=15)) as response:
            response.raise_for_status()
            payload = await response.json()
        if not isinstance(payload, dict):
            raise PreflightError(f"Unexpected JSON response from {path}")
        return payload

    async def public_order_book_event(self, market_id: int, timeout: float) -> Dict[str, Any]:
        async with self._session.ws_connect(WS_URL, heartbeat=20, timeout=timeout) as ws:
            await ws.send_json({"type": "subscribe", "channel": f"order_book/{market_id}"})
            end = time.monotonic() + timeout
            while time.monotonic() < end:
                message = await ws.receive(timeout=max(0.1, end - time.monotonic()))
                if message.type == aiohttp.WSMsgType.TEXT:
                    payload = message.json()
                    if payload.get("error"):
                        raise PreflightError(f"Public WebSocket rejected subscription: {payload['error']}")
                    if not isinstance(payload, dict):
                        raise PreflightError("Public WebSocket event is not an object")
                    if payload.get("channel") == f"order_book:{market_id}":
                        payload["_received_at"] = time.time()
                        return payload
                if message.type in {aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED}:
                    break
        raise PreflightError("No fresh public order book event was received before timeout")


def _discover_lit(payload: Dict[str, Any]) -> Market:
    _success_code(payload, "Order-book metadata response", required=True)
    rows = payload.get("order_book_details")
    if not isinstance(rows, list) or not all(isinstance(raw, dict) for raw in rows):
        raise PreflightError("Order-book metadata has malformed order_book_details schema")
    candidates = []
    for raw in rows:
        market_id = _exact_integer(raw.get("market_id"), "market_id")
        if market_id < 0:
            raise PreflightError("market_id must be nonnegative")
        symbol = raw.get("symbol")
        if not isinstance(symbol, str):
            raise PreflightError("Market symbol must be a string")
        if symbol.upper() != "LIT":
            continue
        for field_name in ("base_asset_id", "quote_asset_id"):
            asset_reference = _exact_integer(raw.get(field_name), field_name)
            if asset_reference < 0:
                raise PreflightError(f"{field_name} must be nonnegative")
        config = raw.get("market_config")
        if not isinstance(config, dict):
            raise PreflightError("LIT market_config must be an object")
        if type(config.get("hidden")) is not bool or type(config.get("force_reduce_only")) is not bool:
            raise PreflightError("LIT hidden and force_reduce_only must be literal booleans")
        if (
            raw.get("market_type") == "perp"
            and raw.get("status") == "active"
            and config["hidden"] is False
            and config["force_reduce_only"] is False
        ):
            candidates.append(raw)
    if len(candidates) != 1:
        raise PreflightError(f"Expected one active LIT perpetual; discovered {len(candidates)}")
    raw = candidates[0]
    size_decimals = _exact_integer(raw.get("supported_size_decimals"), "supported_size_decimals")
    price_decimals = _exact_integer(raw.get("supported_price_decimals"), "supported_price_decimals")
    if not 0 <= size_decimals <= 18 or not 0 <= price_decimals <= 18:
        raise PreflightError("Supported market decimals must be between 0 and 18")
    margin_fraction = _decimal(raw["min_initial_margin_fraction"], "min_initial_margin_fraction")
    if margin_fraction <= 0:
        raise PreflightError("LIT market has invalid initial-margin metadata")
    min_base_amount = _decimal(raw["min_base_amount"], "min_base_amount")
    min_quote_amount = _decimal(raw["min_quote_amount"], "min_quote_amount")
    max_leverage = Decimal("10000") / margin_fraction
    if min_base_amount <= 0 or min_quote_amount <= 0:
        raise PreflightError("LIT market minimum size and notional must be positive")
    if max_leverage < Decimal(LEVERAGE):
        raise PreflightError("LIT market does not support the required leverage 5")
    return Market(
        market_id=_exact_integer(raw["market_id"], "market_id"),
        min_base_amount=min_base_amount,
        min_quote_amount=min_quote_amount,
        size_increment=Decimal(1).scaleb(-size_decimals),
        price_increment=Decimal(1).scaleb(-price_decimals),
        max_leverage=max_leverage,
        mark_price=_decimal(raw.get("mark_price", "0"), "mark_price"),
    )


def _validate_usdg(payload: Dict[str, Any]) -> None:
    _success_code(payload, "Asset metadata response", required=True)
    rows = payload.get("asset_details")
    if not isinstance(rows, list) or not all(isinstance(asset, dict) for asset in rows):
        raise PreflightError("Asset metadata has malformed asset_details schema")
    parsed_assets = []
    for asset in rows:
        asset_id = _exact_integer(asset.get("asset_id"), "asset_id")
        if asset_id < 0:
            raise PreflightError("asset_id must be nonnegative")
        parsed_assets.append((asset_id, asset))
    assets = [asset for asset_id, asset in parsed_assets if asset_id == USDG_ASSET_ID]
    if len(assets) != 1:
        raise PreflightError("Expected exactly one USDG asset 3")
    asset = assets[0]
    symbol = asset.get("symbol")
    if (
        not isinstance(symbol, str)
        or symbol.upper() != "USDG"
        or _exact_integer(asset.get("decimals"), "asset decimals") != USDG_DECIMALS
        or asset.get("margin_mode") != "enabled"
    ):
        raise PreflightError(
            "USDG asset 3 must have 6 decimals and enabled margin; "
            "quote_asset_id=0 is synthetic and cannot identify collateral"
        )


def _event_age(event: Dict[str, Any], now: float) -> float:
    if "_received_at" in event:
        received_at = _decimal(event["_received_at"], "WebSocket receive timestamp")
        return max(0.0, now - float(received_at))
    raw = _decimal(event.get("timestamp", event.get("timestamp_ms")), "WebSocket timestamp")
    if raw >= Decimal("1e15"):
        raw /= Decimal("1e6")
    elif raw >= Decimal("1e12"):
        raw /= Decimal("1e3")
    return float("inf") if raw <= 0 else max(0.0, now - float(raw))


def _validate_book(event: Dict[str, Any], market_id: int, now: float, max_age: float) -> None:
    if not isinstance(event, dict) or event.get("channel") != f"order_book:{market_id}":
        raise PreflightError("WebSocket event came from the wrong market or domain")
    book = event.get("order_book", event)
    if not isinstance(book, dict):
        raise PreflightError("LIT WebSocket order book has malformed schema")
    asks, bids = book.get("asks"), book.get("bids")
    if (not isinstance(asks, list) or not asks or not all(isinstance(level, dict) for level in asks)
            or not isinstance(bids, list) or not bids or not all(isinstance(level, dict) for level in bids)):
        raise PreflightError("LIT WebSocket order book has no two-sided liquidity")
    for side, levels in (("ask", asks), ("bid", bids)):
        if _decimal(levels[0].get("price"), f"best {side} price") <= 0:
            raise PreflightError(f"Best {side} price must be positive")
    age = _event_age(event, now)
    if age > max_age:
        raise PreflightError(f"LIT WebSocket order book is stale ({age:.1f}s > {max_age:.1f}s)")


def _multiple(value: Decimal, increment: Decimal) -> bool:
    return value > 0 and value % increment == 0


def _validate_config(config: Dict[str, Any], market: Market, report: PreflightReport) -> None:
    required_exact = {
        "connector_name": DOMAIN,
        "trading_pair": PAIR,
    }
    for field_name, expected in required_exact.items():
        actual = config.get(field_name)
        if isinstance(expected, Decimal):
            try:
                actual = _decimal(actual, field_name)
            except PreflightError as exc:
                report.add("FAIL", "config", str(exc))
                continue
        if actual != expected:
            detail = f"{field_name} must be {expected}"
            report.add("FAIL", "config", detail)

    parsed_integers: Dict[str, int] = {}
    for field_name, expected in {"leverage": LEVERAGE, "max_open_orders": 2}.items():
        try:
            actual = _exact_integer(config.get(field_name), field_name)
            parsed_integers[field_name] = actual
            if actual != expected:
                report.add("FAIL", "config", f"{field_name} must be {expected}")
        except PreflightError as exc:
            report.add("FAIL", "config", str(exc))

    grid_levels = config.get("grid_levels")
    if type(grid_levels) is not int:
        report.add("FAIL", "config", "grid_levels must be an exact integer")
    else:
        parsed_integers["grid_levels"] = grid_levels

    configured_cap = None
    try:
        configured_cap = _decimal(config.get("max_abs_net_position"), "max_abs_net_position")
        if configured_cap <= 0 or configured_cap > POSITION_CAP:
            report.add("FAIL", "config", "max_abs_net_position must preserve the 1,000 LIT cap")
    except PreflightError as exc:
        report.add("FAIL", "config", str(exc))

    enabled = config.get("enabled")
    if type(enabled) is not bool:
        report.add("FAIL", "config.enabled", "enabled must be a literal boolean")
    elif enabled is not True:
        report.add("INCOMPLETE", "config.enabled", "enabled=false; deliberate operator enablement is still required")
    else:
        report.add("PASS", "config.enabled", "enabled=true was explicitly configured")

    reserve_raw = config.get("margin_reserve_usdg")
    if reserve_raw in (None, ""):
        report.add("INCOMPLETE", "config.reserve", "An explicit margin reserve is required before live readiness")
    else:
        try:
            reserve = _decimal(reserve_raw, "margin_reserve_usdg")
            if reserve < 0:
                report.add("FAIL", "config.reserve", "margin_reserve_usdg must be nonnegative")
        except PreflightError as exc:
            report.add("FAIL", "config.reserve", str(exc))

    lower_raw, upper_raw = config.get("lower_price"), config.get("upper_price")
    if lower_raw in (None, "") or upper_raw in (None, ""):
        report.add("INCOMPLETE", "config.bounds", "Price bounds are incomplete; enter lower_price and upper_price")
        return
    try:
        lower = _decimal(lower_raw, "lower_price")
        upper = _decimal(upper_raw, "upper_price")
        amount = _decimal(config.get("order_amount_base"), "order_amount_base")
    except PreflightError as exc:
        report.add("FAIL", "config.bounds", str(exc))
        return
    if lower <= 0 or upper <= lower:
        report.add("FAIL", "config.bounds", "lower_price must be positive and less than upper_price")
    if not _multiple(lower, market.price_increment) or not _multiple(upper, market.price_increment):
        report.add("FAIL", "config.quantization", f"Bounds must match the {market.price_increment} price increment")
    if not _multiple(amount, market.size_increment):
        report.add("FAIL", "config.quantization", f"order_amount_base must match the {market.size_increment} size increment")
    if amount < market.min_base_amount:
        report.add("FAIL", "config.minimums", f"Order amount is below the {market.min_base_amount} minimum size")
    if amount * lower < market.min_quote_amount:
        report.add("FAIL", "config.minimums", f"Order at lower bound is below the {market.min_quote_amount} minimum notional")
    if configured_cap is not None and amount > configured_cap:
        report.add("FAIL", "config.exposure", "order_amount_base must not exceed the configured position cap")
    levels = parsed_integers.get("grid_levels")
    if levels is not None and levels < 2:
        report.add("FAIL", "config.grid", "grid_levels must be at least 2")
    elif levels is not None and upper > lower:
        first_tick = (lower / market.price_increment).to_integral_value(rounding=ROUND_CEILING)
        last_tick = (upper / market.price_increment).to_integral_value(rounding=ROUND_FLOOR)
        distinct_ticks = max(0, int(last_tick - first_tick + 1))
        if levels > distinct_ticks:
            report.add(
                "FAIL", "config.grid",
                f"grid_levels exceeds the {distinct_ticks} distinct exchange price ticks within bounds",
            )
    leverage = parsed_integers.get("leverage")
    if leverage is not None and Decimal(leverage) > market.max_leverage:
        report.add("FAIL", "config.leverage", f"Configured leverage exceeds market maximum leverage {market.max_leverage}")
    for field_name in ("refresh_seconds", "max_data_age_seconds"):
        try:
            if _decimal(config.get(field_name), field_name) <= 0:
                report.add("FAIL", "config.timing", f"{field_name} must be positive")
        except PreflightError as exc:
            report.add("FAIL", "config.timing", str(exc))
    if not any(check.status != "PASS" and check.name.startswith("config") for check in report.checks):
        report.add("PASS", "config.validation", "Bounds, size, cap, leverage, and exchange minimums are valid")


async def run_public_preflight(
    client: PublicClient,
    sdk_probe: Any,
    config: Optional[Dict[str, Any]] = None,
    max_data_age: float = 10,
    now: Optional[float] = None,
) -> PreflightReport:
    report = PreflightReport(config=config)
    report.add("PASS", "domain", f"{DOMAIN}: {REST_URL}, {WS_URL}, signing chain {SIGNING_CHAIN_ID}")
    try:
        max_age = _decimal(max_data_age, "max_data_age")
        if max_age <= 0:
            raise PreflightError("max_data_age must be positive")
        sdk = sdk_probe.probe()
        sdk_ok = _sdk_tuple(str(sdk.get("version", "0"))) >= SDK_MINIMUM
        if not sdk_ok or sdk.get("native") is not True or sdk.get("chain_id_supported") is not True:
            raise PreflightError(
                f"lighter-sdk >=1.1.4 with native signer and explicit chain_id is required; got {sdk.get('version')}"
            )
        report.add("PASS", "sdk", f"lighter-sdk {sdk['version']} native signer supports explicit chain_id")

        markets_payload, assets_payload = await asyncio.gather(
            client.get_json(ORDER_BOOK_DETAILS), client.get_json(ASSET_DETAILS)
        )
        market = _discover_lit(markets_payload)
        report.market = market
        report.add(
            "PASS",
            "market",
            f"Discovered active LIT perpetual market {market.market_id}; max leverage {market.max_leverage}",
        )
        _validate_usdg(assets_payload)
        report.add(
            "PASS",
            "asset",
            "USDG asset 3 has 6 decimals and enabled margin; quote_asset_id=0 is synthetic and was ignored",
        )
        event = await client.public_order_book_event(market.market_id, timeout=max(5.0, float(max_age)))
        _validate_book(event, market.market_id, now or time.time(), float(max_age))
        report.add("PASS", "websocket", f"Fresh two-sided LIT order book received for market {market.market_id}")
        if config is not None:
            _validate_config(config, market, report)
        else:
            report.add("INCOMPLETE", "config", "No strategy config was supplied; bounds and enabled state were not checked")
    except Exception as exc:
        report.add("FAIL", "public", redact(str(exc)))
    report.add("INCOMPLETE", "private.account", "Private account checks were not run")
    return report


def evaluate_private_snapshot(
    public_report: PreflightReport,
    credentials: PreflightCredentials,
    snapshot: PrivateSnapshot,
    margin_reserve_usdg: Optional[Decimal] = None,
) -> PreflightReport:
    credentials.validate()
    report = public_report
    report.checks = [check for check in report.checks if not check.name.startswith("private.")]
    report.private_checked = True
    try:
        snapshot_account_index = _exact_integer(snapshot.account_index, "private snapshot account_index")
    except PreflightError:
        snapshot_account_index = -1
    if (snapshot_account_index != credentials.account_index
            or snapshot.key_association_valid is not True):
        report.add("FAIL", "private.identity", "API key is not associated with the configured account index")
    else:
        report.add("PASS", "private.identity", f"API key association verified for account {snapshot.account_index}")
    if snapshot.private_subscription_valid is not True:
        report.add("FAIL", "private.websocket", "Authenticated private WebSocket subscription failed")
    else:
        report.add("PASS", "private.websocket", "Authenticated private WebSocket subscription succeeded")
    net_position = _decimal(snapshot.net_position_lit, "private net position")
    available_usdg = _decimal(snapshot.available_usdg, "private available USDG")
    if abs(net_position) > POSITION_CAP:
        report.add("FAIL", "private.position", f"Net position exceeds the {POSITION_CAP} LIT cap")
    else:
        report.add("PASS", "private.position", f"Net position {snapshot.net_position_lit} LIT is within cap")
    if not isinstance(snapshot.active_orders, list) or not all(
            isinstance(order, dict) for order in snapshot.active_orders):
        report.add("FAIL", "private.orders", "Active orders have malformed schema")
    elif snapshot.active_orders:
        report.add("FAIL", "private.orders", "Account has unexplained active orders; exclusive control is required")
    else:
        report.add("PASS", "private.orders", "No active orders were found")

    if report.config is None or report.market is None:
        report.add("INCOMPLETE", "private.margin", "Config bounds are required for conservative margin calculation")
    else:
        upper_raw = report.config.get("upper_price")
        if upper_raw in (None, ""):
            report.add("INCOMPLETE", "private.margin", "upper_price is required for conservative margin calculation")
        else:
            upper = _decimal(upper_raw, "upper_price")
            reserve_raw = margin_reserve_usdg
            if reserve_raw is None:
                reserve_raw = report.config.get("margin_reserve_usdg")
            if reserve_raw in (None, ""):
                report.add("INCOMPLETE", "private.margin", "An explicit margin reserve is required")
                return report
            reserve = _decimal(reserve_raw, "margin_reserve_usdg")
            if reserve < 0:
                report.add("FAIL", "private.margin", "margin_reserve_usdg must be nonnegative")
            else:
                required = upper * POSITION_CAP / Decimal(LEVERAGE) + reserve
                report.required_margin_usdg = required
                if available_usdg < 0:
                    report.add("FAIL", "private.margin", "Available USDG must be nonnegative")
                elif available_usdg < required:
                    report.add(
                        "FAIL",
                        "private.margin",
                        f"Available USDG {available_usdg} is below conservative requirement {required}",
                    )
                else:
                    report.add(
                        "PASS",
                        "private.margin",
                        f"Available USDG {available_usdg} covers conservative requirement {required}",
                    )
    return report


class AuthenticatedReadClient:
    """Restricted client exposing only the reads needed by preflight."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        credentials: PreflightCredentials,
        signer_factory: Optional[Any] = None,
    ):
        credentials.validate()
        self._session = session
        self._credentials = credentials
        self._signer_factory = signer_factory

    async def snapshot(self, market_id: int, timeout: float = 10) -> PrivateSnapshot:
        import lighter

        signer_factory = self._signer_factory or lighter.SignerClient
        try:
            signer = signer_factory(
                url=REST_URL,
                account_index=self._credentials.account_index,
                api_private_keys={self._credentials.api_key_index: self._credentials.api_private_key},
                chain_id=SIGNING_CHAIN_ID,
            )
        except Exception as exc:
            raise PreflightError(redact(exc, [self._credentials.api_private_key])) from None
        try:
            association_error = signer.check_client()
            token, token_error = signer.create_auth_token_with_expiry(
                deadline=600, api_key_index=self._credentials.api_key_index
            )
            if token_error is not None:
                raise PreflightError(f"Could not create read-only auth token: {token_error}")
            headers = {"authorization": token}
            account_params = {
                "by": "index",
                "value": self._credentials.account_index,
                "active_only": "true",
                "auth": token,
            }
            orders_params = {
                "account_index": self._credentials.account_index,
                "market_id": market_id,
                "auth": token,
            }
            account_payload, orders_payload = await asyncio.gather(
                self._read_json(ACCOUNT, account_params, headers),
                self._read_json(ACTIVE_ORDERS, orders_params, headers),
            )
            account = self._select_account(account_payload)
            orders = self._validate_orders(orders_payload)
            net_position = self._net_position(account, market_id)
            available_usdg = self._available_usdg(account)
            ws_valid = await self._private_subscription(token, timeout)
            return PrivateSnapshot(
                account_index=_exact_integer(
                    account.get("account_index", account.get("index")), "account_index"
                ),
                key_association_valid=association_error is None,
                private_subscription_valid=ws_valid,
                available_usdg=available_usdg,
                net_position_lit=net_position,
                active_orders=orders,
            )
        finally:
            await signer.close()

    async def _read_json(self, path: str, params: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
        async with self._session.get(
            f"{REST_URL}{path}", params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
        ) as response:
            response.raise_for_status()
            payload = await response.json()
        if not isinstance(payload, dict):
            raise PreflightError(f"Unexpected authenticated response from {path}")
        _success_code(payload, f"Authenticated response from {path}", required=True)
        return payload

    def _select_account(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        accounts = payload.get("accounts", payload.get("sub_accounts"))
        if not isinstance(accounts, list) or not all(isinstance(account, dict) for account in accounts):
            raise PreflightError("Authenticated account response has malformed accounts schema")
        matches = []
        for account in accounts:
            account_index = _exact_integer(
                account.get("account_index", account.get("index")), "account_index"
            )
            if account_index < 0:
                raise PreflightError("account_index must be nonnegative")
            if account_index == self._credentials.account_index:
                matches.append(account)
        if len(matches) != 1:
            raise PreflightError("Authenticated response did not identify exactly the configured account")
        account = matches[0]
        if self._credentials.l1_address:
            actual = str(account.get("l1_address", "")).lower()
            if actual != self._credentials.l1_address.lower():
                raise PreflightError("Configured L1 address does not match the selected account")
        return account

    @staticmethod
    def _non_negative_decimal(value: Any, label: str) -> Decimal:
        parsed = _decimal(value, label)
        if parsed < 0:
            raise PreflightError(f"{label} must be nonnegative")
        return parsed

    def _validate_orders(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        orders = payload.get("orders")
        if not isinstance(orders, list) or not all(isinstance(order, dict) for order in orders):
            raise PreflightError("Authenticated active orders response has malformed orders schema")
        return orders

    def _net_position(self, account: Dict[str, Any], market_id: int) -> Decimal:
        positions = account.get("positions")
        if not isinstance(positions, list) or not all(isinstance(position, dict) for position in positions):
            raise PreflightError("Authenticated account response has malformed positions schema")
        net_position = Decimal("0")
        for item in positions:
            try:
                item_market_id = _exact_integer(item["market_id"], "position market_id")
            except (KeyError, PreflightError) as exc:
                raise PreflightError("Authenticated position has invalid market_id") from exc
            if item_market_id < 0:
                raise PreflightError("Authenticated position has invalid market_id")
            amount = self._non_negative_decimal(item.get("position"), "position")
            try:
                sign = _exact_integer(item["sign"], "position sign")
            except (KeyError, PreflightError) as exc:
                raise PreflightError("Authenticated position has invalid sign") from exc
            if amount == 0:
                if sign not in (-1, 0, 1):
                    raise PreflightError("Zero-size authenticated position has invalid sign")
            elif sign not in (-1, 1):
                raise PreflightError("Nonzero authenticated position must have sign -1 or 1")
            if item_market_id == market_id:
                net_position += amount * Decimal(sign)
        return net_position

    def _available_usdg(self, account: Dict[str, Any]) -> Decimal:
        assets = account.get("assets")
        if not isinstance(assets, list) or not all(isinstance(asset, dict) for asset in assets):
            raise PreflightError("Authenticated account response has malformed assets schema")
        usdg_assets = []
        for asset in assets:
            asset_id = _exact_integer(asset.get("asset_id"), "asset_id")
            if asset_id < 0:
                raise PreflightError("asset_id must be nonnegative")
            symbol = asset.get("symbol")
            if not isinstance(symbol, str):
                raise PreflightError("Authenticated asset symbol must be a string")
            if asset_id == USDG_ASSET_ID and symbol.upper() == "USDG":
                usdg_assets.append(asset)
        if len(usdg_assets) != 1:
            raise PreflightError("Authenticated account must expose exactly one valid USDG asset 3 balance")
        usdg = usdg_assets[0]
        margin_balance = self._non_negative_decimal(usdg.get("margin_balance"), "USDG margin_balance")
        locked_balance = self._non_negative_decimal(usdg.get("locked_balance"), "USDG locked_balance")
        if locked_balance > margin_balance:
            raise PreflightError("USDG locked_balance exceeds margin_balance")
        account_available = self._non_negative_decimal(account.get("available_balance"), "available_balance")
        return min(account_available, margin_balance - locked_balance)

    async def _private_subscription(self, token: str, timeout: float) -> bool:
        account_index = self._credentials.account_index
        async with self._session.ws_connect(WS_URL, heartbeat=20, timeout=timeout) as ws:
            await ws.send_json(
                {"type": "subscribe", "channel": f"account_all_orders/{account_index}", "auth": token}
            )
            end = time.monotonic() + timeout
            while time.monotonic() < end:
                message = await ws.receive(timeout=max(0.1, end - time.monotonic()))
                if message.type == aiohttp.WSMsgType.TEXT:
                    payload = message.json()
                    if not isinstance(payload, dict):
                        raise PreflightError("Private WebSocket event is not an object")
                    if payload.get("error"):
                        raise PreflightError(f"Private WebSocket rejected subscription: {payload['error']}")
                    if payload.get("channel") == f"account_all_orders:{account_index}":
                        return True
                if message.type in {aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED}:
                    break
        return False


def redact(value: Any, secrets: Iterable[str] = ()) -> str:
    text = str(value)
    for secret in secrets:
        if secret:
            text = text.replace(str(secret), "[REDACTED]")
    patterns = (
        r"(?i)(authorization\s*[=:]\s*)(?:Bearer\s+)?[^\s&]+",
        r"(?i)([?&]auth=)[^&\s]+",
        r"(?i)(api_private_key\s*[=:]\s*)[^\s&]+",
    )
    for pattern in patterns:
        text = re.sub(pattern, r"\1[REDACTED]", text)
    return text


def load_config(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        payload = yaml.safe_load(stream)
    if not isinstance(payload, dict):
        raise PreflightError("Strategy config must be a YAML mapping")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only Robinhood Lighter readiness checks")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--public-only", action="store_true", help="Run public REST/WebSocket checks without keys")
    mode.add_argument("--authenticated", action="store_true", help="Also run authenticated account reads")
    parser.add_argument("--config", type=Path, help="Neutral-grid YAML to validate")
    parser.add_argument("--account-index", type=int, help="Required for authenticated checks")
    parser.add_argument("--api-key-index", type=int, default=0)
    parser.add_argument("--l1-address")
    parser.add_argument("--private-key-env", default="LIGHTER_API_PRIVATE_KEY")
    parser.add_argument("--margin-reserve-usdg", type=Decimal)
    parser.add_argument("--max-data-age", type=float, default=10)
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    config = load_config(args.config) if args.config else None
    secrets: List[str] = []
    async with aiohttp.ClientSession() as session:
        report = await run_public_preflight(
            AiohttpPublicClient(session), SdkProbe(), config=config, max_data_age=args.max_data_age
        )
        if args.authenticated and report.public_ready:
            if args.account_index is None:
                report.checks = [check for check in report.checks if not check.name.startswith("private.")]
                report.private_checked = True
                report.add("FAIL", "private.identity", "--account-index is required; no account was guessed")
            else:
                private_key = os.getenv(args.private_key_env) or getpass.getpass("Lighter API private key: ")
                secrets.append(private_key)
                credentials = PreflightCredentials(
                    account_index=args.account_index,
                    api_key_index=args.api_key_index,
                    api_private_key=private_key,
                    l1_address=args.l1_address,
                )
                try:
                    snapshot = await AuthenticatedReadClient(session, credentials).snapshot(report.market.market_id)
                    report = evaluate_private_snapshot(report, credentials, snapshot, args.margin_reserve_usdg)
                except Exception as exc:
                    report.checks = [check for check in report.checks if not check.name.startswith("private.")]
                    report.private_checked = True
                    report.add("FAIL", "private", redact(exc, secrets))
    print(redact(report.summary(), secrets))
    return report.exit_code(public_only=args.public_only)


def main() -> int:
    args = build_parser().parse_args()
    try:
        return asyncio.run(_async_main(args))
    except Exception as exc:
        print(f"[FAIL] preflight: {redact(exc)}", file=sys.stderr)
        print("NOT LIVE-READY", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
