from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.derivative.lighter_perpetual import (
    lighter_perpetual_constants as CONSTANTS,
    lighter_perpetual_web_utils as web_utils,
)
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState


def exact_int(value: Any, field_name: str) -> int:
    """Parse an exchange integer without truncating floats, decimals, or booleans."""
    if type(value) is int:
        return value
    if isinstance(value, str):
        if value == "0" or (value.startswith("-") and value[1:].isdigit() and value[1:2] != "0"):
            return int(value)
        if value.isdigit() and not value.startswith("0"):
            return int(value)
    raise ValueError(f"{field_name} must be an exact integer")


def leverage_from_account_margin_percentage(value: Any) -> Optional[Decimal]:
    """Convert the account-position margin percentage (for example ``20.00``) to leverage."""
    try:
        percentage = Decimal(str(value))
    except Exception:
        return None
    if not percentage.is_finite() or percentage <= 0:
        return None
    return Decimal("100") / percentage


@dataclass(frozen=True)
class LighterMarketInfo:
    market_id: int
    exchange_symbol: str
    trading_pair: str
    base_asset: str
    quote_asset: str
    market_type: str
    min_base_amount: Decimal
    min_quote_amount: Decimal
    size_decimals: int
    price_decimals: int
    maker_fee: Decimal
    taker_fee: Decimal
    raw_info: Dict[str, Any]

    @property
    def max_leverage(self) -> Decimal:
        fraction = Decimal(str(self.raw_info.get("min_initial_margin_fraction", "0")))
        return Decimal("0") if fraction <= 0 else Decimal("10000") / fraction

    @property
    def min_base_increment(self) -> Decimal:
        return Decimal(f"1e-{self.size_decimals}")

    @property
    def min_price_increment(self) -> Decimal:
        return Decimal(f"1e-{self.price_decimals}")

    def trading_rule(self, collateral_token: Optional[str] = None) -> TradingRule:
        kwargs = {}
        if collateral_token is not None:
            kwargs.update(
                buy_order_collateral_token=collateral_token,
                sell_order_collateral_token=collateral_token,
            )
        return TradingRule(
            self.trading_pair,
            min_order_size=self.min_base_amount,
            min_base_amount_increment=self.min_base_increment,
            min_price_increment=self.min_price_increment,
            min_notional_size=self.min_quote_amount,
            **kwargs,
        )


def perpetual_markets_from_exchange_info(
    exchange_info: Dict[str, Any], domain: str = CONSTANTS.DOMAIN
) -> List[LighterMarketInfo]:
    quote_token = CONSTANTS.get_domain_settings(domain).quote_token
    markets = []
    for raw_market in exchange_info.get("order_book_details", []):
        if not web_utils.is_exchange_information_valid(raw_market):
            continue
        if raw_market.get("market_type", "perp") != "perp":
            continue
        base_asset = str(raw_market["symbol"]).upper()
        trading_pair = combine_to_hb_trading_pair(base=base_asset, quote=quote_token)
        markets.append(
            LighterMarketInfo(
                market_id=exact_int(raw_market["market_id"], "market_id"),
                exchange_symbol=base_asset,
                trading_pair=trading_pair,
                base_asset=base_asset,
                quote_asset=quote_token,
                market_type="perp",
                min_base_amount=Decimal(str(raw_market["min_base_amount"])),
                min_quote_amount=Decimal(str(raw_market["min_quote_amount"])),
                size_decimals=exact_int(raw_market["supported_size_decimals"], "supported_size_decimals"),
                price_decimals=exact_int(raw_market["supported_price_decimals"], "supported_price_decimals"),
                maker_fee=Decimal(str(raw_market["maker_fee"])),
                taker_fee=Decimal(str(raw_market["taker_fee"])),
                raw_info=raw_market,
            )
        )
    return markets


def markets_by_id(markets: Iterable[LighterMarketInfo]) -> Dict[int, LighterMarketInfo]:
    return {market.market_id: market for market in markets}


def markets_by_trading_pair(markets: Iterable[LighterMarketInfo]) -> Dict[str, LighterMarketInfo]:
    return {market.trading_pair: market for market in markets}


def markets_by_exchange_symbol(markets: Iterable[LighterMarketInfo]) -> Dict[str, LighterMarketInfo]:
    return {market.exchange_symbol: market for market in markets}


def trading_pair_symbol_map(markets: Iterable[LighterMarketInfo]) -> bidict[str, str]:
    mapping = bidict()
    for market in markets:
        mapping[market.exchange_symbol] = market.trading_pair
    return mapping


def decimal_to_exchange_int(value: Decimal, decimals: int) -> int:
    scaled = value * (Decimal(10) ** decimals)
    return int(scaled.to_integral_value())


def normalize_timestamp_to_seconds(timestamp: Any) -> float:
    """Convert a Lighter timestamp to seconds.

    Lighter mixes time units across fields — wall-clock fields such as ``timestamp`` /
    ``created_at`` / ``updated_at`` are milliseconds, while ``transaction_time`` is
    microseconds (verified against the live API) — so the unit is inferred from the value's
    magnitude rather than assumed, matching CandlesBase.ensure_timestamp_in_seconds.
    """
    value = float(timestamp or 0)
    if value <= 0:
        return 0.0
    if value >= 1e18:  # nanoseconds
        return value / 1e9
    if value >= 1e15:  # microseconds
        return value / 1e6
    if value >= 1e12:  # milliseconds
        return value / 1e3
    return value  # seconds (or sub-second synthetic values)


def next_funding_timestamp_seconds(last_funding_timestamp_ms: int) -> int:
    return int(last_funding_timestamp_ms / 1e3) + CONSTANTS.FUNDING_INTERVAL_SECONDS


def order_state_from_order_data(order_data: Dict[str, Any]) -> OrderState:
    status = str(order_data["status"])
    if status in CONSTANTS.OPEN_ORDER_STATES:
        filled_amount = Decimal(str(order_data.get("filled_base_amount", "0")))
        if filled_amount > Decimal("0"):
            return OrderState.PARTIALLY_FILLED
    return CONSTANTS.ORDER_STATE[status]


def account_index_from_account(account: Dict[str, Any]) -> int:
    return exact_int(
        account.get("account_index", account.get("accountIndex", account.get("index"))),
        "account_index",
    )


def extract_account_snapshot(
    account_response: Dict[str, Any], account_index: Optional[int] = None, l1_address: Optional[str] = None
) -> Dict[str, Any]:
    accounts = account_response.get("accounts", account_response.get("sub_accounts", []))
    if account_index is not None:
        for account in accounts:
            if account_index_from_account(account) == account_index:
                return account
    elif l1_address is not None:
        matches = [
            account for account in accounts
            if str(account.get("l1_address", "")).lower() == l1_address.lower()
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise IOError(
                f"L1 address {l1_address} has multiple Lighter accounts; configure an explicit account index."
            )
    raise IOError(f"Account {account_index or l1_address} was not found in Lighter account response.")


def own_trade_details(trade: Dict[str, Any], account_index: int) -> Optional[Tuple[TradeType, str, str, bool]]:
    ask_account_id = exact_int(trade.get("ask_account_id", -1), "ask_account_id")
    bid_account_id = exact_int(trade.get("bid_account_id", -1), "bid_account_id")
    if ask_account_id == account_index:
        return (
            TradeType.SELL,
            str(trade.get("ask_client_id_str", trade.get("ask_client_id", ""))),
            str(trade.get("ask_id_str", trade.get("ask_id", ""))),
            bool(trade.get("is_maker_ask", False)),
        )
    if bid_account_id == account_index:
        return (
            TradeType.BUY,
            str(trade.get("bid_client_id_str", trade.get("bid_client_id", ""))),
            str(trade.get("bid_id_str", trade.get("bid_id", ""))),
            not bool(trade.get("is_maker_ask", False)),
        )
    return None


# ---------------------------------------------------------------------------------------------
# Paginated authoritative history and pre-persisted client-id submission (neutral grid,
# spec NG-HIST-001..004 / NG-DB-005). Additive helpers; existing callers are unaffected.
# ---------------------------------------------------------------------------------------------


class LighterHistoryResponseError(IOError):
    """A history/active-orders response does not have the documented SDK 1.1.4 shape."""


@dataclass(frozen=True)
class LighterHistoryPage:
    """One raw page of ``/api/v1/trades`` or ``/api/v1/accountInactiveOrders``.

    ``rows`` are the raw JSON objects exactly as decoded (ids stay ``int``/``str``, never float);
    ``next_cursor`` is the response value passed through verbatim (possibly ``None``/``""`` or a
    malformed non-string, which the history scanner classifies); ``cursor_sent`` echoes the
    request cursor.
    """
    rows: List[Dict[str, Any]]
    next_cursor: Any
    cursor_sent: Optional[str]
    limit: Optional[int]


def history_page_from_response(
    response: Any, rows_field: str, cursor_sent: Optional[str], limit: Optional[int]
) -> LighterHistoryPage:
    if not isinstance(response, dict):
        raise LighterHistoryResponseError(f"Lighter {rows_field} response is not an object.")
    if "code" in response:
        try:
            code = exact_int(response["code"], "code")
        except (TypeError, ValueError):
            raise LighterHistoryResponseError(f"Lighter {rows_field} response has an invalid code.")
        if code != 200:
            raise LighterHistoryResponseError(f"Lighter {rows_field} response failed with code {code}.")
    rows = response.get(rows_field)
    if not isinstance(rows, list):
        raise LighterHistoryResponseError(f"Lighter {rows_field} response is missing the {rows_field} list.")
    if limit is not None and len(rows) > limit:
        raise LighterHistoryResponseError(f"Lighter {rows_field} page has {len(rows)} rows for limit {limit}.")
    if any(not isinstance(row, dict) for row in rows):
        raise LighterHistoryResponseError(f"Lighter {rows_field} page contains a malformed row.")
    return LighterHistoryPage(
        rows=list(rows), next_cursor=response.get("next_cursor"), cursor_sent=cursor_sent, limit=limit
    )


def validate_history_page_request(cursor: Optional[str], limit: int) -> None:
    if type(limit) is not int or not 1 <= limit <= CONSTANTS.HISTORY_PAGE_LIMIT_MAX:
        raise ValueError(f"History page limit must be an int in [1, {CONSTANTS.HISTORY_PAGE_LIMIT_MAX}].")
    if cursor is not None and not isinstance(cursor, str):
        raise ValueError("History cursor must be an opaque string or None.")


def exact_scaled_int(value: Any, decimals: int) -> Optional[int]:
    """Scale a Decimal to the venue integer representation, or None if that would round.

    Unlike :func:`decimal_to_exchange_int` this never quantizes silently (NG-GRID-003).
    """
    if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
        return None
    scaled = value.scaleb(decimals)
    integral = scaled.to_integral_value()
    if scaled != integral:
        return None
    return int(integral)


def strict_decimal(value: Any) -> Optional[Decimal]:
    """Decimal only from exact str/int sources (never float/bool); None if absent or not finite."""
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    try:
        parsed = Decimal(value)
    except Exception:
        return None
    return parsed if parsed.is_finite() else None


def strict_market_position(
    account: Dict[str, Any], market_id: int
) -> Tuple[Decimal, Optional[Decimal], Optional[str]]:
    """Signed net position, confirmed leverage and raw margin mode for one market (fail closed)."""
    raw_positions = account.get("positions")
    if not isinstance(raw_positions, list):
        raise IOError("Lighter account snapshot is missing positions data.")
    net_position = Decimal("0")
    leverage = None
    margin_mode = None
    for raw_position in raw_positions:
        if not isinstance(raw_position, dict):
            raise IOError("Lighter account snapshot contains a malformed position.")
        try:
            position_market_id = exact_int(raw_position["market_id"], "market_id")
        except (KeyError, TypeError, ValueError) as exc:
            raise IOError("Lighter account snapshot contains an invalid position market id.") from exc
        if position_market_id != market_id:
            continue
        size = strict_decimal(raw_position.get("position"))
        try:
            sign = exact_int(raw_position.get("sign"), "sign")
        except (TypeError, ValueError):
            sign = None
        if size is None or size < 0 or sign not in (-1, 0, 1) or (sign == 0 and size != 0):
            raise IOError("Lighter account snapshot contains an invalid position size or sign.")
        net_position += size * sign
        confirmed = leverage_from_account_margin_percentage(raw_position.get("initial_margin_fraction"))
        if confirmed is not None:
            leverage = confirmed
        raw_mode = raw_position.get("margin_mode")
        if type(raw_mode) in (int, str):
            margin_mode = str(raw_mode)
    return net_position, leverage, margin_mode


class LighterTransportOutcome(str, Enum):
    """Mirror of the neutral-grid ``TransportOutcome`` values, kept connector-local for layering."""
    NOT_SENT = "NOT_SENT"
    ACCEPTED = "ACCEPTED"
    DEFINITIVE_REJECT_ZERO_FILL = "DEFINITIVE_REJECT_ZERO_FILL"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class LighterTransportResult:
    outcome: LighterTransportOutcome
    detail: str = ""
    exchange_order_id: Optional[str] = None
    tx_hash: Optional[str] = None
