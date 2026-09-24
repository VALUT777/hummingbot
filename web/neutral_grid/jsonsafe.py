"""JSON encoding that never loses precision on the way to a browser (AC-22, UI part).

Browsers parse JSON numbers as IEEE doubles, so any integer outside +/-(2**53 - 1) is silently
corrupted by ``JSON.parse``. The snapshot contract already stores decimals and ids as strings; this
module enforces that at the API boundary as a second line of defence:

* ``Decimal`` -> exact string;
* id-like keys (``cid``, ``*_id``, ``*_id_str``, ``nonce``, ``order_index`` ...) -> string, always;
* any other integer outside the JS safe range -> string;
* floats are kept only for timestamp/duration keys (seconds), everything else becomes a string so a
  float that slipped past the engine is visible instead of silently rounded again.
"""
from __future__ import annotations

import json
from decimal import Decimal
from enum import Enum
from typing import Any

JS_MAX_SAFE_INTEGER = (1 << 53) - 1

_ID_KEYS = frozenset({
    "cid", "id", "exchange_id", "client_order_id", "client_order_id_str", "order_id", "order_index",
    "nonce", "trade_id", "trade_id_str", "own_exchange_order_id", "own_client_order_id",
    "exchange_order_id", "ask_id_str", "bid_id_str", "ask_client_id_str", "bid_client_id_str",
    "command_id", "event_id", "account_index", "market_id", "cursor", "trades_cursor", "orders_cursor",
    "trades_high_water", "orders_high_water", "idempotency_key",
})
_TIME_KEYS = frozenset({
    "committed_at", "at", "created_at", "applied_at", "fetched_at", "served_at", "lag_s", "age_s",
    "queue_age_s", "last_full_scan_at", "stale_after_s", "timestamp", "updated_at", "expiry_at",
    "started_at", "ts", "last_commit_at", "last_latency_s", "max_latency_s", "slo_s", "baseline_confirmed_at",
})


def _is_id_key(key: str) -> bool:
    return key in _ID_KEYS or key.endswith("_id") or key.endswith("_id_str") or key.endswith("_cid")


def make_safe(value: Any, key: str = "") -> Any:
    """Return a JSON-ready copy of ``value`` with ids/decimals as strings (see module docstring)."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return format(value, "f") if value.is_finite() else str(value)
    if isinstance(value, int):
        if _is_id_key(key) or abs(value) > JS_MAX_SAFE_INTEGER:
            return str(value)
        return value
    if isinstance(value, float):
        if key in _TIME_KEYS:
            return value
        return repr(value)
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return {str(k): make_safe(v, str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [make_safe(item, key) for item in value]
    return str(value)


def dumps(value: Any) -> str:
    return json.dumps(make_safe(value), ensure_ascii=False, separators=(",", ":"), sort_keys=False)


def loads_exact(text: str) -> Any:
    """Parse JSON keeping every number exact (ints stay int, fractions become Decimal)."""
    return json.loads(text, parse_float=Decimal)


def canonical(value: Any) -> str:
    """Stable representation used to compare idempotent request replays."""
    return json.dumps(make_safe(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True)
