"""Read-only data composition for the operator terminal.

The terminal joins one committed engine snapshot with confirmed fills read as of that
snapshot.  Public candles are presentation context only and never participate in
engine decisions.
"""
from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Dict, Optional

import aiohttp

from web.neutral_grid import views
from web.neutral_grid.gateway import SnapshotUnavailable


CANDLE_URL = "https://api.rh.lighter.xyz/api/v1/candles"
ALLOWED_CONNECTOR = "lighter_perpetual_robinhood"
ALLOWED_PAIR = "LIT-USDG"
MARKET_ID = 5
INTERVAL_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400}
MAX_CANDLE_BYTES = 2 * 1024 * 1024
FINAL_ORDER_STATES = frozenset({"TERMINAL", "REJECTED_UNSENT", "REJECTED_ZERO_FILL"})


def _text(value: Any) -> Optional[str]:
    return None if value is None else str(value)


def _decimal_text(value: Any) -> str:
    if value is None or isinstance(value, bool):
        raise ValueError("missing candle decimal")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError("bad candle decimal") from None
    if not parsed.is_finite():
        raise ValueError("non-finite candle decimal")
    return format(parsed, "f")


def _unavailable(interval: str, reason: str, *, cached: bool = False) -> Dict[str, Any]:
    return {
        "source": "unavailable", "trading_pair": ALLOWED_PAIR, "market_id": str(MARKET_ID),
        "interval": interval, "candles": [], "fetched_at": None, "cached": cached,
        "unavailable_reason": reason,
    }


class DemoCandleProvider:
    """Deterministic, explicitly labelled offline fixture."""

    requires_committed_identity = False

    def __init__(self, clock: Callable[[], float] = time.time):
        self._clock = clock

    async def get(self, *, identity: Dict[str, Any], interval: str, limit: int) -> Dict[str, Any]:
        step = INTERVAL_SECONDS[interval]
        end = int(self._clock() // step) * step
        count = min(limit, 72)
        candles = []
        for index in range(count):
            offset = Decimal((index % 12) - 6) / Decimal("100")
            opened = Decimal("5.40") + offset
            closed = opened + (Decimal("0.006") if index % 2 else Decimal("-0.004"))
            candles.append({
                "time": end - (count - 1 - index) * step,
                "open": format(opened, "f"), "high": format(max(opened, closed) + Decimal("0.008"), "f"),
                "low": format(min(opened, closed) - Decimal("0.008"), "f"), "close": format(closed, "f"),
                "volume": format(Decimal(1000 + index * 17), "f"),
            })
        return {
            "source": "demo_fixture", "trading_pair": ALLOWED_PAIR, "market_id": str(MARKET_ID),
            "interval": interval, "candles": candles, "fetched_at": self._clock(), "cached": False,
            "unavailable_reason": None,
        }


class PublicCandleProvider:
    """Bounded unauthenticated Lighter Robinhood candle reader with failure caching."""

    requires_committed_identity = True

    def __init__(self, *, request: Optional[Callable[..., Awaitable[Dict[str, Any]]]] = None,
                 clock: Callable[[], float] = time.time, ttl_s: float = 10.0, max_cache_entries: int = 12):
        self._request = request or self._http_request
        self._clock = clock
        self._ttl_s = ttl_s
        self._max_cache_entries = max_cache_entries
        self._cache: Dict[tuple, tuple] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    async def _http_request(url: str, params: Dict[str, Any], timeout_s: float, max_bytes: int) -> Dict[str, Any]:
        timeout = aiohttp.ClientTimeout(total=timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params, allow_redirects=False) as response:
                if response.status != 200:
                    raise RuntimeError(f"candle upstream status {response.status}")
                length = response.content_length
                if length is not None and length > max_bytes:
                    raise RuntimeError("candle upstream response too large")
                chunks = bytearray()
                async for chunk in response.content.iter_chunked(64 * 1024):
                    chunks.extend(chunk)
                    if len(chunks) > max_bytes:
                        raise RuntimeError("candle upstream response too large")
                raw = bytes(chunks)
        try:
            payload = json.loads(raw.decode("utf-8"), parse_float=Decimal)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise RuntimeError("invalid candle upstream response") from None
        if not isinstance(payload, dict):
            raise RuntimeError("invalid candle upstream response")
        return payload

    async def get(self, *, identity: Dict[str, Any], interval: str, limit: int) -> Dict[str, Any]:
        if (identity.get("connector_name") != ALLOWED_CONNECTOR
                or identity.get("trading_pair") != ALLOWED_PAIR):
            return _unavailable(interval, "committed market identity is not allowlisted")
        key = (interval, limit)
        now = self._clock()
        cached = self._cache.get(key)
        if cached is not None and cached[0] > now:
            return dict(cached[1], cached=True)
        async with self._lock:
            now = self._clock()
            self._cache = {cache_key: value for cache_key, value in self._cache.items() if value[0] > now}
            cached = self._cache.get(key)
            if cached is not None and cached[0] > now:
                return dict(cached[1], cached=True)
            step = INTERVAL_SECONDS[interval]
            end_ms = int(now * 1000)
            params = {
                "market_id": MARKET_ID, "resolution": interval,
                "start_timestamp": end_ms - step * limit * 1000,
                "end_timestamp": end_ms, "count_back": limit,
            }
            try:
                payload = await self._request(CANDLE_URL, params, 5.0, MAX_CANDLE_BYTES)
                rows = payload.get("c")
                if not isinstance(rows, list):
                    raise ValueError("candle list missing")
                by_time = {}
                for row in rows[:limit]:
                    if not isinstance(row, dict) or isinstance(row.get("t"), bool):
                        raise ValueError("bad candle row")
                    timestamp_ms = int(row["t"])
                    lower_bound = params["start_timestamp"] - step * 1000
                    upper_bound = params["end_timestamp"] + step * 1000
                    if not lower_bound <= timestamp_ms <= upper_bound:
                        raise ValueError("candle timestamp outside requested range")
                    opened, high, low, closed = (_decimal_text(row.get(key)) for key in ("o", "h", "l", "c"))
                    values = tuple(Decimal(value) for value in (opened, high, low, closed))
                    if any(value <= 0 for value in values) or values[2] > min(values[0], values[3]) \
                            or values[1] < max(values[0], values[3]) or values[2] > values[1]:
                        raise ValueError("invalid candle OHLC")
                    candle = {
                        "time": timestamp_ms / 1000.0,
                        "open": opened, "high": high, "low": low, "close": closed,
                    }
                    if row.get("v") is not None:
                        volume = _decimal_text(row["v"])
                        if Decimal(volume) < 0:
                            raise ValueError("invalid candle volume")
                        candle["volume"] = volume
                    by_time[timestamp_ms] = candle
                candles = [by_time[timestamp] for timestamp in sorted(by_time)]
                if not candles:
                    raise ValueError("candle upstream returned no rows")
                result = {
                    "source": "lighter_robinhood_public_rest", "trading_pair": ALLOWED_PAIR,
                    "market_id": str(MARKET_ID), "interval": interval, "candles": candles,
                    "fetched_at": now, "cached": False, "unavailable_reason": None,
                }
            except Exception:  # noqa: BLE001 - unavailable market context is an honest UI state
                result = _unavailable(interval, "public candle request failed")
            self._cache[key] = (now + self._ttl_s, result)
            if len(self._cache) > self._max_cache_entries:
                oldest = min(self._cache, key=lambda cache_key: self._cache[cache_key][0])
                self._cache.pop(oldest, None)
            return dict(result)


def _order(cell: Dict[str, Any], leg: Dict[str, Any], role: str) -> Dict[str, Any]:
    side = leg.get("side")
    if side is None:
        entry_side = cell.get("entry_side")
        side = entry_side if role == "ENTRY" else ({"BUY": "SELL", "SELL": "BUY"}.get(entry_side))
    return {
        "cid": _text(leg.get("cid")), "exchange_order_id": _text(leg.get("exchange_id")),
        "cell_id": _text(cell.get("cell_id")), "generation": cell.get("generation"),
        "role": role, "side": side, "state": leg.get("state"),
        "price": _text(leg.get("price")), "requested": _text(leg.get("requested")),
        "filled": _text(leg.get("filled")), "remaining": _text(leg.get("remaining")),
        "expiry_at": leg.get("expiry_at"),
    }


class TerminalService:
    def __init__(self, gateway: Any, candles: Any, identity_provider: Callable[[], Dict[str, Any]],
                 clock: Callable[[], float] = time.time):
        self._gateway = gateway
        self._candles = candles
        self._identity_provider = identity_provider
        self._clock = clock

    async def build(self, *, interval: str, candle_limit: int, fill_limit: int,
                    stale_after_s: float, snapshot_version: Optional[int] = None) -> Dict[str, Any]:
        terminal_read = getattr(self._gateway, "terminal_read", None)
        if callable(terminal_read):
            snapshot, fills, fills_truncated = terminal_read(fill_limit, snapshot_version=snapshot_version)
        else:
            snapshot, fills, fills_truncated = self._gateway.latest_snapshot(), [], False
            if snapshot_version is not None and (snapshot or {}).get("snapshot_version") != snapshot_version:
                raise SnapshotUnavailable(str(snapshot_version))
        shown = views.for_display(snapshot) if snapshot else None
        header = {
            "snapshot_version": _text((shown or {}).get("snapshot_version")),
            "config_revision": (shown or {}).get("config_revision"),
            "engine_revision": (shown or {}).get("engine_revision"),
            "committed_at": (shown or {}).get("committed_at"),
        }
        if shown is None:
            market = _unavailable(interval, "no committed snapshot identity")
            cells = []
        else:
            committed_identity = (shown.get("summary") or {}).get("engine_config")
            if not isinstance(committed_identity, dict):
                committed_identity = {} if getattr(self._candles, "requires_committed_identity", False) \
                    else self._identity_provider()
            market = await self._candles.get(identity=committed_identity, interval=interval, limit=candle_limit)
            cells = list(shown.get("cells") or [])
        served_at = self._clock()
        levels = [{
            "cell_id": _text(cell.get("cell_id")), "low": _text(cell.get("low")),
            "high": _text(cell.get("high")), "entry_side": cell.get("entry_side"),
            "entry_price": _text(cell.get("entry_price")), "tp_price": _text(cell.get("tp_price")),
            "generation": cell.get("generation"), "state": cell.get("state"),
        } for cell in cells]
        levels.sort(key=lambda level: Decimal(level["low"]) if level["low"] is not None else Decimal("Infinity"))
        orders = []
        for cell in cells:
            entry = cell.get("entry")
            if isinstance(entry, dict) and entry.get("state") not in FINAL_ORDER_STATES:
                orders.append(_order(cell, entry, "ENTRY"))
            for tp in cell.get("tp_children") or []:
                if isinstance(tp, dict) and tp.get("state") not in FINAL_ORDER_STATES:
                    orders.append(_order(cell, tp, "TP"))
        summary = (shown or {}).get("summary") or {}
        return {
            "served_at": served_at, "snapshot": header,
            "freshness": views.freshness(shown, served_at, stale_after_s),
            "market": market,
            "grid": {"anchor": _text(summary.get("anchor")), "bid": _text(summary.get("bid")),
                     "ask": _text(summary.get("ask")), "levels": levels},
            "position": {
                "authoritative_net": _text(summary.get("authoritative_net")),
                "baseline": _text(summary.get("baseline")), "P": _text(summary.get("P")),
                "P_min": _text(summary.get("P_min")), "P_max": _text(summary.get("P_max")),
                "gross_worst": _text(summary.get("gross_worst")),
            },
            "orders": {"source": "committed_snapshot_nonfinal_legs", "rows": orders, "truncated": False,
                       "as_of_snapshot_version": header["snapshot_version"]},
            "fills": {"source": "confirmed_store_fills", "rows": fills, "truncated": bool(fills_truncated),
                      "as_of_snapshot_version": header["snapshot_version"]},
        }
