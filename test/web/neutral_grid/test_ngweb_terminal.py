from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest
from ngweb_fakes import FakeGateway, sample_snapshot

from web.neutral_grid.terminal import (
    DemoCandleProvider,
    PublicCandleProvider,
    TerminalService,
)
from web.neutral_grid.gateway import SnapshotUnavailable


IDENTITY = {
    "grid_id": "ng-test",
    "connector_name": "lighter_perpetual_robinhood",
    "trading_pair": "LIT-USDG",
    "account_index": "7",
}


class TerminalGateway(FakeGateway):
    def __init__(self, snapshot, fills=()):
        super().__init__(snapshot)
        self.terminal_fills = list(fills)
        self.read_limits = []

    def terminal_read(self, fill_limit, snapshot_version=None):
        self.read_limits.append((fill_limit, snapshot_version))
        if snapshot_version is not None and (self.snapshot or {}).get("snapshot_version") != snapshot_version:
            raise SnapshotUnavailable(str(snapshot_version))
        return self.latest_snapshot(), self.terminal_fills[:fill_limit], len(self.terminal_fills) > fill_limit


def _fill(n=1):
    return {
        "dedupe_key": f"trade:{n}", "trade_id": str((1 << 53) + n),
        "cid": str(281474976710600 + n), "exchange_order_id": str((1 << 63) + n),
        "grid_id": "ng-test", "cell_id": "21", "generation": 2, "role": "ENTRY",
        "side": "BUY", "size": "3.000", "price": "5.3512",
        "trade_at": 1790250101.234 + n, "late": False,
    }


@pytest.mark.asyncio
async def test_terminal_composes_one_snapshot_and_filters_final_orders():
    snap = sample_snapshot("NORMAL")
    snap["snapshot_version"] = 1842
    snap["committed_at_ms"] = int(snap["committed_at"] * 1000)
    snap["summary"].update({"anchor": "5.4", "bid": "5.39", "ask": "5.41", "ledger_net": "-2"})
    gateway = TerminalGateway(snap, [_fill(1), _fill(2)])
    service = TerminalService(gateway, DemoCandleProvider(), lambda: dict(IDENTITY), clock=lambda: snap["committed_at"])

    result = await service.build(interval="5m", candle_limit=200, fill_limit=1, stale_after_s=15)

    assert result["snapshot"]["snapshot_version"] == "1842"
    assert result["snapshot"]["config_revision"] == 1
    assert result["market"]["source"] == "demo_fixture"
    assert all(isinstance(c["close"], str) for c in result["market"]["candles"])
    assert result["grid"]["levels"][0]["low"] == "5"
    assert result["position"]["authoritative_net"] == "-2"
    rows = result["orders"]["rows"]
    assert {row["state"] for row in rows} == {"LIVE"}
    assert {(row["role"], row["side"]) for row in rows} == {("ENTRY", "BUY"), ("TP", "SELL")}
    assert all(row["cid"] != "281474976710602" for row in rows)  # TERMINAL dust entry
    assert result["fills"]["rows"] == [_fill(1)]
    assert result["fills"]["truncated"] is True
    assert result["fills"]["as_of_snapshot_version"] == "1842"
    assert gateway.read_limits == [(1, None)]


@pytest.mark.asyncio
async def test_terminal_without_snapshot_is_empty_and_does_not_call_public_provider():
    calls = 0

    class Provider:
        async def get(self, **kwargs):
            nonlocal calls
            calls += 1
            raise AssertionError("must not fetch without a committed identity snapshot")

    result = await TerminalService(TerminalGateway(None), Provider(), lambda: dict(IDENTITY)).build(
        interval="5m", candle_limit=200, fill_limit=100, stale_after_s=15)

    assert calls == 0
    assert result["snapshot"]["snapshot_version"] is None
    assert result["market"]["source"] == "unavailable"
    assert result["grid"]["levels"] == [] and result["orders"]["rows"] == [] and result["fills"]["rows"] == []


@pytest.mark.asyncio
async def test_public_candles_are_allowlisted_exact_strings_and_cached_singleflight():
    calls = 0
    release = asyncio.Event()

    async def request(url, params, timeout_s, max_bytes):
        nonlocal calls
        calls += 1
        assert url == "https://api.rh.lighter.xyz/api/v1/candles"
        assert params["market_id"] == 5 and params["resolution"] == "5m" and params["count_back"] == 36
        assert timeout_s == 5.0 and max_bytes == 2 * 1024 * 1024
        await release.wait()
        return {"c": [{"t": 1790254500000, "o": Decimal("5.0588"), "h": Decimal("5.106"),
                       "l": Decimal("5.0563"), "c": Decimal("5.106"),
                       "v": Decimal("5232.640000000001"), "V": Decimal("26642.629144000006")}]}

    provider = PublicCandleProvider(request=request, clock=lambda: 1790265300.0)
    tasks = [asyncio.create_task(provider.get(identity=IDENTITY, interval="5m", limit=36)) for _ in range(3)]
    await asyncio.sleep(0)
    release.set()
    first, second, third = await asyncio.gather(*tasks)

    assert calls == 1
    assert first["candles"] == second["candles"] == third["candles"]
    assert first["candles"][0]["volume"] == "5232.640000000001"
    cached = await provider.get(identity=IDENTITY, interval="5m", limit=36)
    assert calls == 1 and cached["cached"] is True


@pytest.mark.asyncio
async def test_public_candle_failure_is_cached_and_identity_mismatch_never_requests():
    calls = 0

    async def failing(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise TimeoutError("upstream token=secret should be redacted by API boundary")

    provider = PublicCandleProvider(request=failing, clock=lambda: 1790265300.0)
    bad_identity = dict(IDENTITY, trading_pair="BTC-USDC")
    mismatch = await provider.get(identity=bad_identity, interval="5m", limit=20)
    assert mismatch["source"] == "unavailable" and calls == 0

    failed = await provider.get(identity=IDENTITY, interval="5m", limit=20)
    again = await provider.get(identity=IDENTITY, interval="5m", limit=20)
    assert failed["source"] == again["source"] == "unavailable"
    assert "secret" not in failed["unavailable_reason"]
    assert failed["candles"] == [] and calls == 1 and again["cached"] is True


@pytest.mark.asyncio
async def test_terminal_http_defaults_validation_auth_and_error_shape(make_web):
    snap = sample_snapshot("NORMAL")
    snap["committed_at_ms"] = int(snap["committed_at"] * 1000)
    web = await make_web(snap)
    web.ctx.terminal = TerminalService(web.gateway, DemoCandleProvider(), web.ctx.identity)

    assert (await web.get("/api/terminal")).status == 401
    await web.login()
    ok = await web.get("/api/terminal")
    assert ok.status == 200
    body = json.loads(await ok.text())
    assert body["market"]["interval"] == "5m" and len(body["market"]["candles"]) <= 200
    assert body["snapshot"]["snapshot_version"] == "1"

    for query in ("interval=2m", "candle_limit=19", "candle_limit=501", "fill_limit=0", "fill_limit=201",
                  "fill_limit=nope", "fill_limit=²", "candle_limit=" + "9" * 4301):
        response = await web.get("/api/terminal?" + query)
        assert response.status == 400, query
        error = await response.json()
        assert error["error"] == "bad_terminal_query"


@pytest.mark.asyncio
async def test_terminal_http_never_echoes_public_request_exception(make_web):
    async def failing(*args, **kwargs):
        raise RuntimeError("token=secret-value")

    snap = sample_snapshot("NORMAL")
    snap["summary"]["engine_config"] = dict(IDENTITY)
    web = await make_web(snap)
    web.ctx.terminal = TerminalService(web.gateway, PublicCandleProvider(request=failing), web.ctx.identity)
    await web.login()

    response = await web.get("/api/terminal?candle_limit=20")
    text = await response.text()
    assert response.status == 200 and "secret-value" not in text
    assert json.loads(text)["market"]["source"] == "unavailable"


@pytest.mark.asyncio
async def test_terminal_http_selects_exact_snapshot_or_returns_conflict(make_web):
    snap = sample_snapshot("NORMAL")
    snap["snapshot_version"] = 1842
    malicious = _fill(3)
    malicious["size"] = 0.10000000000000002
    gateway = TerminalGateway(snap, [malicious])
    web = await make_web(gateway=gateway)
    web.ctx.terminal = TerminalService(gateway, DemoCandleProvider(), web.ctx.identity)
    await web.login()

    selected = await web.get("/api/terminal?snapshot_version=1842")
    assert selected.status == 200
    selected_body = await selected.json()
    assert selected_body["snapshot"]["snapshot_version"] == "1842"
    assert isinstance(selected_body["market"]["candles"][0]["time"], (int, float))
    assert isinstance(selected_body["fills"]["rows"][0]["trade_at"], (int, float))
    assert selected_body["fills"]["rows"][0]["size"] == "0.10000000000000002"
    missing = await web.get("/api/terminal?snapshot_version=1841")
    assert missing.status == 409
    assert (await missing.json())["error"] == "snapshot_unavailable"
    for value in ("0", "01", "²", "9223372036854775808", "1" * 20):
        bad = await web.get("/api/terminal?snapshot_version=" + value)
        assert bad.status == 400
        assert (await bad.json())["error"] == "bad_terminal_query"
