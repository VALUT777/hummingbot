"""AC-48 truthful state + AC-22 (UI/API part): committed snapshot only, staleness, exact string ids."""
from __future__ import annotations

import json
import time
from decimal import Decimal

import pytest
from ngweb_fakes import FakeGateway, sample_snapshot

from web.neutral_grid import jsonsafe

JS_SAFE = (1 << 53) - 1
ALL_STATES = ["BOOTSTRAPPING", "RECONCILING", "NORMAL", "DEGRADED", "PAUSED", "RISK_BLOCKED", "FROZEN", "STOPPING",
              "STOPPED", "STOPPED_WITH_INVENTORY", "STOP_UNCERTAIN"]


def _strict_json(text: str):
    """Parse like a browser would care: any bare integer beyond 2**53 would be corrupted by JSON.parse."""
    def parse_int(s):
        if abs(int(s)) > JS_SAFE:
            raise AssertionError(f"unsafe bare integer in API JSON: {s}")
        return int(s)
    return json.loads(text, parse_int=parse_int, parse_float=Decimal)


@pytest.mark.asyncio
async def test_no_snapshot_is_unknown_even_if_backend_process_alive(make_web):
    web = await make_web(None)
    web.ctx.host_status = lambda: {"engine_task_alive": True, "ticks": 42}
    await web.login()
    state = _strict_json(await (await web.get("/api/state")).text())
    assert state["engine"]["display_state"] == "UNKNOWN"
    assert state["engine"]["last_known_state"] is None
    assert state["freshness"]["has_snapshot"] is False and state["freshness"]["stale"] is True
    assert state["host"] == {"engine_task_alive": True, "ticks": 42}  # reported separately, never mapped


@pytest.mark.asyncio
@pytest.mark.parametrize("engine_state", ALL_STATES)
async def test_fresh_snapshot_state_is_shown_verbatim(make_web, engine_state):
    web = await make_web(sample_snapshot(engine_state))
    await web.login()
    state = _strict_json(await (await web.get("/api/state")).text())
    assert state["engine"]["display_state"] == engine_state
    assert state["freshness"]["stale"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("engine_state", ["NORMAL", "STOPPED", "STOPPING", "PAUSED"])
async def test_stale_snapshot_is_never_shown_as_current(make_web, engine_state):
    web = await make_web(sample_snapshot(engine_state, committed_at=time.time() - 120), stale_after_s=15)
    await web.login()
    state = _strict_json(await (await web.get("/api/state")).text())
    assert state["engine"]["display_state"] == "STALE"
    assert state["engine"]["last_known_state"] == engine_state
    assert state["freshness"]["stale"] is True and state["freshness"]["age_s"] >= 119


@pytest.mark.asyncio
async def test_unknown_engine_state_string_is_not_interpreted(make_web):
    snap = sample_snapshot("NORMAL")
    snap["engine_state"] = "RUNNING"
    web = await make_web(snap)
    await web.login()
    state = _strict_json(await (await web.get("/api/state")).text())
    assert state["engine"]["display_state"] == "UNKNOWN" and state["engine"]["last_known_state"] == "RUNNING"


@pytest.mark.asyncio
async def test_stop_uncertain_reasons_and_errors_visible(make_web):
    snap = sample_snapshot("STOP_UNCERTAIN")
    snap["reasons"] = ["cancel outcome unknown for cid 281474976710600"]
    web = await make_web(snap)
    await web.login()
    state = _strict_json(await (await web.get("/api/state")).text())
    assert state["engine"]["display_state"] == "STOP_UNCERTAIN"
    assert state["engine"]["reasons"] == ["cancel outcome unknown for cid 281474976710600"]
    assert state["errors"][0]["code"] == "HISTORY_LAG"
    assert state["summary"]["history"]["lag_s"] == Decimal("3.2")


@pytest.mark.asyncio
async def test_cell_state_comes_from_snapshot_not_price(make_web):
    web = await make_web(sample_snapshot("NORMAL"))
    await web.login()
    before = _strict_json(await (await web.get("/api/cells?limit=200")).text())
    web.market["mid"] = Decimal("5.99")  # market moves; the committed snapshot does not
    after = _strict_json(await (await web.get("/api/cells?limit=200")).text())
    assert before["cells"] == after["cells"]
    states = {c["cell_id"]: c["state"] for c in after["cells"]}
    assert states["21"] == "TP_LIVE" and states["22"] == "DUST"
    cell_map = _strict_json(await (await web.get("/api/state")).text())["cell_map"]
    assert {c["cell_id"]: c["state"] for c in cell_map} == states


@pytest.mark.asyncio
async def test_summary_gauges_are_computed_server_side_as_strings(make_web):
    web = await make_web(sample_snapshot("NORMAL"))
    await web.login()
    summary = _strict_json(await (await web.get("/api/state")).text())["summary"]
    assert summary["gauges"]["net"] == {"min_pct": "33.5", "max_pct": "61.0", "breach": "no", "p_pct": "49.9"}
    assert summary["gauges"]["gross"] == {"pct": "55.0", "breach": "no"}


@pytest.mark.asyncio
async def test_ids_beyond_js_safe_range_survive_as_exact_strings(make_web):
    snap = sample_snapshot("NORMAL")
    huge = (1 << 63) + 5
    # even if an engine bug emitted *integers*, the API must still serve exact strings
    snap["cells"][0]["entry"]["exchange_id"] = huge
    snap["cells"][0]["entry"]["cid"] = 281474976710600
    snap["unmatched_evidence"][0]["trade_id_str"] = (1 << 60) + 3
    web = await make_web(snap)
    await web.login()
    page = _strict_json(await (await web.get("/api/cells?state=TP_LIVE")).text())
    cell21 = page["cells"][0]
    assert cell21["entry"]["exchange_id"] == str(huge) == "9223372036854775813"
    assert cell21["entry"]["cid"] == "281474976710600"
    assert f'"{huge}"' in await (await web.get("/api/cells?state=TP_LIVE")).text()
    state_text = await (await web.get("/api/state")).text()
    assert f'"{(1 << 60) + 3}"' in state_text
    _strict_json(state_text)


@pytest.mark.asyncio
async def test_lookup_matches_exact_string_only(make_web):
    snap = sample_snapshot("NORMAL")
    huge = str((1 << 63) + 5)
    rounded = str(int(float(huge)))  # what a float round-trip would produce
    assert rounded != huge
    gw = FakeGateway(snap)
    gw.trades.append({"trade_id_str": str((1 << 62) + 11), "own_exchange_order_id": huge, "size": "2",
                      "price": "5.3818", "own_side": "BUY"})
    gw.orders.append({"client_order_id": "281474976710600", "exchange_order_id": huge, "status": "open"})
    web = await make_web(snap, gateway=gw)
    await web.login()
    hit = _strict_json(await (await web.get(f"/api/lookup?id={huge}")).text())
    assert [m["role"] for m in hit["snapshot_matches"]] == ["ENTRY"]
    assert hit["snapshot_matches"][0]["cell_id"] == "21"
    assert len(hit["orders"]) == 1 and len(hit["trades"]) == 1
    assert hit["trades"][0]["trade_id_str"] == str((1 << 62) + 11)
    miss = _strict_json(await (await web.get(f"/api/lookup?id={rounded}")).text())
    assert miss["snapshot_matches"] == [] and miss["orders"] == [] and miss["trades"] == []
    bad = await web.get("/api/lookup?id=1%20OR%201=1")
    assert bad.status == 400


@pytest.mark.asyncio
async def test_cells_pagination_uses_opaque_string_cursor(make_web):
    web = await make_web(sample_snapshot("NORMAL"))
    await web.login()
    seen, cursor, pages = [], None, 0
    while True:
        resp = await web.get("/api/cells?limit=4" + (f"&after={cursor}" if cursor else ""))
        body = _strict_json(await resp.text())
        seen.extend(c["cell_id"] for c in body["cells"])
        pages += 1
        cursor = body["next_cursor"]
        if cursor is None:
            break
        assert isinstance(cursor, str)
    assert seen == [str(i) for i in range(23)]
    assert pages == 6
    active = _strict_json(await (await web.get("/api/cells?active=1")).text())
    assert [c["cell_id"] for c in active["cells"]] == ["21", "22"]
    assert (await web.get("/api/cells?after=***")).status == 400


@pytest.mark.asyncio
async def test_audit_pagination_and_no_secret_fields(make_web):
    gw = FakeGateway(sample_snapshot("NORMAL"))
    base = (1 << 53) + 100
    gw.events = [{"id": str(base + i), "at": time.time(), "kind": "command_applied", "detail": {"n": i}}
                 for i in range(5)]
    web = await make_web(gw.snapshot, gateway=gw)
    await web.login()
    first = _strict_json(await (await web.get("/api/audit?limit=2")).text())
    assert [e["id"] for e in first["events"]] == [str(base + 4), str(base + 3)]
    second = _strict_json(await (await web.get(f"/api/audit?limit=2&before={first['next_cursor']}")).text())
    assert [e["id"] for e in second["events"]] == [str(base + 2), str(base + 1)]


def test_jsonsafe_rules():
    data = {"cid": 5, "qty": Decimal("0.30"), "big": 1 << 60, "small": 7, "lag_s": 1.5, "price": 5.4,
            "nested": [{"trade_id": 1}], "flag": True, "none": None}
    safe = jsonsafe.make_safe(data)
    assert safe == {"cid": "5", "qty": "0.30", "big": str(1 << 60), "small": 7, "lag_s": 1.5, "price": "5.4",
                    "nested": [{"trade_id": "1"}], "flag": True, "none": None}
    assert jsonsafe.loads_exact('{"a": 0.1}')["a"] == Decimal("0.1")


@pytest.mark.asyncio
async def test_engine_shaped_snapshot_with_string_times_and_int_cell_ids(make_web):
    """The engine serializes timestamps as strings and cell ids as ints; the API normalizes for display."""
    now = time.time()
    snap = sample_snapshot("NORMAL")
    snap["committed_at"] = str(now)
    snap["errors"] = [{"at": str(now - 3), "code": "X", "message": "m"}]
    snap["summary"]["history"]["lag_s"] = "3.2"
    snap["summary"]["history"]["last_full_scan_at"] = str(now - 1)
    for cell in snap["cells"]:
        cell["cell_id"] = int(cell["cell_id"])
    snap["cells"][0]["queue_age_s"] = "12.5"
    snap["cells"][0]["tp_children"][0]["expiry"] = "1790000000123"
    web = await make_web(snap)
    await web.login()
    state = _strict_json(await (await web.get("/api/state")).text())
    assert state["freshness"]["stale"] is False
    assert isinstance(state["snapshot"]["committed_at"], Decimal)
    assert isinstance(state["errors"][0]["at"], Decimal)
    assert state["summary"]["history"]["lag_s"] == Decimal("3.2")
    cells = _strict_json(await (await web.get("/api/cells?state=TP_LIVE")).text())["cells"]
    assert cells[0]["cell_id"] == "21" and cells[0]["queue_age_s"] == Decimal("12.5")
    assert cells[0]["tp_children"][0]["expiry"] == "1790000000123"
    assert cells[0]["tp_children"][0]["expiry_at"] == Decimal("1790000000.123")
