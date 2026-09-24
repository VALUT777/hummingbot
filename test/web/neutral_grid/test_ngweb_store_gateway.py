"""Web API over the real WS-B store (attach mode): committed snapshots only, INSERT-only command client."""
from __future__ import annotations

import asyncio
import json
import sqlite3

import pytest
from ngweb_fakes import sample_snapshot

from hummingbot.strategy_v2.executors.neutral_grid_executor.store import EngineIdentity, NeutralGridStore
from web.neutral_grid.gateway import StoreGateway

JS_SAFE = (1 << 53) - 1


def _strict(text: str):
    def parse_int(s):
        assert abs(int(s)) <= JS_SAFE, f"unsafe bare integer {s}"
        return int(s)
    return json.loads(text, parse_int=parse_int)


@pytest.fixture
def store_pair(tmp_path):
    identity = EngineIdentity(connector_name="lighter_perpetual_robinhood", connector_domain="fake_lighter",
                              account_index=7, trading_pair="LIT-USDG")
    path = tmp_path / "ng.sqlite3"
    writer = NeutralGridStore.open(path, identity, create_if_missing=True, lock_dir=tmp_path / "locks")
    gateway = StoreGateway.open(path)
    yield writer, gateway
    gateway.close()
    writer.close()


def _commit_snapshot(writer, state="NORMAL", **extra):
    body = sample_snapshot(state)
    for key in ("snapshot_version", "config_revision", "engine_revision", "committed_at"):
        body.pop(key)
    body.update(extra)
    return writer.write_snapshot(None, body)


@pytest.mark.asyncio
async def test_api_serves_latest_committed_store_snapshot(make_web, store_pair):
    writer, gateway = store_pair
    web = await make_web(gateway=gateway)
    await web.login()
    empty = _strict(await (await web.get("/api/state")).text())
    assert empty["engine"]["display_state"] == "UNKNOWN"
    first = _commit_snapshot(writer, "RECONCILING")
    second = _commit_snapshot(writer, "PAUSED")
    assert second.snapshot_version == first.snapshot_version + 1
    state = _strict(await (await web.get("/api/state")).text())
    assert state["engine"]["display_state"] == "PAUSED"
    assert state["snapshot"]["snapshot_version"] == second.snapshot_version
    assert (state["snapshot"]["config_revision"], state["snapshot"]["engine_revision"]) == (0, 0)
    assert state["freshness"]["stale"] is False


@pytest.mark.asyncio
async def test_big_ids_from_store_snapshot_stay_exact(make_web, store_pair):
    writer, gateway = store_pair
    huge = (1 << 63) + 5
    body_cells = sample_snapshot()["cells"]
    body_cells[0]["entry"]["exchange_id"] = huge          # an int: the store serializes it as a string
    _commit_snapshot(writer, cells=body_cells)
    web = await make_web(gateway=gateway)
    await web.login()
    text = await (await web.get("/api/cells?state=TP_LIVE")).text()
    assert _strict(text)["cells"][0]["entry"]["exchange_id"] == str(huge)


@pytest.mark.asyncio
async def test_commands_go_to_durable_store_queue_idempotently(make_web, store_pair):
    writer, gateway = store_pair
    _commit_snapshot(writer)
    web = await make_web(gateway=gateway)
    await web.login()
    resp = await web.command("pause", "store-pause-0000001", {"reason": "тест"}, cfg=0, eng=0)
    assert resp.status == 202, await resp.text()
    row = (await resp.json())["command"]
    assert row["status"] == "QUEUED"
    again = await web.command("pause", "store-pause-0000001", {"reason": "тест"}, cfg=0, eng=0)
    assert again.status == 200 and (await again.json())["command"]["id"] == row["id"]
    assert [c.idempotency_key for c in writer.list_commands()] == ["store-pause-0000001"]
    # the engine (writer) applies it; a refresh returns the committed result, not an assumption
    with writer.transaction() as tx:
        claimed = writer.claim_next_command(tx)
        writer.complete_command(tx, claimed.id, "APPLIED", {"paused": True})
    fetched = _strict(await (await web.get(f"/api/commands/{row['id']}")).text())["command"]
    assert fetched["status"] == "APPLIED" and fetched["result"] == {"paused": True}


@pytest.mark.asyncio
async def test_store_side_conflict_when_engine_revision_moved_before_snapshot(make_web, store_pair):
    """Race: the committed snapshot still says r0 but the engine row already moved -> CONFLICT row, 409."""
    writer, gateway = store_pair
    _commit_snapshot(writer)
    writer.bump_engine_revision(None, "test race")
    web = await make_web(gateway=gateway)
    await web.login()
    resp = await web.command("stop", "store-stop-race-0001", cfg=0, eng=0)
    assert resp.status == 409
    body = await resp.json()
    assert body["error"] == "stale_revision" and body["command"]["status"] == "CONFLICT"
    assert "preview" in body
    with writer.transaction() as tx:
        assert writer.claim_next_command(tx) is None  # a CONFLICT row is never applied


@pytest.mark.asyncio
async def test_concurrent_start_through_store_single_queued_row(make_web, store_pair):
    writer, gateway = store_pair
    _commit_snapshot(writer, "BOOTSTRAPPING", reasons=["AWAITING_START"])
    web = await make_web(gateway=gateway)
    await web.login()
    preview = await (await web.get("/api/preview")).json()
    payload = {"expected_initial_position": "0", "baseline_acknowledged": True, "risk_acknowledged": True,
               "preview_id": preview["preview_id"]}
    results = await asyncio.gather(*[web.command("start", f"store-start-{i:06d}", payload, cfg=0, eng=0)
                                     for i in range(10)])
    assert sorted(r.status for r in results) == [202] + [409] * 9
    queued = [c for c in writer.list_commands() if c.kind == "start" and c.status.value == "QUEUED"]
    assert len(queued) == 1


def test_command_client_cannot_write_ledger(store_pair):
    writer, gateway = store_pair
    store = gateway._store
    with pytest.raises(Exception):
        store.write_snapshot(None, {"engine_state": "NORMAL", "reasons": []})
    with pytest.raises(Exception):
        store.bump_engine_revision(None, "web must not")
    with pytest.raises(sqlite3.DatabaseError):
        store._conn.execute("UPDATE engine SET engine_revision = 99 WHERE id = 1")
    assert writer.engine().engine_revision == 0


@pytest.mark.asyncio
async def test_audit_events_from_store_paginate(make_web, store_pair):
    writer, gateway = store_pair
    _commit_snapshot(writer)
    for i in range(5):
        writer.record_audit(None, "operator_note", "test", {"n": i})
    web = await make_web(gateway=gateway)
    await web.login()
    first = _strict(await (await web.get("/api/audit?limit=2")).text())
    assert len(first["events"]) == 2 and first["next_cursor"]
    second = _strict(await (await web.get(f"/api/audit?limit=2&before={first['next_cursor']}")).text())
    ids = [e["id"] for e in first["events"] + second["events"]]
    assert ids == sorted(ids, key=int, reverse=True) and len(set(ids)) == 4
    assert all(isinstance(i, str) for i in ids)


@pytest.mark.asyncio
async def test_audit_action_reaches_store_queue(make_web, store_pair):
    writer, gateway = store_pair
    _commit_snapshot(writer, "FROZEN")
    web = await make_web(gateway=gateway)
    await web.login()
    resp = await web.command("baseline_audit", "store-audit-000001",
                             {"action": "ack_risk_blocked", "note": "проверено", "acknowledge": True}, cfg=0, eng=0)
    assert resp.status == 202, await resp.text()
    [row] = writer.list_commands()
    assert row.kind == "baseline_audit" and row.payload["action"] == "ack_risk_blocked"


@pytest.mark.asyncio
async def test_real_store_snapshot_times_render_as_numbers(make_web, store_pair):
    """Review #6: float times come back from the store as Decimal; the API must serve display numbers."""
    import time as _time

    writer, gateway = store_pair
    at = _time.time() - 3
    _commit_snapshot(writer, errors=[{"at": at, "code": "X", "message": "m"}])
    web = await make_web(gateway=gateway)
    await web.login()
    state = json.loads(await (await web.get("/api/state")).text())
    assert isinstance(state["errors"][0]["at"], float) and abs(state["errors"][0]["at"] - at) < 0.01
    assert isinstance(state["snapshot"]["committed_at"], float)
    assert isinstance(state["freshness"]["committed_at"], float)
