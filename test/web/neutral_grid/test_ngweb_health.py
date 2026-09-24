"""Review #3 (NG-DB-004 / AC-55, §11): a persistence failure the engine cannot commit must still be visible."""
from __future__ import annotations

import json
import os
import time

import pytest
from ngweb_fakes import sample_snapshot

from web.neutral_grid.host import EngineHost
from web.neutral_grid.runtime import health_file_provider


def _write_health(path, **fields):
    body = {"persistence_error": None, "fatal_reason": None, "at": str(time.time()), "engine_revision": 7}
    body.update(fields)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(body))
    os.replace(tmp, path)


@pytest.mark.asyncio
async def test_attach_health_file_error_shown_next_to_stale(make_web, tmp_path):
    db = tmp_path / "ng.sqlite3"
    health = tmp_path / "ng.sqlite3.health.json"
    web = await make_web(sample_snapshot("NORMAL", committed_at=time.time() - 120), stale_after_s=15)
    web.ctx.health_provider = health_file_provider(db)
    await web.login()
    state = await (await web.get("/api/state")).json()
    assert state["engine"]["display_state"] == "STALE"
    assert state["health"]["known"] is False
    assert "состояние хранилища неизвестно" in state["health"]["banner"]
    _write_health(health, persistence_error="disk I/O error: database or disk is full")
    state = await (await web.get("/api/state")).json()
    assert state["health"]["known"] is True and state["health"]["uncommitted"] is True
    assert "disk is full" in state["health"]["banner"] and "не зафиксировано" in state["health"]["banner"]
    assert state["health"]["engine_revision"] == 7
    _write_health(health)  # healthy again
    state = await (await web.get("/api/state")).json()
    assert state["health"]["banner"] is None
    health.write_text("{broken")
    state = await (await web.get("/api/state")).json()
    assert state["health"]["known"] is False and "неизвестно" in state["health"]["banner"]


@pytest.mark.asyncio
async def test_fresh_snapshot_without_health_file_has_no_banner(make_web, tmp_path):
    web = await make_web(sample_snapshot("NORMAL"))
    web.ctx.health_provider = health_file_provider(tmp_path / "missing.sqlite3")
    await web.login()
    state = await (await web.get("/api/state")).json()
    assert state["health"]["banner"] is None


class _Engine:
    def __init__(self):
        self.persistence_error = None
        self.fatal_reason = None

    async def tick(self):
        return None


@pytest.mark.asyncio
async def test_in_process_host_reports_engine_persistence_error(make_web):
    engine = _Engine()
    host = EngineHost(engine, "health-test-identity")
    try:
        web = await make_web(sample_snapshot("NORMAL"))
        web.ctx.host_status = host.status
        web.ctx.health_provider = host.health
        await web.login()
        assert (await (await web.get("/api/state")).json())["health"]["banner"] is None
        engine.persistence_error = "PersistenceError: database is read-only"
        state = await (await web.get("/api/state")).json()
        assert "read-only" in state["health"]["banner"] and state["health"]["known"] is True
        assert state["host"]["persistence_error"] == "PersistenceError: database is read-only"
        engine.persistence_error, engine.fatal_reason = None, "STORE_OPEN_REFUSED:PriorRunEvidenceError"
        state = await (await web.get("/api/state")).json()
        assert "STORE_OPEN_REFUSED" in state["health"]["banner"]
    finally:
        await host.close()
