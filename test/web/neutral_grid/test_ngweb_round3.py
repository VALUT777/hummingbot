"""Round 3: C1 secret redaction, E-03 outdated health sidecar, E-09 START material binding, D2-17 extended audits."""
from __future__ import annotations

import json
import os
import time

import pytest
from ngweb_fakes import FakeGateway, sample_rules, sample_snapshot

from web.neutral_grid.runtime import health_file_provider

SECRET = "SECRETtok3n0123456789abcdefXYZ"
LEAKY = f"GET https://mainnet.zklighter.elliot.ai/api/v1/trades?account_index=7&auth={SECRET}&limit=100 failed"


# ------------------------------------------------------------------------------------------------ C1
def _leaky_snapshot():
    snap = sample_snapshot("DEGRADED")
    snap["reasons"] = [f"HISTORY_FETCH_FAILED: {LEAKY}"]
    snap["errors"] = [{"at": time.time(), "code": "HTTP", "message": LEAKY}]
    snap["summary"].update({
        "entry_blockers": [f"token={SECRET}"], "tp_blockers": [f"Authorization: Bearer {SECRET}"],
        "freezes": {"HISTORY_CONFLICT": f"signature={SECRET}"}, "persistence_error": f"x auth={SECRET}",
        "admission_blocker": f"api_key={SECRET}"})
    snap["summary"]["history"]["incomplete_reason"] = LEAKY
    snap["cells"][0]["blocker"] = LEAKY
    return snap


@pytest.mark.asyncio
async def test_c1_engine_free_text_never_leaks_auth_tokens(make_web):
    gw = FakeGateway(_leaky_snapshot())
    gw.events.append({"id": "1", "at": time.time(), "kind": "error", "detail": {"message": LEAKY, "nested": [LEAKY]}})
    web = await make_web(gateway=gw)
    web.ctx.health_provider = lambda: {"source": "health_file", "known": True, "persistence_error": LEAKY,
                                       "at": time.time()}
    await web.login()
    row = gw.enqueue_command("secret-row-000000001", "pause", 1, 1, {})
    gw.apply(row["id"], "REJECTED", {"error": "X", "detail": LEAKY})
    bodies = []
    for path in ("/api/state", "/api/cells?limit=200", "/api/audit", "/api/commands", f"/api/commands/{row['id']}",
                 "/api/lookup?id=21"):
        resp = await web.get(path)
        assert resp.status == 200, path
        bodies.append(await resp.text())
    text = "\n".join(bodies)
    assert SECRET not in text
    assert "zklighter" in text and "[скрыто]" in text  # context kept, only the secret part removed
    # ids and fingerprints are untouched by redaction
    assert str((1 << 63) + 5) in text


def test_c1_redaction_rules():
    from web.neutral_grid.security import redact_free_text
    assert SECRET not in redact_free_text(LEAKY)
    assert "https://mainnet.zklighter.elliot.ai/api/v1/trades?[скрыто]" in redact_free_text(LEAKY)
    for leak in (f"auth={SECRET}", f"token: {SECRET}", f"Authorization: Bearer {SECRET}", f"'signature': '{SECRET}'",
                 f"x-api-key={SECRET}", "sig=" + "ab" * 40, "deadbeef" * 8):
        assert SECRET not in redact_free_text(leak) and "ab" * 40 not in redact_free_text(leak) and \
            "deadbeef" * 8 not in redact_free_text(leak), leak
    keep = "cid 281474976710600 exchange 9223372036854775813 BELOW_MIN:3<5.0@5.4 HISTORY_STALE"
    assert redact_free_text(keep) == keep


# ------------------------------------------------------------------------------------------------ E-03
def _write_health(path, at, **fields):
    body = {"persistence_error": None, "fatal_reason": None, "at": str(at), "engine_revision": 1}
    body.update(fields)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(body))
    os.replace(tmp, path)


@pytest.mark.asyncio
async def test_e03_outdated_healthy_sidecar_is_unknown_next_to_stale(make_web, tmp_path):
    db = tmp_path / "ng.sqlite3"
    web = await make_web(sample_snapshot("NORMAL", committed_at=time.time() - 120), stale_after_s=15)
    web.ctx.health_provider = health_file_provider(db)
    await web.login()
    _write_health(tmp_path / "ng.sqlite3.health.json", time.time() - 100)  # 'healthy' from the first tick
    state = await (await web.get("/api/state")).json()
    assert state["engine"]["display_state"] == "STALE"
    assert state["health"]["known"] is False
    assert "состояние хранилища неизвестно" in state["health"]["banner"]
    _write_health(tmp_path / "ng.sqlite3.health.json", time.time())  # fresh heartbeat
    state = await (await web.get("/api/state")).json()
    assert state["health"]["known"] is True and state["health"]["banner"] is None


# ------------------------------------------------------------------------------------------------ E-09
@pytest.mark.asyncio
async def test_e09_confirm_baseline_refused_when_material_changed_since_start(make_web):
    snap = sample_snapshot("BOOTSTRAPPING")
    snap["reasons"] = ["AWAITING_START"]
    snap["summary"].update(started=False, baseline=None)
    web = await make_web(snap)
    await web.login()
    preview = await (await web.get("/api/preview")).json()
    start = await web.command("start", "e09-start-000000001", {
        "expected_initial_position": "0", "baseline_acknowledged": True, "risk_acknowledged": True,
        "preview_id": preview["preview_id"]})
    assert start.status == 202
    row = (await start.json())["command"]
    assert row["payload"]["material_id"] == preview["material_id"]
    web.gateway.apply(row["id"], "APPLIED", {"started": True})
    snap["summary"]["started"] = True
    snap["reasons"] = ["BASELINE_NOT_CONFIRMED"]
    snap["engine_revision"] = 2
    web.gateway.snapshot = snap
    web.market["rules"] = sample_rules(min_base=__import__("decimal").Decimal("4"))  # rules changed after Start
    confirm = {"expected_initial_position": "0", "confirm": True}
    resp = await web.command("confirm_baseline", "e09-confirm-00000001", confirm)
    assert resp.status == 409 and (await resp.json())["error"] == "start_material_changed"
    web.market["rules"] = sample_rules()  # same material as acknowledged at Start
    ok = await web.command("confirm_baseline", "e09-confirm-00000002", confirm)
    assert ok.status == 202, await ok.text()


# ------------------------------------------------------------------------------------------------ D2-17
def _cid_frozen_snapshot():
    snap = sample_snapshot("FROZEN")
    snap["summary"]["freezes"] = {"CID_ALLOCATION": "CidCollisionError: cid 281474976710655 owned by a foreign order"}
    snap["summary"]["colliding_cid"] = "281474976710655"
    snap["summary"]["grid_mutation_blockers"] = ["OPEN_CYCLES:3"]
    return snap


@pytest.mark.asyncio
async def test_d2_17_retire_colliding_cid_only_when_frozen_with_explicit_confirmation(make_web):
    web = await make_web(_cid_frozen_snapshot())
    await web.login()
    base = {"action": "retire_colliding_cid", "note": "чужой ордер с этим CID", "acknowledge": True,
            "cid": "281474976710655"}
    wrong = await web.command("baseline_audit", "retire-wrong-text-01", dict(base, confirmation="да"))
    assert wrong.status == 422
    ok = await web.command("baseline_audit", "retire-right-text-01",
                           dict(base, confirmation="СПИСАТЬ CID 281474976710655"))
    assert ok.status == 202, await ok.text()
    assert (await ok.json())["command"]["payload"] == dict(base, confirmation="СПИСАТЬ CID 281474976710655")
    other = await web.command("baseline_audit", "retire-other-cid-01",
                              dict(base, cid="123", confirmation="СПИСАТЬ CID 123"))
    assert other.status == 422  # only the CID the engine reported as colliding
    web.gateway.snapshot = sample_snapshot("NORMAL")  # no CID freeze -> not applicable
    web.gateway.snapshot["summary"]["freezes"] = {}
    resp = await web.command("baseline_audit", "retire-no-freeze-01",
                             dict(base, confirmation="СПИСАТЬ CID 281474976710655"))
    assert resp.status == 409 and (await resp.json())["error"] == "audit_not_applicable"


@pytest.mark.asyncio
async def test_d2_17_migrate_grid_only_when_quiescent_with_explicit_confirmation(make_web):
    snap = _cid_frozen_snapshot()
    web = await make_web(snap)
    await web.login()
    body = {"action": "migrate_grid", "note": "новая сетка после аудита", "acknowledge": True,
            "confirmation": "МИГРАЦИЯ СЕТКИ ng-test"}
    blocked = await web.command("baseline_audit", "migrate-blocked-001", body)
    assert blocked.status == 409 and (await blocked.json())["error"] == "audit_not_applicable"
    snap["summary"].pop("grid_mutation_blockers")
    web.gateway.snapshot = snap
    unknown = await web.command("baseline_audit", "migrate-unknown-001", body)
    assert unknown.status == 409  # blockers not published -> fail closed
    snap["summary"]["grid_mutation_blockers"] = []
    web.gateway.snapshot = snap
    wrong = await web.command("baseline_audit", "migrate-wrongtext01", dict(body, confirmation="МИГРАЦИЯ"))
    assert wrong.status == 422
    ok = await web.command("baseline_audit", "migrate-ok-00000001", body)
    assert ok.status == 202, await ok.text()
    assert (await ok.json())["command"]["payload"] == body


def test_d2_17_extended_actions_match_engine():
    from hummingbot.strategy_v2.executors.neutral_grid_executor import commands as engine_commands
    from web.neutral_grid.commands import EXTENDED_AUDIT_ACTIONS
    assert tuple(engine_commands.EXTENDED_AUDIT_ACTIONS) == EXTENDED_AUDIT_ACTIONS
    for payload in ({"action": "retire_colliding_cid", "note": "n", "acknowledge": True, "cid": "5",
                     "confirmation": "СПИСАТЬ CID 5"},
                    {"action": "migrate_grid", "note": "n", "acknowledge": True, "confirmation": "МИГРАЦИЯ СЕТКИ g"}):
        assert engine_commands.validate_kind("baseline_audit", payload) is None
