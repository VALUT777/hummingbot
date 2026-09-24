"""AC-47 / NG-UI-003: idempotent command queue, stale revisions, single-engine Start."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest
from ngweb_fakes import sample_rules, sample_snapshot

JS_SAFE = (1 << 53) - 1


async def _preview(web, baseline=None):
    path = "/api/preview" + (f"?baseline={baseline}" if baseline is not None else "")
    resp = await web.get(path)
    assert resp.status == 200, await resp.text()
    return await resp.json()


async def _start_payload(web, baseline="0"):
    preview = await _preview(web, baseline)
    return preview, {"expected_initial_position": baseline, "baseline_acknowledged": True,
                     "risk_acknowledged": True, "preview_id": preview["preview_id"]}


@pytest.mark.asyncio
async def test_command_returns_committed_queue_row_not_assumed_outcome(make_web):
    web = await make_web(sample_snapshot("NORMAL"))
    await web.login()
    resp = await web.command("pause", "pause-key-000000001", {"reason": "проверка"})
    assert resp.status == 202
    body = await resp.json()
    cmd = body["command"]
    assert cmd["status"] == "QUEUED" and cmd["kind"] == "pause" and cmd["result"] is None
    assert cmd["payload"] == {"reason": "проверка"}
    assert isinstance(cmd["id"], str) and int(cmd["id"]) > JS_SAFE
    state = await (await web.get("/api/state")).json()
    # the engine has not applied it: the UI must still show the committed NORMAL, never an assumed PAUSED
    assert state["engine"]["display_state"] == "NORMAL"
    assert state["recent_commands"][0]["status"] == "QUEUED"


@pytest.mark.asyncio
async def test_retry_and_refresh_return_the_same_command(make_web):
    web = await make_web(sample_snapshot("NORMAL"))
    await web.login()
    first = await web.command("stop", "stop-key-0000000001")
    assert first.status == 202
    cmd_id = (await first.json())["command"]["id"]
    again = await web.command("stop", "stop-key-0000000001")
    assert again.status == 200
    replay = await again.json()
    assert replay["replay"] is True and replay["command"]["id"] == cmd_id
    assert len(web.gateway.commands) == 1
    # engine applies it -> a later refresh/retry sees the applied result, still no second row
    web.gateway.apply(cmd_id, "APPLIED", {"engine_state": "STOPPING"})
    web.gateway.snapshot["engine_revision"] += 1  # revisions moved on after the first request
    later = await web.command("stop", "stop-key-0000000001", cfg=1, eng=1)
    assert later.status == 200
    row = (await later.json())["command"]
    assert row["id"] == cmd_id and row["status"] == "APPLIED" and row["result"] == {"engine_state": "STOPPING"}
    fetched = await (await web.get(f"/api/commands/{cmd_id}")).json()
    assert fetched["command"]["status"] == "APPLIED"
    assert len(web.gateway.commands) == 1


@pytest.mark.asyncio
async def test_same_key_different_request_is_422_and_not_enqueued(make_web):
    web = await make_web(sample_snapshot("NORMAL"))
    await web.login()
    await web.command("pause", "shared-key-00000001")
    resp = await web.command("stop", "shared-key-00000001")
    assert resp.status == 422
    assert (await resp.json())["error"] == "idempotency_key_reused"
    assert [c["kind"] for c in web.gateway.commands] == ["pause"]


@pytest.mark.asyncio
async def test_stale_revision_is_409_with_fresh_preview_never_applied(make_web):
    web = await make_web(sample_snapshot("NORMAL", config_revision=3, engine_revision=17))
    await web.login()
    resp = await web.command("resume", "resume-key-00000001", cfg=3, eng=16)
    assert resp.status == 409
    body = await resp.json()
    assert body["error"] == "stale_revision"
    assert body["current"] == {"config_revision": 3, "engine_revision": 17, "engine_state": "NORMAL",
                               "committed_at": web.gateway.snapshot["committed_at"]}
    assert body["preview"]["config_revision"] == 3 and body["preview"]["engine_revision"] == 17
    assert body["preview"]["grid"]["cells"] == 55
    assert web.gateway.enqueue_calls == 0 and web.gateway.commands == []
    # a retry with the same (stale) key stays a conflict; it is never auto-applied
    again = await web.command("resume", "resume-key-00000001", cfg=3, eng=16)
    assert again.status == 409 and web.gateway.commands == []
    fresh = await web.command("resume", "resume-key-00000002", cfg=3, eng=17)
    assert fresh.status == 202


@pytest.mark.asyncio
async def test_bad_command_inputs_rejected(make_web):
    web = await make_web(sample_snapshot("NORMAL"))
    await web.login()
    cases = [
        ({"kind": "market_close", "idempotency_key": "x" * 20, "expected_config_revision": 1,
          "expected_engine_revision": 1}, 400),
        ({"kind": "pause", "idempotency_key": "short", "expected_config_revision": 1,
          "expected_engine_revision": 1}, 400),
        ({"kind": "pause", "idempotency_key": "x" * 20, "expected_config_revision": 1.5,
          "expected_engine_revision": 1}, 400),
        ({"kind": "pause", "idempotency_key": "x" * 20, "expected_config_revision": -1,
          "expected_engine_revision": 1}, 400),
        ({"kind": "pause", "idempotency_key": "x" * 20, "expected_config_revision": 1,
          "expected_engine_revision": 1, "payload": {"reason": "r" * 501}}, 422),
        ({"kind": "baseline_audit", "idempotency_key": "x" * 20, "expected_config_revision": 1,
          "expected_engine_revision": 1, "payload": {"observed_position": 5, "note": "n", "acknowledge": True}}, 422),
        ({"kind": "confirm_baseline", "idempotency_key": "x" * 20, "expected_config_revision": 1,
          "expected_engine_revision": 1, "payload": {"expected_initial_position": "0"}}, 422),
    ]
    for body, status in cases:
        resp = await web.post("/api/commands", body)
        assert resp.status == status, (body, await resp.text())
    assert web.gateway.commands == []


@pytest.mark.asyncio
async def test_start_requires_explicit_baseline_and_risk_confirmation(make_web):
    web = await make_web(None)  # nothing started yet: revisions 0/0
    await web.login()
    preview, payload = await _start_payload(web, "0")
    assert preview["live_confirmation_required"] is True
    for missing in ("risk_acknowledged", "baseline_acknowledged", "preview_id", "expected_initial_position"):
        broken = dict(payload)
        broken.pop(missing)
        resp = await web.command("start", f"start-missing-{missing}"[:40].ljust(20, "x"), broken)
        assert resp.status == 422, missing
    float_b = dict(payload, expected_initial_position=0.0)
    assert (await web.command("start", "start-float-b-000001", float_b)).status == 422
    assert web.gateway.commands == []
    ok = await web.command("start", "start-ok-0000000001", payload)
    assert ok.status == 202, await ok.text()
    row = (await ok.json())["command"]
    assert row["payload"] == {"expected_initial_position": "0", "risk_acknowledged": True,
                              "baseline_acknowledged": True, "preview_id": preview["preview_id"]}


@pytest.mark.asyncio
async def test_start_with_baseline_outside_cap_is_rejected(make_web):
    web = await make_web(None)
    await web.login()
    _, payload = await _start_payload(web, "900")  # reachable P_max = 900 + 220 > 1000
    resp = await web.command("start", "start-cap-000000001", payload)
    assert resp.status == 422
    body = await resp.json()
    assert body["error"] == "preview_invalid"
    assert any("max_abs_net_position" in e for e in body["errors"])
    assert web.gateway.commands == []


@pytest.mark.asyncio
async def test_start_with_stale_preview_gets_409_and_fresh_preview(make_web):
    web = await make_web(None)
    await web.login()
    old_preview, payload = await _start_payload(web)
    web.market["rules"] = sample_rules(min_base=Decimal("4"))  # runtime rules changed after the preview
    resp = await web.command("start", "start-stale-0000001", payload)
    assert resp.status == 409
    body = await resp.json()
    assert body["error"] == "stale_preview"
    assert body["preview"]["preview_id"] != old_preview["preview_id"]
    assert web.gateway.commands == []


@pytest.mark.asyncio
async def test_preview_id_ignores_moving_mid_price(make_web):
    web = await make_web(None)
    await web.login()
    before = await _preview(web)
    web.market["mid"] = Decimal("5.61")
    after = await _preview(web)
    assert before["preview_id"] == after["preview_id"]
    # the advisory split does move with the mid, the confirmation fingerprint does not
    assert (before["sides"]["buy"], after["sides"]["buy"]) == (22, 34)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["BOOTSTRAPPING", "RECONCILING", "NORMAL", "DEGRADED", "PAUSED", "RISK_BLOCKED",
                                   "FROZEN", "STOPPING"])
async def test_start_while_engine_active_returns_engine_identity_not_second_engine(make_web, state):
    web = await make_web(sample_snapshot(state))
    await web.login()
    _, payload = await _start_payload(web)
    resp = await web.command("start", "start-dup-000000001", payload)
    assert resp.status == 409
    body = await resp.json()
    assert body["error"] == "engine_already_running"
    assert body["engine_identity"]["grid_id"] == "ng-test" and body["engine_state"] == state
    assert web.gateway.commands == []


@pytest.mark.asyncio
async def test_concurrent_start_different_keys_enqueues_exactly_one(make_web):
    web = await make_web(None)
    await web.login()
    _, payload = await _start_payload(web)
    results = await asyncio.gather(*[
        web.command("start", f"start-concurrent-{i:04d}", payload) for i in range(12)])
    statuses = sorted(r.status for r in results)
    assert statuses.count(202) == 1 and statuses.count(409) == 11
    starts = [c for c in web.gateway.commands if c["kind"] == "start"]
    assert len(starts) == 1
    for r in results:
        body = await r.json()
        if r.status == 409:
            assert body["error"] == "start_already_queued"
            assert body["command"]["id"] == starts[0]["id"]
            assert body["engine_identity"]["grid_id"] == "ng-test"


@pytest.mark.asyncio
async def test_concurrent_same_key_returns_same_row(make_web):
    web = await make_web(None)
    await web.login()
    _, payload = await _start_payload(web)
    results = await asyncio.gather(*[web.command("start", "start-same-key-0001", payload) for _ in range(8)])
    ids = {(await r.json())["command"]["id"] for r in results}
    assert len(ids) == 1
    assert sorted(r.status for r in results) == [200] * 7 + [202]
    assert len(web.gateway.commands) == 1


@pytest.mark.asyncio
async def test_start_allowed_again_after_clean_stop_but_not_twice(make_web):
    web = await make_web(sample_snapshot("STOPPED_WITH_INVENTORY", config_revision=2, engine_revision=40))
    await web.login()
    _, payload = await _start_payload(web)
    first = await web.command("start", "restart-key-0000001", payload)
    assert first.status == 202
    second = await web.command("start", "restart-key-0000002", payload)
    assert second.status == 409 and (await second.json())["error"] == "start_already_queued"


@pytest.mark.asyncio
async def test_non_start_commands_need_a_committed_engine(make_web):
    web = await make_web(None)
    await web.login()
    resp = await web.command("pause", "pause-nosnap-000001", cfg=0, eng=0)
    assert resp.status == 409 and (await resp.json())["error"] == "engine_not_started"


@pytest.mark.asyncio
async def test_all_operator_commands_enqueue_normalized_payloads(make_web):
    web = await make_web(sample_snapshot("PAUSED"))
    await web.login()
    cases = {
        "pause": ({}, {}),
        "resume": ({"reason": " после проверки "}, {"reason": "после проверки"}),
        "stop": ({}, {}),
        "confirm_baseline": ({"expected_initial_position": "-120.50", "confirm": True},
                             {"expected_initial_position": "-120.50", "confirm": True}),
        "baseline_audit": ({"observed_position": "+10", "note": "ручная сделка", "acknowledge": True},
                           {"observed_position": "10", "note": "ручная сделка", "acknowledge": True}),
    }
    for i, (kind, (payload, expected)) in enumerate(cases.items()):
        resp = await web.command(kind, f"op-cmd-{kind}-{i}".ljust(20, "0"), payload)
        assert resp.status == 202, (kind, await resp.text())
        assert (await resp.json())["command"]["payload"] == expected


@pytest.mark.asyncio
async def test_command_list_pagination_with_big_string_ids(make_web):
    web = await make_web(sample_snapshot("NORMAL"))
    await web.login()
    for i in range(7):
        assert (await web.command("pause", f"page-key-{i:010d}")).status == 202
    seen = []
    cursor = None
    while True:
        resp = await web.get("/api/commands?limit=3" + (f"&before={cursor}" if cursor else ""))
        text = await resp.text()
        body = json.loads(text, parse_int=lambda s: pytest.fail(f"bare integer in JSON: {s}") if int(s) > JS_SAFE
                          else int(s))
        seen.extend(c["id"] for c in body["commands"])
        cursor = body["next_cursor"]
        if not cursor:
            break
        assert isinstance(cursor, str)
    assert len(seen) == 7 == len(set(seen))
    assert seen == sorted(seen, key=lambda s: (len(s), s), reverse=True)
    assert all(int(s) > JS_SAFE for s in seen)
