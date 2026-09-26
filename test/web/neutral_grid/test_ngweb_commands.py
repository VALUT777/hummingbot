"""AC-47 / NG-UI-003: idempotent command queue, stale revisions, single-engine Start."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest
from ngweb_fakes import sample_rules, sample_snapshot

JS_SAFE = (1 << 53) - 1


async def _preview(web):
    resp = await web.get("/api/preview")
    assert resp.status == 200, await resp.text()
    return await resp.json()


async def _start_payload(web, baseline="0"):
    preview = await _preview(web)
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
                              "baseline_acknowledged": True, "preview_id": preview["preview_id"],
                              "material_id": preview["material_id"]}


@pytest.mark.asyncio
async def test_start_baseline_must_match_config(make_web):
    web = await make_web(None)  # config B = 0
    await web.login()
    _, payload = await _start_payload(web, "330")
    resp = await web.command("start", "start-b-mismatch-001", payload)
    assert resp.status == 422
    assert "не совпадает" in (await resp.json())["message"]
    for typed in ("0", "+0", "0.000", "-0"):
        _, same = await _start_payload(web, typed)
        resp = await web.command("start", f"start-b-match-{typed}".replace("+", "p").replace(".", "d").ljust(20, "x"),
                                 same)
        assert resp.status in (202, 409), (typed, await resp.text())
    assert len([c for c in web.gateway.commands if c["kind"] == "start"]) == 1
    confirm = await web.command("confirm_baseline", "confirm-b-mismatch01", {"expected_initial_position": "5",
                                                                             "confirm": True})
    assert confirm.status == 422 and "не совпадает" in (await confirm.json())["message"]


@pytest.mark.asyncio
async def test_start_with_invalid_preview_is_rejected(make_web):
    from ngweb_fakes import sample_config
    web = await make_web(None, config=sample_config(expected_initial_position=Decimal("-1100")))
    await web.login()
    _, payload = await _start_payload(web, "-1100")
    resp = await web.command("start", "start-cap-000000001", payload)
    assert resp.status == 422
    body = await resp.json()
    assert body["error"] == "preview_invalid"
    assert any("max_abs_net_position" in e and e.startswith("Baseline B") for e in body["errors"])
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
    assert "AWAITING_START" not in web.gateway.snapshot["reasons"]
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
        "confirm_baseline": ({"expected_initial_position": "0.00", "confirm": True},
                             {"expected_initial_position": "0.00", "confirm": True}),
        "baseline_audit": ({"observed_position": "+10", "note": "ручная сделка", "acknowledge": True},
                           {"action": "baseline", "observed_position": "10", "note": "ручная сделка",
                            "acknowledge": True}),
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


@pytest.mark.asyncio
async def test_engine_awaiting_start_is_not_a_running_engine(make_web):
    snap = sample_snapshot("BOOTSTRAPPING")
    snap["reasons"] = ["AWAITING_START"]
    web = await make_web(snap)
    await web.login()
    state = await (await web.get("/api/state")).json()
    assert state["engine_started"] is False and state["engine"]["display_state"] == "BOOTSTRAPPING"
    _, payload = await _start_payload(web)
    resp = await web.command("start", "start-awaiting-00001", payload)
    assert resp.status == 202, await resp.text()
    # the engine applied it and now reports itself started: a second Start is a duplicate
    web.gateway.apply(web.gateway.commands[0]["id"], "APPLIED", {"started": True})
    snap["reasons"] = ["history cut incomplete"]
    snap["engine_revision"] += 1
    web.gateway.snapshot = snap
    _, payload = await _start_payload(web)
    dup = await web.command("start", "start-awaiting-00002", payload)
    assert dup.status == 409 and (await dup.json())["error"] == "engine_already_running"


@pytest.mark.asyncio
async def test_explicit_started_flag_wins_over_reasons(make_web):
    snap = sample_snapshot("RECONCILING")
    snap["summary"]["started"] = False
    web = await make_web(snap)
    await web.login()
    _, payload = await _start_payload(web)
    assert (await web.command("start", "start-explicit-0001", payload)).status == 202


@pytest.mark.asyncio
async def test_audit_actions_are_baseline_audit_commands(make_web):
    web = await make_web(sample_snapshot("FROZEN"))
    await web.login()
    ok = await web.command("baseline_audit", "audit-late-00000001",
                           {"action": "ack_late_evidence", "note": "проверено по истории", "acknowledge": True})
    assert ok.status == 202
    expected = {"action": "ack_late_evidence", "note": "проверено по истории", "acknowledge": True}
    assert (await ok.json())["command"]["payload"] == expected
    cid = await web.command("baseline_audit", "audit-cid-000000001",
                            {"action": "resolve_unknown_submit", "note": "нет в истории", "acknowledge": True,
                             "cid": "281474976710600"})
    assert cid.status == 202
    assert (await cid.json())["command"]["payload"]["cid"] == "281474976710600"
    for i, bad in enumerate(({"action": "flatten", "note": "x", "acknowledge": True},
                             {"action": "ack_risk_blocked", "acknowledge": True},
                             {"action": "ack_risk_blocked", "note": "x"},
                             {"action": "baseline", "note": "x", "acknowledge": True},
                             {"action": "resolve_unknown_submit", "note": "x", "acknowledge": True, "cid": str(1 << 48)},
                             {"action": "resolve_unknown_submit", "note": "x", "acknowledge": True, "cid": 5})):
        resp = await web.command("baseline_audit", f"audit-bad-{i:09d}", bad)
        assert resp.status == 422, bad
    legacy = await web.command("manual_reconcile", "legacy-kind-0000001", {"action": "ack_late_evidence"})
    assert legacy.status == 400 and (await legacy.json())["error"] == "bad_kind"


@pytest.mark.asyncio
@pytest.mark.parametrize(("payload", "message_fragment"), [
    ({"action": "extend_grid", "note": "audited", "acknowledge": True}, "proof_id"),
    ({"action": "extend_grid", "proof_id": "ab" * 32, "acknowledge": True}, "note"),
    ({"action": "extend_grid", "proof_id": "ab" * 32, "note": "audited"}, "acknowledge=true"),
    ({"action": "extend_grid", "proof_id": "AB" * 32, "note": "audited", "acknowledge": True}, "proof_id"),
    ({"action": "extend_grid", "proof_id": "ab" * 31, "note": "audited", "acknowledge": True}, "proof_id"),
])
async def test_extend_grid_rejects_incomplete_or_malformed_audit(make_web, payload, message_fragment):
    snap = sample_snapshot("STOPPED_WITH_INVENTORY")
    snap["summary"]["grid_extension_candidate"] = {"proof_id": "ab" * 32, "blockers": []}
    web = await make_web(snap)
    await web.login()

    resp = await web.command("baseline_audit", "extend-invalid-00001", payload)

    assert resp.status == 422, await resp.text()
    body = await resp.json()
    assert body["error"] == "invalid_payload"
    assert message_fragment in body["message"]
    assert web.gateway.commands == []


@pytest.mark.asyncio
async def test_extend_grid_enqueues_only_published_proof_with_revision_guard(make_web):
    proof_id = "ab" * 32
    snap = sample_snapshot("STOPPED_WITH_INVENTORY", config_revision=7, engine_revision=12)
    snap["summary"]["grid_extension_candidate"] = {"proof_id": proof_id, "blockers": []}
    web = await make_web(snap)
    await web.login()
    payload = {"action": "extend_grid", "proof_id": proof_id, "note": "  add 4.8–4.9 only  ",
               "acknowledge": True}

    stale = await web.command("baseline_audit", "extend-stale-000001", payload, cfg=7, eng=11)
    assert stale.status == 409
    assert (await stale.json())["error"] == "stale_revision"
    assert web.gateway.commands == []

    accepted = await web.command("baseline_audit", "extend-valid-000001", payload, cfg=7, eng=12)
    assert accepted.status == 202, await accepted.text()
    row = (await accepted.json())["command"]
    assert row["payload"] == {"action": "extend_grid", "proof_id": proof_id,
                              "note": "add 4.8–4.9 only", "acknowledge": True}
    assert "confirmation" not in row["payload"]


@pytest.mark.asyncio
@pytest.mark.parametrize(("candidate", "expected_error"), [
    (None, "grid_extension_not_eligible"),
    ({"proof_id": "ab" * 32, "blockers": ["ACTIVE_ORDERS"]}, "grid_extension_not_eligible"),
    ({"proof_id": "cd" * 32, "blockers": []}, "grid_extension_proof_changed"),
])
async def test_extend_grid_is_bound_to_eligible_published_candidate(make_web, candidate, expected_error):
    snap = sample_snapshot("STOPPED_WITH_INVENTORY")
    if candidate is not None:
        snap["summary"]["grid_extension_candidate"] = candidate
    web = await make_web(snap)
    await web.login()

    resp = await web.command("baseline_audit", "extend-bound-000001",
                             {"action": "extend_grid", "proof_id": "ab" * 32,
                              "note": "audited", "acknowledge": True})

    assert resp.status == 409, await resp.text()
    assert (await resp.json())["error"] == expected_error
    assert web.gateway.commands == []


@pytest.mark.asyncio
@pytest.mark.parametrize(("payload_update", "message_fragment"), [
    ({"proof_id": None}, "proof_id"),
    ({"proof_id": "AB" * 32}, "proof_id"),
    ({"note": ""}, "note"),
    ({"acknowledge": False}, "acknowledge=true"),
    ({"confirmation": "ADOPT SOMETHING ELSE"}, "confirmation"),
])
async def test_external_entry_adoption_rejects_malformed_proof_only_request(
        make_web, payload_update, message_fragment):
    proof_id = "ef" * 32
    confirmation = "ADOPT EXTERNAL BUY 100 LIT INTO arbitrary-grid CELL 10 TP 4.9"
    snap = sample_snapshot("STOPPED_WITH_INVENTORY")
    snap["summary"]["grid_external_entry_candidate"] = {
        "proof_id": proof_id, "blockers": [], "confirmation": confirmation,
    }
    web = await make_web(snap)
    await web.login()
    payload = {"action": "extend_grid_with_external_entry", "proof_id": proof_id,
               "note": "reviewed exchange history", "acknowledge": True, "confirmation": confirmation}
    payload.update(payload_update)

    resp = await web.command("baseline_audit", "adopt-invalid-00001", payload)

    assert resp.status == 422, await resp.text()
    body = await resp.json()
    assert body["error"] == "invalid_payload"
    assert message_fragment in body["message"]
    assert web.gateway.commands == []


@pytest.mark.asyncio
@pytest.mark.parametrize("override", [
    {"target_config": {"lower_price": "4.7"}},
    {"order_id": "operator-selected"},
    {"trade_ids": ["operator-selected"]},
    {"quantity": "200"},
    {"price": "4.7"},
    {"cell_id": "99"},
])
async def test_external_entry_adoption_rejects_caller_accounting_and_evidence_overrides(make_web, override):
    proof_id = "ef" * 32
    confirmation = "ADOPT EXTERNAL BUY 100 LIT INTO arbitrary-grid CELL 10 TP 4.9"
    snap = sample_snapshot("STOPPED")
    snap["summary"]["grid_external_entry_candidate"] = {
        "proof_id": proof_id, "blockers": [], "confirmation": confirmation,
    }
    web = await make_web(snap)
    await web.login()
    payload = {"action": "extend_grid_with_external_entry", "proof_id": proof_id,
               "note": "reviewed exchange history", "acknowledge": True, "confirmation": confirmation,
               **override}

    resp = await web.command("baseline_audit", "adopt-override-0001", payload)

    assert resp.status == 422, await resp.text()
    assert (await resp.json())["error"] == "invalid_payload"
    assert web.gateway.commands == []


@pytest.mark.asyncio
async def test_external_entry_adoption_enqueues_only_candidate_bound_request(make_web):
    proof_id = "ef" * 32
    confirmation = "ADOPT EXTERNAL BUY 100 LIT INTO operator-grid CELL 10 TP 4.9"
    snap = sample_snapshot("STOPPED_WITH_INVENTORY", config_revision=8, engine_revision=31)
    snap["summary"]["grid_external_entry_candidate"] = {
        "proof_id": proof_id, "blockers": [], "confirmation": confirmation,
        "target": {"grid_id": "operator-grid", "lower_price": "4.8", "upper_price": "5.9"},
        "proposed_cycle": {"cell_id": "10", "quantity": "100", "tp_price": "4.9"},
        "manual_order": {"exchange_order_id": "not-from-browser"},
        "trades": [{"trade_id": "not-from-browser"}],
    }
    web = await make_web(snap)
    await web.login()

    resp = await web.command(
        "baseline_audit", "adopt-valid-0000001",
        {"action": "extend_grid_with_external_entry", "proof_id": proof_id,
         "note": "  reviewed exact candidate  ", "acknowledge": True, "confirmation": confirmation},
        cfg=8, eng=31)

    assert resp.status == 202, await resp.text()
    assert (await resp.json())["command"]["payload"] == {
        "action": "extend_grid_with_external_entry", "proof_id": proof_id,
        "note": "reviewed exact candidate", "acknowledge": True, "confirmation": confirmation,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(("candidate", "expected_error"), [
    (None, "grid_external_entry_not_eligible"),
    ({"proof_id": "ef" * 32, "blockers": ["ACTIVE_ORDERS"], "confirmation": "x"},
     "grid_external_entry_not_eligible"),
    ({"proof_id": "01" * 32, "blockers": [], "confirmation": "x"},
     "grid_external_entry_proof_changed"),
])
async def test_external_entry_adoption_requires_exact_clear_candidate(make_web, candidate, expected_error):
    proof_id = "ef" * 32
    confirmation = "ADOPT EXTERNAL BUY 100 LIT INTO operator-grid CELL 10 TP 4.9"
    snap = sample_snapshot("STOPPED_WITH_INVENTORY")
    if candidate is not None:
        snap["summary"]["grid_external_entry_candidate"] = candidate
    web = await make_web(snap)
    await web.login()

    resp = await web.command(
        "baseline_audit", "adopt-bound-0000001",
        {"action": "extend_grid_with_external_entry", "proof_id": proof_id,
         "note": "reviewed", "acknowledge": True, "confirmation": confirmation})

    assert resp.status == 409, await resp.text()
    assert (await resp.json())["error"] == expected_error
    assert web.gateway.commands == []


def test_audit_actions_match_engine():
    from hummingbot.strategy_v2.executors.neutral_grid_executor import commands as engine_commands
    from web.neutral_grid.commands import AUDIT_ACTION_BASELINE, AUDIT_ACTIONS
    assert tuple(engine_commands.AUDIT_ACTIONS) == AUDIT_ACTIONS
    assert engine_commands.AUDIT_ACTION_BASELINE == AUDIT_ACTION_BASELINE
    for action in AUDIT_ACTIONS:
        payload = {"action": action, "note": "n", "acknowledge": True}
        if action == "baseline":
            payload["observed_position"] = "0"
        if action == "resolve_unknown_submit":
            payload["cid"] = "1"
        assert engine_commands.validate_kind("baseline_audit", payload) is None, action


@pytest.mark.asyncio
@pytest.mark.parametrize("started_flag", [False, None])
async def test_confirm_baseline_refused_before_start(make_web, started_flag):
    """Review #1: confirm_baseline must not bootstrap an engine that never got a risk-acknowledged Start."""
    snap = sample_snapshot("BOOTSTRAPPING")
    snap["reasons"] = ["AWAITING_START", "BASELINE_NOT_CONFIRMED"]
    snap["summary"]["baseline"] = None
    if started_flag is not None:
        snap["summary"]["started"] = started_flag
    web = await make_web(snap)
    await web.login()
    confirm = {"expected_initial_position": "0", "confirm": True}
    resp = await web.command("confirm_baseline", "confirm-before-start", confirm)
    assert resp.status == 409
    assert (await resp.json())["error"] == "start_required"
    assert web.gateway.commands == []
    # once the engine reports an applied Start, the confirmation is accepted
    snap["summary"]["started"] = True
    snap["reasons"] = ["BASELINE_NOT_CONFIRMED"]
    web.gateway.snapshot = snap
    ok = await web.command("confirm_baseline", "confirm-after-start1", confirm)
    assert ok.status == 202, await ok.text()
