"""Round 4 / M1 (AC-40, NG-UI-002): ack_history_conflict is bound to the conflict set the operator reviewed."""
from __future__ import annotations

import json

import pytest
from ngweb_fakes import sample_snapshot

JS_SAFE = (1 << 53) - 1
TRADE_ID = str((1 << 64) + 11)
ORDER_ID = str((1 << 63) + 5)
FP_COMMITTED, FP_OTHER = "a" * 16, "b" * 16
FP_X, FP_Y = "c" * 16, "d" * 16


def conflict_snapshot(set_id="set-000000000001", extra_version=False):
    snap = sample_snapshot("FROZEN")
    snap["summary"]["freezes"] = {"HISTORY_CONFLICT": "conflicting duplicate rows"}
    versions_b = [
        {"fingerprint": FP_X, "committed": False,
         "summary": {"size": "2.5", "price": "5.3818", "side": "BUY", "trade_id_str": TRADE_ID}},
        {"fingerprint": FP_Y, "committed": False,
         "summary": {"size": "3.0", "price": "5.3818", "side": "BUY", "trade_id_str": TRADE_ID}},
    ]
    if extra_version:
        versions_b.append({"fingerprint": "e" * 16, "committed": False,
                           "summary": {"size": "9.9", "price": "5.3818", "side": "BUY", "trade_id_str": TRADE_ID}})
    snap["summary"]["history_conflicts"] = [
        {"stream": "TRADES", "key": f"trade:{TRADE_ID}:BUY:{ORDER_ID}", "versions": [
            {"fingerprint": FP_COMMITTED, "committed": True,
             "summary": {"size": "2", "filled": "2", "price": "5.3818", "side": "BUY",
                         "trade_id_str": TRADE_ID, "exchange_order_id": (1 << 63) + 5}},
            {"fingerprint": FP_OTHER, "committed": False,
             "summary": {"size": "2.5", "price": "5.3818", "side": "BUY", "trade_id_str": TRADE_ID,
                         "exchange_order_id": ORDER_ID}}]},
        {"stream": "INACTIVE_ORDERS", "key": f"order:{ORDER_ID}", "versions": versions_b},
    ]
    snap["summary"]["conflict_set_id"] = set_id
    return snap


def _strict(text):
    def parse_int(s):
        assert abs(int(s)) <= JS_SAFE, s
        return int(s)
    return json.loads(text, parse_int=parse_int)


def ack_payload(set_id="set-000000000001", accepted=None, **extra):
    body = {"action": "ack_history_conflict", "note": "сверено по истории биржи", "acknowledge": True,
            "conflict_set_id": set_id, "accepted": {f"order:{ORDER_ID}": FP_Y} if accepted is None else accepted,
            "confirmation": f"ПРИНЯТЬ НАБОР {set_id}"}
    body.update(extra)
    return body


@pytest.mark.asyncio
async def test_conflict_set_rendered_exactly_and_in_drilldown(make_web):
    web = await make_web(conflict_snapshot())
    await web.login()
    state = _strict(await (await web.get("/api/state")).text())
    conflicts = state["summary"]["history_conflicts"]
    assert state["summary"]["conflict_set_id"] == "set-000000000001"
    first = conflicts[0]["versions"][0]
    assert first["committed"] is True and first["summary"]["exchange_order_id"] == ORDER_ID  # int -> exact str
    assert first["summary"]["trade_id_str"] == TRADE_ID
    hit = _strict(await (await web.get(f"/api/lookup?id={TRADE_ID}")).text())
    sources = [m["source"] for m in hit["snapshot_matches"]]
    assert sources.count("history_conflict") == 2
    match = next(m for m in hit["snapshot_matches"] if m["source"] == "history_conflict")
    assert match["stream"] == "TRADES" and len(match["versions"]) == 2


@pytest.mark.asyncio
async def test_ack_payload_carries_set_id_and_accepted_versions(make_web):
    web = await make_web(conflict_snapshot())
    await web.login()
    ok = await web.command("baseline_audit", "ack-conflict-ok-0001", ack_payload())
    assert ok.status == 202, await ok.text()
    payload = (await ok.json())["command"]["payload"]
    assert payload == ack_payload()


@pytest.mark.asyncio
@pytest.mark.parametrize("override,status", [
    ({"conflict_set_id": None}, 422),                                   # required
    ({"accepted": {}}, 422),                                            # key without committed version needs a pick
    ({"accepted": {f"order:{ORDER_ID}": "f" * 16}}, 422),               # not one of the versions shown
    ({"accepted": {"order:unknown": FP_X, f"order:{ORDER_ID}": FP_X}}, 422),  # key not in the set
    ({"confirmation": "да"}, 422),                                      # typed phrase kept
    ({"note": ""}, 422),
])
async def test_ack_payload_validation(make_web, override, status):
    web = await make_web(conflict_snapshot())
    await web.login()
    body = ack_payload()
    body.update(override)
    if body.get("conflict_set_id") is None:
        body.pop("conflict_set_id")
    resp = await web.command("baseline_audit", f"ack-bad-{abs(hash(str(override))) % 10**10:010d}", body)
    assert resp.status == status, (override, await resp.text())
    assert web.gateway.commands == []


@pytest.mark.asyncio
async def test_changed_set_is_409_with_fresh_set_never_enqueued(make_web):
    web = await make_web(conflict_snapshot())
    await web.login()
    web.gateway.snapshot = conflict_snapshot(set_id="set-000000000002", extra_version=True)  # new contradiction
    resp = await web.command("baseline_audit", "ack-conflict-stale01", ack_payload())
    assert resp.status == 409
    body = await resp.json()
    assert body["error"] == "conflict_set_changed"
    assert body["conflict_set_id"] == "set-000000000002"
    assert len(body["history_conflicts"][1]["versions"]) == 3
    assert web.gateway.commands == []
    unpublished = conflict_snapshot()
    unpublished["summary"].pop("conflict_set_id")
    web.gateway.snapshot = unpublished
    resp = await web.command("baseline_audit", "ack-conflict-nopub01", ack_payload())
    assert resp.status == 409  # set not published -> cannot bind -> fail closed


def test_engine_accepts_the_web_payload_shape():
    from hummingbot.strategy_v2.executors.neutral_grid_executor import commands as engine_commands
    assert engine_commands.validate_kind("baseline_audit", ack_payload()) is None
