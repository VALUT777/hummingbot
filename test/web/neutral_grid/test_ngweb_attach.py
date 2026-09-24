"""Review #2/#4: attach mode takes config, baseline check and identity from the engine's committed snapshot
(`summary.engine_config`, fingerprint-verified) and market rules from `summary.runtime_rules` with explicit
limit/post-only support and freshness gates. Nothing is defaulted."""
from __future__ import annotations

import time
import types
from decimal import Decimal

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer
from ngweb_fakes import FakeGateway, sample_config, sample_snapshot

from hummingbot.strategy_v2.executors.neutral_grid_executor import grid as core_grid
from web.neutral_grid.runtime import attach_context, engine_config_from_snapshot
from web.neutral_grid.security import AccessGate
from web.neutral_grid.server import create_app

TOKEN = "attach-test-token-0123456789"


def engine_config_json(**overrides):
    cfg = sample_config(grid_id="ng-engine-grid", expected_initial_position=Decimal("330"), **overrides)
    data = {}
    for name, value in cfg.__dict__.items():
        if isinstance(value, Decimal):
            data[name] = str(value)
        elif hasattr(value, "value"):
            data[name] = value.value
        else:
            data[name] = value
    data["fingerprint"] = core_grid.config_fingerprint(cfg)
    return data


def attach_snapshot(*, engine_config="default", rules_age_s=1.0, supports=(True, True), committed_age_s=0.0,
                    max_age_s="180"):
    snap = sample_snapshot("BOOTSTRAPPING", committed_at=time.time() - committed_age_s)
    snap["reasons"] = ["AWAITING_START"]
    snap["summary"]["started"] = False
    snap["summary"]["baseline"] = None
    snap["summary"]["bid"], snap["summary"]["ask"] = "5.3999", "5.4001"
    snap["summary"]["runtime_rules"] = {
        "tick_size": "0.0001", "size_step": "0.1", "min_base": "5", "min_notional": "10", "max_base": "100000",
        "max_leverage": "10", "max_active_orders_venue": None, "fetched_at": str(time.time() - rules_age_s),
        "supports_limit": supports[0], "supports_post_only": supports[1]}
    if max_age_s is not None:
        snap["summary"]["runtime_rules"]["max_age_s"] = max_age_s
    if engine_config == "default":
        snap["summary"]["engine_config"] = engine_config_json()
    elif engine_config is not None:
        snap["summary"]["engine_config"] = engine_config
    return snap


@pytest_asyncio.fixture
async def attach_web():
    clients = []

    async def factory(snapshot):
        gateway = FakeGateway(snapshot)
        gateway.live_clock = False
        args = types.SimpleNamespace(host="127.0.0.1", stale_after=15.0, allowed_host=[], attach_db="unused")
        ctx = attach_context(gateway, args, health_provider=lambda: {})
        ctx.access = AccessGate(TOKEN)
        client = TestClient(TestServer(create_app(ctx), host="127.0.0.1"))
        await client.start_server()
        clients.append(client)
        origin = f"http://127.0.0.1:{client.port}"
        resp = await client.post("/api/login", json={"token": TOKEN}, headers={"Origin": origin})
        csrf = (await resp.json())["csrf_token"]

        async def command(kind, key, payload):
            snap = gateway.latest_snapshot()
            return await client.post("/api/commands", json={
                "kind": kind, "idempotency_key": key, "payload": payload,
                "expected_config_revision": snap["config_revision"],
                "expected_engine_revision": snap["engine_revision"]},
                headers={"Origin": origin, "X-CSRF-Token": csrf})
        return types.SimpleNamespace(client=client, gateway=gateway, command=command, ctx=ctx)

    yield factory
    for client in clients:
        await client.close()


async def _preview(web):
    return await (await web.client.get("/api/preview")).json()


@pytest.mark.asyncio
async def test_attach_preview_identity_and_baseline_come_from_engine_config(attach_web):
    web = await attach_web(attach_snapshot())
    preview = await _preview(web)
    assert preview["errors"] == [], preview["errors"]
    assert preview["config"]["grid_id"] == "ng-engine-grid" and preview["baseline"]["value"] == "330"
    assert (preview["reachable"]["P_min"], preview["reachable"]["P_max"]) == ("0", "550")
    state = await (await web.client.get("/api/state")).json()
    assert state["engine_identity"]["grid_id"] == "ng-engine-grid"
    start = {"expected_initial_position": "0", "baseline_acknowledged": True, "risk_acknowledged": True,
             "preview_id": preview["preview_id"]}
    resp = await web.command("start", "attach-start-wrong-b", start)
    assert resp.status == 422 and "330" in (await resp.json())["message"]
    resp = await web.command("start", "attach-start-right-b", dict(start, expected_initial_position="330"))
    assert resp.status == 202, await resp.text()


@pytest.mark.asyncio
async def test_attach_credentials_are_read_only_and_bound_to_snapshot_identity(attach_web):
    web = await attach_web(attach_snapshot())
    body = await (await web.client.get("/api/keystore")).json()
    assert body == {
        "mutable": False,
        "attached_identity": {
            "grid_id": "ng-engine-grid",
            "connector_name": "lighter_perpetual_robinhood",
            "trading_pair": "LIT-USDG",
            "account_index": "7",
        },
        "credential_owner": "hummingbot_host",
        "message": (
            "Выбор и разблокировка ключей выполняются в Hummingbot. "
            "Панель показывает привязку по последнему снимку движка и не меняет её."
        ),
    }
    origin = f"http://127.0.0.1:{web.client.port}"
    # Use the fixture's authenticated request helper shape directly: both mutation routes must refuse before
    # reading a password or local connector profile.
    csrf = (await (await web.client.get("/api/session")).json())["csrf_token"]
    headers = {"Origin": origin, "X-CSRF-Token": csrf}
    for path, payload in (
        ("/api/keystore/select", {"profile": "lighter_perpetual_robinhood"}),
        ("/api/keystore/unlock", {"password": "must-not-be-read"}),
    ):
        resp = await web.client.post(path, json=payload, headers=headers)
        assert resp.status == 409
        assert (await resp.json())["error"] == "attached_credentials_read_only"


@pytest.mark.asyncio
async def test_attach_credentials_do_not_invent_identity_when_snapshot_config_is_absent(attach_web):
    web = await attach_web(attach_snapshot(engine_config=None))
    body = await (await web.client.get("/api/keystore")).json()
    assert body["mutable"] is False
    assert body["attached_identity"]["grid_id"] is None
    assert "config_error" in body["attached_identity"]
    assert "connector_name" not in body["attached_identity"]
    assert "account_index" not in body["attached_identity"]


def _drop(key):
    def mutate(cfg):
        cfg.pop(key)
        return cfg
    return mutate


def _set(**changes):
    def mutate(cfg):
        cfg.update(changes)
        return cfg
    return mutate


@pytest.mark.asyncio
@pytest.mark.parametrize("mutate,needle", [
    (lambda cfg: None, "engine_config"),
    (_drop("account_index"), "account_index"),
    (_set(surprise_key=1), "surprise_key"),
    (_set(cell_count=56), "fingerprint"),
    (_set(order_amount_base=10.0), "order_amount_base"),
])
async def test_attach_engine_config_is_strict(attach_web, mutate, needle):
    snap = attach_snapshot(engine_config=mutate(engine_config_json()))
    web = await attach_web(snap)
    preview = await _preview(web)
    assert preview["can_start"] is False
    assert any(needle in e for e in preview["errors"]), preview["errors"]
    resp = await web.command("start", f"attach-strict-{needle}"[:40].ljust(20, "x"), {
        "expected_initial_position": "330", "baseline_acknowledged": True, "risk_acknowledged": True,
        "preview_id": preview["preview_id"]})
    assert resp.status == 422
    assert web.gateway.commands == []


def test_engine_config_parser_direct():
    cfg, fp = engine_config_from_snapshot({"summary": {"engine_config": engine_config_json()}})
    assert cfg.grid_id == "ng-engine-grid" and fp == core_grid.config_fingerprint(cfg)
    with pytest.raises(ValueError):
        engine_config_from_snapshot({"summary": {}})


@pytest.mark.asyncio
@pytest.mark.parametrize("kwargs,needle", [
    ({"supports": (True, False)}, "post"),              # LIMIT_MAKER entry but the venue says no post-only
    ({"supports": (None, True)}, "supports_limit"),     # unknown support is not assumed
    ({"rules_age_s": 200.0}, "устарел"),                # rules older than the engine's published max_age_s
    ({"max_age_s": None}, "max_age_s"),                 # no published bound -> fail closed
    ({"committed_age_s": 120.0}, "Снимок"),            # stale committed snapshot
])
async def test_attach_market_rules_flags_and_freshness(attach_web, kwargs, needle):
    web = await attach_web(attach_snapshot(**kwargs))
    preview = await _preview(web)
    assert preview["can_start"] is False
    assert any(needle.lower() in e.lower() for e in preview["errors"]), preview["errors"]


@pytest.mark.asyncio
async def test_attach_rules_fetched_at_is_the_rules_time_not_snapshot_time(attach_web):
    snap = attach_snapshot(rules_age_s=4.0)
    web = await attach_web(snap)
    preview = await _preview(web)
    assert abs(preview["rules_fetched_at"] - float(snap["summary"]["runtime_rules"]["fetched_at"])) < 1e-6
    assert preview["runtime_rules"]["supports_post_only"] is True


@pytest.mark.asyncio
async def test_rules_freshness_uses_engine_max_age_not_history_freshness(attach_web):
    """Rules refreshed every 60 s are fresh under the engine's own bound even though history_freshness_s is 10 s."""
    web = await attach_web(attach_snapshot(rules_age_s=60.0, max_age_s="180"))
    preview = await _preview(web)
    assert preview["errors"] == [], preview["errors"]
    assert preview["can_start"] is True
