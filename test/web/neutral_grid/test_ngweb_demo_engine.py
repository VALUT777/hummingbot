"""AC-50 end to end on the offline demo: real NeutralGridEngine + FakeExchange + SQLite store behind the web API.

Everything the operator does goes through the HTTP API (session + CSRF + Origin); the fake venue is driven
only through the demo actions. Assertions read the committed snapshot the UI renders.
"""
from __future__ import annotations

import asyncio
import importlib.util
import time
import types
from decimal import Decimal

import pytest
from aiohttp.test_utils import TestClient, TestServer

ENGINE_AVAILABLE = importlib.util.find_spec("hummingbot.strategy_v2.executors.neutral_grid_executor.engine") is not None
pytestmark = [pytest.mark.skipif(not ENGINE_AVAILABLE, reason="WS-D engine not merged yet"),
              pytest.mark.timeout(240)]


class Api:
    def __init__(self, client: TestClient, token: str):
        self.client = client
        self.token = token
        self.origin = f"http://127.0.0.1:{client.port}"
        self.csrf = None

    async def login(self):
        resp = await self.client.post("/api/login", json={"token": self.token}, headers={"Origin": self.origin})
        assert resp.status == 200
        self.csrf = (await resp.json())["csrf_token"]

    async def get(self, path):
        resp = await self.client.get(path)
        assert resp.status == 200, (path, resp.status, await resp.text())
        return await resp.json()

    async def post(self, path, body):
        return await self.client.post(path, json=body, headers={"Origin": self.origin, "X-CSRF-Token": self.csrf})

    async def command(self, kind, key, payload=None):
        state = await self.get("/api/state")
        snap = state["snapshot"] or {}
        resp = await self.post("/api/commands", {
            "kind": kind, "idempotency_key": key, "payload": payload or {},
            "expected_config_revision": snap.get("config_revision", 0),
            "expected_engine_revision": snap.get("engine_revision", 0)})
        return resp.status, await resp.json()

    async def wait_command(self, command_id, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            row = (await self.get(f"/api/commands/{command_id}"))["command"]
            if row["status"] != "QUEUED":
                return row
            await asyncio.sleep(0.5)
        raise AssertionError(f"command {command_id} still QUEUED")

    async def wait_state(self, predicate, what, timeout=90):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            last = await self.get("/api/state")
            if predicate(last):
                return last
            await asyncio.sleep(0.5)
        raise AssertionError(f"timeout waiting for {what}: engine={last and last['engine']} "
                             f"reasons={last and last['engine'].get('reasons')}")

    async def cells(self):
        return (await self.get("/api/cells?limit=200"))["cells"]

    async def demo(self, action):
        resp = await self.post("/api/demo/action", {"action": action})
        assert resp.status == 200, await resp.text()
        return (await resp.json())["result"]


async def _command_ok(api, kind, key, payload=None, *, retries=5):
    """Send a command; on a stale-revision 409 (the engine moved on) re-read and retry with a new key."""
    for attempt in range(retries):
        status, body = await api.command(kind, f"{key}-{attempt:02d}".ljust(16, "x"), payload)
        if status == 202:
            return await api.wait_command(body["command"]["id"])
        assert status == 409 and body["error"] in ("stale_revision",), body
        await asyncio.sleep(0.3)
    raise AssertionError(f"{kind} kept conflicting")


@pytest.mark.asyncio
async def test_demo_engine_full_operator_flow(tmp_path):
    from web.neutral_grid import runtime
    from web.neutral_grid.server import create_app

    args = types.SimpleNamespace(data_dir=tmp_path / "demo", host="127.0.0.1", stale_after=15.0, allowed_host=[])
    bundle = await runtime.build_demo(args)
    client = TestClient(TestServer(create_app(bundle.context), host="127.0.0.1"))
    await client.start_server()
    api = Api(client, bundle.context.access.token)
    try:
        await api.login()
        state = await api.get("/api/state")
        assert state["engine_started"] is False and state["mode"] == "demo"
        assert state["host"]["engine_task_alive"] is True
        preview = await api.get("/api/preview")
        assert preview["errors"] == [], preview["errors"]
        assert (preview["grid"]["boundaries"], preview["grid"]["cells"]) == (56, 55)

        start = await _command_ok(api, "start", "demo-start", {
            "expected_initial_position": "0", "baseline_acknowledged": True, "risk_acknowledged": True,
            "preview_id": preview["preview_id"]})
        assert start["status"] == "APPLIED", start
        # a second Start for the same identity is refused, never a second engine
        status, body = await api.command("start", "demo-start-duplicate", {
            "expected_initial_position": "0", "baseline_acknowledged": True, "risk_acknowledged": True,
            "preview_id": (await api.get("/api/preview"))["preview_id"]})
        assert status == 409 and body["error"] in ("engine_already_running", "stale_revision"), body

        await api.wait_state(lambda s: (s["summary"].get("bootstrap") or {}).get("ready") is True,
                             "bootstrap ready")
        confirm = await _command_ok(api, "confirm_baseline", "demo-confirm",
                                    {"expected_initial_position": "0", "confirm": True})
        assert confirm["status"] == "APPLIED", confirm
        await api.wait_state(lambda s: s["engine"]["display_state"] == "NORMAL"
                             and int(s["summary"].get("owned_active") or 0) > 0, "NORMAL with live entries")

        # partial entry 3 (< min 5: obligation kept, no TP yet), then +3 -> TP 6 while the entry stays live
        assert (await api.demo("partial_entry"))["ok"]
        assert (await api.demo("partial_entry"))["ok"]

        def entry_and_tp_live(s):
            return any(c.get("entry") and c["entry"]["filled"] == "6" and c["tp_children"]
                       and c["tp_children"][0]["requested"] == "6" and c["entry"]["state"] == "LIVE"
                       for c in s)
        deadline = time.monotonic() + 60
        cells = await api.cells()
        while not entry_and_tp_live(cells) and time.monotonic() < deadline:
            await asyncio.sleep(0.5)
            cells = await api.cells()
        assert entry_and_tp_live(cells), [c for c in cells if c.get("entry") and c["entry"]["filled"] != "0"]
        cell = next(c for c in cells if c.get("entry") and c["entry"]["filled"] == "6")
        assert Decimal(cell["obligation"]["E"]) == Decimal("6")
        assert isinstance(cell["tp_children"][0]["cid"], str)

        # partial TP only reduces the obligation; the cell stays locked
        assert (await api.demo("partial_tp"))["ok"]
        await api.wait_state(lambda s: any(c["state"] not in ("IDLE", "QUEUED", "COMPLETE") for c in s["cell_map"]),
                             "cell still locked after partial TP")

        # history lag: the WS fill is signalled immediately but history (proof) lags
        assert (await api.demo("history_lag_on"))["ok"]
        assert (await api.demo("partial_entry"))["ok"]
        lagged = await api.wait_state(
            lambda s: float(s["summary"]["history"].get("lag_s") or 0) > 0 or s["engine"]["reasons"], "lag visible",
            timeout=20)
        assert lagged["summary"]["history"] is not None
        assert (await api.demo("history_lag_off"))["ok"]

        # dust: +2 then the venue cancels the rest -> exact 2 below the 5 floor is kept and visible
        assert (await api.demo("dust"))["ok"]
        await api.wait_state(lambda s: Decimal(str(s["summary"].get("dust_total") or "0")) > 0
                             or any(c["state"] == "DUST" for c in s["cell_map"]), "dust visible")

        # pause / resume through the queue
        paused = await _command_ok(api, "pause", "demo-pause", {"reason": "acceptance"})
        assert paused["status"] == "APPLIED"
        await api.wait_state(lambda s: s["engine"]["display_state"] == "PAUSED", "PAUSED")
        resumed = await _command_ok(api, "resume", "demo-resume")
        assert resumed["status"] in ("APPLIED", "REJECTED")
        if resumed["status"] == "APPLIED":
            await api.wait_state(lambda s: s["engine"]["display_state"] != "PAUSED", "left PAUSED")

        # stop with cancels that never prove terminal -> honest STOP_UNCERTAIN, not STOPPED
        assert (await api.demo("cancel_blackhole"))["ok"]
        stopped = await _command_ok(api, "stop", "demo-stop")
        assert stopped["status"] == "APPLIED"
        final = await api.wait_state(lambda s: s["engine"]["display_state"] == "STOP_UNCERTAIN", "STOP_UNCERTAIN",
                                     timeout=90)
        assert final["engine"]["display_state"] not in ("STOPPED", "STOPPED_WITH_INVENTORY")
    finally:
        await client.close()
        await bundle.close()
