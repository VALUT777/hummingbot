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


async def _wait_cell(api, predicate, what, timeout=60):
    deadline = time.monotonic() + timeout
    cells = []
    while time.monotonic() < deadline:
        cells = await api.cells()
        for cell in cells:
            if predicate(cell):
                return cell
        await asyncio.sleep(0.5)
    busy = [c for c in cells if c["state"] not in ("IDLE", "QUEUED", "ENTRY_LIVE")]
    raise AssertionError(f"timeout waiting for {what}; non-idle cells: {busy[:4]}")


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

        # AC-05 on the UI: +3 (< min 5) is a visible below-min obligation, no TP yet; the entry stays live
        assert (await api.demo("partial_entry"))["ok"]
        cell = await _wait_cell(api, lambda c: c.get("entry") and c["entry"]["filled"] == "3", "entry filled 3")
        assert set(cell["state_flags"]) >= {"ENTRY_LIVE", "TP_REQUIRED"}, cell
        assert (cell["blocker"] or "").startswith("BELOW_MIN"), cell
        cell_id = cell["cell_id"]
        # +3 more -> TP of exactly 6 while the entry remainder is still live (ENTRY_LIVE + TP_LIVE)
        assert (await api.demo("partial_entry"))["ok"]
        cell = await _wait_cell(api, lambda c: c["cell_id"] == cell_id and c["tp_children"]
                                and c["tp_children"][0]["requested"] == "6" and c["tp_children"][0]["state"] == "LIVE",
                                "TP 6 live")
        assert cell["entry"]["state"] == "LIVE" and cell["entry"]["remaining"] == "4"
        assert set(cell["state_flags"]) >= {"ENTRY_LIVE", "TP_LIVE"}
        assert Decimal(cell["obligation"]["E"]) == Decimal("6") and isinstance(cell["tp_children"][0]["cid"], str)
        # partial TP only reduces the obligation; same fixed target, cell stays locked
        assert (await api.demo("partial_tp"))["ok"]
        cell = await _wait_cell(api, lambda c: c["cell_id"] == cell_id and c["tp_children"]
                                and c["tp_children"][0]["filled"] == "2", "TP filled 2")
        assert cell["tp_children"][0]["remaining"] == "4" and cell["state"] not in ("IDLE", "COMPLETE")
        assert cell["tp_children"][0]["price"] == cell["tp_price"]

        # pause / resume through the queue (resume passes the freshness/risk gate or is REJECTED honestly)
        paused = await _command_ok(api, "pause", "demo-pause", {"reason": "acceptance"})
        assert paused["status"] == "APPLIED"
        state = await api.wait_state(lambda s: s["engine"]["display_state"] == "PAUSED", "PAUSED")
        assert "OPERATOR_PAUSE" in state["engine"]["reasons"]
        for attempt in range(6):
            resumed = await _command_ok(api, "resume", f"demo-resume-{attempt}")
            if resumed["status"] == "APPLIED":
                break
            assert resumed["status"] == "REJECTED" and resumed["result"]["error"] == "RESUME_GATE", resumed
            await asyncio.sleep(3)
        assert resumed["status"] == "APPLIED", resumed
        await api.wait_state(lambda s: s["engine"]["display_state"] == "NORMAL", "NORMAL after resume")

        # dust: +2 on an untouched entry, then the venue cancels the rest -> exact 2 below the floor, visible
        assert (await api.demo("dust"))["ok"]
        dust = await _wait_cell(api, lambda c: c["state"] == "DUST", "DUST cell", timeout=90)
        assert dust["obligation"]["dust"] == "2" and (dust["blocker"] or "").startswith("BELOW_MIN")
        state = await api.get("/api/state")
        assert Decimal(state["summary"]["dust_total"]) >= Decimal("2")

        # history lag: the WS fill is signalled at once, history (the only proof) lags -> lag visible, no TP yet
        assert (await api.demo("history_lag_on"))["ok"]
        before = next(c for c in await api.cells() if c["cell_id"] == cell_id)
        assert (await api.demo("partial_entry"))["ok"]
        lagged = await api.wait_state(lambda s: Decimal(str(s["summary"]["history"]["lag_s"] or "0")) > 3,
                                      "history lag visible", timeout=30)
        assert lagged["summary"]["history"]["lag_s"] is not None
        during = next(c for c in await api.cells() if c["cell_id"] == cell_id)
        assert during["entry"]["filled"] == before["entry"]["filled"]  # nothing credited without history
        assert (await api.demo("history_lag_off"))["ok"]

        # stop while cancels never prove terminal -> honest STOP_UNCERTAIN, never STOPPED
        assert (await api.demo("cancel_blackhole"))["ok"]
        stopped = await _command_ok(api, "stop", "demo-stop")
        assert stopped["status"] == "APPLIED"
        final = await api.wait_state(lambda s: s["engine"]["display_state"] == "STOP_UNCERTAIN", "STOP_UNCERTAIN",
                                     timeout=90)
        assert final["summary"]["stop_outcome"] == "STOP_UNCERTAIN"
        assert int(final["summary"]["unknown_orders"]) > 0
        # browser/backend restart does not stop or restart the engine: a fresh session sees the same state
        api2 = Api(client, bundle.context.access.token)
        await api2.login()
        again = await api2.get("/api/state")
        assert again["engine"]["display_state"] == "STOP_UNCERTAIN" and again["host"]["engine_task_alive"] is True
    finally:
        await client.close()
        await bundle.close()
