from __future__ import annotations

import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Optional

import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from ngweb_fakes import FakeGateway, ReferenceGridCore, sample_config, sample_rules  # noqa: E402

from web.neutral_grid.keystore import KeystoreService  # noqa: E402
from web.neutral_grid.preview import MarketContext, PreviewService  # noqa: E402
from web.neutral_grid.security import AccessGate  # noqa: E402
from web.neutral_grid.server import WebContext, create_app  # noqa: E402

ACCESS_TOKEN = "test-access-token-0123456789abcdef"


class WebHarness:
    def __init__(self, client: TestClient, ctx: WebContext, market: Dict[str, Any]):
        self.client = client
        self.ctx = ctx
        self.market = market
        self.csrf: Optional[str] = None
        self.responses = []

    @property
    def origin(self) -> str:
        return f"http://127.0.0.1:{self.client.port}"

    @property
    def gateway(self) -> FakeGateway:
        return self.ctx.gateway

    async def login(self) -> str:
        resp = await self.post("/api/login", {"token": ACCESS_TOKEN}, csrf=False)
        assert resp.status == 200, await resp.text()
        self.csrf = (await resp.json())["csrf_token"]
        return self.csrf

    async def _record(self, resp):
        text = await resp.text()
        self.responses.append(text + "\n" + "\n".join(f"{k}: {v}" for k, v in resp.headers.items()))
        return resp

    async def get(self, path: str, **kw):
        return await self._record(await self.client.get(path, **kw))

    async def post(self, path: str, body: Any, *, csrf: bool = True, origin: Optional[str] = "default",
                   headers: Optional[Dict[str, str]] = None, raw: Optional[str] = None,
                   content_type: str = "application/json"):
        hdrs = {"Content-Type": content_type}
        if origin == "default":
            hdrs["Origin"] = self.origin
        elif origin is not None:
            hdrs["Origin"] = origin
        if csrf and self.csrf:
            hdrs["X-CSRF-Token"] = self.csrf
        hdrs.update(headers or {})
        import json as _json
        data = raw if raw is not None else _json.dumps(body)
        return await self._record(await self.client.post(path, data=data, headers=hdrs))

    async def command(self, kind: str, key: str, payload: Optional[Dict[str, Any]] = None,
                      cfg: Optional[int] = None, eng: Optional[int] = None):
        snap = self.gateway.snapshot or {}
        body = {"kind": kind, "idempotency_key": key,
                "expected_config_revision": snap.get("config_revision", 0) if cfg is None else cfg,
                "expected_engine_revision": snap.get("engine_revision", 0) if eng is None else eng,
                "payload": payload or {}}
        return await self.post("/api/commands", body)


def default_market(market: Dict[str, Any]):
    async def source() -> MarketContext:
        return MarketContext(rules=market["rules"], mid=market["mid"], available_collateral=market["available"],
                             fetched_at=time.time(), source="test")
    return source


@pytest_asyncio.fixture
async def make_web():
    clients = []

    async def factory(snapshot=None, *, config=None, core=None, mode="demo", keystore=None, stale_after_s=15.0,
                      demo=None, gateway=None, bind_host="127.0.0.1", policy=None):
        market = {"rules": sample_rules(), "mid": Decimal("5.4"), "available": Decimal("3000")}
        cfg = config or sample_config()
        ctx = WebContext(
            gateway=gateway or FakeGateway(snapshot),
            preview=PreviewService(cfg, default_market(market), core=core or ReferenceGridCore(), mode=mode),
            keystore=keystore or KeystoreService(demo=(mode == "demo")),
            engine_identity={"grid_id": cfg.grid_id, "connector_name": cfg.connector_name,
                             "trading_pair": cfg.trading_pair, "account_index": cfg.account_index},
            mode=mode, stale_after_s=stale_after_s, access=AccessGate(ACCESS_TOKEN), demo=demo,
            bind_host=bind_host,
        )
        if policy is not None:
            ctx.policy = policy
        client = TestClient(TestServer(create_app(ctx), host="127.0.0.1"))
        await client.start_server()
        clients.append(client)
        return WebHarness(client, ctx, market)

    yield factory
    for client in clients:
        await client.close()
