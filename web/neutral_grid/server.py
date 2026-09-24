"""aiohttp application for the neutral grid local UI (NG-UI-001..003).

Read endpoints serve only the latest *committed* snapshot (plus read-only store lookups); the only
write path to the engine is ``POST /api/commands`` -> durable command queue. There is no endpoint
that returns a credential, and the browser never talks to the exchange.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional

from aiohttp import web

from web.neutral_grid import jsonsafe, views
from web.neutral_grid.commands import CommandService
from web.neutral_grid.gateway import EngineGateway
from web.neutral_grid.keystore import KeystoreError, KeystoreService
from web.neutral_grid.preview import PreviewService, parse_signed_decimal
from web.neutral_grid.security import (
    AccessGate,
    SecurityPolicy,
    SessionManager,
    build_security_middleware,
    clear_session_cookie,
    json_error,
    set_session_cookie,
)

LOGGER = logging.getLogger("web.neutral_grid.server")
STATIC_DIR = Path(__file__).resolve().parent / "static"
MAX_BODY = 64 * 1024
CTX_KEY = web.AppKey("ng_web_context", object)


@dataclass
class DemoControls:
    """Offline-demo-only hooks that drive the *fake exchange* (never the engine state)."""
    actions: Dict[str, str]
    run: Callable[[str], Awaitable[Dict[str, Any]]]


@dataclass
class WebContext:
    gateway: EngineGateway
    preview: PreviewService
    keystore: KeystoreService
    engine_identity: Dict[str, Any]
    mode: str = "demo"                     # demo | attach | live
    bind_host: str = "127.0.0.1"
    stale_after_s: float = 15.0
    clock: Callable[[], float] = time.time
    sessions: SessionManager = field(default_factory=SessionManager)
    access: AccessGate = field(default_factory=AccessGate)
    policy: SecurityPolicy = field(default_factory=SecurityPolicy)
    host_status: Callable[[], Dict[str, Any]] = lambda: {}
    demo: Optional[DemoControls] = None
    commands: Optional[CommandService] = None

    def __post_init__(self) -> None:
        if self.commands is None:
            self.commands = CommandService(self.gateway, self.build_preview, self.engine_identity)

    async def build_preview(self, *, config_revision: int, engine_revision: int,
                            baseline_override: Optional[Decimal] = None) -> Dict[str, Any]:
        snapshot = self.gateway.latest_snapshot()
        confirmed = bool(snapshot) and (snapshot.get("summary") or {}).get("baseline") not in (None, "")
        return await self.preview.build(config_revision=config_revision, engine_revision=engine_revision,
                                        baseline_override=baseline_override, baseline_confirmed_in_ledger=confirmed)


def _ctx(request: web.Request) -> WebContext:
    return request.app[CTX_KEY]


def _json(data: Any, status: int = 200) -> web.Response:
    return web.Response(status=status, text=jsonsafe.dumps(data), content_type="application/json")


async def _read_json(request: web.Request) -> Any:
    raw = await request.content.read(MAX_BODY + 1)
    if len(raw) > MAX_BODY:
        raise web.HTTPRequestEntityTooLarge(max_size=MAX_BODY, actual_size=len(raw))
    try:
        return json.loads(raw.decode("utf-8") or "{}", parse_float=Decimal)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise web.HTTPBadRequest(reason="bad json") from None


def _int_param(request: web.Request, name: str, default: int, lo: int, hi: int) -> int:
    raw = request.query.get(name)
    if raw is None:
        return default
    if not raw.isdigit():
        raise web.HTTPBadRequest(reason=f"bad {name}")
    return max(lo, min(hi, int(raw)))


# ---------------------------------------------------------------------------- pages
async def index(request: web.Request) -> web.StreamResponse:
    response = web.FileResponse(STATIC_DIR / "index.html")
    response.headers["Cache-Control"] = "no-store"
    return response


async def health(request: web.Request) -> web.Response:
    return _json({"ok": True})


# ---------------------------------------------------------------------------- session
async def login(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    body = await _read_json(request)
    token = body.get("token") if isinstance(body, dict) else None
    if ctx.access.locked_out():
        return json_error(429, "locked_out", "Слишком много неверных попыток входа. Подождите минуту.")
    if not ctx.access.verify(token):
        LOGGER.warning("Login rejected: invalid access token.")
        return json_error(401, "bad_token", "Неверный токен доступа.")
    session = ctx.sessions.create()
    response = _json({"csrf_token": session.csrf_token, "mode": ctx.mode})
    set_session_cookie(response, session)
    LOGGER.info("Session created.")
    return response


async def logout(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    ctx.sessions.drop(request["session"].session_id)
    response = _json({"ok": True})
    clear_session_cookie(response)
    return response


async def session_info(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    return _json({"csrf_token": request["session"].csrf_token, "mode": ctx.mode,
                  "engine_identity": ctx.engine_identity, "stale_after_s": ctx.stale_after_s,
                  "demo_actions": ctx.demo.actions if ctx.demo else None})


# ---------------------------------------------------------------------------- read side
async def state(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    snapshot = ctx.gateway.latest_snapshot()
    fresh = views.freshness(snapshot, ctx.clock(), ctx.stale_after_s)
    meta = None
    if snapshot:
        meta = {k: snapshot.get(k) for k in ("snapshot_version", "config_revision", "engine_revision",
                                             "committed_at")}
    return _json({
        "mode": ctx.mode,
        "engine_identity": ctx.engine_identity,
        "engine": views.engine_view(snapshot, fresh),
        "freshness": fresh,
        "snapshot": meta,
        "summary": views.summary_view(snapshot),
        "cell_map": views.cell_map(snapshot),
        "unmatched_evidence": (snapshot or {}).get("unmatched_evidence") or [],
        "errors": list((snapshot or {}).get("errors") or [])[-20:],
        "snapshot_commands": list((snapshot or {}).get("commands") or [])[-20:],
        "recent_commands": ctx.gateway.list_commands(limit=10),
        "host": ctx.host_status(),
    })


async def preview(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    snapshot = ctx.gateway.latest_snapshot()
    cfg_rev, eng_rev = views.snapshot_revisions(snapshot)
    baseline = None
    if "baseline" in request.query:
        try:
            baseline = parse_signed_decimal(request.query["baseline"])
        except ValueError as exc:
            return json_error(400, "bad_baseline", f"expected_initial_position: {exc}")
    return _json(await ctx.build_preview(config_revision=cfg_rev, engine_revision=eng_rev,
                                         baseline_override=baseline))


async def cells(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    snapshot = ctx.gateway.latest_snapshot()
    try:
        page = views.page_cells(snapshot, after=request.query.get("after"),
                                limit=_int_param(request, "limit", 60, 1, views.MAX_PAGE),
                                state=request.query.get("state") or None,
                                only_active=request.query.get("active") == "1")
    except ValueError:
        return json_error(400, "bad_cursor", "Некорректный курсор страницы.")
    page["snapshot"] = {k: (snapshot or {}).get(k) for k in ("snapshot_version", "config_revision",
                                                              "engine_revision", "committed_at")}
    return _json(page)


async def commands_list(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    before = request.query.get("before") or None
    if before is not None and not views.valid_lookup_id(before):
        return json_error(400, "bad_cursor", "Некорректный курсор.")
    limit = _int_param(request, "limit", 50, 1, views.MAX_PAGE)
    rows = ctx.gateway.list_commands(limit=limit + 1, before=before)
    next_cursor = str(rows[limit - 1]["id"]) if len(rows) > limit else None
    return _json({"commands": rows[:limit], "next_cursor": next_cursor})


async def command_get(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    command_id = request.match_info["command_id"]
    if not views.valid_lookup_id(command_id):
        return json_error(400, "bad_id", "Некорректный идентификатор.")
    row = ctx.gateway.get_command(command_id)
    if row is None:
        return json_error(404, "not_found", "Команда не найдена.")
    return _json({"command": row})


async def lookup(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    wanted = (request.query.get("id") or "").strip()
    if not views.valid_lookup_id(wanted):
        return json_error(400, "bad_id", "ID: 1–96 символов [0-9A-Za-z_:.-]; сравнивается как строка.")
    snapshot = ctx.gateway.latest_snapshot()
    return _json({
        "id": wanted,
        "snapshot_matches": views.lookup_in_snapshot(snapshot, wanted),
        "orders": ctx.gateway.find_orders(wanted),
        "trades": ctx.gateway.find_trades(wanted),
    })


async def audit(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    before = request.query.get("before") or None
    if before is not None and not views.valid_lookup_id(before):
        return json_error(400, "bad_cursor", "Некорректный курсор.")
    limit = _int_param(request, "limit", 50, 1, views.MAX_PAGE)
    rows = ctx.gateway.audit_events(limit=limit + 1, before=before)
    next_cursor = str(rows[limit - 1]["id"]) if len(rows) > limit else None
    return _json({"events": rows[:limit], "next_cursor": next_cursor})


# ---------------------------------------------------------------------------- write side
async def commands_post(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    outcome = await ctx.commands.submit(await _read_json(request))
    return _json(outcome.body, outcome.status)


async def keystore_get(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    return _json({"status": ctx.keystore.status(), "profiles": ctx.keystore.profiles()})


async def keystore_select(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    body = await _read_json(request)
    try:
        status = ctx.keystore.select(body.get("profile") if isinstance(body, dict) else None)
    except KeystoreError as exc:
        return json_error(422, "keystore", str(exc))
    return _json({"status": status})


async def keystore_unlock(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    body = await _read_json(request)
    password = body.pop("password", None) if isinstance(body, dict) else None
    try:
        status = ctx.keystore.unlock(password)
    except KeystoreError as exc:
        return json_error(422, "keystore", str(exc))
    finally:
        del password
    return _json({"status": status})


async def demo_action(request: web.Request) -> web.Response:
    ctx = _ctx(request)
    if ctx.demo is None:
        return json_error(404, "not_found", "Демо-режим выключен.")
    body = await _read_json(request)
    action = body.get("action") if isinstance(body, dict) else None
    if action not in ctx.demo.actions:
        return json_error(400, "bad_action", "Неизвестное демо-действие.")
    return _json({"action": action, "result": await ctx.demo.run(action)})


def create_app(ctx: WebContext) -> web.Application:
    app = web.Application(
        middlewares=[build_security_middleware(sessions=ctx.sessions, bind_host=ctx.bind_host, policy=ctx.policy)],
        client_max_size=MAX_BODY,
    )
    app[CTX_KEY] = ctx
    app.router.add_get("/", index)
    app.router.add_static("/static/", STATIC_DIR, show_index=False, follow_symlinks=False)
    app.router.add_get("/api/health", health)
    app.router.add_post("/api/login", login)
    app.router.add_post("/api/logout", logout)
    app.router.add_get("/api/session", session_info)
    app.router.add_get("/api/state", state)
    app.router.add_get("/api/preview", preview)
    app.router.add_get("/api/cells", cells)
    app.router.add_get("/api/commands", commands_list)
    app.router.add_post("/api/commands", commands_post)
    app.router.add_get("/api/commands/{command_id}", command_get)
    app.router.add_get("/api/lookup", lookup)
    app.router.add_get("/api/audit", audit)
    app.router.add_get("/api/keystore", keystore_get)
    app.router.add_post("/api/keystore/select", keystore_select)
    app.router.add_post("/api/keystore/unlock", keystore_unlock)
    if ctx.demo is not None:
        app.router.add_post("/api/demo/action", demo_action)
    return app
