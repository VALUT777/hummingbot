"""Local-only access control for the neutral grid web backend (NG-UI-001, AC-49).

Layers, applied to every request by :func:`security_middleware`:

1. **Host check** (all requests): the ``Host`` header must name the loopback address/port the server
   actually listens on. This defeats DNS-rebinding pages that resolve an attacker domain to 127.0.0.1.
2. **Origin check** (state-changing requests): ``Origin`` must be present and equal to one of the
   server's own origins. Missing/foreign Origin -> 403.
3. **Session** (all ``/api`` routes except login/health): an unguessable random cookie
   (``HttpOnly; SameSite=Strict; Path=/``) created only after presenting the launcher's access token.
   No session -> 401.
4. **CSRF** (state-changing requests): ``X-CSRF-Token`` header must equal the per-session token that
   the page reads from ``/api/session``. Missing/invalid -> 403.

Secrets (keystore password, API key, access token, session ids) are never logged: the request log
written here contains method, path without query string, status and duration only.
"""
from __future__ import annotations

import hmac
import ipaddress
import logging
import re
import secrets
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, FrozenSet, Iterable, Optional, Tuple

from aiohttp import web

LOGGER = logging.getLogger("web.neutral_grid.security")

SESSION_COOKIE = "ngw_session"
CSRF_HEADER = "X-CSRF-Token"
UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
PUBLIC_API_PATHS = frozenset({"/api/login", "/api/health"})
MAX_SESSIONS = 64

NON_LOOPBACK_WARNING = (
    "ВНИМАНИЕ: сервер слушает не-loopback адрес. Встроенная защита рассчитана только на локальный "
    "доступ (session + CSRF + Origin, без TLS). Для удалённого доступа используйте SSH-туннель "
    "(ssh -L 8787:127.0.0.1:8787 host) либо обратный прокси с аутентификацией и TLS."
)


class BindRefused(ValueError):
    pass


def is_loopback_host(host: str) -> bool:
    candidate = host.strip().strip("[]")
    if candidate.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(candidate).is_loopback
    except ValueError:
        return False


def check_bind(host: str, allow_non_loopback: bool) -> Optional[str]:
    """Refuse a non-loopback bind unless explicitly allowed; return a warning text when allowed."""
    if is_loopback_host(host):
        return None
    if not allow_non_loopback:
        raise BindRefused(
            f"Адрес {host!r} не является loopback. По умолчанию разрешён только 127.0.0.1; "
            "для другого адреса нужен явный флаг --allow-non-loopback-bind (см. README)."
        )
    return NON_LOOPBACK_WARNING


@dataclass
class Session:
    session_id: str
    csrf_token: str
    created_at: float
    last_seen: float
    data: Dict[str, object] = field(default_factory=dict)


class SessionManager:
    """In-memory sessions: they die with the backend process, never persisted, never logged."""

    def __init__(self, idle_timeout_s: float = 12 * 3600, clock: Callable[[], float] = time.time):
        self._sessions: Dict[str, Session] = {}
        self._idle_timeout_s = idle_timeout_s
        self._clock = clock

    def create(self) -> Session:
        self._expire()
        if len(self._sessions) >= MAX_SESSIONS:
            oldest = min(self._sessions.values(), key=lambda s: s.last_seen)
            self._sessions.pop(oldest.session_id, None)
        now = self._clock()
        session = Session(secrets.token_urlsafe(32), secrets.token_urlsafe(32), now, now)
        self._sessions[session.session_id] = session
        return session

    def get(self, session_id: Optional[str]) -> Optional[Session]:
        if not session_id:
            return None
        self._expire()
        for known_id, session in self._sessions.items():
            if hmac.compare_digest(known_id.encode(), session_id.encode()):
                session.last_seen = self._clock()
                return session
        return None

    def drop(self, session_id: Optional[str]) -> None:
        if session_id:
            self._sessions.pop(session_id, None)

    def _expire(self) -> None:
        cutoff = self._clock() - self._idle_timeout_s
        for session_id in [k for k, s in self._sessions.items() if s.last_seen < cutoff]:
            self._sessions.pop(session_id, None)


class AccessGate:
    """Launcher access token (printed once on the TTY, like Jupyter). Rate-limited on failures."""

    def __init__(self, token: Optional[str] = None, clock: Callable[[], float] = time.time,
                 max_failures: int = 10, window_s: float = 60.0):
        self.token = token or secrets.token_urlsafe(24)
        self._clock = clock
        self._failures: list = []
        self._max_failures = max_failures
        self._window_s = window_s

    def locked_out(self) -> bool:
        now = self._clock()
        self._failures = [t for t in self._failures if t > now - self._window_s]
        return len(self._failures) >= self._max_failures

    def verify(self, candidate: object) -> bool:
        if self.locked_out():
            return False
        ok = isinstance(candidate, str) and hmac.compare_digest(candidate.encode(), self.token.encode())
        if not ok:
            self._failures.append(self._clock())
        return ok


@dataclass(frozen=True)
class SecurityPolicy:
    extra_hostnames: FrozenSet[str] = frozenset()

    def allowed_hostnames(self, bind_host: str) -> FrozenSet[str]:
        names = {"127.0.0.1", "localhost", "[::1]"}
        host = bind_host.strip()
        if host and host not in ("0.0.0.0", "::"):
            names.add(f"[{host}]" if ":" in host and not host.startswith("[") else host)
        return frozenset(names | set(self.extra_hostnames))


def _local_port(request: web.Request) -> Optional[int]:
    transport = request.transport
    sockname = transport.get_extra_info("sockname") if transport is not None else None
    if isinstance(sockname, (tuple, list)) and len(sockname) >= 2:
        return int(sockname[1])
    return None


def allowed_hosts_and_origins(request: web.Request, bind_host: str,
                              policy: SecurityPolicy) -> Tuple[FrozenSet[str], FrozenSet[str]]:
    port = _local_port(request)
    names = policy.allowed_hostnames(bind_host)
    hosts = set()
    origins = set()
    for name in names:
        if port is None:
            continue
        hosts.add(f"{name}:{port}")
        origins.add(f"http://{name}:{port}")
    return frozenset(hosts), frozenset(origins)


def json_error(status: int, code: str, message: str, **extra: object) -> web.Response:
    from web.neutral_grid import jsonsafe
    body = {"error": code, "message": message}
    body.update(extra)
    safe = redact_tree(jsonsafe.make_safe(body))
    return web.Response(status=status, text=jsonsafe.dumps(safe), content_type="application/json")


SECURITY_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
        "connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'none'; "
        "frame-ancestors 'none'; form-action 'self'"
    ),
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Cross-Origin-Opener-Policy": "same-origin",
    "Cross-Origin-Resource-Policy": "same-origin",
    "Permissions-Policy": "camera=(), microphone=(), geolocation=(), payment=()",
}


def _session_from_request(request: web.Request, sessions: SessionManager) -> Optional[Session]:
    return sessions.get(request.cookies.get(SESSION_COOKIE))


def build_security_middleware(*, sessions: SessionManager, bind_host: str, policy: SecurityPolicy,
                              request_log: Optional[logging.Logger] = None
                              ) -> Callable[[web.Request, Callable[[web.Request], Awaitable[web.StreamResponse]]],
                                            Awaitable[web.StreamResponse]]:
    log = request_log or logging.getLogger("web.neutral_grid.access")

    @web.middleware
    async def security_middleware(request: web.Request, handler):
        started = time.monotonic()
        response = await _guarded(request, handler)
        for name, value in SECURITY_HEADERS.items():
            response.headers.setdefault(name, value)
        if request.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        # Path only: never the query string, headers, cookies or body.
        log.info("%s %s -> %s (%.0f ms)", request.method, request.rel_url.path, response.status,
                 (time.monotonic() - started) * 1000)
        return response

    async def _guarded(request: web.Request, handler) -> web.StreamResponse:
        hosts, origins = allowed_hosts_and_origins(request, bind_host, policy)
        host = request.headers.get("Host", "")
        if host not in hosts:
            return json_error(403, "bad_host", "Недопустимый заголовок Host (защита от DNS rebinding).")
        unsafe = request.method in UNSAFE_METHODS
        if unsafe:
            origin = request.headers.get("Origin")
            if origin is None or origin not in origins:
                return json_error(403, "bad_origin", "Запрос с чужого или неизвестного Origin отклонён.")
            content_type = request.headers.get("Content-Type", "").split(";")[0].strip().lower()
            if content_type != "application/json":
                return json_error(415, "json_required", "Команды принимаются только как application/json.")
        elif request.headers.get("Sec-Fetch-Site") == "cross-site":
            return json_error(403, "cross_site", "Межсайтовые запросы отклонены.")
        if request.path.startswith("/api/") and request.path not in PUBLIC_API_PATHS:
            session = _session_from_request(request, sessions)
            if session is None:
                return json_error(401, "no_session", "Нет сессии: откройте ссылку с токеном доступа из терминала.")
            if unsafe:
                supplied = request.headers.get(CSRF_HEADER, "")
                if not supplied or not hmac.compare_digest(supplied.encode(), session.csrf_token.encode()):
                    return json_error(403, "csrf", "Отсутствует или неверен CSRF-токен.")
            request["session"] = session
        try:
            return await handler(request)
        except web.HTTPException as exc:
            if exc.status < 400:
                raise
            return json_error(exc.status, "http_error", exc.reason or "Ошибка запроса")

    return security_middleware


def set_session_cookie(response: web.StreamResponse, session: Session) -> None:
    response.set_cookie(SESSION_COOKIE, session.session_id, httponly=True, samesite="Strict", path="/")


def clear_session_cookie(response: web.StreamResponse) -> None:
    response.del_cookie(SESSION_COOKIE, path="/")


def redact_text(text: str, secret_values: Iterable[str]) -> str:
    """Replace any known secret value in ``text`` (defence in depth for error messages)."""
    redacted = text
    for secret in secret_values:
        if secret and len(secret) >= 4:
            redacted = redacted.replace(secret, "[скрыто]")
    return redacted


# ---------------------------------------------------------------------------------------------- free-text redaction
# Engine-provided free text (errors, reasons, blockers, audit details, command results) may quote an exception
# that embeds a URL with an auth token. Defence in depth (C1): strip URL query strings and any secret-looking
# key/value or opaque blob before the text leaves the backend. Ids (pure digits) and short codes are kept.
REDACTED = "[скрыто]"
_URL_QUERY = re.compile(r"(https?://[^\s?#\"'<>]+)\?[^\s\"'<>]*")
_SECRET_KV = re.compile(
    r"(?i)\b((?:x-)?(?:auth(?:orization)?|(?:access_|refresh_|auth_)?token|api[_-]?key|apikey|secret|password|passwd"
    r"|signature|sig|session|private[_-]?key|credential))(['\"]?\s*[=:]\s*['\"]?)(?:bearer\s+)?([^\s&'\",;}\])]+)")
_BEARER = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+")
_HEX_BLOB = re.compile(r"\b(?:0x)?[0-9a-fA-F]{40,}\b")
_OPAQUE_BLOB = re.compile(r"(?<![A-Za-z0-9_+/=-])(?=[A-Za-z0-9_+/=-]*[A-Za-z])(?=[A-Za-z0-9_+/=-]*[0-9])"
                          r"[A-Za-z0-9_+/=-]{32,}(?![A-Za-z0-9_+/=-])")


def redact_free_text(text: str) -> str:
    """Remove URL query strings, secret key/values, bearer tokens and opaque/hex blobs from free text."""
    redacted = _URL_QUERY.sub(lambda m: m.group(1) + "?" + REDACTED, text)
    redacted = _SECRET_KV.sub(lambda m: m.group(1) + m.group(2) + REDACTED, redacted)
    redacted = _BEARER.sub("Bearer " + REDACTED, redacted)
    redacted = _HEX_BLOB.sub(REDACTED, redacted)
    return _OPAQUE_BLOB.sub(REDACTED, redacted)


# Keys whose values (recursively) are engine/operator free text, never ids, cursors, prices or fingerprints.
FREE_TEXT_KEYS = frozenset({
    "message", "reasons", "entry_blockers", "tp_blockers", "admission_blocker", "persistence_error", "fatal_reason",
    "incomplete_reason", "warning", "blocker", "detail", "note", "result", "errors", "freezes", "banner",
    "last_tick_error", "stop_reason", "pause_reason", "reason", "cancel_reason", "transport", "state_reason",
    "error", "store_blockers", "config_error", "manual_reconcile_reason", "outcome_detail",
})


# Identifier keys stay exact even inside a free-text container (e.g. an engine result carrying the 32-hex
# conflict_set_id or version fingerprints): they are digests/ids, never free text, and must round-trip.
EXACT_KEYS = frozenset({
    "fingerprint", "accepted", "noise", "key", "keys", "cid", "stream", "committed", "conflict_set_id",
    "acknowledged", "resolved_conflicts", "retired_cid", "preview_id", "material_id", "cell_id", "trade_id_str",
    "accepted_trades",
})


def _exact_key(key: str) -> bool:
    return key in EXACT_KEYS or key.endswith("_id") or key.endswith("_id_str") or key.endswith("_cid")


def redact_tree(value, key: str = "", inside: bool = False):
    """Redact every string under a free-text key; ids, digests, cursors and decimals are left exact."""
    if key in FREE_TEXT_KEYS:
        inside = True
    elif _exact_key(key):
        inside = False
    if isinstance(value, str):
        return redact_free_text(value) if inside else value
    if isinstance(value, dict):
        return {k: redact_tree(v, str(k), inside) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_tree(v, key, inside) for v in value]
    return value
