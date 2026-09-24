"""State-changing command intake (NG-UI-001/003, AC-47).

Every command carries a client-generated unique ``idempotency_key`` plus the ``config_revision`` /
``engine_revision`` the operator was looking at. The service never performs the action itself — it
only validates and enqueues a durable command row for the single engine, then returns that committed
row. The engine applies it on its own tick and records APPLIED / REJECTED / CONFLICT.

* same key + same request -> the original row (refresh/retry returns the same result, 200);
* same key + different request -> 422, nothing enqueued;
* stale expected revisions -> 409 with a fresh preview, nothing enqueued (never auto-applied);
* Start while a Start is queued or the engine for this identity is active -> 409 carrying the
  existing command / engine identity; a second engine is never created (AC-47).

A single asyncio lock serialises intake so concurrent requests cannot interleave the
check-then-enqueue sequence inside this process; the store's unique key and the engine host's
single-writer lock cover everything outside it.
"""
from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CommandKind
from web.neutral_grid import jsonsafe
from web.neutral_grid.gateway import ACTIVE_ENGINE_STATES, EngineGateway
from web.neutral_grid.preview import parse_signed_decimal
from web.neutral_grid.views import snapshot_revisions

IDEMPOTENCY_KEY_RE = re.compile(r"[A-Za-z0-9_\-]{16,128}\Z")
MAX_NOTE = 500
COMMAND_KINDS = {k.value: k for k in CommandKind}


@dataclass(frozen=True)
class CommandOutcome:
    status: int
    body: Dict[str, Any]


PreviewBuilder = Callable[..., Awaitable[Dict[str, Any]]]


def _parse_revision(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(label)
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.isdigit():
        parsed = int(value)
    else:
        raise ValueError(label)
    if parsed < 0:
        raise ValueError(label)
    return parsed


def _note(payload: Dict[str, Any], field: str = "note") -> Optional[str]:
    value = payload.get(field)
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > MAX_NOTE:
        raise ValueError(f"{field}: строка не длиннее {MAX_NOTE} символов")
    return value.strip()


class CommandService:
    def __init__(self, gateway: EngineGateway, preview: PreviewBuilder, engine_identity: Dict[str, Any]):
        self._gateway = gateway
        self._preview = preview
        self._identity = dict(engine_identity)
        self._lock = asyncio.Lock()

    @property
    def engine_identity(self) -> Dict[str, Any]:
        return dict(self._identity)

    async def submit(self, body: Any) -> CommandOutcome:
        if not isinstance(body, dict):
            return self._error(400, "bad_request", "Тело запроса должно быть JSON-объектом.")
        kind_raw = body.get("kind")
        if kind_raw not in COMMAND_KINDS:
            return self._error(400, "bad_kind", "Неизвестная команда.", allowed=sorted(COMMAND_KINDS))
        key = body.get("idempotency_key")
        if not isinstance(key, str) or not IDEMPOTENCY_KEY_RE.fullmatch(key):
            return self._error(400, "bad_idempotency_key",
                               "idempotency_key: 16–128 символов [A-Za-z0-9_-], уникальный для каждого действия.")
        try:
            exp_cfg = _parse_revision(body.get("expected_config_revision"), "expected_config_revision")
            exp_eng = _parse_revision(body.get("expected_engine_revision"), "expected_engine_revision")
        except ValueError as exc:
            return self._error(400, "bad_revision", f"{exc}: ожидается неотрицательное целое.")
        payload = body.get("payload") or {}
        if not isinstance(payload, dict):
            return self._error(400, "bad_payload", "payload должен быть объектом.")

        async with self._lock:
            existing = self._gateway.get_command_by_key(key)
            if existing is not None:
                return self._replay(existing, kind_raw, exp_cfg, exp_eng, payload)
            snapshot = self._gateway.latest_snapshot()
            cur_cfg, cur_eng = snapshot_revisions(snapshot)
            if (exp_cfg, exp_eng) != (cur_cfg, cur_eng):
                return await self._stale(cur_cfg, cur_eng, snapshot,
                                         "Состояние изменилось с момента просмотра: проверьте свежие данные "
                                         "и подтвердите заново.")
            try:
                normalized, blocked = await self._validate(COMMAND_KINDS[kind_raw], payload, snapshot,
                                                           cur_cfg, cur_eng)
            except ValueError as exc:
                return self._error(422, "invalid_payload", str(exc))
            if blocked is not None:
                return blocked
            row = self._gateway.enqueue_command(key, kind_raw, exp_cfg, exp_eng, normalized)
        return CommandOutcome(202, {"command": row, "replay": False, "engine_identity": self.engine_identity,
                                    "note": "Команда записана в очередь движка; результат появится после её "
                                            "применения движком."})

    # ------------------------------------------------------------------ helpers
    def _replay(self, row: Dict[str, Any], kind: str, exp_cfg: int, exp_eng: int,
                payload: Dict[str, Any]) -> CommandOutcome:
        same = (row.get("kind") == kind
                and int(row.get("expected_config_revision")) == exp_cfg
                and int(row.get("expected_engine_revision")) == exp_eng
                and self._payload_matches(kind, row.get("payload") or {}, payload))
        if not same:
            return self._error(422, "idempotency_key_reused",
                               "Этот idempotency_key уже использован для другой команды.", command=row)
        return CommandOutcome(200, {"command": row, "replay": True, "engine_identity": self.engine_identity})

    def _payload_matches(self, kind: str, stored: Dict[str, Any], incoming: Dict[str, Any]) -> bool:
        try:
            normalized = self._normalize_only(COMMAND_KINDS[kind], incoming)
        except ValueError:
            return False
        return jsonsafe.canonical(stored) == jsonsafe.canonical(normalized)

    async def _stale(self, cur_cfg: int, cur_eng: int, snapshot: Optional[Dict[str, Any]],
                     message: str, code: str = "stale_revision") -> CommandOutcome:
        preview = await self._preview(config_revision=cur_cfg, engine_revision=cur_eng)
        return CommandOutcome(409, {
            "error": code, "message": message,
            "current": {"config_revision": cur_cfg, "engine_revision": cur_eng,
                        "engine_state": (snapshot or {}).get("engine_state"),
                        "committed_at": (snapshot or {}).get("committed_at")},
            "preview": preview,
        })

    @staticmethod
    def _error(status: int, code: str, message: str, **extra: Any) -> CommandOutcome:
        body = {"error": code, "message": message}
        body.update(extra)
        return CommandOutcome(status, body)

    def _normalize_only(self, kind: CommandKind, payload: Dict[str, Any]) -> Dict[str, Any]:
        if kind == CommandKind.START:
            baseline = parse_signed_decimal(payload.get("expected_initial_position"))
            if payload.get("risk_acknowledged") is not True:
                raise ValueError("Нужно явно подтвердить риск (risk_acknowledged=true).")
            if payload.get("baseline_acknowledged") is not True:
                raise ValueError("Нужно явно подтвердить expected_initial_position (baseline_acknowledged=true).")
            preview_id = payload.get("preview_id")
            if not isinstance(preview_id, str) or not re.fullmatch(r"[0-9a-f]{24}", preview_id):
                raise ValueError("preview_id отсутствует: старт возможен только из просмотренного превью.")
            return {"expected_initial_position": format(baseline, "f"), "risk_acknowledged": True,
                    "baseline_acknowledged": True, "preview_id": preview_id}
        if kind == CommandKind.CONFIRM_BASELINE:
            baseline = parse_signed_decimal(payload.get("expected_initial_position"))
            if payload.get("confirm") is not True:
                raise ValueError("Нужно явное подтверждение (confirm=true).")
            return {"expected_initial_position": format(baseline, "f"), "confirm": True}
        if kind == CommandKind.BASELINE_AUDIT:
            observed = parse_signed_decimal(payload.get("observed_position"))
            note = _note(payload)
            if not note:
                raise ValueError("Для аудита baseline нужна причина (note).")
            if payload.get("acknowledge") is not True:
                raise ValueError("Нужно подтвердить, что аудит не меняет обязательства ячеек (acknowledge=true).")
            return {"observed_position": format(observed, "f"), "note": note, "acknowledge": True}
        reason = _note(payload, "reason")
        return {"reason": reason} if reason else {}

    async def _validate(self, kind: CommandKind, payload: Dict[str, Any], snapshot: Optional[Dict[str, Any]],
                        cur_cfg: int, cur_eng: int) -> Tuple[Dict[str, Any], Optional[CommandOutcome]]:
        normalized = self._normalize_only(kind, payload)
        if kind != CommandKind.START:
            if snapshot is None:
                return normalized, self._error(409, "engine_not_started",
                                               "Движок ещё не опубликовал состояние; команда неприменима.")
            return normalized, None
        pending = self._gateway.list_commands(limit=1, kind=CommandKind.START.value, status="QUEUED")
        if pending:
            return normalized, self._error(
                409, "start_already_queued", "Команда старта для этого движка уже в очереди.",
                command=pending[0], engine_identity=self.engine_identity)
        state = str((snapshot or {}).get("engine_state") or "")
        if state in ACTIVE_ENGINE_STATES:
            return normalized, self._error(
                409, "engine_already_running",
                "Движок с этой идентичностью уже работает; второй движок не создаётся.",
                engine_identity=self.engine_identity, engine_state=state)
        baseline = Decimal(normalized["expected_initial_position"])
        preview = await self._preview(config_revision=cur_cfg, engine_revision=cur_eng, baseline_override=baseline)
        if preview.get("preview_id") != normalized["preview_id"]:
            outcome = await self._stale(cur_cfg, cur_eng, snapshot,
                                        "Превью устарело (изменились правила рынка или конфигурация). "
                                        "Проверьте свежее превью и подтвердите заново.", code="stale_preview")
            return normalized, outcome
        if preview.get("errors"):
            return normalized, self._error(422, "preview_invalid", "Конфигурация не прошла проверку.",
                                           errors=preview["errors"], preview=preview)
        return normalized, None
