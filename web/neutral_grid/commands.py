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
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, Union

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CommandKind
from web.neutral_grid import jsonsafe
from web.neutral_grid.gateway import EngineGateway
from web.neutral_grid.preview import parse_signed_decimal
from web.neutral_grid.views import engine_started, is_engine_active, snapshot_revisions

IDEMPOTENCY_KEY_RE = re.compile(r"[A-Za-z0-9_\-]{16,128}\Z")
MAX_NOTE = 500
# Operator audits are ``baseline_audit`` commands whose payload ``action`` selects what was audited (engine
# ``commands.AUDIT_ACTIONS``; the store accepts only ``contracts.CommandKind``). None of them resets the ledger.
AUDIT_ACTION_BASELINE = "baseline"
AUDIT_ACTIONS = (
    AUDIT_ACTION_BASELINE, "ack_late_evidence", "ack_history_conflict", "ack_retention_gap", "ack_risk_blocked",
    "resolve_unknown_submit",
)
# Audited operator actions beyond the base set (engine ``commands.EXTENDED_AUDIT_ACTIONS``): offered only when the
# committed snapshot shows they apply. Destructive retirement/migration/settlement also require a typed phrase;
# proof-bound add-only extension requires the exact published proof instead; external-entry adoption also binds
# the typed phrase published by its candidate.
EXTENDED_AUDIT_ACTIONS = (
    "retire_colliding_cid", "migrate_grid", "extend_grid", "extend_grid_with_external_entry",
    "settle_external_close",
)
CONFIRMATION_AUDIT_ACTIONS = ("retire_colliding_cid", "migrate_grid", "settle_external_close")
FREEZE_CID = "CID_ALLOCATION"
COMMAND_KINDS = {k.value: k.value for k in CommandKind}
MAX_CID = (1 << 48) - 1
_OPAQUE_RE = re.compile(r"[A-Za-z0-9_:.\-]{1,160}\Z")   # conflict set id / version fingerprints / conflict keys


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
    def __init__(self, gateway: EngineGateway, preview: PreviewBuilder,
                 engine_identity: Union[Dict[str, Any], Callable[[], Dict[str, Any]]],
                 config_baseline: Union[Optional[Decimal], Callable[[], Tuple[Optional[Decimal], Optional[str]]]] = None):
        self._gateway = gateway
        self._preview = preview
        self._identity = engine_identity if callable(engine_identity) else (lambda: dict(engine_identity))
        # (baseline, error): error set when the engine config itself is unavailable/untrusted
        self._config_baseline = (config_baseline if callable(config_baseline)
                                 else (lambda: (config_baseline, None)))
        self._lock = asyncio.Lock()

    @property
    def engine_identity(self) -> Dict[str, Any]:
        return dict(self._identity())

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
        supported = getattr(self._gateway, "supported_kinds", None)
        if supported is not None and kind_raw not in supported:
            return self._error(422, "unsupported_kind", "Хранилище движка не принимает эту команду.",
                               allowed=sorted(supported))

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
                normalized, blocked = await self._validate(kind_raw, payload, snapshot, cur_cfg, cur_eng)
            except ValueError as exc:
                return self._error(422, "invalid_payload", str(exc))
            if blocked is not None:
                return blocked
            row = self._gateway.enqueue_command(key, kind_raw, exp_cfg, exp_eng, normalized)
            if row.get("duplicate"):  # another process enqueued the same key first
                return self._replay(row, kind_raw, exp_cfg, exp_eng, payload)
            if row.get("status") == "CONFLICT":  # the store re-checked revisions/queued Start atomically
                result = row.get("result") or {}
                code = "start_already_queued" if result.get("reason") == "start_already_queued" else "stale_revision"
                fresh = self._gateway.latest_snapshot()
                cfg_now, eng_now = snapshot_revisions(fresh)
                outcome = await self._stale(cfg_now, eng_now, fresh,
                                            "Хранилище движка отклонило команду как конфликт; она не будет "
                                            "применена.", code=code)
                outcome.body["command"] = row
                outcome.body["engine_identity"] = self.engine_identity
                return outcome
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
        stored = {k: v for k, v in stored.items() if k != "material_id"}  # server-added at enqueue
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

    def confirmation_phrase(self, action: str, cid: Optional[str] = None) -> str:
        if action == "retire_colliding_cid":
            return f"СПИСАТЬ CID {cid}"
        if action == "settle_external_close":
            return f"SETTLE EXTERNAL CLOSE {self.engine_identity.get('grid_id')} AT FLAT 0"
        return f"МИГРАЦИЯ СЕТКИ {self.engine_identity.get('grid_id')}"

    def _extended_audit_blocker(self, normalized: Dict[str, Any], snapshot: Dict[str, Any]) -> Optional[CommandOutcome]:
        """D2-17: extended audits only when the committed snapshot shows they apply (fail closed otherwise)."""
        summary = snapshot.get("summary") or {}
        action = normalized.get("action")
        if action == "retire_colliding_cid":
            if FREEZE_CID not in (summary.get("freezes") or {}):
                return self._error(409, "audit_not_applicable",
                                   "Списание CID доступно только при заморозке CID_ALLOCATION (коллизия CID).")
            colliding = summary.get("colliding_cid")
            if colliding not in (None, "") and str(colliding) != normalized["cid"]:
                raise ValueError(f"Списать можно только CID из коллизии, о которой сообщил движок: {colliding}.")
        if action == "migrate_grid":
            blockers = summary.get("grid_mutation_blockers")
            if not isinstance(blockers, list) or blockers:
                return self._error(409, "audit_not_applicable",
                                   "Миграция сетки доступна только для полностью спокойной сетки "
                                   "(grid_mutation_blockers пуст и опубликован движком).", blockers=blockers)
        if action == "settle_external_close":
            state = snapshot.get("engine_state")
            if state not in ("STOPPED", "STOPPED_WITH_INVENTORY"):
                return self._error(409, "external_close_not_stopped",
                                   "Зачесть ручное закрытие можно только когда сетка полностью остановлена.",
                                   engine_state=state)
            candidate = summary.get("external_close_candidate")
            if not isinstance(candidate, dict):
                return self._error(409, "external_close_not_eligible",
                                   "Движок не опубликовал проверяемое ручное закрытие.", blockers=["NO_CANDIDATE"])
            blockers = candidate.get("blockers")
            if not isinstance(blockers, list) or blockers:
                return self._error(409, "external_close_not_eligible",
                                   "Доказательств недостаточно для точного зачёта ручного закрытия.",
                                   blockers=blockers)
            if normalized["proof_id"] != candidate.get("proof_id"):
                return self._error(409, "external_close_proof_changed",
                                   "Набор доказательств изменился после просмотра. Проверьте его заново.",
                                   proof_id=candidate.get("proof_id"))
        if action == "extend_grid":
            state = snapshot.get("engine_state")
            if state not in ("STOPPED", "STOPPED_WITH_INVENTORY"):
                return self._error(409, "grid_extension_not_stopped",
                                   "Расширить сетку можно только когда движок полностью остановлен.",
                                   engine_state=state)
            candidate = summary.get("grid_extension_candidate")
            if not isinstance(candidate, dict):
                return self._error(409, "grid_extension_not_eligible",
                                   "Движок не опубликовал проверяемое расширение сетки.",
                                   blockers=["NO_CANDIDATE"])
            blockers = candidate.get("blockers")
            if not isinstance(blockers, list) or blockers:
                return self._error(409, "grid_extension_not_eligible",
                                   "Условия безопасного расширения сетки не выполнены.", blockers=blockers)
            if normalized["proof_id"] != candidate.get("proof_id"):
                return self._error(409, "grid_extension_proof_changed",
                                   "План расширения изменился после просмотра. Проверьте его заново.",
                                   proof_id=candidate.get("proof_id"))
        if action == "extend_grid_with_external_entry":
            state = snapshot.get("engine_state")
            if state not in ("STOPPED", "STOPPED_WITH_INVENTORY"):
                return self._error(409, "grid_external_entry_not_stopped",
                                   "Принять внешнюю покупку можно только когда движок полностью остановлен.",
                                   engine_state=state)
            candidate = summary.get("grid_external_entry_candidate")
            if not isinstance(candidate, dict):
                return self._error(409, "grid_external_entry_not_eligible",
                                   "Движок не опубликовал проверяемое принятие внешней покупки.",
                                   blockers=["NO_CANDIDATE"])
            blockers = candidate.get("blockers")
            expected_confirmation = candidate.get("confirmation")
            if not isinstance(blockers, list) or blockers or not isinstance(expected_confirmation, str) \
                    or not expected_confirmation:
                return self._error(409, "grid_external_entry_not_eligible",
                                   "Условия безопасного принятия внешней покупки не выполнены.",
                                   blockers=blockers)
            if normalized["proof_id"] != candidate.get("proof_id"):
                return self._error(409, "grid_external_entry_proof_changed",
                                   "Доказательства внешней покупки изменились после просмотра. Проверьте их заново.",
                                   proof_id=candidate.get("proof_id"))
            if normalized["confirmation"] != expected_confirmation:
                raise ValueError(f"confirmation: введите точную фразу из опубликованного кандидата: "
                                 f"«{expected_confirmation}».")
        return None

    def _conflict_set_blocker(self, normalized: Dict[str, Any], snapshot: Dict[str, Any]) -> Optional[CommandOutcome]:
        """M1: the ack names the published conflict set; every key without a committed version has an explicit pick."""
        summary = snapshot.get("summary") or {}
        current = summary.get("conflict_set_id")
        conflicts = summary.get("history_conflicts")
        if not current or not isinstance(conflicts, list):
            return self._error(409, "conflict_set_unavailable",
                               "Движок не опубликовал набор конфликтов истории; подтверждение привязать не к чему.")
        if normalized["conflict_set_id"] != current:
            return self._error(409, "conflict_set_changed",
                               "Набор конфликтов изменился после просмотра (появились новые версии). Проверьте "
                               "свежий набор и подтвердите заново.",
                               conflict_set_id=current, history_conflicts=conflicts)
        accepted = normalized["accepted"]
        # Keys may repeat across streams (store_conflict ids vs active_evidence cells): iterate, never dict-by-key.
        unknown = sorted(set(accepted) - {str(c.get("key")) for c in conflicts if isinstance(c, dict)})
        if unknown:
            raise ValueError(f"accepted: ключи не из набора конфликтов: {unknown}.")
        for conflict in conflicts:
            if not isinstance(conflict, dict):
                continue
            key = str(conflict.get("key"))
            versions = conflict.get("versions") or []
            committed = [str(v.get("fingerprint")) for v in versions if v.get("committed")]
            fingerprints = {str(v.get("fingerprint")) for v in versions}
            if key not in accepted:
                if not committed:
                    raise ValueError(f"Для {key} нет зафиксированной версии: выберите принимаемую версию явно.")
                continue
            if committed and accepted[key] not in committed:
                # engine: LEDGER_CORRECTION_NOT_SUPPORTED (the ledger keeps what it committed, R6)
                raise ValueError(f"{key}: исправление зафиксированной версии не поддерживается — для этого ключа "
                                 "принимается только зафиксированная версия.")
            if accepted[key] not in fingerprints:
                raise ValueError(f"accepted[{key}]: такой версии нет среди показанных.")
        return None

    async def _start_material_blocker(self, snapshot: Dict[str, Any], cur_cfg: int,
                                      cur_eng: int) -> Optional[CommandOutcome]:
        """E-09: the baseline is confirmed only for the grid/rules the applied Start acknowledged."""
        applied = self._gateway.list_commands(limit=1, kind=CommandKind.START.value, status="APPLIED")
        acknowledged = ((applied[0].get("payload") or {}).get("material_id")) if applied else None
        if not acknowledged:
            return None  # a Start not issued through this UI: the engine binds it to its config fingerprint
        preview = await self._preview(config_revision=cur_cfg, engine_revision=cur_eng)
        if preview.get("material_id") != acknowledged:
            return self._error(409, "start_material_changed",
                               "Конфигурация или правила рынка изменились после подтверждённого «Старта»; "
                               "baseline не подтверждается. Остановите движок и выполните новый «Старт».",
                               preview=preview)
        return None

    def _check_config_baseline(self, typed: Decimal) -> None:
        """B is part of the config (NG-ARCH-003); the operator re-types it as the explicit confirmation."""
        baseline, error = self._config_baseline()
        if error:
            raise ValueError(f"Конфигурация движка недоступна ({error}): B сверить не с чем.")
        if baseline is None:
            raise ValueError("expected_initial_position не задан в конфигурации: старт невозможен.")
        if typed != baseline:
            raise ValueError(f"Введённый B={typed} не совпадает с expected_initial_position={baseline} "
                             "из конфигурации. Бот не принимает текущую позицию автоматически.")

    def _normalize_only(self, kind: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if kind == CommandKind.START.value:
            baseline = parse_signed_decimal(payload.get("expected_initial_position"))
            self._check_config_baseline(baseline)
            if payload.get("risk_acknowledged") is not True:
                raise ValueError("Нужно явно подтвердить риск (risk_acknowledged=true).")
            if payload.get("baseline_acknowledged") is not True:
                raise ValueError("Нужно явно подтвердить expected_initial_position (baseline_acknowledged=true).")
            preview_id = payload.get("preview_id")
            if not isinstance(preview_id, str) or not re.fullmatch(r"[0-9a-f]{24}", preview_id):
                raise ValueError("preview_id отсутствует: старт возможен только из просмотренного превью.")
            return {"expected_initial_position": format(baseline, "f"), "risk_acknowledged": True,
                    "baseline_acknowledged": True, "preview_id": preview_id}
        if kind == CommandKind.CONFIRM_BASELINE.value:
            baseline = parse_signed_decimal(payload.get("expected_initial_position"))
            self._check_config_baseline(baseline)
            if payload.get("confirm") is not True:
                raise ValueError("Нужно явное подтверждение (confirm=true).")
            return {"expected_initial_position": format(baseline, "f"), "confirm": True}
        if kind == CommandKind.BASELINE_AUDIT.value:
            action = payload.get("action", AUDIT_ACTION_BASELINE)
            if action not in AUDIT_ACTIONS + EXTENDED_AUDIT_ACTIONS:
                raise ValueError("Недопустимое действие аудита.")
            note = _note(payload)
            if not note:
                raise ValueError("Для аудита нужна причина (note) для журнала.")
            if payload.get("acknowledge") is not True:
                if action == "settle_external_close":
                    raise ValueError("Нужно подтвердить запись показанного внешнего закрытия с сохранением fills, "
                                     "CID, P&L и истории и без запуска бота (acknowledge=true).")
                if action == "extend_grid":
                    raise ValueError("Нужно подтвердить показанное add-only расширение с сохранением всех старых "
                                     "ячеек, циклов и TP (acknowledge=true).")
                if action == "extend_grid_with_external_entry":
                    raise ValueError("Нужно подтвердить принятие показанной внешней покупки без изменения baseline "
                                     "и без создания синтетических исполнений (acknowledge=true).")
                raise ValueError("Нужно подтвердить, что аудит проведён и не меняет обязательства ячеек "
                                 "(acknowledge=true).")
            normalized = {"action": action, "note": note, "acknowledge": True}
            if action == AUDIT_ACTION_BASELINE:
                normalized["observed_position"] = format(parse_signed_decimal(payload.get("observed_position")), "f")
            if action in ("resolve_unknown_submit", "retire_colliding_cid"):
                cid = payload.get("cid")
                if not isinstance(cid, str) or not cid.isdigit() or int(cid) > MAX_CID:
                    raise ValueError("cid: строка с 48-битным client order ID.")
                normalized["cid"] = cid
            if action in ("settle_external_close", "extend_grid", "extend_grid_with_external_entry"):
                proof_id = payload.get("proof_id")
                if not isinstance(proof_id, str) or not re.fullmatch(r"[0-9a-f]{64}", proof_id):
                    raise ValueError("proof_id: нужен SHA-256 опубликованного набора доказательств.")
                normalized["proof_id"] = proof_id
            if action == "extend_grid_with_external_entry":
                allowed = {"action", "proof_id", "note", "acknowledge", "confirmation"}
                forbidden = sorted(set(payload) - allowed)
                if forbidden:
                    raise ValueError(f"Недопустимые поля для proof-only команды: {forbidden}.")
                confirmation = payload.get("confirmation")
                if not isinstance(confirmation, str) or not confirmation:
                    raise ValueError("confirmation: нужна точная фраза из опубликованного кандидата.")
                normalized["confirmation"] = confirmation
            if action == "ack_history_conflict":
                # M1 (AC-40): bound to the exact conflict set the operator reviewed; accepted versions explicit.
                set_id = payload.get("conflict_set_id")
                if not isinstance(set_id, str) or not _OPAQUE_RE.fullmatch(set_id):
                    raise ValueError("conflict_set_id: нужен идентификатор просмотренного набора конфликтов.")
                accepted = payload.get("accepted") or {}
                if not isinstance(accepted, dict) or not all(
                        isinstance(k, str) and _OPAQUE_RE.fullmatch(k) and isinstance(v, str) and _OPAQUE_RE.fullmatch(v)
                        for k, v in accepted.items()):
                    raise ValueError("accepted: объект {ключ конфликта: отпечаток выбранной версии}.")
                expected = f"ПРИНЯТЬ НАБОР {set_id}"
                if payload.get("confirmation") != expected:
                    raise ValueError(f"Введите точную фразу подтверждения: «{expected}».")
                normalized.update(conflict_set_id=set_id, accepted=dict(accepted), confirmation=expected)
            if action in CONFIRMATION_AUDIT_ACTIONS:
                expected = self.confirmation_phrase(action, normalized.get("cid"))
                if payload.get("confirmation") != expected:
                    raise ValueError(f"Для этого действия введите точную фразу подтверждения: «{expected}».")
                normalized["confirmation"] = expected
            return normalized
        reason = _note(payload, "reason")
        return {"reason": reason} if reason else {}

    async def _validate(self, kind: str, payload: Dict[str, Any], snapshot: Optional[Dict[str, Any]],
                        cur_cfg: int, cur_eng: int) -> Tuple[Dict[str, Any], Optional[CommandOutcome]]:
        normalized = self._normalize_only(kind, payload)
        if kind != CommandKind.START.value:
            if snapshot is None:
                return normalized, self._error(409, "engine_not_started",
                                               "Движок ещё не опубликовал состояние; команда неприменима.")
            if kind == CommandKind.CONFIRM_BASELINE.value and engine_started(snapshot) is not True:
                # The baseline is confirmed only after a Start that carried the risk acknowledgement and a
                # checked preview_id was applied; otherwise confirm_baseline would bootstrap trading without them.
                return normalized, self._error(
                    409, "start_required", "Сначала отправьте «Старт» с подтверждением риска по превью; "
                    "baseline подтверждается только после применённого старта.")
            if kind == CommandKind.CONFIRM_BASELINE.value:
                return normalized, await self._start_material_blocker(snapshot, cur_cfg, cur_eng)
            if kind == CommandKind.BASELINE_AUDIT.value and normalized.get("action") == "ack_history_conflict":
                return normalized, self._conflict_set_blocker(normalized, snapshot)
            if kind == CommandKind.BASELINE_AUDIT.value:
                return normalized, self._extended_audit_blocker(normalized, snapshot)
            return normalized, None
        pending = self._gateway.list_commands(limit=1, kind=CommandKind.START.value, status="QUEUED")
        if pending:
            return normalized, self._error(
                409, "start_already_queued", "Команда старта для этого движка уже в очереди.",
                command=pending[0], engine_identity=self.engine_identity)
        if is_engine_active(snapshot):
            return normalized, self._error(
                409, "engine_already_running",
                "Движок с этой идентичностью уже работает; второй движок не создаётся.",
                engine_identity=self.engine_identity, engine_state=(snapshot or {}).get("engine_state"))
        preview = await self._preview(config_revision=cur_cfg, engine_revision=cur_eng)
        if preview.get("preview_id") != normalized["preview_id"]:
            outcome = await self._stale(cur_cfg, cur_eng, snapshot,
                                        "Превью устарело (изменились правила рынка или конфигурация). "
                                        "Проверьте свежее превью и подтвердите заново.", code="stale_preview")
            return normalized, outcome
        if preview.get("errors"):
            return normalized, self._error(422, "preview_invalid", "Конфигурация не прошла проверку.",
                                           errors=preview["errors"], preview=preview)
        normalized["material_id"] = preview.get("material_id")  # what the risk acknowledgement was about (E-09)
        return normalized, None
