"""The only two capabilities the web backend has over the engine: read committed state, enqueue commands.

``EngineGateway`` is the boundary the HTTP layer depends on. The production implementation wraps the
engine's SQLite store (``store.latest_snapshot()`` / read-only queries / ``store.enqueue_command``);
tests may provide an in-memory fake with the same shape. No method here may call the exchange, mutate
ledger state or return credentials.

Command rows are plain dicts::

    {id, idempotency_key, kind, expected_config_revision, expected_engine_revision, payload,
     status (QUEUED|APPLIED|REJECTED|CONFLICT), result, created_at, applied_at}
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol


class EngineGateway(Protocol):
    def latest_snapshot(self) -> Optional[Dict[str, Any]]:
        """Latest *committed* snapshot (parsed JSON dict, ids/decimals as strings) or None."""

    def enqueue_command(self, idempotency_key: str, kind: str, expected_config_revision: int,
                        expected_engine_revision: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Durably enqueue a command; the same idempotency key returns the original row."""

    def get_command(self, command_id: str) -> Optional[Dict[str, Any]]:
        """Command row by its (string) id."""

    def get_command_by_key(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        """Command row by idempotency key."""

    def list_commands(self, *, limit: int, before: Optional[str] = None, kind: Optional[str] = None,
                      status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Newest first. ``before`` is an exclusive command id cursor (string)."""

    def find_orders(self, id_str: str) -> List[Dict[str, Any]]:
        """Order rows whose client or exchange id equals ``id_str`` exactly (string compare)."""

    def find_trades(self, id_str: str) -> List[Dict[str, Any]]:
        """Own trade legs whose trade id / order ids equal ``id_str`` exactly."""

    def audit_events(self, *, limit: int, before: Optional[str] = None) -> List[Dict[str, Any]]:
        """Newest first operator/engine audit events (no secrets are ever stored there)."""


ACTIVE_ENGINE_STATES = frozenset({
    "BOOTSTRAPPING", "RECONCILING", "NORMAL", "DEGRADED", "PAUSED", "RISK_BLOCKED", "FROZEN", "STOPPING",
})
TERMINAL_ENGINE_STATES = frozenset({"STOPPED", "STOPPED_WITH_INVENTORY"})


MAX_CID = (1 << 48) - 1
_SCAN_LIMIT = 5000


def _ms_to_s(value: Optional[int]) -> Optional[float]:
    return None if value is None else value / 1000.0


def command_to_dict(record: Any) -> Dict[str, Any]:
    """Store ``CommandRecord`` -> API dict (ids as strings, times in unix seconds)."""
    status = getattr(record.status, "value", record.status)
    return {
        "id": str(record.id), "idempotency_key": record.idempotency_key, "kind": record.kind,
        "expected_config_revision": record.expected_config_revision,
        "expected_engine_revision": record.expected_engine_revision,
        "payload": dict(record.payload or {}), "status": status, "result": record.result,
        "created_at": _ms_to_s(record.created_at_ms), "applied_at": _ms_to_s(record.applied_at_ms),
        "claim_count": getattr(record, "claim_count", None),
        "duplicate": bool(getattr(record, "duplicate", False)),
        "request_mismatch": bool(getattr(record, "request_mismatch", False)),
    }


def _plain(value: Any) -> Any:
    """Dataclass record -> JSON-ready dict; Decimals stay Decimal (jsonsafe renders them as strings)."""
    import dataclasses
    from enum import Enum

    if dataclasses.is_dataclass(value):
        return {f.name: _plain(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


class StoreGateway:
    """``EngineGateway`` over the engine's SQLite store opened with ``open_command_client``.

    That handle can read committed rows and may only INSERT into ``commands`` (SQLite authorizer): the web
    backend is never a ledger writer. Snapshots come from ``store.latest_snapshot()`` only.
    """

    def __init__(self, store: Any):
        self._store = store

    @classmethod
    def open(cls, path: Any) -> "StoreGateway":
        from hummingbot.strategy_v2.executors.neutral_grid_executor.store import NeutralGridStore
        return cls(NeutralGridStore.open_command_client(path))

    def close(self) -> None:
        self._store.close()

    @property
    def supported_kinds(self) -> frozenset:
        from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CommandKind
        return frozenset(k.value for k in CommandKind)

    def latest_snapshot(self) -> Optional[Dict[str, Any]]:
        stored = self._store.latest_snapshot()
        return None if stored is None else stored.payload

    def enqueue_command(self, idempotency_key: str, kind: str, expected_config_revision: int,
                        expected_engine_revision: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        record = self._store.enqueue_command(idempotency_key, kind, expected_config_revision,
                                             expected_engine_revision, payload)
        return command_to_dict(record)

    def get_command(self, command_id: str) -> Optional[Dict[str, Any]]:
        if not str(command_id).isdigit():
            return None
        record = self._store.get_command(command_id=int(command_id))
        return None if record is None else command_to_dict(record)

    def get_command_by_key(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        record = self._store.get_command(idempotency_key=idempotency_key)
        return None if record is None else command_to_dict(record)

    def list_commands(self, *, limit: int, before: Optional[str] = None, kind: Optional[str] = None,
                      status: Optional[str] = None) -> List[Dict[str, Any]]:
        fetch = limit if (before is None and kind is None) else _SCAN_LIMIT
        rows = [command_to_dict(r) for r in self._store.list_commands(limit=fetch, status=status)]
        if before is not None:
            if not before.isdigit():
                return []
            rows = [r for r in rows if int(r["id"]) < int(before)]
        if kind is not None:
            rows = [r for r in rows if r["kind"] == kind]
        return rows[:limit]

    def find_orders(self, id_str: str) -> List[Dict[str, Any]]:
        found: List[Dict[str, Any]] = []
        if id_str.isdigit() and int(id_str) <= MAX_CID:
            cid = int(id_str)
            leg, order = self._store.leg(cid), self._store.order(cid)
            if leg is not None or order is not None:
                found.append({"match": "client_order_id", "leg": _plain(leg), "order": _plain(order)})
        for leg in self._store.legs()[-_SCAN_LIMIT:]:
            order = self._store.order(leg.cid)
            if order is not None and id_str in (order.exchange_order_id, order.order_index) \
                    and not any(f["leg"] and f["leg"]["cid"] == leg.cid for f in found):
                found.append({"match": "exchange_order_id", "leg": _plain(leg), "order": _plain(order)})
        return found

    def find_trades(self, id_str: str) -> List[Dict[str, Any]]:
        fills = self._store.fills()[-_SCAN_LIMIT:]
        found = [dict(_plain(f), match="fill") for f in fills
                 if id_str in (f.trade_id_str, f.own_exchange_order_id) or str(f.cid) == id_str]
        for rec in self._store.unmatched_evidence(include_resolved=True):
            if id_str in {str(v) for v in (rec.payload or {}).values() if isinstance(v, (str, int))}:
                found.append({"match": "unmatched_evidence", "inbox_id": rec.id, "stream": rec.stream,
                              "status": rec.status, "payload": rec.payload, "detail": rec.detail,
                              "received_at": _ms_to_s(rec.received_at_ms)})
        return found

    def audit_events(self, *, limit: int, before: Optional[str] = None) -> List[Dict[str, Any]]:
        fetch = limit if before is None else _SCAN_LIMIT
        rows = [{"id": str(e.id), "at": _ms_to_s(e.at_ms), "kind": e.kind, "actor": e.actor,
                 "engine_revision": e.engine_revision, "detail": e.payload}
                for e in self._store.audit_events(limit=fetch)]
        if before is not None:
            rows = [r for r in rows if before.isdigit() and int(r["id"]) < int(before)]
        return rows[:limit]
