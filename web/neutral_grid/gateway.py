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

    def get_command(self, command_id: str) -> Optional[Dict[str, Any]]: ...

    def get_command_by_key(self, idempotency_key: str) -> Optional[Dict[str, Any]]: ...

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
