"""Durable operator command semantics (NG-UI-001, NG-UI-003, NG-OPS-001/003).

The web UI and the CLI never mutate engine state directly: they enqueue a command row (unique
idempotency key + expected config/engine revisions) through ``NeutralGridStore.enqueue_command`` and the
single engine applies it at the start of its next tick (claim + effects + completion in one transaction).
A refresh/retry with the same key returns the original row; a command built from a stale preview gets
``CONFLICT`` (with the current revisions / latest snapshot version) and is never applied automatically.
"""
from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CommandKind, CommandStatus

# Operator audits are all ``baseline_audit`` commands (the store accepts only ``contracts.CommandKind``); the
# payload ``action`` selects what is audited. Every action writes an audit event and never resets the ledger.
AUDIT_ACTION_BASELINE = "baseline"                  # AC-30: drift / manual trades -> explicit rebase
AUDIT_ACTIONS = (
    AUDIT_ACTION_BASELINE,
    "ack_late_evidence",        # AC-42: operator audited late evidence; obligations stay
    "ack_history_conflict",     # AC-40/12: operator audited conflicting history rows
    "ack_retention_gap",        # AC-53: audited manual reconciliation; no reset / no rebaseline
    "ack_risk_blocked",         # NG-RISK-002: operator acknowledged a cap/obligation conflict
    "resolve_unknown_submit",   # AC-16/17: audited "did not land" for a zero-fill SUBMIT_UNKNOWN
)
# Audited operator actions of the launcher / CLI path that the web UI does not offer (yet): same semantics,
# accepted by the engine; ``AUDIT_ACTIONS`` stays the web contract.
EXTENDED_AUDIT_ACTIONS = (
    "retire_colliding_cid",     # AC-43: audited retire of a CID a foreign order owns; clears the CID freeze
    "migrate_grid",             # AC-52: audited replacement of a quiescent grid (old cycles kept, no reset)
    "extend_grid",              # audited add-only window extension retaining cells, cycles and baseline
    "extend_grid_with_external_entry",  # atomic add-only extension + proven manual entry adoption
    "settle_external_close",    # proof-bound accounting of one exact manual reduce-only close
)
ALL_AUDIT_ACTIONS = AUDIT_ACTIONS + EXTENDED_AUDIT_ACTIONS

ALL_KINDS = tuple(k.value for k in CommandKind)


@dataclass
class CommandOutcome:
    status: CommandStatus
    result: Dict[str, Any]
    audit: Optional[Tuple[str, Dict[str, Any]]] = None
    reload: bool = False             # the command changed durable ledger rows: rebuild the projection


def new_idempotency_key(prefix: str = "cmd") -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def validate_kind(kind: str, payload: Dict[str, Any]) -> Optional[str]:
    """Client-side validation before enqueueing (the engine re-validates at apply time)."""
    if kind not in ALL_KINDS:
        return f"unknown command kind {kind!r}"
    if kind == CommandKind.CONFIRM_BASELINE.value and "expected_initial_position" not in payload:
        return "confirm_baseline requires expected_initial_position"
    if kind == CommandKind.BASELINE_AUDIT.value:
        action = payload.get("action", AUDIT_ACTION_BASELINE)
        if action not in ALL_AUDIT_ACTIONS:
            return f"baseline_audit action must be one of {ALL_AUDIT_ACTIONS}"
        if action == AUDIT_ACTION_BASELINE and "observed_position" not in payload:
            return "baseline audit requires observed_position"
        if action == "resolve_unknown_submit" and "cid" not in payload:
            return "resolve_unknown_submit requires cid"
        if action == "settle_external_close" and "proof_id" not in payload:
            return "settle_external_close requires proof_id"
        if action == "extend_grid":
            proof_id = payload.get("proof_id")
            if not isinstance(proof_id, str) or not re.fullmatch(r"[0-9a-f]{64}", proof_id):
                return "extend_grid requires a 64-character lowercase hexadecimal proof_id"
            if payload.get("acknowledge") is not True:
                return "extend_grid requires acknowledge=true"
        if action == "extend_grid_with_external_entry":
            proof_id = payload.get("proof_id")
            if not isinstance(proof_id, str) or not re.fullmatch(r"[0-9a-f]{64}", proof_id):
                return "extend_grid_with_external_entry requires a 64-character lowercase hexadecimal proof_id"
            if payload.get("acknowledge") is not True or not isinstance(payload.get("confirmation"), str):
                return "extend_grid_with_external_entry requires acknowledge=true and confirmation"
    for value in payload.values():
        if isinstance(value, float):
            return "payload numbers must be strings (no float)"
    return None
