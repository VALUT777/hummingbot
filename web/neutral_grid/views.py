"""Pure functions turning a committed snapshot into API views.

Honesty rules (AC-48):

* the engine state shown is *only* the committed ``engine_state`` of the latest snapshot;
* no snapshot -> ``UNKNOWN`` ("нет данных"), never NORMAL/STOPPED inferred from a live process;
* a snapshot older than ``stale_after_s`` -> ``display_state = "STALE"`` with the last known state kept
  as secondary information, never presented as current;
* cell state is copied from the snapshot; nothing is derived from the current market price.
"""
from __future__ import annotations

import base64
import binascii
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Tuple

from web.neutral_grid.gateway import ACTIVE_ENGINE_STATES

KNOWN_ENGINE_STATES = frozenset({
    "BOOTSTRAPPING", "RECONCILING", "NORMAL", "DEGRADED", "PAUSED", "RISK_BLOCKED", "FROZEN", "STOPPING",
    "STOPPED", "STOPPED_WITH_INVENTORY", "STOP_UNCERTAIN",
})
_ID_RE = re.compile(r"[0-9A-Za-z_:.\-]{1,96}\Z")
MAX_PAGE = 200


# Timestamps (unix s) and durations (s) the engine may serialize as strings; converted for display only.
# They are never quantities or ids, so a float is harmless here (ids/decimals stay exact strings).
DISPLAY_TIME_KEYS = frozenset({
    "committed_at", "at", "last_full_scan_at", "last_commit_at", "lag_s", "queue_age_s", "fetched_at",
    "created_at", "applied_at", "last_latency_s", "max_latency_s", "slo_s", "baseline_confirmed_at",
})
_NUMERIC_RE = re.compile(r"-?[0-9]+(\.[0-9]+)?\Z")


def for_display(value: Any, key: str = "") -> Any:
    """Deep copy of a snapshot fragment with time fields as numbers and GTT expiry (ms) as ``expiry_at``."""
    if isinstance(value, dict):
        out = {k: for_display(v, str(k)) for k, v in value.items()}
        expiry = value.get("expiry")
        if isinstance(expiry, str) and expiry.isdigit():
            out["expiry_at"] = int(expiry) / 1000.0
        elif isinstance(expiry, int) and not isinstance(expiry, bool):
            out["expiry_at"] = expiry / 1000.0
        return out
    if isinstance(value, list):
        return [for_display(v, key) for v in value]
    if key in DISPLAY_TIME_KEYS:
        if isinstance(value, str) and _NUMERIC_RE.fullmatch(value):
            return float(value)
        if isinstance(value, Decimal) and value.is_finite():  # store JSON floats are parsed as Decimal
            return float(value)
    return value


def snapshot_revisions(snapshot: Optional[Dict[str, Any]]) -> Tuple[int, int]:
    if not snapshot:
        return 0, 0
    return int(snapshot.get("config_revision") or 0), int(snapshot.get("engine_revision") or 0)


def freshness(snapshot: Optional[Dict[str, Any]], now: float, stale_after_s: float) -> Dict[str, Any]:
    if not snapshot or snapshot.get("committed_at") is None:
        return {"has_snapshot": False, "age_s": None, "stale": True, "stale_after_s": stale_after_s,
                "committed_at": None, "served_at": now}
    committed_at = float(snapshot["committed_at"])
    age = max(0.0, now - committed_at)
    return {"has_snapshot": True, "age_s": round(age, 3), "stale": age > stale_after_s,
            "stale_after_s": stale_after_s, "committed_at": committed_at, "served_at": now}


def engine_view(snapshot: Optional[Dict[str, Any]], fresh: Dict[str, Any]) -> Dict[str, Any]:
    if not snapshot:
        return {"display_state": "UNKNOWN", "last_known_state": None, "reasons": [],
                "note": "Нет зафиксированного снимка состояния движка."}
    state = str(snapshot.get("engine_state") or "")
    if state not in KNOWN_ENGINE_STATES:
        return {"display_state": "UNKNOWN", "last_known_state": state or None,
                "reasons": list(snapshot.get("reasons") or []),
                "note": "Неизвестное состояние в снимке — отображается без интерпретации."}
    if fresh["stale"]:
        return {"display_state": "STALE", "last_known_state": state,
                "reasons": list(snapshot.get("reasons") or []),
                "note": "Снимок устарел: текущее состояние движка неизвестно."}
    return {"display_state": state, "last_known_state": state, "reasons": list(snapshot.get("reasons") or []),
            "note": None}


def _dec(value: Any) -> Optional[Decimal]:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _pct(part: Decimal, whole: Decimal) -> str:
    if whole <= 0:
        return "0"
    ratio = min(max(part / whole * 100, Decimal(0)), Decimal(100))
    return format(ratio.quantize(Decimal("0.1")), "f")


def net_gauge(summary: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """Positions of P_min/P/P_max on a symmetric [-cap, +cap] bar as percentage strings.

    Computed server-side with Decimal so the browser never does arithmetic on quantities.
    """
    cap = _dec(summary.get("max_abs_net_position"))
    p_min, p, p_max = (_dec(summary.get(k)) for k in ("P_min", "P", "P_max"))
    if cap is None or cap <= 0 or p_min is None or p_max is None:
        return None
    span = cap * 2

    def pos(value: Decimal) -> str:
        return _pct(value + cap, span)

    gauge = {"min_pct": pos(p_min), "max_pct": pos(p_max),
             "breach": "yes" if (p_max > cap or p_min < -cap) else "no"}
    if p is not None:
        gauge["p_pct"] = pos(p)
    return gauge


def ratio_gauge(value: Any, cap: Any) -> Optional[Dict[str, str]]:
    v, c = _dec(value), _dec(cap)
    if v is None or c is None or c <= 0:
        return None
    return {"pct": _pct(abs(v), c), "breach": "yes" if abs(v) > c else "no"}


def summary_view(snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not snapshot:
        return {}
    payload = snapshot.get("summary") or {}
    view = dict(payload)
    view["gauges"] = {
        "net": net_gauge(payload),
        "gross": ratio_gauge(payload.get("gross_worst"), payload.get("max_gross_position")),
    }
    return view


def _cell_sort_key(cell: Dict[str, Any]) -> Tuple[int, str]:
    raw = str(cell.get("cell_id", ""))
    return (int(raw) if raw.isdigit() else 1 << 62, raw)


def encode_cursor(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


def decode_cursor(cursor: Optional[str]) -> Optional[str]:
    if not cursor:
        return None
    if not re.fullmatch(r"[A-Za-z0-9_\-]{1,200}", cursor):
        raise ValueError("bad cursor")
    padded = cursor + "=" * (-len(cursor) % 4)
    try:
        return base64.urlsafe_b64decode(padded.encode()).decode()
    except (binascii.Error, UnicodeDecodeError):
        raise ValueError("bad cursor") from None


def page_cells(snapshot: Optional[Dict[str, Any]], *, after: Optional[str], limit: int,
               state: Optional[str] = None, only_active: bool = False) -> Dict[str, Any]:
    cells = sorted((snapshot or {}).get("cells") or [], key=_cell_sort_key)
    if state:
        cells = [c for c in cells if str(c.get("state")) == state]
    if only_active:
        cells = [c for c in cells if str(c.get("state")) not in ("IDLE", "COMPLETE")]
    after_id = decode_cursor(after)
    if after_id is not None:
        boundary = _cell_sort_key({"cell_id": after_id})
        cells = [c for c in cells if _cell_sort_key(c) > boundary]
    limit = max(1, min(limit, MAX_PAGE))
    page = cells[:limit]
    next_cursor = encode_cursor(str(page[-1]["cell_id"])) if len(cells) > limit and page else None
    return {"cells": page, "next_cursor": next_cursor, "total_matching": len(cells)}


def cell_map(snapshot: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Compact per-cell state list for the overview strip (state from snapshot only)."""
    cells = sorted((snapshot or {}).get("cells") or [], key=_cell_sort_key)
    return [{"cell_id": c.get("cell_id"), "state": c.get("state"), "entry_side": c.get("entry_side"),
             "blocker": c.get("blocker")} for c in cells]


def valid_lookup_id(value: str) -> bool:
    return bool(_ID_RE.fullmatch(value or ""))


def _str_eq(candidate: Any, wanted: str) -> bool:
    return candidate is not None and str(candidate) == wanted


def _legs(cell: Dict[str, Any]) -> Iterable[Tuple[str, Dict[str, Any]]]:
    entry = cell.get("entry")
    if isinstance(entry, dict):
        yield "ENTRY", entry
    for child in cell.get("tp_children") or []:
        if isinstance(child, dict):
            yield "TP", child


def lookup_in_snapshot(snapshot: Optional[Dict[str, Any]], wanted: str) -> List[Dict[str, Any]]:
    """Exact string match of ``wanted`` against leg client/exchange ids and unmatched evidence."""
    matches: List[Dict[str, Any]] = []
    for cell in (snapshot or {}).get("cells") or []:
        for role, leg in _legs(cell):
            if _str_eq(leg.get("cid"), wanted) or _str_eq(leg.get("exchange_id"), wanted):
                matches.append({"source": "snapshot_leg", "cell_id": cell.get("cell_id"),
                                "generation": cell.get("generation"), "role": role, "leg": leg,
                                "cell_state": cell.get("state"), "low": cell.get("low"), "high": cell.get("high"),
                                "entry_side": cell.get("entry_side")})
    for item in (snapshot or {}).get("unmatched_evidence") or []:
        if isinstance(item, dict) and any(_str_eq(v, wanted) for v in item.values()):
            matches.append({"source": "unmatched_evidence", "evidence": item})
    return matches


AWAITING_START = "AWAITING_START"


def engine_started(snapshot: Optional[Dict[str, Any]]) -> Optional[bool]:
    """Whether the committed engine has been started (None = unknown / no snapshot).

    Prefers an explicit ``summary.started`` flag; the engine currently signals "not started yet" with the
    ``AWAITING_START`` reason while its state is still BOOTSTRAPPING.
    """
    if not snapshot:
        return None
    explicit = (snapshot.get("summary") or {}).get("started")
    if isinstance(explicit, bool):
        return explicit
    return AWAITING_START not in (snapshot.get("reasons") or [])


def is_engine_active(snapshot: Optional[Dict[str, Any]]) -> bool:
    """An engine that is started and not cleanly stopped owns the identity: a new Start is a duplicate."""
    if not snapshot or str(snapshot.get("engine_state")) not in ACTIVE_ENGINE_STATES:
        return False
    return engine_started(snapshot) is not False
