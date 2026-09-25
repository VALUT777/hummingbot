"""Versioned snapshot (CONTRACTS.md "Snapshot JSON (v1)") and the CLI status renderer.

UI and CLI read the same committed snapshot (``NeutralGridStore.write_snapshot`` assigns the version and the
committed revisions). All decimals and ids are JSON strings. Nothing secret is ever included (no credentials,
no auth tokens, no URLs with secrets). Cell state is never inferred from the current price.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional

from hummingbot.strategy_v2.executors.neutral_grid_executor import grid, risk
from hummingbot.strategy_v2.executors.neutral_grid_executor.cells import EMPTY_BUCKETS
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import OrderState
from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import SNAPSHOT_VERSION, grid_config_to_json

ZERO = Decimal("0")
_UNKNOWN_STATES = (OrderState.INTENT, OrderState.SUBMIT_UNKNOWN, OrderState.CANCEL_UNKNOWN,
                   OrderState.TERMINAL_UNKNOWN)


def _s(value: Any) -> Optional[str]:
    return None if value is None else str(value)


def _leg_view(engine, leg) -> Dict[str, Any]:
    meta = engine.order_meta.get(leg.cid) if leg.cid is not None else None
    return {
        "cid": _s(leg.cid),
        "exchange_id": leg.exchange_order_id,
        "revision": leg.identity.revision,
        "generation": leg.identity.generation,
        "side": leg.side.value,
        "price": str(leg.price),
        "requested": str(leg.requested),
        "filled": str(leg.filled),
        "remaining": str(leg.remaining),
        "state": leg.state.value,
        "expiry": _s(leg.expiry_ms),
        "late_evidence": bool(leg.late_evidence),
        "cancel_reason": meta.cancel_reason if meta else None,
        "transport_detail": meta.transport_detail if meta else None,
        # additive: aggregate TP shares (generation -> qty) and the operator-audited cumulative after late evidence
        "allocation": None if not leg.allocation else {str(g): str(q) for g, q in sorted(leg.allocation.items())},
        "audited_cumulative": meta.audited_cumulative if meta else None,
    }


def _cell_view(engine, ledger, now: float) -> Dict[str, Any]:
    cycles = ledger.open_cycles() or [c for c in ledger.cycles[-1:] if c.generation > 0]
    entry = None
    tps: List[Dict[str, Any]] = []
    for cycle in cycles:
        for leg in cycle.entries:
            entry = _leg_view(engine, leg)
        tps.extend(_leg_view(engine, t) for t in cycle.tps)
        if entry is None and cycle.external_entered > ZERO:
            entry = engine.store.external_entry_summary(engine.grid_id, ledger.cell_id, cycle.generation)
    # Once a cycle is released, retain its final E/X/S in the cell view instead of replacing the audited history
    # with zeroes.  For a live cell this remains the aggregate of its open cycles.
    b = sum((cycle.buckets() for cycle in cycles), start=EMPTY_BUCKETS)
    since = [v for k, v in engine.meta.obligations.items() if k.split(":")[0] == str(ledger.cell_id)]
    blocker = engine.cell_blockers.get(ledger.cell_id)
    plan = engine.admission_plan
    if plan is not None and blocker is None and ledger.cell_id in plan.blocked:
        blocker = plan.blocked[ledger.cell_id]
    queued = plan is not None and ledger.cell_id in plan.queued
    state = ledger.primary_state().value
    if queued and ledger.current is None:
        state = "QUEUED"
    return {
        "cell_id": ledger.cell_id,
        "low": str(ledger.spec.low_price),
        "high": str(ledger.spec.high_price),
        "entry_side": ledger.spec.entry_side.value,
        "entry_price": str(ledger.spec.entry_price),
        "tp_side": ledger.spec.tp_side.value,
        "tp_price": str(ledger.spec.tp_price),
        "generation": ledger.generation,
        "state": state,
        "state_flags": sorted(s.value for s in ledger.state_flags()),
        "entry": entry,
        "tp_children": tps,
        "obligation": {
            "E": str(b.E), "X": str(b.X), "external_settled": str(b.external_settled),
            "external_entered": str(b.external_entered),
            "open": str(b.open_obligation), "live_tp": str(b.live_tp_remainder),
            "reserved_unassigned": str(b.reserved_tp_unassigned), "unassigned": str(b.unassigned),
            "dust": str(b.dust),
        },
        "late_evidence": any(c.late_evidence for c in ledger.cycles),
        "armed": plan is not None and ledger.cell_id in plan.armed,
        "queued": queued,
        "reserved_slots": int(engine.reservations.get(ledger.cell_id, 0)),
        "blocker": blocker,
        "queue_age_s": _s(round(now - min(since) / 1000, 3)) if since else None,
    }


def _revisions(engine) -> Dict[str, int]:
    b = engine.b_engine
    return {"config_revision": b.config_revision if b else 0, "engine_revision": b.engine_revision if b else 0}


def build_summary(engine, now: float) -> Dict[str, Any]:
    ep = engine.endpoints
    plan = engine.admission_plan
    slots = plan.slots if plan is not None else None
    position = engine.position
    rules = engine.rules
    legs = engine.non_final_legs()
    dust_total = sum((ledger.buckets().dust for ledger in engine.cells.values()), ZERO)
    latencies = [lat for _, lat in engine.tp_latencies]
    boot_ready = engine.bootstrap_ready(now) if not engine.bootstrapped else (None, None)
    conflicts, conflict_set_id = engine.history_conflict_set()
    return {
        "grid_id": engine.grid_id,
        "connector": engine.config.connector_name,
        "trading_pair": engine.config.trading_pair,
        "baseline": _s(engine.baseline),
        "effective_baseline": _s(engine.effective_baseline),
        "authoritative_net": _s(position.net_base) if position is not None else None,
        "ledger_net": _s(ep.P) if ep else None,
        "P": _s(ep.P) if ep else None,
        "P_min": _s(ep.P_min) if ep else None,
        "P_max": _s(ep.P_max) if ep else None,
        "gross_worst": _s(ep.gross_worst) if ep else None,
        "max_abs_net_position": str(engine.config.max_abs_net_position),
        "max_gross_position": str(engine.config.max_gross_position),
        "leverage": str(engine.config.leverage),
        "anchor": _s(engine.grid_record.anchor) if engine.grid_record is not None else None,
        "lower_price": str(engine.config.lower_price),
        "upper_price": str(engine.config.upper_price),
        "cell_count": engine.config.cell_count,
        "order_amount_base": str(engine.config.order_amount_base),
        "bid": _s(engine.bid),
        "ask": _s(engine.ask),
        "slots": {
            "actual": slots.actual if slots else 0,
            "reserved": slots.reserved_unused if slots else 0,
            "free": slots.free if slots else None,
            "cap": slots.cap if slots else engine.config.max_active_orders,
        },
        "armed": len(plan.armed) if plan else 0,
        "queued": len(plan.queued) if plan else 0,
        "admission_blocker": plan.blocker if plan else None,
        "owned_active": sum(1 for leg in legs if leg.state in (OrderState.LIVE, OrderState.CANCEL_PENDING)),
        "unknown_orders": sum(1 for leg in legs if leg.state in _UNKNOWN_STATES),
        "unknown_active_orders": [
            {"client_order_id": _s(r.client_order_id), "order_index": r.order_index, "side": r.side.value,
             "price": str(r.price), "remaining": str(r.remaining_base_amount)} for r in engine.unknown_active],
        "history": {
            "complete": engine.history_complete,
            "incomplete_reason": engine.history_incomplete_reason,
            "lag_s": _s(round(engine.history_lag_s(now), 3)),
            "trades_cursor": engine.high_water.get("trades"),
            "orders_cursor": engine.high_water.get("inactive_orders"),
            "last_full_scan_at": _s(engine.last_complete_scan_at),
            "last_commit_at": _s(engine.last_history_commit_at),
            "pages_read": dict(engine.last_scan_pages),
            "progress": engine.scanner.progress_summary(),
            "weight_used_60s": engine.weight_used(now),
        },
        "margin": {
            "available": _s(position.available_collateral) if position is not None else None,
            "required_estimate": _s(engine.margin_required),
            "warning": engine.margin_warning,
        },
        "runtime_rules": None if rules is None else {
            "tick_size": str(rules.tick_size), "size_step": str(rules.size_step), "min_base": str(rules.min_base),
            "min_notional": str(rules.min_notional), "max_base": _s(rules.max_base),
            "max_leverage": _s(rules.max_leverage), "max_active_orders_venue": rules.max_active_orders_venue,
            "supports_limit": bool(rules.supports_limit), "supports_post_only": bool(rules.supports_post_only),
            "ordinary_limit_blocker": rules.ordinary_limit_blocker,
            "fetched_at": _s(rules.fetched_at),
            # the engine's own staleness bound for these rules (the UI's attach/preview gate, W2)
            "max_age_s": str(engine.options.rules_max_age_published_s),
        },
        # exactly the GridConfig the engine runs (decimals as strings) + the core fingerprint (W2)
        "engine_config": dict(grid_config_to_json(engine.config), fingerprint=engine.fingerprint,
                              directional_outside_bounds_entries=engine.config.directional_outside_bounds_entries),
        "dust_total": str(dust_total),
        "freezes": dict(engine.meta.freezes),
        # web D2-17 gates (the engine re-verifies at apply time)
        "colliding_cid": _s(engine.meta.colliding_cid),
        "grid_mutation_blockers": engine.grid_mutation_blockers(),
        "grid_extension_candidate": engine.grid_extension_candidate(now),
        "grid_external_entry_candidate": engine.grid_external_entry_candidate(now),
        "store_blockers": list(engine.store_entry_blockers),
        "open_conflicts": [{"id": c.id, "kind": c.kind, "cid": _s(c.cid), "detail": c.detail}
                           for c in engine.open_conflicts],
        # M1: exactly what an ack_history_conflict audits; the ack must carry this conflict_set_id
        "history_conflicts": conflicts,
        "conflict_set_id": conflict_set_id,
        "entry_blockers": list(engine.entry_blockers),
        "tp_blockers": list(engine.tp_blockers),
        "persistence_error": engine.persistence_error,
        "fatal_reason": engine.fatal_reason,
        "operator_paused": engine.meta.operator_paused,
        "started": bool(engine.meta.started),
        "stop_outcome": engine.meta.stop_outcome,
        "external_close_candidate": engine.external_close_candidate(now),
        "tp_dispatch": {
            "slo_s": str(engine.options.tp_dispatch_slo_s),
            "last_latency_s": _s(round(latencies[-1], 3)) if latencies else None,
            "max_latency_s": _s(round(max(latencies), 3)) if latencies else None,
        },
        "position_reconciled": engine.position_reconciled,
        "bootstrap": {
            "bootstrapped": engine.bootstrapped,
            "ready": boot_ready[0],
            "detail": boot_ready[1],
            "observed_position": _s(position.net_base) if position is not None else None,
            "expected_initial_position": _s(engine.config.expected_initial_position),
            "cut_ts_ms": _s(engine.b_engine.bootstrap_cut_ts_ms) if engine.b_engine is not None else None,
        },
    }


def _commands(engine) -> List[Dict[str, Any]]:
    """Committed command rows (including CONFLICTs the store decided without the engine)."""
    store = getattr(engine, "store", None)
    if store is None or store.closed:
        return [{"id": _s(c["id"]), "kind": c["kind"], "status": c["status"], "result": c["result"]}
                for c in engine.recent_commands]
    return [{"id": _s(c.id), "kind": c.kind, "status": c.status.value, "result": c.result,
             "idempotency_key": c.idempotency_key} for c in store.list_commands(limit=20)]


def build_snapshot(engine, now: float) -> Dict[str, Any]:
    cells = [_cell_view(engine, engine.cells[i], now) for i in engine.active_cell_ids()]
    snap = {
        "snapshot_version": None,  # assigned by the store on commit
        "schema_version": SNAPSHOT_VERSION,
        "committed_at": now,
        "engine_state": engine.engine_state.value,
        "reasons": list(engine.reasons),
        "summary": build_summary(engine, now),
        "cells": cells,
        "unmatched_evidence": [
            {"id": rec.id, "stream": rec.stream, "status": rec.status, "cid": _s(rec.cid), "detail": rec.detail,
             "payload": rec.payload} for rec in engine.unmatched],
        "errors": [{"at": e["at"], "code": e["code"], "message": e["message"]} for e in engine.errors],
        "commands": _commands(engine),
    }
    snap.update(_revisions(engine))
    return snap


def build_preview(engine) -> Dict[str, Any]:
    """Pre-start preview from config + fresh rules (NG-UI-002); never from exchange cell state."""
    cfg = engine.config
    baseline = engine.baseline if engine.baseline is not None else cfg.expected_initial_position
    out: Dict[str, Any] = dict(_revisions(engine))
    try:
        preview = grid.build_preview(cfg, engine.rules, engine.mid, baseline, bootstrap=not engine.bootstrapped)
    except Exception as exc:  # noqa: BLE001 - preview must never crash the engine
        out["errors"] = [f"preview failed: {type(exc).__name__}: {exc}"]
        return out
    out.update({
        "errors": list(preview.errors),
        "boundaries": len(preview.prices),
        "cells": len(preview.cells),
        "anchor": _s(preview.anchor),
        "buy_cells": preview.buy_cells,
        "sell_cells": preview.sell_cells,
        "baseline": _s(preview.baseline),
        "reachable_min": _s(preview.reachable_min),
        "reachable_max": _s(preview.reachable_max),
        "gross_worst": _s(preview.gross_worst),
        "armed": preview.armed,
        "queued": preview.queued,
        "slots_reserved": preview.slots_reserved,
        "slots_free": preview.slots_free,
        "slot_cap": preview.slot_cap,
        "estimated_max_notional": _s(preview.estimated_max_notional),
    })
    if engine.position is not None and preview.reachable_min is not None and engine.mid is not None:
        required = risk.required_margin_estimate(preview.reachable_min, preview.reachable_max, engine.mid,
                                                 cfg.leverage)
        warning, blocks = risk.margin_advisory(engine.position.available_collateral, required)
        out["margin_required_estimate"] = _s(required)
        out["margin_warning"] = warning
        out["margin_blocks"] = blocks
    return out


# ---------------------------------------------------------------------------------------------- CLI status
def format_status(snapshot: Optional[Dict[str, Any]], *, stale_after_s: float = 15.0,
                  now: Optional[float] = None, local_error: Optional[str] = None) -> str:
    """Render the committed snapshot for ``status`` (same data as the UI, no secrets)."""
    if snapshot is None:
        return "Neutral grid: no committed snapshot yet" + (f" | local error: {local_error}" if local_error else "")
    s = snapshot["summary"]
    lines: List[str] = []
    committed_at = float(snapshot["committed_at"])
    age = None if now is None else now - committed_at
    stale = age is not None and age > stale_after_s
    lines.append(
        f"Neutral grid {s['grid_id']} {s['connector']} {s['trading_pair']} | state {snapshot['engine_state']}"
        f"{' (SNAPSHOT STALE)' if stale else ''} | snapshot v{snapshot.get('snapshot_version')} "
        f"rev cfg={snapshot['config_revision']} eng={snapshot['engine_revision']}"
        + (f" | age {age:.1f}s" if age is not None else ""))
    if local_error:
        lines.append(f"  LOCAL ERROR (not committed): {local_error}")
    if snapshot["reasons"]:
        lines.append("  reasons: " + ", ".join(snapshot["reasons"]))
    lines.append(f"  baseline {s['baseline']} (effective {s['effective_baseline']}) | venue net "
                 f"{s['authoritative_net']} | ledger net {s['ledger_net']} | P_min {s['P_min']} P_max {s['P_max']}")
    lines.append(f"  gross worst {s['gross_worst']} / cap {s['max_gross_position']} | net cap "
                 f"±{s['max_abs_net_position']} | anchor {s['anchor']} | bid {s['bid']} ask {s['ask']}")
    h = s["history"]
    lines.append(f"  history complete={h['complete']} reason={h['incomplete_reason']} lag={h['lag_s']}s "
                 f"last_full_scan={h['last_full_scan_at']} weight60s={h['weight_used_60s']}")
    progress = h.get("progress") or {}
    walk = ", ".join(f"{name} pages {p.get('pages_read')} done={p.get('done')} oldest={p.get('oldest_ts_ms')}"
                     for name, p in sorted((progress.get("streams") or {}).items()))
    lines.append(f"  trades cursor {h.get('trades_cursor')} | orders cursor {h.get('orders_cursor')} | pages "
                 f"{h.get('pages_read')} | walk {'resumable' if progress.get('resumable') else 'idle'}"
                 + (f" ({walk})" if walk else "")
                 + (f" | backoff until {progress.get('backoff_until')}" if progress.get("backoff_until") else ""))
    sl = s["slots"]
    lines.append(f"  slots actual {sl['actual']} reserved {sl['reserved']} free {sl['free']} cap {sl['cap']} | "
                 f"armed {s['armed']} queued {s['queued']} | owned active {s['owned_active']} unknown "
                 f"{s['unknown_orders']} | dust {s['dust_total']}")
    if s["margin"]["warning"]:
        lines.append(f"  MARGIN WARNING: {s['margin']['warning']}")
    if s["unknown_active_orders"]:
        lines.append(f"  unknown active orders (never cancelled/adopted): {len(s['unknown_active_orders'])}")
    if snapshot["unmatched_evidence"]:
        lines.append(f"  unmatched evidence: {len(snapshot['unmatched_evidence'])} rows")
    if s["freezes"] or s["store_blockers"]:
        lines.append("  blocked: " + "; ".join([f"{k}: {v}" for k, v in sorted(s["freezes"].items())]
                                               + list(s["store_blockers"])))
    for cell in snapshot["cells"]:
        obligation = cell["obligation"]
        if cell["state"] in ("IDLE", "QUEUED") and not cell["blocker"] and obligation["dust"] == "0" \
                and obligation["E"] == "0" and obligation["X"] == "0" \
                and obligation.get("external_settled", "0") == "0":
            continue
        e = cell["entry"] or {}
        tp_text = ",".join(f"{t['cid']}/{t['exchange_id']}:{t['filled']}/{t['requested']} remaining {t['remaining']} "
                           f"{t['state']}" for t in cell["tp_children"])
        lines.append(
            f"  cell {cell['cell_id']:>3} {cell['low']}-{cell['high']} {cell['entry_side']:<4} g{cell['generation']} "
            f"{cell['state']:<22} entry {e.get('cid')}/{e.get('exchange_id')} {e.get('filled')}/{e.get('requested')} "
            f"rem {e.get('remaining')} {e.get('state')} | TP [{tp_text}] | E {cell['obligation']['E']} "
            f"X {cell['obligation']['X']} S {cell['obligation'].get('external_settled', '0')} "
            f"open {cell['obligation'].get('open')} dust {cell['obligation']['dust']}"
            + (f" | blocker {cell['blocker']}" if cell["blocker"] else "")
            + (f" | queue {cell['queue_age_s']}s" if cell["queue_age_s"] else ""))
    for err in snapshot["errors"][-5:]:
        lines.append(f"  error {err['code']}: {err['message']}")
    return "\n".join(lines)
