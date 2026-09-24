from decimal import Decimal

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CommandKind,
    CommandStatus,
    LegRole,
    Side,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.engine import open_engine
from hummingbot.strategy_v2.executors.neutral_grid_executor.snapshot import build_preview, format_status

from ng_engine_harness import Harness, make_config, start_payload


D = Decimal


def _command(h: Harness, key: str):
    return next(c for c in h.engine.store.list_commands() if c.idempotency_key == key)


def _exact_manual_close(h: Harness):
    h.bootstrap()
    cell_id = h.buy_cells()[-1]
    entry = h.live_order(cell_id, LegRole.ENTRY)
    h.fx.fill(entry.cid, D("10"))
    h.run_until(lambda: h.live_order(cell_id, LegRole.TP) is not None, max_ticks=20)
    tp = h.live_order(cell_id, LegRole.TP)
    h.fx.venue_cancel(tp.cid)
    h.settle()

    manual = h.fx.place_manual_order(Side.SELL, D("5.3"), D("10"))
    manual.reduce_only = True
    h.fx.fill(manual.client_order_id, D("4.38"))
    h.fx.fill(manual.client_order_id, D("5.62"))
    h.tick(20)
    h.command(CommandKind.STOP, key="stop-before-settlement")
    h.run_until(lambda: h.engine.is_stopped, max_ticks=80)
    h.tick(20)
    return cell_id, manual


def test_exact_manual_close_publishes_stable_candidate_and_applies_without_transport(tmp_path):
    h = Harness(tmp_path)
    try:
        cell_id, _ = _exact_manual_close(h)
        assert h.engine.meta.stop_outcome == "STOPPED_WITH_INVENTORY"
        candidate = h.engine.last_snapshot["summary"]["external_close_candidate"]
        assert candidate["blockers"] == []
        assert candidate["cycle"].items() >= {
            "grid_id": "grid-t", "cell_id": str(cell_id), "E": "10", "X": "0",
            "external_settled": "0", "proposed_settlement": "10", "open_after": "0",
        }.items()
        assert sorted(t["quantity"] for t in candidate["trades"]) == ["4.38", "5.62"]
        proof = candidate["proof_id"]
        h.tick(15)
        assert h.engine.last_snapshot["summary"]["external_close_candidate"]["proof_id"] == proof

        calls = h.engine.transport_calls
        h.command(CommandKind.BASELINE_AUDIT, {
            "action": "settle_external_close",
            "proof_id": proof,
            "confirmation": "SETTLE EXTERNAL CLOSE grid-t AT FLAT 0",
            "note": "manual reduce-only close after test",
            "acknowledge": True,
        }, key="settle-external-0001")
        h.tick()
        record = _command(h, "settle-external-0001")
        assert record.status == CommandStatus.APPLIED, record.result
        assert h.engine.transport_calls == calls
        assert h.engine.is_stopped and h.engine.meta.stop_requested_ms is not None
        h.tick(3)
        assert h.engine.meta.stop_outcome == "STOPPED"
        cycle = next(c for c in h.engine.cells[cell_id].cycles if c.generation == int(candidate["cycle"]["generation"]))
        assert cycle.E == D("10") and cycle.X == D("0")
        assert cycle.external_settled == D("10") and cycle.open_obligation == D("0")
        assert h.engine.endpoints.P == h.fx.net_position == D("0")
        cell_view = next(c for c in h.engine.last_snapshot["cells"] if c["cell_id"] == cell_id)
        assert cell_view["obligation"].items() >= {
            "E": "10", "X": "0", "external_settled": "10", "open": "0",
        }.items()
        assert "E 10 X 0 S 10 open 0" in format_status(h.engine.last_snapshot, now=h.clock())

        h.tick(15)
        h.command(CommandKind.BASELINE_AUDIT, {"observed_position": "0", "note": "post-settlement drift audit"},
                  key="audit-after-external-settlement")
        h.tick()
        audit = _command(h, "audit-after-external-settlement")
        assert audit.status == CommandStatus.APPLIED, audit.result
        assert h.engine.effective_baseline == D("0") and h.engine.endpoints.P == D("0")
    finally:
        h.close()


def test_external_close_refuses_changed_proof_and_normal_or_paused_state(tmp_path):
    h = Harness(tmp_path)
    try:
        h.bootstrap()
        assert h.engine.external_close_candidate()["blockers"]
        for action in (CommandKind.BASELINE_AUDIT,):
            h.command(action, {
                "action": "settle_external_close", "proof_id": "0" * 64,
                "confirmation": "SETTLE EXTERNAL CLOSE grid-t AT FLAT 0",
                "note": "must fail", "acknowledge": True,
            }, key="settle-invalid-state")
            h.tick()
            assert _command(h, "settle-invalid-state").status == CommandStatus.REJECTED
    finally:
        h.close()


def test_external_close_survives_restart_is_idempotent_and_never_recreates_old_tp(tmp_path):
    h = Harness(tmp_path)
    try:
        cell_id, _ = _exact_manual_close(h)
        candidate = h.engine.last_snapshot["summary"]["external_close_candidate"]
        payload = {"action": "settle_external_close", "proof_id": candidate["proof_id"],
                   "confirmation": "SETTLE EXTERNAL CLOSE grid-t AT FLAT 0",
                   "note": "manual reduce-only close", "acknowledge": True}
        old_tp_cids = {leg.cid for leg in h.legs(cell_id, LegRole.TP)}
        h.command(CommandKind.BASELINE_AUDIT, payload, key="settle-idempotent-01")
        h.tick()
        assert _command(h, "settle-idempotent-01").status == CommandStatus.APPLIED
        assert h.engine.store._x("SELECT count(*) FROM external_settlements").fetchone()[0] == 1

        h.command(CommandKind.BASELINE_AUDIT, payload, key="settle-idempotent-01")
        h.tick()
        assert h.engine.store._x("SELECT count(*) FROM external_settlements").fetchone()[0] == 1
        h.restart()
        h.tick(20)
        cycle = next(c for c in h.engine.cells[cell_id].cycles if c.generation == int(candidate["cycle"]["generation"]))
        assert cycle.external_settled == D("10") and cycle.open_obligation == 0
        assert h.engine.is_stopped and h.engine.meta.stop_requested_ms is not None

        h.command(CommandKind.START, start_payload(), key="explicit-start-after-settlement")
        h.tick(10)
        assert _command(h, "explicit-start-after-settlement").status == CommandStatus.APPLIED
        assert {leg.cid for leg in h.legs(cell_id, LegRole.TP)} == old_tp_cids
    finally:
        h.close()


def test_stopped_maintenance_restart_refreshes_evidence_without_clearing_stop(tmp_path):
    h = Harness(tmp_path)
    try:
        _exact_manual_close(h)
        proof = h.engine.last_snapshot["summary"]["external_close_candidate"]["proof_id"]
        stop_ms = h.engine.meta.stop_requested_ms
        h.restart()  # maintenance attach: no START or resume authority
        assert h.engine.meta.stop_requested_ms == stop_ms and h.engine.is_stopped
        h.run_until(lambda: not h.engine.external_close_candidate()["blockers"], max_ticks=80)
        candidate = h.engine.external_close_candidate()
        assert candidate["proof_id"] == proof
        assert h.engine.meta.stop_requested_ms == stop_ms and h.engine.is_stopped
        assert not h.fx.open_orders()
    finally:
        h.close()


def test_settlement_migration_start_24x20_keeps_old_generation_closed(tmp_path):
    h = Harness(tmp_path)
    try:
        cell_id, _ = _exact_manual_close(h)
        candidate = h.engine.last_snapshot["summary"]["external_close_candidate"]
        h.command(CommandKind.BASELINE_AUDIT, {
            "action": "settle_external_close", "proof_id": candidate["proof_id"],
            "confirmation": "SETTLE EXTERNAL CLOSE grid-t AT FLAT 0",
            "note": "manual close before grid v2", "acknowledge": True,
        }, key="settle-before-v2")
        h.tick()
        assert _command(h, "settle-before-v2").status == CommandStatus.APPLIED
        old_grid = h.engine.grid_id
        old_tp_cids = {leg.cid for leg in h.legs(cell_id, LegRole.TP)}

        h.fx.remove_ws_listener(h.engine.wake)
        h.engine.store.close()
        new = make_config(grid_id="lit-neutral-fixed-v2", lower_price=D("4.9"), upper_price=D("5.9"),
                          cell_count=24, order_amount_base=D("20"), leverage=D("5"), max_active_orders=120,
                          max_abs_net_position=D("1000"), max_gross_position=D("1000"))
        engine = open_engine(new, str(h.db_path), h.fx, clock=h.clock, options=h.options,
                             lock_dir=str(h.lock_dir), allow_grid_migration=True, offline_demo=True)
        h.engine, h.config = engine, new
        h.fx.add_ws_listener(engine.wake)
        h.tick(10)
        h.command(CommandKind.BASELINE_AUDIT, {"action": "migrate_grid", "note": "confirmed v2"}, key="migrate-v2")
        h.tick(2)
        assert _command(h, "migrate-v2").status == CommandStatus.APPLIED, _command(h, "migrate-v2").result
        preview = build_preview(h.engine)
        assert preview["boundaries"] == 25 and preview["cells"] == 24
        assert preview["slots_reserved"] == 120 and preview["slot_cap"] == 120
        assert h.engine.is_stopped and h.engine.meta.stop_requested_ms is not None
        old = h.engine.store.cycle(old_grid, cell_id, int(candidate["cycle"]["generation"]))
        assert old.entry_filled == D("10") and old.exit_filled == 0 and old.external_settled == D("10")

        h.tick(15)
        h.command(CommandKind.BASELINE_AUDIT, {"observed_position": "0", "note": "audit after v2 migration"},
                  key="audit-after-v2-migration")
        h.tick()
        audit = _command(h, "audit-after-v2-migration")
        assert audit.status == CommandStatus.APPLIED, audit.result
        assert h.engine.effective_baseline == D("0") and h.engine.endpoints.P == D("0")

        before = len(h.fx.submits())
        h.command(CommandKind.START, start_payload(), key="start-v2-explicit")
        h.tick(12)
        assert _command(h, "start-v2-explicit").status == CommandStatus.APPLIED
        assert len(h.fx.submits()) > before
        assert all(call.request.amount == D("20") for call in h.fx.submits()[before:])
        old_tp_after = {leg.cid for lr in h.engine.store.legs(grid_id=old_grid, cell_id=cell_id)
                        if lr.role == LegRole.TP for leg in [lr]}
        assert old_tp_after == old_tp_cids
    finally:
        h.close()
