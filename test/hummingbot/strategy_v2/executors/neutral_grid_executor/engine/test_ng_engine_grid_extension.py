from decimal import Decimal

from ng_engine_harness import Harness, make_config, start_payload

from hummingbot.strategy_v2.executors.neutral_grid_executor.commands import validate_kind
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CommandKind, CommandStatus, LegRole
from hummingbot.strategy_v2.executors.neutral_grid_executor.engine import open_engine


D = Decimal


def test_stopped_engine_applies_proof_bound_lower_extension_without_renumbering(tmp_path):
    h = Harness(tmp_path)
    try:
        h.bootstrap()
        h.tick(2)
        h.command(CommandKind.STOP, key="stop-before-extension")
        h.run_until(lambda: h.engine.is_stopped, max_ticks=50)
        h.fx.remove_ws_listener(h.engine.wake)
        h.engine.store.close()

        new_config = make_config(lower_price=D("4.9"), upper_price=D("6"), cell_count=11)
        h.config = new_config
        h.engine = open_engine(new_config, str(h.db_path), h.fx, clock=h.clock, options=h.options,
                               lock_dir=str(h.lock_dir), allow_grid_migration=True, offline_demo=True)
        h.fx.add_ws_listener(h.engine.wake)
        h.tick(8)

        candidate = h.engine.last_snapshot["summary"]["grid_extension_candidate"]
        assert h.engine.last_snapshot["engine_state"] == "STOPPED"
        assert candidate["blockers"] == []
        old_fill_seq, old_active_seq = h.engine.fills_commit_seq, h.engine.active_seq
        h.engine.fills_commit_seq = h.engine.position_seq + 1
        h.engine.active_seq = h.engine.position_seq - 1
        incoherent = h.engine.grid_extension_candidate()
        assert "POSITION_NOT_FRESH" in incoherent["blockers"]
        assert any(value.startswith("COHERENT_CUT:") for value in incoherent["blockers"])
        h.engine.fills_commit_seq, h.engine.active_seq = old_fill_seq, old_active_seq
        assert candidate["source"]["lower_price"] == "5"
        assert candidate["target"]["lower_price"] == "4.9"
        assert candidate["added_cells"] == [{
            "cell_id": "10", "low_price": "4.9", "high_price": "5", "entry_side": "BUY"}]
        payload = {"action": "extend_grid", "proof_id": candidate["proof_id"],
                   "note": "add one lower cell", "acknowledge": True}
        assert validate_kind("baseline_audit", payload) is None
        retained_latch = {"reason": "retain me", "fingerprint": h.engine._rules_fingerprint()}
        h.engine.meta.reject_latches = {"0:ENTRY": retained_latch}
        h.command(CommandKind.BASELINE_AUDIT, payload, key="extend-grid")
        h.tick(2)

        result = h.engine.store.get_command(idempotency_key="extend-grid")
        assert result.status == CommandStatus.APPLIED, result.result
        assert h.engine.grid_record.lower_price == D("4.9") and h.engine.grid_record.cell_count == 11
        assert [cell["cell_id"] for cell in h.engine.last_snapshot["cells"]] == [10] + list(range(10))
        assert h.engine.effective_baseline == D("0")
        assert h.engine.meta.reject_latches == {"0:ENTRY": retained_latch}
    finally:
        h.close()


def test_extension_rejects_a_stale_candidate_proof(tmp_path):
    h = Harness(tmp_path)
    try:
        h.bootstrap()
        payload = {"action": "extend_grid", "proof_id": "0" * 64,
                   "note": "stale", "acknowledge": True}
        h.command(CommandKind.BASELINE_AUDIT, payload, key="stale-extension")
        h.tick()
        result = h.engine.store.get_command(idempotency_key="stale-extension")
        assert result.status == CommandStatus.REJECTED
        assert result.result["error"] in ("GRID_EXTENSION_NOT_ELIGIBLE", "GRID_EXTENSION_PROOF_CHANGED")
    finally:
        h.close()


def test_live_profile_extension_retains_four_cycles_and_recreates_original_tps(tmp_path):
    config = make_config(lower_price=D("4.9"), upper_price=D("5.9"), cell_count=10,
                         order_amount_base=D("100"), max_active_orders=210,
                         max_abs_net_position=D("1000"), max_gross_position=D("1000"))
    h = Harness(tmp_path, config=config)
    try:
        h.bootstrap()
        h.tick(3)
        entry_cids = {cell_id: h.live_order(cell_id, LegRole.ENTRY).cid for cell_id in range(4)}
        h.fx.fill(entry_cids[0], D("100"))
        h.fx.fill(entry_cids[1], D("100"))
        h.fx.fill(entry_cids[2], D("73.22"))
        h.fx.fill(entry_cids[2], D("26.78"))
        h.fx.fill(entry_cids[3], D("100"))
        h.run_until(lambda: h.fx.net_position == D("400") and
                    all(sum((leg.requested for leg in h.legs(i, LegRole.TP)), D("0")) >= D("100")
                        for i in range(4)), max_ticks=40)

        h.command(CommandKind.STOP, key="stop-live-extension")
        h.run_until(lambda: h.engine.is_stopped, max_ticks=80)
        h.fx.remove_ws_listener(h.engine.wake)
        h.engine.store.close()
        new_config = make_config(lower_price=D("4.8"), upper_price=D("5.9"), cell_count=11,
                                 order_amount_base=D("100"), max_active_orders=210,
                                 max_abs_net_position=D("1000"), max_gross_position=D("1000"))
        h.config = new_config
        h.engine = open_engine(new_config, str(h.db_path), h.fx, clock=h.clock, options=h.options,
                               lock_dir=str(h.lock_dir), allow_grid_migration=True, offline_demo=True)
        h.fx.add_ws_listener(h.engine.wake)
        h.tick(8)
        candidate = h.engine.last_snapshot["summary"]["grid_extension_candidate"]
        assert candidate["blockers"] == [] and candidate["observed_position"] == "400"
        assert candidate["target"]["config"]["max_abs_net_position"] == "1000"
        assert candidate["target"]["config"]["max_gross_position"] == "1000"
        assert candidate["target"]["config"]["leverage"] == "5"
        assert candidate["target"]["config"]["max_active_orders"] == 210
        assert h.engine.effective_baseline == D("0")
        assert sorted(fill.size for fill in h.engine.store.fills(entry_cids[2])) == [D("26.78"), D("73.22")]
        assert [(row["cell_id"], row["quantity"], row["tp_price"]) for row in candidate["retained_obligations"]] == [
            ("0", "100", "5"), ("1", "100", "5.1"), ("2", "100", "5.2"), ("3", "100", "5.3")]
        h.command(CommandKind.BASELINE_AUDIT, {
            "action": "extend_grid", "proof_id": candidate["proof_id"],
            "note": "add 4.8 lower cell", "acknowledge": True}, key="extend-live")
        h.tick(2)
        assert h.engine.store.get_command(idempotency_key="extend-live").status == CommandStatus.APPLIED
        assert h.engine.effective_baseline == D("0") and h.engine.endpoints.P == D("400")
        assert h.engine.active_cell_ids() == [10] + list(range(10))

        h.command(CommandKind.START, start_payload(), key="resume-extended")
        h.run_until(lambda: all(sum((leg.remaining for leg in h.legs(i, LegRole.TP) if not leg.is_final), D("0"))
                                == D("100") for i in range(4)), max_ticks=50)
        assert [h.engine.cells[i].spec.tp_price for i in range(4)] == [D("5"), D("5.1"), D("5.2"), D("5.3")]
        assert all(len([leg for leg in h.legs(i, LegRole.ENTRY) if leg.filled > 0]) == 1 for i in range(4))
        assert h.engine.endpoints.gross_worst <= D("1000")
        assert len(h.engine.non_final_legs()) <= 210
        assert 10 in h.engine.active_cell_ids()
    finally:
        h.close()
