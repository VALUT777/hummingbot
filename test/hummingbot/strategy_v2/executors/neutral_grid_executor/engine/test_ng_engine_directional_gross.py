from decimal import Decimal

from ng_engine_harness import Harness, make_config, start_payload

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CommandKind,
    CommandStatus,
    LegRole,
    OrderState,
    Side,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.engine import open_engine
from hummingbot.strategy_v2.executors.neutral_grid_executor.snapshot import format_status


D = Decimal


def _command(h, key):
    return h.engine.store.get_command(idempotency_key=key)


def test_reviewed_start_is_the_only_policy_activation_path_and_persists_restart(tmp_path):
    h = Harness(tmp_path, config=make_config(directional_gross_limits=False))
    try:
        h.bootstrap()
        h.tick(3)
        h.fx.fill(h.live_order(0, LegRole.ENTRY).cid, D("10"))
        h.run_until(lambda: h.live_order(0, LegRole.TP) is not None, max_ticks=50)
        tp_cid = h.live_order(0, LegRole.TP).cid

        h.config = make_config(directional_gross_limits=True)
        h.restart()
        h.run_until(lambda: "RISK_POLICY_REVIEW_REQUIRED" in h.engine.entry_blockers, max_ticks=50)
        assert h.engine.directional_gross_limits_active is False
        assert h.engine.config.directional_gross_limits is True
        assert "RISK_POLICY_REVIEW_REQUIRED" not in h.engine.tp_blockers
        assert h.engine.leg_by_cid(tp_cid) is not None

        h.command(CommandKind.START, dict(start_payload(), source="launcher"), key="launcher-policy-bypass")
        h.tick(2)
        refused = _command(h, "launcher-policy-bypass")
        assert refused.status == CommandStatus.REJECTED
        assert refused.result["error"] == "RISK_POLICY_REVIEW_REQUIRED"
        assert h.engine.b_engine.config_revision == 1

        h.command(CommandKind.START, start_payload(), key="reviewed-policy-start")
        h.tick(2)
        applied = _command(h, "reviewed-policy-start")
        assert applied.status == CommandStatus.APPLIED, applied.result
        assert applied.result["risk_policy_config_revision"] == 2
        assert h.engine.directional_gross_limits_active is True
        assert "RISK_POLICY_REVIEW_REQUIRED" not in h.engine.entry_blockers
        assert h.engine.store.config_payload()["directional_gross_limits"] is True

        h.restart()
        h.tick(5)
        assert h.engine.directional_gross_limits_active is True
        assert not h.engine.risk_policy_review_required
        assert h.engine.b_engine.config_revision == 2
        h.command(CommandKind.STOP, key="stop-after-policy-review")
        h.run_until(lambda: h.engine.is_stopped, max_ticks=80)
        h.fx.remove_ws_listener(h.engine.wake)
        h.engine.store.close()
        expanded = make_config(lower_price=D("4.9"), upper_price=D("6"), cell_count=11,
                               directional_gross_limits=True)
        h.config = expanded
        h.engine = open_engine(expanded, str(h.db_path), h.fx, clock=h.clock, options=h.options,
                               lock_dir=str(h.lock_dir), allow_grid_migration=True, offline_demo=True)
        h.fx.add_ws_listener(h.engine.wake)
        h.tick(10)
        blockers = h.engine.grid_extension_candidate()["blockers"]
        assert "CONFIG_CHANGE_FORBIDDEN:directional_gross_limits" not in blockers
    finally:
        h.close()


def test_concrete_long_400_six_sell_entries_and_buy_100_fit_independent_caps(tmp_path):
    config = make_config(lower_price=D("4.8"), upper_price=D("5.9"), cell_count=11,
                         order_amount_base=D("100"), max_active_orders=210,
                         max_abs_net_position=D("1000"), max_gross_position=D("1000"),
                         directional_gross_limits=True)
    h = Harness(tmp_path, config=config)
    try:
        h.fx.set_book(D("5.29"), D("5.31"))
        h.bootstrap()
        h.run_until(lambda: all(h.live_order(cell_id, LegRole.ENTRY) is not None for cell_id in range(10)),
                    max_ticks=80)
        for cell_id in (1, 2, 3, 4):
            h.fx.fill(h.live_order(cell_id, LegRole.ENTRY).cid, D("100"))
        h.run_until(lambda: h.live_order(10, LegRole.ENTRY) is not None and
                    all(h.live_order(cell_id, LegRole.TP) is not None for cell_id in (1, 2, 3, 4)),
                    max_ticks=80)
        h.tick()
        ep = h.engine.endpoints
        assert (ep.P, ep.P_min, ep.P_max) == (D("400"), D("-600"), D("500"))
        assert (ep.long_entry_worst, ep.short_entry_worst, ep.gross_worst) == (
            D("500"), D("600"), D("1100"))
        assert h.engine.directional_gross_limits_active
        assert not [blocker for blocker in h.engine.entry_blockers if blocker.startswith("GROSS_CAP")]
        summary = h.engine.last_snapshot["summary"]
        assert (summary["long_entry_worst"], summary["short_entry_worst"], summary["gross_worst"]) == (
            "500", "600", "1100")
        assert summary["directional_gross_limits_active"] is True
        status = format_status(h.engine.last_snapshot, now=h.clock())
        assert "directional gross long 500 / 1000 short 600 / 1000" in status
        assert "aggregate 1100 info" in status
    finally:
        h.close()


def test_pending_policy_review_uses_persisted_caps_and_rejects_bundled_cap_change(tmp_path):
    h = Harness(tmp_path, config=make_config(max_gross_position=D("1000"), directional_gross_limits=False))
    try:
        h.bootstrap()
        h.config = make_config(max_gross_position=D("2000"), directional_gross_limits=True)
        h.restart()
        h.run_until(lambda: "RISK_POLICY_REVIEW_REQUIRED" in h.engine.entry_blockers, max_ticks=50)
        assert h.engine.limits.max_gross_position == D("1000")
        assert h.engine.max_gross_position_active == D("1000")
        assert h.engine.config.max_gross_position == D("2000")
        summary = h.engine.last_snapshot["summary"]
        assert summary["max_gross_position"] == "1000"
        assert summary["max_gross_position_requested"] == "2000"
        h.command(CommandKind.START, start_payload(), key="bundled-policy-and-cap")
        h.tick(2)
        refused = _command(h, "bundled-policy-and-cap")
        assert refused.status == CommandStatus.REJECTED
        assert refused.result["error"] == "RISK_POLICY_CONFIG_CHANGE_FORBIDDEN"
        assert refused.result["changes"] == ["directional_gross_limits", "max_gross_position"]
        assert h.engine.b_engine.config_revision == 1
    finally:
        h.close()


def test_grid_extension_cannot_approve_pending_directional_policy(tmp_path):
    h = Harness(tmp_path, config=make_config(directional_gross_limits=False))
    try:
        h.bootstrap()
        h.command(CommandKind.STOP, key="stop-before-extension")
        h.run_until(lambda: h.engine.is_stopped, max_ticks=80)
        h.fx.remove_ws_listener(h.engine.wake)
        h.engine.store.close()
        target = make_config(lower_price=D("4.9"), upper_price=D("6"), cell_count=11,
                             directional_gross_limits=True)
        h.config = target
        h.engine = open_engine(target, str(h.db_path), h.fx, clock=h.clock, options=h.options,
                               lock_dir=str(h.lock_dir), allow_grid_migration=True, offline_demo=True)
        h.fx.add_ws_listener(h.engine.wake)
        h.tick(10)
        candidate = h.engine.grid_extension_candidate()
        assert "CONFIG_CHANGE_FORBIDDEN:directional_gross_limits" in candidate["blockers"]
        assert h.engine.risk_policy_review_required
        assert h.engine.directional_gross_limits_active is False
    finally:
        h.close()


def test_tightening_to_legacy_aggregate_requires_review_and_keeps_canonical_json(tmp_path):
    h = Harness(tmp_path, config=make_config(directional_gross_limits=True))
    try:
        h.bootstrap()
        h.config = make_config(directional_gross_limits=False)
        h.restart()
        h.run_until(lambda: "RISK_POLICY_REVIEW_REQUIRED" in h.engine.entry_blockers, max_ticks=50)
        assert h.engine.directional_gross_limits_active is True
        h.command(CommandKind.START, start_payload(), key="review-aggregate-policy")
        h.tick(2)
        assert _command(h, "review-aggregate-policy").status == CommandStatus.APPLIED
        assert h.engine.directional_gross_limits_active is False
        assert "directional_gross_limits" not in h.engine.store.config_payload()
    finally:
        h.close()


def test_submit_unknown_entry_keeps_its_directional_reservation_after_crash_restart(tmp_path):
    h = Harness(tmp_path, config=make_config(directional_gross_limits=True))
    try:
        h.bootstrap(confirm_tick=False)
        h.hooks.arm("before_transport")
        assert h.tick_crashing(1)
        h.tick(1)
        unknown = [leg for leg in h.engine.non_final_legs() if leg.state == OrderState.SUBMIT_UNKNOWN]
        assert len(unknown) == 1
        ep = h.engine.endpoints
        expected = (D("10"), D("0")) if unknown[0].side == Side.BUY else (D("0"), D("10"))
        assert (ep.long_entry_worst, ep.short_entry_worst) == expected
    finally:
        h.close()
