from decimal import Decimal

import pytest

from ng_engine_harness import Harness, make_config, start_payload

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CommandKind, CommandStatus, LegRole, Side
from hummingbot.strategy_v2.executors.neutral_grid_executor.engine import open_engine


D = Decimal


def test_manual_buy_is_atomically_adopted_into_new_cell_and_routes_five_tps(tmp_path):
    old = make_config(lower_price=D("4.9"), upper_price=D("5.9"), cell_count=10,
                      order_amount_base=D("100"), max_active_orders=210,
                      max_abs_net_position=D("1000"), max_gross_position=D("1000"))
    h = Harness(tmp_path, config=old)
    try:
        h.bootstrap()
        h.tick(3)
        for cell_id in range(4):
            h.fx.fill(h.live_order(cell_id, LegRole.ENTRY).cid, D("100"))
        h.run_until(lambda: h.fx.net_position == D("400") and
                    all(h.live_order(cell_id, LegRole.TP) is not None for cell_id in range(4)), max_ticks=50)
        h.command(CommandKind.STOP, key="stop-before-manual-buy")
        h.run_until(lambda: h.engine.is_stopped, max_ticks=80)
        manual = h.fx.place_manual_order(Side.BUY, D("4.8"), D("100"))
        h.fx.fill(manual.client_order_id, D("40"), price=D("4.79"))
        h.fx.fill(manual.client_order_id, D("60"), price=D("4.78"))
        h.tick(20)
        assert h.fx.net_position == D("500")

        h.fx.remove_ws_listener(h.engine.wake)
        h.engine.store.close()
        target = make_config(lower_price=D("4.8"), upper_price=D("5.9"), cell_count=11,
                             order_amount_base=D("100"), max_active_orders=210,
                             max_abs_net_position=D("1000"), max_gross_position=D("1000"),
                             directional_outside_bounds_entries=True)
        h.config = target
        h.engine = open_engine(target, str(h.db_path), h.fx, clock=h.clock, options=h.options,
                               lock_dir=str(h.lock_dir), allow_grid_migration=True, offline_demo=True)
        h.fx.add_ws_listener(h.engine.wake)
        h.run_until(lambda: h.engine.grid_external_entry_candidate()["blockers"] == [], max_ticks=100)
        candidate = h.engine.grid_external_entry_candidate()
        assert candidate["old_ledger_position"] == "400"
        assert candidate["observed_position"] == "500"
        assert candidate["proposed_cycle"]["tp_price"] == "4.9"
        h.command(CommandKind.BASELINE_AUDIT, {
            "action": "extend_grid_with_external_entry", "proof_id": "0" * 64,
            "confirmation": candidate["confirmation"], "acknowledge": True,
            "note": "stale adoption proof"}, key="stale-manual-buy")
        h.tick(2)
        stale = h.engine.store.get_command(idempotency_key="stale-manual-buy")
        assert stale.status == CommandStatus.REJECTED
        assert h.engine.store.grid().lower_price == D("4.9")
        h.command(CommandKind.BASELINE_AUDIT, {
            "action": "extend_grid_with_external_entry", "proof_id": candidate["proof_id"],
            "confirmation": candidate["confirmation"], "acknowledge": True,
            "note": "adopt manual lower-cell buy"}, key="adopt-manual-buy")
        h.tick(2)
        record = h.engine.store.get_command(idempotency_key="adopt-manual-buy")
        assert record.status == CommandStatus.APPLIED, record.result
        assert h.engine.effective_baseline == D("0") and h.engine.endpoints.P == D("500")
        cycle = h.engine.cells[10].current
        assert cycle.E == D("100") and cycle.open_obligation == D("100") and cycle.entries == []

        h.fx.set_book(D("4.7"), D("4.75"))
        h.command(CommandKind.START, start_payload(), key="start-after-adoption")
        h.run_until(lambda: all(h.live_order(cell_id, LegRole.TP) is not None for cell_id in (10, 0, 1, 2, 3)),
                    max_ticks=80)
        assert [h.engine.cells[cell_id].spec.tp_price for cell_id in (10, 0, 1, 2, 3)] == [
            D("4.9"), D("5"), D("5.1"), D("5.2"), D("5.3")]
        sell_entries = [leg for leg in h.engine.non_final_legs()
                        if leg.identity.role == LegRole.ENTRY and leg.side == Side.SELL]
        assert len(sell_entries) == 5
        assert all(leg.side == Side.SELL for leg in h.engine.non_final_legs())
        external_tp = h.live_order(10, LegRole.TP)
        h.fx.fill(external_tp.cid, D("40"))
        h.run_until(lambda: h.engine.cells[10].current.X == D("40"), max_ticks=50)
        h.crash_restart()
        h.run_until(lambda: h.engine.endpoints is not None and h.engine.endpoints.P == D("460"), max_ticks=50)
        cycle = h.engine.cells[10].current
        assert cycle.E == D("100") and cycle.X == D("40") and cycle.open_obligation == D("60")
        assert h.engine.effective_baseline == D("0") and h.engine.endpoints.P == D("460")
        assert not [leg for leg in h.legs(10, LegRole.ENTRY) if not leg.is_final]
        external_tp = h.live_order(10, LegRole.TP)
        h.fx.fill(external_tp.cid, D("60"))
        h.run_until(lambda: h.fx.net_position == D("400") and h.engine.cells[10].current is None, max_ticks=50)
        assert h.engine.endpoints.P == D("400")
        assert not [leg for leg in h.legs(10, LegRole.ENTRY) if not leg.is_final]
    finally:
        h.close()


@pytest.mark.parametrize("variant,clean", [("nonterminal", False), ("multiple_orders", False), ("cid_zero", True)])
def test_external_entry_candidate_requires_one_terminal_order_and_accepts_client_id_zero(tmp_path, variant, clean):
    old = make_config(lower_price=D("4.9"), upper_price=D("5.9"), cell_count=10,
                      order_amount_base=D("100"), max_active_orders=210)
    h = Harness(tmp_path, config=old)
    try:
        h.bootstrap()
        h.command(CommandKind.STOP, key="stop-before-manual-buy")
        h.run_until(lambda: h.engine.is_stopped, max_ticks=80)
        if variant == "multiple_orders":
            first = h.fx.place_manual_order(Side.BUY, D("4.8"), D("40"))
            second = h.fx.place_manual_order(Side.BUY, D("4.8"), D("60"))
            h.fx.fill(first.client_order_id, D("40"), price=D("4.79"))
            h.fx.fill(second.client_order_id, D("60"), price=D("4.78"))
        else:
            quantity = D("110") if variant == "nonterminal" else D("100")
            client_id = 0 if variant == "cid_zero" else None
            manual = h.fx.place_manual_order(Side.BUY, D("4.8"), quantity, client_order_id=client_id)
            h.fx.fill(manual.client_order_id, D("100"), price=D("4.79"))
        h.tick(20)
        h.fx.remove_ws_listener(h.engine.wake)
        h.engine.store.close()
        target = make_config(lower_price=D("4.8"), upper_price=D("5.9"), cell_count=11,
                             order_amount_base=D("100"), max_active_orders=210)
        h.config = target
        h.engine = open_engine(target, str(h.db_path), h.fx, clock=h.clock, options=h.options,
                               lock_dir=str(h.lock_dir), allow_grid_migration=True, offline_demo=True)
        h.fx.add_ws_listener(h.engine.wake)
        h.tick(30)
        candidate = h.engine.grid_external_entry_candidate()
        assert (candidate["blockers"] == []) is clean
    finally:
        h.close()
