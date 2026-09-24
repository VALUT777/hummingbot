from dataclasses import replace
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    EngineState,
    OrderState,
    OrderTypePolicy,
    Side,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    ExternalSettlementCycle,
    ExternalSettlementEvidence,
    ExternalSettlementRequest,
    FaultHooks,
    InvalidTransitionError,
    SimulatedCrash,
    StoreIntegrityError,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    BOOT_CUT_MS,
    GRID_ID,
    Env,
    FakeTransport,
    fill_and_terminate,
    order_row,
    record_entry_intent,
    submit_via_protocol,
    tp_leg,
    trade_row,
)


def _prepared(store, *, trade_client_ids=(700_000_000_001, 700_000_000_001),
              terminal_client_id=700_000_000_001):
    store.set_engine_state(None, EngineState.STOPPED, "synthetic external-settlement fixture")
    entry = record_entry_intent(store, 3)
    submit_via_protocol(store, FakeTransport(), entry)
    fill_and_terminate(store, entry.cid, "owned-entry")
    manual_order_id = "manual-order-1"
    rows = [
        trade_row("manual-1", trade_client_ids[0], Side.SELL, "4.38", exchange_order_id=manual_order_id),
        trade_row("manual-2", trade_client_ids[1], Side.SELL, "5.62", exchange_order_id=manual_order_id),
        replace(order_row(terminal_client_id, Side.SELL, Decimal("5.1"), Decimal("10"), Decimal("10"),
                          order_id=manual_order_id), reduce_only=True),
    ]
    with store.transaction() as tx:
        result = store.apply_history_batch(tx, rows)
    assert len(result.unmatched) == 3
    trade_evidence = [ExternalSettlementEvidence(r.id, "TRADE", Decimal(r.payload["size"]))
                      for r in result.unmatched if r.stream == "TRADES"]
    order_evidence = [ExternalSettlementEvidence(r.id, "TERMINAL_ORDER")
                      for r in result.unmatched if r.stream == "INACTIVE_ORDERS"]
    engine = store.engine()
    request = ExternalSettlementRequest(
        proof_id="proof-1", grid_id=GRID_ID, settlement_side=Side.SELL, observed_position=Decimal("0"),
        actor="operator", reason="manual reduce-only close", expected_config_revision=engine.config_revision,
        expected_engine_revision=engine.engine_revision, position_observed_at_ms=BOOT_CUT_MS + 30_000,
        active_observed_at_ms=BOOT_CUT_MS + 30_001, history_scan_started_at_ms=BOOT_CUT_MS + 30_002,
        history_scan_completed_at_ms=BOOT_CUT_MS + 30_003, trades_high_water="trade-high",
        orders_high_water="order-high", cycles=(ExternalSettlementCycle(GRID_ID, 3, 1, Decimal("10")),),
        evidence=tuple(trade_evidence + order_evidence),
    )
    return request, entry.cid


def _prepared_multi(store, *, include_opposite=False):
    store.set_engine_state(None, EngineState.STOPPED, "synthetic multi-cycle external-settlement fixture")
    transport = FakeTransport()
    cycles = []
    cell_ids = (3, 4, 30) if include_opposite else (3, 4)
    for cell_id in cell_ids:
        entry = record_entry_intent(store, cell_id)
        submit_via_protocol(store, transport, entry)
        fill_and_terminate(store, entry.cid, f"owned-entry-{cell_id}")
        cycle = store.cycle(GRID_ID, cell_id, store.leg(entry.cid).generation)
        cycles.append(ExternalSettlementCycle(GRID_ID, cell_id, cycle.generation, cycle.open_obligation))

    manual_client_id = 700_000_000_010
    manual_order_id = "manual-order-multi"
    total = sum((cycle.quantity for cycle in cycles), Decimal("0"))
    with store.transaction() as tx:
        result = store.apply_history_batch(tx, [
            trade_row("manual-multi", manual_client_id, Side.SELL, str(total), exchange_order_id=manual_order_id),
            replace(order_row(manual_client_id, Side.SELL, Decimal("5.1"), total, total,
                              order_id=manual_order_id), reduce_only=True),
        ])
    trade = next(row for row in result.unmatched if row.stream == "TRADES")
    terminal = next(row for row in result.unmatched if row.stream == "INACTIVE_ORDERS")
    engine = store.engine()
    request = ExternalSettlementRequest(
        proof_id="proof-multi", grid_id=GRID_ID, settlement_side=Side.SELL, observed_position=Decimal("0"),
        actor="operator", reason="one manual order closed two cycles",
        expected_config_revision=engine.config_revision, expected_engine_revision=engine.engine_revision,
        position_observed_at_ms=BOOT_CUT_MS + 30_000, active_observed_at_ms=BOOT_CUT_MS + 30_001,
        history_scan_started_at_ms=BOOT_CUT_MS + 30_002,
        history_scan_completed_at_ms=BOOT_CUT_MS + 30_003, trades_high_water="trade-high-multi",
        orders_high_water="order-high-multi", cycles=tuple(cycles),
        evidence=(ExternalSettlementEvidence(trade.id, "TRADE", total),
                  ExternalSettlementEvidence(terminal.id, "TERMINAL_ORDER")),
    )
    return request


def test_one_manual_order_atomically_settles_multiple_exact_cycles(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    request = _prepared_multi(store)

    with store.transaction() as tx:
        settlement_id = store.record_external_settlement(tx, request)

    settled = store._conn.raw.execute(
        "SELECT cell_id, generation, quantity FROM external_settlement_cycles "
        "WHERE settlement_id = ? ORDER BY cell_id, generation", (settlement_id,)).fetchall()
    assert [tuple(row) for row in settled] == [(3, 1, "10"), (4, 1, "10")]
    assert store.external_position_totals() == (Decimal("0"), Decimal("20"))
    assert store.position_ledger().net == Decimal("0")
    assert store.engine().engine_state == EngineState.STOPPED
    for cycle_request in request.cycles:
        cycle = store.cycle(cycle_request.grid_id, cycle_request.cell_id, cycle_request.generation)
        assert (cycle.external_settled, cycle.open_obligation, cycle.state) == (
            Decimal("10"), Decimal("0"), "COMPLETE")
    evidence = store._conn.raw.execute(
        "SELECT inbox_id FROM external_settlement_evidence WHERE settlement_id = ? ORDER BY inbox_id",
        (settlement_id,)).fetchall()
    assert len(evidence) == 2
    assert store.grid_mutation_blockers() == []
    assert store.verify_ledger() == []
    store.close()
    reopened = env.open()
    assert reopened.verify_ledger() == []


@pytest.mark.parametrize("change", ["bad_total", "reallocated", "mixed_side", "blocked_cycle", "stale_revision"])
def test_multi_cycle_refusal_is_atomic(tmp_path, change):
    env = Env(tmp_path)
    hooks = FaultHooks()
    store = env.open(fault_hooks=hooks)
    env.bootstrap(store)
    request = _prepared_multi(store, include_opposite=change == "mixed_side")
    if change == "bad_total":
        request = replace(request, cycles=request.cycles[:-1])
    elif change == "reallocated":
        first, second = request.cycles
        request = replace(request, cycles=(replace(first, quantity=Decimal("5")),
                                           replace(second, quantity=Decimal("15"))))
    elif change == "blocked_cycle":
        cycle = request.cycles[-1]
        with store.transaction() as tx:
            store.prepare_submit(tx, tp_leg(cycle.cell_id, cycle.generation), side=Side.SELL,
                                 price=store.cycle(GRID_ID, cycle.cell_id, cycle.generation).tp_price,
                                 amount=Decimal("10"), order_type=OrderTypePolicy.LIMIT)
        request = replace(request, expected_engine_revision=store.engine().engine_revision)
    elif change == "stale_revision":
        store.set_engine_state(None, EngineState.PAUSED, "advance revision")
        store.set_engine_state(None, EngineState.STOPPED, "restore stopped state")

    with pytest.raises((ValueError, InvalidTransitionError)):
        with store.transaction() as tx:
            store.record_external_settlement(tx, request)

    assert store._conn.raw.execute("SELECT count(*) FROM external_settlements").fetchone()[0] == 0
    assert store._conn.raw.execute("SELECT count(*) FROM external_settlement_cycles").fetchone()[0] == 0
    assert store._conn.raw.execute("SELECT count(*) FROM external_settlement_evidence").fetchone()[0] == 0
    assert all(store.cycle(c.grid_id, c.cell_id, c.generation).external_settled == 0 for c in request.cycles[:2])


def test_multi_cycle_crash_rolls_back_cycles_and_evidence_together(tmp_path):
    env = Env(tmp_path)
    hooks = FaultHooks()
    store = env.open(fault_hooks=hooks)
    env.bootstrap(store)
    request = _prepared_multi(store)
    hooks.arm("before_command_commit")

    with pytest.raises(SimulatedCrash):
        with store.transaction() as tx:
            store.record_external_settlement(tx, request)

    reopened = env.open()
    assert reopened._conn.raw.execute("SELECT count(*) FROM external_settlements").fetchone()[0] == 0
    assert reopened._conn.raw.execute("SELECT count(*) FROM external_settlement_cycles").fetchone()[0] == 0
    assert reopened._conn.raw.execute("SELECT count(*) FROM external_settlement_evidence").fetchone()[0] == 0
    assert len(reopened.unmatched_evidence()) == 2
    for cycle_request in request.cycles:
        cycle = reopened.cycle(cycle_request.grid_id, cycle_request.cell_id, cycle_request.generation)
        assert cycle.state == "OPEN" and cycle.external_settled == 0 and cycle.open_obligation == 10


def test_exact_manual_close_is_atomic_audited_and_append_only(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    request, entry_cid = _prepared(store)
    before = (store.leg(entry_cid), store.cycle(GRID_ID, 3, 1), store.position_ledger())

    with store.transaction() as tx:
        settlement_id = store.record_external_settlement(tx, request)

    cycle = store.cycle(GRID_ID, 3, 1)
    assert settlement_id > 0
    assert (cycle.entry_filled, cycle.exit_filled, cycle.external_settled, cycle.open_obligation) == (
        Decimal("10"), Decimal("0"), Decimal("10"), Decimal("0"))
    assert store.external_position_totals() == (Decimal("0"), Decimal("10"))
    assert store.position_ledger().net == Decimal("0")
    assert store.leg(entry_cid) == before[0]
    assert store.fills(entry_cid)
    assert all(row.resolution == f"external_settlement:{settlement_id}" for row in store.unmatched_evidence(True))
    assert store.verify_ledger() == []
    assert cycle.state == "COMPLETE"
    assert store.cell(GRID_ID, 3).state == "IDLE"
    assert store.engine().engine_state == EngineState.STOPPED

    for table in ("external_settlements", "external_settlement_cycles", "external_settlement_evidence"):
        for statement in (f"UPDATE {table} SET rowid = rowid", f"DELETE FROM {table}"):
            with pytest.raises(StoreIntegrityError):
                with store.transaction():
                    store._x(statement)


def test_schema_v6_missing_settlement_table_fails_closed(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    store._conn.raw.execute("DROP TABLE external_settlement_cycles")

    with pytest.raises(StoreIntegrityError, match="schema v6 is missing"):
        store.external_settled_quantity(GRID_ID, 1, 1)


def test_late_tp_fill_is_retained_and_conflicts_when_x_plus_s_exceeds_e(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    request, _ = _prepared(store)
    cycle = store.cycle(GRID_ID, 3, 1)
    with store.transaction() as tx:
        tp = store.prepare_submit(tx, tp_leg(3), side=Side.SELL, price=cycle.tp_price,
                                  amount=Decimal("10"), order_type=OrderTypePolicy.LIMIT)
    submit_via_protocol(store, FakeTransport(), tp)
    leg = store.leg(tp.cid)
    venue_id = store.order(tp.cid).exchange_order_id
    terminal = order_row(tp.cid, leg.side, leg.price, leg.amount, Decimal("0"),
                         status="canceled", order_id=venue_id)
    with store.transaction() as tx:
        store.apply_history_batch(tx, [terminal])
        store.set_leg_state(tx, tp.cid, OrderState.TERMINAL, reason="proven canceled")
    with store.transaction() as tx:
        store.record_external_settlement(tx, request)

    with store.transaction() as tx:
        result = store.apply_history_batch(tx, [trade_row("late-tp", tp.cid, Side.SELL, "1", price=str(leg.price),
                                                          exchange_order_id=venue_id)])

    cycle = store.cycle(GRID_ID, 3, 1)
    assert (cycle.exit_filled, cycle.external_settled) == (Decimal("1"), Decimal("10"))
    assert any(conflict.kind == "EXTERNAL_SETTLEMENT_OVERALLOCATION" for conflict in result.conflicts)
    assert any("X+S" in problem for problem in store.verify_ledger())


def test_two_exact_manual_orders_are_ambiguous_and_refused(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    request, _ = _prepared(store)
    other_id = "manual-order-2"
    other_client_id = 700_000_000_002
    with store.transaction() as tx:
        store.apply_history_batch(tx, [
            trade_row("other-1", other_client_id, Side.SELL, "10", exchange_order_id=other_id),
            replace(order_row(other_client_id, Side.SELL, Decimal("5.1"), Decimal("10"), Decimal("10"),
                              order_id=other_id),
                    reduce_only=True),
        ])

    with pytest.raises(InvalidTransitionError, match="ambiguous"):
        with store.transaction() as tx:
            store.record_external_settlement(tx, request)
    assert store._conn.raw.execute("SELECT count(*) FROM external_settlements").fetchone()[0] == 0


@pytest.mark.parametrize("trade_clients,terminal_client", [
    ((700_000_000_001, 700_000_000_002), 700_000_000_001),
    ((700_000_000_001, 700_000_000_001), 700_000_000_002),
])
def test_manual_order_client_identity_mismatch_is_refused_without_writes(
        tmp_path, trade_clients, terminal_client):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    request, _ = _prepared(store, trade_client_ids=trade_clients, terminal_client_id=terminal_client)

    with pytest.raises(InvalidTransitionError, match="client order id"):
        with store.transaction() as tx:
            store.record_external_settlement(tx, request)
    assert store._conn.raw.execute("SELECT count(*) FROM external_settlements").fetchone()[0] == 0


def test_late_entry_after_settlement_reopens_exact_remainder_and_is_not_discarded(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    store.set_engine_state(None, EngineState.STOPPED, "synthetic external-settlement fixture")
    entry = record_entry_intent(store, 3)
    submit_via_protocol(store, FakeTransport(), entry)
    leg = store.leg(entry.cid)
    venue_id = store.order(entry.cid).exchange_order_id
    with store.transaction() as tx:
        store.apply_history_batch(tx, [
            trade_row("owned-partial", entry.cid, leg.side, "5", price=str(leg.price),
                      exchange_order_id=venue_id),
            order_row(entry.cid, leg.side, leg.price, leg.amount, Decimal("5"), status="canceled",
                      order_id=venue_id),
        ])
        store.set_leg_state(tx, entry.cid, OrderState.TERMINAL, reason="proven partial cancel")
    manual_id = "manual-partial"
    manual_client_id = 700_000_000_003
    with store.transaction() as tx:
        result = store.apply_history_batch(tx, [
            trade_row("manual-partial-trade", manual_client_id, Side.SELL, "5", exchange_order_id=manual_id),
            replace(order_row(manual_client_id, Side.SELL, Decimal("5.1"), Decimal("5"), Decimal("5"),
                              order_id=manual_id),
                    reduce_only=True),
        ])
    trade = next(row for row in result.unmatched if row.stream == "TRADES")
    terminal = next(row for row in result.unmatched if row.stream == "INACTIVE_ORDERS")
    engine = store.engine()
    request = ExternalSettlementRequest(
        proof_id="proof-partial-entry", grid_id=GRID_ID, settlement_side=Side.SELL,
        observed_position=Decimal("0"), actor="operator", reason="manual close",
        expected_config_revision=engine.config_revision, expected_engine_revision=engine.engine_revision,
        position_observed_at_ms=BOOT_CUT_MS + 30_000, active_observed_at_ms=BOOT_CUT_MS + 30_001,
        history_scan_started_at_ms=BOOT_CUT_MS + 30_002,
        history_scan_completed_at_ms=BOOT_CUT_MS + 30_003, trades_high_water=None, orders_high_water=None,
        cycles=(ExternalSettlementCycle(GRID_ID, 3, 1, Decimal("5")),),
        evidence=(ExternalSettlementEvidence(trade.id, "TRADE", Decimal("5")),
                  ExternalSettlementEvidence(terminal.id, "TERMINAL_ORDER")),
    )
    with store.transaction() as tx:
        store.record_external_settlement(tx, request)
    with store.transaction() as tx:
        late = store.apply_history_batch(tx, [trade_row("owned-late", entry.cid, leg.side, "1",
                                                        price=str(leg.price), exchange_order_id=venue_id)])

    cycle = store.cycle(GRID_ID, 3, 1)
    assert [fill.trade_id_str for fill in late.late_fills] == ["owned-late"]
    assert (cycle.entry_filled, cycle.exit_filled, cycle.external_settled, cycle.open_obligation) == (
        Decimal("6"), Decimal("0"), Decimal("5"), Decimal("1"))
    assert store.late_obligation_cycles() == [cycle]


def test_crash_before_settlement_commit_rolls_back_everything(tmp_path):
    env = Env(tmp_path)
    hooks = FaultHooks()
    store = env.open(fault_hooks=hooks)
    env.bootstrap(store)
    request, _ = _prepared(store)
    hooks.arm("before_command_commit")

    with pytest.raises(SimulatedCrash):
        with store.transaction() as tx:
            store.record_external_settlement(tx, request)

    reopened = env.open()
    assert reopened._conn.raw.execute("SELECT count(*) FROM external_settlements").fetchone()[0] == 0
    assert len(reopened.unmatched_evidence()) == 3
    cycle = reopened.cycle(GRID_ID, 3, 1)
    assert cycle.state == "OPEN" and cycle.external_settled == Decimal("0")


def test_successful_settlement_reopens_with_s_and_stop_intact(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    request, _ = _prepared(store)
    with store.transaction() as tx:
        settlement_id = store.record_external_settlement(tx, request)
    store.close()

    reopened = env.open()
    cycle = reopened.cycle(GRID_ID, 3, 1)
    assert (cycle.external_settled, cycle.open_obligation, cycle.state) == (
        Decimal("10"), Decimal("0"), "COMPLETE")
    assert reopened.position_ledger().net == Decimal("0")
    assert reopened.engine().engine_state == EngineState.STOPPED
    assert {row.resolution for row in reopened.unmatched_evidence(True)} == {
        f"external_settlement:{settlement_id}"}


def test_store_boundary_refuses_settlement_unless_engine_is_cleanly_stopped(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    request, _ = _prepared(store)
    store.set_engine_state(None, EngineState.PAUSED, "not stopped")
    request = replace(request, expected_engine_revision=store.engine().engine_revision)

    with pytest.raises(InvalidTransitionError, match="stopped"):
        with store.transaction() as tx:
            store.record_external_settlement(tx, request)


@pytest.mark.parametrize("change", ["partial", "wrong_side", "not_reduce_only", "not_final_status", "reused"])
def test_refusal_leaves_no_settlement_writes(tmp_path, change):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    request, _ = _prepared(store)
    if change == "partial":
        request = replace(request, evidence=request.evidence[:1] + request.evidence[-1:])
    elif change == "wrong_side":
        request = replace(request, settlement_side=Side.BUY)
    elif change == "not_reduce_only":
        order_id = request.evidence[-1].inbox_id
        store._conn.raw.execute("UPDATE history_inbox SET payload_json = replace(payload_json, '\"reduce_only\":true', "
                                "'\"reduce_only\":false') WHERE id = ?", (order_id,))
    elif change == "not_final_status":
        order_id = request.evidence[-1].inbox_id
        store._conn.raw.execute("UPDATE history_inbox SET payload_json = replace(payload_json, '\"status\":\"filled\"', "
                                "'\"status\":\"open\"') WHERE id = ?", (order_id,))
    elif change == "reused":
        with store.transaction() as tx:
            store.record_external_settlement(tx, request)
        request = replace(request, proof_id="proof-2")

    before = tuple(store._conn.raw.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in (
        "external_settlements", "external_settlement_cycles", "external_settlement_evidence"))
    with pytest.raises((ValueError, InvalidTransitionError, StoreIntegrityError)):
        with store.transaction() as tx:
            store.record_external_settlement(tx, request)
    expected = 1 if change == "reused" else 0
    assert store._conn.raw.execute("SELECT count(*) FROM external_settlements").fetchone()[0] == expected
    after = tuple(store._conn.raw.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in (
        "external_settlements", "external_settlement_cycles", "external_settlement_evidence"))
    assert after == before
