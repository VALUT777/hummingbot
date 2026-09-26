from dataclasses import replace
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import EngineState, Side
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    ExternalEntryEvidence,
    ExternalEntryRequest,
    FaultHooks,
    InvalidTransitionError,
    SimulatedCrash,
    StoreIntegrityError,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    BOOT_CUT_MS,
    GRID_ID,
    Env,
    order_row,
    trade_row,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.test_ng_store_grid_extension import (
    _bootstrap,
    _extension,
)


D = Decimal


def _request(store, *, trades=(D("40"), D("60")), side=Side.BUY, reduce_only=False,
             observed=D("100"), order_id="manual-buy-1", client_id=700_000_001):
    store.set_engine_state(None, EngineState.STOPPED_WITH_INVENTORY, "manual buy adoption fixture")
    rows = [trade_row(f"manual-buy-{i}", client_id, side, str(q), price="4.79",
                      exchange_order_id=order_id) for i, q in enumerate(trades)]
    rows.append(replace(order_row(client_id, side, D("4.8"), sum(trades), sum(trades), order_id=order_id),
                        reduce_only=reduce_only))
    with store.transaction() as tx:
        result = store.apply_history_batch(tx, rows)
    evidence = tuple(ExternalEntryEvidence(row.id, "TRADE", D(row.payload["size"]))
                     if row.stream == "TRADES" else ExternalEntryEvidence(row.id, "TERMINAL_ORDER")
                     for row in result.unmatched)
    engine = store.engine()
    reason = "adopt manual lower buy"
    return ExternalEntryRequest(
        extension=_extension(proof_id="b" * 64, actor="operator", reason=reason),
        proof_id="b" * 64, cell_id=10, entry_side=Side.BUY,
        quantity=D("100"), observed_position=observed, actor="operator", reason="adopt manual lower buy",
        expected_config_revision=engine.config_revision, expected_engine_revision=engine.engine_revision,
        position_observed_at_ms=BOOT_CUT_MS + 30_000, active_observed_at_ms=BOOT_CUT_MS + 30_001,
        history_scan_started_at_ms=BOOT_CUT_MS + 30_002,
        history_scan_completed_at_ms=BOOT_CUT_MS + 30_003, trades_high_water="trade-high",
        orders_high_water="order-high", evidence=evidence)


def test_atomic_extension_adopts_real_external_buy_without_fake_leg_or_baseline(tmp_path):
    store = Env(tmp_path).open()
    _bootstrap(store)
    request = _request(store)

    adoption_id = store.extend_grid_with_external_entry(None, request)

    cycle = store.cycle(GRID_ID, 10, 1)
    assert adoption_id > 0
    assert (store.grid().lower_price, cycle.entry_filled, cycle.open_obligation, cycle.tp_price) == (
        D("4.8"), D("100"), D("100"), D("4.9"))
    assert store.engine().effective_baseline == D("0")
    assert store.position_ledger().net == D("100")
    assert store.legs(grid_id=GRID_ID, cell_id=10) == []
    assert store.fills() == []
    assert store.verify_ledger() == []
    env = Env(tmp_path)
    store.close()
    reopened = env.open(config_fingerprint="fp-extended")
    assert reopened.position_ledger().net == D("100")
    assert reopened.cycle(GRID_ID, 10, 1).open_obligation == D("100")
    assert reopened.verify_ledger() == []


@pytest.mark.parametrize("change", ["partial", "wrong_side", "reduce_only", "wrong_position", "reused"])
def test_external_entry_refusal_is_atomic(tmp_path, change):
    store = Env(tmp_path).open()
    _bootstrap(store)
    request = _request(store, side=Side.SELL if change == "wrong_side" else Side.BUY,
                       reduce_only=change == "reduce_only",
                       observed=D("99") if change == "wrong_position" else D("100"))
    if change == "partial":
        request = replace(request, evidence=request.evidence[1:])
    if change == "reused":
        with store.transaction():
            store._x("UPDATE history_inbox SET resolved_at_ms=1, resolution='already-used' WHERE id=?",
                     (request.evidence[0].inbox_id,))
    with pytest.raises((InvalidTransitionError, ValueError)):
        store.extend_grid_with_external_entry(None, request)
    assert store.grid().lower_price == D("4.9")
    assert len(store.cells()) == 10
    assert store.engine().effective_baseline == D("0")


def test_external_entry_fault_rolls_back_window_cycle_evidence_and_audit(tmp_path):
    hooks = FaultHooks()
    store = Env(tmp_path).open(fault_hooks=hooks)
    _bootstrap(store)
    request = _request(store)
    hooks.arm("before_command_commit")
    with pytest.raises(SimulatedCrash):
        store.extend_grid_with_external_entry(None, request)
    store = Env(tmp_path).open()
    assert store.grid().lower_price == D("4.9") and len(store.cells()) == 10
    assert store._x("SELECT count(*) FROM external_entries").fetchone()[0] == 0
    assert all(row.resolved_at_ms is None for row in store.unmatched_evidence())
    assert store.audit_events("grid_extension_external_entry") == []


def test_applied_v8_missing_claim_trigger_fails_closed(tmp_path):
    store = Env(tmp_path).open()
    store._conn.raw.execute("DROP TRIGGER manual_evidence_claims_append_only_d")
    with pytest.raises(StoreIntegrityError, match="schema v8"):
        store.external_entered_quantity(GRID_ID, 10, 1)


def test_manual_client_id_zero_is_valid_evidence_identity(tmp_path):
    store = Env(tmp_path).open()
    _bootstrap(store)
    request = _request(store, client_id=0)
    assert store.extend_grid_with_external_entry(None, request) > 0


def test_verify_ledger_rejects_corrupted_external_entry_payload(tmp_path):
    store = Env(tmp_path).open()
    _bootstrap(store)
    request = _request(store)
    store.extend_grid_with_external_entry(None, request)
    trade_id = next(item.inbox_id for item in request.evidence if item.evidence_role == "TRADE")
    store._conn.raw.execute(
        "UPDATE history_inbox SET payload_json = replace(payload_json, '\"own_side\":\"BUY\"', "
        "'\"own_side\":\"SELL\"') WHERE id = ?", (trade_id,))
    assert any("trade allocation/payload mismatch" in problem for problem in store.verify_ledger())
