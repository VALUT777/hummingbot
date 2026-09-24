"""Regression tests for the orchestrator's adversarial review of WS-B (review-wsb.json, 11 confirmed issues plus the
WS-C order-key alignment). Each test failed on 41d347fd6, before the fixes."""
import contextlib
import shutil
from decimal import Decimal

import pytest

import hummingbot
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    OrderState,
    OrderTypePolicy,
    Side,
    SubmitRequest,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    CursorUpdate,
    FaultHooks,
    InvalidTransitionError,
    NeutralGridStore,
    PersistenceError,
    PriorRunEvidenceError,
    SimulatedCrash,
    StoreLockedError,
    canonical_json,
    default_db_path,
    order_dedupe_key,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    ACCOUNT,
    BOOT_CUT_MS,
    DOMAIN,
    GRID_ID,
    IDENTITY,
    MARKET,
    Env,
    FakeTransport,
    entry_leg,
    fill_and_terminate,
    order_row,
    record_entry_intent,
    submit_via_protocol,
    tp_leg,
    trade_row,
)

HOST_DIR_ENV = "HUMMINGBOT_NEUTRAL_GRID_HOST_DIR"


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


@pytest.fixture
def store(env):
    opened = env.open(fault_hooks=FaultHooks())
    env.bootstrap(opened)
    return opened


def _apply(store, rows, **kwargs):
    with store.transaction() as tx:
        return store.apply_history_batch(tx, rows, **kwargs)


def _live(store, cell_id, transport=None, **kwargs):
    intent = record_entry_intent(store, cell_id=cell_id)
    submit_via_protocol(store, transport or FakeTransport(), intent)
    return store.leg(intent.cid)


def _fill(store, leg, trade_id, size, ts=BOOT_CUT_MS + 10_000):
    order = store.order(leg.cid)
    return _apply(store, [trade_row(trade_id, leg.cid, leg.side, size, price=str(leg.price), ts=ts,
                                    exchange_order_id=order.exchange_order_id)])


def _final_row(store, leg, filled, status="canceled"):
    order = store.order(leg.cid)
    return order_row(leg.cid, leg.side, leg.price, leg.amount, Decimal(filled), status=status,
                     order_id=order.exchange_order_id)


def _tp(store, cell_id, revision, amount, generation=1, allocations=None, transport=None):
    cycle = store.cycle(GRID_ID, cell_id, generation)
    leg = tp_leg(cell_id, generation, revision)
    with store.transaction() as tx:
        cid = store.allocate_cid(tx, leg)
        intent = store.record_intent(tx, leg, SubmitRequest(cid, cycle.tp_side, cycle.tp_price, Decimal(amount),
                                                            OrderTypePolicy.LIMIT), allocations=allocations)
    if transport is not None:
        submit_via_protocol(store, transport, intent)
    return intent


# ---------------------------------------------------------------------------------------------- 1 (HIGH) host lock


def test_review1_lock_and_prior_run_marker_are_host_wide_not_per_data_path(tmp_path, monkeypatch):
    monkeypatch.setenv(HOST_DIR_ENV, str(tmp_path / "host"))
    checkout_a, checkout_b = tmp_path / "checkout-a" / "data", tmp_path / "checkout-b" / "data"
    monkeypatch.setattr(hummingbot, "_data_path", str(checkout_a))
    first = NeutralGridStore.open(None, IDENTITY, create_if_missing=True)
    assert first.path == default_db_path(IDENTITY, checkout_a).absolute()
    monkeypatch.setattr(hummingbot, "_data_path", str(checkout_b))
    with pytest.raises(StoreLockedError):  # second checkout, same account/market, default lock dir
        NeutralGridStore.open(None, IDENTITY, create_if_missing=True)
    first.close()
    with pytest.raises(PriorRunEvidenceError):  # checkout B may not start a fresh epoch-1 database either
        NeutralGridStore.open(None, IDENTITY, create_if_missing=True)
    shutil.rmtree(checkout_a)  # wiping the whole data dir of A does not allow a fresh bootstrap
    monkeypatch.setattr(hummingbot, "_data_path", str(checkout_a))
    with pytest.raises(PriorRunEvidenceError):
        NeutralGridStore.open(None, IDENTITY, create_if_missing=True)
    assert not list((tmp_path / "checkout-a").rglob("*.sqlite3"))


# ---------------------------------------------------------------------------------------------- 2 (HIGH) late obligation


def _released_cell_with_late_fill(store, cell_id=3):
    transport = FakeTransport()
    entry = _live(store, cell_id, transport)
    _fill(store, entry, "e-1", "4")
    with store.transaction() as tx:
        store.apply_history_batch(tx, [_final_row(store, entry, "4")])
        store.set_leg_state(tx, entry.cid, OrderState.TERMINAL, reason="canceled after partial")
    tp = _tp(store, cell_id, 0, "4", transport=transport)
    fill_and_terminate(store, tp.cid, "t-1", size=Decimal("4"), ts=BOOT_CUT_MS + 20_000)
    with store.transaction() as tx:
        store.close_cycle(tx, GRID_ID, cell_id, 1, "released")
    late = _fill(store, entry, "e-late", "1", ts=BOOT_CUT_MS + 90_000)
    return entry, late


def test_review2_late_fill_on_released_cycle_is_an_outstanding_obligation(env, store):
    entry, late = _released_cell_with_late_fill(store)
    cycle = store.cycle(GRID_ID, 3, 1)
    assert (cycle.state, cycle.entry_filled, cycle.exit_filled) == ("COMPLETE", Decimal("5"), Decimal("4"))
    store.close()
    store = env.open()
    state = store.load_state()
    assert [(c.cell_id, c.generation) for c in state.late_cycles] == [(3, 1)]  # restart sees the obligation
    assert any("cell 3 gen 1" in b for b in store.grid_mutation_blockers())
    with pytest.raises(InvalidTransitionError):
        with store.transaction() as tx:
            store.open_cycle(tx, GRID_ID, 3)
    store.record_manual_reconciliation(None, "operator", "late fill confirmed on venue", {"trade": "e-late"},
                                       resolved_conflict_ids=[c.id for c in store.open_conflicts()])
    with pytest.raises(InvalidTransitionError):  # audited, but the 1 LIT obligation is still uncovered
        with store.transaction() as tx:
            store.open_cycle(tx, GRID_ID, 3)
    transport = FakeTransport()
    late_tp = _tp(store, 3, 1, "1", transport=transport)  # TP of the old cycle after the audit
    fill_and_terminate(store, late_tp.cid, "t-late", size=Decimal("1"), ts=BOOT_CUT_MS + 95_000)
    assert store.open_conflicts() == []  # the covering TP fill is not itself "late"
    cycle = store.cycle(GRID_ID, 3, 1)
    assert cycle.entry_filled == cycle.exit_filled == Decimal("5")
    assert store.load_state().late_cycles == [] and store.grid_mutation_blockers() == []
    with store.transaction() as tx:
        assert store.open_cycle(tx, GRID_ID, 3).generation == 2


# ---------------------------------------------------------------------------------------------- 3 (HIGH) aggregate TP


def _entry_filled(store, cell_id, size="10"):
    entry = _live(store, cell_id)
    _fill(store, entry, f"e-{cell_id}", size)
    return entry


def test_review3_terminal_aggregate_tp_with_unallocated_fills_cannot_admit_duplicate_tp(store):
    _entry_filled(store, 5)
    transport = FakeTransport()
    tp = _tp(store, 5, 0, "10", allocations=[(GRID_ID, 5, 1, Decimal("10"))], transport=transport)
    leg = store.leg(tp.cid)
    _apply(store, [trade_row("tp-1", tp.cid, leg.side, "10", price=str(leg.price),
                             exchange_order_id=store.order(tp.cid).exchange_order_id),
                   _final_row(store, leg, "10", status="filled")])
    with pytest.raises(InvalidTransitionError, match="unallocated"):
        with store.transaction() as tx:
            store.set_leg_state(tx, tp.cid, OrderState.TERMINAL, reason="final row")
    with pytest.raises(InvalidTransitionError, match="exceeds confirmed entry"):
        _tp(store, 5, 1, "10")


def test_review3_late_unallocated_fill_on_final_aggregate_tp_still_counts(store):
    _entry_filled(store, 5)
    transport = FakeTransport()
    tp = _tp(store, 5, 0, "10", allocations=[(GRID_ID, 5, 1, Decimal("10"))], transport=transport)
    leg = store.leg(tp.cid)
    result = _apply(store, [trade_row("tp-1", tp.cid, leg.side, "6", price=str(leg.price),
                                      exchange_order_id=store.order(tp.cid).exchange_order_id)])
    with store.transaction() as tx:
        store.allocate_fill(tx, result.new_fills[0].dedupe_key, [(GRID_ID, 5, 1, Decimal("6"))])
        store.apply_history_batch(tx, [_final_row(store, leg, "6")])
        store.set_leg_state(tx, tp.cid, OrderState.TERMINAL, reason="canceled after 6")
    _apply(store, [trade_row("tp-late", tp.cid, leg.side, "2", price=str(leg.price), ts=BOOT_CUT_MS + 80_000,
                             exchange_order_id=store.order(tp.cid).exchange_order_id)])
    with pytest.raises(InvalidTransitionError, match="exceeds confirmed entry"):
        _tp(store, 5, 1, "4")  # E=10, X=6, 2 already sold but unallocated: only 2 may still be covered


# ---------------------------------------------------------------------------------------------- 4 NOT_SENT after send


@pytest.mark.parametrize("outcome", [TransportOutcome.NOT_SENT, TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL])
def test_review4_release_outcome_on_a_resend_keeps_unknown_and_reservation(store, outcome):
    transport = FakeTransport()
    transport.results.append(TransportResult(TransportOutcome.UNKNOWN, "timeout"))
    intent = record_entry_intent(store, cell_id=3)
    submit_via_protocol(store, transport, intent)
    with store.transaction() as tx:
        store.mark_dispatching(tx, intent.outbox_id, resend_same_cid=True)
    with store.transaction() as tx:
        store.record_transport_result(tx, intent.cid, TransportResult(outcome, "duplicate client id / pre-send"))
    assert store.leg(intent.cid).state == OrderState.SUBMIT_UNKNOWN
    assert [r.cid for r in store.reservations()] == [intent.cid]
    assert [a["outcome"] for a in store.outbox_attempts(intent.outbox_id)] == ["UNKNOWN", outcome.value]
    cell = store.cell(GRID_ID, 3)
    with pytest.raises(InvalidTransitionError):  # no new CID for the same exposure
        with store.transaction() as tx:
            store.prepare_submit(tx, entry_leg(3, revision=1), side=cell.entry_side, price=cell.spec().entry_price,
                                 amount=Decimal("10"), order_type=OrderTypePolicy.LIMIT_MAKER)


def test_review4_not_sent_after_venue_evidence_does_not_release(store):
    intent = record_entry_intent(store, cell_id=3)
    with store.transaction() as tx:
        store.mark_dispatching(tx, intent.outbox_id)
    leg = store.leg(intent.cid)
    with store.transaction() as tx:  # the order shows up in active orders before any result is written
        store.record_order_evidence(tx, order_row(intent.cid, leg.side, leg.price, leg.amount, Decimal("0"),
                                                  status="open", order_id="777"))
        store.set_leg_state(tx, intent.cid, OrderState.LIVE, reason="active orders")
    with store.transaction() as tx:
        store.record_transport_result(tx, intent.cid, TransportResult(TransportOutcome.NOT_SENT, "pre-send"))
    assert store.leg(intent.cid).state == OrderState.LIVE
    assert [r.cid for r in store.reservations()] == [intent.cid]


# ---------------------------------------------------------------------------------------------- 5 DISPATCHED re-dispatch


def test_review5_dispatched_row_needs_explicit_same_cid_resend_and_respects_degraded(env, store):
    transport = FakeTransport()
    intent = record_entry_intent(store, cell_id=3)
    store.fault_hooks.arm("after_transport")
    with pytest.raises(SimulatedCrash):
        submit_via_protocol(store, transport, intent)
    assert len(transport.submits) == 1
    store = env.open()
    [row] = store.unresolved_outbox()
    assert row.status == "DISPATCHED" and row.attempts == 1
    with pytest.raises(InvalidTransitionError):
        with store.transaction() as tx:
            store.mark_dispatching(tx, row.id)
    store.degraded_reason = "commit: SQLITE_FULL"
    with pytest.raises(PersistenceError, match="degraded"):
        with store.transaction() as tx:
            store.mark_dispatching(tx, row.id, resend_same_cid=True)
    store.degraded_reason = None
    with store.transaction() as tx:
        again = store.mark_dispatching(tx, row.id, resend_same_cid=True)
    assert again.attempts == 2


# ---------------------------------------------------------------------------------------------- 6 no half-applied tx


def test_review6_caught_guard_error_cannot_commit_half_applied_allocation(store):
    _entry_filled(store, 5)
    tp = _tp(store, 5, 0, "10", allocations=[(GRID_ID, 5, 1, Decimal("10"))], transport=FakeTransport())
    leg = store.leg(tp.cid)
    result = _apply(store, [trade_row("tp-1", tp.cid, leg.side, "10", price=str(leg.price),
                                      exchange_order_id=store.order(tp.cid).exchange_order_id)])
    key = result.new_fills[0].dedupe_key
    with contextlib.suppress(PersistenceError):
        with store.transaction() as tx:
            try:
                store.allocate_fill(tx, key, [(GRID_ID, 5, 1, Decimal("4")), (GRID_ID, 99, 1, Decimal("6"))])
            except (InvalidTransitionError, KeyError, ValueError):
                pass  # engine shows the blocker and carries on
    assert store.cycle(GRID_ID, 5, 1).exit_filled == Decimal("0")
    assert [f.dedupe_key for f in store.unallocated_fills()] == [key]
    store.allocate_fill(None, key, [(GRID_ID, 5, 1, Decimal("10"))])
    assert store.cycle(GRID_ID, 5, 1).exit_filled == Decimal("10")


def test_review6_caught_allocation_sum_error_leaves_no_dispatchable_intent(store):
    _entry_filled(store, 5)
    cycle = store.cycle(GRID_ID, 5, 1)
    with contextlib.suppress(PersistenceError):
        with store.transaction() as tx:
            cid = store.allocate_cid(tx, tp_leg(5))
            try:
                store.record_intent(tx, tp_leg(5), SubmitRequest(cid, cycle.tp_side, cycle.tp_price, Decimal("10"),
                                                                 OrderTypePolicy.LIMIT),
                                    allocations=[(GRID_ID, 5, 1, Decimal("9"))])
            except (InvalidTransitionError, ValueError):
                pass
    assert [leg for leg in store.legs(cell_id=5) if leg.role.value == "TP"] == []
    assert all(o.kind != "SUBMIT" or store.leg(o.cid).role.value != "TP" for o in store.unresolved_outbox())


def test_review6_any_store_error_after_writes_poisons_the_transaction(store):
    entry = _live(store, 3)
    with store.transaction() as tx:
        store.update_cursors(tx, [CursorUpdate("TRADES", high_water_ts_ms=BOOT_CUT_MS + 50)])
    with pytest.raises(PersistenceError):
        with store.transaction() as tx:
            try:  # rows are applied first, then the cursor update is refused as moving backwards
                store.apply_history_batch(tx, [trade_row("t-1", entry.cid, entry.side, "2", price=str(entry.price),
                                                         exchange_order_id=store.order(entry.cid).exchange_order_id)],
                                          [CursorUpdate("TRADES", high_water_ts_ms=BOOT_CUT_MS + 10)])
            except InvalidTransitionError:
                pass
    assert store.leg(entry.cid).filled == Decimal("0") and store.fills() == []


# ---------------------------------------------------------------------------------------------- 7 / 8 allocations


def test_review7_allocations_must_share_tp_side_and_fixed_tp_price(store):
    _entry_filled(store, 5)       # BUY cell: TP SELL @ P6
    _entry_filled(store, 40)      # SELL cell: TP BUY @ P40
    _entry_filled(store, 6)       # BUY cell: TP SELL @ P7 (same side, other price)
    for other in (40, 6):
        with pytest.raises(InvalidTransitionError, match="same TP side and price"):
            _tp(store, 5, 0 if other == 40 else 1, "20",
                allocations=[(GRID_ID, 5, 1, Decimal("10")), (GRID_ID, other, 1, Decimal("10"))])
    assert [leg for leg in store.legs() if leg.role.value == "TP"] == []


def test_review8_entry_intents_cannot_carry_allocations(store):
    with store.transaction() as tx:
        store.open_cycle(tx, GRID_ID, 3)
        store.open_cycle(tx, GRID_ID, 4)
    cell = store.cell(GRID_ID, 3)
    with pytest.raises(InvalidTransitionError, match="ENTRY"):
        with store.transaction() as tx:
            cid = store.allocate_cid(tx, entry_leg(3))
            store.record_intent(tx, entry_leg(3), SubmitRequest(cid, cell.entry_side, cell.spec().entry_price,
                                                                Decimal("20"), OrderTypePolicy.LIMIT_MAKER),
                                allocations=[(GRID_ID, 3, 1, Decimal("10")), (GRID_ID, 4, 1, Decimal("10"))])
    assert store.legs() == []


# ---------------------------------------------------------------------------------------------- 9 cumulative re-flag


def test_review9_new_excess_after_resolved_cumulative_conflict_is_flagged_again(store):
    entry = _live(store, 3)
    _apply(store, [_final_row(store, entry, "3")])
    first = _fill(store, entry, "t-1", "4")
    [conflict] = first.conflicts
    assert conflict.kind == "CUMULATIVE_EXCEEDS_ORDER"
    store.record_manual_reconciliation(None, "operator", "checked", {"x": 1}, resolved_conflict_ids=[conflict.id])
    second = _fill(store, entry, "t-2", "2", ts=BOOT_CUT_MS + 20_000)
    assert [c.kind for c in second.conflicts if c.new] == ["CUMULATIVE_EXCEEDS_ORDER"]
    assert [c.kind for c in store.open_conflicts()] == ["CUMULATIVE_EXCEEDS_ORDER"]


# ---------------------------------------------------------------------------------------------- 10 retention gap


def test_review10_retention_gap_is_cleared_only_by_a_reconciliation_naming_it(store):
    entry = _live(store, 3)
    _fill(store, entry, "t-1", "2")
    conflict = _fill(store, entry, "t-1", "3").conflicts[0]  # unrelated payload conflict
    store.mark_retention_gap(None, "TRADES", required_boundary_ts_ms=BOOT_CUT_MS - 3_600_000,
                             oldest_available_ts_ms=BOOT_CUT_MS - 60_000)
    store.record_manual_reconciliation(None, "operator", "size 2 confirmed", {"ticket": "X"},
                                       resolved_conflict_ids=[conflict.id])
    assert any("retention gap on TRADES" in b for b in store.entry_blockers())
    with pytest.raises(InvalidTransitionError):  # the scanner cannot declare the gapped stream complete
        with store.transaction() as tx:
            store.update_cursors(tx, [CursorUpdate("TRADES", complete=True)])
    store.record_manual_reconciliation(None, "operator", "venue export covers the gap", {"export": "trades.csv"},
                                       resolved_retention_gaps=["TRADES"])
    assert store.entry_blockers() == []
    with store.transaction() as tx:
        store.update_cursors(tx, [CursorUpdate("TRADES", complete=True)])


def test_review10_gap_written_through_cursors_blocks_and_cannot_be_complete(store):
    with pytest.raises(InvalidTransitionError):
        with store.transaction() as tx:
            store.update_cursors(tx, [CursorUpdate("INACTIVE_ORDERS", required_boundary_ts_ms=BOOT_CUT_MS - 10,
                                                   oldest_available_ts_ms=BOOT_CUT_MS, complete=True)])
    with store.transaction() as tx:
        store.update_cursors(tx, [CursorUpdate("INACTIVE_ORDERS", required_boundary_ts_ms=BOOT_CUT_MS - 10,
                                               oldest_available_ts_ms=BOOT_CUT_MS, complete=False,
                                               incomplete_reason="boundary older than history")])
    assert any("retention gap on INACTIVE_ORDERS" in b for b in store.entry_blockers())


# ---------------------------------------------------------------------------------------------- 11 final only from history


def test_review11_non_history_row_can_never_be_terminal_proof(store):
    entry = _live(store, 3)
    ws_cancel = _final_row(store, entry, "0")
    with pytest.raises(ValueError):
        with store.transaction() as tx:
            store.record_order_evidence(tx, ws_cancel, final=True)
    with store.transaction() as tx:
        store.record_order_evidence(tx, ws_cancel)
    with pytest.raises(InvalidTransitionError, match="unproven"):
        with store.transaction() as tx:
            store.set_leg_state(tx, entry.cid, OrderState.TERMINAL, reason="ws cancel")
    assert [r.cid for r in store.reservations()] == [entry.cid]


# ---------------------------------------------------------------------------------------------- WS-C order key


def test_order_key_is_the_exact_exchange_order_id_like_ws_c(store):
    entry = _live(store, 3)
    exchange_id = store.order(entry.cid).exchange_order_id
    row = _final_row(store, entry, "0")
    assert order_dedupe_key(DOMAIN, row) == canonical_json(["ORDER", DOMAIN, ACCOUNT, MARKET, exchange_id])
    _apply(store, [row], dedupe_keys=[(DOMAIN, ACCOUNT, MARKET, exchange_id)])
    other_client = order_row(424242, entry.side, entry.price, entry.amount, Decimal("0"), status="canceled",
                             order_id=exchange_id)
    assert [c.kind for c in _apply(store, [other_client]).conflicts] == ["PAYLOAD_MISMATCH"]
    no_id = order_row(entry.cid, Side.BUY, entry.price, entry.amount, Decimal("0"), order_id=None)
    with pytest.raises(ValueError, match="exchange order id"):
        _apply(store, [no_id])
