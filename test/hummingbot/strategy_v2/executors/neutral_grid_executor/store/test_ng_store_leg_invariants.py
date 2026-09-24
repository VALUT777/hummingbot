"""Leg/cycle invariants the store enforces from its own durable facts (NG-CELL-001/002, NG-DB-005; persistence
parts of AC-05, AC-07, AC-17, AC-21, AC-56)."""
import sqlite3
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    OrderState,
    OrderTypePolicy,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.migrations import MIGRATIONS, Migration
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    EngineIdentity,
    IdentityMismatchError,
    InvalidTransitionError,
    NeutralGridStore,
    PersistenceError,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    DOMAIN,
    GRID_ID,
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


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


@pytest.fixture
def store(env):
    opened = env.open()
    env.bootstrap(opened)
    return opened


def _live_entry(store, cell_id=3):
    intent = record_entry_intent(store, cell_id=cell_id)
    submit_via_protocol(store, FakeTransport(), intent)
    return store.leg(intent.cid)


def _fill(store, leg, trade_id, size):
    with store.transaction() as tx:
        store.apply_history_batch(tx, [trade_row(trade_id, leg.cid, leg.side, size, price=str(leg.price),
                                                 exchange_order_id=f"9{leg.cid}")])


def _tp(store, cell_id, revision, amount, transport=None):
    cycle = store.cycle(GRID_ID, cell_id, 1)
    with store.transaction() as tx:
        intent = store.prepare_submit(tx, tp_leg(cell_id, revision=revision), side=cycle.tp_side,
                                      price=cycle.tp_price, amount=Decimal(amount), order_type=OrderTypePolicy.LIMIT)
    if transport is not None:
        submit_via_protocol(store, transport, intent)
    return intent


def test_one_physical_entry_per_cycle(store):
    entry = _live_entry(store)
    cell = store.cell(GRID_ID, 3)
    with pytest.raises(InvalidTransitionError, match="already has entry leg"):
        with store.transaction() as tx:
            store.prepare_submit(tx, entry_leg(3, revision=1), side=cell.entry_side, price=cell.spec().entry_price,
                                 amount=Decimal("10"), order_type=OrderTypePolicy.LIMIT_MAKER)
    _fill(store, entry, "e-1", "4")  # AC-07: entry canceled after partial fill
    with store.transaction() as tx:
        store.apply_history_batch(tx, [order_row(entry.cid, entry.side, entry.price, entry.amount, Decimal("4"),
                                                 status="canceled", order_id=f"9{entry.cid}")])
        store.set_leg_state(tx, entry.cid, OrderState.TERMINAL, reason="canceled after partial")
    with pytest.raises(InvalidTransitionError, match="new cycle starts with the full amount"):
        with store.transaction() as tx:
            store.prepare_submit(tx, entry_leg(3, revision=1), side=cell.entry_side, price=cell.spec().entry_price,
                                 amount=Decimal("10"), order_type=OrderTypePolicy.LIMIT_MAKER)


def test_tp_never_exceeds_confirmed_entry_partial_sequence(store):
    transport = FakeTransport()
    entry = _live_entry(store)
    with pytest.raises(InvalidTransitionError, match="exceeds confirmed entry"):
        _tp(store, 3, 0, "1")  # nothing confirmed by history yet (a WS fill is not enough)
    _fill(store, entry, "e-1", "2")
    _tp(store, 3, 0, "2", transport)
    with pytest.raises(InvalidTransitionError, match="exceeds confirmed entry"):
        _tp(store, 3, 1, "1")
    _fill(store, entry, "e-2", "3")
    _tp(store, 3, 1, "3", transport)
    _fill(store, entry, "e-3", "5")
    _tp(store, 3, 2, "5", transport)
    cycle = store.cycle(GRID_ID, 3, 1)
    assert cycle.entry_filled == Decimal("10") and store.leg(entry.cid).state == OrderState.LIVE
    tps = [leg for leg in store.legs(cell_id=3) if leg.role.value == "TP"]
    assert sum(leg.amount for leg in tps) == Decimal("10")  # AC-05: 2 + 3 + 5, nothing lost or doubled


def test_unknown_tp_keeps_reservation_and_blocks_duplicate_tp(store):
    transport = FakeTransport()
    entry = _live_entry(store)
    _fill(store, entry, "e-1", "10")
    transport.results.append(TransportResult(TransportOutcome.UNKNOWN, "timeout"))
    first = _tp(store, 3, 0, "10", transport)
    assert store.leg(first.cid).state == OrderState.SUBMIT_UNKNOWN
    with pytest.raises(InvalidTransitionError, match="exceeds confirmed entry"):
        _tp(store, 3, 1, "10")  # status lag must not create a duplicate TP
    assert first.cid in {r.cid for r in store.reservations()}


def test_live_requires_evidence_and_active_row_resolves_unknown(store):
    transport = FakeTransport()
    transport.results.append(TransportResult(TransportOutcome.UNKNOWN, "timeout"))
    intent = record_entry_intent(store, cell_id=3)
    submit_via_protocol(store, transport, intent)
    with pytest.raises(InvalidTransitionError, match="LIVE unproven"):
        with store.transaction() as tx:
            store.set_leg_state(tx, intent.cid, OrderState.LIVE, reason="guess")
    leg = store.leg(intent.cid)
    active = order_row(intent.cid, leg.side, leg.price, leg.amount, Decimal("0"), status="open", order_id="777")
    with store.transaction() as tx:  # AC-17: the saved CID is found in active orders
        assert store.record_order_evidence(tx, active) == intent.cid
        store.set_leg_state(tx, intent.cid, OrderState.LIVE, reason="seen in active orders")
    assert store.order(intent.cid).exchange_order_id == "777"
    assert store.order(intent.cid).venue_final is False
    with store.transaction() as tx:  # an unknown active order is never adopted
        assert store.record_order_evidence(tx, order_row(424242, leg.side, leg.price, leg.amount, Decimal("0"),
                                                         status="open", order_id="888")) is None


def test_zero_fill_rejection_needs_proof_timeouts_stay_unknown(store):
    transport = FakeTransport()
    transport.results.append(TransportResult(TransportOutcome.UNKNOWN, "not found"))
    intent = record_entry_intent(store, cell_id=3)
    submit_via_protocol(store, transport, intent)
    with pytest.raises(InvalidTransitionError, match="rejection unproven"):
        with store.transaction() as tx:
            store.set_leg_state(tx, intent.cid, OrderState.REJECTED_ZERO_FILL, reason="vanished")
    with pytest.raises(InvalidTransitionError):  # engine override follows the same evidence rules
        with store.transaction() as tx:
            store.record_transport_result(tx, intent.cid, TransportResult(TransportOutcome.UNKNOWN, "not found"),
                                          leg_state=OrderState.TERMINAL)
    assert store.leg(intent.cid).state == OrderState.SUBMIT_UNKNOWN
    leg = store.leg(intent.cid)
    with store.transaction() as tx:
        store.apply_history_batch(tx, [order_row(intent.cid, leg.side, leg.price, leg.amount, Decimal("0"),
                                                 status="canceled-post-only", order_id="31")])
        store.set_leg_state(tx, intent.cid, OrderState.REJECTED_ZERO_FILL, reason="final zero-fill row")
    assert store.reservations() == []


def test_full_cycle_release_then_same_side_rearm(store):
    transport = FakeTransport()
    entry = _live_entry(store, cell_id=10)
    fill_and_terminate(store, entry.cid, "e-10")
    tp = _tp(store, 10, 0, "10", transport)
    fill_and_terminate(store, tp.cid, "t-10")
    with store.transaction() as tx:
        store.close_cycle(tx, GRID_ID, 10, 1, "proven")
        second = store.open_cycle(tx, GRID_ID, 10)
    assert second.generation == 2 and second.entry_side == entry.side and second.planned_amount == Decimal("10")


def test_resend_is_refused_while_degraded(env, store):
    transport = FakeTransport()
    transport.results.append(TransportResult(TransportOutcome.UNKNOWN, "timeout"))
    intent = record_entry_intent(store, cell_id=3)
    submit_via_protocol(store, transport, intent)
    store.degraded_reason = "commit: SQLITE_FULL: database or disk is full"
    with pytest.raises(PersistenceError, match="degraded"):
        with store.transaction() as tx:
            store.mark_dispatching(tx, intent.outbox_id, resend_same_cid=True)


def test_foreign_database_is_not_migrated_before_identity_check(env):
    env.open().close()
    other = EngineIdentity(connector_name=DOMAIN, connector_domain=DOMAIN, account_index=99, trading_pair="LIT-USDG")
    v2 = Migration(2, "add_example", "CREATE TABLE example_v2 (id INTEGER PRIMARY KEY) STRICT;")
    with pytest.raises(IdentityMismatchError):
        NeutralGridStore.open(env.db, other, lock_dir=env.locks, clock_ms=env.clock, migrations=MIGRATIONS + (v2,))
    raw = sqlite3.connect(str(env.db))
    try:
        assert raw.execute("PRAGMA user_version").fetchone()[0] == 1
    finally:
        raw.close()
