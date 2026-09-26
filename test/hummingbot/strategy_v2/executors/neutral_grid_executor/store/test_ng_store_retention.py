"""History retention gap and audited manual reconciliation (AC-53 persistence part, NG-DB-004)."""
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    OrderState,
    OrderTypePolicy,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    CursorUpdate,
    EntryBlockedError,
    InvalidTransitionError,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    BOOT_CUT_MS,
    GRID_ID,
    Env,
    FakeTransport,
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
    env.bootstrap(opened, baseline=Decimal("50"))
    return opened


def test_required_overlap_boundary_is_durable(env, store):
    with store.transaction() as tx:
        store.update_cursors(tx, [CursorUpdate("INACTIVE_ORDERS", required_boundary_ts_ms=BOOT_CUT_MS - 60_000,
                                               required_boundary_marker="oldest-unresolved-cid",
                                               complete=False, incomplete_reason="scan in progress")])
    store.close()
    cursor = env.open().cursor("INACTIVE_ORDERS")
    assert cursor.required_boundary_ts_ms == BOOT_CUT_MS - 60_000
    assert cursor.required_boundary_marker == "oldest-unresolved-cid" and cursor.complete is False


def test_retention_gap_blocks_entries_but_not_tp_until_audited_reconciliation(env, store):
    transport = FakeTransport()
    entry = record_entry_intent(store, cell_id=2)
    submit_via_protocol(store, transport, entry)
    leg = store.leg(entry.cid)
    with store.transaction() as tx:
        store.apply_history_batch(tx, [trade_row("t-1", entry.cid, leg.side, "4", price=str(leg.price),
                                                 exchange_order_id=f"9{entry.cid}")])
    fills_before = store.fills()
    store.mark_retention_gap(None, "TRADES", required_boundary_ts_ms=BOOT_CUT_MS - 3_600_000,
                             oldest_available_ts_ms=BOOT_CUT_MS - 60_000)
    store.close()

    reopened = env.open()
    engine = reopened.engine()
    assert engine.manual_reconcile_required and "retention gap on TRADES" in engine.manual_reconcile_reason
    cursor = reopened.cursor("TRADES")
    assert cursor.complete is False and cursor.oldest_available_ts_ms == BOOT_CUT_MS - 60_000
    with pytest.raises(EntryBlockedError, match="manual reconciliation"):
        record_entry_intent(reopened, cell_id=3)
    cycle = reopened.cycle(GRID_ID, 2, 1)
    with reopened.transaction() as tx:  # confirmed obligation still gets its TP
        reopened.prepare_submit(tx, tp_leg(2), side=cycle.tp_side, price=cycle.tp_price, amount=Decimal("4"),
                                order_type=OrderTypePolicy.LIMIT)
    with pytest.raises(ValueError):
        reopened.record_manual_reconciliation(None, "operator", "checked", {})
    audit_id = reopened.record_manual_reconciliation(
        None, "operator", "exported venue history from UI; no unknown executions",
        {"export": "trades-2026-09-24.csv", "sha256": "abc"}, resolved_retention_gaps=["TRADES"])
    after = reopened.engine()
    assert not after.manual_reconcile_required and after.reconciliation_revision == engine.reconciliation_revision + 1
    # no reset / rebaseline: baseline, cells, cycle quantities and fills are untouched
    assert after.initial_baseline == Decimal("50") == after.effective_baseline
    assert reopened.fills() == fills_before and reopened.cycle(GRID_ID, 2, 1).entry_filled == Decimal("4")
    assert len(reopened.cells()) == 55
    assert reopened.audit_events("manual_reconcile")[0].id == audit_id
    assert [e.kind for e in reopened.audit_events()][:3].count("manual_reconcile") == 1
    record_entry_intent(reopened, cell_id=3)


def test_manual_reconciliation_resolves_unknown_legs_only_with_audit(store):
    transport = FakeTransport()
    transport.results.append(TransportResult(TransportOutcome.UNKNOWN, "timeout"))
    unknown = record_entry_intent(store, cell_id=4)
    submit_via_protocol(store, transport, unknown)
    assert store.leg(unknown.cid).state == OrderState.SUBMIT_UNKNOWN
    store.record_manual_reconciliation(None, "operator", "venue shows no such order after 7 days",
                                       {"venue_query": "no order with client id"},
                                       leg_resolutions={unknown.cid: OrderState.REJECTED_ZERO_FILL})
    assert store.leg(unknown.cid).state == OrderState.REJECTED_ZERO_FILL
    assert store.reservations() == [] and store.unresolved_outbox() == []
    transitions = store.state_transitions("LEG", str(unknown.cid))
    assert transitions[-1]["reason"].startswith("manual_reconcile:")


def test_manual_reconciliation_cannot_erase_fills(store):
    transport = FakeTransport()
    entry = record_entry_intent(store, cell_id=4)
    submit_via_protocol(store, transport, entry)
    leg = store.leg(entry.cid)
    with store.transaction() as tx:
        store.apply_history_batch(tx, [trade_row("t-1", entry.cid, leg.side, "1", price=str(leg.price),
                                                 exchange_order_id=f"9{entry.cid}")])
    with pytest.raises(InvalidTransitionError, match="has fills"):
        store.record_manual_reconciliation(None, "operator", "wrong", {"x": 1},
                                           leg_resolutions={entry.cid: OrderState.REJECTED_ZERO_FILL})
    assert store.leg(entry.cid).state == OrderState.LIVE and store.leg(entry.cid).filled == Decimal("1")
