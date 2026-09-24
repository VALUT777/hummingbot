"""Exact client/exchange order identity ownership across every durable binding path."""
import asyncio
import sqlite3
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    HistoryPage,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.history import (
    HistoryScanner,
    InMemoryHistoryCursorView,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.migrations import LATEST_VERSION, MIGRATIONS
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    InvalidTransitionError,
    StoreIntegrityError,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    DOMAIN,
    Env,
    FakeTransport,
    order_row,
    record_entry_intent,
    submit_via_protocol,
    trade_row,
)


EXCHANGE_ID = str(2**53 + 901)


class _OneTradePort:
    domain = DOMAIN
    account_index = 7
    market_id = 5

    def __init__(self, row):
        self.row = row

    def request_weight(self, endpoint):
        return 600 if endpoint == "trades" else 100

    async def trades_page(self, cursor, limit=100):
        return HistoryPage([self.row], None, cursor)

    async def inactive_orders_page(self, cursor, limit=100):
        return HistoryPage([], None, cursor)


def _two_orders(store):
    first = record_entry_intent(store, 1)
    second = record_entry_intent(store, 2)
    no_exchange_id = FakeTransport()
    no_exchange_id.results.append(TransportResult(TransportOutcome.ACCEPTED, "accepted-no-order-id"))
    with_exchange_id = FakeTransport()
    with_exchange_id.results.append(
        TransportResult(TransportOutcome.ACCEPTED, "accepted", exchange_order_id=EXCHANGE_ID)
    )
    submit_via_protocol(store, no_exchange_id, first)
    submit_via_protocol(store, with_exchange_id, second)
    return first, second


def test_scanned_trade_with_contradictory_client_and_exchange_ids_is_durable_conflict(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    first, second = _two_orders(store)
    first_leg = store.leg(first.cid)
    row = trade_row(
        str(2**53 + 1007),
        first.cid,
        first_leg.side,
        "1",
        price=str(first_leg.price),
        exchange_order_id=EXCHANGE_ID,
    )

    scanner = HistoryScanner(
        _OneTradePort(row),
        InMemoryHistoryCursorView(domain=DOMAIN),
        weight_budget=700,
    )
    scan = asyncio.run(scanner.scan())
    assert scan.complete and scan.conflicts == [] and scan.new_trades == [row]

    with store.transaction() as tx:
        result = store.apply_history_batch(tx, scan.new_trades)

    assert [conflict.kind for conflict in result.conflicts] == [
        "ID_OWNERSHIP_MISMATCH", "ID_OWNERSHIP_MISMATCH"
    ]
    assert {conflict.cid for conflict in result.conflicts} == {first.cid, second.cid}
    assert result.new_fills == []
    assert store.leg(first.cid).filled == Decimal("0")
    assert store.leg(second.cid).filled == Decimal("0")
    assert store.order(first.cid).exchange_order_id is None
    assert store.order(second.cid).exchange_order_id == EXCHANGE_ID
    store.close()

    reopened = env.open()
    assert [conflict.kind for conflict in reopened.open_conflicts()] == [
        "ID_OWNERSHIP_MISMATCH", "ID_OWNERSHIP_MISMATCH"
    ]
    assert {conflict.cid for conflict in reopened.open_conflicts()} == {first.cid, second.cid}
    assert reopened.leg(first.cid).filled == Decimal("0")
    assert reopened.order(first.cid).exchange_order_id is None
    assert reopened.order(second.cid).exchange_order_id == EXCHANGE_ID


def test_inactive_order_row_with_contradictory_ids_is_not_applied_or_bound(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    first, second = _two_orders(store)
    leg = store.leg(first.cid)
    row = order_row(
        first.cid,
        leg.side,
        leg.price,
        leg.amount,
        Decimal("1"),
        status="canceled",
        order_id=EXCHANGE_ID,
    )

    with store.transaction() as tx:
        result = store.apply_history_batch(tx, [row])

    assert [conflict.kind for conflict in result.conflicts] == [
        "ID_OWNERSHIP_MISMATCH", "ID_OWNERSHIP_MISMATCH"
    ]
    assert {conflict.cid for conflict in result.conflicts} == {first.cid, second.cid}
    assert result.applied == []
    assert store.order(first.cid).exchange_order_id is None
    assert store.order(first.cid).venue_status is None
    assert store.order(second.cid).exchange_order_id == EXCHANGE_ID


def test_active_order_evidence_cannot_take_exchange_id_owned_by_another_cid(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    first, second = _two_orders(store)
    leg = store.leg(first.cid)
    row = order_row(
        first.cid,
        leg.side,
        leg.price,
        leg.amount,
        Decimal("0"),
        status="open",
        order_id=EXCHANGE_ID,
    )

    with pytest.raises(InvalidTransitionError, match="owned by CID"):
        with store.transaction() as tx:
            store.record_order_evidence(tx, row)

    assert store.order(first.cid).exchange_order_id is None
    assert store.order(first.cid).last_evidence_ms is None
    assert store.order(second.cid).exchange_order_id == EXCHANGE_ID


def test_transport_ack_cannot_take_exchange_id_owned_by_another_cid_and_survives_reopen(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    first = record_entry_intent(store, 1)
    second = record_entry_intent(store, 2)
    second_transport = FakeTransport()
    second_transport.results.append(
        TransportResult(TransportOutcome.ACCEPTED, "accepted", exchange_order_id=EXCHANGE_ID)
    )
    submit_via_protocol(store, second_transport, second)
    with store.transaction() as tx:
        store.mark_dispatching(tx, first.outbox_id)

    with pytest.raises(InvalidTransitionError, match="owned by CID"):
        with store.transaction() as tx:
            store.record_transport_result(
                tx,
                first.cid,
                TransportResult(TransportOutcome.ACCEPTED, "accepted", exchange_order_id=EXCHANGE_ID),
            )

    assert store.order(first.cid).exchange_order_id is None
    assert store.outbox_entry(first.outbox_id).status == "DISPATCHED"
    assert store.order(second.cid).exchange_order_id == EXCHANGE_ID
    store.close()

    reopened = env.open()
    assert reopened.order(first.cid).exchange_order_id is None
    assert reopened.outbox_entry(first.outbox_id).status == "DISPATCHED"
    assert reopened.order(second.cid).exchange_order_id == EXCHANGE_ID


def test_replayed_transport_ack_still_validates_exchange_id(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    env.bootstrap(store)
    intent = record_entry_intent(store, 1)
    transport = FakeTransport()
    transport.results.append(
        TransportResult(TransportOutcome.ACCEPTED, "accepted", exchange_order_id=EXCHANGE_ID)
    )
    submit_via_protocol(store, transport, intent)

    with pytest.raises(InvalidTransitionError, match="differs from recorded"):
        with store.transaction() as tx:
            store.record_transport_result(
                tx,
                intent.cid,
                TransportResult(TransportOutcome.ACCEPTED, "accepted", exchange_order_id=str(2**53 + 902)),
            )

    assert store.order(intent.cid).exchange_order_id == EXCHANGE_ID


def test_legacy_duplicate_exchange_ids_are_diagnosed_and_migration_fails_closed(tmp_path):
    env = Env(tmp_path)
    legacy_migrations = MIGRATIONS[:-1]
    store = env.open(migrations=legacy_migrations)
    env.bootstrap(store)
    first = record_entry_intent(store, 1)
    second = record_entry_intent(store, 2)
    with store.transaction():
        store._x("UPDATE orders SET exchange_order_id = ? WHERE cid IN (?, ?)",
                 (EXCHANGE_ID, first.cid, second.cid))

    assert any(EXCHANGE_ID in problem and "multiple CIDs" in problem for problem in store.verify_ledger())
    store.close()

    with pytest.raises(StoreIntegrityError, match="exchange_order_id"):
        env.open()

    raw = sqlite3.connect(env.db)
    try:
        assert raw.execute("PRAGMA user_version").fetchone()[0] == LATEST_VERSION - 1
        assert raw.execute(
            "SELECT count(*) FROM orders WHERE exchange_order_id = ?", (EXCHANGE_ID,)
        ).fetchone()[0] == 2
    finally:
        raw.close()
