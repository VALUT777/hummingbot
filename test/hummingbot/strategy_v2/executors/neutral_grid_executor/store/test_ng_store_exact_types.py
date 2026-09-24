"""Exact storage of money and ids (AC-22 DB part, NG-DB-001): no REAL affinity anywhere, ids beyond 2**53 and
48-bit CIDs round-trip exactly through a reopened file, floats are refused, append-only facts are protected."""
import json
import sqlite3
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    MAX_CLIENT_ORDER_ID,
    EngineState,
    OrderState,
    OrderTypePolicy,
    Side,
    SubmitRequest,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    CID_EPOCH_SHIFT,
    PersistenceError,
    StoreIntegrityError,
    canonical_decimal,
    canonical_json,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    GRID_ID,
    Env,
    FakeTransport,
    entry_leg,
    order_row,
    record_entry_intent,
    submit_via_protocol,
    trade_row,
)

BIG_TRADE_ID = str(2 ** 63 + 12345)          # beyond int64 and far beyond float/JS safe range
BIG_ORDER_ID = str(2 ** 53 + 1)              # first integer a float/JS number cannot represent


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


def _columns(path):
    conn = sqlite3.connect(str(path))
    try:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' "
                                             "AND name NOT LIKE 'sqlite_%'")]
        result = {}
        for table in tables:
            strict = conn.execute("SELECT strict FROM pragma_table_list WHERE name = ?", (table,)).fetchone()[0]
            result[table] = (strict, [(c[1], c[2].upper()) for c in conn.execute(f"PRAGMA table_info({table})")])
        return result
    finally:
        conn.close()


def test_schema_has_no_real_or_numeric_affinity_and_all_tables_are_strict(env):
    env.open().close()
    columns = _columns(env.db)
    expected = {"engine", "config_revisions", "cells", "cycles", "legs", "orders", "fills", "history_inbox",
                "dedupe_keys", "cursors", "outbox", "commands", "audit_events", "snapshots", "cid_map"}
    assert expected <= set(columns)
    for table, (strict, cols) in columns.items():
        assert strict == 1, f"{table} must be STRICT"
        for name, declared in cols:
            assert declared in ("TEXT", "INTEGER"), f"{table}.{name} declared {declared}"


def test_ids_beyond_float_range_and_48bit_cid_round_trip_exactly(env):
    store = env.open()
    env.bootstrap(store)
    intent = record_entry_intent(store, cell_id=2)
    transport = FakeTransport()
    transport.results.append(TransportResult(TransportOutcome.ACCEPTED, "tx", exchange_order_id=BIG_ORDER_ID))
    submit_via_protocol(store, transport, intent)
    cell = store.cell(GRID_ID, 2)
    price = cell.spec().entry_price
    with store.transaction() as tx:
        result = store.apply_history_batch(tx, [
            trade_row(BIG_TRADE_ID, intent.cid, cell.entry_side, "2.5", price=str(price),
                      exchange_order_id=BIG_ORDER_ID),
            order_row(intent.cid, cell.entry_side, price, Decimal("10"), Decimal("2.5"), order_id=BIG_ORDER_ID,
                      status="canceled"),
        ])
    assert result.conflicts == [] and len(result.new_fills) == 1
    store.close()

    reopened = env.open()
    [fill] = reopened.fills()
    assert fill.trade_id_str == BIG_TRADE_ID and fill.own_exchange_order_id == BIG_ORDER_ID
    assert reopened.order(intent.cid).exchange_order_id == BIG_ORDER_ID
    assert fill.size == Decimal("2.5") and isinstance(fill.size, Decimal)
    assert reopened.leg(intent.cid).cid == intent.cid and intent.cid < 2 ** 48
    raw = sqlite3.connect(str(env.db))
    try:
        assert raw.execute("SELECT typeof(trade_id_str), trade_id_str FROM fills").fetchone() == ("text", BIG_TRADE_ID)
        assert raw.execute("SELECT typeof(size), size FROM fills").fetchone() == ("text", "2.5")
        assert raw.execute("SELECT typeof(cid) FROM cid_map").fetchone() == ("integer",)
        # the dedupe key keeps the id as a string
        key = raw.execute("SELECT dedupe_key FROM dedupe_keys WHERE stream = 'TRADES'").fetchone()[0]
        assert json.loads(key)[4] == BIG_TRADE_ID
    finally:
        raw.close()


def test_snapshot_serializes_big_ints_and_decimals_as_strings(env):
    store = env.open()
    env.bootstrap(store)
    with store.transaction() as tx:
        snap = store.write_snapshot(tx, {"engine_state": EngineState.RECONCILING,
                                         "summary": {"baseline": Decimal("-330.50"), "exchange_id": 2 ** 60,
                                                     "small": 5}})
    doc = json.loads(snap.snapshot_json)
    assert doc["summary"] == {"baseline": "-330.5", "exchange_id": str(2 ** 60), "small": 5}
    assert snap.payload["summary"]["baseline"] == "-330.5"


def test_canonical_decimal_is_exact_and_rejects_float():
    assert canonical_decimal(Decimal("10.000")) == "10"
    assert canonical_decimal(Decimal("1E+2")) == "100"
    assert canonical_decimal(Decimal("-0.00")) == "0"
    assert canonical_decimal(Decimal("0.00001230")) == "0.0000123"
    long = Decimal("123456789012345678901234567890.123456789")  # > 28 significant digits: never rounded
    assert canonical_decimal(long) == "123456789012345678901234567890.123456789"
    for bad in (1.5, True, Decimal("NaN"), Decimal("Infinity")):
        with pytest.raises((TypeError, ValueError)):
            canonical_decimal(bad)
    with pytest.raises(TypeError):
        canonical_json({"qty": 0.1})


def test_float_never_reaches_sqlite(env):
    store = env.open()
    env.bootstrap(store)
    with store.transaction() as tx:
        store.open_cycle(tx, GRID_ID, 1)
        cid = store.allocate_cid(tx, entry_leg(1))
    cell = store.cell(GRID_ID, 1)
    bad = SubmitRequest(client_order_id=cid, side=cell.entry_side, price=5.0, amount=Decimal("10"),
                        order_type=OrderTypePolicy.LIMIT_MAKER)
    with pytest.raises(TypeError):
        with store.transaction() as tx:
            store.record_intent(tx, store.identity_for_cid(cid), bad)
    with pytest.raises(TypeError):
        store.kv_set(None, "x", {"v": 1.25})
    assert store.leg(cid) is None


def test_cids_are_epoch_prefixed_and_below_48_bits(env):
    store = env.open()
    env.bootstrap(store)
    intents = [record_entry_intent(store, cell_id=i) for i in (0, 1, 2)]
    cids = [i.cid for i in intents]
    assert cids == sorted(cids) and len(set(cids)) == 3
    assert all(0 < cid <= MAX_CLIENT_ORDER_ID and cid >> CID_EPOCH_SHIFT == 1 for cid in cids)


@pytest.mark.parametrize("sql", [
    "DELETE FROM cid_map",
    "UPDATE cid_map SET cell_id = 9",
    "DELETE FROM fills",
    "UPDATE dedupe_keys SET payload_hash = 'x'",
    "DELETE FROM audit_events",
    "UPDATE engine SET initial_baseline = '5'",
    "UPDATE grids SET order_amount_base = '11'",
    "UPDATE cells SET low_price = '5.1'",
    "DELETE FROM cycles",
    "UPDATE legs SET price = '1'",
    "DELETE FROM outbox",
])
def test_append_only_and_immutable_facts_are_enforced_by_sqlite(env, sql):
    store = env.open()
    env.bootstrap(store)
    intent = record_entry_intent(store, cell_id=4)
    submit_via_protocol(store, FakeTransport(), intent)
    cell = store.cell(GRID_ID, 4)
    with store.transaction() as tx:
        store.apply_history_batch(tx, [trade_row("t-1", intent.cid, cell.entry_side, "1",
                                                 price=str(cell.spec().entry_price))])
    store.close()
    raw = sqlite3.connect(str(env.db))
    try:
        with pytest.raises(sqlite3.IntegrityError):
            raw.execute(sql)
    finally:
        raw.close()


def test_trigger_violation_through_store_is_a_persistence_error_and_rolls_back(env):
    store = env.open()
    env.bootstrap(store)
    before = store.engine()
    with pytest.raises(StoreIntegrityError):
        with store.transaction() as tx:
            store.set_engine_state(tx, EngineState.PAUSED, "x")
            store._x("UPDATE engine SET initial_baseline = '99' WHERE id = 1")
    assert isinstance(StoreIntegrityError("x"), PersistenceError)
    after = store.engine()
    assert after.engine_state == before.engine_state and after.initial_baseline == Decimal("0")
    assert store.degraded_reason is not None


def test_leg_states_round_trip(env):
    store = env.open()
    env.bootstrap(store)
    intent = record_entry_intent(store, cell_id=5)
    submit_via_protocol(store, FakeTransport(), intent)
    store.close()
    leg = env.open().leg(intent.cid)
    assert leg.state == OrderState.LIVE and leg.side in (Side.BUY, Side.SELL) and leg.amount == Decimal("10")
