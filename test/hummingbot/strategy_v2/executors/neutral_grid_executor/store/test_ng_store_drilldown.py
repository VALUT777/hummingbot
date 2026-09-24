"""Indexed read-only drill-down for the local UI (AC-22, NG-UI-002 "no silent truncation"): exact-string id lookups
and keyset pagination beyond the web's former 5000-row scan, on the command client and the read-only store."""
from dataclasses import fields, is_dataclass
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CommandKind,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import NeutralGridStore, OrderMatch
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    Env,
    FakeTransport,
    order_row,
    record_entry_intent,
    submit_via_protocol,
    trade_row,
)

BIG_ORDER_ID = str(2 ** 63 + 5)      # beyond int64 and far beyond the float/JS safe range
BIG_TRADE_ID = str(2 ** 64 + 1)
ROWS = 6000                          # more than the 5000 rows the web used to scan


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


@pytest.fixture
def store(env):
    opened = env.open()
    env.bootstrap(opened)
    return opened


def _handles(env):
    return [NeutralGridStore.open_command_client(env.db), NeutralGridStore.open_readonly(env.db)]


def _assert_no_float(value):
    assert not isinstance(value, float), value
    if is_dataclass(value):
        for f in fields(value):
            _assert_no_float(getattr(value, f.name))
    elif isinstance(value, dict):
        for k, v in value.items():
            _assert_no_float(k)
            _assert_no_float(v)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _assert_no_float(item)


def _big_id_order(store):
    transport = FakeTransport()
    transport.results.append(TransportResult(TransportOutcome.ACCEPTED, "tx", exchange_order_id=BIG_ORDER_ID))
    intent = record_entry_intent(store, cell_id=4)
    submit_via_protocol(store, transport, intent)
    leg = store.leg(intent.cid)
    with store.transaction() as tx:
        store.apply_history_batch(tx, [
            trade_row(BIG_TRADE_ID, leg.cid, leg.side, "2.5", price=str(leg.price), exchange_order_id=BIG_ORDER_ID),
            order_row(leg.cid, leg.side, leg.price, leg.amount, Decimal("2.5"), status="canceled",
                      order_id=BIG_ORDER_ID)])
    return leg


def test_find_orders_and_fills_by_exact_big_id_strings(env, store):
    leg = _big_id_order(store)
    near_misses = [str(int(float(BIG_ORDER_ID))), str(int(BIG_ORDER_ID) + 1), BIG_ORDER_ID + "0", " " + BIG_ORDER_ID]
    for handle in [store] + _handles(env):
        [by_exchange] = handle.find_orders_by_id(BIG_ORDER_ID)
        assert isinstance(by_exchange, OrderMatch) and by_exchange.leg.cid == leg.cid
        assert set(by_exchange.matched_on) == {"exchange_order_id", "order_index"}
        assert by_exchange.order.exchange_order_id == BIG_ORDER_ID  # exact string round trip
        [by_client] = handle.find_orders_by_id(str(leg.cid))
        assert by_client.matched_on == ("client_order_id",) and by_client.leg.cid == leg.cid
        [fill] = handle.find_fills_by_trade_id(BIG_TRADE_ID)
        assert fill.trade_id_str == BIG_TRADE_ID and fill.size == Decimal("2.5")
        for miss in near_misses[:3]:
            assert handle.find_orders_by_id(miss) == []
        assert handle.find_fills_by_trade_id(str(int(float(BIG_TRADE_ID)))) == []
        with pytest.raises(TypeError):
            handle.find_orders_by_id(near_misses[3])  # padded text is not an exact id
        with pytest.raises(TypeError):
            handle.find_orders_by_id(int(BIG_ORDER_ID))  # numbers are refused: ids are exact strings
        with pytest.raises(TypeError):
            handle.find_fills_by_trade_id(float(2 ** 53))
        _assert_no_float(handle.find_orders_by_id(BIG_ORDER_ID))
        _assert_no_float(handle.find_fills_by_trade_id(BIG_TRADE_ID))


def test_self_trade_returns_both_own_legs(store):
    transport = FakeTransport()
    buy, sell = record_entry_intent(store, cell_id=1), record_entry_intent(store, cell_id=50)
    submit_via_protocol(store, transport, buy)
    submit_via_protocol(store, transport, sell)
    rows = []
    for intent in (buy, sell):
        leg = store.leg(intent.cid)
        rows.append(trade_row("self-1", leg.cid, leg.side, "1", price=str(leg.price),
                              exchange_order_id=store.order(leg.cid).exchange_order_id))
    with store.transaction() as tx:
        store.apply_history_batch(tx, rows)
    assert {f.cid for f in store.find_fills_by_trade_id("self-1")} == {buy.cid, sell.cid}


def test_keyset_pages_reach_every_row_beyond_5000(env, store):
    engine = store.engine()
    with store.transaction() as tx:  # one transaction: 6000 commands + 6000 audit events
        for n in range(ROWS):
            store.enqueue_command(f"key-{n}", CommandKind.PAUSE, engine.config_revision, engine.engine_revision,
                                  tx=tx)
            store.record_audit(tx, "ui_test", "operator", {"n": n, "exchange_id": BIG_ORDER_ID})
    total_audit = store._conn.raw.execute("SELECT count(*) FROM audit_events").fetchone()[0]
    for handle in [store] + _handles(env):
        seen, before = [], None
        while True:
            page = handle.commands_page(before_id=before, limit=997)
            if not page:
                break
            assert [c.id for c in page] == sorted((c.id for c in page), reverse=True)
            seen.extend(c.id for c in page)
            before = page[-1].id
        assert seen == list(range(ROWS, 0, -1))  # newest first, nothing skipped or truncated, incl. id 1
        assert handle.commands_page(before_id=1, limit=10) == []
        audit, before = [], None
        while True:
            page = handle.audit_page(before_id=before, limit=1000)
            if not page:
                break
            audit.extend(page)
            before = page[-1].id
        assert len(audit) == total_audit > 5000 and len({e.id for e in audit}) == total_audit
        assert audit[-1].id == 1 and audit[-1].kind == "store_created"
        oldest_ui = [e for e in audit if e.kind == "ui_test"][-1]
        assert oldest_ui.payload == {"n": 0, "exchange_id": BIG_ORDER_ID}  # big id stays an exact string
        _assert_no_float(audit[:50])
        with pytest.raises(ValueError):
            handle.commands_page(limit=1001)
        with pytest.raises((TypeError, ValueError)):
            handle.audit_page(before_id=0, limit=10)


def test_drilldown_queries_use_indexes(store):
    plans = {
        "SELECT cid FROM orders WHERE exchange_order_id = ?": "orders_by_exchange_id",
        "SELECT cid FROM orders WHERE order_index = ?": "orders_by_order_index",
        "SELECT * FROM fills WHERE trade_id_str = ? ORDER BY own_side, dedupe_key": "fills_by_trade_id",
    }
    for sql, index in plans.items():
        plan = " ".join(str(row[-1]) for row in store._conn.raw.execute("EXPLAIN QUERY PLAN " + sql, ("x",)))
        assert index in plan, plan
    for table in ("commands", "audit_events"):
        plan = " ".join(str(row[-1]) for row in store._conn.raw.execute(
            f"EXPLAIN QUERY PLAN SELECT * FROM {table} WHERE id < ? ORDER BY id DESC LIMIT ?", (5, 5)))
        assert "INTEGER PRIMARY KEY" in plan and "TEMP B-TREE" not in plan, plan
