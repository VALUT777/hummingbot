"""Review #5 closed on the real WS-B store: drill-down and journal paging use indexed store reads, so an old id
beyond any scan window is found, pages reach the true end, and nothing is reported as truncated."""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import TransportOutcome, TransportResult
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import NeutralGridStore
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    Env,
    FakeTransport,
    order_row,
    record_entry_intent,
    submit_via_protocol,
    trade_row,
)
from web.neutral_grid import gateway as gateway_module
from web.neutral_grid.gateway import StoreGateway

BIG_ORDER_ID = str(2 ** 63 + 5)
BIG_TRADE_ID = str(2 ** 64 + 1)
JS_SAFE = (1 << 53) - 1


def _strict(text):
    def parse_int(s):
        assert abs(int(s)) <= JS_SAFE, s
        return int(s)
    return json.loads(text, parse_int=parse_int)


def _filled_order(store, cell_id, exchange_id, trade_id):
    transport = FakeTransport()
    transport.results.append(TransportResult(TransportOutcome.ACCEPTED, "tx", exchange_order_id=exchange_id))
    intent = record_entry_intent(store, cell_id=cell_id)
    submit_via_protocol(store, transport, intent)
    leg = store.leg(intent.cid)
    with store.transaction() as tx:
        store.apply_history_batch(tx, [
            trade_row(trade_id, leg.cid, leg.side, "2.5", price=str(leg.price), exchange_order_id=exchange_id),
            order_row(leg.cid, leg.side, leg.price, leg.amount, Decimal("2.5"), status="canceled",
                      order_id=exchange_id)])
    return leg


@pytest.fixture
def real_store(tmp_path, monkeypatch):
    monkeypatch.setattr(gateway_module, "_SCAN_LIMIT", 2, raising=False)  # the old window would miss old rows
    env = Env(tmp_path)
    writer = env.open()
    env.bootstrap(writer)
    oldest = _filled_order(writer, 4, BIG_ORDER_ID, BIG_TRADE_ID)
    for i, cell in enumerate((5, 6, 7)):  # newer legs push the oldest out of any small window
        _filled_order(writer, cell, str(10 ** 20 + i), str(10 ** 21 + i))
    for i in range(7):
        writer.record_audit(None, "operator_note", "test", {"n": i})
    gateway = StoreGateway(NeutralGridStore.open_command_client(env.db))
    yield writer, gateway, oldest
    gateway.close()
    writer.close()


@pytest.mark.asyncio
async def test_old_big_ids_found_through_indexed_store_reads(make_web, real_store):
    from ngweb_fakes import sample_snapshot

    writer, gateway, oldest = real_store
    gateway.latest_snapshot = lambda: sample_snapshot("NORMAL")
    web = await make_web(gateway=gateway)
    await web.login()
    by_exchange = _strict(await (await web.get(f"/api/lookup?id={BIG_ORDER_ID}")).text())
    assert by_exchange["truncated"] is False
    [match] = by_exchange["orders"]
    assert match["leg"]["cid"] == str(oldest.cid) and match["order"]["exchange_order_id"] == BIG_ORDER_ID
    assert "exchange_order_id" in match["matched_on"]
    by_trade = _strict(await (await web.get(f"/api/lookup?id={BIG_TRADE_ID}")).text())
    assert [t["trade_id_str"] for t in by_trade["trades"]] == [BIG_TRADE_ID] and by_trade["truncated"] is False
    by_cid = _strict(await (await web.get(f"/api/lookup?id={oldest.cid}")).text())
    assert [m["leg"]["cid"] for m in by_cid["orders"]] == [str(oldest.cid)]
    near_miss = _strict(await (await web.get(f"/api/lookup?id={int(float(BIG_ORDER_ID))}")).text())
    assert near_miss["orders"] == [] and near_miss["truncated"] is False


@pytest.mark.asyncio
async def test_journal_pages_reach_the_true_end(make_web, real_store):
    from ngweb_fakes import sample_snapshot

    writer, gateway, _ = real_store
    gateway.latest_snapshot = lambda: sample_snapshot("NORMAL")
    web = await make_web(gateway=gateway)
    await web.login()
    total = len(writer.audit_page(None, 1000))
    seen, cursor = [], None
    while True:
        page = _strict(await (await web.get("/api/audit?limit=2" + (f"&before={cursor}" if cursor else ""))).text())
        assert page["truncated"] is False
        seen.extend(e["id"] for e in page["events"])
        cursor = page["next_cursor"]
        if not cursor:
            break
    assert len(seen) == total == len(set(seen)) and seen == sorted(seen, key=int, reverse=True)
    for i in range(5):
        writer.enqueue_command(f"drill-cmd-{i:010d}", "pause", 0, 0, {})
    seen, cursor = [], None
    while True:
        page = _strict(await (await web.get("/api/commands?limit=2" + (f"&before={cursor}" if cursor else ""))).text())
        assert page["truncated"] is False
        seen.extend(c["id"] for c in page["commands"])
        cursor = page["next_cursor"]
        if not cursor:
            break
    assert len(seen) == 5 and seen == sorted(seen, key=int, reverse=True)


ROWS = 6000  # beyond the former 5000-row scan window


@pytest.mark.asyncio
@pytest.mark.timeout(240)
async def test_more_than_5000_rows_old_trade_and_oldest_journal_rows_reachable(make_web, tmp_path):
    """Orchestrator (d): > 5000 newer fills/commands/audit rows; the oldest ones are still found and paged to."""
    from ngweb_fakes import sample_snapshot

    from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CommandKind

    env = Env(tmp_path)
    writer = env.open()
    env.bootstrap(writer)
    _filled_order(writer, 4, BIG_ORDER_ID, BIG_TRADE_ID)          # the oldest trade
    newer = record_entry_intent(writer, cell_id=5)
    transport = FakeTransport()
    transport.results.append(TransportResult(TransportOutcome.ACCEPTED, "tx", exchange_order_id="77"))
    submit_via_protocol(writer, transport, newer)
    leg = writer.leg(newer.cid)
    engine = writer.engine()
    with writer.transaction() as tx:
        writer.apply_history_batch(tx, [trade_row(str(10 ** 22 + n), leg.cid, leg.side, "0.001", price=str(leg.price),
                                                  exchange_order_id="77") for n in range(ROWS)])
        for n in range(ROWS):
            writer.enqueue_command(f"bulk-key-{n:08d}", CommandKind.PAUSE, engine.config_revision,
                                   engine.engine_revision, tx=tx)
            writer.record_audit(tx, "ui_bulk", "operator", {"n": n})
    assert len(writer.fills()) > ROWS
    gateway = StoreGateway(NeutralGridStore.open_command_client(env.db))
    gateway.latest_snapshot = lambda: sample_snapshot("NORMAL")
    try:
        web = await make_web(gateway=gateway)
        await web.login()
        old = _strict(await (await web.get(f"/api/lookup?id={BIG_TRADE_ID}")).text())
        assert [t["trade_id_str"] for t in old["trades"]] == [BIG_TRADE_ID] and old["truncated"] is False
        by_order = _strict(await (await web.get(f"/api/lookup?id={BIG_ORDER_ID}")).text())
        assert len(by_order["orders"]) == 1 and by_order["truncated"] is False
        for path, key in (("/api/commands", "commands"), ("/api/audit", "events")):
            seen, cursor = [], None
            while True:
                page = _strict(await (await web.get(f"{path}?limit=200" + (f"&before={cursor}" if cursor else ""))
                                      ).text())
                assert page["truncated"] is False
                seen.extend(row["id"] for row in page[key])
                cursor = page["next_cursor"]
                if not cursor:
                    break
            assert len(seen) >= ROWS and len(set(seen)) == len(seen) and seen[-1] == "1", (path, len(seen))
    finally:
        gateway.close()
        writer.close()
