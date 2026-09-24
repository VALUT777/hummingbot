"""The fake exchange itself must behave like the venue contract the engine relies on."""
import asyncio
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    OrderTypePolicy,
    Side,
    SubmitRequest,
    TransportOutcome,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import (
    CANCELED_POST_ONLY,
    CancelBehavior,
    FakeClock,
    FakeDataUnavailable,
    FakeExchange,
    FakeTransportError,
    SubmitBehavior,
)

D = Decimal


def run(coro):
    return asyncio.run(coro)


def req(cid, side=Side.BUY, price="5.2", amount="10", order_type=OrderTypePolicy.LIMIT_MAKER, expiry_ms=None):
    return SubmitRequest(client_order_id=cid, side=side, price=D(price), amount=D(amount), order_type=order_type,
                         expiry_ms=expiry_ms)


async def _all_pages(fx, method):
    rows, cursor, pages = [], None, 0
    while True:
        page = await method(cursor)
        pages += 1
        rows.extend(page.rows)
        if page.next_cursor is None:
            return rows, pages
        cursor = page.next_cursor


def test_cursor_pagination_over_100_rows_newest_first():
    fx = FakeExchange(FakeClock())
    for i in range(1, 251):
        run(fx.submit(req(i, price="5.0001", amount="5")))
        fx.clock.advance(0.01)
    for i in range(1, 251):
        fx.fill(i, D("5"))
        fx.clock.advance(0.01)
    trades, pages = run(_all_pages(fx, fx.trades_page))
    assert pages == 3
    assert len(trades) == 250
    assert len({t.trade_id_str for t in trades}) == 250
    ts = [t.timestamp_ms for t in trades]
    assert ts == sorted(ts, reverse=True)
    orders, pages = run(_all_pages(fx, fx.inactive_orders_page))
    assert pages == 3 and len(orders) == 250
    # exchange ids are exact strings above the float-safe range
    assert all(int(o.order_index) > 2 ** 53 for o in orders)
    assert all(int(t.trade_id_str) > 2 ** 60 for t in trades)


def test_history_lag_hides_rows_but_ws_signals_immediately():
    fx = FakeExchange(FakeClock(), history_lag_s=3.0)
    events = []
    fx.add_ws_listener(events.append)
    run(fx.submit(req(7)))
    fx.fill(7, D("4"))
    assert any(e["type"] == "trade" for e in events)
    assert run(fx.trades_page(None)).rows == []
    assert fx.net_position == D("4")
    fx.clock.advance(3.0)
    rows = run(fx.trades_page(None)).rows
    assert [r.size for r in rows] == [D("4")]


def test_duplicate_boundary_and_page_faults():
    fx = FakeExchange(FakeClock())
    for i in range(1, 121):
        run(fx.submit(req(i, amount="5")))
        fx.fill(i, D("5"))
        fx.clock.advance(0.01)
    fx.duplicate_boundary["trades"] = True
    first = run(fx.trades_page(None))
    second = run(fx.trades_page(first.next_cursor))
    assert second.rows[0].raw_json == first.rows[-1].raw_json
    fx.inject_page_fault("trades", "repeat_cursor", page=2)
    page2 = run(fx.trades_page(first.next_cursor))
    assert page2.next_cursor == first.next_cursor
    fx.inject_page_fault("trades", "malformed_cursor", page=1)
    bad = run(fx.trades_page(None))
    with pytest.raises(FakeTransportError):
        run(fx.trades_page(bad.next_cursor))
    fx.inject_page_fault("inactive_orders", "page_error", page=1)
    with pytest.raises(FakeDataUnavailable):
        run(fx.inactive_orders_page(None))


def test_submit_behaviors_and_definitive_reject():
    fx = FakeExchange(FakeClock())
    fx.script_submit(SubmitBehavior.TIMEOUT_LANDED)
    res = run(fx.submit(req(1)))
    assert res.outcome == TransportOutcome.UNKNOWN and fx.order_by_cid(1) is not None
    fx.script_submit(SubmitBehavior.TIMEOUT_NOT_LANDED)
    res = run(fx.submit(req(2)))
    assert res.outcome == TransportOutcome.UNKNOWN and fx.order_by_cid(2) is None
    fx.script_submit(SubmitBehavior.REJECT_ZERO_FILL)
    res = run(fx.submit(req(3)))
    assert res.outcome == TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL and fx.order_by_cid(3) is None
    # post-only crossing is an accepted tx that the venue cancels with zero fill
    res = run(fx.submit(req(4, side=Side.BUY, price="5.5")))
    assert res.outcome == TransportOutcome.ACCEPTED
    assert fx.order_by_cid(4).status == CANCELED_POST_ONLY and fx.order_by_cid(4).filled == 0
    fx.set_rules(min_base=D("20"))
    res = run(fx.submit(req(5)))
    assert res.outcome == TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL
    assert any("below min base" in v for v in fx.violations)


def test_cancel_behaviors():
    fx = FakeExchange(FakeClock())
    for cid in (1, 2, 3):
        run(fx.submit(req(cid)))
    fx.script_cancel(CancelBehavior.TIMEOUT_NOT_LANDED)
    assert run(fx.cancel(1, None)).outcome == TransportOutcome.UNKNOWN
    assert fx.order_by_cid(1).is_open
    fx.script_cancel(CancelBehavior.TIMEOUT_LANDED)
    assert run(fx.cancel(2, None)).outcome == TransportOutcome.UNKNOWN
    assert not fx.order_by_cid(2).is_open
    fx.script_cancel(CancelBehavior.NOT_FOUND)
    assert run(fx.cancel(3, None)).outcome == TransportOutcome.UNKNOWN
    assert fx.order_by_cid(3).is_open


def test_self_trade_produces_two_own_legs_with_one_trade_id():
    fx = FakeExchange(FakeClock())
    run(fx.submit(req(10, side=Side.SELL, price="5.3")))
    run(fx.submit(req(11, side=Side.BUY, price="5.3", order_type=OrderTypePolicy.LIMIT)))
    before = fx.net_position
    trade_id = fx.self_trade(10, 11, D("4"))
    rows = run(fx.trades_page(None)).rows
    assert len(rows) == 2 and {r.own_side for r in rows} == {Side.BUY, Side.SELL}
    assert {r.trade_id_str for r in rows} == {trade_id}
    assert len({r.dedupe_key(fx.domain) for r in rows}) == 2
    assert fx.net_position == before


def test_manual_order_and_trade_are_unowned_and_move_position():
    fx = FakeExchange(FakeClock())
    manual = fx.place_manual_order(Side.BUY, D("5.1"), D("5"))
    active = run(fx.active_orders())
    assert [a.client_order_id for a in active] == [manual.client_order_id]
    fx.manual_trade(Side.SELL, D("7"), D("5.4"))
    assert fx.net_position == D("-7")


def test_retention_window_and_weights():
    fx = FakeExchange(FakeClock(), retention_s=60)
    run(fx.submit(req(1)))
    fx.fill(1, D("10"))
    fx.clock.advance(61)
    assert run(fx.trades_page(None)).rows == []
    assert fx.request_weight("trades") == 600 and fx.request_weight("inactive_orders") == 100
    assert fx.request_weight("position") == 300
    assert fx.weight_used(60) >= 600


def test_gtt_expiry_is_venue_terminal():
    fx = FakeExchange(FakeClock())
    expiry = int(fx.clock.now() * 1000) + 5000
    run(fx.submit(req(9, side=Side.SELL, price="5.6", order_type=OrderTypePolicy.LIMIT, expiry_ms=expiry)))
    fx.fill(9, D("3"))
    fx.clock.advance(6)
    rows = run(fx.inactive_orders_page(None)).rows
    assert rows[0].status == "canceled-expired" and rows[0].filled_base_amount == D("3")
