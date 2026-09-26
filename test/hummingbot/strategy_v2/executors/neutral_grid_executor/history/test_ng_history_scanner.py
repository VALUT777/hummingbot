"""HistoryScanner / settlement tests (AC-09, AC-10, AC-11, AC-12, AC-40, AC-41, AC-42 scan, AC-53 scan)."""
import asyncio
import unittest
from dataclasses import replace
from decimal import Decimal
from typing import Callable, Dict, List, Optional, Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    ExchangeOrderRow,
    ExchangeTradeRow,
    HistoryPage,
    Side,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.history import (
    REASON_BACKOFF,
    REASON_CONFLICT,
    REASON_EMPTY_PAGE_WITH_CURSOR,
    REASON_IN_PROGRESS,
    REASON_MALFORMED_CURSOR,
    REASON_ORDERING_VIOLATION,
    REASON_OVERSIZED_PAGE,
    REASON_PAGE_FETCH_ERROR,
    REASON_REPEATED_CURSOR,
    REASON_RETENTION_GAP,
    REASON_SCHEMA_ERROR,
    STREAM_ORDERS,
    STREAM_TRADES,
    HighWaterMark,
    HistorySchemaError,
    HistoryScanner,
    InMemoryHistoryCursorView,
    ScanRecord,
    attribute_trades_to_order,
    evaluate_terminal_release,
    order_dedupe_key,
)

DOMAIN = "lighter_perpetual_robinhood"
ACCOUNT = 724450
MARKET = 5
BASE_TS = 1_790_000_000_000  # ms
BIG = 1 << 53  # ids above the float-safe range (AC-22)


def trade(i: int, ts_ms: int, size: str = "1", side: Side = Side.BUY, order_id: Optional[str] = None,
          cid: Optional[int] = None, price: str = "5.4") -> ExchangeTradeRow:
    return ExchangeTradeRow(
        trade_id_str=str(BIG + i * 10), account_index=ACCOUNT, market_id=MARKET, own_side=side,
        own_exchange_order_id=order_id if order_id is not None else str(BIG + 500_000 + i),
        own_client_order_id=cid, size=Decimal(size), price=Decimal(price), is_maker=True,
        timestamp_ms=ts_ms, raw_json="{}",
    )


def order(i: int, ts_ms: int, filled: str = "0", status: str = "canceled", cid: Optional[int] = None,
          initial: str = "10", side: Side = Side.BUY) -> ExchangeOrderRow:
    cid = cid if cid is not None else 1000 + i
    exchange_id = str(BIG + 100_000 + i)
    return ExchangeOrderRow(
        client_order_id=cid, client_order_id_str=str(cid), order_id=exchange_id, order_index=exchange_id,
        nonce=str(i), account_index=ACCOUNT, market_id=MARKET, side=side, price=Decimal("5.4"),
        initial_base_amount=Decimal(initial), filled_base_amount=Decimal(filled),
        remaining_base_amount=Decimal(initial) - Decimal(filled), status=status, reduce_only=False,
        timestamp_ms=ts_ms, raw_json="{}",
    )


class FakeClock:
    def __init__(self, now: float = BASE_TS / 1000 + 10_000):
        self.now = now

    def __call__(self) -> float:
        return self.now


class FakeHistoryPort:
    """Deterministic newest->oldest cursor pagination over in-memory rows, with fault hooks."""

    domain = DOMAIN
    account_index = ACCOUNT
    market_id = MARKET

    def __init__(self, trades: List[ExchangeTradeRow] = (), orders: List[ExchangeOrderRow] = ()):
        self.rows: Dict[str, List[object]] = {STREAM_TRADES: list(trades), STREAM_ORDERS: list(orders)}
        self.calls: List[Tuple[str, Optional[str], int]] = []
        # (stream, page_no) -> callable(HistoryPage) -> HistoryPage | raises
        self.faults: Dict[Tuple[str, int], Callable[[HistoryPage], HistoryPage]] = {}
        self.gate: Optional[asyncio.Event] = None

    def request_weight(self, endpoint: str) -> int:
        return {"trades": 600, "inactive_orders": 100}.get(endpoint, 300)

    async def _page(self, stream: str, cursor: Optional[str], limit: int) -> HistoryPage:
        self.calls.append((stream, cursor, limit))
        if self.gate is not None:
            await self.gate.wait()
        offset = 0 if cursor is None else int(cursor.split(":")[-1])
        rows = self.rows[stream][offset:offset + limit]
        end = offset + limit
        next_cursor = f"opaque/{stream}+=:{end}" if end < len(self.rows[stream]) else None
        page = HistoryPage(rows=list(rows), next_cursor=next_cursor, raw_cursor_sent=cursor)
        page_no = sum(1 for call in self.calls if call[0] == stream) - 1
        fault = self.faults.get((stream, page_no))
        return fault(page) if fault is not None else page

    async def trades_page(self, cursor: Optional[str], limit: int = 100) -> HistoryPage:
        return await self._page(STREAM_TRADES, cursor, limit)

    async def inactive_orders_page(self, cursor: Optional[str], limit: int = 100) -> HistoryPage:
        return await self._page(STREAM_ORDERS, cursor, limit)

    def pages_requested(self, stream: str) -> int:
        return sum(1 for call in self.calls if call[0] == stream)


def newest_first_trades(count: int, start_ts: int = BASE_TS, step_ms: int = 1000, **kwargs) -> List[ExchangeTradeRow]:
    return [trade(i, start_ts + i * step_ms, **kwargs) for i in reversed(range(count))]


def newest_first_orders(count: int, start_ts: int = BASE_TS, step_ms: int = 1000, **kwargs) -> List[ExchangeOrderRow]:
    return [order(i, start_ts + i * step_ms, **kwargs) for i in reversed(range(count))]


async def scan_until_done(scanner: HistoryScanner, max_calls: int = 50):
    results = []
    for _ in range(max_calls):
        result = await scanner.scan()
        results.append(result)
        if result.incomplete_reason != REASON_IN_PROGRESS:
            return results
    raise AssertionError("scan did not finish")


class HistoryScannerTest(unittest.IsolatedAsyncioTestCase):

    def make(self, port: FakeHistoryPort, view: Optional[InMemoryHistoryCursorView] = None, **kwargs):
        view = view or InMemoryHistoryCursorView(DOMAIN)
        clock = kwargs.pop("clock", FakeClock())
        scanner = HistoryScanner(port, view, kwargs.pop("overlap_s", Decimal("60")),
                                 kwargs.pop("weight_budget", 100_000), clock=clock, **kwargs)
        return scanner, view, clock

    # ---------------------------------------------------------------- AC-10
    async def test_ac10_target_order_on_later_page_first_page_not_complete(self):
        orders = newest_first_orders(250)
        target_cid = orders[180].client_order_id  # third page (rows 100..199 are page 2 -> index 180)
        port = FakeHistoryPort(orders=orders)
        scanner, view, _ = self.make(port, max_pages_per_call=1)

        first = await scanner.scan()
        self.assertFalse(first.complete)
        self.assertEqual(REASON_IN_PROGRESS, first.incomplete_reason)
        self.assertIsNone(first.orders_high_water)
        self.assertNotIn(target_cid, {row.client_order_id for row in first.new_orders})

        results = [first] + await scan_until_done(scanner)
        final = results[-1]
        self.assertTrue(final.complete, final.incomplete_reason)
        self.assertIn(target_cid, {row.client_order_id for row in final.new_orders})
        self.assertEqual(250, len(final.new_orders))
        self.assertEqual(3, port.pages_requested(STREAM_ORDERS))
        # the cursor was passed back verbatim
        self.assertEqual([None, f"opaque/{STREAM_ORDERS}+=:100", f"opaque/{STREAM_ORDERS}+=:200"],
                         [c for s, c, _ in port.calls if s == STREAM_ORDERS])
        self.assertTrue(all(limit == 100 for _, _, limit in port.calls))
        self.assertIsNotNone(final.orders_high_water)

    # ---------------------------------------------------------------- AC-11
    async def test_ac11_more_than_100_trades_boundary_duplicate_deduped_cumulative_exact(self):
        trades = newest_first_trades(230, size="0.37")
        boundary_duplicate = trades[99]
        port = FakeHistoryPort(trades=trades)

        def duplicate_boundary(page: HistoryPage) -> HistoryPage:
            return replace(page, rows=[boundary_duplicate] + page.rows)

        port.faults[(STREAM_TRADES, 1)] = duplicate_boundary
        scanner, view, _ = self.make(port)
        result = (await scan_until_done(scanner))[-1]

        self.assertTrue(result.complete, result.incomplete_reason)
        self.assertEqual(3, port.pages_requested(STREAM_TRADES))
        self.assertEqual(230, len(result.new_trades))
        self.assertEqual(230, len({row.trade_id_str for row in result.new_trades}))
        self.assertEqual(Decimal("0.37") * 230, sum((row.size for row in result.new_trades), Decimal("0")))
        view.commit(result)

        # A repeat scan re-reads the overlap, finds only already committed rows and adds nothing.
        port.calls.clear()
        again = (await scan_until_done(scanner))[-1]
        self.assertTrue(again.complete, again.incomplete_reason)
        self.assertEqual([], again.new_trades)
        view.commit(again)
        self.assertEqual(230, len(view.trades))
        self.assertEqual(Decimal("85.10"), sum((row.size for row in view.trades), Decimal("0")))

    # ---------------------------------------------------------------- AC-12
    async def test_ac12_bad_pagination_makes_history_incomplete(self):
        def repeated(page):
            return replace(page, next_cursor=page.raw_cursor_sent or f"opaque/{STREAM_TRADES}+=:100")

        def repeated_first(page):
            return replace(page, next_cursor=f"opaque/{STREAM_TRADES}+=:100")

        def malformed_type(page):
            return replace(page, next_cursor=12345)

        def malformed_space(page):
            return replace(page, next_cursor="abc def")

        def conflicting(page):
            bad = replace(page.rows[0], size=page.rows[0].size + 1)
            return replace(page, rows=page.rows[:1] + [bad] + page.rows[1:])

        def missing_page(page):
            raise ConnectionError("HTTP 502")

        def schema(page):
            raise HistorySchemaError("bad trade row")

        def empty_with_cursor(page):
            return replace(page, rows=[])

        def oversized(page):
            return replace(page, rows=page.rows * 3)

        def reordered(page):
            return replace(page, rows=list(reversed(page.rows)))

        def wrong_echo(page):
            return replace(page, raw_cursor_sent="something-else")

        cases = {
            "repeated_cursor": ({(STREAM_TRADES, 1): repeated_first}, REASON_REPEATED_CURSOR),
            "self_repeating_cursor": ({(STREAM_TRADES, 0): repeated, (STREAM_TRADES, 1): repeated},
                                      REASON_REPEATED_CURSOR),
            "malformed_cursor_type": ({(STREAM_TRADES, 0): malformed_type}, REASON_MALFORMED_CURSOR),
            "malformed_cursor_text": ({(STREAM_TRADES, 0): malformed_space}, REASON_MALFORMED_CURSOR),
            "conflicting_duplicate": ({(STREAM_TRADES, 0): conflicting}, REASON_CONFLICT),
            "missing_required_page": ({(STREAM_TRADES, 1): missing_page}, REASON_PAGE_FETCH_ERROR),
            "schema_error": ({(STREAM_TRADES, 1): schema}, REASON_SCHEMA_ERROR),
            "empty_page_with_cursor": ({(STREAM_TRADES, 0): empty_with_cursor}, REASON_EMPTY_PAGE_WITH_CURSOR),
            "oversized_page": ({(STREAM_TRADES, 0): oversized}, REASON_OVERSIZED_PAGE),
            "reordered_page": ({(STREAM_TRADES, 0): reordered}, REASON_ORDERING_VIOLATION),
            "cursor_echo_mismatch": ({(STREAM_TRADES, 1): wrong_echo}, REASON_SCHEMA_ERROR),
        }
        for name, (faults, reason) in cases.items():
            with self.subTest(name):
                port = FakeHistoryPort(trades=newest_first_trades(250), orders=newest_first_orders(3))
                port.faults.update(faults)
                scanner, _, _ = self.make(port)
                result = (await scan_until_done(scanner))[-1]
                self.assertFalse(result.complete)
                self.assertTrue(result.incomplete_reason.startswith(reason), result.incomplete_reason)
                self.assertIsNone(result.trades_high_water)
                self.assertIsNone(result.orders_high_water)
                if name == "conflicting_duplicate":
                    conflicted = port.rows[STREAM_TRADES][0].dedupe_key(DOMAIN)
                    self.assertNotIn(conflicted, {row.dedupe_key(DOMAIN) for row in result.new_trades})
                    self.assertEqual(249, len(result.new_trades))

    async def test_ac12_break_before_boundary_is_incomplete_and_resumes(self):
        port = FakeHistoryPort(trades=newest_first_trades(250), orders=newest_first_orders(250))
        scanner, _, _ = self.make(port, weight_budget=700)  # one trades page + one orders page per call
        first = await scanner.scan()
        self.assertFalse(first.complete)
        self.assertEqual(REASON_IN_PROGRESS, first.incomplete_reason)
        self.assertLessEqual(first.weight_used, 700)
        self.assertEqual({STREAM_TRADES: 1, STREAM_ORDERS: 1}, first.pages_read)
        self.assertTrue(scanner.resumable_work_pending)
        self.assertTrue(scanner.should_scan())
        final = (await scan_until_done(scanner))[-1]
        self.assertTrue(final.complete, final.incomplete_reason)
        self.assertEqual(250, len(final.new_trades))
        self.assertEqual(3, port.pages_requested(STREAM_TRADES))

    # ---------------------------------------------------------------- AC-40
    async def test_ac40_trades_exceeding_terminal_order_cumulative_is_conflict(self):
        filled_order = order(1, BASE_TS + 5000, filled="3", status="canceled")
        exchange_id = filled_order.order_index
        trades = [
            trade(2, BASE_TS + 4000, size="2", order_id=exchange_id),
            trade(1, BASE_TS + 3000, size="2", order_id=exchange_id),
        ]
        port = FakeHistoryPort(trades=trades, orders=[filled_order])
        scanner, _, _ = self.make(port)
        result = (await scan_until_done(scanner))[-1]
        self.assertFalse(result.complete)
        self.assertEqual(REASON_CONFLICT, result.incomplete_reason)
        self.assertTrue(any("trades_exceed_order_cumulative" in c for c in result.conflicts), result.conflicts)
        # the contradicted order and every execution attributed to it are withheld (audit only)
        self.assertEqual([], result.new_orders)
        self.assertEqual([], result.new_trades)
        self.assertEqual({STREAM_ORDERS: 1, STREAM_TRADES: 2},
                         {k: len(v) for k, v in scanner.last_conflicted_rows.items()})

    async def test_ac40_same_key_different_payload_across_scans_is_conflict(self):
        trades = newest_first_trades(5)
        port = FakeHistoryPort(trades=trades)
        scanner, view, _ = self.make(port)
        first = (await scan_until_done(scanner))[-1]
        self.assertTrue(first.complete)
        view.commit(first)
        port.rows[STREAM_TRADES][2] = replace(trades[2], size=Decimal("9"))
        second = (await scan_until_done(scanner))[-1]
        self.assertFalse(second.complete)
        self.assertTrue(any("committed_payload_mismatch" in c for c in second.conflicts), second.conflicts)

    # ---------------------------------------------------------------- AC-41
    async def test_ac41_found_or_duplicate_ids_do_not_stop_scan_before_verified_boundary(self):
        # Earlier complete scan committed 301 trades; high-water = newest at BASE_TS + 300 s.
        old = newest_first_trades(301)
        port = FakeHistoryPort(trades=old, orders=newest_first_orders(301))
        scanner, view, _ = self.make(port, overlap_s=Decimal("150"))
        view.commit((await scan_until_done(scanner))[-1])
        high_water = HighWaterMark.decode(view.high_water(STREAM_TRADES))
        self.assertEqual(BASE_TS + 300_000, high_water.timestamp_ms)
        # Now: 99 newer trades, then the already committed high-water row (a duplicate / "found"
        # id ends page 1), and a late-indexed own trade older than the high-water mark but inside
        # the overlap window that only appears on page 2.
        newer = [trade(1000 + i, BASE_TS + 301_000 + i * 10) for i in reversed(range(99))]
        late = replace(trade(0, BASE_TS + 200_500), trade_id_str=str(BIG + 2005),
                       own_exchange_order_id=str(BIG + 777_777))
        port.rows[STREAM_TRADES] = newer + old[:100] + [late] + old[100:]
        port.calls.clear()

        result = (await scan_until_done(scanner))[-1]

        self.assertTrue(result.complete, result.incomplete_reason)
        self.assertIn(late.own_exchange_order_id, {row.own_exchange_order_id for row in result.new_trades})
        self.assertEqual(100, len(result.new_trades))  # 99 newer + the late one; duplicates are idempotent
        # page 1 held the duplicate, page 2 the late row, page 3 crossed the boundary (300 s - 150 s)
        self.assertEqual(3, port.pages_requested(STREAM_TRADES))
        self.assertEqual(2, port.pages_requested(STREAM_ORDERS))

    async def test_ac41_unresolved_lookback_extends_boundary_for_both_endpoints(self):
        trades = newest_first_trades(400)
        orders = newest_first_orders(400)
        port = FakeHistoryPort(trades=trades, orders=orders)
        scanner, view, _ = self.make(port, overlap_s=Decimal("60"))
        view.commit((await scan_until_done(scanner))[-1])
        port.calls.clear()
        # an unresolved own order was created 350 s before the high-water mark
        view.unresolved_ms = BASE_TS + 49_000
        result = (await scan_until_done(scanner))[-1]
        self.assertTrue(result.complete, result.incomplete_reason)
        # boundary = BASE_TS + 49 s - 60 s < BASE_TS: both streams must reach their natural end
        self.assertEqual(4, port.pages_requested(STREAM_TRADES))
        self.assertEqual(4, port.pages_requested(STREAM_ORDERS))

    # ---------------------------------------------------------------- AC-09
    async def test_ac09_duplicate_and_reordered_rows_do_not_double_or_prove_terminal(self):
        trades = newest_first_trades(10, size="2")
        port = FakeHistoryPort(trades=trades + [trades[-1]])  # duplicated oldest row (identical payload)
        scanner, view, _ = self.make(port)
        result = (await scan_until_done(scanner))[-1]
        self.assertTrue(result.complete, result.incomplete_reason)
        self.assertEqual(10, len(result.new_trades))
        view.commit(result)
        view.commit(result)  # replayed commit is idempotent
        self.assertEqual(Decimal("20"), sum((row.size for row in view.trades), Decimal("0")))

        port.rows[STREAM_TRADES] = [trades[3], trades[0], trades[1]]  # reordered delivery
        reordered = (await scan_until_done(scanner))[-1]
        self.assertFalse(reordered.complete)
        self.assertTrue(reordered.incomplete_reason.startswith(REASON_ORDERING_VIOLATION))

        # duplicated legs never inflate the settlement cumulative
        terminal = order(7, BASE_TS + 20_000, filled="2", status="filled")
        leg = trade(7, BASE_TS + 19_000, size="2", order_id=terminal.order_index)
        decision = evaluate_terminal_release(
            terminal_row=terminal, owned_trades=[leg, leg, leg], history_complete=True,
            first_terminal_seen_at=100.0, complete_scans=[ScanRecord(100.0, 101.0), ScanRecord(106.0, 107.0)],
            now=110.0, settlement_delay_s=Decimal("5"), settlement_scans=2,
        )
        self.assertTrue(decision.release)
        self.assertEqual(Decimal("2"), decision.trades_cumulative)

    async def test_self_trade_keeps_both_own_legs(self):
        buy = trade(1, BASE_TS, side=Side.BUY, order_id=str(BIG + 11))
        sell = replace(buy, own_side=Side.SELL, own_exchange_order_id=str(BIG + 12), is_maker=False)
        port = FakeHistoryPort(trades=[buy, sell])
        scanner, _, _ = self.make(port)
        result = (await scan_until_done(scanner))[-1]
        self.assertTrue(result.complete, result.incomplete_reason)
        self.assertEqual({Side.BUY, Side.SELL}, {row.own_side for row in result.new_trades})
        self.assertEqual(2, len(result.new_trades))

    # ---------------------------------------------------------------- AC-53 (scan part)
    async def test_ac53_retention_gap_when_high_water_row_no_longer_served(self):
        trades = newest_first_trades(20)
        port = FakeHistoryPort(trades=trades)
        scanner, view, _ = self.make(port)
        view.commit((await scan_until_done(scanner))[-1])
        # venue history now only holds rows newer than the committed high-water row
        port.rows[STREAM_TRADES] = [trade(500 + i, BASE_TS + 3_600_000 + i) for i in reversed(range(5))]
        result = (await scan_until_done(scanner))[-1]
        self.assertFalse(result.complete)
        self.assertEqual(REASON_RETENTION_GAP, result.incomplete_reason)
        self.assertTrue(any(c.startswith(REASON_RETENTION_GAP) for c in result.conflicts))
        self.assertIsNone(result.trades_high_water)

    async def test_ac53_retention_horizon_policy(self):
        port = FakeHistoryPort(trades=newest_first_trades(5))
        clock = FakeClock(now=BASE_TS / 1000 + 40 * 86400)
        scanner, view, _ = self.make(port, clock=clock, retention_horizon_s=Decimal(30 * 86400))
        view.floor_ms = BASE_TS
        result = (await scan_until_done(scanner))[-1]
        self.assertFalse(result.complete)
        self.assertEqual(REASON_RETENTION_GAP, result.incomplete_reason)

    async def test_missing_high_water_row_inside_window_is_conflict(self):
        trades = newest_first_trades(20)
        port = FakeHistoryPort(trades=trades)
        scanner, view, _ = self.make(port)
        view.commit((await scan_until_done(scanner))[-1])
        port.rows[STREAM_TRADES] = trades[1:]  # the committed newest row vanished, older rows remain
        result = (await scan_until_done(scanner))[-1]
        self.assertFalse(result.complete)
        self.assertTrue(any("high_water_row_missing" in c for c in result.conflicts), result.conflicts)

    # ---------------------------------------------------------------- NG-HIST-003 cadence
    async def test_concurrent_scans_are_coalesced(self):
        port = FakeHistoryPort(trades=newest_first_trades(3), orders=newest_first_orders(3))
        port.gate = asyncio.Event()
        scanner, _, _ = self.make(port)
        first = asyncio.ensure_future(scanner.scan())
        await asyncio.sleep(0)
        second = asyncio.ensure_future(scanner.scan())
        await asyncio.sleep(0)
        self.assertTrue(scanner.scanning)
        self.assertFalse(scanner.should_scan())
        port.gate.set()
        r1, r2 = await asyncio.gather(first, second)
        self.assertIs(r1, r2)
        self.assertEqual(1, scanner.coalesced_calls)
        self.assertEqual(1, port.pages_requested(STREAM_TRADES))
        self.assertEqual(1, port.pages_requested(STREAM_ORDERS))

    async def test_failures_back_off_and_wakeups_coalesce(self):
        port = FakeHistoryPort(trades=newest_first_trades(3))

        def boom(page):
            raise TimeoutError()

        port.faults[(STREAM_TRADES, 0)] = boom
        scanner, _, clock = self.make(port, backoff_initial_s=Decimal("5"), poll_interval_s=Decimal("30"))
        failed = await scanner.scan()
        self.assertEqual(f"{REASON_PAGE_FETCH_ERROR}:{STREAM_TRADES}:TimeoutError", failed.incomplete_reason)
        calls = len(port.calls)
        backoff = await scanner.scan()
        self.assertEqual(REASON_BACKOFF, backoff.incomplete_reason)
        self.assertEqual(calls, len(port.calls))
        self.assertFalse(scanner.should_scan())
        clock.now += 5
        self.assertTrue(scanner.should_scan())
        ok = await scanner.scan()
        self.assertTrue(ok.complete, ok.incomplete_reason)
        self.assertIsNone(scanner.backoff_until)
        # poll cadence: no scan before the interval unless woken; many wakeups -> one scan
        clock.now += 0.5
        self.assertFalse(scanner.should_scan())
        for _ in range(10):
            scanner.wake()
        clock.now += 1
        self.assertTrue(scanner.should_scan())
        await scanner.scan()
        self.assertFalse(scanner.should_scan())

    def test_budget_must_pay_for_one_page(self):
        with self.assertRaises(ValueError):
            HistoryScanner(FakeHistoryPort(), InMemoryHistoryCursorView(DOMAIN), Decimal("60"), 500)

    async def test_steady_state_reads_only_to_overlap_boundary(self):
        port = FakeHistoryPort(trades=newest_first_trades(1000, step_ms=1000),
                               orders=newest_first_orders(1000, step_ms=1000))
        scanner, view, _ = self.make(port, overlap_s=Decimal("60"))
        view.commit((await scan_until_done(scanner))[-1])
        port.calls.clear()
        result = (await scan_until_done(scanner))[-1]
        self.assertTrue(result.complete, result.incomplete_reason)
        self.assertEqual(1, port.pages_requested(STREAM_TRADES))
        self.assertEqual(1, port.pages_requested(STREAM_ORDERS))
        self.assertEqual(700, result.weight_used)


class ConflictWithholdingTest(unittest.IsolatedAsyncioTestCase):
    """Review findings 1 and 2: a contradicted key is never offered for commit; orders key by exchange id."""

    def make(self, port, **kwargs):
        view = InMemoryHistoryCursorView(DOMAIN)
        return HistoryScanner(port, view, Decimal("60"), 100_000, clock=FakeClock(), **kwargs), view

    async def test_ac40_in_scan_conflicting_trade_duplicate_is_withheld_and_never_committed(self):
        trades = newest_first_trades(5, size="1")
        bad = replace(trades[0], size=Decimal("7"))
        port = FakeHistoryPort(trades=[trades[0], bad] + trades[1:])
        scanner, view = self.make(port)
        result = (await scan_until_done(scanner))[-1]
        key = trades[0].dedupe_key(DOMAIN)
        self.assertFalse(result.complete)
        self.assertEqual(REASON_CONFLICT, result.incomplete_reason)
        self.assertNotIn(key, {row.dedupe_key(DOMAIN) for row in result.new_trades})
        self.assertEqual(4, len(result.new_trades))
        self.assertEqual({Decimal("1"), Decimal("7")},
                         {row.size for row in scanner.last_conflicted_rows[STREAM_TRADES]})
        view.commit(result)
        self.assertIsNone(view.committed_payload(STREAM_TRADES, key))

    async def test_conflict_on_a_later_page_of_a_multi_call_walk_is_never_pre_committed(self):
        trades = newest_first_trades(150, size="1")
        # the last row of page 1 is served again at the top of page 2 with a different size
        contradicting = replace(trades[99], size=Decimal("2"))
        port = FakeHistoryPort(trades=trades[:100] + [contradicting] + trades[100:])
        scanner = HistoryScanner(port, InMemoryHistoryCursorView(DOMAIN), Decimal("60"), 700, clock=FakeClock())
        view = scanner._view
        first = await scanner.scan()  # one inactive-orders page + one trades page
        self.assertEqual(REASON_IN_PROGRESS, first.incomplete_reason)
        self.assertEqual(1, port.pages_requested(STREAM_TRADES))
        self.assertEqual([], first.new_trades)  # nothing is offered before the walk has finished
        view.commit(first)
        final = (await scan_until_done(scanner))[-1]
        self.assertFalse(final.complete)
        key = trades[99].dedupe_key(DOMAIN)
        self.assertNotIn(key, {row.dedupe_key(DOMAIN) for row in final.new_trades})
        self.assertEqual(149, len(final.new_trades))
        view.commit(final)
        self.assertIsNone(view.committed_payload(STREAM_TRADES, key))

    async def test_ac40_inactive_order_with_contradicting_filled_is_withheld(self):
        orders = newest_first_orders(3, filled="3")
        port = FakeHistoryPort(orders=[orders[0], replace(orders[0], filled_base_amount=Decimal("5"))] + orders[1:])
        scanner, _ = self.make(port)
        result = (await scan_until_done(scanner))[-1]
        self.assertFalse(result.complete)
        self.assertNotIn(orders[0].order_index, {row.order_index for row in result.new_orders})
        self.assertEqual(2, len(result.new_orders))

    async def test_non_terminal_inactive_row_is_withheld(self):
        orders = newest_first_orders(2)
        port = FakeHistoryPort(orders=[replace(orders[0], status="open"), orders[1]])
        scanner, _ = self.make(port)
        result = (await scan_until_done(scanner))[-1]
        self.assertFalse(result.complete)
        self.assertEqual([orders[1].order_index], [row.order_index for row in result.new_orders])

    async def test_ac40_same_exchange_order_with_different_client_id_is_conflict(self):
        """Finding 2: orders key by exact exchange id; a client-id disagreement is a payload conflict."""
        order_a = order(1, BASE_TS, filled="3")
        order_b = replace(order_a, client_order_id=999, client_order_id_str="999", filled_base_amount=Decimal("5"))
        self.assertEqual(order_dedupe_key(DOMAIN, order_a), order_dedupe_key(DOMAIN, order_b))
        port = FakeHistoryPort(orders=[order_a, order_b])
        scanner, view = self.make(port)
        result = (await scan_until_done(scanner))[-1]
        self.assertFalse(result.complete)
        self.assertEqual([], result.new_orders)

        port.rows[STREAM_ORDERS] = [order_a]
        clean = (await scan_until_done(scanner))[-1]
        self.assertTrue(clean.complete, clean.incomplete_reason)
        view.commit(clean)
        port.rows[STREAM_ORDERS] = [replace(order_a, client_order_id=999, client_order_id_str="999")]
        later = (await scan_until_done(scanner))[-1]
        self.assertFalse(later.complete)
        self.assertTrue(any("committed_payload_mismatch" in c for c in later.conflicts), later.conflicts)

    async def test_order_row_without_exchange_id_is_schema_error(self):
        port = FakeHistoryPort(orders=[replace(order(1, BASE_TS), order_id=None, order_index=None)])
        scanner, _ = self.make(port)
        result = (await scan_until_done(scanner))[-1]
        self.assertFalse(result.complete)
        self.assertTrue(result.incomplete_reason.startswith(REASON_SCHEMA_ERROR), result.incomplete_reason)


class SettlementTest(unittest.TestCase):
    """AC-42 (scan part): terminal release = terminal row + full scan + cumulative equality + delay + repeat."""

    def setUp(self):
        self.terminal = order(1, BASE_TS, filled="10", status="filled")
        self.legs = [trade(1, BASE_TS - 2, size="4", order_id=self.terminal.order_index),
                     trade(2, BASE_TS - 1, size="6", order_id=None, cid=self.terminal.client_order_id)]
        self.legs[1] = replace(self.legs[1], own_exchange_order_id=None)
        self.scans = [ScanRecord(100.0, 101.0), ScanRecord(106.0, 107.0)]

    def decide(self, **overrides):
        kwargs = dict(terminal_row=self.terminal, owned_trades=self.legs, history_complete=True,
                      first_terminal_seen_at=100.0, complete_scans=self.scans, now=110.0,
                      settlement_delay_s=Decimal("5"), settlement_scans=2)
        kwargs.update(overrides)
        return evaluate_terminal_release(**kwargs)

    def test_released_only_when_every_condition_holds(self):
        decision = self.decide()
        self.assertTrue(decision.release, decision.reason)
        self.assertEqual(Decimal("10"), decision.trades_cumulative)

    def test_blockers(self):
        cases = {
            "no_terminal_row": dict(terminal_row=None),
            "order_row_not_terminal": dict(terminal_row=replace(self.terminal, status="open")),
            "awaiting_owned_executions": dict(owned_trades=self.legs[:1]),
            "history_incomplete": dict(history_complete=False),
            "terminal_seen_time_unknown": dict(first_terminal_seen_at=None),
            "settlement_delay": dict(now=103.0),
            "awaiting_repeat_scans": dict(complete_scans=self.scans[1:]),
            "awaiting_post_delay_scan": dict(complete_scans=[ScanRecord(100.0, 101), ScanRecord(101.0, 102)]),
        }
        for reason, overrides in cases.items():
            with self.subTest(reason):
                decision = self.decide(**overrides)
                self.assertFalse(decision.release)
                self.assertEqual(reason, decision.reason)
                self.assertFalse(decision.conflict)

    def test_ac40_conflicts_block_release(self):
        extra = trade(3, BASE_TS - 3, size="1", order_id=self.terminal.order_index)
        over = self.decide(owned_trades=self.legs + [extra])
        self.assertFalse(over.release)
        self.assertTrue(over.conflict)
        self.assertEqual("trades_exceed_terminal_cumulative", over.reason)

        wrong_cid = replace(self.legs[0], own_client_order_id=self.terminal.client_order_id + 1)
        mismatch = self.decide(owned_trades=[wrong_cid, self.legs[1]])
        self.assertTrue(mismatch.conflict)
        self.assertEqual("attribution_conflict", mismatch.reason)

        resized = replace(self.legs[0], size=Decimal("5"))
        duplicate = self.decide(owned_trades=self.legs + [resized])
        self.assertTrue(duplicate.conflict)

        wrong_side = replace(self.legs[0], own_side=Side.SELL)
        self.assertTrue(self.decide(owned_trades=[wrong_side, self.legs[1]]).conflict)

    def test_zero_fill_terminal_requires_the_same_proof(self):
        canceled = order(2, BASE_TS, filled="0", status="canceled-post-only")
        self.assertTrue(self.decide(terminal_row=canceled, owned_trades=[]).release)
        self.assertEqual("settlement_delay", self.decide(terminal_row=canceled, owned_trades=[], now=101.0).reason)

    def test_attribution_is_by_exact_id_only(self):
        unrelated = trade(9, BASE_TS, size="10", order_id=str(BIG + 999_999))
        matched, conflicts = attribute_trades_to_order(self.terminal, [unrelated])
        self.assertEqual([], matched)
        self.assertEqual([], conflicts)


if __name__ == "__main__":
    unittest.main()
