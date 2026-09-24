"""Audited conflict resolutions (round 3, critic C3): NG-HIST-002, AC-40, AC-12, NG-OPS-003."""
import unittest
from dataclasses import replace
from decimal import Decimal

from test_ng_history_scanner import (
    BASE_TS,
    DOMAIN,
    FakeClock,
    FakeHistoryPort,
    newest_first_orders,
    newest_first_trades,
    scan_until_done,
)

from hummingbot.strategy_v2.executors.neutral_grid_executor.history import (
    REASON_CONFLICT,
    REASON_LEDGER_CORRECTION,
    STREAM_ORDERS,
    STREAM_TRADES,
    AuditedResolution,
    HistoryScanner,
    InMemoryHistoryCursorView,
    order_dedupe_key,
    order_payload_fingerprint,
    trade_payload_fingerprint,
)


class AuditedResolutionTest(unittest.IsolatedAsyncioTestCase):

    def make(self, port):
        view = InMemoryHistoryCursorView(DOMAIN)
        return HistoryScanner(port, view, Decimal("60"), 100_000, clock=FakeClock()), view

    async def committed_walk(self, trades, orders=()):
        port = FakeHistoryPort(trades=trades, orders=list(orders))
        scanner, view = self.make(port)
        first = (await scan_until_done(scanner))[-1]
        self.assertTrue(first.complete, first.incomplete_reason)
        view.commit(first)
        return port, scanner, view

    async def test_without_resolution_committed_mismatch_conflicts_on_every_walk(self):
        trades = newest_first_trades(5)
        port, scanner, view = await self.committed_walk(trades)
        port.rows[STREAM_TRADES][2] = replace(trades[2], size=Decimal("9"))
        for _ in range(3):
            result = (await scan_until_done(scanner))[-1]
            self.assertFalse(result.complete)
            self.assertEqual(REASON_CONFLICT, result.incomplete_reason)

    async def test_rejected_audited_payload_is_noise_and_walk_completes(self):
        trades = newest_first_trades(5)
        port, scanner, view = await self.committed_walk(trades)
        venue_variant = replace(trades[2], size=Decimal("9"))
        port.rows[STREAM_TRADES][2] = venue_variant
        key = trades[2].dedupe_key(DOMAIN)
        view.record_resolution(AuditedResolution(
            stream=STREAM_TRADES, key=key, accepted_fingerprint=trade_payload_fingerprint(trades[2]),
            rejected_fingerprints=frozenset({trade_payload_fingerprint(venue_variant)})))
        for _ in range(2):
            result = (await scan_until_done(scanner))[-1]
            self.assertTrue(result.complete, (result.incomplete_reason, result.conflicts))
            self.assertEqual([], result.new_trades)
            self.assertEqual([], scanner.last_ledger_corrections)
            self.assertEqual(1, scanner.last_audited_noise[STREAM_TRADES])
            view.commit(result)
        self.assertEqual(trade_payload_fingerprint(trades[2]), view.committed_payload(STREAM_TRADES, key))

    async def test_third_unaudited_payload_still_conflicts(self):
        trades = newest_first_trades(5)
        port, scanner, view = await self.committed_walk(trades)
        variant_b = replace(trades[2], size=Decimal("9"))
        variant_c = replace(trades[2], size=Decimal("11"))
        key = trades[2].dedupe_key(DOMAIN)
        view.record_resolution(AuditedResolution(
            stream=STREAM_TRADES, key=key, accepted_fingerprint=trade_payload_fingerprint(trades[2]),
            rejected_fingerprints=frozenset({trade_payload_fingerprint(variant_b)})))
        for served in ([variant_c], [trades[2], variant_c], [variant_b, variant_c], [variant_c, variant_b]):
            with self.subTest(served=[str(row.size) for row in served]):
                port.rows[STREAM_TRADES] = trades[:2] + served + trades[3:]
                result = (await scan_until_done(scanner))[-1]
                self.assertFalse(result.complete)
                self.assertEqual(REASON_CONFLICT, result.incomplete_reason)
                self.assertNotIn(key, {row.dedupe_key(DOMAIN) for row in result.new_trades})

    async def test_accepted_payload_differing_from_committed_requires_explicit_ledger_correction(self):
        trades = newest_first_trades(5)
        port, scanner, view = await self.committed_walk(trades)
        truth = replace(trades[2], size=Decimal("9"))
        port.rows[STREAM_TRADES][2] = truth
        key = trades[2].dedupe_key(DOMAIN)
        view.record_resolution(AuditedResolution(
            stream=STREAM_TRADES, key=key, accepted_fingerprint=trade_payload_fingerprint(truth),
            rejected_fingerprints=frozenset({trade_payload_fingerprint(trades[2])})))
        result = (await scan_until_done(scanner))[-1]
        self.assertFalse(result.complete)
        self.assertEqual(REASON_LEDGER_CORRECTION, result.incomplete_reason)
        self.assertEqual([], result.new_trades)  # never applied through the ordinary commit path
        (correction,) = scanner.last_ledger_corrections
        self.assertEqual((STREAM_TRADES, key), (correction.stream, correction.key))
        self.assertEqual(trade_payload_fingerprint(trades[2]), correction.committed_fingerprint)
        self.assertEqual(trade_payload_fingerprint(truth), correction.accepted_fingerprint)
        self.assertEqual(Decimal("9"), correction.row.size)
        view.commit(result)
        self.assertEqual(trade_payload_fingerprint(trades[2]), view.committed_payload(STREAM_TRADES, key))
        # the engine applies it explicitly (one transaction); afterwards walks are clean
        view.apply_ledger_corrections(scanner.last_ledger_corrections)
        clean = (await scan_until_done(scanner))[-1]
        self.assertTrue(clean.complete, (clean.incomplete_reason, clean.conflicts))
        self.assertEqual([], scanner.last_ledger_corrections)

    async def test_committed_payload_not_audited_as_rejected_is_still_a_conflict(self):
        trades = newest_first_trades(5)
        port, scanner, view = await self.committed_walk(trades)
        truth = replace(trades[2], size=Decimal("9"))
        port.rows[STREAM_TRADES][2] = truth
        view.record_resolution(AuditedResolution(
            stream=STREAM_TRADES, key=trades[2].dedupe_key(DOMAIN),
            accepted_fingerprint=trade_payload_fingerprint(truth)))
        result = (await scan_until_done(scanner))[-1]
        self.assertEqual(REASON_CONFLICT, result.incomplete_reason)
        self.assertEqual([], scanner.last_ledger_corrections)

    async def test_in_scan_duplicate_resolved_offers_only_the_accepted_payload(self):
        trades = newest_first_trades(5)
        variant = replace(trades[0], size=Decimal("7"))
        port = FakeHistoryPort(trades=[trades[0], variant] + trades[1:])
        scanner, view = self.make(port)
        conflicted = (await scan_until_done(scanner))[-1]
        self.assertEqual(REASON_CONFLICT, conflicted.incomplete_reason)
        key = trades[0].dedupe_key(DOMAIN)
        view.record_resolution(AuditedResolution(
            stream=STREAM_TRADES, key=key, accepted_fingerprint=trade_payload_fingerprint(variant),
            rejected_fingerprints=frozenset({trade_payload_fingerprint(trades[0])})))
        resolved = (await scan_until_done(scanner))[-1]
        self.assertTrue(resolved.complete, (resolved.incomplete_reason, resolved.conflicts))
        offered = {row.dedupe_key(DOMAIN): row for row in resolved.new_trades}
        self.assertEqual(Decimal("7"), offered[key].size)
        self.assertEqual(5, len(resolved.new_trades))

    async def test_order_stream_resolution_and_non_terminal_noise(self):
        orders = newest_first_orders(3)
        port, scanner, view = await self.committed_walk([], orders)
        stale_open = replace(orders[1], status="open")
        port.rows[STREAM_ORDERS] = [orders[0], stale_open, orders[1], orders[2]]
        blocked = (await scan_until_done(scanner))[-1]
        self.assertFalse(blocked.complete)
        view.record_resolution(AuditedResolution(
            stream=STREAM_ORDERS, key=order_dedupe_key(DOMAIN, orders[1]),
            accepted_fingerprint=order_payload_fingerprint(orders[1]),
            rejected_fingerprints=frozenset({order_payload_fingerprint(stale_open)})))
        resolved = (await scan_until_done(scanner))[-1]
        self.assertTrue(resolved.complete, (resolved.incomplete_reason, resolved.conflicts))

    async def test_malformed_resolutions_fail_closed(self):
        trades = newest_first_trades(3)
        port, scanner, view = await self.committed_walk(trades)
        view.record_resolution(AuditedResolution(stream="bogus", key=("x",), accepted_fingerprint="{}"))
        result = await scanner.scan()
        self.assertFalse(result.complete)
        self.assertTrue(result.incomplete_reason.startswith("schema_error"), result.incomplete_reason)

    def test_views_without_the_optional_method_keep_previous_behaviour(self):
        class LegacyView:
            def high_water(self, stream):
                return None

            def committed_payload(self, stream, key):
                return None

            def oldest_unresolved_ms(self):
                return None

            def bootstrap_floor_ms(self, stream):
                return BASE_TS

        scanner = HistoryScanner(FakeHistoryPort(), LegacyView(), Decimal("60"), 100_000, clock=FakeClock())
        self.assertEqual({}, scanner._load_resolutions())


if __name__ == "__main__":
    unittest.main()
