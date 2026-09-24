"""NG-HIST-004 client order id allocation logic; AC-43 (logic part; durable map is WS-B), AC-22 (48-bit CID)."""
import unittest

from hummingbot.strategy_v2.executors.neutral_grid_executor.cid import (
    CidAllocator,
    CidCollision,
    CidError,
    CidExhausted,
    CidInvalid,
    validate_cid,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import MAX_CLIENT_ORDER_ID, LegIdentity, LegRole


def ident(cell=0, gen=0, role=LegRole.ENTRY, rev=0, grid="g1"):
    return LegIdentity(grid, cell, gen, role, rev)


class TestCid(unittest.TestCase):
    def test_monotonic_unique_and_identity_stable(self):
        alloc = CidAllocator(exists=lambda cid: False, high_water=41)
        a = alloc.allocate(ident(0))
        b = alloc.allocate(ident(1))
        c = alloc.allocate(ident(0, role=LegRole.TP))
        self.assertEqual((42, 43, 44), (a, b, c))
        # Timeout / status lag / crash-retry of the same leg never mints a new id.
        self.assertEqual(42, alloc.allocate(ident(0)))
        self.assertEqual(44, alloc.high_water)
        self.assertEqual(ident(1), alloc.identity_of(43))

    def test_durable_lookup_survives_restart_without_new_id(self):
        durable = {}
        first = CidAllocator(exists=lambda cid: cid in durable.values())
        durable[ident(5)] = first.allocate(ident(5))
        restarted = CidAllocator(exists=lambda cid: cid in durable.values(), high_water=first.high_water,
                                 lookup=durable.get)
        self.assertEqual(durable[ident(5)], restarted.allocate(ident(5)))
        self.assertEqual(first.high_water, restarted.high_water)       # nothing consumed
        self.assertEqual(durable[ident(5)] + 1, restarted.allocate(ident(6)))

    def test_collision_fails_closed_without_skipping(self):
        alloc = CidAllocator(exists=lambda cid: cid == 3, high_water=2)
        with self.assertRaises(CidCollision):
            alloc.allocate(ident(1))
        self.assertEqual(2, alloc.high_water)
        self.assertIsNotNone(alloc.failed)
        with self.assertRaises(CidError):                               # latched: no retry with another id
            alloc.allocate(ident(2))

    def test_durable_map_pointing_two_identities_at_one_cid_is_a_collision(self):
        alloc = CidAllocator(exists=lambda cid: False, high_water=10, lookup=lambda i: 7)
        alloc.allocate(ident(1))
        with self.assertRaises(CidCollision):
            alloc.allocate(ident(2))

    def test_48_bit_exhaustion_fails_closed(self):
        alloc = CidAllocator(exists=lambda cid: False, high_water=MAX_CLIENT_ORDER_ID - 1)
        self.assertEqual(MAX_CLIENT_ORDER_ID, alloc.allocate(ident(0)))
        with self.assertRaises(CidExhausted):
            alloc.allocate(ident(1))
        with self.assertRaises(CidError):
            alloc.allocate(ident(2))
        self.assertEqual(MAX_CLIENT_ORDER_ID, alloc.allocate(ident(0)))  # known leg still resolves

    def test_malformed_durable_lookup_latches_fail_closed(self):
        # Review #8: a corrupt durable row must latch the allocator, not only raise once.
        alloc = CidAllocator(exists=lambda cid: False,
                             lookup=lambda i: (1 << 48) if i.cell_id == 0 else None)
        with self.assertRaises(CidInvalid):
            alloc.allocate(ident(0))
        self.assertIsNotNone(alloc.failed)
        with self.assertRaises(CidError):
            alloc.allocate(ident(1))
        self.assertEqual(0, alloc.high_water)

    def test_validate_never_truncates_hashes_or_coerces(self):
        self.assertEqual(MAX_CLIENT_ORDER_ID, validate_cid((1 << 48) - 1))
        for bad in (0, -1, 1 << 48, (1 << 48) + 5, 1.0, "12", True, None):
            with self.subTest(bad=bad), self.assertRaises(CidInvalid):
                validate_cid(bad)
        with self.assertRaises(CidInvalid):
            CidAllocator(exists=lambda cid: False, lookup=lambda i: 1 << 48).allocate(ident(0))
        with self.assertRaises(CidInvalid):
            CidAllocator(exists=lambda cid: False, high_water=-1)
        with self.assertRaises(CidInvalid):
            CidAllocator(exists=lambda cid: False).allocate(("g1", 0, 0, "ENTRY", 0))


if __name__ == "__main__":
    unittest.main()
