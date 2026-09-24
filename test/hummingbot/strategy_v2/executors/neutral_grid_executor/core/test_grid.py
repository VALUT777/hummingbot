"""NG-GRID-001/002/003, NG-RISK-005 bootstrap validation, AC-03."""
import unittest

from hummingbot.strategy_v2.executors.neutral_grid_executor import grid
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import OrderTypePolicy, Side
from hummingbot.strategy_v2.executors.neutral_grid_executor.grid import GridValidationError

from .helpers import D, config, rules


class TestIntegerTickGrid(unittest.TestCase):
    def test_sample_5_6_n55_anchor_5_4_gives_56_lines_22_buy_33_sell_for_every_plausible_tick(self):
        # The sample is tick-independent as long as T >= N: floor(i*T/55) < 0.4*T  <=>  i < 22.
        for tick in ("0.01", "0.001", "0.0001", "0.00001"):
            with self.subTest(tick=tick):
                prices = grid.build_grid(D("5"), D("6"), 55, rules(tick=tick))
                self.assertEqual(56, len(prices))
                cells = grid.assign_cells(prices, grid.compute_anchor(D("5.4"), D("5"), D("6")))
                self.assertEqual(55, len(cells))
                self.assertEqual({"BUY": 22, "SELL": 33}, grid.side_counts(cells))
                # BUY cells are exactly the 22 lowest cells; the boundary line P[22] equals the anchor.
                self.assertTrue(all(c.entry_side == Side.BUY for c in cells[:22]))
                self.assertTrue(all(c.entry_side == Side.SELL for c in cells[22:]))
                self.assertEqual(D("5.4"), prices[22])

    def test_sample_explicit_arithmetic_with_tick_0_0001(self):
        # L_tick=50000, U_tick=60000, T=10000, N=55: P_i = 50000 + floor(i*10000/55).
        prices = grid.build_grid(D("5"), D("6"), 55, rules(tick="0.0001"))
        self.assertEqual(D("5"), prices[0])
        self.assertEqual(D("5.0181"), prices[1])      # floor(10000/55) = 181
        self.assertEqual(D("5.0363"), prices[2])      # floor(20000/55) = 363
        self.assertEqual(D("5.3818"), prices[21])     # floor(210000/55) = 3818 < 4000 -> last BUY cell
        self.assertEqual(D("5.4000"), prices[22])     # floor(220000/55) = 4000 == anchor -> SELL
        self.assertEqual(D("6"), prices[55])
        widths = {int((b - a) / D("0.0001")) for a, b in zip(prices, prices[1:])}
        self.assertEqual({181, 182}, widths)          # uneven widths: no float step, no drift
        for p in prices:
            self.assertEqual(0, (p / D("0.0001")) % 1)  # exact tick multiples

    def test_cell_sides_entry_and_tp_prices(self):
        prices = grid.build_grid(D("5"), D("6"), 55, rules())
        cells = grid.assign_cells(prices, D("5.4"))
        buy, sell = cells[21], cells[22]
        self.assertEqual((Side.BUY, prices[21], Side.SELL, prices[22]),
                         (buy.entry_side, buy.entry_price, buy.tp_side, buy.tp_price))
        self.assertEqual((Side.SELL, prices[23], Side.BUY, prices[22]),
                         (sell.entry_side, sell.entry_price, sell.tp_side, sell.tp_price))

    def test_anchor_is_clamped_to_bounds(self):
        prices = grid.build_grid(D("5"), D("6"), 55, rules())
        below = grid.assign_cells(prices, grid.compute_anchor(D("4.2"), D("5"), D("6")))
        above = grid.assign_cells(prices, grid.compute_anchor(D("7.9"), D("5"), D("6")))
        self.assertEqual({"BUY": 0, "SELL": 55}, grid.side_counts(below))
        self.assertEqual({"BUY": 55, "SELL": 0}, grid.side_counts(above))
        self.assertEqual(D("5"), grid.compute_anchor(D("4.2"), D("5"), D("6")))
        self.assertEqual(D("6"), grid.compute_anchor(D("7.9"), D("5"), D("6")))

    def test_non_multiple_bounds_are_rejected_not_quantized(self):
        with self.assertRaises(GridValidationError):
            grid.build_grid(D("5.00005"), D("6"), 55, rules(tick="0.0001"))
        with self.assertRaises(GridValidationError):
            grid.build_grid(D("5"), D("6.00001"), 55, rules(tick="0.0001"))

    def test_collapse_and_insufficient_ticks_are_rejected(self):
        with self.assertRaises(GridValidationError):
            grid.build_grid(D("6"), D("6"), 5, rules())
        with self.assertRaises(GridValidationError):
            grid.build_grid(D("6"), D("5"), 5, rules())
        with self.assertRaises(GridValidationError):          # T=10 < N=55
            grid.build_grid(D("5"), D("6"), 55, rules(tick="0.1"))
        self.assertEqual(11, len(grid.build_grid(D("5"), D("6"), 10, rules(tick="0.1"))))  # T == N ok

    def test_bad_types_are_rejected(self):
        for n in (0, -1, True, 5.0, "55"):
            with self.subTest(n=n), self.assertRaises(GridValidationError):
                grid.build_grid(D("5"), D("6"), n, rules())
        with self.assertRaises(GridValidationError):
            grid.build_grid(5.0, D("6"), 55, rules())
        with self.assertRaises(GridValidationError):
            grid.build_grid(D("5"), D("NaN"), 55, rules())
        with self.assertRaises(GridValidationError):
            grid.assign_cells([D("5"), D("5")], D("5"))

    def test_quantize_never_rounds_quantity_up(self):
        self.assertEqual(D("7.12"), grid.quantize_down(D("7.129"), D("0.01")))
        self.assertEqual(D("7.13"), grid.quantize_up(D("7.121"), D("0.01")))
        self.assertEqual(D("7.12"), grid.quantize_up(D("7.12"), D("0.01")))

    def test_min_valid_qty_is_exact_even_for_repeating_quotients(self):
        # 10 / 5.4 = 1.851851...; step 0.01 -> 1.86 and 1.86*5.4 >= 10 exactly.
        r = rules(min_base="0", min_notional="10")
        qty = grid.min_valid_order_qty(r, D("5.4"))
        self.assertEqual(D("1.86"), qty)
        self.assertGreaterEqual(qty * D("5.4"), D("10"))
        self.assertLess((qty - D("0.01")) * D("5.4"), D("10"))
        self.assertEqual(D("0.01"), grid.min_valid_order_qty(rules(min_base="0", min_notional="0"), D("5")))


class TestValidateConfig(unittest.TestCase):
    def test_sample_config_is_valid(self):
        self.assertEqual([], grid.validate_config(config(), rules(), D("5.4")))

    def test_q_not_step_multiple_is_rejected(self):
        errors = grid.validate_config(config(order_amount_base=D("10.005")), rules(step="0.01"), D("5.4"))
        self.assertTrue(any("not an exact multiple of size step" in e for e in errors), errors)

    def test_full_q_below_minimum_at_any_entry_or_tp_price_is_rejected_per_cell(self):
        # min_notional 55: Q=10 is invalid at prices < 5.5 (both as entry price and as TP price).
        errors = grid.validate_config(config(), rules(min_notional="55"), D("5.4"))
        joined = "\n".join(errors)
        self.assertIn("cell 0 entry @ 5", joined)
        self.assertIn("cell 0 tp @ 5.0181", joined)
        # SELL cell 26: entry P[27]=5.4909 and TP P[26]=5.4727 both invalid.
        self.assertIn("cell 26 entry @ 5.4909", joined)
        self.assertIn("cell 26 tp @ 5.4727", joined)
        # SELL cell 27: entry P[28]=5.5090 valid, but its TP P[27]=5.4909 is not -> rejected on the TP leg alone.
        self.assertIn("cell 27 tp @ 5.4909", joined)
        self.assertNotIn("cell 27 entry", joined)
        self.assertNotIn("cell 54 ", joined)   # 5.9818 / 6 valid
        # TP side alone can be the only failing leg: min_base above Q.
        errors = grid.validate_config(config(order_amount_base=D("4")), rules(), D("5.4"))
        self.assertTrue(any("BELOW_MIN_BASE" in e for e in errors))

    def test_market_and_unsupported_order_types_are_forbidden(self):
        errors = grid.validate_config(config(entry_order_type="MARKET"), rules(), D("5.4"))
        self.assertTrue(any("entry_order_type" in e and "MARKET" in e for e in errors), errors)
        errors = grid.validate_config(config(tp_order_type="MARKET"), rules(), D("5.4"))
        self.assertTrue(any("tp_order_type" in e for e in errors), errors)
        errors = grid.validate_config(config(), rules(supports_post_only=False), D("5.4"))
        self.assertTrue(any("post-only" in e for e in errors), errors)
        self.assertEqual([], grid.validate_config(config(entry_order_type=OrderTypePolicy.LIMIT), rules(), D("5.4")))

    def test_leverage_caps_and_venue_limits(self):
        self.assertTrue(grid.validate_config(config(leverage=D("25")), rules(max_leverage="20"), D("5.4")))
        self.assertTrue(any("maximum leverage unknown" in e
                            for e in grid.validate_config(config(), rules(max_leverage=None), D("5.4"))))
        self.assertTrue(any("exceeds venue limit" in e
                            for e in grid.validate_config(config(max_active_orders=200), rules(venue_cap=150),
                                                          D("5.4"))))
        for field_name in ("max_abs_net_position", "max_gross_position"):
            errors = grid.validate_config(config(**{field_name: D("0")}), rules(), D("5.4"))
            self.assertTrue(any(field_name in e for e in errors))
        self.assertTrue(grid.validate_config(config(max_active_orders=0), rules(), D("5.4")))

    def test_baseline_required_only_on_bootstrap_and_within_cap(self):
        cfg = config(expected_initial_position=None)
        self.assertTrue(any("expected_initial_position" in e for e in grid.validate_config(cfg, rules(), D("5.4"))))
        self.assertEqual([], grid.validate_config(cfg, rules(), D("5.4"), bootstrap=False))
        self.assertEqual([], grid.validate_config(config(expected_initial_position=D("-330")), rules(), D("5.4")))
        self.assertTrue(grid.validate_config(config(expected_initial_position=D("1000.01")), rules(), D("5.4")))

    def test_unknown_mid_rules_and_design_floors(self):
        self.assertTrue(any("mid_price" in e for e in grid.validate_config(config(), rules(), None)))
        self.assertIn("RULES_UNKNOWN", grid.validate_config(config(), None, D("5.4")))
        self.assertTrue(any("history_overlap_s" in e
                            for e in grid.validate_config(config(history_overlap_s=D("30")), rules(), D("5.4"))))
        self.assertTrue(any("poll_interval_s" in e
                            for e in grid.validate_config(config(poll_interval_s=D("1")), rules(), D("5.4"))))
        self.assertTrue(any("grid:" in e
                            for e in grid.validate_config(config(lower_price=D("5.00001")), rules(), D("5.4"))))

    def test_fingerprint_is_stable_and_tracks_dimensions(self):
        a = grid.config_fingerprint(config())
        self.assertEqual(a, grid.config_fingerprint(config(max_active_orders=80)))       # not a dimension
        self.assertEqual(a, grid.config_fingerprint(config(order_amount_base=D("10.00"))))  # same exact value
        self.assertNotEqual(a, grid.config_fingerprint(config(order_amount_base=D("11"))))
        self.assertNotEqual(a, grid.config_fingerprint(config(cell_count=54)))
        self.assertNotEqual(a, grid.config_fingerprint(config(upper_price=D("6.1"))))
        self.assertEqual(a, config().fingerprint())                    # contracts.GridConfig delegates here


class TestPreview(unittest.TestCase):
    def test_sample_preview_counts_range_and_slots(self):
        p = grid.build_preview(config(), rules(), D("5.4"))
        self.assertEqual([], p.errors)
        self.assertEqual((56, 55, 22, 33), (len(p.prices), len(p.cells), p.buy_cells, p.sell_cells))
        self.assertEqual((D("-330"), D("220")), (p.reachable_min, p.reachable_max))
        self.assertEqual((40, 15), (p.armed, p.queued))
        self.assertEqual({3}, set(p.slots_per_cell.values()))
        self.assertEqual((120, 0), (p.slots_reserved, p.slots_free))

    def test_preview_with_baseline_330(self):
        p = grid.build_preview(config(expected_initial_position=D("330")), rules(), D("5.4"))
        self.assertEqual((D("0"), D("550")), (p.reachable_min, p.reachable_max))


if __name__ == "__main__":
    unittest.main()
