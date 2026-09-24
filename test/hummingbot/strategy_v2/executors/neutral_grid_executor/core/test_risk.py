"""NG-RISK-002 / NG-RISK-004; AC-24, AC-25, AC-26, AC-27, AC-28, AC-37."""
import unittest

from hummingbot.strategy_v2.executors.neutral_grid_executor import grid, risk
from hummingbot.strategy_v2.executors.neutral_grid_executor.cells import CellLedger
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CellSpec,
    LegRole,
    OrderState,
    OrderTypePolicy,
    Side,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.risk import OpenLeg, RiskLimits

from .helpers import GRID_ID, Q, D, Harness, rules

LIMITS = RiskLimits(max_abs_net_position=D("1000"), max_gross_position=D("1000"))


def sample_ledgers_all_entries_resting():
    r = rules()
    prices = grid.build_grid(D("5"), D("6"), 55, r)
    cells = grid.assign_cells(prices, D("5.4"))
    ledgers = []
    for i, c in enumerate(cells):
        ledger = CellLedger(GRID_ID, c, Q)
        leg = ledger.begin_entry(1000 + i, r, order_type=OrderTypePolicy.LIMIT_MAKER)
        ledger.record_transport(leg.identity, TransportResult(TransportOutcome.ACCEPTED, exchange_order_id=str(i)))
        ledgers.append(ledger)
    return ledgers


class TestEndpoints(unittest.TestCase):
    def test_sample_reachable_intervals_for_baselines(self):
        # AC-28: positive/zero/negative B accepted; the baseline itself gets no cell/TP/seed.
        ledgers = sample_ledgers_all_entries_resting()
        for b, expected in ((D("0"), (D("-330"), D("220"))), (D("330"), (D("0"), D("550"))),
                            (D("-200"), (D("-530"), D("20")))):
            with self.subTest(b=b):
                ep = risk.endpoints_from_ledgers(b, ledgers)
                self.assertEqual(expected, (ep.P_min, ep.P_max))
                self.assertEqual(b, ep.P)
                self.assertEqual(D("550"), ep.gross_worst)            # baseline not in gross
                self.assertEqual([], risk.cap_violations(ep, LIMITS))
        cells = [ledger.spec for ledger in ledgers]
        self.assertEqual((D("-330"), D("220")), risk.reachable_interval(D("0"), cells, Q))
        self.assertEqual((D("0"), D("550")), risk.reachable_interval(D("330"), cells, Q))

    def test_full_remainder_counts_until_terminal_proven(self):
        h = Harness()
        e = h.entry()
        h.fill(e, "4")
        ep = risk.endpoints_from_ledgers(D("0"), [h.ledger])
        self.assertEqual((D("4"), D("10")), (ep.P, ep.P_max))        # 4 confirmed + 6 still executable
        h.ledger.set_state(e, OrderState.CANCEL_PENDING)
        h.ledger.set_state(e, OrderState.TERMINAL_UNKNOWN)            # cancel ack is not a proof
        self.assertEqual(D("10"), risk.endpoints_from_ledgers(D("0"), [h.ledger]).P_max)
        h.ledger.confirm_terminal(e, D("4"))
        self.assertEqual(D("4"), risk.endpoints_from_ledgers(D("0"), [h.ledger]).P_max)

    def test_cancel_timeout_keeps_full_remainder_reserved_and_cell_locked(self):
        # AC-21 (core part): CANCEL_UNKNOWN keeps the order, its whole remainder and its slot; no new cycle.
        from hummingbot.strategy_v2.executors.neutral_grid_executor import admission
        h = Harness()
        e = h.entry()
        h.fill(e, "3")
        h.ledger.set_state(e, OrderState.CANCEL_PENDING)
        h.ledger.set_state(e, OrderState.CANCEL_UNKNOWN)
        ep = risk.endpoints_from_ledgers(D("0"), [h.ledger])
        self.assertEqual((D("3"), D("10")), (ep.P, ep.P_max))
        self.assertIn("ENTRY_NOT_TERMINAL", h.ledger.can_release(True).reasons)
        self.assertEqual(1, len(h.ledger.non_final_legs()))
        self.assertGreaterEqual(admission.slot_need_from_ledger(h.ledger, h.rules), 1)
        with self.assertRaises(Exception):
            h.ledger.next_entry_identity()

    def test_unknown_role_counts_in_net_and_gross(self):
        ep = risk.endpoints(D("0"), D("0"), D("0"), [OpenLeg(Side.SELL, D("7"), role=None,
                                                             state=OrderState.SUBMIT_UNKNOWN)])
        self.assertEqual((D("-7"), D("0"), D("7")), (ep.P_min, ep.P_max, ep.gross_worst))
        tp = risk.endpoints(D("0"), D("0"), D("0"), [OpenLeg(Side.SELL, D("7"), role=LegRole.TP)])
        self.assertEqual((D("-7"), D("0")), (tp.P_min, tp.gross_worst))  # TP moves net, adds no gross

    def test_float_inputs_rejected(self):
        with self.assertRaises(ValueError):
            risk.endpoints(0.0, D("0"), D("0"), [])
        with self.assertRaises(ValueError):
            RiskLimits(max_abs_net_position=1000, max_gross_position=D("1"))


class TestCaps(unittest.TestCase):
    def test_ac24_pending_buy_rejected_when_p_max_exceeds_cap(self):
        ep = risk.RiskEndpoints(P=D("985"), P_min=D("985"), P_max=D("995"), gross_worst=D("10"))
        self.assertIsNone(risk.check_submit(ep, Side.BUY, D("5"), LegRole.ENTRY, LIMITS))
        self.assertTrue(risk.check_submit(ep, Side.BUY, D("10"), LegRole.ENTRY, LIMITS).startswith("NET_CAP_LONG"))
        self.assertTrue(risk.check_submit(ep, Side.BUY, D("10"), LegRole.TP, LIMITS).startswith("NET_CAP_LONG"))
        self.assertIsNone(risk.check_submit(ep, Side.SELL, D("10"), LegRole.ENTRY, LIMITS))

    def test_ac25_pending_sell_rejected_when_p_min_below_minus_cap_shorts_within_cap_work(self):
        ep = risk.RiskEndpoints(P=D("-990"), P_min=D("-995"), P_max=D("-990"), gross_worst=D("5"))
        self.assertTrue(risk.check_submit(ep, Side.SELL, D("10"), LegRole.ENTRY, LIMITS).startswith("NET_CAP_SHORT"))
        self.assertIsNone(risk.check_submit(ep, Side.SELL, D("5"), LegRole.ENTRY, LIMITS))
        self.assertIsNone(risk.check_submit(ep, Side.BUY, D("10"), LegRole.ENTRY, LIMITS))

    def test_ac26_gross_cap_with_venue_net_zero(self):
        long_cell = Harness(CellSpec(1, D("5.0181"), D("5.0363"), Side.BUY))
        short_cell = Harness(CellSpec(40, D("5.7272"), D("5.7454"), Side.SELL))
        for h in (long_cell, short_cell):
            e = h.entry()
            h.fill(e, "10")
            h.ledger.confirm_terminal(e, D("10"))
        ep = risk.endpoints_from_ledgers(D("0"), [long_cell.ledger, short_cell.ledger])
        self.assertEqual((D("0"), D("20")), (ep.P, ep.gross_worst))    # offsetting cells do not net out gross
        limits = RiskLimits(max_abs_net_position=D("1000"), max_gross_position=D("25"))
        self.assertTrue(risk.check_submit(ep, Side.BUY, D("10"), LegRole.ENTRY, limits).startswith("GROSS_CAP"))
        self.assertIsNone(risk.check_submit(ep, Side.SELL, D("10"), LegRole.TP, limits))  # TP never blocked by gross

    def test_ac27_virtual_tp_crosses_zero_non_reduce_only(self):
        long_cell = Harness(CellSpec(1, D("5.0181"), D("5.0363"), Side.BUY))
        short_cell = Harness(CellSpec(40, D("5.7272"), D("5.7454"), Side.SELL))
        for h in (long_cell, short_cell):
            e = h.entry()
            h.fill(e, "10")
            h.ledger.confirm_terminal(e, D("10"))
        _, (tp,) = long_cell.dispatch_tps(accept=False)
        req = long_cell.ledger.submit_request(tp, OrderTypePolicy.LIMIT)
        self.assertEqual((Side.SELL, D("10"), False), (req.side, req.amount, req.reduce_only))
        ep = risk.endpoints_from_ledgers(D("0"), [long_cell.ledger, short_cell.ledger])
        self.assertEqual((D("0"), D("-10")), (ep.P, ep.P_min))        # SELL TP at venue net 0 can go net short
        long_cell.ledger.record_transport(tp, TransportResult(TransportOutcome.ACCEPTED, exchange_order_id="t"))
        long_cell.fill(tp, "10")
        long_cell.ledger.confirm_terminal(tp, D("10"))
        ep = risk.endpoints_from_ledgers(D("0"), [long_cell.ledger, short_cell.ledger])
        self.assertEqual((D("-10"), D("10")), (ep.P, ep.gross_worst))  # venue short 10; only short cell unpaired
        self.assertTrue(long_cell.ledger.can_release(True).ok)
        self.assertFalse(short_cell.ledger.can_release(True).ok)

    def test_cap_violations_detect_drift(self):
        ep = risk.RiskEndpoints(P=D("1001"), P_min=D("1001"), P_max=D("1001"), gross_worst=D("1001"))
        self.assertEqual(2, len(risk.cap_violations(ep, LIMITS)))


class TestTpHeadroom(unittest.TestCase):
    def entries(self):
        return [
            OpenLeg(Side.BUY, D("10"), LegRole.ENTRY, OrderState.LIVE, key="near", cell_id=20, price=D("5.36")),
            OpenLeg(Side.BUY, D("10"), LegRole.ENTRY, OrderState.LIVE, key="far", cell_id=2, price=D("5.03")),
            OpenLeg(Side.BUY, D("10"), LegRole.ENTRY, OrderState.CANCEL_PENDING, key="inflight", cell_id=5,
                    price=D("5.09")),
            OpenLeg(Side.SELL, D("10"), LegRole.ENTRY, OrderState.LIVE, key="sell", cell_id=30, price=D("5.6")),
            OpenLeg(Side.BUY, D("10"), LegRole.TP, OrderState.LIVE, key="tp", cell_id=33, price=D("5.6")),
        ]

    def test_within_cap_submits(self):
        ep = risk.RiskEndpoints(P=D("0"), P_min=D("-10"), P_max=D("40"), gross_worst=D("40"))
        self.assertTrue(risk.plan_tp_headroom(ep, Side.BUY, D("10"), self.entries(), LIMITS, D("5.4")).ok)

    def test_buy_tp_cancels_farthest_same_side_entries_and_waits(self):
        limits = RiskLimits(max_abs_net_position=D("45"), max_gross_position=D("1000"))
        ep = risk.RiskEndpoints(P=D("0"), P_min=D("-10"), P_max=D("50"), gross_worst=D("40"))
        # excess = 50 + 10 - 45 = 15; in-flight 10 -> cancel one more, the farthest from mid.
        d = risk.plan_tp_headroom(ep, Side.BUY, D("10"), self.entries(), limits, D("5.4"))
        self.assertEqual((False, ("far",), True, False), (d.ok, d.cancel, d.wait, d.risk_blocked))

    def test_in_flight_cancels_suffice_no_new_cancel(self):
        limits = RiskLimits(max_abs_net_position=D("55"), max_gross_position=D("1000"))
        ep = risk.RiskEndpoints(P=D("0"), P_min=D("-10"), P_max=D("50"), gross_worst=D("40"))
        d = risk.plan_tp_headroom(ep, Side.BUY, D("10"), self.entries(), limits, D("5.4"))
        self.assertEqual(((), True), (d.cancel, d.wait))

    def test_submit_unknown_same_side_entry_is_pending_not_risk_blocked(self):
        # Review #3: an entry in SUBMIT_UNKNOWN may still resolve to LIVE (then cancellable) or a zero-fill
        # reject, so the TP waits instead of escalating to operator-required RISK_BLOCKED.
        limits = RiskLimits(max_abs_net_position=D("20"), max_gross_position=D("1000"))
        ep = risk.RiskEndpoints(P=D("0"), P_min=D("0"), P_max=D("20"), gross_worst=D("20"))
        unknown = [OpenLeg(Side.BUY, D("20"), LegRole.ENTRY, OrderState.SUBMIT_UNKNOWN, key="u", cell_id=3,
                           price=D("5.05"))]
        d = risk.plan_tp_headroom(ep, Side.BUY, D("10"), unknown, limits, D("5.4"))
        self.assertEqual((False, False, True, ()), (d.ok, d.risk_blocked, d.wait, d.cancel))
        # LIVE entries are still cancelled first; the unknown one is only waited for.
        mixed = unknown + [OpenLeg(Side.BUY, D("5"), LegRole.ENTRY, OrderState.LIVE, key="l", cell_id=4,
                                   price=D("5.07"))]
        d = risk.plan_tp_headroom(ep, Side.BUY, D("10"), mixed, limits, D("5.4"))
        self.assertEqual((False, ("l",), True), (d.risk_blocked, d.cancel, d.wait))
        # Confirmed position + non-entry orders alone above the cap -> RISK_BLOCKED.
        tp_only = [OpenLeg(Side.BUY, D("20"), LegRole.TP, OrderState.LIVE, key="t")]
        d = risk.plan_tp_headroom(ep, Side.BUY, D("10"), tp_only, limits, D("5.4"))
        self.assertTrue(d.risk_blocked)

    def test_confirmed_position_cannot_be_freed_risk_blocked(self):
        limits = RiskLimits(max_abs_net_position=D("20"), max_gross_position=D("1000"))
        ep = risk.RiskEndpoints(P=D("30"), P_min=D("20"), P_max=D("60"), gross_worst=D("40"))
        d = risk.plan_tp_headroom(ep, Side.BUY, D("10"), self.entries(), limits, D("5.4"))
        self.assertTrue(d.risk_blocked)
        self.assertEqual((), d.cancel)
        self.assertTrue(d.reason.startswith("RISK_BLOCKED"))


class TestMarginAdvisory(unittest.TestCase):
    def test_ac37_known_shortfall_warns_only(self):
        warning, blocks = risk.margin_advisory(D("100"), D("660"))
        self.assertFalse(blocks)
        self.assertIn("MARGIN_SHORTFALL", warning)
        self.assertEqual((None, False), risk.margin_advisory(D("3000"), D("660")))

    def test_ac37_unknown_or_malformed_blocks(self):
        for available, required in ((None, D("1")), (D("1"), None), (D("NaN"), D("1")), (D("-1"), D("1")),
                                    (100.0, D("1")), ("100", D("1")), (D("1"), D("Infinity"))):
            with self.subTest(available=available, required=required):
                warning, blocks = risk.margin_advisory(available, required)
                self.assertTrue(blocks)
                self.assertIn("MARGIN_UNKNOWN", warning)

    def test_required_margin_estimate(self):
        self.assertEqual(D("132"), risk.required_margin_estimate(D("-330"), D("220"), D("2"), D("5")))
        self.assertIsNone(risk.required_margin_estimate(D("-330"), D("220"), None, D("5")))
        self.assertIsNone(risk.required_margin_estimate(D("-330"), D("220"), D("2"), D("0")))

    def test_exposure_gate_unknown_data_blocks_margin_shortfall_only_warns(self):
        ok = risk.ExposureInputs(position_known=True, leverage_ok=True, position_mode_ok=True, market_active=True,
                                 rules=rules(), history_complete=True, account_identity_ok=True,
                                 data_age_s=D("3"), freshness_limit_s=D("10"),
                                 margin_available=D("10"), margin_required=D("100"))
        blockers, warning = risk.exposure_blockers(ok)
        self.assertEqual([], blockers)
        self.assertIn("MARGIN_SHORTFALL", warning)
        stale = risk.ExposureInputs(**{**ok.__dict__, "data_age_s": D("11"), "history_complete": None,
                                       "margin_available": None})
        blockers, warning = risk.exposure_blockers(stale)
        self.assertIsNone(warning)
        self.assertTrue(any(b.startswith("FRESHNESS:STALE") for b in blockers))
        self.assertIn("HISTORY_COMPLETE:UNKNOWN", blockers)
        self.assertTrue(any(b.startswith("MARGIN_UNKNOWN") for b in blockers))
        no_rules = risk.ExposureInputs(**{**ok.__dict__, "rules": None})
        self.assertIn("RULES_UNKNOWN", risk.exposure_blockers(no_rules)[0])


if __name__ == "__main__":
    unittest.main()
