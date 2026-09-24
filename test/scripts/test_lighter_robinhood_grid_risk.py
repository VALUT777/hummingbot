import json
from decimal import Decimal

import pytest

from scripts.lighter_robinhood_grid_risk import ExposureEpoch, IntentJournal, JournalError, ReservationError


def test_reservations_bound_both_position_edges_and_are_atomic():
    epoch = ExposureEpoch(Decimal("995"), Decimal("1000"), Decimal("0.01"))

    epoch.reserve("buy-ok", "BUY", Decimal("5.009"))
    with pytest.raises(ReservationError):
        epoch.reserve("buy-over", "BUY", Decimal("0.01"))
    epoch.reserve("sell-ok", "SELL", Decimal("10"))

    assert epoch.reserved_buy == Decimal("5.00")
    assert epoch.reserved_sell == Decimal("10.00")
    assert set(epoch.orders) == {"buy-ok", "sell-ok"}


@pytest.mark.parametrize(
    "args",
    [
        (Decimal("NaN"), Decimal("1000"), Decimal("0.01")),
        (Decimal("Infinity"), Decimal("1000"), Decimal("0.01")),
        (Decimal("1000.01"), Decimal("1000"), Decimal("0.01")),
        (Decimal("0"), Decimal("-1"), Decimal("0.01")),
        (Decimal("0"), Decimal("0"), Decimal("0.01")),
        (Decimal("0"), Decimal("1000"), Decimal("-0.01")),
        (Decimal("0.001"), Decimal("1000"), Decimal("0.01")),
    ],
)
def test_invalid_baseline_or_cap_inputs_are_rejected(args):
    with pytest.raises(ValueError):
        ExposureEpoch(*args)


@pytest.mark.parametrize("amount", ["NaN", "Infinity", "-1", "0"])
def test_invalid_reservation_amounts_are_rejected(amount):
    epoch = ExposureEpoch(Decimal("0"), Decimal("1000"), Decimal("0.01"))
    with pytest.raises(ReservationError):
        epoch.reserve("id", "BUY", Decimal(amount))


def test_duplicate_fills_are_idempotent_and_unknown_ids_fail_closed():
    epoch = ExposureEpoch(Decimal("0"), Decimal("1000"), Decimal("0.01"))
    epoch.reserve("bid", "BUY", Decimal("10"))

    assert epoch.record_fill("bid", "trade-1", Decimal("4"))
    assert epoch.record_fill("bid", "trade-1", Decimal("4"))
    assert epoch.signed_fills == Decimal("4")
    assert not epoch.record_fill("missing", "trade-2", Decimal("1"))
    assert epoch.invalid_reason == "fill for unknown order missing"


def test_conflicting_duplicate_fill_permanently_fails_closed():
    epoch = ExposureEpoch(Decimal("0"), Decimal("1000"), Decimal("0.01"))
    epoch.reserve("bid", "BUY", Decimal("10"))
    epoch.record_fill("bid", "trade-1", Decimal("4"))

    assert not epoch.record_fill("bid", "trade-1", Decimal("5"))
    assert epoch.invalid_reason == "conflicting duplicate fill trade-1"


def test_non_power_of_ten_increment_quantizes_to_an_actual_multiple():
    epoch = ExposureEpoch(Decimal("0"), Decimal("1000"), Decimal("0.05"))

    assert epoch.reserve("bid", "BUY", Decimal("1.03")) == Decimal("1.00")


def test_authoritative_terminal_cumulative_fill_covers_missed_callback_without_double_counting():
    epoch = ExposureEpoch(Decimal("0"), Decimal("1000"), Decimal("0.01"))
    epoch.reserve("bid", "BUY", Decimal("10"))
    epoch.record_fill("bid", "seen", Decimal("3"))

    assert epoch.set_authoritative_cumulative("bid", Decimal("5"))
    assert epoch.signed_fills == Decimal("5")
    assert epoch.record_fill("bid", "late", Decimal("2"))
    assert epoch.signed_fills == Decimal("5")


def test_partial_fill_cancel_late_fill_requires_fresh_consistent_observations():
    epoch = ExposureEpoch(Decimal("100"), Decimal("1000"), Decimal("0.01"))
    epoch.reserve("bid", "BUY", Decimal("10"))
    epoch.record_fill("bid", "fill-1", Decimal("3"))
    epoch.mark_terminal("bid")

    assert not epoch.observe("v1", Decimal("100"), active_order_ids=set(), pending_submissions=False)
    assert not epoch.observe("v2", Decimal("103"), active_order_ids=set(), pending_submissions=False)
    assert not epoch.observe("v2", Decimal("103"), active_order_ids=set(), pending_submissions=False)

    epoch.record_fill("bid", "fill-2", Decimal("2"))
    assert not epoch.observe("v3", Decimal("105"), active_order_ids=set(), pending_submissions=False)
    assert epoch.observe("v4", Decimal("105"), active_order_ids=set(), pending_submissions=False)


def test_off_increment_account_position_never_counts_as_consistent():
    epoch = ExposureEpoch(Decimal("990"), Decimal("1000"), Decimal("0.01"))
    epoch.reserve("bid", "BUY", Decimal("10"))
    epoch.set_authoritative_cumulative("bid", Decimal("10"))
    epoch.mark_terminal("bid")

    assert not epoch.observe("v1", Decimal("999.999"), set(), False)
    assert not epoch.observe("v2", Decimal("999.999"), set(), False)
    assert not epoch.observe("v3", Decimal("1000"), set(), False)
    assert epoch.observe("v4", Decimal("1000"), set(), False)


def test_accepted_submission_is_not_terminal_and_full_quantity_stays_reserved():
    epoch = ExposureEpoch(Decimal("0"), Decimal("1000"), Decimal("0.01"))
    epoch.reserve("ask", "SELL", Decimal("10"))
    epoch.mark_accepted("ask")
    epoch.record_fill("ask", "fill", Decimal("2"))

    assert epoch.reserved_sell == Decimal("10")
    assert not epoch.observe("v1", Decimal("-2"), active_order_ids=set(), pending_submissions=False)


def test_interleaved_reservations_can_never_exceed_either_bound():
    epoch = ExposureEpoch(Decimal("990"), Decimal("1000"), Decimal("0.01"))
    for index, side in enumerate(["SELL", "BUY", "SELL", "BUY"]):
        try:
            epoch.reserve(f"order-{index}", side, Decimal("10"))
        except ReservationError:
            pass

        assert epoch.baseline + epoch.reserved_buy <= epoch.cap
        assert epoch.baseline - epoch.reserved_sell >= -epoch.cap


def test_provisional_reservation_can_be_bound_to_returned_client_order_id():
    epoch = ExposureEpoch(Decimal("0"), Decimal("1000"), Decimal("0.01"))
    epoch.reserve("pending-1", "BUY", Decimal("10"))

    epoch.rename_order("pending-1", "client-1")

    assert "pending-1" not in epoch.orders
    assert epoch.orders["client-1"].amount == Decimal("10")


def test_intent_journal_is_durable_before_and_after_client_id_binding(tmp_path):
    path = tmp_path / "grid-intents.json"
    journal = IntentJournal(path)

    journal.reserve(
        "pending-1", "BUY", Decimal("10"), Decimal("990"),
        "lighter_perpetual_robinhood", "LIT-USDG", 724450,
    )
    assert IntentJournal(path).entries["pending-1"]["client_order_id"] is None

    journal.bind_client_order_id("pending-1", "123")
    restored = IntentJournal(path)
    assert restored.entries["pending-1"]["client_order_id"] == "123"
    assert restored.entries["pending-1"]["amount"] == "10"
    assert restored.entries["pending-1"]["account_index"] == 724450

    restored.clear()
    assert IntentJournal(path).entries == {}


def test_corrupt_intent_journal_fails_closed(tmp_path):
    path = tmp_path / "grid-intents.json"
    path.write_text("not-json")
    with pytest.raises(JournalError):
        IntentJournal(path)


def test_intent_journal_rejects_duplicate_bound_client_ids_on_bind_and_load(tmp_path):
    path = tmp_path / "grid-intents.json"
    journal = IntentJournal(path)
    for provisional_id, side in (("pending-1", "BUY"), ("pending-2", "SELL")):
        journal.reserve(
            provisional_id, side, Decimal("10"), Decimal("0"),
            "lighter_perpetual_robinhood", "LIT-USDG", 724450,
        )
    journal.bind_client_order_id("pending-1", "123")
    with pytest.raises(JournalError):
        journal.bind_client_order_id("pending-2", "123")

    duplicate = dict(journal.entries["pending-1"])
    document = {"version": 2, "entries": {"pending-1": duplicate, "pending-2": duplicate}}
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(JournalError):
        IntentJournal(path)
