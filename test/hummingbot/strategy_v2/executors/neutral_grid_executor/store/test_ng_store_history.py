"""History inbox, canonical dedupe, exact-ID attribution and durable conflicts (NG-HIST-001/002; persistence parts
of AC-09, AC-11, AC-19, AC-39, AC-40, AC-42)."""
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    LegRole,
    OrderState,
    OrderTypePolicy,
    Side,
    SubmitRequest,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    BootstrapError,
    EntryBlockedError,
    FillAllocation,
    InvalidTransitionError,
    trade_dedupe_key,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    ACCOUNT,
    BOOT_CUT_MS,
    DOMAIN,
    GRID_ID,
    Env,
    FakeTransport,
    complete_cycle,
    order_row,
    record_entry_intent,
    submit_via_protocol,
    tp_leg,
    trade_row,
)


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


@pytest.fixture
def store(env):
    opened = env.open()
    env.bootstrap(opened)
    return opened


@pytest.fixture
def live(store):
    intent = record_entry_intent(store, cell_id=3)
    submit_via_protocol(store, FakeTransport(), intent)
    leg = store.leg(intent.cid)
    return leg


def _apply(store, rows, **kwargs):
    with store.transaction() as tx:
        return store.apply_history_batch(tx, rows, **kwargs)


def _trade(leg, trade_id, size, **kwargs):
    kwargs.setdefault("exchange_order_id", f"9{leg.cid}")
    return trade_row(trade_id, leg.cid, leg.side, size, price=str(leg.price), **kwargs)


def test_history_requires_bootstrap_cut(env):
    store = env.open()
    with pytest.raises(BootstrapError):
        _apply(store, [trade_row("t", None, Side.BUY, "1")])


def test_identical_duplicates_and_reordering_are_idempotent(store, live):
    rows = [_trade(live, "t-1", "2"), _trade(live, "t-2", "3"), _trade(live, "t-3", "5")]
    first = _apply(store, [rows[2], rows[0]])
    second = _apply(store, rows[::-1] + rows)  # page overlap: boundary rows repeat, order differs
    assert len(first.new_fills) == 2 and len(second.new_fills) == 1 and second.duplicates == 5
    assert store.leg(live.cid).filled == Decimal("10")
    assert store.cycle(GRID_ID, 3, 1).entry_filled == Decimal("10")
    assert store.verify_ledger() == []


def test_self_trade_two_own_legs_are_two_facts(store):
    transport = FakeTransport()
    buy = record_entry_intent(store, cell_id=1)      # BUY cell
    sell = record_entry_intent(store, cell_id=50)    # SELL cell
    submit_via_protocol(store, transport, buy)
    submit_via_protocol(store, transport, sell)
    buy_leg, sell_leg = store.leg(buy.cid), store.leg(sell.cid)
    assert (buy_leg.side, sell_leg.side) == (Side.BUY, Side.SELL)
    result = _apply(store, [_trade(buy_leg, "self-1", "1"), _trade(sell_leg, "self-1", "1")])
    assert len(result.new_fills) == 2 and {f.own_side for f in result.new_fills} == {Side.BUY, Side.SELL}
    assert store.position_ledger().net == Decimal("0")


def test_same_key_different_payload_is_a_durable_conflict(env, store, live):
    _apply(store, [_trade(live, "t-1", "2")])
    changed = _trade(live, "t-1", "3")  # same canonical key, different size
    result = _apply(store, [changed])
    [conflict] = result.conflicts
    assert conflict.kind == "PAYLOAD_MISMATCH" and conflict.new
    assert store.leg(live.cid).filled == Decimal("2")  # quantity never guessed
    replay = _apply(store, [changed])
    assert [c.id for c in replay.conflicts] == [conflict.id] and not replay.conflicts[0].new
    store.close()
    reopened = env.open()
    assert [c.id for c in reopened.open_conflicts()] == [conflict.id]
    with pytest.raises(EntryBlockedError, match="history conflict"):
        record_entry_intent(reopened, cell_id=4)
    reopened.record_manual_reconciliation(None, "operator", "venue support confirmed size 2",
                                          {"ticket": "LIGHTER-9"}, resolved_conflict_ids=[conflict.id])
    assert reopened.open_conflicts() == []
    record_entry_intent(reopened, cell_id=4)


def test_overfill_and_side_or_id_mismatch_are_conflicts_not_fills(store, live):
    result = _apply(store, [
        _trade(live, "t-1", "11"),                                                    # more than requested
        trade_row("t-2", live.cid, Side.SELL if live.side == Side.BUY else Side.BUY, "1", price=str(live.price),
                  exchange_order_id=f"9{live.cid}"),                                  # wrong side for this CID
        _trade(live, "t-3", "1", exchange_order_id="555"),                            # other exchange order id
    ])
    assert sorted(c.kind for c in result.conflicts) == ["EXCHANGE_ID_MISMATCH", "OVERFILL", "SIDE_MISMATCH"]
    assert result.new_fills == [] and store.leg(live.cid).filled == Decimal("0")


def test_trade_cumulative_above_terminal_order_cumulative_is_conflict(store, live):
    _apply(store, [order_row(live.cid, live.side, live.price, live.amount, Decimal("4"), status="canceled",
                             order_id=f"9{live.cid}")])
    result = _apply(store, [_trade(live, "t-1", "4"), _trade(live, "t-2", "1")])
    assert [c.kind for c in result.conflicts] == ["CUMULATIVE_EXCEEDS_ORDER"]
    with pytest.raises(InvalidTransitionError, match="TERMINAL unproven"):
        with store.transaction() as tx:
            store.set_leg_state(tx, live.cid, OrderState.TERMINAL, reason="x")


def test_order_row_cumulative_regression_is_conflict(store, live):
    active = order_row(live.cid, live.side, live.price, live.amount, Decimal("4"), status="open",
                       order_id=f"9{live.cid}")
    with store.transaction() as tx:
        store.record_order_evidence(tx, active)
    with pytest.raises(InvalidTransitionError, match="regressed"):  # active snapshot going backwards
        with store.transaction() as tx:
            store.record_order_evidence(tx, order_row(live.cid, live.side, live.price, live.amount, Decimal("3"),
                                                      status="open", order_id=f"9{live.cid}"))
    _apply(store, [order_row(live.cid, live.side, live.price, live.amount, Decimal("4"), status="canceled",
                             order_id=f"9{live.cid}")])
    result = _apply(store, [order_row(live.cid, live.side, live.price, live.amount, Decimal("3"),
                                      status="canceled", order_id=f"9{live.cid}")])
    assert [c.kind for c in result.conflicts] == ["PAYLOAD_MISMATCH"]  # terminal rows are immutable facts
    assert store.order(live.cid).venue_filled == Decimal("4")


def test_terminal_requires_exact_final_row_and_equal_history_cumulative(store, live):
    _apply(store, [_trade(live, "t-1", "4")])
    with pytest.raises(InvalidTransitionError, match="no exact final venue order row"):
        with store.transaction() as tx:
            store.set_leg_state(tx, live.cid, OrderState.TERMINAL, reason="disappeared from active list")
    _apply(store, [order_row(live.cid, live.side, live.price, live.amount, Decimal("6"), status="canceled",
                             order_id=f"9{live.cid}")])
    with pytest.raises(InvalidTransitionError, match="!= terminal cumulative"):
        with store.transaction() as tx:
            store.set_leg_state(tx, live.cid, OrderState.TERMINAL, reason="history lag")
    _apply(store, [_trade(live, "t-2", "2")])
    with store.transaction() as tx:
        store.set_leg_state(tx, live.cid, OrderState.TERMINAL, reason="proven")
    assert store.reservations() == []  # terminal proof releases the reservation


def test_unowned_rows_are_unmatched_or_pre_cut_and_never_attributed_by_price(store, live):
    lookalike = trade_row("x-1", None, live.side, "10", price=str(live.price), ts=BOOT_CUT_MS + 5)
    result = _apply(store, [lookalike, trade_row("x-0", None, live.side, "10", price=str(live.price),
                                                 ts=BOOT_CUT_MS - 5)])
    assert [r.status for r in result.unmatched] == ["UNMATCHED"]
    assert [r.status for r in result.pre_cut] == ["PRE_CUT"]
    assert store.leg(live.cid).filled == Decimal("0")
    foreign = _apply(store, [trade_row("x-2", live.cid, live.side, "1", price=str(live.price),
                                       exchange_order_id=f"9{live.cid}", account=ACCOUNT + 1)])
    assert [c.kind for c in foreign.conflicts] == ["FOREIGN_SCOPE"]


def test_evidence_for_never_dispatched_intent_is_a_conflict(store):
    intent = record_entry_intent(store, cell_id=6)  # PENDING: transport provably not called
    leg = store.leg(intent.cid)
    result = _apply(store, [trade_row("c-1", intent.cid, leg.side, "1", price=str(leg.price))])
    assert [c.kind for c in result.conflicts] == ["EVIDENCE_FOR_UNDISPATCHED_INTENT"]
    assert store.leg(intent.cid).filled == Decimal("0")


def test_released_cycle_rejects_moves_and_extra_execution_is_a_conflict(store):
    transport = FakeTransport()
    entry_cid, tp_cid = complete_cycle(store, transport, cell_id=3)
    with pytest.raises(InvalidTransitionError):  # final states are final
        with store.transaction() as tx:
            store.set_leg_state(tx, tp_cid, OrderState.LIVE)
    entry = store.leg(entry_cid)
    # an execution beyond the fully filled entry cannot be attributed: overfill conflict, quantity not applied
    late = _apply(store, [trade_row("late-1", entry_cid, entry.side, "1", price=str(entry.price),
                                    exchange_order_id=f"9{entry_cid}")])
    assert [c.kind for c in late.conflicts] == ["OVERFILL"]
    assert store.leg(entry_cid).filled == Decimal("10") and store.leg(tp_cid).state == OrderState.TERMINAL


def test_late_fill_on_terminal_partial_leg_is_applied_to_old_cycle(store, live):
    _apply(store, [_trade(live, "t-1", "4"),
                   order_row(live.cid, live.side, live.price, live.amount, Decimal("4"), status="canceled",
                             order_id=f"9{live.cid}")])
    with store.transaction() as tx:
        store.set_leg_state(tx, live.cid, OrderState.TERMINAL, reason="proven partial cancel")
    result = _apply(store, [_trade(live, "t-late", "1", ts=BOOT_CUT_MS + 99_000)])
    assert [f.trade_id_str for f in result.late_fills] == ["t-late"]
    assert {c.kind for c in result.conflicts} == {"LATE_FILL", "CUMULATIVE_EXCEEDS_ORDER"}
    cycle = store.cycle(GRID_ID, 3, 1)
    assert cycle.entry_filled == Decimal("5") and cycle.late_evidence == 1
    assert "late evidence not acknowledged" in store.cycle_release_blockers(GRID_ID, 3, 1)
    with pytest.raises(EntryBlockedError):
        record_entry_intent(store, cell_id=4)


def test_ledger_transitions_callable_sees_only_new_facts(store, live):
    seen = []

    def transitions(tx, result):
        seen.append([f.trade_id_str for f in result.new_fills])
        if result.new_fills:
            store.set_leg_state(tx, live.cid, OrderState.LIVE, reason="history")

    rows = [_trade(live, "t-1", "2")]
    _apply(store, rows, ledger_transitions=transitions)
    _apply(store, rows, ledger_transitions=transitions)
    assert seen == [["t-1"], []]


def test_dedupe_key_is_the_canonical_tuple(live):
    row = _trade(live, "12345678901234567890", "1")
    assert trade_dedupe_key(DOMAIN, row) == (f'["TRADE","{DOMAIN}",{ACCOUNT},5,"12345678901234567890",'
                                             f'"{live.side.value}","9{live.cid}"]')


def test_aggregated_tp_fill_allocation_is_exact_and_idempotent(store):
    transport = FakeTransport()
    # an aggregated TP carries explicit per-cycle allocations (the split policy is WS-A's dust.aggregate); the
    # store keeps the allocation, withholds cycle credit until the fill is allocated and checks the arithmetic
    entry = record_entry_intent(store, cell_id=5)
    submit_via_protocol(store, transport, entry)
    leg = store.leg(entry.cid)
    _apply(store, [_trade(leg, "e-1", "10"),
                   order_row(leg.cid, leg.side, leg.price, leg.amount, Decimal("10"), order_id=f"9{leg.cid}")])
    cycle = store.cycle(GRID_ID, 5, 1)
    with store.transaction() as tx:
        cid = store.allocate_cid(tx, tp_leg(5))
        store.record_intent(tx, tp_leg(5), SubmitRequest(cid, cycle.tp_side, cycle.tp_price, Decimal("10"),
                                                         OrderTypePolicy.LIMIT),
                            allocations=[(GRID_ID, 5, 1, Decimal("10"))])
    tp = store.leg(cid)
    with store.transaction() as tx:
        store.mark_dispatching(tx, store.outbox_for_cid(cid)[0].id)
        store.record_transport_result(tx, cid, TransportResult(TransportOutcome.ACCEPTED, exchange_order_id="T1"))
    result = _apply(store, [trade_row("tp-1", cid, tp.side, "3", price=str(tp.price), exchange_order_id="T1")])
    assert store.cycle(GRID_ID, 5, 1).exit_filled == Decimal("0")  # not credited until allocated
    assert [f.trade_id_str for f in store.unallocated_fills()] == ["tp-1"]
    key = result.new_fills[0].dedupe_key
    with pytest.raises(ValueError):
        store.allocate_fill(None, key, [(GRID_ID, 5, 1, Decimal("2"))])
    allocation = FillAllocation(key, ((GRID_ID, 5, 1, Decimal("3")),))
    for _ in range(2):
        with store.transaction() as tx:
            store.apply_history_batch(tx, [], ledger_transitions=[allocation])
    assert store.cycle(GRID_ID, 5, 1).exit_filled == Decimal("3") and store.unallocated_fills() == []
    assert store.verify_ledger() == []
    assert tp.role == LegRole.TP
