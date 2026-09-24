"""Crash windows of the outbox/history protocol (AC-15, AC-16, AC-18, AC-19; NG-DB-002, NG-HIST-002).

Every "crash" drops the store like a dead process (connection closed without commit, locks released) and the
assertions are made on a NEW store opened from the file on disk. ``test_real_process_death_*`` do the same with
an actual child process killed by ``os._exit`` inside the transaction.
"""
import subprocess
import sys
import textwrap
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    LegRole,
    OrderState,
    Side,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    CursorUpdate,
    FaultHooks,
    InvalidTransitionError,
    LegStateChange,
    SimulatedCrash,
    StoreClosedError,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    BOOT_CUT_MS,
    GRID_ID,
    REPO_ROOT,
    Env,
    FakeTransport,
    entry_leg,
    order_row,
    record_entry_intent,
    submit_via_protocol,
    trade_row,
)


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


@pytest.fixture
def boot(env):
    store = env.open(fault_hooks=FaultHooks())
    env.bootstrap(store)
    return store


def test_ac15_crash_before_intent_commit_leaves_no_phantom_order(env, boot):
    transport = FakeTransport()
    boot.fault_hooks.arm("before_intent_commit")
    with pytest.raises(SimulatedCrash):
        intent = record_entry_intent(boot, cell_id=3)
        submit_via_protocol(boot, transport, intent)
    assert boot.crashed and transport.submits == []
    with pytest.raises(StoreClosedError):
        boot.engine()

    store = env.open()
    assert store.legs() == []
    assert store.unresolved_outbox() == []
    assert store.reservations() == []
    assert store.cid_for(entry_leg(3)) is None
    assert store.current_cycle(GRID_ID, 3) is None  # the cycle opened in the same transaction is gone too
    # the cell is free: a new attempt allocates and commits normally
    intent = record_entry_intent(store, cell_id=3)
    assert store.leg(intent.cid).state == OrderState.INTENT


def test_ac16_crash_after_intent_commit_keeps_same_cid_and_no_new_id(env, boot):
    transport = FakeTransport()
    boot.fault_hooks.arm("after_intent_commit")
    with pytest.raises(SimulatedCrash):
        record_entry_intent(boot, cell_id=3)
    assert transport.submits == []

    store = env.open()
    [pending] = store.unresolved_outbox()
    leg = store.leg(pending.cid)
    assert leg.identity == entry_leg(3) and leg.state == OrderState.INTENT
    assert pending.status == "PENDING" and not pending.transport_possibly_invoked
    assert [r.cid for r in store.reservations()] == [pending.cid]
    # restart asks for the leg's CID again: same CID, no new id
    with store.transaction() as tx:
        assert store.allocate_cid(tx, entry_leg(3)) == pending.cid
    assert len(store.legs()) == 1
    # the durable request is exactly what will be sent, with the same CID
    assert pending.request["client_order_id"] == pending.cid
    assert pending.request["amount"] == "10" and pending.request["reduce_only"] is False


def test_ac16_crash_after_dispatch_mark_is_ambiguous_not_absent(env, boot):
    transport = FakeTransport()
    intent = record_entry_intent(boot, cell_id=40)
    boot.fault_hooks.arm("after_dispatch_commit")
    with pytest.raises(SimulatedCrash):
        submit_via_protocol(boot, transport, intent)
    assert transport.submits == []  # died before transport, but the store cannot know that

    store = env.open()
    [entry] = store.unresolved_outbox()
    assert entry.cid == intent.cid and entry.status == "DISPATCHED" and entry.transport_possibly_invoked
    assert store.leg(intent.cid).state == OrderState.SUBMIT_UNKNOWN
    assert store.reservations()[0].cid == intent.cid  # reservation kept while unknown
    # evidence cannot declare absence: no zero-fill rejection without venue evidence, no new CID
    with store.transaction() as tx:
        assert store.allocate_cid(tx, entry_leg(40)) == intent.cid
    with pytest.raises(InvalidTransitionError):
        with store.transaction() as tx:
            store.set_leg_state(tx, intent.cid, OrderState.TERMINAL, reason="guess")


def test_ac18_crash_after_transport_before_result_commit_exactly_once_ledger(env, boot):
    transport = FakeTransport()
    intent = record_entry_intent(boot, cell_id=3)
    boot.fault_hooks.arm("after_transport_before_result_commit")
    with pytest.raises(SimulatedCrash):
        submit_via_protocol(boot, transport, intent)
    assert len(transport.submits) == 1 and transport.submits[0].client_order_id == intent.cid

    store = env.open()
    assert store.leg(intent.cid).state == OrderState.SUBMIT_UNKNOWN
    cell = store.cell(GRID_ID, 3)
    fill = trade_row("t-1", intent.cid, cell.entry_side, "10", price=str(cell.spec().entry_price),
                     exchange_order_id=f"9{intent.cid}")
    terminal = order_row(intent.cid, cell.entry_side, cell.spec().entry_price, Decimal("10"), Decimal("10"),
                         order_id=f"9{intent.cid}")
    for _ in range(3):  # history replay after restart is idempotent
        with store.transaction() as tx:
            store.apply_history_batch(tx, [fill, terminal])
    leg = store.leg(intent.cid)
    assert leg.filled == Decimal("10") and len(store.fills(intent.cid)) == 1
    assert store.cycle(GRID_ID, 3, 1).entry_filled == Decimal("10")
    with store.transaction() as tx:
        store.set_leg_state(tx, intent.cid, OrderState.TERMINAL, reason="terminal row + fills")
    assert store.verify_ledger() == []
    assert store.position_ledger().net == Decimal("10") * (1 if cell.entry_side == Side.BUY else -1)


def _history_batches(store, cid, side, price):
    first = [trade_row("t-1", cid, side, "4", price=price, ts=BOOT_CUT_MS + 5_000)]
    second = [trade_row("t-1", cid, side, "4", price=price, ts=BOOT_CUT_MS + 5_000),
              trade_row("t-2", cid, side, "3", price=price, ts=BOOT_CUT_MS + 6_000),
              trade_row("t-3", cid, side, "2", price=price, ts=BOOT_CUT_MS + 7_000)]
    return first, second


@pytest.mark.parametrize("point", ["before_cursor_commit", "mid_history_batch", "before_commit"])
def test_ac19_crash_during_history_batch_rolls_back_and_replays_idempotently(env, boot, point):
    transport = FakeTransport()
    intent = record_entry_intent(boot, cell_id=3)
    submit_via_protocol(boot, transport, intent)
    cell = boot.cell(GRID_ID, 3)
    first, second = _history_batches(boot, intent.cid, cell.entry_side, str(cell.spec().entry_price))
    with boot.transaction() as tx:
        boot.apply_history_batch(tx, first, [CursorUpdate("TRADES", cursor="c-1", high_water="t-1",
                                                          high_water_ts_ms=BOOT_CUT_MS + 5_000)])
    boot.fault_hooks.arm(point)
    with pytest.raises(SimulatedCrash):
        with boot.transaction() as tx:
            boot.apply_history_batch(
                tx, second, [CursorUpdate("TRADES", cursor="c-2", high_water="t-3", high_water_ts_ms=BOOT_CUT_MS + 7_000)],
                ledger_transitions=[LegStateChange(intent.cid, OrderState.LIVE, "seen in history")])

    store = env.open()
    # nothing of the crashed batch is visible: fill, inbox, dedupe, cursor all rolled back together
    assert store.leg(intent.cid).filled == Decimal("4")
    assert [f.trade_id_str for f in store.fills()] == ["t-1"]
    assert store.cursor("TRADES").cursor == "c-1" and store.cursor("TRADES").high_water == "t-1"
    # replaying the crashed batch (twice) applies each fill exactly once
    for _ in range(2):
        with store.transaction() as tx:
            result = store.apply_history_batch(tx, second, [CursorUpdate("TRADES", cursor="c-2", high_water="t-3",
                                                                         high_water_ts_ms=BOOT_CUT_MS + 7_000)])
    assert result.duplicates == 3 and result.new_fills == []
    assert store.leg(intent.cid).filled == Decimal("9")
    assert sorted(f.trade_id_str for f in store.fills()) == ["t-1", "t-2", "t-3"]
    assert store.cursor("TRADES").cursor == "c-2"
    assert store.verify_ledger() == []


def test_crash_after_history_commit_is_durable(env, boot):
    intent = record_entry_intent(boot, cell_id=50)
    submit_via_protocol(boot, FakeTransport(), intent)
    cell = boot.cell(GRID_ID, 50)
    boot.fault_hooks.arm("after_history_commit")
    with pytest.raises(SimulatedCrash):
        with boot.transaction() as tx:
            boot.apply_history_batch(tx, [trade_row("t-9", intent.cid, cell.entry_side, "10",
                                                    price=str(cell.spec().entry_price))],
                                     [CursorUpdate("TRADES", cursor="c-9")])
    store = env.open()
    assert store.leg(intent.cid).filled == Decimal("10") and store.cursor("TRADES").cursor == "c-9"


def test_cursor_high_water_never_moves_backwards(env, boot):
    with boot.transaction() as tx:
        boot.update_cursors(tx, [CursorUpdate("TRADES", high_water_ts_ms=BOOT_CUT_MS + 10)])
    with pytest.raises(InvalidTransitionError):
        with boot.transaction() as tx:
            boot.update_cursors(tx, [CursorUpdate("TRADES", high_water_ts_ms=BOOT_CUT_MS + 5)])
    assert boot.cursor("TRADES").high_water_ts_ms == BOOT_CUT_MS + 10


def test_transport_result_requires_committed_dispatch_mark(boot):
    intent = record_entry_intent(boot, cell_id=3)
    with pytest.raises(InvalidTransitionError, match="never marked dispatching"):
        with boot.transaction() as tx:
            boot.record_transport_result(tx, intent.cid, TransportResult(TransportOutcome.ACCEPTED))
    # a proven pre-send validation failure may release the unsent intent (NG-DB-005)
    with boot.transaction() as tx:
        boot.record_transport_result(tx, intent.cid, TransportResult(TransportOutcome.NOT_SENT, "min notional"))
    assert boot.leg(intent.cid).state == OrderState.REJECTED_UNSENT
    assert boot.reservations() == []


def test_unknown_submit_keeps_reservation_and_same_cid_resend_only_explicit(boot):
    transport = FakeTransport()
    transport.results.append(TransportResult(TransportOutcome.UNKNOWN, "timeout"))
    intent = record_entry_intent(boot, cell_id=3)
    submit_via_protocol(boot, transport, intent)
    assert boot.leg(intent.cid).state == OrderState.SUBMIT_UNKNOWN
    assert [r.cid for r in boot.reservations()] == [intent.cid]
    [entry] = boot.unresolved_outbox()
    with pytest.raises(InvalidTransitionError):
        with boot.transaction() as tx:
            boot.mark_dispatching(tx, entry.id)  # no blind retry
    with boot.transaction() as tx:
        again = boot.mark_dispatching(tx, entry.id, resend_same_cid=True)
    assert again.attempts == 2 and again.cid == intent.cid


def test_definitive_zero_fill_reject_releases_but_timeout_does_not(boot):
    transport = FakeTransport()
    transport.results.append(TransportResult(TransportOutcome.DEFINITIVE_REJECT_ZERO_FILL, "post-only would cross"))
    intent = record_entry_intent(boot, cell_id=3)
    submit_via_protocol(boot, transport, intent)
    assert boot.leg(intent.cid).state == OrderState.REJECTED_ZERO_FILL
    assert boot.reservations() == []
    # a new revision (not a new cycle) may now be recorded; it gets a distinct CID
    with boot.transaction() as tx:
        cell = boot.cell(GRID_ID, 3)
        second = boot.prepare_submit(tx, entry_leg(3, revision=1), side=cell.entry_side,
                                     price=cell.spec().entry_price, amount=Decimal("10"),
                                     order_type=intent.request.order_type)
    assert second.cid != intent.cid


def test_cancel_ack_is_not_terminal_and_keeps_reservation(boot):
    transport = FakeTransport()
    intent = record_entry_intent(boot, cell_id=3)
    submit_via_protocol(boot, transport, intent)
    with boot.transaction() as tx:
        cancel = boot.record_cancel_intent(tx, intent.cid, "out of bounds")
    with boot.transaction() as tx:
        boot.mark_dispatching(tx, cancel.id)
    with boot.transaction() as tx:
        boot.record_transport_result(tx, intent.cid, transport.cancel(intent.cid), kind="CANCEL")
    assert boot.leg(intent.cid).state == OrderState.TERMINAL_UNKNOWN
    assert [r.cid for r in boot.reservations()] == [intent.cid]  # AC-21: full remainder reserved until proof
    with pytest.raises(InvalidTransitionError):
        with boot.transaction() as tx:
            boot.close_cycle(tx, GRID_ID, 3, 1, "cancelled")


_CHILD = textwrap.dedent("""
    import os, sys
    from decimal import Decimal
    sys.path.insert(0, {root!r})
    from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
        Env, record_entry_intent)
    from hummingbot.strategy_v2.executors.neutral_grid_executor.store import FaultHooks
    import pathlib
    env = Env(pathlib.Path({tmp!r}))
    hooks = FaultHooks()
    store = env.open(fault_hooks=hooks)
    hooks.arm({point!r}, action=lambda point: os._exit(17))
    record_entry_intent(store, cell_id=3)
    os._exit(0)
""")


@pytest.mark.parametrize("point,expect_intent", [("before_intent_commit", False), ("after_intent_commit", True)])
def test_real_process_death_around_intent_commit(env, boot, point, expect_intent):
    boot.close()
    code = _CHILD.format(root=str(REPO_ROOT), tmp=str(env.tmp), point=point)
    proc = subprocess.run([sys.executable, "-c", code], cwd=str(REPO_ROOT), capture_output=True, text=True,
                          timeout=60)
    assert proc.returncode == 17, proc.stderr
    store = env.open()  # the dead child's flock was released by the OS
    legs = store.legs()
    if expect_intent:
        assert len(legs) == 1 and legs[0].identity == entry_leg(3) and legs[0].role == LegRole.ENTRY
        assert store.unresolved_outbox()[0].status == "PENDING"
    else:
        assert legs == [] and store.unresolved_outbox() == [] and store.current_cycle(GRID_ID, 3) is None
