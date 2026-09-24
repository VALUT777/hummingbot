"""Durable 48-bit client order id map (AC-43 durable part, NG-HIST-004)."""
import sqlite3
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    MAX_CLIENT_ORDER_ID,
    LegIdentity,
    LegRole,
    OrderTypePolicy,
    Side,
    SubmitRequest,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    CID_EPOCH_SHIFT,
    CidCollisionError,
    CidExhaustedError,
    InvalidTransitionError,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    BOOT_CUT_MS,
    GRID_ID,
    Env,
    FakeTransport,
    complete_cycle,
    entry_leg,
    record_entry_intent,
    trade_row,
)

CID_MAX_SEQ = (1 << CID_EPOCH_SHIFT) - 1


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


@pytest.fixture
def store(env):
    opened = env.open()
    env.bootstrap(opened)
    return opened


def _open_cycle(store, cell_id):
    with store.transaction() as tx:
        return store.open_cycle(tx, GRID_ID, cell_id)


def _next_candidate(store):
    raw = store._conn.raw.execute("SELECT cid_epoch, next_seq FROM cid_state").fetchone()
    return (raw[0] << CID_EPOCH_SHIFT) | raw[1]


def test_same_identity_same_cid_across_restart_and_full_identity_mapping(env, store):
    _open_cycle(store, 7)
    leg = LegIdentity(GRID_ID, 7, 1, LegRole.TP, 3)
    with store.transaction() as tx:
        cid = store.allocate_cid(tx, leg)
    store.close()
    reopened = env.open()
    with reopened.transaction() as tx:
        assert reopened.allocate_cid(tx, leg) == cid
    assert reopened.identity_for_cid(cid) == leg
    assert reopened.cid_for(leg) == cid


def test_cids_are_unique_monotonic_and_never_reused_after_restart(env, store):
    transport = FakeTransport()
    first_entry, first_tp = complete_cycle(store, transport, cell_id=2, tag="g1")
    store.close()
    reopened = env.open()
    second = record_entry_intent(reopened, cell_id=2)  # generation 2 of the same cell
    assert reopened.leg(second.cid).generation == 2
    assert second.cid > max(first_entry, first_tp)
    all_cids = [row[0] for row in reopened._conn.raw.execute("SELECT cid FROM cid_map")]
    assert len(all_cids) == len(set(all_cids)) == 3


def test_exhaustion_fails_closed_without_consuming_state(env, store):
    _open_cycle(store, 1)
    _open_cycle(store, 2)
    store.close()
    raw = sqlite3.connect(str(env.db))
    raw.execute("UPDATE cid_state SET next_seq = ?", (CID_MAX_SEQ,))
    raw.commit()
    raw.close()
    reopened = env.open()
    with reopened.transaction() as tx:
        last = reopened.allocate_cid(tx, entry_leg(1))
    assert last == (1 << CID_EPOCH_SHIFT) | CID_MAX_SEQ and last <= MAX_CLIENT_ORDER_ID
    with pytest.raises(CidExhaustedError):
        with reopened.transaction() as tx:
            reopened.allocate_cid(tx, entry_leg(2))
    assert reopened.cid_for(entry_leg(2)) is None


def test_collision_with_foreign_cid_fails_closed_until_retired(store):
    _open_cycle(store, 1)
    candidate = _next_candidate(store)
    with store.transaction() as tx:
        store.note_foreign_cids(tx, [candidate], "active_orders")
    with pytest.raises(CidCollisionError) as caught:
        with store.transaction() as tx:
            store.allocate_cid(tx, entry_leg(1))
    assert caught.value.cid == candidate and store.cid_for(entry_leg(1)) is None
    store.retire_cid(None, candidate, "operator", "manual order uses this client id")
    with store.transaction() as tx:
        cid = store.allocate_cid(tx, entry_leg(1))
    assert cid == candidate + 1
    assert store.audit_events("cid_retired")[0].payload["cid"] == candidate


def test_collision_from_history_evidence_and_caller_check(store):
    _open_cycle(store, 1)
    candidate = _next_candidate(store)
    with store.transaction() as tx:  # a manual order's trade on the account carries the next candidate CID
        result = store.apply_history_batch(tx, [trade_row("m-1", candidate, Side.BUY, "1",
                                                          ts=BOOT_CUT_MS + 50_000)])
    assert [r.status for r in result.unmatched] == ["UNMATCHED"]
    with pytest.raises(CidCollisionError):
        with store.transaction() as tx:
            store.allocate_cid(tx, entry_leg(1))
    store.retire_cid(None, candidate, "operator", "seen in history")
    with pytest.raises(CidCollisionError):
        with store.transaction() as tx:
            store.allocate_cid(tx, entry_leg(1), is_foreign_cid=lambda cid: cid == candidate + 1)


def test_tampered_counter_colliding_with_own_map_fails_closed(env, store):
    record_entry_intent(store, cell_id=1)
    _open_cycle(store, 2)
    store.close()
    raw = sqlite3.connect(str(env.db))
    raw.execute("UPDATE cid_state SET next_seq = 1")
    raw.commit()
    raw.close()
    reopened = env.open()
    with pytest.raises(CidCollisionError, match="integrity"):
        with reopened.transaction() as tx:
            reopened.allocate_cid(tx, entry_leg(2))


def test_intent_must_use_the_cid_mapped_to_its_leg(store):
    _open_cycle(store, 1)
    _open_cycle(store, 2)
    with store.transaction() as tx:
        cid_1 = store.allocate_cid(tx, entry_leg(1))
    cell = store.cell(GRID_ID, 2)
    wrong = SubmitRequest(client_order_id=cid_1, side=cell.entry_side, price=cell.spec().entry_price,
                          amount=Decimal("10"), order_type=OrderTypePolicy.LIMIT_MAKER)
    with pytest.raises(InvalidTransitionError, match="not allocated"):
        with store.transaction() as tx:
            store.record_intent(tx, entry_leg(2), wrong)
    too_big = SubmitRequest(client_order_id=MAX_CLIENT_ORDER_ID + 1, side=cell.entry_side,
                            price=cell.spec().entry_price, amount=Decimal("10"),
                            order_type=OrderTypePolicy.LIMIT_MAKER)
    with pytest.raises(CidExhaustedError):
        with store.transaction() as tx:
            store.record_intent(tx, entry_leg(2), too_big)


def test_no_cid_for_closed_or_missing_cycle(store):
    with pytest.raises(KeyError):
        with store.transaction() as tx:
            store.allocate_cid(tx, entry_leg(9))
    transport = FakeTransport()
    complete_cycle(store, transport, cell_id=3)
    with pytest.raises(InvalidTransitionError, match="not open"):
        with store.transaction() as tx:
            store.allocate_cid(tx, LegIdentity(GRID_ID, 3, 1, LegRole.TP, 5))
