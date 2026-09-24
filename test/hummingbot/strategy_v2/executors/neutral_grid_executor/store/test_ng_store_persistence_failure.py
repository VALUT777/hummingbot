"""Disk full / read-only / commit failure (AC-55, NG-DB-004): no submit or cancel leaves without a committed
intent, nothing is half-applied, the failure is visible, known live orders stay reserved.

Disk-full and read-only faults are produced by SQLite itself (``max_page_count`` cap -> ``SQLITE_FULL``,
``query_only`` / a 0444 database file -> ``SQLITE_READONLY``); commit/statement I/O errors are injected through
the store's connection wrapper."""
import os
import sqlite3
import stat
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import OrderState
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    FaultHooks,
    PersistenceError,
    StoreIntegrityError,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    GRID_ID,
    Env,
    FakeTransport,
    record_entry_intent,
    submit_via_protocol,
)


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


@pytest.fixture
def hooks():
    return FaultHooks()


@pytest.fixture
def store(env, hooks):
    opened = env.open(fault_hooks=hooks)
    env.bootstrap(opened)
    return opened


def _assert_cell_untouched(store, cell_id):
    assert store.current_cycle(GRID_ID, cell_id) is None
    assert store.legs(cell_id=cell_id) == []


def test_real_disk_full_rolls_back_the_whole_intent_and_nothing_is_sent(env, hooks, store):
    transport = FakeTransport()
    live = record_entry_intent(store, cell_id=0)
    submit_via_protocol(store, transport, live)
    hooks.simulate_disk_full()
    committed, failed_cell = [], None
    for cell_id in range(1, 55):
        try:
            intent = record_entry_intent(store, cell_id=cell_id)
        except PersistenceError as exc:
            failed_cell = cell_id
            assert "SQLITE_FULL" in str(exc)
            break
        committed.append(intent)
        submit_via_protocol(store, transport, intent)  # may itself fail -> then transport is not called
    assert failed_cell is not None, "the page cap never produced SQLITE_FULL"
    assert "SQLITE_FULL" in store.degraded_reason
    sent = {request.client_order_id for request in transport.submits}
    dispatched = {o.cid for o in store.unresolved_outbox() if o.status == "DISPATCHED"}
    assert sent <= {live.cid} | {i.cid for i in committed}
    assert not dispatched  # every dispatch mark that committed was followed by a recorded result
    store.close()

    hooks.restore_storage()
    reopened = env.open()
    _assert_cell_untouched(reopened, failed_cell)
    assert reopened.leg(live.cid).state == OrderState.LIVE
    assert live.cid in {r.cid for r in reopened.reservations()}  # the known live order stays reserved
    assert reopened.verify_ledger() == []


def test_read_only_storage_blocks_submit_and_cancel_and_keeps_live_reservation(env, hooks, store):
    transport = FakeTransport()
    live = record_entry_intent(store, cell_id=0)
    submit_via_protocol(store, transport, live)
    hooks.simulate_read_only()
    with pytest.raises(PersistenceError, match="SQLITE_READONLY"):
        record_entry_intent(store, cell_id=1)
    with pytest.raises(PersistenceError):
        with store.transaction() as tx:
            store.record_cancel_intent(tx, live.cid, "stop")
    assert len(transport.submits) == 1 and transport.cancels == []
    assert "SQLITE_READONLY" in store.degraded_reason
    assert any("persistence degraded" in b for b in store.entry_blockers())
    store.close()
    hooks.restore_storage()
    reopened = env.open()
    assert reopened.leg(live.cid).state == OrderState.LIVE
    assert [r.cid for r in reopened.reservations()] == [live.cid]
    assert reopened.outbox_for_cid(live.cid)[-1].kind == "SUBMIT"  # no cancel intent was persisted
    _assert_cell_untouched(reopened, 1)


def test_injected_commit_io_error_applies_nothing(env, hooks, store):
    hooks.fail_commit(sqlite3.OperationalError("disk I/O error"))
    with pytest.raises(PersistenceError, match="commit"):
        record_entry_intent(store, cell_id=3)
    assert store.is_degraded
    store.close()
    _assert_cell_untouched(env.open(), 3)


def test_failure_of_one_statement_rolls_back_earlier_statements(env, hooks, store):
    hooks.fail_statement("INSERT INTO reservations", sqlite3.OperationalError("disk I/O error"))
    with pytest.raises(PersistenceError):
        record_entry_intent(store, cell_id=3)
    store.close()
    reopened = env.open()
    _assert_cell_untouched(reopened, 3)
    assert reopened.unresolved_outbox() == []


def test_swallowed_statement_error_still_prevents_commit(env, hooks, store):
    hooks.fail_statement("INSERT INTO engine_kv", sqlite3.OperationalError("disk I/O error"))
    with pytest.raises(PersistenceError, match="failed statement"):
        with store.transaction() as tx:
            store.open_cycle(tx, GRID_ID, 4)
            try:
                store.kv_set(tx, "k", 1)
            except PersistenceError:
                pass  # engine bug: ignoring the failure must not commit a partial transaction
    store.close()
    _assert_cell_untouched(env.open(), 4)


def test_degraded_store_refuses_new_submits_until_audited_clear(env, hooks, store):
    pending = record_entry_intent(store, cell_id=2)
    hooks.fail_commit(sqlite3.OperationalError("disk I/O error"))
    with pytest.raises(PersistenceError):
        record_entry_intent(store, cell_id=3)
    # storage works again, but the engine has not acknowledged the failure
    with pytest.raises(PersistenceError, match="degraded"):
        record_entry_intent(store, cell_id=3)
    with pytest.raises(PersistenceError, match="degraded"):
        with store.transaction() as tx:
            store.mark_dispatching(tx, pending.outbox_id)
    store.clear_degraded("engine", "storage recovered, restart reconciliation done")
    assert store.degraded_reason is None
    assert store.audit_events("degraded_cleared")[0].payload["previous_reason"].startswith("commit")
    record_entry_intent(store, cell_id=3)


def test_constraint_violation_is_persistence_error_without_side_effects(env, store):
    intent = record_entry_intent(store, cell_id=5)
    with pytest.raises(StoreIntegrityError):
        with store.transaction():
            store._x("INSERT INTO cid_map(cid, grid_id, cell_id, generation, role, revision, allocated_at_ms) "
                     "VALUES (?, 'g', 0, 1, 'ENTRY', 0, 0)", (intent.cid,))
    assert store.leg(intent.cid).state == OrderState.INTENT


def test_read_only_database_file_on_restart_fails_closed(env, store):
    store.close()
    mode = env.db.stat().st_mode
    try:
        os.chmod(env.db, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        with pytest.raises(PersistenceError):
            env.open()
    finally:
        # SQLite creates -wal/-shm with the database file's permissions: the operator fixes all of them
        for path in (env.db, env.db.with_name(env.db.name + "-wal"), env.db.with_name(env.db.name + "-shm")):
            if path.exists():
                os.chmod(path, mode)
    assert env.open().engine().initial_baseline == Decimal("0")
