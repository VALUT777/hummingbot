"""Missing / corrupt / replaced database with evidence of a previous run (AC-54, NG-DB-004).

The store must never turn such a situation into a fresh bootstrap and never adopt the current position as B."""
import hashlib
import shutil
import sqlite3
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    CID_EPOCH_SHIFT,
    BootstrapError,
    EntryBlockedError,
    ManualRecovery,
    NeutralGridStore,
    PriorRunEvidenceError,
    StoreCorruptError,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    IDENTITY,
    Env,
    FakeTransport,
    record_entry_intent,
    submit_via_protocol,
)


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


def _used_store(env: Env) -> int:
    """A previous engine that bootstrapped and sent one order; returns that order's CID."""
    store = env.open()
    env.bootstrap(store, baseline=Decimal("330"))
    intent = record_entry_intent(store, cell_id=1)
    submit_via_protocol(store, FakeTransport(), intent)
    store.close()
    return intent.cid


def _digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _remove_db(env: Env):
    for suffix in ("", "-wal", "-shm"):
        path = env.db.with_name(env.db.name + suffix)
        if path.exists():
            path.unlink()


def test_missing_database_next_to_marker_is_not_recreated(env):
    _used_store(env)
    _remove_db(env)
    with pytest.raises(PriorRunEvidenceError) as caught:
        env.open(create=True)
    assert any("marker" in item for item in caught.value.evidence)
    assert not env.db.exists()


def test_wiped_data_directory_still_blocked_by_host_marker(env):
    _used_store(env)
    shutil.rmtree(env.db.parent)
    with pytest.raises(PriorRunEvidenceError):
        env.open(create=True)
    assert not env.db.exists()


@pytest.mark.parametrize("callback_result", [True, ["exchange has orders with our CID epoch"]])
def test_caller_provided_evidence_blocks_fresh_database(env, callback_result):
    seen = []

    def evidence(identity, path):
        seen.append((identity, path))
        return callback_result

    with pytest.raises(PriorRunEvidenceError):
        env.open(create=True, prior_run_markers=evidence)
    assert seen == [(IDENTITY, env.db.absolute())]
    assert not env.db.exists()


def test_failing_evidence_callback_fails_closed(env):
    def broken(identity, path):
        raise RuntimeError("exchange unreachable")

    with pytest.raises(PriorRunEvidenceError, match="callback failed"):
        env.open(create=True, prior_run_markers=broken)
    assert not env.db.exists()


def test_leftover_wal_without_database_is_evidence(env):
    env.db.parent.mkdir(parents=True)
    env.db.with_name(env.db.name + "-wal").write_bytes(b"\x00" * 64)
    with pytest.raises(PriorRunEvidenceError, match="missing"):
        env.open(create=True)


def test_corrupt_header_is_refused_and_file_untouched(env):
    _used_store(env)
    with env.db.open("r+b") as handle:
        handle.write(b"garbage-garbage!")
    before = _digest(env.db)
    with pytest.raises(StoreCorruptError):
        env.open(create=True)
    assert _digest(env.db) == before


def test_corrupt_pages_fail_integrity_check(env):
    _used_store(env)
    page_size = 4096
    size = env.db.stat().st_size
    with env.db.open("r+b") as handle:
        for offset in range(2 * page_size, size, page_size):
            handle.seek(offset + 8)
            handle.write(b"\xff" * 200)
    with pytest.raises(StoreCorruptError):
        env.open(create=True)


def test_foreign_sqlite_database_is_refused(env):
    env.db.parent.mkdir(parents=True)
    raw = sqlite3.connect(str(env.db))
    raw.execute("CREATE TABLE other_app (id INTEGER)")
    raw.commit()
    raw.close()
    with pytest.raises(StoreCorruptError, match="not a neutral grid store"):
        env.open(create=True)


def test_replaced_database_does_not_match_marker(env, tmp_path):
    _used_store(env)
    other = Env(tmp_path / "other")
    other.open().close()  # a brand-new database of the same identity created elsewhere
    _remove_db(env)
    shutil.copy(other.db, env.db)
    with pytest.raises(PriorRunEvidenceError, match="does not match"):
        env.open(create=True)


def test_interrupted_creation_without_marker_is_a_fresh_start(env):
    env.db.parent.mkdir(parents=True)
    env.db.write_bytes(b"")  # crash between file creation and the first commit; no marker was written yet
    store = env.open(create=True)
    assert store.engine().bootstrapped is False


def test_deleted_marker_is_repaired_and_audited(env):
    _used_store(env)
    marker = env.db.with_name(env.db.name + ".marker.json")
    marker.unlink()
    store = env.open()
    assert marker.exists()
    assert store.audit_events("marker_repaired")[0].payload["markers"] == [str(marker)]


def test_manual_recovery_requires_evidence_and_never_adopts_position(env):
    old_cid = _used_store(env)
    _remove_db(env)
    recovery = ManualRecovery(actor="operator", reason="disk replaced; backup unavailable",
                              evidence={"ticket": "OPS-1", "venue_position": "330", "open_orders": "none"})
    store = env.open(create=True, manual_recovery=recovery)
    engine = store.engine()
    assert engine.recovered_from_loss and engine.manual_reconcile_required
    assert engine.bootstrapped is False and engine.effective_baseline is None  # position NOT adopted
    with pytest.raises(BootstrapError):
        store.position_ledger()
    event = store.audit_events("manual_recovery")[0]
    assert event.payload["evidence"]["ticket"] == "OPS-1" and event.payload["new_cid_epoch"] == 2
    assert event.payload["prior_markers"]
    # explicit operator bootstrap is still required and new entries stay blocked until manual reconciliation
    env.bootstrap(store, baseline=Decimal("330"))
    with pytest.raises(EntryBlockedError, match="manual reconciliation"):
        record_entry_intent(store, cell_id=1)
    store.record_manual_reconciliation(None, "operator", "venue checked by hand", {"venue_position": "330"})
    new = record_entry_intent(store, cell_id=1)
    assert new.cid >> CID_EPOCH_SHIFT == 2 and old_cid >> CID_EPOCH_SHIFT == 1 and new.cid != old_cid
    store.close()
    # the recovered database is now the one the markers point to
    env.open().close()


def test_manual_recovery_is_refused_when_database_exists(env):
    _used_store(env)
    with pytest.raises(ValueError):
        env.open(manual_recovery=ManualRecovery("op", "x", {"a": 1}))


def test_readers_refuse_corrupt_or_missing_files(env):
    with pytest.raises(Exception):
        NeutralGridStore.open_readonly(env.db)
    _used_store(env)
    with env.db.open("r+b") as handle:
        handle.write(b"not sqlite at all")
    with pytest.raises(StoreCorruptError):
        NeutralGridStore.open_readonly(env.db)
