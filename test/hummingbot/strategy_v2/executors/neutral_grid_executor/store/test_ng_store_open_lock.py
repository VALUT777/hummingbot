"""Open, single-writer lock, owner fencing, schema versioning and reader handles (NG-DB-001, NG-DB-003 step 1)."""
import sqlite3
import subprocess
import sys
import textwrap
from decimal import Decimal

import pytest

import hummingbot.strategy_v2.executors.neutral_grid_executor.store as store_module
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CommandKind, EngineState
from hummingbot.strategy_v2.executors.neutral_grid_executor.migrations import MIGRATIONS, Migration
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    EngineIdentity,
    FaultHooks,
    IdentityMismatchError,
    NeutralGridStore,
    OwnershipLostError,
    PersistenceError,
    ReadOnlyStoreError,
    SchemaVersionError,
    SimulatedCrash,
    StoreError,
    StoreLockedError,
    StoreMissingError,
    default_db_path,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    DOMAIN,
    IDENTITY,
    REPO_ROOT,
    Env,
)


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


def test_default_path_is_under_hummingbot_data_path(tmp_path):
    path = default_db_path(IDENTITY, base_dir=tmp_path)
    assert path == tmp_path / "neutral_grid" / "neutral_grid.lighter_perpetual_robinhood.7.LIT-USDG.sqlite3"
    from hummingbot import data_path
    assert default_db_path(IDENTITY).parent == type(path)(data_path()) / "neutral_grid"


def test_wal_and_synchronous_full_are_enforced(env):
    store = env.open()
    assert store._conn.raw.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert store._conn.raw.execute("PRAGMA synchronous").fetchone()[0] == 2  # FULL
    assert store._conn.raw.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_missing_database_is_not_created_without_permission(env):
    with pytest.raises(StoreMissingError):
        env.open(create=False)
    assert not env.db.exists()


def test_second_writer_on_same_host_fails_until_first_closes(env):
    first = env.open()
    with pytest.raises(StoreLockedError):
        env.open()
    first.close()
    env.open().close()


def test_same_account_market_is_locked_even_with_another_database_path(env, tmp_path):
    first = env.open()
    other = Env(tmp_path / "elsewhere")
    other.locks = env.locks  # same host-wide lock directory
    with pytest.raises(StoreLockedError):
        other.open()
    first.close()


def test_other_process_cannot_open_while_engine_runs(env):
    store = env.open()
    code = textwrap.dedent(f"""
        import sys, pathlib
        sys.path.insert(0, {str(REPO_ROOT)!r})
        from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import Env
        from hummingbot.strategy_v2.executors.neutral_grid_executor.store import StoreLockedError
        try:
            Env(pathlib.Path({str(env.tmp)!r})).open()
        except StoreLockedError:
            sys.exit(3)
        sys.exit(0)
    """)
    proc = subprocess.run([sys.executable, "-c", code], cwd=str(REPO_ROOT), capture_output=True, text=True,
                          timeout=60)
    assert proc.returncode == 3, proc.stderr
    store.close()


def test_lock_is_documented_as_not_distributed():
    assert "NOT a distributed lock" in store_module.__doc__
    assert "NOT a distributed lock" in store_module._HostLock.__doc__


def test_owner_row_of_foreign_host_is_refused_and_takeover_is_audited(env):
    env.open().close()
    raw = sqlite3.connect(str(env.db))
    raw.execute("UPDATE engine_owner SET hostname = 'other-host', released_at_ms = NULL")
    raw.commit()
    raw.close()
    with pytest.raises(StoreLockedError, match="other-host"):
        env.open()
    store = env.open(takeover_foreign_host=True)
    event = store.audit_events("owner_acquired")[0]
    assert event.payload["foreign_takeover"] is True
    assert event.payload["previous_owner"]["hostname"] == "other-host"


def test_stale_owner_of_dead_process_on_same_host_is_replaced_and_audited(env):
    hooks = FaultHooks()
    store = env.open(fault_hooks=hooks)
    hooks.arm("before_commit")
    with pytest.raises(SimulatedCrash):
        with store.transaction() as tx:
            store.set_engine_state(tx, EngineState.RECONCILING, "x")
    reopened = env.open()
    event = reopened.audit_events("owner_acquired")[0]
    assert event.payload["previous_owner_unreleased"] is True and event.payload["foreign_takeover"] is False


def test_owner_fencing_detects_takeover_inside_transactions(env):
    store = env.open()
    raw = sqlite3.connect(str(env.db))
    raw.execute("UPDATE engine_owner SET owner_token = 'intruder'")
    raw.commit()
    raw.close()
    with pytest.raises(OwnershipLostError):
        with store.transaction() as tx:
            store.set_engine_state(tx, EngineState.PAUSED, "should not apply")
    with pytest.raises(PersistenceError, match="ownership lost"):  # fail closed: the handle is unusable
        with store.transaction():
            pass
    with pytest.raises(PersistenceError):
        store.engine()
    reader = NeutralGridStore.open_readonly(env.db)
    assert reader.engine().engine_state == EngineState.BOOTSTRAPPING  # nothing of the fenced tx applied
    reader.close()


def test_identity_mismatch_is_refused(env):
    env.open().close()
    other = EngineIdentity(connector_name=DOMAIN, connector_domain=DOMAIN, account_index=8, trading_pair="LIT-USDG")
    with pytest.raises(IdentityMismatchError):
        NeutralGridStore.open(env.db, other, lock_dir=env.locks, clock_ms=env.clock)


def _insert_future_migration(path):
    raw = sqlite3.connect(str(path))
    raw.execute("INSERT INTO schema_migrations(version, name, checksum, applied_at_ms) VALUES (2, 'future', 'x', 0)")
    raw.execute("PRAGMA user_version = 2")
    raw.commit()
    raw.close()


def test_newer_schema_version_fails_closed_for_writer_and_readers(env):
    env.open().close()
    _insert_future_migration(env.db)
    with pytest.raises(SchemaVersionError, match="newer/unknown"):
        env.open()
    with pytest.raises(SchemaVersionError):
        NeutralGridStore.open_readonly(env.db)
    with pytest.raises(SchemaVersionError):
        NeutralGridStore.open_command_client(env.db)


def test_edited_migration_checksum_fails_closed(env):
    env.open().close()
    edited = (Migration(1, MIGRATIONS[0].name, MIGRATIONS[0].sql + "\n-- edited\n"),)
    with pytest.raises(SchemaVersionError, match="differs"):
        env.open(migrations=edited)


def test_pending_migration_is_applied_atomically_and_audited(env):
    env.open().close()
    v2 = Migration(2, "add_example", "CREATE TABLE example_v2 (id INTEGER PRIMARY KEY) STRICT;")
    store = env.open(migrations=MIGRATIONS + (v2,))
    assert store._conn.raw.execute("PRAGMA user_version").fetchone()[0] == 2
    [event] = store.audit_events("migration")
    assert event.payload["version"] == 2 and event.payload["name"] == "add_example"
    store.close()
    with pytest.raises(SchemaVersionError):
        env.open()  # older code refuses the upgraded database


def test_failed_migration_leaves_previous_version(env):
    env.open().close()
    broken = Migration(2, "broken", "CREATE TABLE ok_part (id INTEGER PRIMARY KEY) STRICT;\nCREATE TABLE engine (x);")
    with pytest.raises(PersistenceError):
        env.open(migrations=MIGRATIONS + (broken,))
    raw = sqlite3.connect(str(env.db))
    try:
        assert raw.execute("PRAGMA user_version").fetchone()[0] == 1
        assert raw.execute("SELECT count(*) FROM sqlite_master WHERE name = 'ok_part'").fetchone()[0] == 0
    finally:
        raw.close()
    env.open().close()


def test_readonly_reader_sees_only_committed_state_and_cannot_write(env):
    store = env.open()
    env.bootstrap(store)
    with store.transaction() as tx:
        store.write_snapshot(tx, {"engine_state": EngineState.RECONCILING, "summary": {"n": 1}})
    reader = NeutralGridStore.open_readonly(env.db)
    assert reader.latest_snapshot().snapshot_version == 1
    with store.transaction() as tx:
        store.write_snapshot(tx, {"engine_state": EngineState.NORMAL, "summary": {"n": 2}})
        assert reader.latest_snapshot().snapshot_version == 1  # uncommitted snapshot is invisible
    assert reader.latest_snapshot().snapshot_version == 2
    with pytest.raises(ReadOnlyStoreError):
        with reader.transaction():
            pass
    with pytest.raises(ReadOnlyStoreError):
        reader.enqueue_command("k", CommandKind.PAUSE, 1, 1)
    with pytest.raises(PersistenceError):
        reader._x("UPDATE engine SET state_reason = 'x'")
    store.close()
    assert reader.latest_snapshot().engine_state == EngineState.NORMAL  # still readable after writer exit
    reader.close()


def test_command_client_may_only_append_commands(env):
    store = env.open()
    env.bootstrap(store)
    engine = store.engine()
    client = NeutralGridStore.open_command_client(env.db)
    command = client.enqueue_command("ui-1", CommandKind.PAUSE, engine.config_revision, engine.engine_revision)
    assert command.id == 1 and store.get_command(command.id).idempotency_key == "ui-1"
    for sql in ("UPDATE engine SET state_reason = 'x'", "DELETE FROM commands",
                "UPDATE commands SET status = 'APPLIED'", "INSERT INTO audit_events(at_ms, kind, actor, payload_json) "
                                                          "VALUES (0, 'x', 'x', '{}')"):
        with pytest.raises(PersistenceError):
            with client.transaction():
                client._x(sql)
    with pytest.raises(ReadOnlyStoreError):
        with client.transaction() as tx:
            client.set_engine_state(tx, EngineState.PAUSED)
    assert store.engine().engine_state == EngineState.BOOTSTRAPPING
    assert client.latest_snapshot() is None


def test_transactions_are_not_nested_and_methods_need_the_open_transaction(env):
    store = env.open()
    env.bootstrap(store)
    with store.transaction():
        with pytest.raises(StoreError):
            with store.transaction():
                pass
    other = Env(env.tmp / "b").open()
    with store.transaction():
        with pytest.raises(StoreError):
            with other.transaction() as foreign_tx:
                store.kv_set(foreign_tx, "k", 1)


def test_restart_reads_back_state(env):
    store = env.open()
    env.bootstrap(store, baseline=Decimal("-12.5"))
    with store.transaction() as tx:
        store.set_engine_state(tx, EngineState.PAUSED, "operator", pause_reason="maintenance")
        store.kv_set(tx, "slots", {"cap": 120, "reserved": Decimal("3")})
    store.close()
    again = env.open()
    engine = again.engine()
    assert engine.engine_state == EngineState.PAUSED and engine.pause_reason == "maintenance"
    assert again.kv_get("slots") == {"cap": 120, "reserved": "3"}
    state = again.load_state()
    assert state.engine.effective_baseline == Decimal("-12.5") and len(state.cells) == 55
