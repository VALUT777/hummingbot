"""Durable command queue and committed snapshots (NG-UI-001/003 persistence part; supports AC-47/AC-48)."""
import threading
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CommandKind,
    CommandStatus,
    EngineState,
    Snapshot,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    FaultHooks,
    InvalidTransitionError,
    NeutralGridStore,
    SimulatedCrash,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import Env


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


@pytest.fixture
def store(env):
    opened = env.open(fault_hooks=FaultHooks())
    env.bootstrap(opened)
    return opened


def _revs(store):
    engine = store.engine()
    return engine.config_revision, engine.engine_revision


def test_duplicate_idempotency_key_returns_original_row_across_processes(env, store):
    first = store.enqueue_command("key-1", CommandKind.PAUSE, *_revs(store), payload={"why": "maintenance"})
    assert first.status == CommandStatus.QUEUED and not first.duplicate
    client = NeutralGridStore.open_command_client(env.db)
    again = client.enqueue_command("key-1", CommandKind.PAUSE, *_revs(store), payload={"why": "maintenance"})
    assert again.duplicate and again.id == first.id and not again.request_mismatch
    other = client.enqueue_command("key-1", CommandKind.STOP, *_revs(store))
    assert other.id == first.id and other.kind == "pause" and other.request_mismatch
    assert len(store.list_commands()) == 1


def test_stale_revisions_produce_conflict_row_that_is_never_applied(store):
    config_rev, engine_rev = _revs(store)
    stale = store.enqueue_command("key-2", CommandKind.RESUME, config_rev, engine_rev - 1)
    assert stale.status == CommandStatus.CONFLICT
    assert stale.result["reason"] == "stale_revision" and stale.result["current_engine_revision"] == engine_rev
    with store.transaction() as tx:
        assert store.claim_next_command(tx) is None
    retry = store.enqueue_command("key-2", CommandKind.RESUME, config_rev, engine_rev)
    assert retry.duplicate and retry.status == CommandStatus.CONFLICT  # refresh shows the same result


def test_second_start_while_one_is_queued_conflicts(store):
    first = store.enqueue_command("start-a", CommandKind.START, *_revs(store))
    second = store.enqueue_command("start-b", CommandKind.START, *_revs(store))
    assert first.status == CommandStatus.QUEUED
    assert second.status == CommandStatus.CONFLICT and second.result == {"reason": "start_already_queued",
                                                                         "command_id": first.id}


def test_claim_apply_complete_is_exactly_once_across_crash(env, store):
    command = store.enqueue_command("pause-1", CommandKind.PAUSE, *_revs(store))
    store.fault_hooks.arm("before_commit")
    with pytest.raises(SimulatedCrash):
        with store.transaction() as tx:
            claimed = store.claim_next_command(tx)
            store.set_engine_state(tx, EngineState.PAUSED, "operator pause", pause_reason="ui")
            store.complete_command(tx, claimed.id, CommandStatus.APPLIED, {"engine_state": "PAUSED"})
    reopened = env.open()
    assert reopened.get_command(command.id).status == CommandStatus.QUEUED
    assert reopened.get_command(command.id).claim_count == 0
    assert reopened.engine().engine_state == EngineState.BOOTSTRAPPING
    with reopened.transaction() as tx:
        claimed = reopened.claim_next_command(tx)
        reopened.set_engine_state(tx, EngineState.PAUSED, "operator pause", pause_reason="ui")
        done = reopened.complete_command(tx, claimed.id, CommandStatus.APPLIED, {"engine_state": "PAUSED"})
    assert done.status == CommandStatus.APPLIED and done.claim_count == 1
    with reopened.transaction() as tx:
        assert reopened.claim_next_command(tx) is None
        assert reopened.complete_command(tx, command.id, CommandStatus.APPLIED,
                                         {"engine_state": "PAUSED"}).status == CommandStatus.APPLIED
        with pytest.raises(InvalidTransitionError):
            reopened.complete_command(tx, command.id, CommandStatus.REJECTED, {"x": 1})


def test_command_that_went_stale_before_apply_becomes_conflict(store):
    command = store.enqueue_command("resume-1", CommandKind.RESUME, *_revs(store))
    with store.transaction() as tx:
        store.set_engine_state(tx, EngineState.RISK_BLOCKED, "cap conflict")  # bumps engine_revision
    with store.transaction() as tx:
        assert store.claim_next_command(tx) is None
    row = store.get_command(command.id)
    assert row.status == CommandStatus.CONFLICT and row.result["reason"] == "stale_at_apply"


def test_engine_revision_moves_only_on_state_change(store):
    _, before = _revs(store)
    with store.transaction() as tx:
        store.set_engine_state(tx, EngineState.RECONCILING, "scan 1")
    with store.transaction() as tx:
        store.set_engine_state(tx, EngineState.RECONCILING, "scan 2")  # reason only
    assert _revs(store)[1] == before + 1


def test_concurrent_enqueue_with_same_key_creates_one_row(env, store):
    revs = _revs(store)
    results, errors = [], []

    def worker():
        try:
            client = NeutralGridStore.open_command_client(env.db)
            results.append(client.enqueue_command("same-key", CommandKind.STOP, *revs))
            client.close()
        except Exception as exc:  # pragma: no cover - reported below
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert len({r.id for r in results}) == 1 and sum(not r.duplicate for r in results) == 1
    assert len(store.list_commands()) == 1


def test_snapshot_versions_are_monotonic_across_pruning_and_restart(env, store):
    versions = []
    for n in range(5):
        with store.transaction() as tx:
            versions.append(store.write_snapshot(tx, {"engine_state": EngineState.NORMAL, "summary": {"n": n}},
                                                 keep_last=2).snapshot_version)
    assert versions == [1, 2, 3, 4, 5]
    assert store.snapshot(3) is None and store.snapshot(4) is not None
    store.close()
    reopened = env.open()
    with reopened.transaction() as tx:
        assert reopened.write_snapshot(tx, {"engine_state": EngineState.PAUSED}).snapshot_version == 6


def test_snapshot_carries_committed_revisions_and_rejects_mismatch(store):
    config_rev, engine_rev = _revs(store)
    with store.transaction() as tx:
        stored = store.write_snapshot(tx, Snapshot(0, config_rev, engine_rev, 0.0, EngineState.RECONCILING,
                                                   ["history incomplete"], {"summary": {"baseline": Decimal("0")}}))
    contract = stored.to_contract()
    assert contract.snapshot_version == 1 and contract.reasons == ["history incomplete"]
    assert contract.payload["summary"]["baseline"] == "0" and contract.engine_state == EngineState.RECONCILING
    doc = stored.payload
    assert (doc["config_revision"], doc["engine_revision"]) == (config_rev, engine_rev)
    with pytest.raises(InvalidTransitionError):
        with store.transaction() as tx:
            store.write_snapshot(tx, {"engine_state": EngineState.NORMAL, "engine_revision": engine_rev + 7})


def test_crash_before_snapshot_commit_keeps_previous_snapshot(env, store):
    with store.transaction() as tx:
        store.write_snapshot(tx, {"engine_state": EngineState.NORMAL})
    store.fault_hooks.arm("before_snapshot_commit")
    with pytest.raises(SimulatedCrash):
        with store.transaction() as tx:
            store.set_engine_state(tx, EngineState.STOPPED, "stop")
            store.write_snapshot(tx, {"engine_state": EngineState.STOPPED})
    reader = NeutralGridStore.open_readonly(env.db)
    latest = reader.latest_snapshot()
    assert latest.snapshot_version == 1 and latest.engine_state == EngineState.NORMAL  # no fake STOPPED
