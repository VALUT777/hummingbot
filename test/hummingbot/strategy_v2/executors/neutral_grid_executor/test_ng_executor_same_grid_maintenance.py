from types import SimpleNamespace

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CommandKind
from hummingbot.strategy_v2.executors.neutral_grid_executor.executor import NeutralGridExecutor


class _OpenStore:
    closed = False


def _executor(*, stored_grid_id, configured_grid_id):
    executor = NeutralGridExecutor.__new__(NeutralGridExecutor)
    executor.config = SimpleNamespace(
        operator_confirmed_migration=True,
        id="maintenance-test",
        grid_id=configured_grid_id,
    )
    executor.engine = SimpleNamespace(
        store=_OpenStore(),
        meta=SimpleNamespace(freezes={"CONFIG_MISMATCH": "maintenance config differs"}),
        rules=object(),
        mid=object(),
        grid_record=SimpleNamespace(grid_id=stored_grid_id),
    )
    executor._migration_key = None
    executor._migration_attempts = 0
    executor._migration_refused = False
    enqueued = []
    executor._enqueue = lambda kind, payload, key: enqueued.append((kind, payload, key))
    return executor, enqueued


def test_same_grid_maintenance_does_not_enqueue_legacy_cold_migration():
    executor, enqueued = _executor(stored_grid_id="same-grid", configured_grid_id="same-grid")

    executor._maybe_migrate()

    assert enqueued == []
    assert executor._migration_key is None
    assert executor._migration_attempts == 0


def test_different_grid_keeps_launcher_confirmed_cold_migration():
    executor, enqueued = _executor(stored_grid_id="old-grid", configured_grid_id="new-grid")

    executor._maybe_migrate()

    assert enqueued == [(
        CommandKind.BASELINE_AUDIT,
        {"action": "migrate_grid", "actor": "launcher-confirmed-operator",
         "note": "launcher-confirmed migration to new-grid"},
        "launcher-migrate-maintenance-test-1",
    )]
    assert executor._migration_key == "launcher-migrate-maintenance-test-1"
    assert executor._migration_attempts == 1
