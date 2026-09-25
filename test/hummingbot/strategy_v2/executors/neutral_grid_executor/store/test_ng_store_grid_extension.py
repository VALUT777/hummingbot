from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CellSpec, EngineState, Side
from hummingbot.strategy_v2.executors.neutral_grid_executor.migrations import MIGRATIONS
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    BootstrapRecord,
    ConfigMutationError,
    GridExtension,
    StoreIntegrityError,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    BOOT_CUT_MS,
    GRID_ID,
    MARKET,
    Env,
)


D = Decimal


def _prices(low="4.9", high="5.9"):
    lower, upper = D(low), D(high)
    return [lower + D("0.1") * i for i in range(int((upper - lower) / D("0.1")) + 1)]


def _cells(prices, anchor=D("5.23215"), *, first_id=0):
    return [CellSpec(first_id + i, prices[i], prices[i + 1],
                     Side.BUY if prices[i] < anchor else Side.SELL)
            for i in range(len(prices) - 1)]


def _bootstrap(store):
    prices = _prices()
    config = {"grid_id": GRID_ID, "lower_price": "4.9", "upper_price": "5.9", "cell_count": 10,
              "order_amount_base": "100", "max_abs_net_position": "1000", "max_gross_position": "1000"}
    store.bootstrap(None, BootstrapRecord(
        grid_id=GRID_ID, config_fingerprint="fp-old", config=config,
        lower_price=D("4.9"), upper_price=D("5.9"), order_amount_base=D("100"), prices=prices,
        cells=_cells(prices), anchor=D("5.23215"), baseline=D("0"), market_id=MARKET,
        bootstrap_cut_ts_ms=BOOT_CUT_MS, actor="operator", confirmation="confirmed",
        trades_cut="trade-cut", orders_cut="order-cut"))


def _extension(**overrides):
    config = {"grid_id": GRID_ID, "lower_price": "4.8", "upper_price": "5.9", "cell_count": 11,
              "order_amount_base": "100", "max_abs_net_position": "1000", "max_gross_position": "1000"}
    values = dict(
        config_fingerprint="fp-extended", config=config,
        lower_price=D("4.8"), upper_price=D("5.9"), order_amount_base=D("100"),
        prices=_prices("4.8", "5.9"), added_cells=(CellSpec(10, D("4.8"), D("4.9"), Side.BUY),),
        anchor=D("5.23215"), actor="operator", reason="extend one step lower", proof_id="a" * 64)
    values.update(overrides)
    return GridExtension(**values)


def test_add_only_extension_preserves_cells_and_overlays_effective_window(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    _bootstrap(store)
    store.set_engine_state(None, EngineState.STOPPED_WITH_INVENTORY, "test clean stop")
    before = store.cells()

    effective = store.extend_grid(None, _extension())

    assert (effective.grid_id, effective.lower_price, effective.upper_price, effective.cell_count) == \
           (GRID_ID, D("4.8"), D("5.9"), 11)
    assert effective.anchor == D("5.23215") and effective.order_amount_base == D("100")
    assert [cell.cell_id for cell in store.active_cells()] == [10] + list(range(10))
    assert store.cells()[:10] == before
    assert store.cell(GRID_ID, 10).spec() == CellSpec(10, D("4.8"), D("4.9"), Side.BUY)
    assert store.engine().initial_baseline == store.engine().effective_baseline == D("0")
    event = store.audit_events("grid_extension")[0].payload
    assert event["proof_id"] == "a" * 64 and event["added_cell_ids"] == [10]

    store.close()
    reopened = env.open(config_fingerprint="fp-extended")
    assert reopened.grid().prices == tuple(_prices("4.8", "5.9"))
    assert [cell.cell_id for cell in reopened.active_cells()] == [10] + list(range(10))


@pytest.mark.parametrize("change", [
    {"lower_price": D("5.0"), "prices": _prices("5.0", "5.9"), "added_cells": ()},
    {"upper_price": D("5.8"), "prices": _prices("4.8", "5.8")},
    {"order_amount_base": D("99")},
    {"anchor": D("5.3")},
])
def test_extension_rejects_contraction_reprice_q_or_anchor_change(tmp_path, change):
    env = Env(tmp_path)
    store = env.open()
    _bootstrap(store)
    store.set_engine_state(None, EngineState.STOPPED_WITH_INVENTORY, "test clean stop")
    with pytest.raises(ConfigMutationError):
        store.extend_grid(None, _extension(**change))
    assert store.grid().lower_price == D("4.9") and len(store.cells()) == 10


def test_extension_store_guard_rejects_engine_that_is_not_durably_stopped(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    _bootstrap(store)
    with pytest.raises(ConfigMutationError, match="STOPPED"):
        store.extend_grid(None, _extension())
    assert store.grid().lower_price == D("4.9") and len(store.cells()) == 10


def test_extension_cannot_smuggle_a_risk_cap_change(tmp_path):
    env = Env(tmp_path)
    store = env.open()
    _bootstrap(store)
    store.set_engine_state(None, EngineState.STOPPED_WITH_INVENTORY, "test clean stop")
    changed = dict(_extension().config, max_gross_position="2")
    with pytest.raises(ConfigMutationError, match="max_gross_position"):
        store.extend_grid(None, _extension(config=changed))


def test_applied_v7_missing_window_table_fails_closed_while_explicit_v6_upgrades(tmp_path):
    env = Env(tmp_path)
    legacy = env.open(migrations=MIGRATIONS[:6])
    _bootstrap(legacy)
    legacy.close()
    upgraded = env.open()
    assert upgraded.grid().lower_price == D("4.9")
    upgraded.close()

    import sqlite3
    raw = sqlite3.connect(str(env.db))
    raw.execute("DROP TABLE grid_window_revisions")
    raw.commit()
    raw.close()
    with pytest.raises(StoreIntegrityError, match="schema v7"):
        env.open(config_fingerprint="fp-old")
