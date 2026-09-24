"""Bootstrap record stored once and never recaptured (NG-RISK-001, AC-45 persistence part) and immutable grid
dimensions with audited migration only (AC-52, NG-ARCH-003)."""
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CellSpec, OrderState, Side
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    BootstrapError,
    BootstrapRecord,
    ConfigMutationError,
    EntryBlockedError,
    GridMigration,
    InvalidTransitionError,
    NeutralGridStore,
)
from test.hummingbot.strategy_v2.executors.neutral_grid_executor.store.ng_store_support import (
    ACCOUNT,
    BOOT_CUT_MS,
    DOMAIN,
    FINGERPRINT,
    GRID_ID,
    MARKET,
    PAIR,
    Q,
    Env,
    FakeTransport,
    build_grid,
    complete_cycle,
    record_entry_intent,
    submit_via_protocol,
    trade_row,
)


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


@pytest.mark.parametrize("baseline", [Decimal("-330"), Decimal("0"), Decimal("330")])
def test_bootstrap_stores_identity_grid_anchor_baseline_and_cut_once(env, baseline):
    store = env.open()
    env.bootstrap(store, baseline=baseline)
    engine = store.engine()
    assert (engine.connector_domain, engine.account_index, engine.trading_pair, engine.market_id) == \
           (DOMAIN, ACCOUNT, PAIR, MARKET)
    assert engine.initial_baseline == baseline == engine.effective_baseline
    assert engine.bootstrap_cut_ts_ms == BOOT_CUT_MS and engine.bootstrap_trades_cut == "trade-cut-1"
    grid = store.grid()
    assert grid.fingerprint == FINGERPRINT and grid.anchor == Decimal("5.4") and grid.order_amount_base == Q
    assert len(grid.prices) == 56 and grid.cell_count == 55
    cells = store.cells()
    assert [c.entry_side for c in cells].count(Side.BUY) == 22 and len(cells) == 55
    assert store.cursor("TRADES").high_water == "trade-cut-1"
    assert store.audit_events("baseline_confirmed")[0].payload["baseline"] == str(baseline)
    # no seed/TP/flatten for B: bootstrap creates no cycles, legs or intents
    assert store.open_cycles() == [] and store.legs() == [] and store.unresolved_outbox() == []


def test_restart_never_recaptures_baseline(env):
    store = env.open()
    env.bootstrap(store, baseline=Decimal("330"))
    store.close()
    reopened = env.open()
    with pytest.raises(BootstrapError, match="never"):
        env.bootstrap(reopened, baseline=Decimal("0"))  # e.g. current venue position after downtime
    assert reopened.engine().initial_baseline == Decimal("330")
    assert len(reopened.audit_events("baseline_confirmed")) == 1


def test_bootstrap_requires_explicit_confirmation_and_consistent_grid(env):
    store = env.open()
    prices, cells = build_grid()
    base = dict(grid_id=GRID_ID, config_fingerprint=FINGERPRINT, config={}, lower_price=Decimal("5"),
                upper_price=Decimal("6"), order_amount_base=Q, prices=prices, cells=cells, anchor=Decimal("5.4"),
                baseline=Decimal("0"), market_id=MARKET, bootstrap_cut_ts_ms=BOOT_CUT_MS, actor="operator",
                confirmation="yes")
    flipped = [CellSpec(c.cell_id, c.low_price, c.high_price, Side.SELL) for c in cells]  # ignores anchor rule
    for bad in (dict(confirmation=""), dict(prices=prices[:-1]), dict(anchor=Decimal("7")),
                dict(baseline=0.0), dict(order_amount_base=Decimal("0")), dict(cells=cells[::-1]),
                dict(cells=flipped), dict(anchor=Decimal("5.6"))):
        with pytest.raises((ValueError, TypeError)):
            store.bootstrap(None, BootstrapRecord(**{**base, **bad}))
    assert store.engine().bootstrapped is False


def test_pre_cut_rows_are_part_of_baseline_and_post_cut_manual_trade_blocks_entries(env):
    store = env.open()
    env.bootstrap(store, baseline=Decimal("330"))
    with store.transaction() as tx:
        result = store.apply_history_batch(tx, [
            trade_row("old-1", 123, Side.BUY, "330", ts=BOOT_CUT_MS - 60_000),   # how B was built
            trade_row("old-2", None, Side.SELL, "5", ts=BOOT_CUT_MS),            # at the cut: still pre-cut
        ])
    assert [r.status for r in result.pre_cut] == ["PRE_CUT", "PRE_CUT"] and result.unmatched == []
    assert store.position_ledger().net == Decimal("330")  # not 330 + 330 - 5
    assert store.entry_blockers() == []
    with store.transaction() as tx:  # overlap re-read of the same rows is a no-op
        assert store.apply_history_batch(tx, [trade_row("old-1", 123, Side.BUY, "330",
                                                        ts=BOOT_CUT_MS - 60_000)]).duplicates == 1
    with store.transaction() as tx:
        manual = store.apply_history_batch(tx, [trade_row("man-1", None, Side.SELL, "3", ts=BOOT_CUT_MS + 1)])
    assert [r.status for r in manual.unmatched] == ["UNMATCHED"]
    with pytest.raises(EntryBlockedError, match="baseline audit"):
        record_entry_intent(store, cell_id=0)
    audit_id = store.record_baseline_audit(None, "operator", "manual sell of 3 LIT", Decimal("327"),
                                           new_baseline=Decimal("327"),
                                           resolved_inbox_ids=[manual.unmatched[0].id])
    engine = store.engine()
    assert engine.initial_baseline == Decimal("330") and engine.effective_baseline == Decimal("327")
    assert store.unmatched_evidence(include_resolved=True)[0].resolution == f"baseline_audit:{audit_id}"
    record_entry_intent(store, cell_id=0)


def test_changed_dimensions_are_rejected_at_open_and_revision(env):
    store = env.open()
    env.bootstrap(store)
    store.close()
    with pytest.raises(ConfigMutationError):
        env.open(config_fingerprint="fp-5-6-60-10")
    store = env.open(config_fingerprint=FINGERPRINT)
    with pytest.raises(ConfigMutationError):
        store.record_config_revision(None, {"cell_count": 60}, "fp-5-6-60-10", "operator", "more cells")
    revision = store.record_config_revision(None, {"max_active_orders": 100}, FINGERPRINT, "operator", "lower cap")
    assert revision == 2 and store.engine().config_revision == 2
    assert store.grid().order_amount_base == Q


def _migration(new_grid_id="grid-b", n=40):
    prices, cells = build_grid(n=n, anchor=Decimal("5.5"))
    return GridMigration(new_grid_id=new_grid_id, config_fingerprint=f"fp-5-6-{n}-10", config={"cell_count": n},
                         lower_price=Decimal("5"), upper_price=Decimal("6"), order_amount_base=Q, prices=prices,
                         cells=cells, anchor=Decimal("5.5"), actor="operator", reason="resize grid")


def test_grid_with_open_orders_or_obligations_cannot_be_migrated(env):
    store = env.open()
    env.bootstrap(store)
    transport = FakeTransport()
    live = record_entry_intent(store, cell_id=0)
    submit_via_protocol(store, transport, live)
    with pytest.raises(ConfigMutationError) as caught:
        store.migrate_grid(None, _migration())
    assert any(str(live.cid) in b for b in caught.value.blockers)
    assert store.grid().grid_id == GRID_ID


def test_audited_migration_preserves_old_cycles_and_never_resets(env):
    store = env.open()
    env.bootstrap(store, baseline=Decimal("12"))
    transport = FakeTransport()
    entry_cid, tp_cid = complete_cycle(store, transport, cell_id=0)
    fills_before = store.fills()
    new_grid = store.migrate_grid(None, _migration())
    assert new_grid.grid_id == "grid-b" and new_grid.migrated_from == GRID_ID and new_grid.cell_count == 40
    assert store.grid(GRID_ID).status == "RETIRED" and store.grid().grid_id == "grid-b"
    old_cycle = store.cycle(GRID_ID, 0, 1)
    assert old_cycle.state == "COMPLETE" and old_cycle.entry_filled == Q == old_cycle.exit_filled
    assert store.leg(entry_cid).state == OrderState.TERMINAL and store.fills() == fills_before
    assert store.engine().initial_baseline == Decimal("12")
    event = store.audit_events("grid_migration")[0].payload
    assert event["old_grid_id"] == GRID_ID and event["preserved_cycles"] == 1
    with pytest.raises(InvalidTransitionError):  # the retired grid does not start new cycles
        with store.transaction() as tx:
            store.open_cycle(tx, GRID_ID, 1)
    store.close()
    assert env.open(config_fingerprint="fp-5-6-40-10").grid().grid_id == "grid-b"


def test_there_is_no_reset_or_rebaseline_api():
    forbidden = ("reset", "wipe", "truncate", "drop", "delete", "purge", "rebaseline", "recapture")
    public = [name for name in dir(NeutralGridStore) if not name.startswith("_")]
    assert not [name for name in public if any(word in name.lower() for word in forbidden)]


def test_cell_is_locked_until_cycle_proven_complete(env):
    store = env.open()
    env.bootstrap(store)
    transport = FakeTransport()
    intent = record_entry_intent(store, cell_id=30)
    with pytest.raises(InvalidTransitionError, match="still open"):
        with store.transaction() as tx:
            store.open_cycle(tx, GRID_ID, 30)
    submit_via_protocol(store, transport, intent)
    with pytest.raises(InvalidTransitionError, match="cannot be released"):
        with store.transaction() as tx:
            store.close_cycle(tx, GRID_ID, 30, 1, "premature")
    with store.transaction() as tx:
        store.update_cycle(tx, GRID_ID, 30, 1, dust=Decimal("0.5"))
    assert any("dust" in b for b in store.cycle_release_blockers(GRID_ID, 30, 1))
    cell = store.cell(GRID_ID, 30)
    assert cell.entry_side == Side.SELL  # SELL cell re-arms as SELL
