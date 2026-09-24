"""Engine integration harness: deterministic FakeExchange + real on-disk SQLite store + crash/restart.

Every test drives the real ``NeutralGridEngine`` through the fake venue. ``Harness.crash_restart()`` models a
process death: the old engine object is dropped (its store connection is closed without committing and OS
locks are released) and a new engine opens the same database file, exactly like a new process would.
"""
from __future__ import annotations

import asyncio
import itertools
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CommandKind,
    EngineState,
    GridConfig,
    LegRole,
    OrderState,
    OrderTypePolicy,
    Side,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import EngineOptions
from hummingbot.strategy_v2.executors.neutral_grid_executor.engine import (
    NeutralGridEngine,
    open_engine,
    open_engine_store,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import FakeClock, FakeExchange
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import FaultHooks, SimulatedCrash

D = Decimal
START = 1_790_000_000.0


def make_config(**overrides: Any) -> GridConfig:
    values: Dict[str, Any] = dict(
        grid_id="grid-t", connector_name="fake_lighter", trading_pair="LIT-USDG", account_index=4242,
        lower_price=D("5"), upper_price=D("6"), cell_count=10, order_amount_base=D("10"), leverage=D("5"),
        expected_initial_position=D("0"), max_abs_net_position=D("1000"), max_gross_position=D("1000"),
        max_active_orders=120, history_freshness_s=D("10"), settlement_delay_s=D("5"), settlement_scans=2,
        history_overlap_s=D("60"), poll_interval_s=D("5"), entry_order_type=OrderTypePolicy.LIMIT_MAKER,
        tp_order_type=OrderTypePolicy.LIMIT, tp_gtt_seconds=28 * 24 * 3600, enabled=True,
    )
    values.update(overrides)
    return GridConfig(**values)


class Harness:
    def __init__(self, tmp_path: Path, *, config: Optional[GridConfig] = None,
                 options: Optional[EngineOptions] = None, fx_kwargs: Optional[Dict[str, Any]] = None,
                 **config_overrides: Any):
        self.tmp_path = Path(tmp_path)
        self.clock = FakeClock(START)
        kwargs = dict(fx_kwargs or {})
        self.fx = FakeExchange(self.clock, **kwargs)
        self.config = config or make_config(**config_overrides)
        self.options = options or EngineOptions(tick_interval_s=1.0)
        self.db_path = self.tmp_path / "data" / "ng.sqlite3"
        self.lock_dir = self.tmp_path / "locks"
        self.hooks = FaultHooks()
        self.loop = asyncio.new_event_loop()
        self._keys = itertools.count(1)
        self.crashes = 0
        self.engine: NeutralGridEngine = self.open()

    # ------------------------------------------------------------------ lifecycle
    def open(self) -> NeutralGridEngine:
        store = open_engine_store(str(self.db_path), self.config, self.fx, lock_dir=str(self.lock_dir),
                                  fault_hooks=self.hooks, clock=self.clock)
        engine = NeutralGridEngine(self.config, store, self.fx, self.clock, options=self.options, offline_demo=True)
        self.fx.add_ws_listener(engine.wake)
        return engine

    def open_failclosed(self, **kwargs) -> NeutralGridEngine:
        return open_engine(self.config, str(self.db_path), self.fx, clock=self.clock, options=self.options,
                           lock_dir=str(self.lock_dir), fault_hooks=self.hooks, **kwargs)

    def restart(self) -> NeutralGridEngine:
        """Clean process restart (store closed)."""
        self.fx.remove_ws_listener(self.engine.wake)
        if self.engine.store is not None and not self.engine.store.closed:
            self.engine.store.close()
        self.engine = self.open()
        return self.engine

    def crash_restart(self) -> NeutralGridEngine:
        """Process death at an arbitrary point: nothing in memory survives, the DB file does."""
        self.crashes += 1
        self.fx.remove_ws_listener(self.engine.wake)
        store = self.engine.store
        if store is not None and not store.closed:
            store._die()
        self.hooks.disarm()
        self.engine = self.open()
        return self.engine

    def close(self) -> None:
        if self.engine.store is not None and not self.engine.store.closed:
            self.engine.store.close()
        self.loop.close()

    # ------------------------------------------------------------------ driving
    def tick(self, n: int = 1, dt: float = 1.0) -> None:
        for _ in range(n):
            self.loop.run_until_complete(self.engine.tick())
            self.clock.advance(dt)

    def tick_crashing(self, n: int = 1, dt: float = 1.0) -> bool:
        """Tick; a SimulatedCrash restarts the engine from disk. Returns True if a crash happened."""
        crashed = False
        for _ in range(n):
            try:
                self.loop.run_until_complete(self.engine.tick())
            except SimulatedCrash:
                crashed = True
                self.crash_restart()
            self.clock.advance(dt)
        return crashed

    def run_until(self, predicate: Callable[[], bool], max_ticks: int = 200, dt: float = 1.0) -> int:
        for i in range(max_ticks):
            if predicate():
                return i
            self.tick(1, dt)
        if predicate():
            return max_ticks
        raise AssertionError(f"condition not reached in {max_ticks} ticks; state={self.engine.engine_state} "
                             f"reasons={self.engine.reasons}")

    def command(self, kind: CommandKind, payload: Optional[Dict[str, Any]] = None, key: Optional[str] = None,
                expected=None):
        return self.engine.enqueue(kind, payload or {}, key=key or f"test-{next(self._keys)}", expected=expected)

    def bootstrap(self, baseline: Decimal = D("0"), start: bool = True) -> None:
        """First bootstrap: wait for a stable snapshot + history cut, then the explicit operator confirmation."""
        if start:
            self.command(CommandKind.START)
        self.run_until(lambda: self.engine.bootstrap_ready(self.clock())[0], max_ticks=60)
        self.command(CommandKind.CONFIRM_BASELINE, {"expected_initial_position": str(baseline)})
        self.tick()
        assert self.engine.bootstrapped, self.engine.recent_commands

    def settle(self, max_ticks: int = 80) -> None:
        """Run until every owned order that is terminal *at the venue* (test oracle) is proven TERMINAL."""
        def done() -> bool:
            for leg in self.engine.non_final_legs():
                venue = self.fx.order_by_cid(leg.cid) if leg.cid is not None else None
                if venue is not None and not venue.is_open:
                    return False
            return self.engine.history_complete
        self.run_until(done, max_ticks=max_ticks)

    # ------------------------------------------------------------------ inspection
    @property
    def state(self) -> EngineState:
        return self.engine.engine_state

    def cell(self, cell_id: int):
        return self.engine.cells[cell_id]

    def legs(self, cell_id: int, role: Optional[LegRole] = None) -> List[Any]:
        legs = self.engine.cells[cell_id].legs()
        return [leg for leg in legs if role is None or leg.identity.role == role]

    def live_order(self, cell_id: int, role: LegRole):
        for leg in reversed(self.legs(cell_id, role)):
            if leg.state not in (OrderState.TERMINAL, OrderState.REJECTED_UNSENT, OrderState.REJECTED_ZERO_FILL):
                return leg
        return None

    def buy_cells(self) -> List[int]:
        return [c for c, ledger in self.engine.cells.items() if ledger.spec.entry_side == Side.BUY]

    def sell_cells(self) -> List[int]:
        return [c for c, ledger in self.engine.cells.items() if ledger.spec.entry_side == Side.SELL]

    def submits_for(self, cid: int) -> List[Any]:
        return [c for c in self.fx.submits() if c.client_order_id == cid]

    def assert_safe_orders(self) -> None:
        """Global invariants on every request the venue ever saw (AC-38, NG-ORD-002)."""
        for call in self.fx.submits():
            req = call.request
            assert req.reduce_only is False
            assert req.order_type in (OrderTypePolicy.LIMIT_MAKER, OrderTypePolicy.LIMIT)
        assert not [v for v in self.fx.violations if "reduce_only" in v or "duplicate client order id" in v], \
            self.fx.violations
        cids = [c.client_order_id for c in self.fx.submits()]
        # A CID reaches the venue at most once unless the same outbox row was re-dispatched after a crash
        # (never a *new* CID for the same leg).
        assert len(cids) == len(set(cids)) or self.crashes > 0
