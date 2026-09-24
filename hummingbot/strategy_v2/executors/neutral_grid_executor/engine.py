"""Persistent neutral fixed-cell grid engine (spec docs/superpowers/specs/2026-09-24-neutral-grid-spec.md).

One engine owns every cell of one grid on one account/market and runs inside ONE asyncio task (the
executor control loop, AC-04). A tick is strictly ordered:

1. drain the durable command queue (claim + effects + completion in ONE store transaction);
2. read account facts (book, rules, position, active orders) under a weight budget;
3. one bounded, resumable ``HistoryScanner`` step (cursor pagination of trades + inactive orders);
4. apply new history rows: inbox + dedupe keys + fill attribution + leg transitions + cursor/high-water in ONE
   store transaction (``NeutralGridStore.apply_history_batch``);
5. reconcile the active-order list (acceptance evidence only; disappearance is never terminal proof);
6. settle orders (exact terminal row + complete scan + cumulative equality + delay + repeat scans), refresh
   dust and release cells (NG-CELL-001);
7. drift detection and honest engine state/reasons;
8. plan (admission + router) and dispatch: intent+CID+reservation commit -> ``mark_dispatching`` commit ->
   transport -> result commit (NG-DB-002);
9. commit a versioned snapshot (the only thing UI and CLI read).

Durable truth is the WS-B store (legs, fills, cycles, outbox, cursors, commands). The WS-A ``CellLedger``
objects are an in-memory projection rebuilt from the store on load and mirrored after every store write.
Nothing here uses float for quantities or ids. WebSocket events only wake the scanner.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Sequence, Set, Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor import admission, grid, risk, router
from hummingbot.strategy_v2.executors.neutral_grid_executor.cells import (
    FINAL_STATES,
    CellLedger,
    Cycle,
    FillRecord as LedgerFill,
    LedgerError,
    Leg,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.commands import (
    AUDIT_ACTION_BASELINE,
    AUDIT_ACTIONS,
    CommandOutcome,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CellState,
    CommandKind,
    CommandStatus,
    EngineState,
    ExchangeOrderRow,
    ExchangePort,
    ExchangeTradeRow,
    GridConfig,
    LegIdentity,
    LegRole,
    OrderState,
    OrderTypePolicy,
    PositionSnapshot,
    Side,
    SubmitRequest,
    TradingRules,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.data_types import (
    EngineMeta,
    EngineOptions,
    OrderMeta,
    grid_config_to_json,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.history import (
    ENDPOINT_ACCOUNT,
    ENDPOINT_ACTIVE_ORDERS,
    ENDPOINT_INACTIVE_ORDERS,
    ENDPOINT_TRADES,
    ENDPOINT_TRADING_RULES,
    STREAM_ORDERS,
    STREAM_TRADES,
    HighWaterMark,
    HistoryScanner,
    evaluate_terminal_release,
    order_dedupe_key as c_order_key,
    order_payload_fingerprint,
    trade_payload_fingerprint,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    STREAM_INACTIVE_ORDERS as B_ORDERS,
    STREAM_TRADES as B_TRADES,
    BootstrapRecord,
    CidAllocationError,
    CursorUpdate,
    EngineIdentity as StoreIdentity,
    EntryBlockedError,
    NeutralGridStore,
    PersistenceError,
    Reservation,
    StoreClosedError,
    StoreError,
    order_dedupe_key as b_order_key,
)

ZERO = Decimal("0")
LOGGER = logging.getLogger(__name__)
ACTOR = "engine"

# Engine-level operator-cleared blockers (the store keeps its own durable ones: conflicts, unmatched trades,
# manual_reconcile_required; both are shown).
FREEZE_RISK_BLOCKED = "RISK_BLOCKED"
FREEZE_INVARIANT = "LEDGER_INVARIANT"
FREEZE_CID = "CID_ALLOCATION"
FREEZE_CONFIG_MISMATCH = "CONFIG_MISMATCH"
_FREEZES_BLOCKING_TP = {FREEZE_INVARIANT, FREEZE_CID, FREEZE_CONFIG_MISMATCH}

CANCELLABLE = {OrderState.LIVE}
STOPPED_OUTCOMES = {EngineState.STOPPED.value, EngineState.STOPPED_WITH_INVENTORY.value}
_C_TO_B = {STREAM_TRADES: B_TRADES, STREAM_ORDERS: B_ORDERS}


def _ms(ts: float) -> int:
    return int(round(ts * 1000))


def _retry_only(cycle: Cycle) -> bool:
    """Every entry revision was proven unsent / definitively rejected with zero fill: the same cycle may retry
    with a new revision (NG-DB-005); it is neither an open exposure nor a completed cycle."""
    return (bool(cycle.entries) and not cycle.tps and cycle.E == 0
            and all(e.state in (OrderState.REJECTED_UNSENT, OrderState.REJECTED_ZERO_FILL) for e in cycle.entries))


# ------------------------------------------------------------------------------------------ row json helpers
def order_row_to_json(row: ExchangeOrderRow) -> Dict[str, Any]:
    return {
        "client_order_id": None if row.client_order_id is None else str(row.client_order_id),
        "client_order_id_str": row.client_order_id_str, "order_id": row.order_id, "order_index": row.order_index,
        "nonce": row.nonce, "account_index": row.account_index, "market_id": row.market_id,
        "side": row.side.value, "price": str(row.price), "initial_base_amount": str(row.initial_base_amount),
        "filled_base_amount": str(row.filled_base_amount), "remaining_base_amount": str(row.remaining_base_amount),
        "status": row.status, "reduce_only": row.reduce_only, "timestamp_ms": row.timestamp_ms,
        "raw_json": row.raw_json,
    }


def order_row_from_json(d: Dict[str, Any]) -> ExchangeOrderRow:
    return ExchangeOrderRow(
        client_order_id=None if d["client_order_id"] is None else int(d["client_order_id"]),
        client_order_id_str=d["client_order_id_str"], order_id=d["order_id"], order_index=d["order_index"],
        nonce=d["nonce"], account_index=int(d["account_index"]), market_id=int(d["market_id"]),
        side=Side(d["side"]), price=Decimal(str(d["price"])),
        initial_base_amount=Decimal(str(d["initial_base_amount"])),
        filled_base_amount=Decimal(str(d["filled_base_amount"])),
        remaining_base_amount=Decimal(str(d["remaining_base_amount"])), status=d["status"],
        reduce_only=bool(d["reduce_only"]), timestamp_ms=int(d["timestamp_ms"]), raw_json=d["raw_json"],
    )


# ------------------------------------------------------------------------------------------ store opening
def engine_store_identity(connector_name: str, trading_pair: str, port: ExchangePort) -> StoreIdentity:
    return StoreIdentity(connector_name=connector_name, connector_domain=port.domain,
                         account_index=port.account_index, trading_pair=trading_pair)


def open_engine_store(db_path: Optional[str], config: Any, port: ExchangePort, *, create_if_missing: bool = True,
                      lock_dir: Optional[str] = None, fault_hooks: Any = None,
                      clock: Callable[[], float] = time.time,
                      prior_run_markers: Optional[Callable] = None) -> NeutralGridStore:
    """Open the single-writer store (fails closed on missing/corrupt DB with prior-run evidence, AC-54)."""
    grid_config = config if isinstance(config, GridConfig) else config.to_grid_config(port.account_index)
    identity = engine_store_identity(grid_config.connector_name, grid_config.trading_pair, port)
    return NeutralGridStore.open(
        Path(db_path) if db_path else None, identity, create_if_missing=create_if_missing,
        prior_run_markers=prior_run_markers, lock_dir=lock_dir,
        config_fingerprint=grid.config_fingerprint(grid_config), fault_hooks=fault_hooks,
        clock_ms=lambda: _ms(clock()))


def open_engine(config: GridConfig, db_path: Optional[str], port: ExchangePort, *,
                clock: Callable[[], float] = time.time, options: Optional[EngineOptions] = None,
                lock_dir: Optional[str] = None, fault_hooks: Any = None, create_if_missing: bool = True,
                offline_demo: bool = False, prior_run_markers: Optional[Callable] = None) -> "NeutralGridEngine":
    """Open store + engine. Any store refusal yields a fail-closed engine (never a fresh bootstrap)."""
    try:
        store = open_engine_store(db_path, config, port, create_if_missing=create_if_missing, lock_dir=lock_dir,
                                  fault_hooks=fault_hooks, clock=clock, prior_run_markers=prior_run_markers)
    except (StoreError, PersistenceError, OSError) as exc:
        return NeutralGridEngine(config, None, port, clock, options=options, offline_demo=offline_demo,
                                 fatal_reason=f"STORE_OPEN_REFUSED:{type(exc).__name__}: {exc}")
    return NeutralGridEngine(config, store, port, clock, options=options, offline_demo=offline_demo)


class _CursorView:
    """``history.HistoryCursorView`` over the engine's committed history caches."""

    def __init__(self, engine: "NeutralGridEngine"):
        self._engine = engine

    def high_water(self, stream: str) -> Optional[str]:
        if stream in self._engine.meta.history_reset:
            return None
        return self._engine.high_water.get(stream)

    def committed_payload(self, stream: str, key: Tuple) -> Optional[str]:
        return self._engine.committed.get(stream, {}).get(tuple(key))

    def oldest_unresolved_ms(self) -> Optional[int]:
        return self._engine.oldest_unresolved_ms()

    def bootstrap_floor_ms(self, stream: str) -> Optional[int]:
        meta = self._engine.meta
        return meta.history_reset.get(stream, meta.bootstrap_floor_ms)


class NeutralGridEngine:
    """Single-task engine for all cells of one fixed grid (AC-04)."""

    def __init__(self, config: GridConfig, store: Optional[NeutralGridStore], port: ExchangePort,
                 clock: Callable[[], float] = time.time, scanner: Optional[HistoryScanner] = None, *,
                 options: Optional[EngineOptions] = None, offline_demo: bool = False,
                 fatal_reason: Optional[str] = None):
        self.config = config
        self.port = port
        self.clock = clock
        self.options = options or EngineOptions()
        self.offline_demo = offline_demo
        self.store = store
        self.limits = risk.RiskLimits(config.max_abs_net_position, config.max_gross_position)
        self.fingerprint = grid.config_fingerprint(config)
        self.fatal_reason = fatal_reason if (fatal_reason or store is not None) else "NO_STORE"
        # ---------------- durable model (projection of the store) ----------------
        self.meta = EngineMeta()
        self.b_engine = None
        self.grid_record = None
        self.cells: Dict[int, CellLedger] = {}
        self.order_meta: Dict[int, OrderMeta] = {}
        self.reservations: Dict[int, int] = {}
        self.persisted_cell_state: Dict[int, Tuple[str, Optional[str], int]] = {}
        self.high_water: Dict[str, Optional[str]] = {STREAM_TRADES: None, STREAM_ORDERS: None}
        self.cursor_ts: Dict[str, int] = {}
        self.committed: Dict[str, Dict[Tuple, str]] = {STREAM_TRADES: {}, STREAM_ORDERS: {}}
        self.trades_by_cid: Dict[int, List[ExchangeTradeRow]] = {}
        self.terminal_rows: Dict[int, ExchangeOrderRow] = {}
        self.store_entry_blockers: List[str] = []
        self.open_conflicts: List[Any] = []
        self.unmatched: List[Any] = []
        # ---------------- runtime (never trusted across restarts) ----------------
        self.rules: Optional[TradingRules] = None
        self.rules_at: Optional[float] = None
        self.bid: Optional[Decimal] = None
        self.ask: Optional[Decimal] = None
        self.position: Optional[PositionSnapshot] = None
        self.position_at: Optional[float] = None
        self.active_rows: Optional[List[ExchangeOrderRow]] = None
        self.active_at: Optional[float] = None
        self.errors: Deque[Dict[str, Any]] = deque(maxlen=50)
        self.recent_commands: Deque[Dict[str, Any]] = deque(maxlen=20)
        self.persistence_error: Optional[str] = None
        self.reload_needed = False
        self.history_complete = False
        self.history_incomplete_reason: Optional[str] = "no complete scan yet"
        self.last_complete_scan_at: Optional[float] = None
        self.last_history_commit_at: Optional[float] = None
        self.last_scan_pages: Dict[str, int] = {}
        self.weight_log: Deque[Tuple[float, int]] = deque()
        self.position_gap_since: Optional[float] = None
        self.unknown_active: List[ExchangeOrderRow] = []
        self.ws_pending: Dict[str, float] = {}
        self.last_ws_event_at: Optional[float] = None
        self.tp_latencies: Deque[Tuple[int, float]] = deque(maxlen=200)
        self.cell_blockers: Dict[int, str] = {}
        self.entry_blockers: List[str] = []
        self.tp_blockers: List[str] = []
        self.admission_plan: Optional[admission.AdmissionPlan] = None
        self.router_plan: Optional[router.RouterPlan] = None
        self.endpoints: Optional[risk.RiskEndpoints] = None
        self.margin_warning: Optional[str] = None
        self.margin_required: Optional[Decimal] = None
        self.position_reconciled = False
        self.startup_reconciled = False
        self.bootstrap_probe: Optional[Dict[str, Any]] = None
        self.bootstrap_rows_seen = 0
        self.engine_state = EngineState.BOOTSTRAPPING
        self.reasons: List[str] = []
        self.last_tick_at: Optional[float] = None
        self.ticks = 0
        self.transport_calls = 0
        self.last_snapshot: Optional[Dict[str, Any]] = None
        self._last_account_poll_at: Optional[float] = None
        self._active_reconciled_at: Optional[float] = None
        self._submits_this_tick = 0
        self._last_degraded_probe: Optional[float] = None
        self._active_evidence_cache: Dict[int, Tuple[Optional[str], Decimal]] = {}
        if self.fatal_reason is None:
            try:
                self._load()
            except (StoreError, PersistenceError) as exc:
                self.fatal_reason = f"STORE_LOAD_FAILED:{type(exc).__name__}: {exc}"
        self.scanner = scanner or HistoryScanner(
            port, _CursorView(self), overlap_s=config.history_overlap_s,
            weight_budget=self.options.scan_weight_budget, clock=clock,
            max_pages_per_call=self.options.max_scan_pages_per_tick,
            poll_interval_s=config.poll_interval_s,
            min_wake_interval_s=Decimal(str(self.options.min_wake_interval_s)),
            retention_horizon_s=(None if self.options.history_retention_horizon_s is None
                                 else Decimal(str(self.options.history_retention_horizon_s))),
        )
        if self.fatal_reason is not None:
            self.engine_state = EngineState.DEGRADED
            self.reasons = [self.fatal_reason]

    # ================================================================================== loading
    @property
    def grid_id(self) -> str:
        return self.grid_record.grid_id if self.grid_record is not None else self.config.grid_id

    @property
    def bootstrapped(self) -> bool:
        return self.b_engine is not None and self.b_engine.bootstrapped

    def _now_ms(self) -> int:
        return _ms(self.clock())

    def _load(self) -> None:
        s = self.store
        self.b_engine = s.engine()
        data = s.kv_get("engine_meta")
        if data is None:
            self.meta = EngineMeta(bootstrap_floor_ms=self._now_ms() - int(self.config.history_overlap_s * 1000))
            with s.transaction() as tx:
                s.kv_set(tx, "engine_meta", self.meta.to_json())
        else:
            self.meta = EngineMeta.from_json(data)
        self.cells, self.order_meta, self.trades_by_cid, self.terminal_rows = {}, {}, {}, {}
        self.reservations, self.persisted_cell_state = {}, {}
        self.committed = {STREAM_TRADES: {}, STREAM_ORDERS: {}}
        self.high_water = {STREAM_TRADES: None, STREAM_ORDERS: None}
        self.grid_record = None
        if not self.bootstrapped:
            return
        self.grid_record = s.grid()
        if self.grid_record.fingerprint != self.fingerprint:
            self.meta.freezes[FREEZE_CONFIG_MISMATCH] = "running grid dimensions/Q differ from config (AC-52)"
        self._rebuild_ledgers()
        cursors = s.cursors()
        for c_stream, b_stream in _C_TO_B.items():
            self.high_water[c_stream] = cursors[b_stream].high_water
            self.cursor_ts[c_stream] = cursors[b_stream].high_water_ts_ms or 0
        self._load_committed()
        self._refresh_store_facts()

    def _rebuild_ledgers(self) -> None:
        s = self.store
        grid_id = self.grid_record.grid_id
        q = self.grid_record.order_amount_base
        legs = s.legs(grid_id=grid_id)
        fills_by_cid: Dict[int, List[Any]] = {}
        for f in s.fills():
            fills_by_cid.setdefault(f.cid, []).append(f)
        for lr in legs:
            data = s.kv_get(f"om:{lr.cid}")
            self.order_meta[lr.cid] = OrderMeta.from_json(data) if data else OrderMeta(
                cid=lr.cid, cell_id=lr.cell_id, generation=lr.generation, role=lr.role.value,
                intent_ms=lr.created_at_ms)
        for cr in s.cells(grid_id):
            spec = cr.spec()
            ledger = CellLedger(grid_id, spec, q)
            # Genesis sentinel: the store numbers cycles from 1 (cells.generation 0 == no cycle yet).
            ledger.cycles = [Cycle(generation=0, entry_side=spec.entry_side, planned_qty=q, closed=True)]
            cell_legs = [lr for lr in legs if lr.cell_id == cr.cell_id]
            gens = sorted({lr.generation for lr in cell_legs} | ({cr.generation} if cr.generation > 0 else set()))
            for g in gens:
                cy = s.cycle(grid_id, cr.cell_id, g)
                cycle = Cycle(generation=g, entry_side=cy.entry_side, planned_qty=cy.planned_amount, dust=cy.dust,
                              closed=cy.state == "COMPLETE", late_evidence=cy.late_evidence == 1)
                for lr in sorted((x for x in cell_legs if x.generation == g),
                                 key=lambda x: (x.role.value, x.revision)):
                    leg = self._leg_from_store(lr, fills_by_cid.get(lr.cid, []))
                    (cycle.entries if lr.role == LegRole.ENTRY else cycle.tps).append(leg)
                ledger.cycles.append(cycle)
            for lr in cell_legs:
                for f in fills_by_cid.get(lr.cid, []):
                    ledger.fills[f.dedupe_key] = LedgerFill(identity=lr.identity, qty=f.size, price=f.price,
                                                            side=f.own_side)
            self.cells[cr.cell_id] = ledger
            self.reservations[cr.cell_id] = cr.reserved_slots
            self.persisted_cell_state[cr.cell_id] = (cr.state, cr.blocker, cr.reserved_slots)
        for cid, fs in fills_by_cid.items():
            self.trades_by_cid[cid] = [self._trade_from_fill(f) for f in fs]
        for cid, meta in self.order_meta.items():
            if meta.terminal_row:
                self.terminal_rows[cid] = order_row_from_json(meta.terminal_row)

    def _leg_from_store(self, lr, fills: Sequence[Any]) -> Leg:
        order = self.store.order(lr.cid)
        meta = self.order_meta.get(lr.cid)
        return Leg(identity=lr.identity, side=lr.side, price=lr.price, requested=lr.amount, state=lr.state,
                   cid=lr.cid, exchange_order_id=order.exchange_order_id if order else None, filled=lr.filled,
                   terminal_cumulative=(order.venue_filled if lr.state == OrderState.TERMINAL and order else None),
                   order_type=lr.order_type, expiry_ms=lr.expiry_ms, seq=meta.seq if meta else 0,
                   late_evidence=any(f.late for f in fills))

    def _trade_from_fill(self, f) -> ExchangeTradeRow:
        market_id = self.b_engine.market_id if self.b_engine.market_id is not None else self.port.market_id
        return ExchangeTradeRow(
            trade_id_str=f.trade_id_str, account_index=self.b_engine.account_index, market_id=market_id,
            own_side=f.own_side, own_exchange_order_id=f.own_exchange_order_id, own_client_order_id=f.cid,
            size=f.size, price=f.price, is_maker=None, timestamp_ms=f.timestamp_ms, raw_json="{}")

    def _load_committed(self) -> None:
        """Recent committed inbox rows -> scanner keys/fingerprints (older rows are deduped by the store)."""
        domain = self.port.domain
        for rec in self.store.inbox(limit=self.options.committed_cache_rows):
            try:
                p = dict(rec.payload)
                if rec.stream == B_TRADES:
                    row = ExchangeTradeRow(
                        trade_id_str=str(p["trade_id_str"]), account_index=int(p["account_index"]),
                        market_id=int(p["market_id"]), own_side=Side(p["own_side"]),
                        own_exchange_order_id=p["own_exchange_order_id"],
                        own_client_order_id=None if p["own_client_order_id"] is None
                        else int(p["own_client_order_id"]),
                        size=Decimal(str(p["size"])), price=Decimal(str(p["price"])), is_maker=p["is_maker"],
                        timestamp_ms=int(p["timestamp_ms"]), raw_json=rec.raw_json)
                    self.committed[STREAM_TRADES][row.dedupe_key(domain)] = trade_payload_fingerprint(row)
                else:
                    p["raw_json"] = rec.raw_json
                    p["client_order_id"] = None if p["client_order_id"] is None else str(p["client_order_id"])
                    row = order_row_from_json(p)
                    self.committed[STREAM_ORDERS][c_order_key(domain, row)] = order_payload_fingerprint(row)
            except (KeyError, TypeError, ValueError, ArithmeticError):
                continue

    def _refresh_store_facts(self) -> None:
        s = self.store
        self.b_engine = s.engine()
        self.store_entry_blockers = s.entry_blockers()
        self.open_conflicts = s.open_conflicts()
        self.unmatched = s.unmatched_evidence()

    def _reload(self) -> None:
        """After a failed transaction memory may be ahead of disk: rebuild everything from the store."""
        self._load()
        self.reload_needed = False
        self._active_evidence_cache = {}
        self._active_reconciled_at = None

    # ================================================================================== lookups / mirror
    def leg_by_cid(self, cid: int) -> Optional[Leg]:
        meta = self.order_meta.get(cid)
        if meta is None:
            return None
        ledger = self.cells.get(meta.cell_id)
        if ledger is None:
            return None
        for cycle in ledger.cycles:
            if cycle.generation != meta.generation:
                continue
            for leg in cycle.legs:
                if leg.cid == cid:
                    return leg
        return None

    def _mirror(self, cid: int) -> None:
        """Project the store's durable leg/order/cycle rows onto the in-memory ledger (store is authoritative)."""
        leg = self.leg_by_cid(cid)
        lr = self.store.leg(cid)
        if leg is None or lr is None:
            return
        order = self.store.order(cid)
        leg.state = lr.state
        leg.filled = lr.filled
        if order is not None and order.exchange_order_id:
            leg.exchange_order_id = order.exchange_order_id
        if lr.state == OrderState.TERMINAL and order is not None:
            leg.terminal_cumulative = order.venue_filled
        self._mirror_cycle(leg.identity.cell_id, leg.identity.generation)

    def _mirror_cycle(self, cell_id: int, generation: int) -> None:
        cy = self.store.cycle(self.grid_id, cell_id, generation)
        for cycle in self.cells[cell_id].cycles:
            if cycle.generation == generation:
                cycle.dust = cy.dust
                cycle.closed = cy.state == "COMPLETE"
                cycle.late_evidence = cy.late_evidence == 1
                return

    def all_legs(self) -> List[Leg]:
        return [leg for ledger in self.cells.values() for leg in ledger.legs()]

    def non_final_legs(self) -> List[Leg]:
        return [leg for ledger in self.cells.values() for leg in ledger.non_final_legs()]

    def oldest_unresolved_ms(self) -> Optional[int]:
        times = [self.order_meta[leg.cid].intent_ms for leg in self.non_final_legs() if leg.cid in self.order_meta]
        return min(times) if times else None

    @property
    def baseline(self) -> Optional[Decimal]:
        return None if self.b_engine is None else self.b_engine.initial_baseline

    @property
    def effective_baseline(self) -> Optional[Decimal]:
        return None if self.b_engine is None else self.b_engine.effective_baseline

    @property
    def mid(self) -> Optional[Decimal]:
        if self.bid is None or self.ask is None:
            return None
        return (self.bid + self.ask) / 2

    @property
    def state(self) -> EngineState:
        return self.engine_state

    @property
    def is_stopped(self) -> bool:
        return self.meta.stop_outcome in STOPPED_OUTCOMES

    # ================================================================================== public API
    def wake(self, event: Optional[Dict[str, Any]] = None) -> None:
        """Private WS signal: scan history soon (coalesced). Never a fill/terminal proof (NG-HIST-001).

        Only executions and terminal/cancel events need authoritative history; an acceptance ack of our own
        order does not, so it does not spend request weight (NG-HIST-003).
        """
        now = self.clock()
        event = event or {}
        status = str(event.get("status") or "")
        if event.get("type") == "order" and status in ("open", "pending", "in-progress"):
            return
        self.last_ws_event_at = now
        self.scanner.wake()
        if event.get("type") == "trade":
            label = str(event.get("trade_id") or event.get("client_order_id"))
            self.ws_pending.setdefault(label, now)

    def history_lag_s(self, now: Optional[float] = None) -> float:
        """Age of the oldest WS trade signal not yet reflected in committed history (AC-13)."""
        now = self.clock() if now is None else now
        if not self.ws_pending:
            return 0.0
        return max(0.0, now - min(self.ws_pending.values()))

    async def run_forever(self, stop_event: Optional[asyncio.Event] = None) -> None:
        """Offline demo / standalone loop. Inside Hummingbot the executor control loop calls ``tick``."""
        while stop_event is None or not stop_event.is_set():
            await self.tick()
            await asyncio.sleep(self.options.tick_interval_s)

    async def tick(self) -> None:
        now = self.clock()
        self.ticks += 1
        self._submits_this_tick = 0
        self.last_tick_at = now
        if self.fatal_reason is not None:
            self._evaluate_state(now)
            return
        try:
            if self.store.is_degraded or self.persistence_error is not None:
                if not self._probe_persistence(now):
                    await self._refresh_account(now)
                    self._evaluate_state(now)
                    return
            if self.reload_needed:
                self._reload()
            self._process_commands(now)
            await self._refresh_account(now)
            await self._scan_history(now)
            self._reconcile_active(now)
            self._settle(now)
            self._detect_drift(now)
            self._evaluate_state(now)
            await self._act(now)
            self._finish_stop(now)
            self._evaluate_state(now)
            self._commit_snapshot(now)
        except PersistenceError as exc:
            self._persistence_failed(exc, now)
        except StoreClosedError:
            raise
        except (StoreError, LedgerError) as exc:
            LOGGER.exception("neutral grid store/ledger refusal")
            self._error("STORE_REFUSED", f"{type(exc).__name__}: {exc}", now)
            self.meta.freezes.setdefault(FREEZE_INVARIANT, f"{type(exc).__name__}: {exc}")
            self.reload_needed = True
            self._evaluate_state(now)

    def _probe_persistence(self, now: float) -> bool:
        """Degraded store: try an audited ``clear_degraded`` at most every 5 s; reload on success."""
        if self._last_degraded_probe is not None and now - self._last_degraded_probe < 5:
            return False
        self._last_degraded_probe = now
        try:
            self.store.clear_degraded(ACTOR, "storage probe: audit row committed")
        except (PersistenceError, StoreError) as exc:
            self.persistence_error = f"{exc}"
            return False
        self.persistence_error = None
        self.reload_needed = True
        return True

    def _persistence_failed(self, exc: Exception, now: float) -> None:
        """NG-DB-004 / AC-55: nothing is sent without a committed intent; memory may be ahead of disk."""
        self.persistence_error = str(exc)
        self.reload_needed = True
        self._error("PERSISTENCE_FAILURE", str(exc), now)
        self._evaluate_state(now)

    def _error(self, code: str, message: str, now: float) -> None:
        self.errors.append({"at": now, "code": code, "message": message})

    # ================================================================================== weights
    def _charge(self, now: float, weight: int) -> None:
        self.weight_log.append((now, weight))

    def weight_used(self, now: float) -> int:
        while self.weight_log and self.weight_log[0][0] <= now - 60:
            self.weight_log.popleft()
        return sum(w for _, w in self.weight_log)

    def _can_spend(self, now: float, weight: int) -> bool:
        return self.weight_used(now) + weight <= self.options.weight_budget_per_min

    # ================================================================================== commands
    def _process_commands(self, now: float) -> None:
        s = self.store
        for _ in range(50):
            with s.transaction() as tx:
                cmd = s.claim_next_command(tx)
                if cmd is None:
                    break
                try:
                    outcome = self._apply_command(cmd, now, tx)
                except PersistenceError:
                    raise
                except (StoreError, LedgerError, ValueError, KeyError) as exc:
                    outcome = CommandOutcome(CommandStatus.REJECTED,
                                             {"error": "COMMAND_FAILED", "detail": f"{type(exc).__name__}: {exc}"})
                s.complete_command(tx, cmd.id, outcome.status, outcome.result)
                if outcome.status == CommandStatus.APPLIED:
                    s.bump_engine_revision(tx, f"command {cmd.kind}")
                s.kv_set(tx, "engine_meta", self.meta.to_json())
                if outcome.audit is not None:
                    s.record_audit(tx, outcome.audit[0], ACTOR, outcome.audit[1])
            if outcome.reload:
                self._reload()
            self._refresh_store_facts()
            self.recent_commands.append({"id": cmd.id, "kind": cmd.kind, "status": outcome.status.value,
                                         "result": outcome.result})

    def _apply_command(self, cmd, now: float, tx) -> CommandOutcome:
        kind = cmd.kind
        payload = cmd.payload or {}
        if kind == CommandKind.CONFIRM_BASELINE.value:
            return self._cmd_confirm_baseline(payload, now, tx)
        if kind == CommandKind.START.value:
            if not (self.config.enabled or self.offline_demo):
                return CommandOutcome(CommandStatus.REJECTED, {"error": "CONFIG_DISABLED",
                                                               "detail": "enabled=false refuses live start"})
            if self.meta.started and self.meta.stop_requested_ms is None:
                return CommandOutcome(CommandStatus.APPLIED, {"already_started": True, "grid_id": self.grid_id,
                                                              "engine_state": self.engine_state.value})
            self.meta.started = True
            self.meta.stop_requested_ms = None
            self.meta.stop_outcome = None
            self.meta.stop_reason = None
            return CommandOutcome(CommandStatus.APPLIED, {"started": True, "grid_id": self.grid_id},
                                  audit=("start", {"grid_id": self.grid_id}))
        if kind == CommandKind.PAUSE.value:
            self.meta.operator_paused = True
            self.meta.pause_reason = str(payload.get("reason") or "operator pause")
            return CommandOutcome(CommandStatus.APPLIED, {"paused": True},
                                  audit=("pause", {"reason": self.meta.pause_reason}))
        if kind == CommandKind.RESUME.value:
            return self._cmd_resume(now)
        if kind == CommandKind.STOP.value:
            if self.meta.stop_requested_ms is None or self.meta.stop_outcome is not None:
                self.meta.stop_requested_ms = _ms(now)
                self.meta.stop_outcome = None
                self.meta.stop_reason = str(payload.get("reason") or "operator stop")
            return CommandOutcome(CommandStatus.APPLIED, {"stopping": True},
                                  audit=("stop", {"reason": self.meta.stop_reason}))
        if kind == CommandKind.BASELINE_AUDIT.value:
            action = payload.get("action", AUDIT_ACTION_BASELINE)
            if action == AUDIT_ACTION_BASELINE:
                return self._cmd_baseline_audit(payload, now, tx)
            return self._cmd_reconcile(action, payload, now, tx)
        return CommandOutcome(CommandStatus.REJECTED, {"error": "UNKNOWN_COMMAND", "kind": kind})

    def _cmd_resume(self, now: float) -> CommandOutcome:
        gate = []
        if self.meta.stop_requested_ms is not None:
            gate.append("STOPPING")
        if not self.history_complete or self._history_stale(now):
            gate.append("HISTORY_NOT_FRESH")
        if self._position_stale(now):
            gate.append("POSITION_NOT_FRESH")
        if self.persistence_error:
            gate.append("PERSISTENCE_FAILURE")
        gate.extend(f"FROZEN:{code}" for code in sorted(self.meta.freezes))
        gate.extend(f"STORE:{b}" for b in self.store_entry_blockers)
        if self.endpoints is not None:
            gate.extend(risk.cap_violations(self.endpoints, self.limits))
        if gate:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "RESUME_GATE", "blockers": gate})
        self.meta.operator_paused = False
        self.meta.pause_reason = None
        return CommandOutcome(CommandStatus.APPLIED, {"resumed": True}, audit=("resume", {}))

    def bootstrap_ready(self, now: float) -> Tuple[bool, str]:
        """Stable snapshot + full history cut (NG-RISK-001, AC-45)."""
        if self.bootstrapped:
            return False, "already bootstrapped"
        if self.position is None or self._position_stale(now):
            return False, "position unknown or stale"
        if not self.history_complete or self._history_stale(now):
            return False, "history cut incomplete"
        if self.unknown_active:
            return False, "unknown active orders on market"
        if grid.rules_blockers(self.rules):
            return False, "trading rules unknown"
        if self.mid is None:
            return False, "order book unknown"
        probe = self.bootstrap_probe
        if probe is None or probe["scans"] < 2:
            return False, "waiting for two identical complete scans"
        if now - probe["since"] < float(self.config.settlement_delay_s):
            return False, "waiting for stability window"
        return True, "ready"

    def _cmd_confirm_baseline(self, payload: Dict[str, Any], now: float, tx) -> CommandOutcome:
        if self.bootstrapped:
            return CommandOutcome(CommandStatus.REJECTED, {
                "error": "BASELINE_ALREADY_CONFIRMED", "baseline": str(self.baseline),
                "detail": "baseline is loaded from the ledger and never recaptured (NG-RISK-001)"})
        try:
            expected = Decimal(str(payload["expected_initial_position"]))
        except Exception:  # noqa: BLE001
            return CommandOutcome(CommandStatus.REJECTED, {"error": "BASELINE_PAYLOAD_INVALID"})
        if not expected.is_finite():
            return CommandOutcome(CommandStatus.REJECTED, {"error": "BASELINE_PAYLOAD_INVALID"})
        if self.config.expected_initial_position is None or expected != self.config.expected_initial_position:
            return CommandOutcome(CommandStatus.REJECTED, {
                "error": "BASELINE_CONFIG_MISMATCH", "config": str(self.config.expected_initial_position),
                "command": str(expected)})
        ready, why = self.bootstrap_ready(now)
        if not ready:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "BOOTSTRAP_NOT_READY", "detail": why})
        observed = self.position.net_base
        if observed != expected:
            return CommandOutcome(CommandStatus.REJECTED, {
                "error": "BASELINE_MISMATCH", "observed_position": str(observed), "expected": str(expected),
                "detail": "the bot never adopts the current position as baseline automatically"})
        errors = grid.validate_config(self.config, self.rules, self.mid, bootstrap=True)
        if errors:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "CONFIG_INVALID", "errors": errors})
        prices = grid.build_grid(self.config.lower_price, self.config.upper_price, self.config.cell_count, self.rules)
        anchor = grid.compute_anchor(self.mid, self.config.lower_price, self.config.upper_price)
        specs = grid.assign_cells(prices, anchor)
        trades_hw = self.high_water.get(STREAM_TRADES)
        orders_hw = self.high_water.get(STREAM_ORDERS)
        marks = [HighWaterMark.decode(h) for h in (trades_hw, orders_hw) if h is not None]
        cut_ts = max([m.timestamp_ms for m in marks] + [int(self.bootstrap_probe.get("scan_ms", 0))])
        record = BootstrapRecord(
            grid_id=self.config.grid_id, config_fingerprint=self.fingerprint,
            config=grid_config_to_json(self.config), lower_price=self.config.lower_price,
            upper_price=self.config.upper_price, order_amount_base=self.config.order_amount_base, prices=prices,
            cells=specs, anchor=anchor, baseline=expected, market_id=self.port.market_id, bootstrap_cut_ts_ms=cut_ts,
            actor=str(payload.get("actor") or "operator"),
            confirmation=f"operator confirmed signed baseline {expected} == observed {observed}",
            trades_cut=trades_hw, orders_cut=orders_hw)
        self.store.bootstrap(tx, record)
        self.meta.started = True
        buy = sum(1 for c in specs if c.entry_side == Side.BUY)
        return CommandOutcome(
            CommandStatus.APPLIED,
            {"baseline": str(expected), "anchor": str(anchor), "cells": len(specs), "buy_cells": buy,
             "sell_cells": len(specs) - buy, "cut_ts_ms": cut_ts}, reload=True)

    def _confirmed_fills(self) -> Tuple[Decimal, Decimal]:
        buys = sells = ZERO
        for leg in self.all_legs():
            if leg.side == Side.BUY:
                buys += leg.filled
            else:
                sells += leg.filled
        return buys, sells

    def _cmd_baseline_audit(self, payload: Dict[str, Any], now: float, tx) -> CommandOutcome:
        """AC-30: explicit operator audit/rebase; never changes cell obligations or hides unknown fills."""
        if not self.bootstrapped:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "NOT_BOOTSTRAPPED"})
        if not self.history_complete or self._history_stale(now) or self._position_stale(now):
            return CommandOutcome(CommandStatus.REJECTED, {"error": "AUDIT_NEEDS_FRESH_COMPLETE_HISTORY"})
        try:
            observed = Decimal(str(payload["observed_position"]))
        except Exception:  # noqa: BLE001
            return CommandOutcome(CommandStatus.REJECTED, {"error": "AUDIT_PAYLOAD_INVALID"})
        venue = self.position.net_base
        if observed != venue:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "AUDIT_STALE_OBSERVATION",
                                                           "observed_position": str(venue)})
        unresolved = [leg.cid for leg in self.non_final_legs() if leg.state != OrderState.LIVE]
        if unresolved:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "AUDIT_UNRESOLVED_ORDERS",
                                                           "cids": [str(c) for c in unresolved]})
        buys, sells = self._confirmed_fills()
        new_baseline = venue - buys + sells
        inbox_ids = [rec.id for rec in self.unmatched if rec.stream == B_TRADES]
        old = self.effective_baseline
        self.store.record_baseline_audit(tx, str(payload.get("actor") or "operator"),
                                         str(payload.get("note") or "baseline audit"), venue,
                                         new_baseline=new_baseline, resolved_inbox_ids=inbox_ids)
        if self.b_engine.manual_reconcile_required and "drift" in (self.b_engine.manual_reconcile_reason or ""):
            self.store.record_manual_reconciliation(tx, str(payload.get("actor") or "operator"),
                                                    "baseline audit resolved position drift",
                                                    {"observed_position": str(venue),
                                                     "new_baseline": str(new_baseline)},
                                                    clear_manual_reconcile=True)
        self.position_gap_since = None
        return CommandOutcome(CommandStatus.APPLIED, {
            "observed_position": str(venue), "old_effective_baseline": str(old),
            "new_effective_baseline": str(new_baseline), "acknowledged_unmatched": inbox_ids}, reload=True)

    def _cmd_reconcile(self, action: str, payload: Dict[str, Any], now: float, tx) -> CommandOutcome:
        if action not in AUDIT_ACTIONS:
            return CommandOutcome(CommandStatus.REJECTED, {"error": "UNKNOWN_AUDIT_ACTION", "action": action})
        s = self.store
        actor = str(payload.get("actor") or "operator")
        note = str(payload.get("note") or action)
        evidence = {"action": action, "note": note, "engine_state": self.engine_state.value}
        if action == "ack_late_evidence":
            ids = [c.id for c in self.open_conflicts if c.kind == "LATE_FILL"]
            s.record_manual_reconciliation(tx, actor, note, evidence, resolved_conflict_ids=ids,
                                           clear_manual_reconcile=False)
            return CommandOutcome(CommandStatus.APPLIED, {"resolved_conflicts": ids}, reload=True)
        if action == "ack_history_conflict":
            ids = [c.id for c in self.open_conflicts if c.kind != "LATE_FILL"]
            s.record_manual_reconciliation(tx, actor, note, evidence, resolved_conflict_ids=ids,
                                           clear_manual_reconcile=True)
            self.meta.freezes.pop(FREEZE_INVARIANT, None)
            return CommandOutcome(CommandStatus.APPLIED, {"resolved_conflicts": ids}, reload=True)
        if action == "ack_retention_gap":
            # The unreachable old boundary is replaced by the currently available history (audited); baseline,
            # cells and fills are untouched (no reset, no rebaseline). Drift checks still apply afterwards.
            s.record_manual_reconciliation(tx, actor, note, evidence, clear_manual_reconcile=True)
            floor = _ms(now) - int(self.config.history_overlap_s * 1000)
            self.meta.history_reset = {STREAM_TRADES: floor, STREAM_ORDERS: floor}
            return CommandOutcome(CommandStatus.APPLIED, {"history_reset_floor_ms": floor}, reload=True)
        if action == "ack_risk_blocked":
            detail = self.meta.freezes.pop(FREEZE_RISK_BLOCKED, None)
            return CommandOutcome(CommandStatus.APPLIED, {"cleared": FREEZE_RISK_BLOCKED, "detail": detail},
                                  audit=("ack_risk_blocked", {"detail": detail, "note": note}))
        if action == "resolve_unknown_submit":
            cid = int(payload["cid"])
            leg = self.leg_by_cid(cid)
            if leg is None or leg.state != OrderState.SUBMIT_UNKNOWN or leg.filled != 0:
                return CommandOutcome(CommandStatus.REJECTED, {"error": "NOT_AN_UNRESOLVED_ZERO_FILL_SUBMIT"})
            if not self.history_complete or any(r.client_order_id == cid for r in (self.active_rows or [])) \
                    or cid in self.terminal_rows:
                return CommandOutcome(CommandStatus.REJECTED, {"error": "EVIDENCE_EXISTS_OR_HISTORY_INCOMPLETE"})
            s.record_manual_reconciliation(tx, actor, note, dict(evidence, cid=str(cid)),
                                           leg_resolutions={cid: OrderState.REJECTED_ZERO_FILL},
                                           clear_manual_reconcile=False)
            return CommandOutcome(CommandStatus.APPLIED, {"cid": str(cid), "resolution": "not_landed"}, reload=True)
        return CommandOutcome(CommandStatus.REJECTED, {"error": "UNKNOWN_AUDIT_ACTION", "action": action})

    # ================================================================================== account reads
    def _history_stale(self, now: float) -> bool:
        return (self.last_complete_scan_at is None
                or now - self.last_complete_scan_at > float(self.config.history_freshness_s))

    def _position_stale(self, now: float) -> bool:
        return self.position_at is None or now - self.position_at > float(self.config.history_freshness_s)

    def _rules_stale(self, now: float) -> bool:
        return self.rules_at is None or now - self.rules_at > self.options.rules_max_age_s

    async def _refresh_account(self, now: float) -> None:
        try:
            self.bid, self.ask = await self.port.best_bid_ask()
        except Exception as exc:  # noqa: BLE001
            self.bid = self.ask = None
            self._error("BOOK_UNAVAILABLE", f"{type(exc).__name__}: {exc}", now)
        if self.rules is None or now - (self.rules_at or 0) >= self.options.rules_refresh_s:
            weight = self.port.request_weight(ENDPOINT_TRADING_RULES)
            if self._can_spend(now, weight):
                self._charge(now, weight)
                try:
                    self.rules = await self.port.trading_rules()
                    self.rules_at = now
                except Exception as exc:  # noqa: BLE001
                    self._error("RULES_UNAVAILABLE", f"{type(exc).__name__}: {exc}", now)
        due = (self._last_account_poll_at is None
               or now - self._last_account_poll_at >= float(self.config.poll_interval_s)
               or (not self.startup_reconciled
                   and now - self._last_account_poll_at >= self.options.min_wake_interval_s))
        if not due:
            return
        weight = self.port.request_weight(ENDPOINT_ACCOUNT) + self.port.request_weight(ENDPOINT_ACTIVE_ORDERS)
        if not self._can_spend(now, weight):
            self._error("WEIGHT_BUDGET", "account poll delayed by weight budget", now)
            return
        self._charge(now, weight)
        self._last_account_poll_at = now
        try:
            self.position = await self.port.position()
            self.position_at = now
        except Exception as exc:  # noqa: BLE001
            self._error("POSITION_UNAVAILABLE", f"{type(exc).__name__}: {exc}", now)
        try:
            self.active_rows = await self.port.active_orders()
            self.active_at = now
        except Exception as exc:  # noqa: BLE001
            self.active_rows = None
            self._error("ACTIVE_ORDERS_UNAVAILABLE", f"{type(exc).__name__}: {exc}", now)

    # ================================================================================== history
    async def _scan_history(self, now: float) -> None:
        if not self.scanner.should_scan(now):
            return
        one_page_each = self.port.request_weight(ENDPOINT_TRADES) + self.port.request_weight(ENDPOINT_INACTIVE_ORDERS)
        if not self._can_spend(now, one_page_each):
            # Exhaustion delays new exposure (history goes stale), it never skips a proof (NG-HIST-003).
            self._error("WEIGHT_BUDGET", "history scan delayed by weight budget", now)
            return
        result = await self.scanner.scan()
        self._charge(now, result.weight_used)
        for stream, pages in result.pages_read.items():
            self.last_scan_pages[stream] = pages
        in_progress = not result.complete and result.incomplete_reason in ("in_progress", "backoff")
        if self.bootstrapped:
            self._apply_history(result, now, in_progress)
        else:
            self._apply_prebootstrap(result)
        if result.complete:
            self.history_complete = True
            self.history_incomplete_reason = None
            self.last_complete_scan_at = now
            if not self.startup_reconciled and self.active_rows is not None and self.position is not None:
                self.startup_reconciled = True
            self._update_bootstrap_probe(now)
        elif not in_progress:
            self.history_complete = False
            self.history_incomplete_reason = result.incomplete_reason
            self.bootstrap_probe = None
            self._error("HISTORY_INCOMPLETE", f"{result.incomplete_reason}: {result.conflicts[:3]}", now)

    def _remember_committed(self, result) -> None:
        domain = self.port.domain
        for row in result.new_trades:
            self.committed[STREAM_TRADES][row.dedupe_key(domain)] = trade_payload_fingerprint(row)
            self.ws_pending.pop(row.trade_id_str, None)
            if row.own_client_order_id is not None:
                self.ws_pending.pop(str(row.own_client_order_id), None)
        for row in result.new_orders:
            self.committed[STREAM_ORDERS][c_order_key(domain, row)] = order_payload_fingerprint(row)

    def _apply_prebootstrap(self, result) -> None:
        """Before bootstrap every row is pre-cut; only the cut (high-water) matters and it stays in memory."""
        self.bootstrap_rows_seen += len(result.new_trades) + len(result.new_orders)
        self._remember_committed(result)
        if result.complete:
            if result.trades_high_water is not None:
                self.high_water[STREAM_TRADES] = result.trades_high_water
            if result.orders_high_water is not None:
                self.high_water[STREAM_ORDERS] = result.orders_high_water

    def _update_bootstrap_probe(self, now: float) -> None:
        if self.bootstrapped or self.position is None:
            return
        key = (str(self.position.net_base), self.high_water.get(STREAM_TRADES), self.high_water.get(STREAM_ORDERS))
        probe = self.bootstrap_probe
        if probe is not None and probe["key"] == key:
            probe["scans"] += 1
        else:
            self.bootstrap_probe = {"key": key, "scans": 1, "since": now, "scan_ms": _ms(now)}

    def _apply_history(self, result, now: float, in_progress: bool) -> None:
        """Inbox + dedupe + attribution + leg transitions + cursor: ONE store transaction (NG-HIST-002)."""
        s = self.store
        now_ms = _ms(now)
        domain = self.b_engine.connector_domain
        rows: List[Any] = list(result.new_orders) + list(result.new_trades)
        order_rows = {b_order_key(domain, row): row for row in result.new_orders}
        conflicts = list(result.conflicts)
        touched_terminal: Dict[int, ExchangeOrderRow] = {}
        cursor_updates: List[CursorUpdate] = []
        if result.complete:
            for c_stream, hw in ((STREAM_TRADES, result.trades_high_water), (STREAM_ORDERS, result.orders_high_water)):
                ts = self.cursor_ts.get(c_stream, 0)
                if hw is not None:
                    ts = max(ts, HighWaterMark.decode(hw).timestamp_ms)
                cursor_updates.append(CursorUpdate(stream=_C_TO_B[c_stream], high_water=hw, high_water_ts_ms=ts,
                                                   complete=True, last_full_scan_ms=now_ms))
        elif not in_progress:
            for b_stream in _C_TO_B.values():
                cursor_updates.append(CursorUpdate(stream=b_stream, complete=False,
                                                   incomplete_reason=str(result.incomplete_reason)[:500]))
        if not rows and not cursor_updates and not conflicts:
            return

        def transitions(tx, res) -> None:
            for rec in res.applied:
                if rec.stream != B_ORDERS or rec.cid is None:
                    continue
                row = order_rows.get(rec.dedupe_key)
                meta = self.order_meta.get(rec.cid)
                if row is None or meta is None:
                    continue
                if meta.first_terminal_seen_ms is None:
                    meta.first_terminal_seen_ms = now_ms
                meta.terminal_row = order_row_to_json(row)
                s.kv_set(tx, f"om:{rec.cid}", meta.to_json())
                lr = s.leg(rec.cid)
                if lr is not None and not lr.final and lr.state not in (OrderState.TERMINAL_UNKNOWN,
                                                                        OrderState.INTENT):
                    s.set_leg_state(tx, rec.cid, OrderState.TERMINAL_UNKNOWN, reason="exact terminal row seen")
                touched_terminal[rec.cid] = row
            for fill in res.new_fills:
                if fill.role == LegRole.ENTRY:
                    self.meta.obligations.setdefault(f"{fill.cell_id}:{fill.generation}", now_ms)
            gaps = [c for c in conflicts if c.startswith("retention_gap")]
            others = [c for c in conflicts if not c.startswith("retention_gap")]
            already = self.b_engine.manual_reconcile_required
            if gaps and not already:
                s.mark_manual_reconcile_required(tx, "history retention gap: required overlap boundary is older "
                                                     "than the venue's available history (AC-53)",
                                                 evidence={"conflicts": gaps[:10]})
            if others and not already:
                s.mark_manual_reconcile_required(tx, f"history conflict: {others[0]}"[:1900],
                                                 evidence={"conflicts": others[:10]})
            s.kv_set(tx, "engine_meta", self.meta.to_json())

        res = s.apply_history_batch(None, rows, cursor_updates, transitions, batch_id=f"scan-{now_ms}")
        # Projection only after the commit succeeded.
        late_cids = {f.cid for f in res.late_fills}
        for fill in res.new_fills:
            ledger = self.cells.get(fill.cell_id)
            identity = s.identity_for_cid(fill.cid)
            if ledger is not None and identity is not None:
                ledger.fills[fill.dedupe_key] = LedgerFill(identity=identity, qty=fill.size, price=fill.price,
                                                           side=fill.own_side)
            self.trades_by_cid.setdefault(fill.cid, []).append(self._trade_from_fill(fill))
        for cid in sorted(res.touched_cids | set(touched_terminal)):
            self._mirror(cid)
            if cid in late_cids:
                leg = self.leg_by_cid(cid)
                if leg is not None:
                    leg.late_evidence = True
        self.terminal_rows.update(touched_terminal)
        self._remember_committed(result)
        if result.complete:
            for c_stream, hw in ((STREAM_TRADES, result.trades_high_water), (STREAM_ORDERS, result.orders_high_water)):
                if hw is not None:
                    self.high_water[c_stream] = hw
                    self.cursor_ts[c_stream] = max(self.cursor_ts.get(c_stream, 0),
                                                   HighWaterMark.decode(hw).timestamp_ms)
            cleared = [c for c, hw in ((STREAM_TRADES, result.trades_high_water),
                                       (STREAM_ORDERS, result.orders_high_water))
                       if hw is not None and c in self.meta.history_reset]
            if cleared:
                for c_stream in cleared:
                    del self.meta.history_reset[c_stream]
                with s.transaction() as tx:
                    s.kv_set(tx, "engine_meta", self.meta.to_json())
        self.last_history_commit_at = now
        self._refresh_store_facts()

    # ================================================================================== active list
    def _reconcile_active(self, now: float) -> None:
        """Active list proves acceptance; disappearance is never a terminal proof (TERMINAL_UNKNOWN)."""
        if self.active_rows is None or self.active_at is None or self.active_at == self._active_reconciled_at:
            return
        self._active_reconciled_at = self.active_at
        polled_ms = _ms(self.active_at)
        owned: Dict[int, ExchangeOrderRow] = {}
        unknown: List[ExchangeOrderRow] = []
        by_exchange = {leg.exchange_order_id: leg.cid for leg in self.all_legs() if leg.exchange_order_id}
        for row in self.active_rows:
            cid = row.client_order_id if row.client_order_id in self.order_meta else None
            if cid is None and (row.order_index or row.order_id) in by_exchange:
                cid = by_exchange[row.order_index or row.order_id]
            if cid is None:
                unknown.append(row)
            else:
                owned[cid] = row
        self.unknown_active = unknown
        if not self.bootstrapped:
            return
        s = self.store
        changed: Set[int] = set()
        with s.transaction() as tx:
            ids = [r.client_order_id for r in unknown if r.client_order_id is not None]
            if ids:
                s.note_foreign_cids(tx, ids, "active_orders")
            for leg in self.all_legs():
                if leg.cid is None or leg.state == OrderState.INTENT:
                    continue
                row = owned.get(leg.cid)
                if row is not None:
                    if leg.state in FINAL_STATES:
                        s.mark_manual_reconcile_required(tx, f"final order {leg.cid} ({leg.state.value}) is still "
                                                             f"active on the venue")
                        continue
                    key = (row.order_index or row.order_id, row.filled_base_amount)
                    if self._active_evidence_cache.get(leg.cid) != key:
                        s.record_order_evidence(tx, row, final=False)
                        self._active_evidence_cache[leg.cid] = key
                        changed.add(leg.cid)
                    if leg.state in (OrderState.SUBMIT_UNKNOWN, OrderState.TERMINAL_UNKNOWN) \
                            and leg.cid not in self.terminal_rows:
                        s.set_leg_state(tx, leg.cid, OrderState.LIVE, reason="seen in active orders")
                        changed.add(leg.cid)
                elif leg.state in (OrderState.LIVE, OrderState.CANCEL_PENDING, OrderState.CANCEL_UNKNOWN) \
                        and self._sent_before(leg.cid, polled_ms):
                    s.set_leg_state(tx, leg.cid, OrderState.TERMINAL_UNKNOWN,
                                    reason="absent from active orders (not a terminal proof)")
                    changed.add(leg.cid)
        for cid in changed:
            self._mirror(cid)
        if unknown or changed:
            self._refresh_store_facts()

    def _sent_before(self, cid: int, polled_ms: int) -> bool:
        """Only an active list fetched after the order was sent can say anything about its absence."""
        meta = self.order_meta.get(cid)
        return meta is not None and meta.intent_ms < polled_ms

    # ================================================================================== settlement
    def _settle(self, now: float) -> None:
        if not self.bootstrapped:
            return
        s = self.store
        conflicts: List[str] = []
        terminal: List[int] = []
        for leg in self.non_final_legs():
            if leg.cid is None or leg.cid not in self.terminal_rows:
                continue
            meta = self.order_meta[leg.cid]
            decision = evaluate_terminal_release(
                terminal_row=self.terminal_rows[leg.cid],
                owned_trades=self.trades_by_cid.get(leg.cid, []),
                history_complete=self.history_complete,
                first_terminal_seen_at=(None if meta.first_terminal_seen_ms is None
                                        else meta.first_terminal_seen_ms / 1000),
                complete_scans=list(self.scanner.completed_scans),
                now=now,
                settlement_delay_s=self.config.settlement_delay_s,
                settlement_scans=self.config.settlement_scans,
            )
            if decision.conflict:
                conflicts.append(f"cid {leg.cid}: {decision.reason}")
            elif decision.release:
                terminal.append(leg.cid)
        if terminal or conflicts:
            with s.transaction() as tx:
                for cid in terminal:
                    s.set_leg_state(tx, cid, OrderState.TERMINAL,
                                    reason="terminal row + complete scan + cumulative equality + settlement")
                    self._close_open_outbox(tx, cid)
                for c in conflicts:
                    s.mark_manual_reconcile_required(tx, f"settlement conflict {c}"[:1900])
            for cid in terminal:
                self._mirror(cid)
            if conflicts:
                self._refresh_store_facts()
        dust_changes: List[Tuple[int, int, Decimal]] = []
        releases: List[int] = []
        rules_ok = not grid.rules_blockers(self.rules)
        for ledger in self.cells.values():
            if rules_ok:
                for gen, dust in ledger.refresh_dust(self.rules).items():
                    dust_changes.append((ledger.cell_id, gen, dust))
            if ledger.current is not None and not _retry_only(ledger.current) \
                    and ledger.can_release(self.position_reconciled).ok:
                releases.append(ledger.cell_id)
        if not dust_changes and not releases:
            return
        released: List[Tuple[int, int]] = []
        with s.transaction() as tx:
            for cell_id, gen, dust in dust_changes:
                s.update_cycle(tx, self.grid_id, cell_id, gen, dust=dust)
            for cell_id in releases:
                gen = self.cells[cell_id].current.generation
                s.close_cycle(tx, self.grid_id, cell_id, gen, reason="NG-CELL-001 release proven")
                s.set_cell_state(tx, self.grid_id, cell_id, CellState.IDLE, blocker=None, reserved_slots=0)
                self.meta.obligations.pop(f"{cell_id}:{gen}", None)
                released.append((cell_id, gen))
            s.kv_set(tx, "engine_meta", self.meta.to_json())
        for cell_id, gen in released:
            self.cells[cell_id].release(self.position_reconciled)
            self.reservations[cell_id] = 0
            self.persisted_cell_state[cell_id] = (CellState.IDLE.value, None, 0)

    def _close_open_outbox(self, tx, cid: int) -> None:
        """A proven-final leg may still have an unresolved outbox row (crash between dispatch and result): record
        what is known so the cycle can be released (NOT_SENT only for a never-dispatched PENDING row)."""
        s = self.store
        for row in s.outbox_for_cid(cid):
            if row.status == "DONE":
                continue
            if row.status == "PENDING":
                result = TransportResult(TransportOutcome.NOT_SENT, "leg proven terminal; request never dispatched")
            else:
                result = TransportResult(TransportOutcome.UNKNOWN, "leg proven terminal by history; transport "
                                                                   "outcome of this request never recorded")
            s.record_transport_result(tx, cid, result, kind=row.kind)

    # ================================================================================== drift
    def _detect_drift(self, now: float) -> None:
        if not self.bootstrapped:
            self.endpoints = None
            self.position_reconciled = False
            return
        self.endpoints = risk.endpoints_from_ledgers(self.effective_baseline, list(self.cells.values()))
        if self.position is None or self._position_stale(now) or (
                self.last_history_commit_at is not None and self.position_at < self.last_history_commit_at):
            # A position read older than the ledger cannot be compared with it (it may predate a committed fill).
            self.position_reconciled = False
            return
        venue = self.position.net_base
        ep = self.endpoints
        if self.history_complete and not self._history_stale(now) and not (ep.P_min <= venue <= ep.P_max):
            if not self.b_engine.manual_reconcile_required:
                with self.store.transaction() as tx:
                    self.store.mark_manual_reconcile_required(
                        tx, f"position drift: venue {venue} outside ledger reachable [{ep.P_min}, {ep.P_max}]; "
                            f"baseline audit required", evidence={"venue": str(venue), "P": str(ep.P)})
                self._refresh_store_facts()
        if venue == ep.P:
            self.position_gap_since = None
        elif self.position_gap_since is None:
            self.position_gap_since = now
        self.position_reconciled = venue == ep.P and not self.store_entry_blockers

    # ================================================================================== state
    def _outside_bounds(self) -> bool:
        if self.bid is None or self.ask is None:
            return False
        return self.bid > self.config.upper_price or self.ask < self.config.lower_price

    def _evaluate_state(self, now: float) -> None:
        entry: List[str] = []
        tp: List[str] = []
        if self.fatal_reason is not None:
            entry.append(self.fatal_reason)
            tp.append(self.fatal_reason)
        if self.persistence_error is not None:
            entry.append("PERSISTENCE_FAILURE")
            tp.append("PERSISTENCE_FAILURE")
        for code in sorted(self.meta.freezes):
            entry.append(f"FROZEN:{code}")
            if code in _FREEZES_BLOCKING_TP:
                tp.append(f"FROZEN:{code}")
        entry.extend(f"STORE:{b}" for b in self.store_entry_blockers)
        if any(c.kind != "LATE_FILL" for c in self.open_conflicts):
            tp.append("HISTORY_CONFLICT")
        if not self.bootstrapped:
            entry.append("BASELINE_NOT_CONFIRMED")
            tp.append("BASELINE_NOT_CONFIRMED")
        if not self.meta.started:
            entry.append("AWAITING_START")
        if not self.startup_reconciled:
            entry.append("RECONCILING")
            tp.append("RECONCILING")
        if self.unknown_active:
            entry.append("UNKNOWN_ACTIVE_ORDER")
            if not self.meta.ever_normal:
                tp.append("UNKNOWN_ACTIVE_ORDER_AT_STARTUP")
        if not self.history_complete:
            entry.append(f"HISTORY_INCOMPLETE:{self.history_incomplete_reason}")
        elif self._history_stale(now):
            entry.append("HISTORY_STALE")
        rule_errors = grid.rules_blockers(self.rules)
        if rule_errors:
            tp.extend(rule_errors)
        elif self._rules_stale(now):
            entry.append("RULES_STALE")
        if self.mid is None:
            entry.append("BOOK_UNKNOWN")
        self.margin_warning = None
        self.margin_required = None
        if self.endpoints is not None:
            self.margin_required = risk.required_margin_estimate(
                self.endpoints.P_min, self.endpoints.P_max, self.mid, self.config.leverage)
            entry.extend(risk.cap_violations(self.endpoints, self.limits))
        position = self.position
        fresh = position is not None and not self._position_stale(now)
        exposure, self.margin_warning = risk.exposure_blockers(risk.ExposureInputs(
            position_known=True if fresh else None,
            leverage_ok=None if position is None or position.leverage is None
            else position.leverage == self.config.leverage,
            position_mode_ok=None if position is None or position.margin_mode is None else True,
            market_active=None if self.mid is None or self.rules is None else bool(self.rules.supports_limit),
            rules=self.rules,
            history_complete=self.history_complete and not self._history_stale(now),
            account_identity_ok=self.port.account_index == self.config.account_index,
            data_age_s=None if self.position_at is None else Decimal(str(round(now - self.position_at, 3))),
            freshness_limit_s=self.config.history_freshness_s,
            margin_available=None if position is None else position.available_collateral,
            margin_required=self.margin_required if self.margin_required is not None else Decimal("0")))
        entry.extend(exposure)
        if self.position_gap_since is not None and now - self.position_gap_since > self.options.position_gap_grace_s:
            entry.append("POSITION_UNRECONCILED")
        if self._outside_bounds():
            entry.append("OUTSIDE_BOUNDS")
        if self.meta.operator_paused:
            entry.append("OPERATOR_PAUSE")
        stopping = self.meta.stop_requested_ms is not None
        if stopping:
            entry.append("STOP_REQUESTED")
            tp.append("STOP_REQUESTED")
        self.entry_blockers = entry
        self.tp_blockers = tp

        late = any(c.kind == "LATE_FILL" for c in self.open_conflicts)
        conflict = any(c.kind != "LATE_FILL" for c in self.open_conflicts)
        if stopping:
            state = EngineState(self.meta.stop_outcome) if self.meta.stop_outcome else EngineState.STOPPING
        elif self.fatal_reason is not None or self.persistence_error is not None:
            state = EngineState.DEGRADED
        elif late or conflict or FREEZE_INVARIANT in self.meta.freezes or (
                self.b_engine is not None and self.b_engine.manual_reconcile_required
                and "conflict" in (self.b_engine.manual_reconcile_reason or "")):
            state = EngineState.FROZEN
        elif self.meta.freezes or self.store_entry_blockers or (self.unknown_active and not self.meta.ever_normal) \
                or any(e.startswith(("NET_CAP", "GROSS_CAP", "MARGIN_UNKNOWN", "ACCOUNT_IDENTITY")) for e in entry):
            state = EngineState.RISK_BLOCKED
        elif not self.bootstrapped:
            state = EngineState.BOOTSTRAPPING
        elif not self.startup_reconciled or not self.history_complete:
            state = EngineState.RECONCILING
        elif any(e in ("BOOK_UNKNOWN", "RULES_STALE", "HISTORY_STALE")
                 or e.startswith(("RULES_", "POSITION_KNOWN", "LEVERAGE_OK", "POSITION_MODE", "MARKET_ACTIVE",
                                  "FRESHNESS")) for e in entry):
            state = EngineState.DEGRADED
        elif entry:
            state = EngineState.PAUSED
        else:
            state = EngineState.NORMAL
            self.meta.ever_normal = True
        self.engine_state = state
        self.reasons = sorted(set(entry) | set(tp))

    # ================================================================================== planning
    def _slot_admission(self) -> Optional[admission.AdmissionPlan]:
        if grid.rules_blockers(self.rules) or not self.cells:
            return None
        cells = []
        for cell_id in sorted(self.cells):
            ledger = self.cells[cell_id]
            cur = ledger.current
            open_cycle = (cur is not None and not _retry_only(cur)) or any(
                c.has_open_obligation_or_orders() for c in ledger.cycles if c is not cur)
            eligible, why = True, None
            if not open_cycle:
                eligible, why = self._idle_eligibility(ledger)
            cells.append(admission.CellAdmission(
                cell=ledger.spec, open_cycle=open_cycle, actual_orders=len(ledger.non_final_legs()),
                reserved_slots=self.reservations.get(cell_id, 0) if open_cycle else 0,
                eligible=eligible, blocker=why,
                slot_need=admission.slot_need_from_ledger(ledger, self.rules) if open_cycle else None))
        return admission.plan(cells, cap=self.config.max_active_orders, mid=self.mid, rules=self.rules,
                              order_amount_base=self.config.order_amount_base,
                              entries_allowed=not self.entry_blockers,
                              entries_blocker=self.entry_blockers[0] if self.entry_blockers else None)

    def _idle_eligibility(self, ledger: CellLedger) -> Tuple[bool, Optional[str]]:
        spec = ledger.spec
        try:
            ledger.next_entry_identity()
        except LedgerError as exc:
            return False, f"LOCKED:{exc}"
        if spec.entry_side == Side.BUY and (self.ask is None or spec.entry_price >= self.ask):
            return False, "ENTRY_WOULD_CROSS"
        if spec.entry_side == Side.SELL and (self.bid is None or spec.entry_price <= self.bid):
            return False, "ENTRY_WOULD_CROSS"
        return True, None

    def _tp_candidates(self, now_ms: int) -> List[Tuple[router.RouterIntent, int, int, Decimal]]:
        out = []
        for cell_id, ledger in sorted(self.cells.items()):
            plan = ledger.tp_obligation_to_dispatch(self.rules)
            if plan.blocker:
                self.cell_blockers[cell_id] = plan.blocker
            for index, item in enumerate(plan.items):
                since = self.meta.obligations.setdefault(f"{cell_id}:{item.generation}", now_ms)
                intent = router.RouterIntent(key=f"tp:{cell_id}:{item.generation}:{index}",
                                             side=ledger.spec.tp_side, price=ledger.spec.tp_price, qty=item.qty,
                                             role=LegRole.TP, cell_id=cell_id, seq=since)
                out.append((intent, cell_id, item.generation, item.qty))
        return out

    async def _act(self, now: float) -> None:
        self.cell_blockers = {}
        self.router_plan = None
        if not self.bootstrapped:
            self.admission_plan = None
            return
        cancels: Dict[int, str] = {}
        if self.meta.stop_requested_ms is not None:
            await self._withdraw_pending_intents("STOP")
            for leg in self.non_final_legs():
                if leg.state in CANCELLABLE or self._cancel_retry_due(leg, now):
                    cancels[leg.cid] = "STOP"
            await self._dispatch_cancels(cancels, now)
            self.admission_plan = self._slot_admission()
            return
        if self.tp_blockers:
            self.admission_plan = self._slot_admission()
            if "PERSISTENCE_FAILURE" not in self.tp_blockers and self.startup_reconciled:
                for leg in self.non_final_legs():
                    if self._cancel_retry_due(leg, now):
                        cancels[leg.cid] = "CANCEL_RETRY"
                await self._dispatch_cancels(cancels, now)
            return
        await self._resume_pending_intents(now)
        if self._outside_bounds():
            for leg in self.non_final_legs():
                if leg.identity.role == LegRole.ENTRY and leg.state in CANCELLABLE:
                    cancels[leg.cid] = "OUTSIDE_BOUNDS"
        for leg in self.non_final_legs():
            if self._cancel_retry_due(leg, now):
                cancels[leg.cid] = "CANCEL_RETRY"
        plan = self._slot_admission()
        self.admission_plan = plan
        if plan is None:
            await self._dispatch_cancels(cancels, now)
            return
        self._persist_reservations(plan)
        now_ms = _ms(now)
        tp_items = self._tp_candidates(now_ms)
        entry_items = []
        for rank, cell_id in enumerate(plan.newly_armed):
            spec = self.cells[cell_id].spec
            entry_items.append(router.RouterIntent(key=f"entry:{cell_id}", side=spec.entry_side,
                                                   price=spec.entry_price, qty=self.config.order_amount_base,
                                                   role=LegRole.ENTRY, cell_id=cell_id, seq=now_ms * 1000 + rank))
        owned = [router.RouterOrder.from_leg(leg) for leg in self.non_final_legs()]
        ep = risk.endpoints_from_ledgers(self.effective_baseline, list(self.cells.values()))
        rplan = router.plan_submits([i for i, *_ in tp_items] + entry_items, owned, endpoints=ep,
                                    limits=self.limits, slots=router.SlotBudget.from_plan(plan), mid=self.mid,
                                    entries_allowed=not self.entry_blockers,
                                    entries_blocker=self.entry_blockers[0] if self.entry_blockers
                                    else "ENTRIES_BLOCKED",
                                    owed=risk.obligation_totals(list(self.cells.values())))
        self.router_plan = rplan
        for action in rplan.actions:
            if action.kind == router.ActionKind.CANCEL:
                cancels.setdefault(int(action.key), action.reason)
            elif action.kind == router.ActionKind.BLOCKED:
                self.meta.freezes.setdefault(FREEZE_RISK_BLOCKED, action.reason)
            elif action.kind == router.ActionKind.WAIT:
                self.cell_blockers.setdefault(int(action.key.split(":")[1]), action.reason)
        for key in rplan.withdraws:
            await self._withdraw(int(key), "ROUTER_WITHDRAW")
        await self._dispatch_cancels(cancels, now)
        submits = set(rplan.submits)
        for intent, cell_id, generation, qty in tp_items:
            if intent.key not in submits:
                continue
            if self._submits_this_tick >= self.options.max_submits_per_tick or self.persistence_error:
                self.cell_blockers.setdefault(cell_id, "TP_WAITS_THROTTLE")
                continue
            await self._submit(self.cells[cell_id], LegRole.TP, generation, qty, now, since_ms=intent.seq)
        for intent in entry_items:
            if intent.key not in submits:
                continue
            if self._submits_this_tick >= self.options.max_submits_per_tick or self.persistence_error:
                break
            await self._submit(self.cells[intent.cell_id], LegRole.ENTRY, None, intent.qty, now,
                               reserved_slots=plan.reservations.get(intent.cell_id, 0))

    def _persist_reservations(self, plan: admission.AdmissionPlan) -> None:
        changed = {cell_id: slots for cell_id, slots in plan.reservations.items()
                   if self.reservations.get(cell_id, 0) != slots and self.cells[cell_id].current is not None}
        if not changed:
            return
        with self.store.transaction() as tx:
            for cell_id, slots in changed.items():
                self.store.set_cell_state(tx, self.grid_id, cell_id, self.cells[cell_id].primary_state(),
                                          reserved_slots=slots)
        for cell_id, slots in changed.items():
            self.reservations[cell_id] = slots

    def _cancel_retry_due(self, leg: Leg, now: float) -> bool:
        if leg.state != OrderState.CANCEL_UNKNOWN or leg.cid is None:
            return False
        meta = self.order_meta.get(leg.cid)
        if meta is None or meta.cancel_sent_ms is None:
            return True
        still_active = any(r.client_order_id == leg.cid for r in (self.active_rows or []))
        return still_active and now - meta.cancel_sent_ms / 1000 >= self.options.cancel_retry_s

    # ================================================================================== dispatch
    async def _call(self, coro) -> TransportResult:
        self.transport_calls += 1
        try:
            result = await asyncio.wait_for(coro, timeout=self.options.transport_timeout_s)
        except asyncio.TimeoutError:
            return TransportResult(TransportOutcome.UNKNOWN, "transport timeout")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - an ordinary exception is NOT a rejection (NG-DB-005)
            return TransportResult(TransportOutcome.UNKNOWN, f"transport exception {type(exc).__name__}: {exc}")
        if not isinstance(result, TransportResult):
            return TransportResult(TransportOutcome.UNKNOWN, "malformed transport result")
        return result

    def _pre_send_blocker(self, req: SubmitRequest) -> Optional[str]:
        if req.reduce_only:
            return "REDUCE_ONLY_FORBIDDEN"
        if req.order_type not in (OrderTypePolicy.LIMIT_MAKER, OrderTypePolicy.LIMIT):
            return "ORDER_TYPE_FORBIDDEN"
        rules = self.rules
        if rules is None or grid.rules_blockers(rules):
            return "RULES_UNKNOWN"
        try:
            grid.to_ticks(req.price, rules.tick_size)
        except grid.GridValidationError as exc:
            return f"PRICE_NOT_ON_TICK:{exc}"
        return grid.order_qty_blocker(req.amount, req.price, rules)

    def _is_foreign_cid(self, cid: int) -> bool:
        return any(r.client_order_id == cid for r in (self.active_rows or []))

    async def _submit(self, ledger: CellLedger, role: LegRole, generation: Optional[int], qty: Decimal, now: float,
                      since_ms: Optional[int] = None, reserved_slots: int = 0) -> bool:
        """Intent + CID + reservation committed BEFORE transport; result committed after (NG-DB-002)."""
        s = self.store
        rules = self.rules
        spec = ledger.spec
        now_ms = _ms(now)
        if role == LegRole.ENTRY:
            order_type = self.config.entry_order_type
            side, price, expiry_ms = spec.entry_side, spec.entry_price, None
        else:
            order_type = self.config.tp_order_type
            side, price = spec.tp_side, spec.tp_price
            expiry_ms = now_ms + self.config.tp_gtt_seconds * 1000
        self.meta.seq += 1
        seq = self.meta.seq
        try:
            with s.transaction() as tx:
                if role == LegRole.ENTRY:
                    if ledger.current is None:
                        s.open_cycle(tx, self.grid_id, ledger.cell_id, planned_amount=self.config.order_amount_base)
                    identity = ledger.next_entry_identity()
                else:
                    identity = ledger.next_tp_identity(generation)
                cid = s.allocate_cid(tx, identity, is_foreign_cid=self._is_foreign_cid)
                req = SubmitRequest(client_order_id=cid, side=side, price=price, amount=qty, order_type=order_type,
                                    reduce_only=False, expiry_ms=expiry_ms)
                intent = s.record_intent(tx, identity, req, Reservation(side=side, amount=qty, slots=1),
                                         reason=f"{role.value} intent")
                if role == LegRole.ENTRY:
                    leg = ledger.begin_entry(cid, rules, order_type=order_type, seq=seq)
                    s.set_cell_state(tx, self.grid_id, ledger.cell_id, CellState.ENTRY_INTENT, blocker=None,
                                     reserved_slots=max(reserved_slots, 1))
                else:
                    leg = ledger.add_tp_intent(qty, cid, rules, generation=generation, order_type=order_type,
                                               expiry_ms=expiry_ms, seq=seq)
                if leg.identity != identity:
                    raise LedgerError(f"identity drift {leg.identity} != {identity}")
                meta = OrderMeta(cid=cid, cell_id=ledger.cell_id, generation=identity.generation, role=role.value,
                                 intent_ms=now_ms, seq=seq, obligation_ms=since_ms)
                if role == LegRole.TP:
                    remaining = ledger.tp_obligation_to_dispatch(rules)
                    if not any(i.generation == identity.generation for i in remaining.items):
                        self.meta.obligations.pop(f"{ledger.cell_id}:{identity.generation}", None)
                s.kv_set(tx, f"om:{cid}", meta.to_json())
                s.kv_set(tx, "engine_meta", self.meta.to_json())
        except EntryBlockedError as exc:
            self.reload_needed = True
            self.cell_blockers[ledger.cell_id] = f"STORE_ENTRY_BLOCKED:{exc}"
            return False
        except CidAllocationError as exc:
            self.reload_needed = True
            self.meta.freezes[FREEZE_CID] = f"{type(exc).__name__}: {exc}"
            self._error("CID_ALLOCATION", str(exc), now)
            return False
        except (StoreError, LedgerError):
            self.reload_needed = True
            raise
        self.order_meta[cid] = meta
        if role == LegRole.ENTRY:
            self.reservations[ledger.cell_id] = max(reserved_slots, 1)
            self.persisted_cell_state[ledger.cell_id] = (CellState.ENTRY_INTENT.value, None,
                                                         self.reservations[ledger.cell_id])
        self._submits_this_tick += 1
        if role == LegRole.TP and since_ms is not None:
            self.tp_latencies.append((cid, (now_ms - since_ms) / 1000))
        await self._dispatch_intent(cid, intent.outbox_id, req)
        return True

    async def _dispatch_intent(self, cid: int, outbox_id: int, req: SubmitRequest) -> None:
        s = self.store
        blocker = self._pre_send_blocker(req)
        if blocker is not None:
            # Proven no transport call: the intent is released (NG-DB-005, AC-56).
            s.record_transport_result(None, cid, TransportResult(TransportOutcome.NOT_SENT,
                                                                 f"pre-send validation: {blocker}"), kind="SUBMIT")
            self._mirror(cid)
            return
        with s.transaction() as tx:
            s.mark_dispatching(tx, outbox_id)
        self._mirror(cid)
        s.fault_point("before_transport")
        result = await self._call(self.port.submit(req))
        s.fault_point("after_transport")
        s.record_transport_result(None, cid, result, kind="SUBMIT")
        meta = self.order_meta.get(cid)
        if meta is not None and result.detail:
            meta.transport_detail = result.detail[:500]
        self._mirror(cid)

    async def _withdraw(self, cid: int, reason: str) -> None:
        leg = self.leg_by_cid(cid)
        if leg is None or leg.state != OrderState.INTENT:
            return
        self.store.record_transport_result(None, cid, TransportResult(TransportOutcome.NOT_SENT, reason),
                                           kind="SUBMIT")
        self._mirror(cid)

    def _pending_submit_outbox(self, cid: int) -> Optional[Any]:
        outbox = [o for o in self.store.outbox_for_cid(cid) if o.kind == "SUBMIT"]
        return outbox[-1] if outbox and outbox[-1].status == "PENDING" else None

    async def _withdraw_pending_intents(self, reason: str) -> None:
        for leg in self.non_final_legs():
            if leg.state == OrderState.INTENT and leg.cid is not None and self._pending_submit_outbox(leg.cid):
                await self._withdraw(leg.cid, f"{reason}: proven unsent intent withdrawn")

    async def _resume_pending_intents(self, now: float) -> None:
        """Restart after a crash between intent and dispatch: the PENDING outbox row proves the venue never saw
        the request, so it is dispatched with the SAME CID (never a new one, AC-16) or withdrawn if no longer
        allowed."""
        for leg in self.non_final_legs():
            if leg.state != OrderState.INTENT or leg.cid is None:
                continue
            pending = self._pending_submit_outbox(leg.cid)
            if pending is None:
                continue
            if leg.identity.role == LegRole.ENTRY and self.entry_blockers:
                await self._withdraw(leg.cid, "entries blocked after restart")
                continue
            await self._dispatch_intent(leg.cid, pending.id, self.store.leg(leg.cid).submit_request())
        for leg in self.non_final_legs():
            if leg.cid is not None and leg.state == OrderState.CANCEL_PENDING and any(
                    o.kind == "CANCEL" and o.status == "PENDING" for o in self.store.outbox_for_cid(leg.cid)):
                await self._dispatch_cancels({leg.cid: "RESUME_PENDING_CANCEL"}, now)

    async def _dispatch_cancels(self, cancels: Dict[int, str], now: float) -> None:
        s = self.store
        for cid, reason in sorted(cancels.items()):
            if self.persistence_error:
                return
            leg = self.leg_by_cid(cid)
            if leg is None or leg.state in FINAL_STATES or leg.state == OrderState.INTENT:
                continue
            meta = self.order_meta[cid]
            with s.transaction() as tx:
                outbox = s.record_cancel_intent(tx, cid, reason)
                meta.cancel_reason = reason
                meta.cancel_requested_ms = _ms(now)
                s.kv_set(tx, f"om:{cid}", meta.to_json())
            self._mirror(cid)
            with s.transaction() as tx:
                s.mark_dispatching(tx, outbox.id)
            self._mirror(cid)
            meta.cancel_sent_ms = self._now_ms()
            s.fault_point("before_transport")
            result = await self._call(self.port.cancel(cid, leg.exchange_order_id))
            s.fault_point("after_transport")
            s.record_transport_result(None, cid, result, kind="CANCEL")
            with s.transaction() as tx:
                s.kv_set(tx, f"om:{cid}", meta.to_json())
            self._mirror(cid)

    # ================================================================================== stop
    def _finish_stop(self, now: float) -> None:
        if self.meta.stop_requested_ms is None:
            return
        open_legs = self.non_final_legs()
        previous = self.meta.stop_outcome
        if not open_legs and self.history_complete:
            inventory = any(c.E != c.X or c.dust > 0 for ledger in self.cells.values() for c in ledger.cycles)
            if self.position is not None and self.position.net_base != 0:
                inventory = True
            outcome: Optional[EngineState] = (EngineState.STOPPED_WITH_INVENTORY if inventory
                                              else EngineState.STOPPED)
        elif any(leg.state in (OrderState.CANCEL_UNKNOWN, OrderState.SUBMIT_UNKNOWN) for leg in open_legs) or \
                now - self.meta.stop_requested_ms / 1000 > self.options.stop_uncertain_after_s:
            outcome = EngineState.STOP_UNCERTAIN
        else:
            outcome = None
        new_value = outcome.value if outcome is not None else None
        if new_value != previous:
            self.meta.stop_outcome = new_value
            with self.store.transaction() as tx:
                self.store.kv_set(tx, "engine_meta", self.meta.to_json())
                self.store.record_audit(tx, "stop_outcome", ACTOR, {"outcome": new_value,
                                                                    "open_orders": len(open_legs)})

    # ================================================================================== snapshot
    def preview(self) -> Dict[str, Any]:
        from hummingbot.strategy_v2.executors.neutral_grid_executor.snapshot import build_preview
        return build_preview(self)

    def _commit_snapshot(self, now: float) -> None:
        from hummingbot.strategy_v2.executors.neutral_grid_executor.snapshot import build_snapshot
        s = self.store
        reason = "; ".join(self.reasons)[:1900] or None
        with s.transaction() as tx:
            s.set_engine_state(tx, self.engine_state, reason, pause_reason=self.meta.pause_reason,
                               stop_reason=self.meta.stop_reason)
            s.kv_set(tx, "engine_meta", self.meta.to_json())
            if self.bootstrapped:
                for cell_id, ledger in self.cells.items():
                    state = ledger.primary_state()
                    blocker = self.cell_blockers.get(cell_id)
                    persisted = (state.value, blocker, self.reservations.get(cell_id, 0))
                    if self.persisted_cell_state.get(cell_id) != persisted:
                        s.set_cell_state(tx, self.grid_id, cell_id, state, blocker=blocker,
                                         reserved_slots=self.reservations.get(cell_id, 0))
                        self.persisted_cell_state[cell_id] = persisted
            self.b_engine = s.engine()
            stored = s.write_snapshot(tx, build_snapshot(self, now))
        self.last_snapshot = stored.payload

    def current_snapshot(self) -> Dict[str, Any]:
        from hummingbot.strategy_v2.executors.neutral_grid_executor.snapshot import build_snapshot
        return build_snapshot(self, self.clock())

    def latest_committed_snapshot(self) -> Optional[Dict[str, Any]]:
        if self.store is None or self.store.closed:
            return self.last_snapshot
        stored = self.store.latest_snapshot()
        return None if stored is None else stored.payload

    def enqueue(self, kind: CommandKind, payload: Optional[Dict[str, Any]] = None, *, key: str,
                expected: Optional[Tuple[int, int]] = None):
        """Enqueue with the revisions of the committed engine row (UI/CLI semantics: stale => CONFLICT)."""
        eng = self.store.engine()
        config_rev, engine_rev = expected if expected is not None else (eng.config_revision, eng.engine_revision)
        return self.store.enqueue_command(key, kind, config_rev, engine_rev, payload or {})

    def cells_view(self) -> Sequence[CellLedger]:
        return [self.cells[i] for i in sorted(self.cells)]
