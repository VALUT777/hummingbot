"""Shared contracts for the neutral fixed-cell grid (spec docs/superpowers/specs/2026-09-24-neutral-grid-spec.md).

This module is the frozen interface between the parallel workstreams (core, store, connector,
engine, web). Change it only additively; any breaking change must be coordinated in
docs/neutral-grid/CONTRACTS.md. All quantities/prices are Decimal, all exchange ids are str,
client order ids are int < 2**48. No float anywhere.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Protocol, Tuple

MAX_CLIENT_ORDER_ID = (1 << 48) - 1


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class LegRole(str, Enum):
    ENTRY = "ENTRY"
    TP = "TP"


class OrderState(str, Enum):
    """Order-level (physical leg) state. Several legs of one cycle may be live at once."""
    INTENT = "INTENT"                      # durable intent + CID committed, transport not yet called
    SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"      # transport called (or maybe called), outcome not proven
    LIVE = "LIVE"                          # proven accepted / seen in active orders or history
    CANCEL_PENDING = "CANCEL_PENDING"      # cancel intent committed
    CANCEL_UNKNOWN = "CANCEL_UNKNOWN"      # cancel transport outcome unknown
    TERMINAL_UNKNOWN = "TERMINAL_UNKNOWN"  # not in active list / cancel ack, but no history proof yet
    TERMINAL = "TERMINAL"                  # exact terminal row + full scan + cumulative equality + settlement
    REJECTED_UNSENT = "REJECTED_UNSENT"    # pre-send validation, proven no transport call
    REJECTED_ZERO_FILL = "REJECTED_ZERO_FILL"  # documented definitive venue rejection with zero fill


class CellState(str, Enum):
    """Aggregate, derived view of a cell; legs carry the real state (ENTRY_LIVE + TP_LIVE is legal)."""
    IDLE = "IDLE"
    QUEUED = "QUEUED"
    ENTRY_INTENT = "ENTRY_INTENT"
    ENTRY_LIVE = "ENTRY_LIVE"
    ENTRY_TERMINAL_UNKNOWN = "ENTRY_TERMINAL_UNKNOWN"
    TP_REQUIRED = "TP_REQUIRED"
    TP_INTENT = "TP_INTENT"
    TP_LIVE = "TP_LIVE"
    TP_TERMINAL_UNKNOWN = "TP_TERMINAL_UNKNOWN"
    DUST = "DUST"
    SETTLING = "SETTLING"
    COMPLETE = "COMPLETE"
    PAUSED = "PAUSED"
    BLOCKED = "BLOCKED"


class EngineState(str, Enum):
    BOOTSTRAPPING = "BOOTSTRAPPING"
    RECONCILING = "RECONCILING"
    NORMAL = "NORMAL"
    DEGRADED = "DEGRADED"
    PAUSED = "PAUSED"
    RISK_BLOCKED = "RISK_BLOCKED"
    FROZEN = "FROZEN"                    # late evidence / history conflict: audit required
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    STOPPED_WITH_INVENTORY = "STOPPED_WITH_INVENTORY"
    STOP_UNCERTAIN = "STOP_UNCERTAIN"


class CommandKind(str, Enum):
    START = "start"
    PAUSE = "pause"
    RESUME = "resume"
    STOP = "stop"
    CONFIRM_BASELINE = "confirm_baseline"
    BASELINE_AUDIT = "baseline_audit"


class CommandStatus(str, Enum):
    QUEUED = "QUEUED"
    APPLIED = "APPLIED"
    REJECTED = "REJECTED"
    CONFLICT = "CONFLICT"


class OrderTypePolicy(str, Enum):
    LIMIT_MAKER = "LIMIT_MAKER"   # post-only
    LIMIT = "LIMIT"               # GTT on Lighter (GoodTillTime=1)
    # MARKET is intentionally absent and must be rejected by config validation.


@dataclass(frozen=True)
class TradingRules:
    tick_size: Decimal
    size_step: Decimal
    min_base: Decimal
    min_notional: Decimal
    max_base: Optional[Decimal]
    max_leverage: Optional[Decimal]
    supports_limit: bool
    supports_post_only: bool
    fetched_at: float           # unix seconds (timestamp only, never quantity)
    max_active_orders_venue: Optional[int] = None
    # Why an ordinary LIMIT may not be sent even when post-only is supported. This contains a fixed public reason
    # code only; it never carries an auth token, venue response text or other secret.
    ordinary_limit_blocker: Optional[str] = None


@dataclass(frozen=True)
class GridConfig:
    grid_id: str
    connector_name: str
    trading_pair: str
    account_index: int
    lower_price: Decimal
    upper_price: Decimal
    cell_count: int
    order_amount_base: Decimal
    leverage: Decimal
    expected_initial_position: Optional[Decimal]
    max_abs_net_position: Decimal
    max_gross_position: Decimal
    max_active_orders: int
    history_freshness_s: Decimal = Decimal("10")
    settlement_delay_s: Decimal = Decimal("5")
    settlement_scans: int = 2
    history_overlap_s: Decimal = Decimal("60")
    poll_interval_s: Decimal = Decimal("5")
    entry_order_type: OrderTypePolicy = OrderTypePolicy.LIMIT_MAKER
    tp_order_type: OrderTypePolicy = OrderTypePolicy.LIMIT
    tp_gtt_seconds: int = 28 * 24 * 3600
    enabled: bool = False
    directional_outside_bounds_entries: bool = False

    def fingerprint(self) -> str:
        """Stable identity of grid dimensions (bounds, N, Q, pair, account). Implemented by core."""
        from hummingbot.strategy_v2.executors.neutral_grid_executor.grid import config_fingerprint
        return config_fingerprint(self)


@dataclass(frozen=True)
class CellSpec:
    cell_id: int                # 0..N-1
    low_price: Decimal          # P[i]
    high_price: Decimal         # P[i+1]
    entry_side: Side            # fixed forever after anchor

    @property
    def entry_price(self) -> Decimal:
        return self.low_price if self.entry_side == Side.BUY else self.high_price

    @property
    def tp_price(self) -> Decimal:
        return self.high_price if self.entry_side == Side.BUY else self.low_price

    @property
    def tp_side(self) -> Side:
        return Side.SELL if self.entry_side == Side.BUY else Side.BUY


@dataclass(frozen=True)
class LegIdentity:
    grid_id: str
    cell_id: int
    generation: int             # cycle number of the cell
    role: LegRole
    revision: int               # child index / renewal revision within (cycle, role)


@dataclass(frozen=True)
class ExchangeOrderRow:
    """Normalized active or inactive order row. Ids as exact strings."""
    client_order_id: Optional[int]
    client_order_id_str: Optional[str]
    order_id: Optional[str]
    order_index: Optional[str]
    nonce: Optional[str]
    account_index: int
    market_id: int
    side: Side
    price: Decimal
    initial_base_amount: Decimal
    filled_base_amount: Decimal
    remaining_base_amount: Decimal
    status: str                 # raw venue status string
    reduce_only: bool
    timestamp_ms: int
    raw_json: str               # canonical JSON of the raw row (for conflict detection/audit)


@dataclass(frozen=True)
class ExchangeTradeRow:
    """One *own* leg of a trade. A self-trade yields two rows (one per own side)."""
    trade_id_str: str
    account_index: int
    market_id: int
    own_side: Side
    own_exchange_order_id: Optional[str]
    own_client_order_id: Optional[int]
    size: Decimal
    price: Decimal
    is_maker: Optional[bool]
    timestamp_ms: int
    raw_json: str

    def dedupe_key(self, domain: str) -> Tuple[str, int, int, str, str, str]:
        return (domain, self.account_index, self.market_id, self.trade_id_str, self.own_side.value,
                self.own_exchange_order_id or "")


@dataclass(frozen=True)
class HistoryPage:
    rows: List[object]          # ExchangeOrderRow or ExchangeTradeRow
    next_cursor: Optional[str]  # opaque, passed back verbatim
    raw_cursor_sent: Optional[str]


@dataclass(frozen=True)
class SubmitRequest:
    client_order_id: int
    side: Side
    price: Decimal
    amount: Decimal
    order_type: OrderTypePolicy
    reduce_only: bool = False   # neutral grid: always False (NG-ORD-002)
    expiry_ms: Optional[int] = None


class TransportOutcome(str, Enum):
    NOT_SENT = "NOT_SENT"                    # proven: transport not invoked (pre-send validation)
    ACCEPTED = "ACCEPTED"                    # venue returned success with tx hash / order evidence
    DEFINITIVE_REJECT_ZERO_FILL = "DEFINITIVE_REJECT_ZERO_FILL"
    UNKNOWN = "UNKNOWN"                      # timeout / exception / anything else


@dataclass(frozen=True)
class TransportResult:
    outcome: TransportOutcome
    detail: str = ""
    exchange_order_id: Optional[str] = None


@dataclass(frozen=True)
class PositionSnapshot:
    net_base: Decimal
    fetched_at: float
    leverage: Optional[Decimal] = None
    margin_mode: Optional[str] = None
    available_collateral: Optional[Decimal] = None


class ExchangePort(Protocol):
    """Everything the engine may ask of an exchange. Implemented by the Lighter adapter and FakeExchange.

    No method may guess ownership by price/size/time. History methods are single-page; the
    HistoryScanner owns pagination/completeness.
    """

    domain: str
    account_index: int
    market_id: int

    async def trading_rules(self) -> TradingRules:
        ...

    async def mid_price(self) -> Optional[Decimal]:
        ...

    async def best_bid_ask(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        ...

    async def position(self) -> PositionSnapshot:
        ...

    async def active_orders(self) -> List[ExchangeOrderRow]:
        ...

    async def inactive_orders_page(self, cursor: Optional[str], limit: int = 100) -> HistoryPage:
        ...

    async def trades_page(self, cursor: Optional[str], limit: int = 100) -> HistoryPage:
        ...

    async def submit(self, req: SubmitRequest) -> TransportResult:
        ...

    async def cancel(self, client_order_id: int, exchange_order_id: Optional[str]) -> TransportResult:
        ...

    def request_weight(self, endpoint: str) -> int:
        ...


@dataclass
class HistoryScanResult:
    complete: bool
    incomplete_reason: Optional[str]
    new_trades: List[ExchangeTradeRow] = field(default_factory=list)
    new_orders: List[ExchangeOrderRow] = field(default_factory=list)
    conflicts: List[str] = field(default_factory=list)
    trades_high_water: Optional[str] = None     # opaque/exact boundary marker for next overlap
    orders_high_water: Optional[str] = None
    pages_read: Dict[str, int] = field(default_factory=dict)
    weight_used: int = 0


@dataclass(frozen=True)
class Snapshot:
    """Committed, versioned snapshot read by UI/CLI. Serialized as JSON with Decimals/ids as strings."""
    snapshot_version: int
    config_revision: int
    engine_revision: int
    committed_at: float
    engine_state: EngineState
    reasons: List[str]
    payload: Dict[str, object]   # summary + cells + orders + history + errors (see CONTRACTS.md)
