"""Engine-level data types for the neutral fixed-cell grid (workstream D).

Shared cross-workstream types live in ``contracts.py``; this module only adds what the engine,
executor, controller, CLI status and the offline web demo need on top of them. No float is used for
quantities or ids; timestamps are unix seconds (float) like in ``contracts``.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, Literal, Optional

from pydantic import model_validator

from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import GridConfig, OrderTypePolicy

SNAPSHOT_VERSION = 1


@dataclass(frozen=True)
class EngineOptions:
    """Operational knobs of the engine that are not part of the grid identity.

    Defaults are design choices (spec NG-ARCH-003), not venue guarantees.
    """
    tick_interval_s: float = 1.0                 # executor control-loop cadence (reads are coalesced below)
    transport_timeout_s: float = 10.0            # submit/cancel longer than this is UNKNOWN (never retried with new CID)
    cancel_retry_s: float = 10.0                 # re-send cancel for a still-active order at most this often
    stop_uncertain_after_s: float = 120.0        # draining longer than this without proofs => STOP_UNCERTAIN
    rules_refresh_s: float = 60.0                # trading rules re-read cadence
    rules_max_age_s: float = 180.0               # older rules block new exposure
    weight_budget_per_min: int = 16200           # 90 % of the Standard 18000/min pool (configurable)
    scan_weight_budget: int = 1400               # max weight one scanner step may spend
    max_scan_pages_per_tick: Optional[int] = None
    max_submits_per_tick: int = 20
    position_gap_grace_s: float = 30.0           # venue != ledger (inside [P_min, P_max]) tolerated as history lag
    history_retention_horizon_s: Optional[float] = None
    tp_dispatch_slo_s: float = 2.0
    snapshot_every_tick: bool = True
    min_wake_interval_s: float = 2.0             # WS wakeups coalesce into at most one extra scan per 2 s
    committed_cache_rows: int = 5000             # recent inbox rows kept as scanner "committed" view on restart
    rules_retry_initial_s: float = 2.0           # failed trading-rules read: exponential backoff from here ...
    rules_retry_max_s: float = 60.0              # ... up to here (the rules read never starves history scans)
    reject_backoff_initial_s: float = 30.0       # a latched reject is retried with unchanged rules after this ...
    reject_backoff_max_s: float = 3600.0         # ... doubling up to this
    normal_hysteresis_ticks: int = 3             # back to NORMAL after DEGRADED only after this many clean ticks
    health_heartbeat_s: float = 10.0             # health sidecar rewritten at least this often (older = unknown)
    # An unknown submit may be audited as 'never landed' only with evidence taken this long after its dispatch
    # (never less than settlement_delay_s): a venue active list lagging longer is undetectable (operator risk).
    unknown_resolution_delay_s: float = 120.0

    @property
    def rules_max_age_published_s(self) -> float:
        """Staleness bound the engine publishes with ``runtime_rules`` (the UI's attach gate)."""
        return max(self.rules_max_age_s, 3 * self.rules_refresh_s)


def grid_config_to_json(cfg: GridConfig) -> Dict[str, Any]:
    """Exact JSON form of a GridConfig (Decimals as strings)."""
    out: Dict[str, Any] = {}
    for name, value in cfg.__dict__.items():
        if name in ("directional_outside_bounds_entries", "directional_gross_limits") and value is False:
            continue  # preserve canonical legacy config JSON; true is explicitly proof-bound
        if isinstance(value, Decimal):
            out[name] = str(value)
        elif isinstance(value, OrderTypePolicy):
            out[name] = value.value
        else:
            out[name] = value
    return out


_DECIMAL_FIELDS = {
    "lower_price", "upper_price", "order_amount_base", "leverage", "expected_initial_position",
    "max_abs_net_position", "max_gross_position", "history_freshness_s", "settlement_delay_s",
    "history_overlap_s", "poll_interval_s",
}


def grid_config_from_json(data: Dict[str, Any]) -> GridConfig:
    kwargs: Dict[str, Any] = {}
    for name, value in data.items():
        if name in _DECIMAL_FIELDS and value is not None:
            kwargs[name] = Decimal(str(value))
        elif name in ("entry_order_type", "tp_order_type"):
            kwargs[name] = OrderTypePolicy(value)
        else:
            kwargs[name] = value
    return GridConfig(**kwargs)


@dataclass
class OrderMeta:
    """Engine bookkeeping per client order id that the store's leg/order rows do not carry.

    Stored in the store's ``engine_kv`` (``om:<cid>``), JSON without floats: every time is integer ms.
    Settlement needs ``first_terminal_seen_ms`` and the exact normalized terminal row across restarts.
    """
    cid: int
    cell_id: int
    generation: int
    role: str
    intent_ms: int
    seq: int = 0
    obligation_ms: Optional[int] = None          # history commit that created the TP obligation (SLO start)
    first_terminal_seen_ms: Optional[int] = None
    terminal_row: Optional[Dict[str, Any]] = None
    cancel_reason: Optional[str] = None
    cancel_requested_ms: Optional[int] = None
    cancel_sent_ms: Optional[int] = None
    audited_cumulative: Optional[str] = None     # operator-audited cumulative after late evidence (exact decimal)
    arming_min_tp: Optional[str] = None          # entry only: minimum valid TP quantity when the cycle was armed
    transport_detail: Optional[str] = None

    def to_json(self) -> Dict[str, Any]:
        return dict(self.__dict__)

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "OrderMeta":
        known = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        return cls(**known)


@dataclass
class EngineMeta:
    """Engine-wide durable state that the store's typed engine row does not carry (``engine_kv``)."""
    started: bool = False
    operator_paused: bool = False
    pause_reason: Optional[str] = None
    stop_requested_ms: Optional[int] = None
    stop_reason: Optional[str] = None
    stop_outcome: Optional[str] = None
    freezes: Dict[str, str] = field(default_factory=dict)       # engine-level operator-cleared blockers
    obligations: Dict[str, int] = field(default_factory=dict)   # "cell:gen" -> history commit ms (TP SLO clock)
    seq: int = 0
    ever_normal: bool = False
    bootstrap_floor_ms: Optional[int] = None
    history_reset: Dict[str, int] = field(default_factory=dict)  # stream -> floor ms after audited retention gap
    acknowledged_conflicts: list = field(default_factory=list)  # scanner conflict strings an operator audited
    # cell id -> {"role", "reason", "fingerprint", "retry_at_ms", "failures"}: a rejected intent of this cell/role is
    # not re-issued until the rules/config fingerprint changes or the backoff elapses
    reject_latches: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    risk_blocked: Dict[str, str] = field(default_factory=dict)   # router TP key -> RISK_BLOCKED reason
    start_preview_id: Optional[str] = None                        # preview the applied START acknowledged
    colliding_cid: Optional[int] = None                           # CID a foreign order owns (retire_colliding_cid)
    start_config_fingerprint: Optional[str] = None                # full config the applied START acknowledged
    start_material_id: Optional[str] = None                       # web preview config+rules digest (informational)
    audited_payloads: Dict[str, Dict[str, str]] = field(default_factory=dict)   # stream -> key -> accepted fp
    last_stop_applied_ms: Optional[int] = None                    # latest APPLIED STOP (a resume must name it)
    # unaudited payload contradictions seen by any walk (stream|key -> versions), kept until ack_history_conflict
    history_conflicts: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def to_json(self) -> Dict[str, Any]:
        return dict(self.__dict__)

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "EngineMeta":
        known = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        return cls(**known)


def dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=_default)


def _default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "value"):
        return value.value
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


# ------------------------------------------------------------------------------------------------ executor config
class NeutralGridExecutorConfig(ExecutorConfigBase):
    """V2 executor config: exactly ONE executor hosts the engine for all cells (NG-ARCH-001, AC-04)."""
    type: Literal["neutral_grid_executor"] = "neutral_grid_executor"
    connector_name: str
    trading_pair: str
    grid_id: str
    lower_price: Decimal
    upper_price: Decimal
    cell_count: int
    order_amount_base: Decimal
    leverage: Decimal
    expected_initial_position: Optional[Decimal] = None
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
    directional_gross_limits: bool = False
    db_path: Optional[str] = None                       # None = the store's default per account/market
    unknown_resolution_delay_s: Decimal = Decimal("120")   # see EngineOptions.unknown_resolution_delay_s
    # Explicit operator confirmations collected by the launcher (never implied by enabled=true).
    operator_confirmed_start: bool = False
    operator_confirmed_baseline: bool = False
    operator_confirmed_migration: bool = False             # launcher-confirmed audited grid migration (AC-52)
    operator_resume_stop_ms: Optional[int] = None          # launcher-confirmed resume of exactly this durable stop

    @model_validator(mode="after")
    def _no_market_orders(self):
        if self.entry_order_type not in (OrderTypePolicy.LIMIT_MAKER, OrderTypePolicy.LIMIT) or \
                self.tp_order_type not in (OrderTypePolicy.LIMIT_MAKER, OrderTypePolicy.LIMIT):
            raise ValueError("only LIMIT_MAKER/LIMIT order types are allowed (no MARKET)")
        if self.cell_count <= 0 or self.max_active_orders <= 0:
            raise ValueError("cell_count and max_active_orders must be positive")
        return self

    def to_grid_config(self, account_index: int) -> GridConfig:
        return GridConfig(
            grid_id=self.grid_id, connector_name=self.connector_name, trading_pair=self.trading_pair,
            account_index=account_index, lower_price=self.lower_price, upper_price=self.upper_price,
            cell_count=self.cell_count, order_amount_base=self.order_amount_base, leverage=self.leverage,
            expected_initial_position=self.expected_initial_position,
            max_abs_net_position=self.max_abs_net_position, max_gross_position=self.max_gross_position,
            max_active_orders=self.max_active_orders, history_freshness_s=self.history_freshness_s,
            settlement_delay_s=self.settlement_delay_s, settlement_scans=self.settlement_scans,
            history_overlap_s=self.history_overlap_s, poll_interval_s=self.poll_interval_s,
            entry_order_type=self.entry_order_type, tp_order_type=self.tp_order_type,
            tp_gtt_seconds=self.tp_gtt_seconds, enabled=self.enabled,
            directional_outside_bounds_entries=self.directional_outside_bounds_entries,
            directional_gross_limits=self.directional_gross_limits,
        )
