"""Shared helpers for the neutral grid store tests. Every test uses a real SQLite file under tmp_path."""
import json
from decimal import Decimal
from pathlib import Path
from typing import List, Optional, Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CellSpec,
    ExchangeOrderRow,
    ExchangeTradeRow,
    LegIdentity,
    LegRole,
    OrderTypePolicy,
    Side,
    SubmitRequest,
    TransportOutcome,
    TransportResult,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import (
    BootstrapRecord,
    EngineIdentity,
    IntentRecord,
    NeutralGridStore,
)

DOMAIN = "lighter_perpetual_robinhood"
ACCOUNT = 7
MARKET = 5
PAIR = "LIT-USDG"
IDENTITY = EngineIdentity(connector_name=DOMAIN, connector_domain=DOMAIN, account_index=ACCOUNT, trading_pair=PAIR)
GRID_ID = "grid-a"
FINGERPRINT = "fp-5-6-55-10"
Q = Decimal("10")
BOOT_CUT_MS = 1_700_000_000_000
REPO_ROOT = Path(__file__).resolve().parents[6]


GridFixture = Tuple[List[Decimal], List[CellSpec]]


def build_grid(lower: Decimal = Decimal("5"), upper: Decimal = Decimal("6"), n: int = 55,
               tick: Decimal = Decimal("0.0001"), anchor: Decimal = Decimal("5.4")) -> GridFixture:
    """NG-GRID-001 integer-tick grid (test fixture copy; the real builder is WS-A's grid.build_grid)."""
    low_tick, high_tick = int(lower / tick), int(upper / tick)
    span = high_tick - low_tick
    prices = [(low_tick + (i * span) // n) * tick for i in range(n + 1)]
    cells = [CellSpec(i, prices[i], prices[i + 1], Side.BUY if prices[i] < anchor else Side.SELL) for i in range(n)]
    return prices, cells


class FakeClock:
    def __init__(self, start_ms: int = BOOT_CUT_MS + 1_000):
        self.now = start_ms

    def __call__(self) -> int:
        return self.now

    def advance(self, ms: int = 1_000) -> int:
        self.now += ms
        return self.now


class Env:
    """One engine's on-disk location. ``open()`` behaves like a fresh process opening the database."""

    def __init__(self, tmp_path: Path):
        self.tmp = tmp_path
        self.db = tmp_path / "data" / "neutral_grid" / "ng.sqlite3"
        self.locks = tmp_path / "locks"
        self.clock = FakeClock()

    def open(self, create: bool = True, **kwargs) -> NeutralGridStore:
        return NeutralGridStore.open(self.db, IDENTITY, create_if_missing=create, lock_dir=self.locks,
                                     clock_ms=self.clock, **kwargs)

    def bootstrap(self, store: NeutralGridStore, baseline: Decimal = Decimal("0"), n: int = 55,
                  grid_id: str = GRID_ID, fingerprint: str = FINGERPRINT):
        prices, cells = build_grid(n=n)
        record = BootstrapRecord(
            grid_id=grid_id, config_fingerprint=fingerprint,
            config={"lower_price": Decimal("5"), "upper_price": Decimal("6"), "cell_count": n,
                    "order_amount_base": Q, "max_abs_net_position": Decimal("1000")},
            lower_price=Decimal("5"), upper_price=Decimal("6"), order_amount_base=Q, prices=prices, cells=cells,
            anchor=Decimal("5.4"), baseline=baseline, market_id=MARKET, bootstrap_cut_ts_ms=BOOT_CUT_MS,
            actor="operator", confirmation=f"I confirm position {baseline} LIT", trades_cut="trade-cut-1",
            orders_cut="order-cut-1")
        return store.bootstrap(None, record)


class FakeTransport:
    """Stands in for the exchange port: counts calls, returns scripted results."""

    def __init__(self):
        self.submits: List[SubmitRequest] = []
        self.cancels: List[int] = []
        self.results: List[TransportResult] = []

    def submit(self, request: SubmitRequest) -> TransportResult:
        self.submits.append(request)
        if self.results:
            return self.results.pop(0)
        return TransportResult(TransportOutcome.ACCEPTED, "tx-hash", exchange_order_id=f"9{request.client_order_id}")

    def cancel(self, cid: int) -> TransportResult:
        self.cancels.append(cid)
        return TransportResult(TransportOutcome.ACCEPTED, "cancel-ack")


def entry_leg(cell_id: int, generation: int = 1, revision: int = 0) -> LegIdentity:
    return LegIdentity(GRID_ID, cell_id, generation, LegRole.ENTRY, revision)


def tp_leg(cell_id: int, generation: int = 1, revision: int = 0) -> LegIdentity:
    return LegIdentity(GRID_ID, cell_id, generation, LegRole.TP, revision)


def record_entry_intent(store: NeutralGridStore, cell_id: int, amount: Decimal = Q) -> IntentRecord:
    """Open the next cycle of ``cell_id`` and commit its entry intent (one transaction)."""
    with store.transaction() as tx:
        cycle = store.open_cycle(tx, GRID_ID, cell_id)
        cell = store.cell(GRID_ID, cell_id)
        return store.prepare_submit(tx, entry_leg(cell_id, cycle.generation), side=cell.entry_side,
                                    price=cell.spec().entry_price, amount=amount,
                                    order_type=OrderTypePolicy.LIMIT_MAKER)


def submit_via_protocol(store: NeutralGridStore, transport: FakeTransport, intent: IntentRecord) -> TransportResult:
    """Reference outbox protocol: intent committed -> dispatch mark committed -> transport -> result committed."""
    with store.transaction() as tx:
        store.mark_dispatching(tx, intent.outbox_id)
    store.fault_point("before_transport")
    result = transport.submit(intent.request)
    store.fault_point("after_transport")
    with store.transaction() as tx:
        store.record_transport_result(tx, intent.cid, result)
    return result


def trade_row(trade_id: str, cid: Optional[int], side: Side, size: str, *, price: str = "5.2",
              ts: int = BOOT_CUT_MS + 10_000, exchange_order_id: Optional[str] = None, account: int = ACCOUNT,
              market: int = MARKET, is_maker: Optional[bool] = True) -> ExchangeTradeRow:
    raw = {"trade_id": trade_id, "size": size, "price": price, "client_id": cid, "order_id": exchange_order_id}
    return ExchangeTradeRow(trade_id_str=trade_id, account_index=account, market_id=market, own_side=side,
                            own_exchange_order_id=exchange_order_id, own_client_order_id=cid, size=Decimal(size),
                            price=Decimal(price), is_maker=is_maker, timestamp_ms=ts,
                            raw_json=json.dumps(raw, sort_keys=True))


def order_row(cid: Optional[int], side: Side, price: Decimal, amount: Decimal, filled: Decimal, *,
              status: str = "filled", order_id: Optional[str] = None, ts: int = BOOT_CUT_MS + 20_000,
              account: int = ACCOUNT, market: int = MARKET) -> ExchangeOrderRow:
    raw = {"client_order_id": cid, "order_id": order_id, "filled": str(filled), "status": status}
    return ExchangeOrderRow(client_order_id=cid, client_order_id_str=None if cid is None else str(cid),
                            order_id=order_id, order_index=order_id, nonce="11", account_index=account,
                            market_id=market, side=side, price=price, initial_base_amount=amount,
                            filled_base_amount=filled, remaining_base_amount=amount - filled, status=status,
                            reduce_only=False, timestamp_ms=ts, raw_json=json.dumps(raw, sort_keys=True))
