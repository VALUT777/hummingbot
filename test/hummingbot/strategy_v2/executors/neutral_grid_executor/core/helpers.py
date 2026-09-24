"""Shared fixtures for neutral-grid core tests (sample profile of the spec, not hard-coded product values)."""
from decimal import Decimal
from itertools import count
from typing import Optional

from hummingbot.strategy_v2.executors.neutral_grid_executor.cells import CellLedger
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CellSpec,
    GridConfig,
    LegIdentity,
    OrderTypePolicy,
    Side,
    TradingRules,
    TransportOutcome,
    TransportResult,
)

D = Decimal
GRID_ID = "g1"
Q = D("10")


def rules(tick="0.0001", step="0.01", min_base="5", min_notional="10", max_base=None, max_leverage="20",
          venue_cap: Optional[int] = None, supports_limit=True, supports_post_only=True) -> TradingRules:
    return TradingRules(
        tick_size=D(tick), size_step=D(step), min_base=D(min_base), min_notional=D(min_notional),
        max_base=None if max_base is None else D(max_base),
        max_leverage=None if max_leverage is None else D(max_leverage),
        supports_limit=supports_limit, supports_post_only=supports_post_only, fetched_at=0.0,
        max_active_orders_venue=venue_cap)


def config(**overrides) -> GridConfig:
    base = dict(
        grid_id=GRID_ID, connector_name="lighter_perpetual_robinhood", trading_pair="LIT-USDG", account_index=7,
        lower_price=D("5"), upper_price=D("6"), cell_count=55, order_amount_base=Q, leverage=D("5"),
        expected_initial_position=D("0"), max_abs_net_position=D("1000"), max_gross_position=D("1000"),
        max_active_orders=120)
    base.update(overrides)
    return GridConfig(**base)


BUY_CELL = CellSpec(cell_id=3, low_price=D("5.0545"), high_price=D("5.0727"), entry_side=Side.BUY)
SELL_CELL = CellSpec(cell_id=40, low_price=D("5.7272"), high_price=D("5.7454"), entry_side=Side.SELL)


class Harness:
    """Drives one CellLedger the way the engine would, with monotonic CIDs and canonical fill keys."""

    def __init__(self, spec: CellSpec = BUY_CELL, q: Decimal = Q, r: Optional[TradingRules] = None):
        self.ledger = CellLedger(GRID_ID, spec, q)
        self.rules = r or rules()
        self._cid = count(1000)
        self._trade = count(1)

    def entry(self, accept=True) -> LegIdentity:
        leg = self.ledger.begin_entry(next(self._cid), self.rules, order_type=OrderTypePolicy.LIMIT_MAKER)
        if accept:
            self.ledger.record_transport(leg.identity, TransportResult(TransportOutcome.ACCEPTED,
                                                                       exchange_order_id=f"x{leg.cid}"))
        return leg.identity

    def dispatch_tps(self, accept=True):
        plan = self.ledger.tp_obligation_to_dispatch(self.rules)
        out = []
        for item in plan.items:
            leg = self.ledger.add_tp_intent(item.qty, next(self._cid), self.rules, generation=item.generation,
                                            order_type=OrderTypePolicy.LIMIT)
            if accept:
                self.ledger.record_transport(leg.identity, TransportResult(TransportOutcome.ACCEPTED,
                                                                           exchange_order_id=f"x{leg.cid}"))
            out.append(leg.identity)
        return plan, out

    def fill(self, identity: LegIdentity, qty: str, key=None):
        leg = self.ledger.find_leg(identity)[1]
        key = key if key is not None else ("lighter", 7, 5, str(next(self._trade)), leg.side.value,
                                           f"x{leg.cid}")
        return self.ledger.apply_fill(key, identity, D(qty), leg.price, leg.side), key

    def leg(self, identity: LegIdentity):
        return self.ledger.find_leg(identity)[1]
