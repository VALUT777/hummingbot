from hummingbot.strategy_v2.executors.neutral_grid_executor.cells import CellLedger
from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import CellSpec, Side
from hummingbot.strategy_v2.executors.neutral_grid_executor.risk import endpoints_from_ledgers

from .helpers import D, rules


def _settled_ledger(settled: str) -> CellLedger:
    ledger = CellLedger("grid", CellSpec(1, D("5"), D("6"), Side.BUY), D("10"))
    entry = ledger.begin_entry(1, rules(min_notional="0"))
    cycle = ledger.cycles[0]
    entry.filled = D("10")
    entry.state = entry.state.TERMINAL
    entry.terminal_cumulative = D("10")
    cycle.external_settled = D(settled)
    return ledger


def test_external_settlement_closes_obligation_without_changing_exchange_exit():
    ledger = _settled_ledger("10")
    cycle = ledger.cycles[0]

    assert cycle.X == D("0")
    assert cycle.external_settled == D("10")
    assert cycle.effective_exit == D("10")
    assert cycle.open_obligation == D("0")
    assert cycle.buckets().external_settled == D("10")
    assert ledger.can_release(position_reconciled=True).ok
    endpoints = endpoints_from_ledgers(D("0"), [ledger])
    assert endpoints.P == D("0")
    assert endpoints.gross_worst == D("0")


def test_partial_and_over_settlement_remain_visible_and_fail_closed():
    partial = _settled_ledger("9")
    assert partial.cycles[0].open_obligation == D("1")
    assert not partial.can_release(position_reconciled=True).ok
    assert endpoints_from_ledgers(D("0"), [partial]).gross_worst == D("1")

    over = _settled_ledger("11")
    assert any("external" in problem.lower() for problem in over.check_invariants())
