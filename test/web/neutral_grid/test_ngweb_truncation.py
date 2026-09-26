"""Review #5: the gateway uses only the store's indexed drill-down reads (exact str ids, keyset pages).

The previous bounded 5000-row scans are gone, so no response is truncated; this pins the call contract
(ids passed as the exact str, before_id as int, limit bounded to the store's 1..1000)."""
from __future__ import annotations

import types

from web.neutral_grid.gateway import StoreGateway


class _RecordingStore:
    def __init__(self):
        self.calls = []

    def find_orders_by_id(self, id_str):
        self.calls.append(("find_orders_by_id", id_str))
        assert isinstance(id_str, str)
        return []

    def find_fills_by_trade_id(self, trade_id_str):
        self.calls.append(("find_fills_by_trade_id", trade_id_str))
        assert isinstance(trade_id_str, str)
        return []

    def unmatched_evidence(self, include_resolved=False):
        return []

    def commands_page(self, before_id=None, limit=100):
        self.calls.append(("commands_page", before_id, limit))
        return []

    def audit_page(self, before_id=None, limit=100):
        self.calls.append(("audit_page", before_id, limit))
        return [types.SimpleNamespace(id=2, at_ms=1000, kind="k", actor="a", engine_revision=0, payload={})]


def test_gateway_calls_indexed_reads_with_exact_types():
    store = _RecordingStore()
    gw = StoreGateway(store)
    big = str((1 << 63) + 5)
    assert gw.lookup(big) == {"orders": [], "trades": [], "truncated": False}
    assert ("find_orders_by_id", big) in store.calls and ("find_fills_by_trade_id", big) in store.calls
    assert gw.commands_page(before="9007199254740993", limit=5000) == ([], False)
    assert store.calls[-1] == ("commands_page", 9007199254740993, 1000)
    rows, truncated = gw.audit_page(before=None, limit=3)
    assert truncated is False and rows[0]["id"] == "2" and store.calls[-1] == ("audit_page", None, 3)
    assert gw.audit_page(before="not-a-number", limit=3) == ([], False)
    assert gw.commands_page(before="0", limit=3) == ([], False)
