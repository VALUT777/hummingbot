"""Review #5: no silent truncation. Bounded scans report `truncated: true`; indexed store reads are preferred."""
from __future__ import annotations

import types
from decimal import Decimal

import pytest
from ngweb_fakes import sample_snapshot

from web.neutral_grid import gateway as gateway_module
from web.neutral_grid.gateway import StoreGateway


class _Store:
    """Minimal store double exposing only the public read API StoreGateway uses."""

    def __init__(self, n_legs=6, n_events=6, n_commands=6):
        self.legs_ = [types.SimpleNamespace(cid=1000 + i, role="ENTRY") for i in range(n_legs)]
        self.orders_ = {1000 + i: types.SimpleNamespace(cid=1000 + i, exchange_order_id=str(9007199254741000 + i),
                                                        order_index=None) for i in range(n_legs)}
        self.fills_ = [types.SimpleNamespace(trade_id_str=str(1152921504606846000 + i), own_exchange_order_id=None,
                                             cid=1000 + i, size=Decimal("1")) for i in range(n_legs)]
        self.events = [types.SimpleNamespace(id=i, at_ms=1000 * i, kind="k", actor="a", engine_revision=0,
                                             payload={}) for i in range(1, n_events + 1)]
        self.commands = [types.SimpleNamespace(
            id=i, idempotency_key=f"key-{i:020d}", kind="pause", expected_config_revision=0,
            expected_engine_revision=0, payload={}, status="APPLIED", result=None, created_at_ms=0, applied_at_ms=0,
            claim_count=1) for i in range(1, n_commands + 1)]

    def latest_snapshot(self):
        return None

    def legs(self):
        return list(self.legs_)

    def leg(self, cid):
        return next((leg for leg in self.legs_ if leg.cid == cid), None)

    def order(self, cid):
        return self.orders_.get(cid)

    def fills(self):
        return list(self.fills_)

    def unmatched_evidence(self, include_resolved=False):
        return []

    def audit_events(self, limit=200):
        return list(reversed(self.events))[:limit]

    def list_commands(self, limit=50, status=None):
        return list(reversed(self.commands))[:limit]


@pytest.fixture
def small_window(monkeypatch):
    monkeypatch.setattr(gateway_module, "_SCAN_LIMIT", 3)


def test_old_exchange_order_outside_window_is_reported_truncated(small_window):
    gw = StoreGateway(_Store())
    oldest = str(9007199254741000)
    result = gw.lookup(oldest)
    assert result["orders"] == [] and result["truncated"] is True
    newest = gw.lookup(str(9007199254741005))
    assert len(newest["orders"]) == 1 and newest["truncated"] is True  # window still incomplete overall


def test_fill_lookup_scans_everything_it_loaded(small_window):
    gw = StoreGateway(_Store())
    result = gw.lookup(str(1152921504606846000))  # the oldest fill
    assert len(result["trades"]) == 1


def test_paging_beyond_window_is_reported_truncated(small_window):
    gw = StoreGateway(_Store())
    rows, truncated = gw.audit_page(before="3", limit=2)
    assert truncated is True
    rows, truncated = gw.commands_page(before="3", limit=2)
    assert truncated is True
    rows, truncated = gw.audit_page(before=None, limit=2)
    assert [r["id"] for r in rows] == ["6", "5"] and truncated is False


def test_indexed_store_reads_are_preferred(small_window):
    store = _Store()
    store.find_orders_by_exchange_or_client_id = lambda id_str: [
        (store.leg(1000), store.order(1000))] if id_str == str(9007199254741000) else []
    store.find_fills_by_trade_id = lambda trade_id: [f for f in store.fills_ if f.trade_id_str == trade_id]
    store.audit_page = lambda before_id, limit: [e for e in reversed(store.events) if e.id < before_id][:limit]
    store.commands_page = lambda before_id, limit: [c for c in reversed(store.commands) if c.id < before_id][:limit]
    gw = StoreGateway(store)
    result = gw.lookup(str(9007199254741000))
    assert len(result["orders"]) == 1 and result["truncated"] is False
    rows, truncated = gw.audit_page(before="3", limit=2)
    assert [r["id"] for r in rows] == ["2", "1"] and truncated is False
    rows, truncated = gw.commands_page(before="3", limit=2)
    assert [r["id"] for r in rows] == ["2", "1"] and truncated is False


@pytest.mark.asyncio
async def test_api_surfaces_truncation(make_web, small_window):
    gw = StoreGateway(_Store())
    gw.latest_snapshot = lambda: sample_snapshot("NORMAL")
    web = await make_web(gateway=gw)
    await web.login()
    lookup = await (await web.get("/api/lookup?id=9007199254741000")).json()
    assert lookup["truncated"] is True
    audit = await (await web.get("/api/audit?limit=2&before=3")).json()
    assert audit["truncated"] is True
    commands = await (await web.get("/api/commands?limit=2&before=3")).json()
    assert commands["truncated"] is True
