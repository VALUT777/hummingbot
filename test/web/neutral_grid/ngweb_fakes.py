"""Test doubles for the web layer: an in-memory store gateway and sample config/rules/snapshots.

``FakeGateway`` mimics the store boundary (committed snapshots + durable command queue) so API tests
do not depend on engine internals. Preview arithmetic always uses the real core package.
"""
from __future__ import annotations

import copy
import itertools
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import GridConfig, TradingRules

BIG_BASE = (1 << 53) + 1  # every fake id is beyond the JS safe-integer range (AC-22)


def sample_rules(**overrides) -> TradingRules:
    values = dict(tick_size=Decimal("0.0001"), size_step=Decimal("0.1"), min_base=Decimal("5"),
                  min_notional=Decimal("10"), max_base=Decimal("100000"), max_leverage=Decimal("10"),
                  supports_limit=True, supports_post_only=True, fetched_at=time.time(),
                  max_active_orders_venue=None)
    values.update(overrides)
    return TradingRules(**values)


def sample_config(**overrides) -> GridConfig:
    values = dict(grid_id="ng-test", connector_name="lighter_perpetual_robinhood", trading_pair="LIT-USDG",
                  account_index=7, lower_price=Decimal("5"), upper_price=Decimal("6"), cell_count=55,
                  order_amount_base=Decimal("10"), leverage=Decimal("5"), expected_initial_position=Decimal("0"),
                  max_abs_net_position=Decimal("1000"), max_gross_position=Decimal("1000"),
                  max_active_orders=120, enabled=True)
    values.update(overrides)
    return GridConfig(**values)


def sample_snapshot(state: str = "NORMAL", *, committed_at: Optional[float] = None, config_revision: int = 1,
                    engine_revision: int = 1, big_ids: bool = True) -> Dict[str, Any]:
    trade_id = str(BIG_BASE + 1000)
    exch = str((1 << 63) + 5) if big_ids else "12345"
    return {
        "snapshot_version": 1,
        "config_revision": config_revision,
        "engine_revision": engine_revision,
        "committed_at": time.time() if committed_at is None else committed_at,
        "engine_state": state,
        "reasons": ["history lag 3.2s"] if state != "NORMAL" else [],
        "summary": {
            "baseline": "0", "authoritative_net": "-2", "P": "-2", "P_min": "-330", "P_max": "220",
            "gross_worst": "550", "max_abs_net_position": "1000", "max_gross_position": "1000",
            "slots": {"actual": 3, "reserved": 120, "free": 0, "cap": 120}, "armed": 40, "queued": 15,
            "owned_active": 3, "unknown_orders": 0,
            "history": {"complete": True, "lag_s": 3.2, "trades_cursor": trade_id, "orders_cursor": "c-9",
                        "last_full_scan_at": time.time(), "incomplete_reason": None},
            "margin": {"available": "3000", "required_estimate": "594.00", "warning": None},
            "runtime_rules": {"tick_size": "0.0001", "size_step": "0.1", "min_base": "5", "min_notional": "10"},
            "dust_total": "0.3",
        },
        "cells": [
            {"cell_id": "21", "low": "5.3818", "high": "5.4000", "entry_side": "BUY", "generation": 2,
             "state": "TP_LIVE",
             "entry": {"cid": "281474976710600", "exchange_id": exch, "requested": "10", "filled": "7",
                       "remaining": "3", "state": "LIVE"},
             "tp_children": [{"cid": "281474976710601", "exchange_id": str((1 << 63) + 6), "requested": "5",
                              "filled": "2", "remaining": "3", "state": "LIVE", "expiry": None}],
             "obligation": {"E": "7", "X": "2", "live_tp": "3", "reserved_unassigned": "2", "dust": "0"},
             "blocker": None, "queue_age_s": None},
            {"cell_id": "22", "low": "5.4000", "high": "5.4181", "entry_side": "SELL", "generation": 1,
             "state": "DUST",
             "entry": {"cid": "281474976710602", "exchange_id": "77", "requested": "10", "filled": "10",
                       "remaining": "0", "state": "TERMINAL"},
             "tp_children": [], "obligation": {"E": "10", "X": "9.7", "live_tp": "0", "reserved_unassigned": "0",
                                               "dust": "0.3"},
             "blocker": "остаток 0.3 ниже минимума 5", "queue_age_s": 12.5},
        ] + [
            {"cell_id": str(i), "low": "5", "high": "5.0181", "entry_side": "BUY", "generation": 0, "state": "IDLE",
             "entry": None, "tp_children": [], "obligation": {}, "blocker": None, "queue_age_s": None}
            for i in range(0, 21)
        ],
        "unmatched_evidence": [{"trade_id_str": trade_id, "reason": "unknown owned fill"}],
        "errors": [{"at": time.time(), "code": "HISTORY_LAG", "message": "история отстаёт"}],
        "commands": [],
    }


class FakeGateway:
    def __init__(self, snapshot: Optional[Dict[str, Any]] = None):
        self.snapshot = snapshot
        self.commands: List[Dict[str, Any]] = []
        self._ids = itertools.count(BIG_BASE)
        self.orders: List[Dict[str, Any]] = []
        self.trades: List[Dict[str, Any]] = []
        self.events: List[Dict[str, Any]] = []
        self.enqueue_calls = 0

    # read side
    def latest_snapshot(self):
        return copy.deepcopy(self.snapshot)

    def get_command(self, command_id):
        return next((copy.deepcopy(c) for c in self.commands if c["id"] == str(command_id)), None)

    def get_command_by_key(self, key):
        return next((copy.deepcopy(c) for c in self.commands if c["idempotency_key"] == key), None)

    def list_commands(self, *, limit, before=None, kind=None, status=None):
        rows = sorted(self.commands, key=lambda c: (len(c["id"]), c["id"]), reverse=True)
        if before is not None:
            rows = [c for c in rows if (len(c["id"]), c["id"]) < (len(before), before)]
        if kind:
            rows = [c for c in rows if c["kind"] == kind]
        if status:
            rows = [c for c in rows if c["status"] == status]
        return copy.deepcopy(rows[:limit])

    def find_orders(self, id_str):
        return [o for o in self.orders if id_str in (str(o.get("client_order_id")), str(o.get("exchange_order_id")))]

    def find_trades(self, id_str):
        return [t for t in self.trades if id_str in (str(t.get("trade_id_str")), str(t.get("own_exchange_order_id")))]

    def audit_events(self, *, limit, before=None):
        rows = sorted(self.events, key=lambda e: (len(e["id"]), e["id"]), reverse=True)
        if before is not None:
            rows = [e for e in rows if (len(e["id"]), e["id"]) < (len(before), before)]
        return rows[:limit]

    # write side
    def enqueue_command(self, idempotency_key, kind, expected_config_revision, expected_engine_revision, payload):
        self.enqueue_calls += 1
        existing = self.get_command_by_key(idempotency_key)
        if existing is not None:
            return existing
        row = {"id": str(next(self._ids)), "idempotency_key": idempotency_key, "kind": kind,
               "expected_config_revision": expected_config_revision,
               "expected_engine_revision": expected_engine_revision, "payload": copy.deepcopy(payload),
               "status": "QUEUED", "result": None, "created_at": time.time(), "applied_at": None}
        self.commands.append(row)
        return copy.deepcopy(row)

    # test helpers (what the engine would do)
    def apply(self, command_id, status="APPLIED", result=None):
        for c in self.commands:
            if c["id"] == str(command_id):
                c["status"] = status
                c["result"] = result
                c["applied_at"] = time.time()
