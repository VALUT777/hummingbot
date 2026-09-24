"""Crash consistency, restart and persistence failures on the real SQLite store:
AC-15/16/17/18/19/20/21/54/55/56 plus a crash at every persistence window (store fault hooks)."""
from decimal import Decimal

import pytest
from ng_engine_harness import Harness, start_payload

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import (
    CommandKind,
    EngineState,
    LegRole,
    OrderState,
    Side,
)
from hummingbot.strategy_v2.executors.neutral_grid_executor.fake_exchange import CancelBehavior, SubmitBehavior
from hummingbot.strategy_v2.executors.neutral_grid_executor.store import FAULT_POINTS, SimulatedCrash

D = Decimal
FINAL = (OrderState.TERMINAL, OrderState.REJECTED_UNSENT, OrderState.REJECTED_ZERO_FILL)


def _started(h, baseline=D("0")):
    h.bootstrap(baseline)
    h.tick(2)


def _identities(h):
    """CID -> leg identity for everything the venue ever received."""
    return {c.client_order_id: h.engine.store.identity_for_cid(c.client_order_id) for c in h.fx.submits()}


def _assert_no_phantoms_and_no_new_cid(h):
    ids = _identities(h)
    assert None not in ids.values(), "venue saw a CID the durable map does not know (phantom order)"
    by_identity = {}
    for cid, identity in ids.items():
        by_identity.setdefault(identity, set()).add(cid)
    assert all(len(cids) == 1 for cids in by_identity.values()), "a leg got a second CID"
    for order in h.fx.open_orders(owned=True):
        assert order.client_order_id in h.engine.order_meta


def _assert_fills_exactly_once(h):
    venue = {(leg.trade_id, leg.own_side.value) for leg in h.fx.trade_legs if leg.client_order_id in h.engine.order_meta}
    stored = [(f.trade_id_str, f.own_side.value) for f in h.engine.store.fills()]
    assert len(stored) == len(set(stored))
    assert set(stored) <= venue


def test_ac15_crash_before_intent_commit_leaves_no_phantom(tmp_path):
    h = Harness(tmp_path)
    try:
        h.bootstrap(confirm_tick=False)
        h.hooks.arm("before_intent_commit")
        assert h.tick_crashing(1)
        assert h.fx.submits() == []                                  # API never called
        assert all(not h.cell(c).legs() for c in h.engine.cells)     # no durable intent survived
        h.tick(3)
        _assert_no_phantoms_and_no_new_cid(h)
        assert len(h.fx.submits()) == len(h.engine.cells)
    finally:
        h.close()


def test_ac16_crash_after_intent_before_api_reuses_the_saved_cid(tmp_path):
    h = Harness(tmp_path)
    try:
        h.bootstrap(confirm_tick=False)
        h.hooks.arm("after_intent_commit")
        assert h.tick_crashing(1)
        pending = [leg for c in h.engine.cells for leg in h.legs(c) if leg.state == OrderState.INTENT]
        assert len(pending) == 1 and h.fx.submits() == []
        saved_cid = pending[0].cid
        outbox = h.engine.store.outbox_for_cid(saved_cid)
        assert outbox and outbox[0].status == "PENDING"            # durable recovery question, provably unsent
        h.tick(2)
        assert [c.client_order_id for c in h.fx.submits()].count(saved_cid) == 1
        assert h.engine.leg_by_cid(saved_cid).state == OrderState.LIVE
        _assert_no_phantoms_and_no_new_cid(h)
    finally:
        h.close()


def test_ac16_crash_after_dispatch_commit_is_unknown_not_proven_absent(tmp_path):
    h = Harness(tmp_path)
    try:
        h.bootstrap(confirm_tick=False)
        h.hooks.arm("before_transport")
        assert h.tick_crashing(1)
        unknown = [leg for c in h.engine.cells for leg in h.legs(c) if leg.state == OrderState.SUBMIT_UNKNOWN]
        assert len(unknown) == 1 and h.fx.submits() == []           # transport may or may not have happened
        cid = unknown[0].cid
        h.tick(30)
        leg = h.engine.leg_by_cid(cid)
        assert leg.state == OrderState.SUBMIT_UNKNOWN               # never "proven absent" on its own
        assert h.engine.endpoints.P_max >= leg.remaining            # full reservation kept
        assert cid not in [c.client_order_id for c in h.fx.submits()]
        cell = leg.identity.cell_id
        assert h.live_order(cell, LegRole.ENTRY).cid == cid         # no new CID / revision for that cell
        _assert_no_phantoms_and_no_new_cid(h)
        # Audited manual resolution is the only way out (evidence absent, history complete).
        h.command(CommandKind.BASELINE_AUDIT, {"action": "resolve_unknown_submit", "cid": str(cid),
                                               "note": "venue export shows no order"})
        h.tick(3)
        assert h.engine.leg_by_cid(cid).state == OrderState.REJECTED_ZERO_FILL
    finally:
        h.close()


@pytest.mark.parametrize("landed", [True, False])
def test_ac17_timeout_keeps_unknown_until_history_resolves(tmp_path, landed):
    h = Harness(tmp_path)
    try:
        h.bootstrap(confirm_tick=False)
        h.fx.script_submit(SubmitBehavior.TIMEOUT_LANDED if landed else SubmitBehavior.TIMEOUT_NOT_LANDED)
        h.hooks.arm("after_result_commit")                       # crash right after recording UNKNOWN
        assert h.tick_crashing(1)
        first_cid = h.fx.submits()[0].client_order_id
        leg = h.engine.leg_by_cid(first_cid)
        assert leg.state == OrderState.SUBMIT_UNKNOWN
        h.tick(10)
        leg = h.engine.leg_by_cid(first_cid)
        if landed:
            assert leg.state == OrderState.LIVE                     # active list resolves by the saved CID
            h.fx.fill(first_cid, D("10"))
            h.tick(3)
            assert h.engine.leg_by_cid(first_cid).filled == D("10")
        else:
            assert leg.state == OrderState.SUBMIT_UNKNOWN           # reservation + UNKNOWN kept
            assert h.engine.endpoints is not None
            side_rem = sum(x.remaining for x in h.engine.non_final_legs() if x.side == leg.side)
            assert side_rem >= leg.remaining
        assert [c.client_order_id for c in h.fx.submits()].count(first_cid) == 1
        _assert_no_phantoms_and_no_new_cid(h)
    finally:
        h.close()


def test_ac18_crash_after_api_before_result_commit_exactly_once_ledger(tmp_path):
    h = Harness(tmp_path)
    try:
        h.bootstrap(confirm_tick=False)
        h.hooks.arm("after_transport_before_result_commit")
        assert h.tick_crashing(1)
        cid = h.fx.submits()[0].client_order_id
        assert h.engine.leg_by_cid(cid).state == OrderState.SUBMIT_UNKNOWN
        h.fx.fill(cid, D("10"))
        h.tick(4)
        h.crash_restart()
        h.tick(4)
        leg = h.engine.leg_by_cid(cid)
        assert leg.filled == D("10") and len(h.engine.store.fills(cid)) == 1   # evidence matched by exact CID once
        _assert_fills_exactly_once(h)
        _assert_no_phantoms_and_no_new_cid(h)
    finally:
        h.close()


@pytest.mark.parametrize("point", ["mid_history_batch", "before_cursor_commit", "after_history_commit"])
def test_ac19_crash_around_cursor_commit_is_idempotent(tmp_path, point):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, LegRole.ENTRY)
        h.fx.fill(entry.cid, D("4"))
        h.fx.fill(entry.cid, D("6"))
        h.hooks.arm(point)
        crashed = False
        for _ in range(8):
            crashed |= h.tick_crashing(1)
        assert crashed
        h.tick(8)
        cycle = h.cell(cell).cycles[-1]
        assert cycle.E == D("10")                                    # not lost, not doubled
        assert len(h.engine.store.fills(entry.cid)) == 2
        assert h.engine.store.verify_ledger() == []
        assert sum(t.requested for t in h.legs(cell, LegRole.TP)) == D("10")
    finally:
        h.close()


def test_ac20_restart_after_price_bounce_restores_from_db_and_history(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        anchor = h.engine.grid_record.anchor
        cell = h.buy_cells()[-1]
        entry = h.live_order(cell, LegRole.ENTRY)
        prices_before = {c: (h.cell(c).spec.low_price, h.cell(c).spec.high_price, h.cell(c).spec.entry_side)
                         for c in h.engine.cells}
        h.engine.store.close()                                        # process down
        h.fx.set_book(D("5.0999"), D("5.1001"))                       # price dives ...
        h.fx.fill(entry.cid, D("10"))                                 # ... fills while we are down ...
        h.fx.set_book(D("5.7999"), D("5.8001"))                       # ... and bounces far above
        h.clock.advance(30)
        h.engine = h.open()
        h.tick(6)
        assert h.engine.grid_record.anchor == anchor                  # no re-anchoring
        assert {c: (h.cell(c).spec.low_price, h.cell(c).spec.high_price, h.cell(c).spec.entry_side)
                for c in h.engine.cells} == prices_before
        tp = h.live_order(cell, LegRole.TP)
        assert tp is not None and tp.price == D("5.4") and tp.requested == D("10")   # restored obligation
        assert all(c.request.price in {h.cell(x).spec.entry_price for x in h.engine.cells}
                   | {h.cell(x).spec.tp_price for x in h.engine.cells} for c in h.fx.submits())
    finally:
        h.close()


def test_ac21_cancel_timeout_keeps_full_remainder_reserved_and_cell_locked(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[0]
        entry = h.live_order(cell, LegRole.ENTRY)
        for _ in range(len(h.engine.cells)):
            h.fx.script_cancel(CancelBehavior.TIMEOUT_NOT_LANDED, client_order_id=entry.cid)
        h.fx.set_book(D("6.1"), D("6.2"))                             # outside bounds: entries get cancelled
        h.tick(3)
        leg = h.engine.leg_by_cid(entry.cid)
        assert leg.state == OrderState.CANCEL_UNKNOWN
        assert h.engine.endpoints.P_max >= leg.remaining               # full remainder still reserved
        assert h.fx.order_by_cid(entry.cid).is_open
        h.fx.set_book(D("5.3999"), D("5.4001"))                       # back inside: no duplicate exposure
        h.tick(16)
        assert [x for x in h.legs(cell, LegRole.ENTRY)] == [h.engine.leg_by_cid(entry.cid)]
        assert len([c for c in h.fx.submits() if h.engine.leg_by_cid(c.client_order_id).identity.cell_id == cell
                    and h.engine.leg_by_cid(c.client_order_id).identity.role == LegRole.ENTRY]) == 1
        retries = [c for c in h.fx.cancels() if c.client_order_id == entry.cid]
        assert len(retries) >= 2                                       # bounded cancel retries, same order
    finally:
        h.close()


def test_ac54_missing_or_corrupt_db_with_prior_run_fails_closed(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        submits = len(h.fx.submits())
        h.engine.store.close()
        h.db_path.unlink()                                           # DB gone, markers remain
        for suffix in ("-wal", "-shm"):
            p = h.db_path.with_name(h.db_path.name + suffix)
            if p.exists():
                p.unlink()
        engine = h.open_failclosed()
        assert engine.fatal_reason and "PriorRunEvidence" in engine.fatal_reason
        h.loop.run_until_complete(engine.tick())
        assert engine.engine_state == EngineState.DEGRADED and not engine.bootstrapped
        assert len(h.fx.submits()) == submits                        # no fresh bootstrap, no orders
        h.db_path.write_bytes(b"this is not a sqlite database" * 10)   # corrupt file
        engine = h.open_failclosed()
        assert engine.fatal_reason and ("Corrupt" in engine.fatal_reason or "PriorRun" in engine.fatal_reason)
        assert len(h.fx.submits()) == submits
    finally:
        h.loop.close()


def test_ac55_disk_full_sends_nothing_and_reports_persistence_failure(tmp_path):
    h = Harness(tmp_path)
    try:
        _started(h)
        cell = h.buy_cells()[-1]
        live_before = {leg.cid for leg in h.engine.non_final_legs()}
        submits = len(h.fx.submits())
        cancels = len(h.fx.cancels())
        h.hooks.simulate_disk_full()
        h.fx.fill(h.live_order(cell, LegRole.ENTRY).cid, D("10"))    # a TP would now be owed
        h.fx.set_book(D("6.1"), D("6.2"))                            # and entries would be cancelled
        h.tick(6)
        assert h.state == EngineState.DEGRADED
        assert "PERSISTENCE_FAILURE" in h.engine.reasons
        assert len(h.fx.submits()) == submits and len(h.fx.cancels()) == cancels   # nothing without intent
        assert live_before <= {leg.cid for leg in h.engine.non_final_legs()}       # still reserved
        h.hooks.restore_storage()
        h.fx.set_book(D("5.3999"), D("5.4001"))
        h.tick(12)
        assert h.engine.persistence_error is None
        assert h.live_order(cell, LegRole.TP) is not None             # recovered and TP dispatched
        assert any(e.kind == "degraded_cleared" for e in h.engine.store.audit_events())
    finally:
        h.close()


def test_ac56_presend_rejection_releases_only_without_transport(tmp_path):
    h = Harness(tmp_path)
    try:
        h.bootstrap(confirm_tick=False)
        engine_ref = {}

        def tighten_rules(point):
            engine = engine_ref["e"]
            engine.rules = type(engine.rules)(**{**engine.rules.__dict__, "min_base": D("50")})
        engine_ref["e"] = h.engine
        h.hooks.arm("after_intent_commit", action=tighten_rules)
        h.tick()
        rejected = [leg for c in h.engine.cells for leg in h.legs(c) if leg.state == OrderState.REJECTED_UNSENT]
        assert len(rejected) == 1
        assert rejected[0].cid not in [c.client_order_id for c in h.fx.submits()]   # proven: transport not called
    finally:
        h.close()


def test_ac56_definitive_reject_allows_new_revision_timeout_does_not(tmp_path):
    h = Harness(tmp_path)
    try:
        h.bootstrap(confirm_tick=False)
        h.fx.script_submit(SubmitBehavior.REJECT_ZERO_FILL)
        h.fx.script_submit(SubmitBehavior.TIMEOUT_NOT_LANDED)
        h.tick(3)
        legs = [leg for c in h.engine.cells for leg in h.legs(c)]
        rejected = [x for x in legs if x.state == OrderState.REJECTED_ZERO_FILL]
        unknown = [x for x in legs if x.state == OrderState.SUBMIT_UNKNOWN]
        assert len(rejected) == 1 and len(unknown) == 1
        r_cell = rejected[0].identity.cell_id
        retry = [x for x in h.legs(r_cell, LegRole.ENTRY) if x.identity.revision == 1]
        assert retry and retry[0].identity.generation == rejected[0].identity.generation   # new revision
        u_cell = unknown[0].identity.cell_id
        assert [x.cid for x in h.legs(u_cell)] == [unknown[0].cid]                          # no new CID/revision
    finally:
        h.close()


# ------------------------------------------------------------------------------------ crash at every window
def _crash_tolerant_scenario(h: Harness) -> None:
    """START -> bootstrap -> entries -> fill -> TP -> TP fill -> reuse -> out-of-bounds cancels -> back."""
    def cmd(kind, payload, key):
        for _ in range(3):
            try:
                return h.command(kind, payload, key=key)
            except SimulatedCrash:
                h.crash_restart()
        raise AssertionError("command could not be enqueued")

    cmd(CommandKind.START, start_payload(), "k-start")
    for _ in range(60):
        h.tick_crashing()
        if h.engine.bootstrap_ready(h.clock())[0]:
            break
    for attempt in range(5):
        if h.engine.bootstrapped:
            break
        cmd(CommandKind.CONFIRM_BASELINE, {"expected_initial_position": "0"}, f"k-boot-{attempt}")
        for _ in range(8):
            h.tick_crashing()
            if h.engine.bootstrapped:
                break
    assert h.engine.bootstrapped
    for _ in range(10):
        h.tick_crashing()
    cell = h.buy_cells()[-1]
    entry = h.live_order(cell, LegRole.ENTRY)
    assert entry is not None
    if entry.state == OrderState.SUBMIT_UNKNOWN and h.fx.order_by_cid(entry.cid) is None:
        return                                                     # legit terminal ambiguity; checked by callers
    for _ in range(10):
        if h.fx.order_by_cid(entry.cid) is not None:
            break
        h.tick_crashing()
    h.fx.fill(entry.cid, D("10"))
    for _ in range(20):
        h.tick_crashing()
        tp = h.live_order(cell, LegRole.TP)
        if tp is not None and h.fx.order_by_cid(tp.cid) is not None:
            break
    tp = h.live_order(cell, LegRole.TP)
    if tp is not None and h.fx.order_by_cid(tp.cid) is not None and h.fx.order_by_cid(tp.cid).is_open:
        h.fx.fill(tp.cid, D("10"))
    for _ in range(25):
        h.tick_crashing()
    h.fx.set_book(D("6.1"), D("6.2"))
    for _ in range(25):
        h.tick_crashing()
    h.fx.set_book(D("5.3999"), D("5.4001"))
    for _ in range(25):
        h.tick_crashing()
    cmd(CommandKind.PAUSE, {"reason": "operator"}, "k-pause")
    for _ in range(3):
        h.tick_crashing()
    cmd(CommandKind.RESUME, {}, "k-resume")
    for _ in range(3):
        h.tick_crashing()
    for other in h.buy_cells()[:2]:                     # multi-row history batches (two partial fills each)
        leg = h.live_order(other, LegRole.ENTRY)
        venue = h.fx.order_by_cid(leg.cid) if leg is not None else None
        if venue is not None and venue.is_open and venue.remaining >= 10:
            h.fx.fill(leg.cid, D("5"))
            h.fx.fill(leg.cid, D("5"))
        for _ in range(12):
            h.tick_crashing()


@pytest.mark.parametrize("skip", [0, 1, 3])
@pytest.mark.parametrize("point", sorted(FAULT_POINTS))
def test_crash_at_every_persistence_window_restarts_consistently(tmp_path, point, skip):
    h = Harness(tmp_path)
    try:
        h.hooks.arm(point, skip=skip)
        _crash_tolerant_scenario(h)
        h.tick(15)
        assert h.crashes >= 1, f"fault point {point} (skip {skip}) was never reached by the scenario"
        _assert_no_phantoms_and_no_new_cid(h)
        _assert_fills_exactly_once(h)
        assert h.engine.store.verify_ledger() == []
        assert not h.fx.violations or all("too many" not in v for v in h.fx.violations)
        for call in h.fx.submits():
            assert call.request.reduce_only is False
        ep = h.engine.endpoints
        if ep is not None and h.engine.history_complete:
            assert ep.P_min <= h.fx.net_position <= ep.P_max              # venue inside the reachable interval
            assert ep.P_max <= h.config.max_abs_net_position and ep.P_min >= -h.config.max_abs_net_position
        venue_side = {Side.BUY: D("0"), Side.SELL: D("0")}
        for leg in h.fx.trade_legs:
            if leg.client_order_id in h.engine.order_meta:
                venue_side[leg.own_side] += leg.size
        buys, sells = h.engine._confirmed_fills()
        assert buys <= venue_side[Side.BUY] and sells <= venue_side[Side.SELL]   # never more than executed
    finally:
        h.close()


def test_crash_after_cancel_dispatch_mark_resends_the_same_cancel(tmp_path):
    """A cancel row left DISPATCHED by a dead process is re-sent for the SAME order (explicit resend_same_cid;
    cancelling a known order is idempotent), never refused into a ledger freeze and never a new CID."""
    h = Harness(tmp_path)
    try:
        _started(h)
        h.command(CommandKind.STOP, key="stop-crash")
        h.hooks.arm("before_transport")               # first STOP cancel: dispatch mark committed, venue never hit
        assert h.tick_crashing(1)
        store = h.engine.store
        rows = [o for leg in h.engine.all_legs() for o in store.outbox_for_cid(leg.cid)
                if o.kind == "CANCEL" and o.status == "DISPATCHED"]
        assert len(rows) == 1 and h.fx.order_by_cid(rows[0].cid).is_open
        h.run_until(lambda: h.engine.is_stopped, max_ticks=80)
        assert h.fx.open_orders(owned=True) == []
        assert len(store.outbox_attempts(rows[0].id)) == 2          # same row (same order) dispatched again
        assert "LEDGER_INVARIANT" not in h.engine.meta.freezes
        _assert_no_phantoms_and_no_new_cid(h)
    finally:
        h.close()
