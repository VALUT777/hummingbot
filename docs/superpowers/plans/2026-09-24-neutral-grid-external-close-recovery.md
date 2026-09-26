# Neutral Grid External-Close Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Safely account for the proven manual 10-LIT close, release the retained cell obligation without fake fills or P&L, keep the engine stopped, then migrate offline to the confirmed 24×20 LIT grid for an operator-controlled final START.

**Architecture:** Add append-only external-settlement evidence to the existing SQLite ledger. Core accounting keeps owned TP fills `X` separate from externally settled quantity `S`, while obligation and net reconciliation use both. The existing durable command queue applies a proof-bound `settle_external_close` audit atomically and never clears STOP or calls the exchange.

**Tech Stack:** Python 3.12, SQLite STRICT tables and triggers, `Decimal`, pytest, existing neutral-grid engine/web command pipeline, vanilla JavaScript UI.

---

## Frozen interfaces and parallel ownership

Store/core writer owns only:

- `hummingbot/strategy_v2/executors/neutral_grid_executor/migrations/m0006_external_settlements.py`
- `hummingbot/strategy_v2/executors/neutral_grid_executor/migrations/__init__.py`
- `hummingbot/strategy_v2/executors/neutral_grid_executor/store.py`
- `hummingbot/strategy_v2/executors/neutral_grid_executor/cells.py`
- `hummingbot/strategy_v2/executors/neutral_grid_executor/risk.py`
- store/core tests named below

Engine/web writer owns only:

- `hummingbot/strategy_v2/executors/neutral_grid_executor/commands.py`
- `hummingbot/strategy_v2/executors/neutral_grid_executor/engine.py`
- `hummingbot/strategy_v2/executors/neutral_grid_executor/snapshot.py`
- `web/neutral_grid/commands.py`
- `web/neutral_grid/static/app.js`
- engine/web tests named below

The store/core writer must preserve these exact public names for the engine/web writer:

```python
@dataclass(frozen=True)
class ExternalSettlementCycle:
    grid_id: str
    cell_id: int
    generation: int
    quantity: Decimal

@dataclass(frozen=True)
class ExternalSettlementEvidence:
    inbox_id: int
    evidence_role: str
    allocated_quantity: Optional[Decimal] = None

@dataclass(frozen=True)
class ExternalSettlementRequest:
    proof_id: str
    grid_id: str
    settlement_side: Side
    observed_position: Decimal
    actor: str
    reason: str
    expected_config_revision: int
    expected_engine_revision: int
    position_observed_at_ms: int
    active_observed_at_ms: int
    history_scan_started_at_ms: int
    history_scan_completed_at_ms: int
    trades_high_water: Optional[str]
    orders_high_water: Optional[str]
    cycles: Sequence[ExternalSettlementCycle]
    evidence: Sequence[ExternalSettlementEvidence]

NeutralGridStore.record_external_settlement(tx, request) -> int
NeutralGridStore.external_settled_quantity(grid_id, cell_id, generation) -> Decimal
NeutralGridStore.external_position_totals(grid_id: Optional[str] = None) -> Tuple[Decimal, Decimal]
```

`Cycle.external_settled`, `Cycle.effective_exit`, `Cycle.open_obligation`, `CycleRecord.external_settled`, and `Buckets.external_settled` are also frozen. `Cycle.X`, `CycleRecord.exit_filled`, and `Buckets.X` always mean owned exchange TP fills.

### Task 1: Append-only settlement schema and records

**Files:**
- Create: `hummingbot/strategy_v2/executors/neutral_grid_executor/migrations/m0006_external_settlements.py`
- Modify: `hummingbot/strategy_v2/executors/neutral_grid_executor/migrations/__init__.py`
- Modify: `hummingbot/strategy_v2/executors/neutral_grid_executor/store.py`
- Test: `test/hummingbot/strategy_v2/executors/neutral_grid_executor/store/test_ng_store_external_settlement.py`

- [ ] **Step 1: Write migration and append-only tests**

Create tests that open a v5 fixture through the normal store, assert schema version 6, insert one valid settlement through the public API, and prove SQL `UPDATE`/`DELETE` against all three new tables raises `StoreIntegrityError`.

```python
def test_external_settlement_tables_are_append_only(env):
    store = env.open()
    env.bootstrap(store)
    settlement_id = settle_exact_manual_close(store)
    assert settlement_id > 0
    with pytest.raises(StoreIntegrityError):
        with store.transaction():
            store._x("DELETE FROM external_settlements WHERE id = ?", (settlement_id,))
```

- [ ] **Step 2: Run the new test and confirm it fails**

Run: `pytest -q test/hummingbot/strategy_v2/executors/neutral_grid_executor/store/test_ng_store_external_settlement.py`

Expected: FAIL because migration 6 and settlement records do not exist.

- [ ] **Step 3: Add migration 6 and frozen record types**

Implement the three tables and immutable triggers exactly as specified in the design. Register migration 6 in `MIGRATIONS`. Add the three frozen request records plus read records to `store.py`; validate every string, integer, enum, and `Decimal` before the first SQL write.

- [ ] **Step 4: Implement exact store validation and insertion**

`record_external_settlement()` must reject unless:

```python
assert request.observed_position == Decimal("0")
assert request.grid_id == store.engine().current_grid_id
assert sum(c.quantity for c in request.cycles) == sum(
    e.allocated_quantity for e in request.evidence if e.evidence_role == "TRADE"
)
```

Require exactly one cycle with positive quantity equal to its current `entry_filled - exit_filled - prior_S`; all legs final; no active reservation/outbox; no dust/late evidence. For every trade evidence row require unresolved `UNMATCHED`, stream `TRADES`, the settlement side, positive exact size, correct account/market, and allocated quantity equal to the full trade size. Require one unresolved `INACTIVE_ORDERS` row for the same manual exchange/client order with final filled status, zero remainder, `reduce_only=true`, and cumulative equal to the selected trades. Reject multiple cycles/orders, mixed manual order IDs, split evidence, and all ambiguous alternatives.

Insert the audit, header, cycle rows, and evidence rows, then resolve inbox rows as `external_settlement:<id>` in the caller's transaction. Do not touch `fills`, `legs.filled`, `cycles.exit_filled`, `cid_map`, or `baseline_adjustments`.

- [ ] **Step 5: Add duplicate, ambiguity, and transaction rollback tests**

Cover wrong side, 4.38-only partial selection, 10.01 over-allocation, non-reduce-only terminal order, already-resolved evidence, second proof for the same cycle, and a fault before commit. After each refusal assert all three settlement tables and inbox resolution fields are unchanged.

- [ ] **Step 6: Run store tests and commit**

Run:

```bash
pytest -q test/hummingbot/strategy_v2/executors/neutral_grid_executor/store/test_ng_store_external_settlement.py \
  test/hummingbot/strategy_v2/executors/neutral_grid_executor/store/test_ng_store_crash_consistency.py \
  test/hummingbot/strategy_v2/executors/neutral_grid_executor/store/test_ng_store_bootstrap_config.py
```

Expected: PASS.

Report the owned file list and passing command to the primary agent. Do not stage or commit another writer's files; the primary performs ownership-scoped staging.

### Task 2: Core `E / X / S` accounting

**Files:**
- Modify: `hummingbot/strategy_v2/executors/neutral_grid_executor/cells.py`
- Modify: `hummingbot/strategy_v2/executors/neutral_grid_executor/risk.py`
- Modify: `hummingbot/strategy_v2/executors/neutral_grid_executor/store.py`
- Test: `test/hummingbot/strategy_v2/executors/neutral_grid_executor/core/test_external_settlement.py`
- Test: `test/hummingbot/strategy_v2/executors/neutral_grid_executor/store/test_ng_store_external_settlement.py`

- [ ] **Step 1: Write failing core accounting tests**

Construct a cycle with owned BUY `E=10`, owned TP `X=0`, and `external_settled=10`. Assert:

```python
assert cycle.X == D("0")
assert cycle.external_settled == D("10")
assert cycle.effective_exit == D("10")
assert cycle.open_obligation == D("0")
assert cycle.buckets().external_settled == D("10")
assert ledger.can_release(position_reconciled=True).ok
assert endpoints_from_ledgers(D("0"), [ledger]).P == D("0")
assert endpoints_from_ledgers(D("0"), [ledger]).gross_worst == D("0")
```

Also assert an external SELL 9 leaves obligation/gross 1 and prevents release, while external SELL 11 violates invariants.

- [ ] **Step 2: Run the focused tests and confirm failure**

Run: `pytest -q test/hummingbot/strategy_v2/executors/neutral_grid_executor/core/test_external_settlement.py`

Expected: FAIL because `S` is not modeled.

- [ ] **Step 3: Implement the derived accounting**

Add `external_settled: Decimal = ZERO` to `Cycle`. Keep `X` unchanged. Replace obligation-only expressions with `cycle.open_obligation`, and enforce:

```python
effective_exit = cycle.X + cycle.external_settled
open_obligation = cycle.E - effective_exit
effective_exit + reserved_tp_unfilled <= cycle.E
```

Update TP dispatch, allocation, dust, release, old-cycle lock, cell state, and invariant checks at every `E-X` consumer identified in the design audit. `risk.endpoints_from_ledgers()` adds actual settlement BUY/SELL quantity to confirmed net by using the opposite of the cycle entry side, and uses `abs(open_obligation)` for gross.

- [ ] **Step 4: Project `S` through store records and verification**

Populate `CycleRecord.external_settled` with a correlated sum from `external_settlement_cycles`. Change migration blockers, `late_obligation_cycles`, cycle release blockers, TP capacity checks, and `position_ledger()` to use `S`. `verify_ledger()` must still compare `entry_filled/exit_filled` only to owned fills, then independently report malformed settlement totals/evidence.

- [ ] **Step 5: Run core/store suites and commit**

Before the suite run, add two restart-history regressions. A late owned ENTRY fill after settlement must be committed as real history, set late-evidence/manual-reconcile freeze, yield the exact positive `E-X-S` remainder, and dispatch no TP until the existing late-evidence audit succeeds. A late owned TP fill that produces `X+S>E` must be committed as evidence, raise the history/ledger conflict, keep `S` immutable, and dispatch neither entry nor TP. Neither row may be ignored because a settlement exists.

Run:

```bash
pytest -q test/hummingbot/strategy_v2/executors/neutral_grid_executor/core \
  test/hummingbot/strategy_v2/executors/neutral_grid_executor/store
```

Expected: PASS.

Report the owned file list and passing command to the primary agent. Do not commit from a shared implementation worktree.

### Task 3: Proof-bound engine command

**Files:**
- Modify: `hummingbot/strategy_v2/executors/neutral_grid_executor/commands.py`
- Modify: `hummingbot/strategy_v2/executors/neutral_grid_executor/engine.py`
- Test: `test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/test_ng_engine_external_settlement.py`

- [ ] **Step 1: Write the exact retained-case engine test**

Using `Harness`, fill an owned BUY 10, prove its entry and cancelled TP final, issue a manual reduce-only SELL order with two trades 4.38 and 5.62, set venue position to 0 and active orders to empty, finish the required stable complete scans, then STOP. Assert a candidate is published and a proof-bound command applies.

```python
h.command(CommandKind.BASELINE_AUDIT, {
    "action": "settle_external_close",
    "proof_id": candidate["proof_id"],
    "confirmation": "SETTLE EXTERNAL CLOSE grid-t AT FLAT 0",
    "note": "manual reduce-only close after test",
    "acknowledge": True,
}, key="settle-external-0001")
```

Assert the engine remains stopped, `effective_baseline == 0`, endpoints `P == 0`, no open obligation, and no transport submits/cancels occurred during command application.

- [ ] **Step 2: Run and confirm failure**

Run: `pytest -q test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/test_ng_engine_external_settlement.py`

Expected: FAIL because the audit action/candidate is unknown.

- [ ] **Step 3: Add action, candidate, and proof hash**

Add `settle_external_close` to `EXTENDED_AUDIT_ACTIONS`. Build the candidate only from current store/in-memory facts. Canonically hash identity, revisions, exact cycle `E/X/S`, selected inbox IDs/payload hashes/quantities, observed position value, and empty active-order fingerprint. Do not hash observation/scan timestamps or other values that change on each polling tick. Publish explicit blocker codes when the set is not uniquely provable.

- [ ] **Step 4: Apply with all gates rechecked**

In `_cmd_reconcile`, require stopped-clean state, flat fresh/stable position, fresh empty active list, the existing stable-cut predicate (an unchanged position window followed by a complete history walk), no WS/history/order/store ambiguity, and exact proof equality. Re-read current freshness and store the latest timestamps/high-waters in `ExternalSettlementRequest`; they are audit evidence, not proof identity. Call the store in the command transaction, close the newly releasable cycle, clear only its ordinary obligation metadata, and return `reload=True`. Never mutate stop metadata.

- [ ] **Step 5: Add refusal and crash/reopen tests**

Parameterize stale proof, wrong revision, `STOP_UNCERTAIN`, position 1, active foreign order, active owned order, stale active list, incomplete history, pending WS trade, non-final leg, unresolved outbox, reservation, conflict, and mismatched quantities. Assert rejection and zero settlement rows. Advance several ordinary position/active polling ticks without changing semantic material and assert the proof remains stable and applies once the stable-cut gate is ready. Simulate crash before commit and reopen; then simulate after commit and assert one applied command/settlement and STOP retained.

- [ ] **Step 6: Run engine suites and commit**

Run:

```bash
pytest -q test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/test_ng_engine_external_settlement.py \
  test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/test_ng_engine_ops.py \
  test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/test_ng_engine_review2.py
```

Expected: PASS.

Report the owned file list and passing command to the primary agent. Do not commit from a shared implementation worktree.

### Task 4: Snapshot, command intake, and operator UI

**Files:**
- Modify: `hummingbot/strategy_v2/executors/neutral_grid_executor/snapshot.py`
- Modify: `web/neutral_grid/commands.py`
- Modify: `web/neutral_grid/static/app.js`
- Test: `test/web/neutral_grid/test_ngweb_commands.py`
- Test: `test/web/neutral_grid/test_ngweb_browser.py`
- Test: `test/web/neutral_grid/test_ngweb_round3.py`

- [ ] **Step 1: Write failing snapshot/web tests**

Assert each affected cell exposes:

```json
{"obligation":{"E":"10","X":"0","external_settled":"10","open":"0"}}
```

Assert `summary.external_close_candidate` carries the exact proof and evidence, the action is offered only when candidate blockers are empty, and intake rejects a proof/phrase that does not match the committed snapshot.

- [ ] **Step 2: Run and confirm failure**

Run: `pytest -q test/web/neutral_grid/test_ngweb_commands.py test/web/neutral_grid/test_ngweb_round3.py`

Expected: FAIL because the action is not recognized.

- [ ] **Step 3: Add safe serialization and validation**

Expose `external_settled`, `open`, and the candidate with decimal strings and IDs as strings. Normalize only `proof_id`, exact phrase, note, and acknowledgement; do not accept client-supplied allocations. `_extended_audit_blocker()` compares the proof to `summary.external_close_candidate.proof_id` and requires an empty blocker list.

- [ ] **Step 4: Add the focused UI flow**

Add the action label “Подтвердить ручное закрытие и подготовить перезапуск”. Render the exact cycle and 4.38/5.62 trade evidence, order `reduce_only/final`, flat position, and `E / X биржа / S вручную / остаток`. Use the phrase `SETTLE EXTERNAL CLOSE <grid_id> AT FLAT 0`. After application display “Подготовлено; бот остаётся остановлен. Проверьте превью и нажмите Старт отдельно.”

- [ ] **Step 5: Run web tests and commit**

Run:

```bash
pytest -q test/web/neutral_grid/test_ngweb_commands.py \
  test/web/neutral_grid/test_ngweb_round3.py \
  test/web/neutral_grid/test_ngweb_browser.py
```

Expected: PASS.

Report the owned file list and passing command to the primary agent. Do not commit from a shared implementation worktree.

### Task 5: Maintenance attach remains stopped

**Files:**
- Modify only if a failing test proves necessary: `scripts/lighter_robinhood_fixed_neutral_grid.py`
- Test: `test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/test_ng_engine_review2.py`
- Test: `test/scripts/test_lighter_robinhood_neutral_grid.py`

- [ ] **Step 1: Add a launcher regression test**

With a durable STOP and valid ordinary launch confirmation but no `resume_after_stop_confirmation`, assert the executor is constructed for read-only reconciliation, the launcher START is rejected with `DURABLE_STOP_ACTIVE`, account/history polling continues, and no submit/cancel transport call occurs.

- [ ] **Step 2: Run the test**

Run: `pytest -q test/scripts/test_lighter_robinhood_neutral_grid.py -k durable_stop`

Expected: PASS on current behavior. If it fails, make the minimum launcher/controller change that creates a maintenance executor while omitting resume authority; do not synthesize a resume phrase.

- [ ] **Step 3: Re-run and commit only if source changed**

Run: `pytest -q test/scripts/test_lighter_robinhood_neutral_grid.py`

Expected: PASS.

If source changed, report that exact path and the passing launcher test to the primary agent for scoped staging.

### Task 6: End-to-end settlement, migration, and explicit start

**Files:**
- Test: `test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/test_ng_engine_external_settlement.py`
- Modify if traceability text needs the new action: `docs/neutral-grid/CONTRACTS.md`

- [ ] **Step 1: Add the complete offline acceptance test**

After exact settlement, assert `grid_mutation_blockers() == []`. Close/reopen with config:

```python
make_config(
    grid_id="lit-neutral-fixed-v2",
    lower_price=D("4.9"),
    upper_price=D("5.9"),
    cell_count=24,
    order_amount_base=D("20"),
    leverage=D("5"),
    max_active_orders=120,
    max_abs_net_position=D("1000"),
    max_gross_position=D("1000"),
)
```

Open with migration authority, apply existing `migrate_grid`, and assert the engine is still stopped, the old `E=10/X=0/S=10` cycle and evidence remain queryable, the new grid has 24 cells and 25 boundaries in 4.9…5.9, and preview admits 24 cells with 120 reserved slots under the fixture's 5-LIT minimum.

- [ ] **Step 2: Prove START is separate**

Before START assert no new submits. Apply the existing explicit web-style START with current preview/material IDs and expected revisions. Assert new entry orders use 20 LIT and the v2 grid identity; assert no TP is ever created for the old settled generation.

- [ ] **Step 3: Run end-to-end test and update the contract**

Run: `pytest -q test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/test_ng_engine_external_settlement.py -k settlement_migration_start_24x20`

Expected: PASS.

Document `S`, the proof-bound command, and no-auto-start behavior in `docs/neutral-grid/CONTRACTS.md`.

- [ ] **Step 4: Commit**

Report the test and documentation paths to the primary agent for scoped staging.

### Task 7: Full verification and review

**Files:**
- Verify all modified files

- [ ] **Step 1: Run focused full subsystem suites**

```bash
pytest -q test/hummingbot/strategy_v2/executors/neutral_grid_executor \
  test/web/neutral_grid \
  test/scripts/test_lighter_robinhood_neutral_grid.py
```

Expected: PASS with no skipped new recovery tests.

- [ ] **Step 2: Run static checks used by the repository**

```bash
pre-commit run --files \
  hummingbot/strategy_v2/executors/neutral_grid_executor/migrations/m0006_external_settlements.py \
  hummingbot/strategy_v2/executors/neutral_grid_executor/migrations/__init__.py \
  hummingbot/strategy_v2/executors/neutral_grid_executor/store.py \
  hummingbot/strategy_v2/executors/neutral_grid_executor/cells.py \
  hummingbot/strategy_v2/executors/neutral_grid_executor/risk.py \
  hummingbot/strategy_v2/executors/neutral_grid_executor/commands.py \
  hummingbot/strategy_v2/executors/neutral_grid_executor/engine.py \
  hummingbot/strategy_v2/executors/neutral_grid_executor/snapshot.py \
  web/neutral_grid/commands.py web/neutral_grid/static/app.js
```

Expected: PASS.

- [ ] **Step 3: Review the actual diff against the safety invariants**

Confirm by search/diff that settlement code never updates `fills`, `legs.filled`, `cycles.exit_filled`, `cid_map`, `baseline_adjustments`, or stop metadata; no action invokes connector submit/cancel; all `E-X` obligation consumers were migrated to `E-X-S`; every new table is append-only; and no raw secrets/history payloads enter snapshots or audit events.

- [ ] **Step 4: Hand final documentation fixes to the primary**

Report the documentation paths and verification evidence. The primary stages only reviewed files after both writers finish.

The implementation is then ready for the operator-run maintenance attach and web action. Do not touch the live database or send exchange commands as part of development verification.
