# Neutral Grid External Entry Adoption Implementation Plan

**Goal:** Extend `lit-neutral-fixed-v3` to `4.8–5.9` and atomically adopt one proven external 100 LIT BUY into new
cell 10, preserving `B=0`, the old 400 LIT cycles, and TPs `4.9–5.3`. Add an explicitly configured directional
outside-bounds entry policy and prevent the native launcher from attempting cold migration for a same-grid
maintenance operation.

**Scope:** Development checkout only. No production database/configuration changes, exchange calls, credentials,
or deployment. Preserve legacy behavior unless the new config flag is enabled.

## Task 1: Schema v8 and store-level external-entry accounting

**Files:**

- Create `hummingbot/strategy_v2/executors/neutral_grid_executor/migrations/m0008_external_entries.py`.
- Modify `hummingbot/strategy_v2/executors/neutral_grid_executor/migrations/__init__.py`.
- Modify `hummingbot/strategy_v2/executors/neutral_grid_executor/store.py`.
- Create `test/hummingbot/strategy_v2/executors/neutral_grid_executor/store/test_ng_store_external_entry.py`.

Write failing tests first for a valid atomic extension/adoption, partial evidence, wrong side, reduce-only/non-final
order, mismatched order ID, missing fill fragments, reused evidence/proof, wrong observed position, occupied target
cell, stale revisions, transaction fault rollback, v7 upgrade/restart, and damaged-v8 fail-closed behavior.

Implement append-only external-entry and evidence tables, typed request records, structural schema checks, evidence
validation, and one transaction that appends the window/cell, opens the cycle, credits external E, resolves evidence,
and writes audit state. Extend position and ledger verification to count external entry BUY exactly once and verify
cycle E against owned fills plus external allocations.

Run the new store test, then the full store suite.

## Task 2: Engine candidate, proof, and command

**Files:**

- Modify `hummingbot/strategy_v2/executors/neutral_grid_executor/commands.py`.
- Modify `hummingbot/strategy_v2/executors/neutral_grid_executor/engine.py`.
- Modify `hummingbot/strategy_v2/executors/neutral_grid_executor/snapshot.py`.
- Create `test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/test_ng_engine_external_entry.py`.

Write the representative failing test first: four old cycles, cell-2 split fill `73.22 + 26.78`, complete manual
BUY evidence totaling 100, stopped with no active orders, venue 500, and target config `4.8–5.9`. Cell 2's existing
TP obligation must be represented by children `73.22 + 26.78`. Assert one atomic
command yields `B=0/P=500`, stable IDs `10,0,…,9`, cell-10 `E100/X0/S0`, retained old cycles, and five TPs at
`4.9–5.3` after separately confirmed START.

Add malicious/stale candidate tests for causal cuts, extra/partial/reused evidence, wrong side/order/quantity/price,
position drift, target/config changes, and command proof changes. Implement the server-derived candidate and strict
command allowlist/confirmation. Reuse extension validation and coherent-cut helpers; do not accept accounting values
from the request or hardcode the concrete grid ID, bounds, cell ID, quantity, positions, or TP in runtime logic.
Keep the engine stopped after adoption and preserve unrelated freezes/reject latches. Accept the existing
proof-bound operational `max_active_orders` source/target difference (120 to 210 in this case), without rewriting
the old config revision or allowing a financial-policy change.

Run the focused engine tests and the full engine suite.

## Task 3: Proof-only web review

**Files:**

- Modify `web/neutral_grid/commands.py`.
- Modify `test/web/neutral_grid/test_ngweb_commands.py`.

Write failing tests that require a clean candidate and exact confirmation, reject blocker/stale/malformed requests,
and prove the enqueued payload contains no target config, order ID, trade ID, quantity, price, or cell override.
Implement the smallest allowlist addition and candidate-to-request binding.

Run the web command and round-three integration suites.

## Task 4: Approved directional outside-bounds policy

**Files:**

- Modify `controllers/generic/neutral_grid.py` and its typed executor config mapping.
- Modify `hummingbot/strategy_v2/executors/neutral_grid_executor/contracts.py` or the existing config type owner.
- Modify `hummingbot/strategy_v2/executors/neutral_grid_executor/engine.py`.
- Modify focused grid/risk/controller tests.

Add a default-off boolean configuration field so all existing profiles retain the current all-entry block outside
bounds. With the flag enabled: below the lower bound permit only SELL entries; above the upper bound permit only BUY
entries; inside permit both; unknown/stale/crossed books permit neither. TPs remain eligible. Apply the same side
decision to new admission and risk-reducing cancellation of already-live entries. Preserve every existing freshness,
history, reconciliation, cap, slot, margin, self-trade, terminal-proof, and TP-priority gate.

Test below/inside/above transitions, stale/crossed books, cancellation and terminal proof, TP continuity, restart,
and the live shape where five of six 100 LIT SELL entries can be admitted under gross cap 1000 with gross inventory
500. The target config and candidate proof bind enabling this field; legacy source revisions that lack the field
are interpreted as the default `false` without rewriting history.

## Task 5: Native same-grid maintenance-open guard

**Files:**

- Modify `hummingbot/strategy_v2/executors/neutral_grid_executor/executor.py`.
- Modify `test/controllers/generic/test_neutral_grid.py`.

Write a failing native integration test in which explicit maintenance authorization opens a fingerprint-mismatched
store with the same grid ID but does not enqueue `migrate_grid`. Preserve existing cold migration behavior when the
target grid ID differs. The extension/adoption remains operator proof-bound; this guard only removes the incorrect
legacy migration attempt/noise.

## Task 6: Verification and commit

Run focused adoption, extension, external-settlement, risk/admission, snapshot/commands, controller, and web tests;
then full store and engine suites. Run changed-file `flake8` and `git diff --check`. Review the complete diff for
production paths, secrets, accidental database/config edits, fake fill/CID creation, baseline changes, and weakened
legacy guards. Commit only the implementation, tests, and two design/plan documents owned by this work; exclude
unrelated/shared changes. Do not deploy.
