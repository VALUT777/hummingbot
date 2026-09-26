# Neutral Grid Directional Gross Limits Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in independent long-entry and short-entry gross caps while keeping legacy aggregate-gross and net-cap behavior unchanged.

**Architecture:** Risk endpoints always calculate aggregate and per-direction entry exposure. A default-false config flag selects which gross invariant admission enforces. A persisted-versus-requested policy mismatch blocks only new entries until an explicit reviewed START records the non-geometry config revision.

**Tech Stack:** Python dataclasses and Decimal arithmetic, SQLite configuration revisions, pytest engine harness.

---

### Task 1: Directional risk accounting

**Files:**
- Modify: `hummingbot/strategy_v2/executors/neutral_grid_executor/risk.py`
- Modify: `test/hummingbot/strategy_v2/executors/neutral_grid_executor/core/test_risk.py`
- Modify: `test/hummingbot/strategy_v2/executors/neutral_grid_executor/core/test_router.py`

- [ ] Add failing tests for independent BUY/SELL exposure, partial and unknown reservations, TP exclusion, legacy four-argument endpoint fail-closed behavior, and sequential router admission.
- [ ] Run the focused core tests and confirm failures describe missing directional accounting.
- [ ] Add defaulted directional endpoint fields and the mode to `RiskLimits`; calculate both directional values for cycles and open legs.
- [ ] Update submit, endpoint accumulation, and violation checks while retaining aggregate behavior when the flag is false.
- [ ] Run the focused core tests and confirm they pass.

### Task 2: Configuration compatibility

**Files:**
- Modify: `hummingbot/strategy_v2/executors/neutral_grid_executor/contracts.py`
- Modify: `hummingbot/strategy_v2/executors/neutral_grid_executor/data_types.py`
- Modify: `controllers/generic/neutral_grid.py`
- Modify: `test/hummingbot/strategy_v2/executors/neutral_grid_executor/core/test_grid.py`

- [ ] Add failing tests that default false is omitted from canonical JSON, true round-trips, and legacy JSON loads as false.
- [ ] Add the default-false config field to core, executor, and controller models and propagate it to `GridConfig`.
- [ ] Omit false during canonical serialization and include true explicitly.
- [ ] Run the focused configuration tests and confirm they pass.

### Task 3: Reviewed policy activation

**Files:**
- Modify: `hummingbot/strategy_v2/executors/neutral_grid_executor/engine.py`
- Modify: `hummingbot/strategy_v2/executors/neutral_grid_executor/snapshot.py`
- Create: `test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/test_ng_engine_directional_gross.py`

- [ ] Add failing engine tests for persisted/requested mismatch, TP continuity, blocked entries, launcher refusal, explicit reviewed START revision, restart persistence, unrelated config refusal, and true-to-false tightening.
- [ ] Read the active mode from the latest persisted config revision and build `RiskLimits` from it; expose requested versus active policy.
- [ ] Add the entry-only policy-review blocker without adding it to TP-blocking freezes.
- [ ] Extend explicit START to validate the sole permitted policy delta, call `record_config_revision` in its command transaction, update the full START fingerprint, audit, and reload. Keep launcher and `already_started` paths fail-closed.
- [ ] Include the policy field in future extension proof comparisons and require it to equal the persisted active
      policy; extension actions must never approve a pending policy change.
- [ ] Publish directional exposure and active/requested mode in the snapshot.
- [ ] Run the new engine test file and confirm it passes.

### Task 4: Concrete acceptance and regression

**Files:**
- Modify: `test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/test_ng_engine_directional_gross.py`

- [ ] Add the 400-long, six SELL-entry, BUY-100 acceptance case and assert long 500, short 600, aggregate 1100, reachable net `[-600, 500]`, preserved TPs, and unchanged ±1000 net cap.
- [ ] Add restart checks that pending and unknown entries remain reserved in their original direction.
- [ ] Run focused risk, router, grid, snapshot, external-entry, and directional engine suites.
- [ ] Run changed-file Flake8 and `git diff --check`.
- [ ] Leave the work uncommitted for root review and production rollout.
