# Neutral grid — workstream contracts

Spec: `docs/superpowers/specs/2026-09-24-neutral-grid-spec.md` (authoritative; it wins over this file).
Handoff: `docs/tasks/claude-neutral-grid.md`.
Spec commit: `ba597722d` (origin/codex/lighter-robinhood). Integration branch: `codex/neutral-grid-implementation`.
Frozen shared types: `hummingbot/strategy_v2/executors/neutral_grid_executor/contracts.py` (additive changes only; if you must add a field/enum member, add it with a default and mention it in your trace file).

Python: `$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python` (prepared env used by the repo launchers). Run tests with `<python> -m pytest <paths> -q`.

## Workstreams, branches, file ownership

Each workstream works in its own worktree on its own branch created from the contracts commit, and touches ONLY its owned paths (plus its own trace file). Integration merges them into `codex/neutral-grid-implementation`.

| WS | Branch | Owns | ACs primarily proven here |
|---|---|---|---|
| A core | `codex/ng-core` | `neutral_grid_executor/{grid,cells,risk,admission,router,cid,dust}.py`, `test/hummingbot/strategy_v2/executors/neutral_grid_executor/core/` | 03, 05, 06, 07, 23–28, 31, 33, 34, 39, 43(logic), 44, 57 + property tests |
| B store | `codex/ng-store` | `neutral_grid_executor/store.py`, `neutral_grid_executor/migrations/`, `test/.../neutral_grid_executor/store/` | 15, 16, 18, 19, 22(DB), 43(durable), 52, 53(persist), 54, 55 |
| C connector | `codex/ng-connector` | `hummingbot/connector/derivative/lighter_perpetual/*` (narrow additive changes), `neutral_grid_executor/{history,lighter_port}.py`, `test/hummingbot/connector/derivative/lighter_perpetual/test_lighter_perpetual_history_pagination.py`, `test/.../neutral_grid_executor/history/` | 09, 10, 11, 12, 22(connector), 40, 41, 42(scan), 56(venue mapping) |
| D engine | `codex/ng-engine` | `neutral_grid_executor/{engine,executor,fake_exchange,commands,snapshot}.py`, `neutral_grid_executor/data_types.py`, `controllers/generic/neutral_grid.py`, `scripts/lighter_robinhood_fixed_neutral_grid.py`, `conf/.../*neutral_grid*example*` (new disabled example), `test/.../neutral_grid_executor/engine/`, `test/controllers/generic/test_neutral_grid.py` | 01, 02, 04, 08, 13, 14, 17, 20, 21, 29, 30, 32, 35, 36, 37, 38, 42(release), 45, 51, 53, 56(engine) + cross-module integration of all ACs |
| E web | `codex/ng-web` | `web/neutral_grid/**`, `bin/lighter_robinhood_neutral_grid_web.py`, `test/web/neutral_grid/**` | 46, 47, 48, 49, 50 |

`neutral_grid_executor/__init__.py` is owned by D (A/B/C must not create it with content beyond an empty file; if you need it to import, create it empty).

## Module APIs (target shapes — implement these names)

### A core (pure, no IO, no asyncio)
- `grid.build_grid(lower, upper, cell_count, rules) -> List[Decimal]` — NG-GRID-001 integer ticks, `ValueError` subclass `GridValidationError` on non-multiple/collapse/insufficient ticks.
- `grid.assign_cells(prices, anchor) -> List[CellSpec]`; `grid.validate_config(cfg, rules, mid) -> List[str]` (errors, including full-Q min base/notional/step at both entry and TP price of every cell; MARKET forbidden).
- `cells.CellLedger` — per-cell cycle model: `confirmed_entry E`, `confirmed_exit X`, TP children with requested/filled/state, buckets `live_tp_remainder`, `reserved_tp_unassigned`, `dust`; `apply_fill(leg, qty)` idempotent by fill key; `tp_obligation_to_dispatch(rules)` returns exact quantities (never rounded up; below-min stays undispatched; step-remainder becomes dust only when entry is terminal); `can_release()` checks NG-CELL-001 (1)–(5); aggregate `CellState` derived, never stored as the only truth.
- `risk.endpoints(baseline, confirmed_buys, confirmed_sells, open_legs) -> (P, P_min, P_max, gross_worst)`; `risk.check_submit(...) -> Optional[str blocker]`; margin advisory `(warning|None, blocks: bool)`.
- `admission.required_slots(Q, rules, tp_price)`, `admission.plan(cells, cap, mid) -> armed/queued/slot ledger` (exit priority, distance then cell_id).
- `router.plan_submits(pending_intents, owned_orders) -> actions` — self-trade check vs live/pending/unknown; TP priority: conflicting entry -> cancel-pending, TP waits history terminal; TP–TP FIFO; no internal netting.
- `cid.CidAllocator` logic (monotonic, 48-bit bound, collision check against a provided `exists(cid)` callback, fail-closed `CidExhausted`).
- `dust.aggregate(...)` same side+target only, per-cell allocation, idempotent.

### B store (sqlite3 stdlib, single writer)
- `NeutralGridStore.open(path, engine_identity, create_if_missing: bool, prior_run_markers: callable)`; fails closed on missing/corrupt DB with prior-run evidence (AC-54); `schema_version` table, migrations fail-closed on unknown version.
- Host lock file on `(account_index, market)` (fcntl), documented as NOT distributed.
- `with store.transaction() as tx:` — one SQLite transaction (BEGIN IMMEDIATE); decimals stored as TEXT, ids as TEXT/INTEGER (CID int), never REAL. Raise `PersistenceError` on commit/fsync/disk-full/read-only; engine must then go DEGRADED and send nothing.
- Tables (minimum): `engine`, `config_revisions`, `cells`, `cycles`, `legs`, `orders`, `fills`, `history_inbox`, `dedupe_keys`, `cursors`, `outbox`, `commands`, `audit_events`, `snapshots`, `cid_map`.
- Atomic ops: `record_intent(tx, leg_identity, submit_request, reservation)`; `allocate_cid(tx, leg_identity) -> int` (durable map CID -> full leg identity, never reused, reuse of identity returns same CID); `apply_history_batch(tx, inbox_rows, dedupe_keys, cursor_updates, ledger_transitions)` in ONE transaction; `record_transport_result(tx, cid, result)`; `enqueue_command(idempotency_key, kind, expected_config_rev, expected_engine_rev, payload) -> command row` (same key returns same row); `write_snapshot(tx, snapshot_json)`; `latest_snapshot()`.
- Crash injection: `store.fault_hooks` — named points (`before_intent_commit`, `after_intent_commit`, `after_transport_before_result_commit`, `before_cursor_commit`, ...) that tests can make raise `SimulatedCrash`.

### C connector + history
- Connector (additive, no rewrite): `async fetch_inactive_orders_page(cursor, limit=100)`, `async fetch_trades_page(cursor, limit=100)` returning raw dict + `next_cursor` verbatim (exact SDK 1.1.4 parameter names from the generated client in the env: verify in site-packages `lighter/api/*.py`); `async submit_with_client_id(cid:int, ...)` that uses the pre-persisted numeric CID unchanged (no `_new_client_order_id`), `reduce_only=False` explicit, goes through the existing tx lock/signer/order tracker; returns a classification mapping to `TransportOutcome` (only documented definitive rejects => `DEFINITIVE_REJECT_ZERO_FILL`).
- `lighter_port.LighterExchangePort` implements `contracts.ExchangePort` over the connector, ids as strings, Decimal via `Decimal(str)`, never float for ids.
- `history.HistoryScanner(port, store_cursor_view, overlap_s, weight_budget)` — newest→oldest, `limit=100`, opaque cursor, continues past duplicates/found IDs until natural end or verified overlap boundary; repeated/malformed cursor, conflicting duplicate, schema error, or break before boundary => `complete=False`; returns `HistoryScanResult`. Bounded page work per call (resumable, stays RECONCILING across ticks). Canonical dedupe key per contracts.

### D engine
- `engine.NeutralGridEngine(config, store, port, clock, scanner)`; `async tick()` does: drain command queue → reconcile (position/active/history) → apply fills to ledger (one tx with cursor) → risk/admission → router → outbox dispatch (intent committed before transport) → snapshot commit. Single asyncio task for all cells (AC-04).
- `fake_exchange.FakeExchange` implements `ExchangePort` deterministically: controllable fills/partials, WS signal vs history lag, pagination (>100 rows), duplicate/reorder, timeouts, definitive rejects, cancel ambiguity, self-trade two-leg rows, runtime rule changes, retention window. Lives in the package (web offline demo uses it).
- `executor.NeutralGridExecutor(ExecutorBase)` + `controllers/generic/neutral_grid.py` (V2 controller creating exactly ONE executor) + launcher + disabled example config.
- `commands.py`: apply command rows from store; idempotent; revision checks.
- `snapshot.py`: builds snapshot JSON (schema below) from committed state.

### E web
- stdlib/aiohttp (already a hummingbot dep) server, bind `127.0.0.1` default; refuses non-loopback unless explicit flag + documented warning.
- Reads only `store.latest_snapshot()`; writes only `store.enqueue_command(...)`. Never talks to exchange; never returns secrets.
- Session cookie (HttpOnly, SameSite=Strict) + CSRF token header + Origin check on every POST; keystore profile selection by name, masked presence only; unlock via backend prompt/stdin or form POST body (never argv/logs/localStorage/responses).
- Russian responsive UI, no CDN assets; `--demo-fake-exchange` mode runs the engine with `FakeExchange` for browser acceptance.

## Snapshot JSON (v1)
```
{ "snapshot_version", "config_revision", "engine_revision", "committed_at", "engine_state", "reasons": [],
  "summary": { baseline, authoritative_net, P, P_min, P_max, gross_worst, max_abs_net_position, max_gross_position,
               slots: {actual, reserved, free, cap}, armed, queued, owned_active, unknown_orders,
               history: {complete, lag_s, trades_cursor, orders_cursor, last_full_scan_at, incomplete_reason},
               margin: {available, required_estimate, warning}, runtime_rules: {...}, dust_total },
  "cells": [ { cell_id, low, high, entry_side, generation, state, entry: {cid, exchange_id, requested, filled, remaining, state},
               tp_children: [{cid, exchange_id, requested, filled, remaining, state, expiry}], obligation: {E, X, live_tp, reserved_unassigned, dust},
               blocker, queue_age_s } ],
  "unmatched_evidence": [], "errors": [ {at, code, message} ], "commands": [ {id, kind, status, result} ] }
```
All decimals and ids are JSON strings.

## Commands table
`(id, idempotency_key UNIQUE, kind, expected_config_revision, expected_engine_revision, payload_json, status, result_json, created_at, applied_at)`. Stale expected revision => `CONFLICT` with fresh preview; duplicate key => original row.

## Trace files
Each WS writes `docs/neutral-grid/trace/<ws>.md`: rows `NG-*/AC-xx | component | test node ids | status`. Integration merges them into `docs/neutral-grid/TRACEABILITY.md`.
