# Neutral grid — traceability (requirements → components → tests)

Status legend: PLANNED (before code) → TESTED (node id green on integration branch) → GAP (explicitly unresolved, see acceptance report).
Owners: A core, B store, C connector/history, D engine/controller/launcher, E web. `pkg` = `hummingbot/strategy_v2/executors/neutral_grid_executor`.
Test node ids are filled in at integration from `docs/neutral-grid/trace/ws-*.md`; nothing is marked TESTED without a fresh green run.

## Requirements (NG-*)

| Req | Summary | Component(s) | Owner | Proven by |
|---|---|---|---|---|
| NG-GRID-001 | integer-tick prices, N+1 lines, validation errors not quantization | `pkg/grid.py` | A | AC-03 |
| NG-GRID-002 | anchor=clamp(mid); side by P[i]<anchor; fixed forever | `pkg/grid.py`, `pkg/engine.py` bootstrap | A, D | AC-01, AC-02, AC-03, AC-20 |
| NG-GRID-003 | exact size step, no round-up, dust, same side+target aggregation | `pkg/grid.py`, `pkg/cells.py`, `pkg/dust.py` | A | AC-03, AC-23, AC-33, AC-39 |
| NG-ORD-001 | post-only entry, LIMIT GTT TP, no MARKET/clamp/activation gate | `pkg/grid.py` validate, connector submit, `pkg/engine.py` | A, C, D | AC-01, AC-02, AC-38, AC-51 |
| NG-ORD-002 | virtual cells, reduce_only=false, no PositionAction.CLOSE | connector `submit_with_client_id`, `pkg/engine.py` | C, D | AC-27 |
| NG-ORD-003 | self-trade router, TP priority, TP–TP FIFO, no netting | `pkg/router.py` | A, D | AC-31, AC-44 |
| NG-CELL-001 | whole-cell lock, rearm same side | `pkg/cells.py` | A, D | AC-01, AC-02, AC-06, AC-33 |
| NG-CELL-002 | partial entry → TP obligations, ENTRY_LIVE+TP_LIVE, buckets, ≤2 s dispatch SLO | `pkg/cells.py`, `pkg/engine.py` | A, D | AC-05, AC-13, AC-14, AC-57 |
| NG-CELL-003 | partial TP, terminal partial entry, late fill | `pkg/cells.py`, `pkg/engine.py` | A, D | AC-06, AC-07, AC-08 |
| NG-CELL-004 | distinct durable states (cell aggregate vs leg/order) | `contracts.py`, `pkg/cells.py`, `pkg/store.py` | A, B | AC-05, AC-16, AC-21 |
| NG-HIST-001 | history authoritative; exact id fields; own two-leg self trade | connector, `pkg/lighter_port.py`, `pkg/history.py` | C | AC-09, AC-22, AC-40 |
| NG-HIST-002 | full pagination, overlap boundary, dedupe key, conflicts, settlement, atomic cursor | `pkg/history.py`, `pkg/store.py` | C, B, D | AC-10, AC-11, AC-12, AC-19, AC-41, AC-42 |
| NG-HIST-003 | weights, coalescing, bounded page work, backoff | connector throttler, `pkg/history.py`, `pkg/engine.py` | C, D | AC-04, AC-13 |
| NG-HIST-004 | pre-persisted 48-bit CID, durable map, no new ID after timeout, nonce secondary | `pkg/cid.py`, `pkg/store.py`, connector | A, B, C | AC-16, AC-43, AC-56 |
| NG-RISK-001 | signed first-bootstrap baseline, durable cut, never recaptured | `pkg/engine.py`, `pkg/store.py` | D, B | AC-28, AC-45 |
| NG-RISK-002 | P/P_min/P_max, gross, caps, TP priority headroom, RISK_BLOCKED | `pkg/risk.py`, `pkg/router.py` | A, D | AC-24, AC-25, AC-26, AC-27, AC-28 |
| NG-RISK-003 | unknown active order blocks start; manual trade freezes entries | `pkg/engine.py` | D | AC-29, AC-30 |
| NG-RISK-004 | margin advisory; unknown data blocks exposure | `pkg/risk.py`, `pkg/engine.py` | A, D | AC-37 |
| NG-RISK-005 | full-Q admission at entry+TP, slot formula, bounded queue, exit priority | `pkg/admission.py`, `pkg/grid.py` | A, D | AC-03, AC-34, AC-44, AC-57 |
| NG-DB-001 | SQLite schema, no float, versioned, single writer + host lock | `pkg/store.py`, `pkg/migrations/` | B | AC-22, AC-43, AC-54 |
| NG-DB-002 | intent before side effect | `pkg/store.py`, `pkg/engine.py` | B, D | AC-15, AC-16, AC-17, AC-18 |
| NG-DB-003 | restart procedure | `pkg/engine.py` | D | AC-17, AC-20, AC-36 |
| NG-DB-004 | missing/corrupt DB, retention gap, disk full | `pkg/store.py`, `pkg/history.py`, `pkg/engine.py` | B, C, D | AC-53, AC-54, AC-55 |
| NG-DB-005 | definitive rejection only with proof | connector classification, `pkg/engine.py` | C, D | AC-56 |
| NG-OPS-001 | pause semantics | `pkg/engine.py`, `pkg/commands.py` | D | AC-30, AC-47, AC-50 |
| NG-OPS-002 | outside bounds | `pkg/engine.py` | D | AC-32 |
| NG-OPS-003 | stop / STOP_UNCERTAIN | `pkg/engine.py` | D | AC-35, AC-36 |
| NG-ARCH-001 | native V2 engine, one executor, no 55 PositionExecutors, no GridExecutor rewrite | `pkg/executor.py`, `controllers/generic/neutral_grid.py` | D | AC-04 + diff review |
| NG-ARCH-002 | integration points, old entrypoints unchanged | launcher `scripts/lighter_robinhood_fixed_neutral_grid.py` | D, C | diff review + regression gate |
| NG-ARCH-003 | config table, disabled example, immutable dims, keystore reuse | example config, controller config, `pkg/store.py` | D, B | AC-03, AC-52 |
| NG-UI-001 | loopback, snapshot versions, idempotent commands, session/CSRF/Origin, no secrets | `web/neutral_grid/`, `bin/lighter_robinhood_neutral_grid_web.py` | E | AC-47, AC-49 |
| NG-UI-002 | preview + honest states + tables | `web/neutral_grid/` | E | AC-46, AC-48, AC-50 |
| NG-UI-003 | command semantics, single engine | `web/neutral_grid/`, `pkg/commands.py` | E, D | AC-47 |
| §11 status | CLI status from same committed snapshot | `pkg/snapshot.py`, executor/controller format_status | D | status test in D |

## Acceptance scenarios (AC-*)

| AC | Owner(s) | Planned test location | Status |
|---|---|---|---|
| AC-01 Normal BUY cycle | D (A) | engine/ integration on FakeExchange | PLANNED |
| AC-02 Normal SELL cycle | D (A) | engine/ | PLANNED |
| AC-03 Grid arithmetic 22/33, rejections, bounce | A (D) | core/test_grid, engine bounce | PLANNED |
| AC-04 55 cells, one engine, no storm | D | engine/ | PLANNED |
| AC-05 Partial entry 2+3+5 floor 5 | A, D | core/test_cells, engine/ | PLANNED |
| AC-06 Partial TP | A, D | core/, engine/ | PLANNED |
| AC-07 Terminal partial entry | A, D | core/, engine/ | PLANNED |
| AC-08 Late fill after cancel | D (A) | engine/ | PLANNED |
| AC-09 WS duplicate/reorder | C, D | history/, engine/ | PLANNED |
| AC-10 >100 inactive orders | C | connector pagination + history/ | PLANNED |
| AC-11 >100 trades | C | connector pagination + history/ | PLANNED |
| AC-12 Bad pagination | C (D blocks entries) | history/, engine/ | PLANNED |
| AC-13 History lag | D | engine/ | PLANNED |
| AC-14 TP dispatch SLO ≤2 s | D | engine/ (injectable clock) | PLANNED |
| AC-15 Crash before intent commit | B, D | store/ crash, engine/ crash | PLANNED |
| AC-16 Crash after intent before API | B, D | store/, engine/ | PLANNED |
| AC-17 Crash during/after API timeout | D | engine/ | PLANNED |
| AC-18 Crash after API before response commit | B, D | store/, engine/ | PLANNED |
| AC-19 Crash before cursor commit | B, D | store/, engine/ | PLANNED |
| AC-20 Restart after price bounce | D | engine/ | PLANNED |
| AC-21 Cancel timeout ambiguity | D | engine/ | PLANNED |
| AC-22 String ID precision | B, C, E | store/, connector, web/ | PLANNED |
| AC-23 Rounding and fees | A | core/ | PLANNED |
| AC-24 Net long cap | A, D | core/test_risk, engine/ | PLANNED |
| AC-25 Net short cap | A, D | core/, engine/ | PLANNED |
| AC-26 Gross cap with net zero | A | core/ | PLANNED |
| AC-27 Virtual TP crosses zero | A, D, C | core/, engine/, connector reduce_only=false | PLANNED |
| AC-28 Initial manual baseline ± / 0 | A, D | core/, engine/ | PLANNED |
| AC-29 Manual order at startup | D | engine/ | PLANNED |
| AC-30 Manual trade while running | D | engine/ | PLANNED |
| AC-31 Self-trade conflict | A, D | core/test_router, engine/ | PLANNED |
| AC-32 Outside bounds | D | engine/ | PLANNED |
| AC-33 Dust | A, D | core/, engine/ | PLANNED |
| AC-34 Runtime minimum changes | A, D | core/test_admission, engine/ | PLANNED |
| AC-35 Stop success with inventory | D | engine/ | PLANNED |
| AC-36 Stop cancellation failure | D | engine/ | PLANNED |
| AC-37 Known low margin | A, D | core/test_risk, engine/ | PLANNED |
| AC-38 No market fallback | D | engine/ | PLANNED |
| AC-39 Same-price aggregation | A | core/test_dust | PLANNED |
| AC-40 History conflict | C, B, D | history/, store/, engine/ | PLANNED |
| AC-41 Full overlap | C | history/ | PLANNED |
| AC-42 Settlement and late evidence | C, D | history/, engine/ | PLANNED |
| AC-43 Client ID allocation | A, B | core/test_cid, store/ | PLANNED |
| AC-44 TP capacity | A, D | core/test_router+admission, engine/ | PLANNED |
| AC-45 Stable bootstrap cut | D, B | engine/, store/ | PLANNED |
| AC-46 UI preview | E | web/ | PLANNED |
| AC-47 UI commands | E | web/ | PLANNED |
| AC-48 UI truthful state | E | web/ | PLANNED |
| AC-49 UI security | E | web/ | PLANNED |
| AC-50 UI browser flow | E | web/ browser + manual check log | PLANNED |
| AC-51 GTT renewal | D | engine/ | PLANNED |
| AC-52 Config mutation | B, D | store/, engine/ controller | PLANNED |
| AC-53 History retention gap | C, B, D | history/, store/, engine/ | PLANNED |
| AC-54 Missing/corrupt DB | B, D | store/, engine/ | PLANNED |
| AC-55 Disk full/commit failure | B, D | store/, engine/ | PLANNED |
| AC-56 Zero-fill rejection | C, D | connector classification, engine/ | PLANNED |
| AC-57 Simultaneous partial-fill capacity | A, D | core/test_admission, engine/ | PLANNED |

Property tests (§13.3): A (pure invariants), D (engine orderings incl. restart).
