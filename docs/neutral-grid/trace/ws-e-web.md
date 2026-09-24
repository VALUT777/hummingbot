# WS-E web — traceability (NG-UI-001..003, AC-46..AC-50, AC-22 UI/API)

Branch `codex/ng-web`, worktree `hummingbot-ng-web`. Owned paths:
- `web/neutral_grid/**`;
- `bin/lighter_robinhood_neutral_grid_web.py`;
- `test/web/neutral_grid/**`;
- this file.

Test command:

```bash
$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python -m pytest test/web/neutral_grid -q
```

Test ids below are relative to `test/web/neutral_grid/`. `B:` marks the headless-Chrome browser tests
(`ngweb_cdp.py` drives Chrome over the DevTools protocol, because Playwright is not importable in the env).
`M:` marks manual checks in the Claude built-in browser pane (see "Manual browser checks").

## Components

| File | Role |
|---|---|
| `web/neutral_grid/server.py` | aiohttp app and routes. Read side serves only the committed snapshot and read-only store lookups; the single write path is `POST /api/commands`. |
| `web/neutral_grid/security.py` | Loopback bind policy; Host check (DNS rebinding); Origin, JSON and CSRF checks on unsafe methods; session cookie; access token; security headers; path-only request log. |
| `web/neutral_grid/commands.py` | Command intake: idempotency key, expected revisions, 409 with fresh preview, Start single-engine guard, payload normalisation. |
| `web/neutral_grid/gateway.py` | `EngineGateway` protocol. `StoreGateway` wraps `NeutralGridStore.open_command_client` (reads + INSERT into `commands` only). |
| `web/neutral_grid/preview.py` | Preview composed only from core `grid.build_preview`, `risk.reachable_interval`, `risk.required_margin_estimate` and `risk.margin_advisory`. `preview_id` fingerprints config + rules + revisions. |
| `web/neutral_grid/views.py` | Honest engine view (UNKNOWN / STALE), freshness, server-side Decimal gauges, cell pagination, exact-string lookup, display-time normalisation. |
| `web/neutral_grid/jsonsafe.py` | Boundary encoder: Decimals and ids become strings, as does any int beyond ±(2^53−1). |
| `web/neutral_grid/keystore.py` | Profile listing with masked presence; unlock via native `Security.login`; key-format check via `bin/lighter_robinhood_setup.normalize_api_private_key`. |
| `web/neutral_grid/host.py`, `runtime.py` | Demo: one engine plus FakeExchange plus temp SQLite, ticked by one host task. Attach: StoreGateway, with the preview market taken from the snapshot. |
| `web/neutral_grid/static/{index.html, app.css, app.js}` | Russian responsive UI; no CDN; CSP-safe; textContent only; no storage; no numeric conversion of ids or decimals. |
| `bin/lighter_robinhood_neutral_grid_web.py` | Launcher: `--demo-fake-exchange` or `--attach-db`; loopback default; refuses secret-like argv; `--unlock-tty`. |

## Requirements

| Req | What | Tests / checks | Status |
|---|---|---|---|
| NG-UI-001 | Loopback default; non-loopback bind needs an explicit flag plus a warning | `test_ngweb_security.py::test_default_bind_is_loopback_and_public_bind_refused`, `test_ngweb_launcher.py::test_defaults_bind_loopback_only`, `::test_main_refuses_public_bind_with_exit_code` | DONE |
| NG-UI-001 | Browser has no credentials, never calls the exchange, never writes state | `test_ngweb_store_gateway.py::test_command_client_cannot_write_ledger`, `test_ngweb_security.py::test_no_secret_readback_routes`, `::test_static_assets_are_local_and_js_never_stores_or_numbers_ids` | DONE |
| NG-UI-001 | Versioned committed snapshot with config/engine revision, timestamp and freshness | `test_ngweb_store_gateway.py::test_api_serves_latest_committed_store_snapshot`, `test_ngweb_state.py::test_stale_snapshot_is_never_shown_as_current` | DONE |
| NG-UI-001 | Idempotency key plus expected revisions; durable queue; retry returns the same result; stale gives 409 with a fresh preview | `test_ngweb_commands.py::test_retry_and_refresh_return_the_same_command`, `::test_stale_revision_is_409_with_fresh_preview_never_applied`, `test_ngweb_store_gateway.py::test_commands_go_to_durable_store_queue_idempotently`, `::test_store_side_conflict_when_engine_revision_moved_before_snapshot` | DONE |
| NG-UI-001 | UI never derives cell state from price | `test_ngweb_state.py::test_cell_state_comes_from_snapshot_not_price` | DONE |
| NG-UI-001 | Keystore profile by name, masked presence, unlock via backend (POST body or TTY), no secret readback | `test_ngweb_security.py::test_keystore_unlock_masked_and_secrets_never_leak`, `::test_keystore_is_never_created_from_web`, `::test_demo_mode_never_touches_keystore`, `test_ngweb_launcher.py::test_unlock_tty_requires_terminal_and_uses_hidden_prompt` | DONE |
| NG-UI-001 | Session, CSRF and Origin on commands; no CDN; no heavy dashboard | `test_ngweb_security.py::*` (401/403 tests), `::test_static_assets_are_local_and_js_never_stores_or_numbers_ids` | DONE |
| NG-UI-002 | Preview: N+1/N, BUY/SELL, armed/queued, slots actual/reserved/free, signed B, P_min/P_max, caps, leverage, notional/margin, floors, errors | `test_ngweb_preview.py::*` | DONE |
| NG-UI-002 | First live start: separate explicit confirmation of B and risk | `test_ngweb_commands.py::test_start_requires_explicit_baseline_and_risk_confirmation`, `::test_start_baseline_must_match_config`; M: Start dialog | DONE |
| NG-UI-002 | Engine states with reasons; no optimistic NORMAL or STOPPED; age/staleness visible | `test_ngweb_state.py::test_fresh_snapshot_state_is_shown_verbatim[*]`, `::test_no_snapshot_is_unknown_even_if_backend_process_alive`, `::test_unknown_engine_state_string_is_not_interpreted`, B: `test_ngweb_browser.py::test_browser_truthful_state_security_and_keyboard` | DONE |
| NG-UI-002 | Summary, cell table, drill-down by order/trade id, audit history | `test_ngweb_state.py::test_lookup_matches_exact_string_only`, `::test_cells_pagination_uses_opaque_string_cursor`, `::test_audit_pagination_and_no_secret_fields`, `test_ngweb_store_gateway.py::test_audit_events_from_store_paginate`; M: overview/cells/lookup/journal tabs | DONE |
| NG-UI-003 | Pause, Resume and Stop enqueue only; Stop may end STOP_UNCERTAIN; duplicate/concurrent Start never starts a second engine; responses reflect committed command state | `test_ngweb_commands.py::test_command_returns_committed_queue_row_not_assumed_outcome`, `::test_concurrent_start_different_keys_enqueues_exactly_one`, `::test_concurrent_same_key_returns_same_row`, `::test_start_while_engine_active_returns_engine_identity_not_second_engine[*]`, `::test_engine_awaiting_start_is_not_a_running_engine`, `test_ngweb_store_gateway.py::test_concurrent_start_through_store_single_queued_row` | DONE |
| §11 | Same committed snapshot as the CLI status; no secrets or auth token | `test_ngweb_launcher.py::test_attach_mode_serves_store_and_never_logs_token`, `test_ngweb_security.py::test_keystore_unlock_masked_and_secrets_never_leak` | DONE |

## Acceptance criteria

| AC | Criterion | Tests / checks | Status |
|---|---|---|---|
| AC-46 | Responsive Russian UI preview: grid, counts, baseline, range, caps, floors, advisory and validation before start | `test_ngweb_preview.py::test_sample_preview_counts_range_caps_floors` (56/55, 22/33, 40/15, slots 0/120/0, [−330,+220], gross 550/1000, 5x, notional 2970.0, margin 356.40, floors), `::test_signed_baseline_shifts_reachable_range[*]` ([0,550] for B=330), `::test_missing_baseline_blocks_first_start`, `::test_baseline_beyond_cap_and_reachable_beyond_cap`, `::test_validation_errors_are_shown_and_block_start`, `::test_unknown_rules_and_margin_block`, `::test_live_mode_requires_enabled_config`, `::test_venue_cap_limits_admission`, `::test_ui_is_russian_responsive_and_has_preview_fields`; M: preview tab, desktop and 375 px | DONE |
| AC-47 | Idempotent start/pause/resume/stop queue; refresh and concurrent Start create no duplicate engine or order | `test_ngweb_commands.py::*`, `test_ngweb_store_gateway.py::test_concurrent_start_through_store_single_queued_row`, `::test_commands_go_to_durable_store_queue_idempotently`, `::test_store_side_conflict_when_engine_revision_moved_before_snapshot`, `test_ngweb_demo_engine.py::test_demo_engine_full_operator_flow` (a duplicate Start against the running real engine gets 409; a new browser session sees the same engine). Single engine in the demo: one `EngineHost` per identity plus the store host lock and owner fencing via the engine's `open_engine` | DONE |
| AC-48 | Stale snapshot, DEGRADED, PAUSED and STOP_UNCERTAIN shown without optimistic NORMAL or STOPPED | `test_ngweb_state.py::test_stale_snapshot_is_never_shown_as_current[*]`, `::test_fresh_snapshot_state_is_shown_verbatim[DEGRADED/PAUSED/STOP_UNCERTAIN]`, `::test_stop_uncertain_reasons_and_errors_visible`, B: `test_ngweb_browser.py::test_browser_truthful_state_security_and_keyboard` (queued pause stays NORMAL until committed; STOP_UNCERTAIN; STALE with last known) | DONE |
| AC-49 | Default loopback; CSRF, Origin and session tests; secrets not returned, not logged, not in localStorage or argv | `test_ngweb_security.py::*`, `test_ngweb_launcher.py::test_secrets_are_never_accepted_in_argv[*]`, `::test_argparse_errors_do_not_echo_values`, `::test_launcher_source_has_no_secret_options`, B: storage and cookie checks in `test_browser_truthful_state_security_and_keyboard` | DONE |
| AC-50 | Offline fake-exchange browser flow: partial entry + TP, dust, history lag, pause/resume/stop; keyboard, contrast, desktop/mobile table | Real engine + FakeExchange + SQLite, via the API: `test_ngweb_demo_engine.py::test_demo_engine_full_operator_flow`. B, through the UI only: `test_ngweb_browser.py::test_browser_flow_on_offline_demo_engine`, `::test_browser_contrast_and_mobile_layout[light/dark]` (WCAG AA ≥ 4.5:1 on 16 UI text classes; no horizontal page scroll at 375 px on every tab; stacked cell cards), keyboard in `::test_browser_truthful_state_security_and_keyboard`. M: full demo run (below). | DONE |
| AC-22 (UI/API) | Trade/exchange ids above the safe JS range pass API/UI pagination and matching as strings | `test_ngweb_state.py::test_ids_beyond_js_safe_range_survive_as_exact_strings`, `::test_lookup_matches_exact_string_only` (a float-rounded neighbour does not match), `::test_cells_pagination_uses_opaque_string_cursor`, `::test_jsonsafe_rules`, `test_ngweb_commands.py::test_command_list_pagination_with_big_string_ids`, `test_ngweb_store_gateway.py::test_big_ids_from_store_snapshot_stay_exact`, B: DOM shows `9223372036854775813` exactly | DONE |

## Manual browser checks (Claude browser pane)

### A. Scratch dev server over `FakeGateway` (UI iteration)

| Check | Result |
|---|---|
| Login via `#auth=` fragment | Fragment removed from the URL; the cookie session survives reload. |
| Stale snapshot | Badge «Нет свежих данных · последнее: NORMAL» plus banner. |
| Preview tab | 56/55, 22/33, 40/15, prices coloured by initial side, config table. |
| Start dialog | B re-entry and both acknowledgements usable by keyboard (Tab/Space/Enter). A duplicate Start showed a generic message; now it shows the server's 409 reason. |
| Tabs by keyboard | ArrowRight, End and Home switch tabs; focus outline 3 px solid. |
| 375 px mobile | The cells tab overflowed (document width 442 px, caused by the state select). Fixed; now 375 px with stacked cards, and IDs show exactly as `9223372036854775813`. |

### B. Real demo: `python bin/lighter_robinhood_neutral_grid_web.py --demo-fake-exchange --port 8792`

| Step | Observed |
|---|---|
| Launch | Printed a loopback URL with `#auth=`; the token appears only in that printed line. |
| Before Start | BOOTSTRAPPING with AWAITING_START / BASELINE_NOT_CONFIRMED. |
| Start (keyboard dialog) | «Команда #1 «Старт»: применена»; engine r1; still BOOTSTRAPPING (honest). |
| Confirm baseline | APPLIED with anchor 5.4000, 22 BUY / 33 SELL; state NORMAL; P_min…P_max −200…200; gross 400; admission blocker SLOT_CAP (40 armed / 15 queued). |
| Partial entry ×2 | Cell 22 SELL: entry 10/6/4 still live, TP 6/0/6 live with GTT expiry (ENTRY_LIVE + TP_LIVE). IDs `E 1099511627777 / 9007199254741993`, `TP 1099511627817 / 9007199254742033`. |
| Pause | PAUSED with OPERATOR_PAUSE; position −6. |
| Resume | APPLIED; NORMAL. |
| Dust | Cell 21 BUY «пыль»: entry 10/2/8 terminal, dust 2, blocker `BELOW_MIN:2<5.0@5.4`, queue age 11 s; «Пыль всего» 2. |
| History lag on + fill | «Отставание истории» grows (4 s → 6 s); «Позиция сверена: нет»; later DEGRADED/HISTORY_STALE. |
| Stop with lost cancels | 409 twice while the engine revision churned (risk 6); third confirm queued. State STOP_UNCERTAIN, «Остановка не подтверждена», P_min/P_max stayed conservative (−208…189). |
| Page reload | Engine still STOP_UNCERTAIN: closing or reloading the browser stops nothing. |

Issues found and fixed during this run:
- a stale-response race in the cells, journal and lookup loads (request sequencing);
- result JSON overflowing the pending-command box;
- noisy opaque cursors in the history card (now abbreviated);
- the Start hint wrongly saying «уже работает» for an engine awaiting start;
- a next-step hint added (Start → confirm baseline).

## Cross-workstream API notes, mismatches and risks

1. **Audit commands.** Resolved after the engine merge (`7f15ab300`). The engine dropped its extra
   `manual_reconcile` kind: every operator audit is now `baseline_audit` with a payload `action` in
   `commands.AUDIT_ACTIONS`, and the store accepts only `contracts.CommandKind`. The web validates the same
   action list (`test_ngweb_commands.py::test_audit_actions_match_engine` asserts parity with the engine and
   `validate_kind`). A legacy `manual_reconcile` gets 400 `bad_kind`
   (`test_audit_actions_are_baseline_audit_commands`), and the store path is covered by
   `test_ngweb_store_gateway.py::test_audit_action_reaches_store_queue`.
2. **"Started" flag.** The engine snapshot now carries `summary.started`, and the web prefers it
   (`test_explicit_started_flag_wins_over_reasons`). Fallback when it is absent: the reason `AWAITING_START`
   means "not running" (`test_engine_awaiting_start_is_not_a_running_engine`).
3. **Baseline source.** The engine's CONFIRM_BASELINE requires the payload B to equal
   `config.expected_initial_position`. The web shows B from the config and requires the operator to re-type
   it for both Start and Confirm; a mismatch gets 422 before enqueue (`test_start_baseline_must_match_config`).
4. **Snapshot types.**
   - The engine serialises `lag_s`, `queue_age_s`, error `at` and `last_full_scan_at` as strings, and
     `cell_id` as int. The store header rewrites `committed_at` as a float.
   - `views.for_display` makes the time fields numbers for display only. `jsonsafe` renders `cell_id` and
     all ids as strings.
   - GTT `expiry` (ms string) additionally becomes `expiry_at` (seconds).
   - Test: `test_engine_shaped_snapshot_with_string_times_and_int_cell_ids`.
5. **Store listing API.** Resolved: WS-B indexed drill-down reads and keyset pages (see review fix #5).
6. **Revision churn vs. safety commands (risk for WS-D/integration).**
   - The engine bumps `engine_revision` on autonomous state changes, e.g. every DEGRADED ↔ NORMAL flip
     while history lags. Observed in the manual demo: Stop got 409 twice while the engine flapped
     r12 → r13 → r14.
   - The web behaves as the spec requires: the command is never auto-applied, the dialog re-opens with
     fresh revisions, and an immediate re-confirm queues it. But Stop/Pause can be hard to land during
     flapping.
   - Recommendation: bump `engine_revision` only on operator-relevant lifecycle/config changes, or accept
     Stop/Pause across purely autonomous transitions. This needs a spec-level decision, so the web does not
     relax it.
7. **Live hosting.** Live in-process hosting is not in WS-E: `LighterExchangePort` needs a running Hummingbot
   connector. Live deployments run the engine in the Hummingbot executor, and the web attaches with
   `--attach-db` + `--config`. Keystore unlock is implemented and tested as the credential boundary for a
   future in-process host.
8. **TP dispatch latency in the demo.** The demo's `summary.tp_dispatch` showed last/max ≈ 10.2 s against
   the 2 s SLO: the engine's metric, shown as is. This is WS-D's AC-14 scope; flagged for their review.
9. **Engine request-weight budget in the demo.** With default `EngineOptions.weight_budget_per_min=14400`,
   rapid demo actions can starve history scans. The engine then honestly shows DEGRADED/HISTORY_STALE. The
   demo keeps the realistic defaults.


## Review fixes (adversarial review of `bcd2eebef`, 8 confirmed issues)

Base: merged the integration tip `bb49f434e` into `codex/ng-web` (fast-forward). Each fix has a test that
failed before it.

| # | Issue | Fix | Tests |
|---|---|---|---|
| 1 | `confirm_baseline` accepted before any Start: bootstrap without the risk acknowledgement or preview check | `CommandService` answers 409 `start_required` unless the committed snapshot shows `engine_started` (explicit `summary.started`, or no `AWAITING_START`). The UI disables «Подтвердить baseline» until then. | `test_ngweb_commands.py::test_confirm_baseline_refused_before_start[False/None]` |
| 2 | Attach mode loaded config from a separate YAML: the shipped controller YAML crashed; a hand-written file could drift; unknown keys were defaulted | `--config` removed. Preview config, B check and engine identity come from the committed `summary.engine_config`. Every GridConfig field is required; unknown keys, float decimals and wrong types are errors; `fingerprint` must equal core `grid.config_fingerprint`. If the config is unavailable, the preview shows an error and Start is refused (422). | `test_ngweb_attach.py::test_attach_preview_identity_and_baseline_come_from_engine_config`, `::test_attach_engine_config_is_strict[*]` (missing config, missing key, unknown key, fingerprint mismatch, float decimal), `::test_engine_config_parser_direct`, `test_ngweb_launcher.py::test_config_option_removed_attach_reads_engine_config`, `::test_attach_mode_serves_store_and_never_logs_token` |
| 3 | A persistence failure was never visible in the UI: a failing store cannot commit a snapshot | New uncommitted health channel: demo/in-process via `EngineHost.health()` (engine `persistence_error` and `fatal_reason`, also in `host.status()`); attach via `<db>.health.json`. `/api/state.health` carries a banner marked «не зафиксировано». A stale snapshot with a missing or unreadable file shows «состояние хранилища неизвестно». | `test_ngweb_health.py::*`, B: `test_ngweb_browser.py::test_browser_truthful_state_security_and_keyboard` (banner rendered) |
| 4 | Attach preview hard-coded limit/post-only support, showed the snapshot time as the rules time, and had no freshness gate | `snapshot_market` reads `runtime_rules.supports_limit`, `.supports_post_only` and `.fetched_at`. Unknown flags, rules older than `history_freshness_s` or a stale snapshot are preview errors, so Start is disabled. | `test_ngweb_attach.py::test_attach_market_rules_flags_and_freshness[*]`, `::test_attach_rules_fetched_at_is_the_rules_time_not_snapshot_time` |
| 5 | ID lookup and journal paging silently stopped at the newest 5000 rows | Closed with WS-B `codex/ng-store@3d0a4d162` (merged). All scan windows are removed; the gateway uses only the indexed store reads `find_orders_by_id` (with `matched_on`), `find_fills_by_trade_id` plus fills of matched orders, and keyset `commands_page`/`audit_page(before_id, limit≤1000)`. Ids are passed as exact `str`. `truncated` stays in the API contract (always false) and the UI warning is kept for it. | `test_ngweb_drilldown_store.py::test_old_big_ids_found_through_indexed_store_reads` (real store: the oldest order behind newer legs is found by 2^63+5 exchange id, 2^64+1 trade id and CID; a float-rounded near miss matches nothing; failed before), `::test_journal_pages_reach_the_true_end`, `test_ngweb_truncation.py::test_gateway_calls_indexed_reads_with_exact_types` |
| 6 | Store-parsed Decimal times were rendered as «—» | `views.for_display` converts Decimal and numeric-string time fields; tested on a real store snapshot. | `test_ngweb_store_gateway.py::test_real_store_snapshot_times_render_as_numbers` |
| 7 | The browser test raced its 5 s staleness window against cold Chrome start | `FakeGateway.live_clock` commits fresh snapshots like a running engine; a deliberate 6 s delay reproduces the old failure and now passes. | B: `test_browser_truthful_state_security_and_keyboard`, `test_browser_contrast_and_mobile_layout[*]` |
| 8 | The lag assertions passed without any injected lag | API e2e requires lag ≥ 10 s, the target entry's fill not credited during the lag, and credit of exactly the lagged quantity afterwards (the demo action now returns the filled CID and quantity). The browser check requires ≥ 10 s. Mutation check: with `history_lag_on` set to 1 s the API test fails ("timeout waiting for history lag >= 10 s"). | `test_ngweb_demo_engine.py::test_demo_engine_full_operator_flow`, B: `test_browser_flow_on_offline_demo_engine` |

Contract names coded ahead of WS-D/WS-B publication (tests use snapshots carrying them):
- `summary.engine_config` (all GridConfig fields + `fingerprint`);
- `summary.runtime_rules.supports_limit`, `.supports_post_only` and `.fetched_at`;
- `<db>.health.json` = `{persistence_error, fatal_reason, at, engine_revision}`;
- store indexed reads listed in #5: done, WS-B merged.

Until the engine publishes `engine_config` and the rules flags, attach-mode preview shows an explicit error and
Start is refused (fail-closed). The rules-freshness conflict is resolved by decision (b): the gate now uses the engine-published
`runtime_rules.max_age_s`.


## Orchestrator decisions (b), (c), (d)

| Decision | Implementation | Tests |
|---|---|---|
| (b) Rules freshness uses the engine-published `summary.runtime_rules.max_age_s`, not `history_freshness_s` | `snapshot_market` fails closed when `max_age_s` is absent or non-positive, or when `now - fetched_at > max_age_s`. History freshness stays separate: snapshot staleness and lag. | `test_ngweb_attach.py::test_attach_market_rules_flags_and_freshness[*]` (older than max age; missing max age), `::test_rules_freshness_uses_engine_max_age_not_history_freshness` (60 s old rules are fresh under 180 s even though history freshness is 10 s; failed before) |
| (c) Strict 409 for all four commands; operator-driven one-click re-issue, never automatic | After a 409 the command dialog stays open with the fresh state, uses the revisions from the 409 body, and keeps every operator input (reason/B/confirm/action/observed/note/ack/cid) plus a new key. Nothing is re-sent until the operator clicks. Start keeps B and the acknowledgements only when the preview `material_id` (config + rules, no revisions) is unchanged; otherwise the risk acknowledgement must be redone. | B: `test_ngweb_browser.py::test_browser_409_is_reissued_by_one_operator_click_never_automatically` (Stop: reason kept, nothing sent for 3 s, one click → expected_engine_revision=2; Start revision-only → one click; Start with changed rules → acknowledgements reset, submit disabled; failed before) |
| (d) WS-B indexed reads | Merged `codex/ng-store@3d0a4d162` (`381192b04`); gateway switched in `08bf75dda` (see review fix #5). | `test_ngweb_drilldown_store.py::test_more_than_5000_rows_old_trade_and_oldest_journal_rows_reachable` (6000 newer fills, commands and audit rows; the oldest trade found by its 2^64+1 id; commands and audit paged through the web API down to id 1 with `truncated=false`), `::test_old_big_ids_found_through_indexed_store_reads`, `::test_journal_pages_reach_the_true_end` |


## Single-run gate hygiene

Tests no longer import from `conftest`: `ACCESS_TOKEN` now lives in the uniquely named helper `ngweb_fakes.py`.
Before this, a combined pytest run resolved `conftest` to `engine/conftest.py` and hit two collection errors.
Now `pytest --collect-only test/hummingbot/strategy_v2/executors/neutral_grid_executor test/web/neutral_grid`
collects 655 tests with 0 errors, and the combined run gives 655 passed, 230 subtests passed.


## Round 3 (integration `203e31370`, merged fast-forward)

| Item | Fix | Tests (red → green) |
|---|---|---|
| C1 secret leak, defence in depth | `security.redact_free_text` strips URL query strings, secret key/values (`auth=`, `token`, `Authorization`, `Bearer`, `api-key`, `signature`/`sig`, `password`, `session`, …), hex blobs of ≥ 40 characters and opaque mixed blobs of ≥ 32 characters. `redact_tree` applies it recursively to every engine/operator free-text key (errors, reasons, blockers, freezes, persistence/fatal errors, incomplete reasons, warnings, audit details, command results, notes, banners). It runs at the single JSON output point (`server._json`, `json_error`). Ids, cursors, decimals and fingerprints are outside free-text keys and stay exact. | `test_ngweb_round3.py::test_c1_engine_free_text_never_leaks_auth_tokens` (state/cells/audit/commands/command/lookup), `::test_c1_redaction_rules`, B: `test_ngweb_browser.py::test_browser_round3_redaction_and_extended_audits` (DOM) |
| E-03 outdated sidecar | A health sidecar whose `at` is older than the snapshot staleness bound counts as unknown. Next to STALE the UI shows «состояние хранилища неизвестно (файл здоровья движка устарел N с)», never "healthy". An outdated file that still carries an error keeps showing it, marked as outdated. | `test_ngweb_round3.py::test_e03_outdated_healthy_sidecar_is_unknown_next_to_stale` |
| E-09 START material binding | The web adds the acknowledged preview `material_id` (config + rules, without revisions) to the Start payload at enqueue. `confirm_baseline` gets 409 `start_material_changed` when the latest APPLIED Start's `material_id` differs from the current preview material (attach: from `engine_config` / `runtime_rules`). Starts not issued through the web carry no material and are bound by the engine (WS-D). | `test_ngweb_round3.py::test_e09_confirm_baseline_refused_when_material_changed_since_start` |
| D2-17 extended audits | `retire_colliding_cid` and `migrate_grid` are sent as `baseline_audit` actions (parity with engine `EXTENDED_AUDIT_ACTIONS` and `validate_kind`). Each requires a note, `acknowledge=true` and an exact typed phrase: «СПИСАТЬ CID <cid>» or «МИГРАЦИЯ СЕТКИ <grid_id>». They carry an idempotency key and expected revisions, and the committed result is shown. Server-side gating: retire only while `freezes.CID_ALLOCATION` is present, and the CID must equal the published `colliding_cid`; migrate only when `summary.grid_mutation_blockers` is a published empty list. Otherwise 409 `audit_not_applicable`, fail-closed. The UI offers the options only under the same conditions and pre-fills the colliding CID. | `test_ngweb_round3.py::test_d2_17_retire_colliding_cid_only_when_frozen_with_explicit_confirmation`, `::test_d2_17_migrate_grid_only_when_quiescent_with_explicit_confirmation`, `::test_d2_17_extended_actions_match_engine`, B: `test_browser_round3_redaction_and_extended_audits` |

Fields the web reads that the engine snapshot does not publish yet (WS-D):
- `summary.colliding_cid`. Without it, retire needs a typed CID while the CID freeze is present.
- `summary.grid_mutation_blockers`. Without it, migrate stays unavailable (fail-closed).


## Round 4: M1, audit bound to the reviewed conflict set (AC-40, NG-UI-002)

| Item | Fix | Tests (red → green) |
|---|---|---|
| Rendering | Overview card «Конфликты истории»: for each key, every version side by side (fingerprint, committed / not committed, all summary fields as exact strings), plus the `conflict_set_id`. The drill-down (`/api/lookup`) matches keys, fingerprints and summary values and renders the same table. | `test_ngweb_conflicts.py::test_conflict_set_rendered_exactly_and_in_drilldown` (int id beyond 2^53 served as an exact string) |
| Bound ack | `ack_history_conflict` requires `conflict_set_id` (captured when the dialog opens from the snapshot on screen), `accepted` {key → fingerprint}, a note, `acknowledge` and the typed phrase «ПРИНЯТЬ НАБОР <set id>». The server checks: the set id equals the published `summary.conflict_set_id` (409 `conflict_set_changed` with the fresh set; unpublished → 409 `conflict_set_unavailable`); every key without a committed version has an explicit pick; picks name only shown versions of shown keys (422 otherwise). The UI shows radios only where no version is committed. | `::test_ack_payload_carries_set_id_and_accepted_versions`, `::test_ack_payload_validation[*]`, `::test_changed_set_is_409_with_fresh_set_never_enqueued`, `::test_engine_accepts_the_web_payload_shape` |
| Changed set | On a 409 the dialog re-opens with the fresh set; picks and the phrase naming the old set are dropped, and a new operator click is required (nothing auto-resent). An engine-side REJECTED `CONFLICT_SET_CHANGED` is shown as «аудит НЕ выполнен». | B: `test_ngweb_browser.py::test_browser_history_conflict_ack_bound_to_viewed_set` |

Coded against WS-D's announced fields `summary.history_conflicts` and `summary.conflict_set_id`. They are not in `codex/ng-engine` yet (checked `5f266ca83`), so until they land the web refuses the ack (409, fail-closed).


## Final M1 contract (engine `codex/ng-engine@b36e95497`, merged `d82f5992e`)

- R7 (WS-D): the web passes `conflict_set_id` and `accepted` through. `_normalize_only` keeps them (since
  `6fdf45d76`), together with `note`, `acknowledge` and `confirmation`; the engine ignores the extra keys.
  Proven by the end-to-end test below.
- Rendering: all six streams (`trades`, `inactive_orders`, `store_conflict`, `manual_reconcile`, `freeze`,
  `active_evidence`) get Russian labels plus `cell_id`, in the overview card, the ack dialog and the drill-down.
- Choices match the engine:
  - radios appear only where no version is committed;
  - a committed key may only repeat its committed version; another version gets 422, like the engine's
    `LEDGER_CORRECTION_NOT_SUPPORTED`;
  - validation iterates the list, because keys can repeat across streams.
- Engine error codes are shown as Russian messages, with per-key sub-errors: `CONFLICT_SET_ID_REQUIRED`,
  `CONFLICT_SET_CHANGED`, `NOTHING_TO_AUDIT`, `ACCEPTED_CHOICE_REQUIRED`, `ACCEPTED_INVALID` (`NOT_IN_CONFLICT_SET`,
  `NOT_A_SEEN_VERSION`, `LEDGER_CORRECTION_NOT_SUPPORTED`).
- Bug found by the end-to-end test: the C1 redaction masked the engine result's 32-hex `conflict_set_id`, and would
  have masked fingerprints the same way. Identifier keys (`*_id`, `fingerprint`, `accepted`, `noise`, `key`, `cid`, …)
  now stay exact inside free-text containers; free text around them is still redacted.
  Test: `test_ngweb_round3.py::test_c1_redaction_keeps_ids_and_digests_inside_results`.

| Tests | |
|---|---|
| `test_ngweb_conflicts.py::test_committed_key_may_only_repeat_its_committed_version`, `::test_ack_payload_validation[*]` (incl. ledger correction), `::test_ui_maps_engine_conflict_errors_and_stream_labels`, `::test_conflict_set_rendered_exactly_and_in_drilldown` (six streams, `cell_id`) | contract parity |
| `test_ngweb_demo_engine.py::test_demo_engine_history_conflict_ack_through_web_is_applied` | Real engine + FakeExchange: a persistent conflicting duplicate trade publishes a `trades` conflict with sizes {3, 1}. A web ack for an unseen set gets 409 and is never enqueued. The web ack of the published set is APPLIED, and its result's `conflict_set_id` equals the acknowledged one. |

Verified against `codex/ng-engine@19dc443d6` (merged as `620da6e8f`). The engine's canonical opaque keys
(`trade:…`, `order:…`, `<kind>:sha:<32hex>`, `store_conflict:<id>`, `manual_reconcile:reason`,
`freeze:LEDGER_INVARIANT`, `active_evidence:<cell>`), its 32-hex fingerprints and its 32-hex `conflict_set_id`
pass the web's strict validation unchanged. Fixtures use exactly these forms and the lowercase stream values. The
end-to-end demo test also asserts the canonical forms on the real engine's snapshot before the web ack is APPLIED.
