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
| AC-47 | Idempotent start/pause/resume/stop queue; refresh and concurrent Start create no duplicate engine or order | `test_ngweb_commands.py::*`, `test_ngweb_store_gateway.py::test_concurrent_start_through_store_single_queued_row`, `::test_commands_go_to_durable_store_queue_idempotently`, `::test_store_side_conflict_when_engine_revision_moved_before_snapshot`; demo single host: `web/neutral_grid/host.py` (`EngineAlreadyHosted`) plus the store host lock | DONE (see ENGINE rows below) |
| AC-48 | Stale snapshot, DEGRADED, PAUSED and STOP_UNCERTAIN shown without optimistic NORMAL or STOPPED | `test_ngweb_state.py::test_stale_snapshot_is_never_shown_as_current[*]`, `::test_fresh_snapshot_state_is_shown_verbatim[DEGRADED/PAUSED/STOP_UNCERTAIN]`, `::test_stop_uncertain_reasons_and_errors_visible`, B: `test_ngweb_browser.py::test_browser_truthful_state_security_and_keyboard` (queued pause stays NORMAL until committed; STOP_UNCERTAIN; STALE with last known) | DONE |
| AC-49 | Default loopback; CSRF, Origin and session tests; secrets not returned, not logged, not in localStorage or argv | `test_ngweb_security.py::*`, `test_ngweb_launcher.py::test_secrets_are_never_accepted_in_argv[*]`, `::test_argparse_errors_do_not_echo_values`, `::test_launcher_source_has_no_secret_options`, B: storage and cookie checks in `test_browser_truthful_state_security_and_keyboard` | DONE |
| AC-50 | Offline fake-exchange browser flow: partial entry + TP, dust, history lag, pause/resume/stop; keyboard, contrast, desktop/mobile table | B: `test_ngweb_browser.py::test_browser_contrast_and_mobile_layout[light/dark]` (WCAG AA ≥ 4.5:1 on 16 UI text classes; no horizontal page scroll at 375 px on all tabs; stacked cell cards), keyboard in `test_browser_truthful_state_security_and_keyboard`; engine flow: see ENGINE rows | PARTIAL until the WS-D engine is merged |
| AC-22 (UI/API) | Trade/exchange ids above the safe JS range pass API/UI pagination and matching as strings | `test_ngweb_state.py::test_ids_beyond_js_safe_range_survive_as_exact_strings`, `::test_lookup_matches_exact_string_only` (a float-rounded neighbour does not match), `::test_cells_pagination_uses_opaque_string_cursor`, `::test_jsonsafe_rules`, `test_ngweb_commands.py::test_command_list_pagination_with_big_string_ids`, `test_ngweb_store_gateway.py::test_big_ids_from_store_snapshot_stay_exact`, B: DOM shows `9223372036854775813` exactly | DONE |

## Manual browser checks (Claude browser pane)

Server: scratch dev server over `FakeGateway`, used for UI iteration.

| Check | Result |
|---|---|
| Login via `#auth=` fragment | Fragment removed from the URL; the cookie session survives reload. |
| Header | Mode chip, state badge, snapshot age and revisions shown. |
| Stale snapshot | Badge "Нет свежих данных · последнее: NORMAL" plus banner. |
| Preview tab | 56/55, 22/33, 40/15, prices coloured by initial side, config table. |
| Start dialog | B re-entry and both acknowledgements usable by keyboard (Tab/Space/Enter). A duplicate Start shows the server's 409 reason (fixed: it previously showed a generic "preview changed"). |
| Tabs by keyboard | ArrowRight, End and Home switch tabs; focus outline 3 px solid. |
| 375 px mobile | The cells tab overflowed (document width 442 px, caused by the state select). Fixed in `app.css`; now 375 px, cells render as stacked cards, and IDs show exactly as `9223372036854775813`. |

## Cross-workstream API notes and mismatches

1. **Command kind `manual_reconcile` (engine) vs store.** `store.enqueue_command` accepts only
   `contracts.CommandKind`, but the engine has an extra kind `manual_reconcile`. The web validates the payload
   (action whitelist equals the engine's `MANUAL_RECONCILE_ACTIONS`, checked by
   `test_manual_reconcile_actions_match_engine_when_available`). When the gateway's `supported_kinds` does
   not include the kind, the web returns 422 `unsupported_kind` (`test_manual_reconcile_kind_rejected_when_store_does_not_accept_it`).
   Integration must add the kind to the store or contracts additively.
2. **"Started" flag.** Before START the engine commits `engine_state=BOOTSTRAPPING` with reason
   `AWAITING_START`. The web treats that as "not running". It prefers an explicit `summary.started`
   boolean when present (`test_explicit_started_flag_wins_over_reasons`). Suggested additive field for WS-D.
3. **Baseline source.** The engine's CONFIRM_BASELINE requires the payload B to equal
   `config.expected_initial_position`. The web therefore shows B from the config and requires the operator to
   re-type it; a mismatch gets 422 before enqueue.
4. **Snapshot types.**
   - The engine serialises `committed_at`, `lag_s`, `queue_age_s` and error `at` as strings, and `cell_id`
     as int. The store header overrides `committed_at` with a float.
   - `views.for_display` converts time fields to numbers for display. `jsonsafe` turns `cell_id` and every
     id into strings.
   - GTT `expiry` (ms string) additionally becomes `expiry_at` (seconds).
5. **Store command listing and audit** have no `before` cursor. `StoreGateway` pages by filtering
   `id < before` over a bounded window (`_SCAN_LIMIT=5000`). Lookups by exchange/trade id scan `legs()`,
   `order()`, `fills()` and `unmatched_evidence()` within the same bound.
6. **Live hosting.** Live in-process hosting is not in WS-E: `LighterExchangePort` needs a running
   Hummingbot connector. Live deployments run the engine in the Hummingbot executor, and the web attaches
   with `--attach-db`. Keystore unlock is implemented and tested as the credential boundary for a future
   in-process host.
