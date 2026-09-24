# WS-D engine — traceability, decisions, requests

Branch `codex/ng-engine` (worktree `hummingbot-ng-engine`), created from `a18cb37d4` (contracts commit).
Owned: `neutral_grid_executor/{__init__,engine,executor,fake_exchange,commands,snapshot,data_types}.py`,
`controllers/generic/neutral_grid.py`, `scripts/lighter_robinhood_fixed_neutral_grid.py`,
`conf/controllers/lighter_robinhood_fixed_neutral_grid.yml.example`,
`conf/scripts/lighter_robinhood_fixed_neutral_grid.yml.example`,
`test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/**`, `test/controllers/generic/test_neutral_grid.py`.

Test path prefixes used below: `E = test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/`,
`CTL = test/controllers/generic/test_neutral_grid.py`. Every engine test drives the real `NeutralGridEngine`
through `FakeExchange` + the real on-disk WS-B SQLite store + the real WS-C `HistoryScanner` + the WS-A ledger/
risk/admission/router (`E ng_engine_harness.py`); nothing restates the implementation with mocks.

## Architecture (what the engine is)

* One `NeutralGridEngine` per account/market/grid, one asyncio task (the executor control loop). Tick order:
  commands → account reads (weight-budgeted) → one bounded resumable scanner step → history batch (inbox +
  dedupe + attribution + leg transitions + cursor in ONE store transaction) → active-list evidence → settlement
  (`history.evaluate_terminal_release`) + dust + release → drift → honest state → admission + router → dispatch
  (intent+CID+reservation commit → `mark_dispatching` commit → transport → result commit) → stop outcome →
  snapshot commit.
* Durable truth is the WS-B store; WS-A `CellLedger` objects are an in-memory projection rebuilt from the store on
  load (`_rebuild_ledgers`) and mirrored from store rows after every write (`_mirror`). Engine-only durable
  facts (terminal row + first-seen time, cancel bookkeeping, obligation clocks, pause/stop flags, acknowledged
  scan conflicts) live in `engine_kv` (`om:<cid>`, `engine_meta`), no floats.
* `FakeExchange` (package module, used by WS-E's offline demo) implements `contracts.ExchangePort`.
* `NeutralGridExecutor` (ExecutorBase) hosts the engine; `controllers/generic/neutral_grid.py` creates exactly
  one; `scripts/lighter_robinhood_fixed_neutral_grid.py` is the thin Robinhood adapter.

## Requirements → tests

| Requirement | Component | Tests (pytest node ids) | Status |
|---|---|---|---|
| NG-GRID-001/002 fixed integer-tick grid, anchor, first side, no recenter | engine `_cmd_confirm_baseline` (A `grid.*`, B `bootstrap`) | `E test_ng_engine_risk.py::test_ac03_sample_grid_55_cells_22_buy_33_sell_and_fixed_prices`; `E test_ng_engine_risk.py::test_ac03_invalid_config_is_rejected_at_confirmation`; `E test_ng_engine_crash.py::test_ac20_restart_after_price_bounce_restores_from_db_and_history` | done |
| NG-GRID-003 exact Q, never round up, dust | engine TP planning (A `tp_obligation_to_dispatch`) | `E test_ng_engine_cycles.py::test_ac23_quantities_never_rounded_up_and_fees_do_not_touch_base`; `E test_ng_engine_cycles.py::test_ac33_dust_is_visible_durable_and_blocks_reset` | done |
| NG-ORD-001 post-only entry, LIMIT GTT TP, no MARKET | engine `_submit`, `_pre_send_blocker` | `E test_ng_engine_cycles.py::test_ac01_normal_buy_cycle_rearms_buy`; `E test_ng_engine_risk.py::test_ac38_post_only_rejection_and_blockers_never_fall_back_to_market`; `E test_ng_engine_cycles.py::test_ac51_gtt_renewal_only_after_terminal_with_exact_remainder` | done |
| NG-ORD-002 virtual cells, `reduce_only=False` | engine `_submit` | `E test_ng_engine_cycles.py::test_ac27_virtual_tp_crosses_zero_without_reduce_only`; `E test_ng_engine_properties.py::test_invariants_hold_under_random_fill_cancel_restart_orderings` | done |
| NG-ORD-003 self-trade router, TP priority, TP–TP FIFO | engine `_act` (A `router.plan_submits`) | `E test_ng_engine_risk.py::test_ac31_self_trade_conflict_cancels_entry_and_tp_waits_for_terminal`; `E test_ng_engine_risk.py::test_ac44_tp_tp_conflict_is_fifo_without_netting` | done |
| NG-CELL-001 whole-cell lock, release (1)–(5) | engine `_settle` (A `can_release`, B `close_cycle`), `position_reconciled` | `E test_ng_engine_cycles.py::test_ac01_normal_buy_cycle_rearms_buy`; `E test_ng_engine_properties.py::test_invariants_hold_under_random_fill_cancel_restart_orderings` | done |
| NG-CELL-002 partial entry, TP per confirmed share, ENTRY_LIVE+TP_LIVE, ≤2 s dispatch | engine `_tp_candidates`, `_submit`, SLO clock | `E test_ng_engine_cycles.py::test_ac05_partial_entry_2_3_5_with_floor_5`; `E test_ng_engine_history.py::test_ac14_tp_dispatch_within_slo_and_blocker_queue_age_visible` | done |
| NG-CELL-003 partial TP, terminal partial entry, late fill after cancel | engine | `E test_ng_engine_cycles.py::test_ac06_partial_tp_does_not_unlock_or_repost`; `::test_ac07_terminal_partial_entry_closes_actual_then_full_cycle`; `::test_ac08_late_fill_after_cancel_event_extends_obligation` | done |
| NG-CELL-004 distinct states, concurrent legs | snapshot `state_flags` (A) | `E test_ng_engine_ops.py::test_committed_snapshot_schema_and_cli_status_without_secrets` | done |
| NG-HIST-001 history authoritative, WS only a hint | engine `wake`, executor wakeups | `E test_ng_engine_history.py::test_ac09_ws_duplicates_and_reorder_never_double_fills_or_prove_terminal`; `E test_ng_engine_history.py::test_ac13_history_lag_wakes_poller_tp_waits_and_lag_visible`; `CTL::test_executor_wires_live_history_wakeups_as_hints_only` | done |
| NG-HIST-002 pagination, overlap, dedupe, settlement, late evidence, cursor atomicity | engine `_apply_history`, `_settle`, `_apply_late_evidence` | AC-10/11/12/19/41/42 rows | done |
| NG-HIST-003 weights, coalescing, bounded startup work | engine weight ledger, scanner cadence | `E test_ng_engine_risk.py::test_ac04_55_cells_one_task_no_polling_storm` | done |
| NG-HIST-004 CID before side effect, no new CID after timeout/crash | engine `_submit`, `_resume_pending_intents` | AC-16/17/43 rows | done |
| NG-RISK-001 baseline once, stable cut, never recaptured | engine `bootstrap_ready`, `_cmd_confirm_baseline` | AC-28/45 rows | done |
| NG-RISK-002 P/P_min/P_max, gross, caps, TP headroom priority | engine (A `risk`, router `owed`) | AC-24/25/26 rows; `E test_ng_engine_properties.py::test_invariants_hold_under_random_fill_cancel_restart_orderings` | done |
| NG-RISK-003 manual orders/trades | engine `_reconcile_active`, store unmatched | AC-29/30 rows | done |
| NG-RISK-004 margin/unknown data | engine `_evaluate_state` (A `exposure_blockers`) | AC-37 row | done |
| NG-RISK-005 admission, slots, queue, runtime floors | engine `_slot_admission`, venue-cap guard | AC-34/44/57 rows | done |
| NG-DB-001..005 durable ledger, intent before side effect, restart, persistence loss, definitive reject | engine + B store | AC-15..21, AC-54/55/56 rows; `E test_ng_engine_crash.py::test_crash_at_every_persistence_window_restarts_consistently` | done |
| NG-OPS-001 pause | engine commands | `E test_ng_engine_ops.py::test_ng_ops_001_pause_stops_entries_keeps_tps_and_resume_passes_gate`; `::test_resume_is_rejected_while_blocked` | done |
| NG-OPS-002 outside bounds | engine `_act` | `E test_ng_engine_risk.py::test_ac32_outside_bounds_cancels_entries_keeps_tp_no_recenter` | done |
| NG-OPS-003 stop | engine `_finish_stop`, executor `early_stop`, script `on_stop` | AC-35/36 rows; `E test_ng_engine_ops.py::test_stop_without_inventory_is_stopped` | done |
| NG-ARCH-001/003 one executor, controller, launcher, disabled example, credential validator | controller, executor, script, conf examples | `CTL::test_controller_creates_exactly_one_executor_and_never_recreates`; `CTL::test_example_config_is_disabled_and_matches_the_spec_profile`; `CTL::test_config_rejects_unsafe_values`; `CTL::test_profile_and_confirmation_policy`; `CTL::test_api_key_validator_is_the_existing_80_hex_rule`; `CTL::test_executor_hosts_the_engine_bootstraps_trades_and_stops_without_flattening`; `CTL::test_executor_refuses_disabled_config`; `CTL::test_executor_fails_closed_when_the_store_refuses`; `CTL::test_launcher_baseline_mismatch_is_refused_once_not_spammed`; `CTL::test_disabled_controller_creates_nothing_and_says_so`; `CTL::test_register_executor_type_is_idempotent` | done (see request R1) |
| NG-UI-001/003 (engine side) command queue, idempotency, revisions, single start | engine `_process_commands` (B command table) | `E test_ng_engine_ops.py::test_command_idempotency_revision_conflict_and_single_start`; `::test_enabled_false_refuses_live_start` | done (HTTP/UI: WS-E) |
| NG-UI-002 / §11 honest states, committed snapshot = CLI status, no secrets | `snapshot.build_snapshot`, `format_status` | `E test_ng_engine_ops.py::test_committed_snapshot_schema_and_cli_status_without_secrets`; `::test_status_states_are_honest_during_bootstrap_and_reconcile`; `::test_fail_closed_engine_reports_degraded_not_normal` | done |
| §13 fake exchange | `fake_exchange.FakeExchange` | `E test_ng_engine_fake_exchange.py::*` (9 tests) | done |
| §13.3 property tests | engine end to end | `E test_ng_engine_properties.py::test_invariants_hold_under_random_fill_cancel_restart_orderings` (12 seeds × 70 steps default; 60 × 120 run manually, see below); `E test_ng_engine_properties.py::test_ledger_replay_is_idempotent_across_restarts` | done |

### Acceptance criteria (engine-level integration; AC-46..50 are WS-E)

| AC | Engine-level test(s) | Other WS unit coverage |
|---|---|---|
| AC-01 | `E test_ng_engine_cycles.py::test_ac01_normal_buy_cycle_rearms_buy` | A `core/test_cells.py` |
| AC-02 | `E test_ng_engine_cycles.py::test_ac02_normal_sell_cycle_rearms_sell` | A |
| AC-03 | `E test_ng_engine_risk.py::test_ac03_sample_grid_55_cells_22_buy_33_sell_and_fixed_prices`; `E test_ng_engine_risk.py::test_ac03_invalid_config_is_rejected_at_confirmation` | A `core/test_grid.py` |
| AC-04 | `E test_ng_engine_risk.py::test_ac04_55_cells_one_task_no_polling_storm`; `CTL::test_controller_creates_exactly_one_executor_and_never_recreates` | — |
| AC-05 | `E test_ng_engine_cycles.py::test_ac05_partial_entry_2_3_5_with_floor_5` | A, B |
| AC-06 | `E test_ng_engine_cycles.py::test_ac06_partial_tp_does_not_unlock_or_repost` | A |
| AC-07 | `E test_ng_engine_cycles.py::test_ac07_terminal_partial_entry_closes_actual_then_full_cycle` | A, B |
| AC-08 | `E test_ng_engine_cycles.py::test_ac08_late_fill_after_cancel_event_extends_obligation` | A |
| AC-09 | `E test_ng_engine_history.py::test_ac09_ws_duplicates_and_reorder_never_double_fills_or_prove_terminal` | C `history/test_ng_history_scanner.py`, B |
| AC-10 | `E test_ng_engine_history.py::test_ac10_target_terminal_row_on_a_later_inactive_page_is_found` | C |
| AC-11 | `E test_ng_engine_history.py::test_ac11_more_than_100_trades_exact_cumulative_with_boundary_duplicates` | C, B |
| AC-12 | `E test_ng_engine_history.py::test_ac12_bad_pagination_makes_history_incomplete_and_blocks_entries` (repeat/malformed cursor, page error); `E test_ng_engine_history.py::test_ac12_ac40_conflicting_duplicate_freezes_exposure` | C |
| AC-13 | `E test_ng_engine_history.py::test_ac13_history_lag_wakes_poller_tp_waits_and_lag_visible` | C (wakeups) |
| AC-14 | `E test_ng_engine_history.py::test_ac14_tp_dispatch_within_slo_and_blocker_queue_age_visible` | — |
| AC-15 | `E test_ng_engine_crash.py::test_ac15_crash_before_intent_commit_leaves_no_phantom` | B |
| AC-16 | `E test_ng_engine_crash.py::test_ac16_crash_after_intent_before_api_reuses_the_saved_cid`; `E test_ng_engine_crash.py::test_ac16_crash_after_dispatch_commit_is_unknown_not_proven_absent` | B, A |
| AC-17 | `E test_ng_engine_crash.py::test_ac17_timeout_keeps_unknown_until_history_resolves` (landed / not landed) | B |
| AC-18 | `E test_ng_engine_crash.py::test_ac18_crash_after_api_before_result_commit_exactly_once_ledger` | B |
| AC-19 | `E test_ng_engine_crash.py::test_ac19_crash_around_cursor_commit_is_idempotent` (mid batch / before / after cursor commit) | B |
| AC-20 | `E test_ng_engine_crash.py::test_ac20_restart_after_price_bounce_restores_from_db_and_history` | — |
| AC-21 | `E test_ng_engine_crash.py::test_ac21_cancel_timeout_keeps_full_remainder_reserved_and_cell_locked` | A, B |
| AC-22 | `E test_ng_engine_history.py::test_ac22_ids_beyond_float_range_stay_exact_strings` | B (DB), C (connector), A (CID) |
| AC-23 | `E test_ng_engine_cycles.py::test_ac23_quantities_never_rounded_up_and_fees_do_not_touch_base` | A |
| AC-24 | `E test_ng_engine_risk.py::test_ac24_net_long_cap_rejects_pending_buy` | A `core/test_risk.py` |
| AC-25 | `E test_ng_engine_risk.py::test_ac25_net_short_cap_rejects_pending_sell_shorts_within_cap_work` | A |
| AC-26 | `E test_ng_engine_risk.py::test_ac26_gross_cap_binds_even_when_net_is_flat` | A |
| AC-27 | `E test_ng_engine_cycles.py::test_ac27_virtual_tp_crosses_zero_without_reduce_only` | A |
| AC-28 | `E test_ng_engine_risk.py::test_ac28_signed_baseline_accepted_without_seed_or_flatten` (B = 330 / 0 / −200) | A, B |
| AC-29 | `E test_ng_engine_risk.py::test_ac29_unknown_active_order_blocks_start_and_is_never_touched`; `E test_ng_engine_risk.py::test_ac29_unknown_active_order_at_restart_blocks_everything` | — |
| AC-30 | `E test_ng_engine_risk.py::test_ac30_manual_trade_freezes_entries_keeps_tp_and_needs_audit` | A (risk part), B (audit) |
| AC-31 | `E test_ng_engine_risk.py::test_ac31_self_trade_conflict_cancels_entry_and_tp_waits_for_terminal` | A `core/test_router.py` |
| AC-32 | `E test_ng_engine_risk.py::test_ac32_outside_bounds_cancels_entries_keeps_tp_no_recenter` | — |
| AC-33 | `E test_ng_engine_cycles.py::test_ac33_dust_is_visible_durable_and_blocks_reset` | A |
| AC-34 | `E test_ng_engine_risk.py::test_ac34_runtime_minimum_change_blocks_invalid_submits_without_resize` | A |
| AC-35 | `E test_ng_engine_ops.py::test_ac35_stop_success_with_inventory_never_flattens`; `CTL::test_executor_hosts_the_engine_bootstraps_trades_and_stops_without_flattening` | — |
| AC-36 | `E test_ng_engine_ops.py::test_ac36_stop_with_unknown_cancel_is_stop_uncertain_and_restart_keeps_reconciling` | — |
| AC-37 | `E test_ng_engine_risk.py::test_ac37_margin_shortfall_warns_unknown_blocks` (shortfall / None / NaN) | A |
| AC-38 | `E test_ng_engine_risk.py::test_ac38_post_only_rejection_and_blockers_never_fall_back_to_market`; global check in every harness run (`Harness.assert_safe_orders`, property checker) | A |
| AC-39 | `E test_ng_engine_cycles.py::test_ac39_same_side_target_remainders_merge_into_one_exact_tp` (reachable geometry: same cell, returned GTT remainder + new fills) | A `core/test_dust.py`, `core/test_cells.py` (aggregate legs); B allocations |
| AC-40 | `E test_ng_engine_history.py::test_ac40_trade_cumulative_above_order_cumulative_stops_exposure`; `E test_ng_engine_history.py::test_ac12_ac40_conflicting_duplicate_freezes_exposure` | C, B |
| AC-41 | `E test_ng_engine_history.py::test_ac41_duplicates_and_found_ids_do_not_stop_the_scan` | C |
| AC-42 | `E test_ng_engine_history.py::test_ac42_release_waits_delay_and_repeat_scans`; `E test_ng_engine_history.py::test_ac42_super_delayed_fill_after_reuse_goes_to_old_cycle_and_freezes` (late fill → old cycle → FROZEN → audit → ordinary TP resolves, no re-latch) | C (predicate), B, A |
| AC-43 | `E test_ng_engine_risk.py::test_ac43_cid_collision_fails_closed_without_new_id`; `E test_ng_engine_risk.py::test_ac43_cid_map_binds_full_leg_identity_durably` | A (logic), B (durable, exhaustion) |
| AC-44 | `E test_ng_engine_risk.py::test_ac44_full_cap_tp_priority_cancels_cap_consuming_entry`; `E test_ng_engine_risk.py::test_ac44_tp_tp_conflict_is_fifo_without_netting` | A |
| AC-45 | `E test_ng_engine_risk.py::test_ac45_stable_bootstrap_cut_once_and_never_recaptured`; `E test_ng_engine_risk.py::test_ac45_observed_position_must_equal_confirmed_b` | B |
| AC-46..50 | — | WS-E `test/web/neutral_grid/**` |
| AC-51 | `E test_ng_engine_cycles.py::test_ac51_gtt_renewal_only_after_terminal_with_exact_remainder` | — |
| AC-52 | `E test_ng_engine_ops.py::test_ac52_running_grid_dimensions_cannot_change_and_db_is_not_reset` | B (audited migration) |
| AC-53 | `E test_ng_engine_history.py::test_ac53_retention_gap_blocks_until_audited_manual_reconcile` | C, B |
| AC-54 | `E test_ng_engine_crash.py::test_ac54_missing_or_corrupt_db_with_prior_run_fails_closed`; `CTL::test_executor_fails_closed_when_the_store_refuses` | B |
| AC-55 | `E test_ng_engine_crash.py::test_ac55_disk_full_sends_nothing_and_reports_persistence_failure` | B |
| AC-56 | `E test_ng_engine_crash.py::test_ac56_presend_rejection_releases_only_without_transport`; `E test_ng_engine_crash.py::test_ac56_definitive_reject_allows_new_revision_timeout_does_not` | B, C |
| AC-57 | `E test_ng_engine_cycles.py::test_ac57_simultaneous_partial_fills_use_reserved_slots` | A `core/test_admission.py` |
| Crash at every persistence window (AC-15..19) | `E test_ng_engine_crash.py::test_crash_at_every_persistence_window_restarts_consistently` — every `store.FAULT_POINTS` (19) × occurrence 0/1/3, each case asserts that the point was actually reached, then: no phantom order, never a second CID per leg, fills stored at most once and exactly the venue's after the run, `verify_ledger()` clean, venue net inside `[P_min, P_max]`, caps kept | B |

## Decisions and adaptations (recorded per handoff rules)

1. **Generation numbering.** A's `CellLedger` numbers the first cycle 0; B's schema requires generation ≥ 1
   (`cells.generation = 0` means "no cycle"). The engine prepends a closed empty *genesis* cycle 0 to every
   projected ledger, so A's `next_entry_identity()` yields 1 and identities agree with B. No A/B change needed.
2. **Store is authoritative, A is a projection.** B enforces its own transitions/outbox protocol and applies
   history itself; the engine mirrors B's leg/order/cycle rows into A's objects after each write (A's pure
   planning functions then work on exact durable data). Mismatches surface as `LEDGER_INVARIANT` freezes.
3. **Command kinds.** B accepts only `contracts.CommandKind`; operator audits are `baseline_audit` commands with
   `payload.action` in `commands.AUDIT_ACTIONS` (`baseline`, `ack_late_evidence`, `ack_history_conflict`,
   `ack_retention_gap`, `ack_risk_blocked`, `resolve_unknown_submit`). `contracts.py` was not changed.
4. **Bootstrap.** B refuses history batches before bootstrap, so pre-bootstrap rows stay in memory; the cut is the
   high-water of two identical complete scans + identical position over ≥ `settlement_delay_s`, then
   `store.bootstrap(...)` in the same transaction as the command. CONFIRM_BASELINE is rejected (never deferred)
   when not ready; the launcher path retries only transient refusals.
5. **Restart of intents.** A PENDING submit outbox row proves the venue never saw the request: it is dispatched
   with the SAME CID (AC-16) unless entries are now blocked (then NOT_SENT). A DISPATCHED row stays
   SUBMIT_UNKNOWN (reservation kept, never "proven absent"); only evidence or the audited
   `resolve_unknown_submit` resolves it.
6. **Late evidence vs. WS-C withholding.** C withholds every contradicted row. Executions that exceed an order's
   *already settled* terminal cumulative are exact late executions: the engine commits them to the OLD cycle
   (B records `LATE_FILL` + `CUMULATIVE_EXCEEDS_ORDER`, market FROZEN). Contradictions on unsettled orders stay
   withheld (AC-40, no guessing). Audits record the scanner conflict strings as acknowledged and rebase the walk
   boundary (a walk containing a withheld row never completes).
7. **Wake-ups.** Only executions/terminal events wake the scanner (acceptance acks do not); coalesced to one
   extra scan per `min_wake_interval_s` (2 s). The account poll keeps its own cadence (`poll_interval_s`).
   Weight ledger: 16 200/min default (90 % of the 18 000 Standard pool); exhaustion makes history stale, which
   blocks entries (never skips a proof).
8. **Venue cap.** If the effective cap drops below the orders resting, reservations are only on paper: the
   router gets no slot budget, so a TP obtains a real slot through the emergency entry-cancel path (AC-44).
9. **Unknown order at start** is a per-process gate (`normal_since_start`): after every (re)start an unknown
   active order blocks everything until it disappears; while running it blocks entries only.
10. **State mapping.** DEGRADED = persistence failure / fail-closed store / stale or unknown data; FROZEN = open
    history conflict or late evidence; RISK_BLOCKED = store entry blockers, unknown order at start, cap/margin;
    PAUSED = operator pause or automatic entry freeze (e.g. OUTSIDE_BOUNDS) with reasons; STOP outcomes are
    durable and STOP_UNCERTAIN keeps reconciling.
11. **Explicit confirmation (CLI).** The launcher refuses unless `enabled: true` and
    `live_start_confirmation == "START <grid_id> ON lighter_perpetual_robinhood LIT-USDG WITH B=<B>"`; only then
    are `operator_confirmed_start/baseline` set, and the engine still requires the observed stable position to
    equal B exactly.

## Requests to other workstreams / integrator

* **R1 (integrator, blocking for a clean V2 restart).** Register the executor natively:
  `executor_orchestrator.ExecutorOrchestrator._executor_mapping["neutral_grid_executor"] = NeutralGridExecutor`
  and add `NeutralGridExecutorConfig` to `executors_info.AnyExecutorConfig`. Until then
  `executor.register_executor_type()` adds the mapping at import, `executor_info` falls back to
  `ExecutorInfo.model_construct`, and the launcher detaches the executor before the orchestrator persists
  executors (a persisted row of an unregistered type would fail validation on the next Hummingbot start).
* **R2 (WS-C).** A walk that still contains a withheld conflicted row never completes and the high-water cannot
  advance; after an operator audit the engine must rebase the boundary itself. An API to mark audited conflict
  keys (so an acknowledged row stops invalidating walks) would remove that workaround.
* **R3 (WS-A, optional).** Accept an initial generation (or start at 1) so the genesis sentinel is unnecessary.

## Open risks

* Weight: with `poll_interval_s = 5` the steady-state cost (account 300 + active 300 + trades 600 + inactive 100
  per poll) is ~15 600/min, close to the 18 000 Standard pool that the Hummingbot connector's own polling also
  uses. The budget makes history go stale (entries blocked) rather than overspend; live tuning of
  `poll_interval_s`/`history_freshness_s` is needed before production.
* SUBMIT_UNKNOWN whose request never landed stays unresolved until an operator audits it (by design: no venue
  idempotence is documented); STOP then ends as STOP_UNCERTAIN.
* TP–TP FIFO across the anchor line can delay exits in fast two-sided moves (A handoff #8); visible as
  `WAIT_TP_FIFO` with queue age.
* The CLI confirmation is a config phrase, not an interactive prompt (no WS-D-owned launcher binary); the web
  flow (WS-E) is the interactive path.
* Fake-exchange tests cannot prove Lighter's real consistency/lag behaviour; settlement delay/scans remain design
  choices (spec NG-ARCH-003).

## Commands run (code at `211d0c8bf`; `PY=$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python`)

| Gate | Command | Result |
|---|---|---|
| WS-D tests | `$PY -m pytest test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine test/controllers/generic/test_neutral_grid.py -q` | 177 passed |
| Heavy property sweep | `NG_PROPERTY_SEEDS=60 NG_PROPERTY_STEPS=120 $PY -m pytest .../engine/test_ng_engine_properties.py -q` | 61 passed |
| Lighter connector | `$PY -m pytest test/hummingbot/connector/derivative/lighter_perpetual/test_lighter_perpetual_derivative.py -q` | 85 passed, 8 subtests passed |
| Committed neutral/risk | `$PY -m pytest test/scripts/test_lighter_robinhood_neutral_grid.py test/scripts/test_lighter_robinhood_grid_risk.py -q` | 73 passed |
| Controller/executor regressions | `$PY -m pytest test/hummingbot/strategy_v2/executors/grid_executor test/controllers/generic test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py test/hummingbot/strategy_v2/executors/test_executor_base.py -q` | 132 passed |
| Merged WS-A/B/C suites | `$PY -m pytest test/.../neutral_grid_executor/core test/.../neutral_grid_executor/store test/.../neutral_grid_executor/history test/hummingbot/connector/derivative/lighter_perpetual/test_lighter_perpetual_history_pagination.py -q` | 351 passed, 268 subtests passed |
| Compile/import | `$PY -m py_compile <9 WS-D modules>` + import of engine/executor/fake_exchange/commands/snapshot/data_types/controller/script | OK |
| Lint | `$PY -m flake8 <WS-D modules> test/.../engine test/controllers/generic/test_neutral_grid.py` | clean |
| Whitespace | `git diff --check a18cb37d4 HEAD` | clean |
