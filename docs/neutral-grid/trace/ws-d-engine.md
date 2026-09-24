# WS-D engine — traceability, decisions, requests

Branch `codex/ng-engine` (worktree `hummingbot-ng-engine`), created from `a18cb37d4` (contracts commit).
Owned: `neutral_grid_executor/{__init__,engine,executor,fake_exchange,commands,snapshot,data_types}.py`,
`controllers/generic/neutral_grid.py`, `scripts/lighter_robinhood_fixed_neutral_grid.py`,
`conf/controllers/lighter_robinhood_fixed_neutral_grid.yml.example`,
`conf/scripts/lighter_robinhood_fixed_neutral_grid.yml.example`,
`test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/**`, `test/controllers/generic/test_neutral_grid.py`.

Merged final tips: `codex/ng-core` @ `79f9e2f71`, `codex/ng-store` @ `4af66dacd`, `codex/ng-connector` @ `87a6b34df`
(merge commits on this branch; no newer commits at the time of the last gate run). Every engine/CTL test sets
`$HUMMINGBOT_NEUTRAL_GRID_HOST_DIR` to a tmp dir (autouse fixtures in `E conftest.py` and `CTL`), so the real
host-wide `~/.hummingbot/neutral_grid/{locks,markers}` is never touched.

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
| NG-ORD-001 post-only entry, LIMIT GTT TP, no MARKET | engine `_submit`, `_pre_send_blocker` | `E test_ng_engine_cycles.py::test_ac01_normal_buy_cycle_rearms_buy`; `E test_ng_engine_risk.py::test_ac38_post_only_rejection_and_blockers_never_fall_back_to_market`; `E test_ng_engine_cycles.py::test_ac51_gtt_renewal_only_after_terminal_with_exact_remainder`; `E test_ng_engine_risk.py::test_untradable_market_blocks_new_exposure_and_presend` (C `supports_limit/post_only=False` → `MARKET_NOT_TRADABLE` / `POST_ONLY_UNSUPPORTED`) | done |
| NG-ORD-002 virtual cells, `reduce_only=False` | engine `_submit` | `E test_ng_engine_cycles.py::test_ac27_virtual_tp_crosses_zero_without_reduce_only`; `E test_ng_engine_properties.py::test_invariants_hold_under_random_fill_cancel_restart_orderings` | done |
| NG-ORD-003 self-trade router, TP priority, TP–TP FIFO | engine `_act` (A `router.plan_submits`) | `E test_ng_engine_risk.py::test_ac31_self_trade_conflict_cancels_entry_and_tp_waits_for_terminal`; `E test_ng_engine_risk.py::test_ac44_tp_tp_conflict_is_fifo_without_netting` | done |
| NG-CELL-001 whole-cell lock, release (1)–(5) | engine `_settle` (A `can_release`, B `close_cycle`), `position_reconciled` | `E test_ng_engine_cycles.py::test_ac01_normal_buy_cycle_rearms_buy`; `E test_ng_engine_properties.py::test_invariants_hold_under_random_fill_cancel_restart_orderings` | done |
| NG-CELL-002 partial entry, TP per confirmed share, ENTRY_LIVE+TP_LIVE, ≤2 s dispatch | engine `_tp_candidates`, `_submit`, SLO clock | `E test_ng_engine_cycles.py::test_ac05_partial_entry_2_3_5_with_floor_5`; `E test_ng_engine_history.py::test_ac14_tp_dispatch_within_slo_and_blocker_queue_age_visible` | done |
| NG-CELL-003 partial TP, terminal partial entry, late fill after cancel | engine | `E test_ng_engine_cycles.py::test_ac06_partial_tp_does_not_unlock_or_repost`; `::test_ac07_terminal_partial_entry_closes_actual_then_full_cycle`; `::test_ac08_late_fill_after_cancel_event_extends_obligation` | done |
| NG-CELL-004 distinct states, concurrent legs | snapshot `state_flags` (A) | `E test_ng_engine_ops.py::test_committed_snapshot_schema_and_cli_status_without_secrets` | done |
| NG-HIST-001 history authoritative, WS only a hint | engine `wake`, executor wakeups | `E test_ng_engine_history.py::test_ac09_ws_duplicates_and_reorder_never_double_fills_or_prove_terminal`; `E test_ng_engine_history.py::test_ac13_history_lag_wakes_poller_tp_waits_and_lag_visible`; `CTL::test_executor_wires_live_history_wakeups_as_hints_only` | done |
| NG-HIST-002 pagination, overlap, dedupe, settlement, late evidence, cursor atomicity | engine `_apply_history`, `_settle`, audited `_cmd_ack_late_evidence`, `_only_audited_conflicts` | AC-10/11/12/19/40/41/42 rows | done |
| NG-HIST-003 weights, coalescing, bounded startup work | engine weight ledger, scanner cadence | `E test_ng_engine_risk.py::test_ac04_55_cells_one_task_no_polling_storm` | done |
| NG-HIST-004 CID before side effect, no new CID after timeout/crash | engine `_submit`, `_resume_pending_intents` | AC-16/17/43 rows | done |
| NG-RISK-001 baseline once, stable cut, never recaptured | engine `bootstrap_ready`, `_cmd_confirm_baseline` | AC-28/45 rows | done |
| NG-RISK-002 P/P_min/P_max, gross, caps, TP headroom priority | engine (A `risk`, router `owed`) | AC-24/25/26 rows; `E test_ng_engine_properties.py::test_invariants_hold_under_random_fill_cancel_restart_orderings` | done |
| NG-RISK-003 manual orders/trades | engine `_reconcile_active`, store unmatched | AC-29/30 rows | done |
| NG-RISK-004 margin/unknown data/untradable market | engine `_evaluate_state` (A `exposure_blockers`), `_pre_send_blocker` | AC-37 row; `E test_ng_engine_risk.py::test_untradable_market_blocks_new_exposure_and_presend` (not tradable / post-only unsupported) | done |
| NG-RISK-005 admission, slots, queue, runtime floors | engine `_slot_admission`, venue-cap guard | AC-34/44/57 rows | done |
| NG-DB-001..005 durable ledger, intent before side effect, restart, persistence loss, definitive reject | engine + B store | AC-15..21, AC-54/55/56 rows; `E test_ng_engine_crash.py::test_crash_at_every_persistence_window_restarts_consistently`; `E test_ng_engine_crash.py::test_crash_after_cancel_dispatch_mark_resends_the_same_cancel` (DISPATCHED cancel row → `resend_same_cid`); `E test_ng_engine_ops.py::test_command_failing_after_a_store_write_is_rolled_back_then_rejected` (poisoned tx never committed) | done |
| NG-OPS-001 pause | engine commands | `E test_ng_engine_ops.py::test_ng_ops_001_pause_stops_entries_keeps_tps_and_resume_passes_gate`; `::test_resume_is_rejected_while_blocked` | done |
| NG-OPS-002 outside bounds | engine `_act` | `E test_ng_engine_risk.py::test_ac32_outside_bounds_cancels_entries_keeps_tp_no_recenter` | done |
| NG-OPS-003 stop | engine `_finish_stop`, executor `early_stop`, script `on_stop` → `drain_neutral_executors` | AC-35/36 rows; `E test_ng_engine_ops.py::test_stop_without_inventory_is_stopped`; `CTL::test_launcher_stop_drives_the_engine_drain` (StopExecutorAction → early_stop → durable STOP → engine cancels with durable intents → STOPPED_WITH_INVENTORY → executor closed and detached; the launcher cancels nothing itself) | done |
| NG-ARCH-001/003 one executor, controller, launcher, disabled example, credential validator | controller, executor, script, conf examples | `CTL::test_controller_creates_exactly_one_executor_and_never_recreates`; `CTL::test_example_config_is_disabled_and_matches_the_spec_profile`; `CTL::test_config_rejects_unsafe_values`; `CTL::test_profile_and_confirmation_policy`; `CTL::test_api_key_validator_is_the_existing_80_hex_rule`; `CTL::test_executor_hosts_the_engine_bootstraps_trades_and_stops_without_flattening`; `CTL::test_executor_refuses_disabled_config`; `CTL::test_executor_fails_closed_when_the_store_refuses`; `CTL::test_launcher_baseline_mismatch_is_refused_once_not_spammed`; `CTL::test_disabled_controller_creates_nothing_and_says_so`; `CTL::test_register_executor_type_is_idempotent`; connector CID ownership: `CTL::test_durable_cids_are_registered_before_polling_and_released_once_final`, `CTL::test_register_durable_cids_needs_an_owner_and_an_existing_ledger` | done (see request R1) |
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
| AC-39 | `E test_ng_engine_cycles.py::test_ac39_same_side_target_remainders_merge_into_one_exact_tp` (reachable geometry: same cell, returned GTT remainder + new fills); `E test_ng_engine_history.py::test_ac39_aggregate_tp_over_audited_late_and_current_cycle_is_exact_and_durable` (one TP over two generations, `allocation` stored with the intent, water-filled X per generation, identical after crash restart) | A `core/test_dust.py`, `core/test_cells.py` (aggregate legs); B allocations |
| AC-40 | `E test_ng_engine_history.py::test_ac40_trade_cumulative_above_order_cumulative_stops_exposure`; `E test_ng_engine_history.py::test_ac12_ac40_conflicting_duplicate_freezes_exposure`; `E test_ng_engine_history.py::test_ac40_new_contradiction_after_late_audit_is_never_masked_or_applied` | C, B |
| AC-41 | `E test_ng_engine_history.py::test_ac41_duplicates_and_found_ids_do_not_stop_the_scan` | C |
| AC-42 | `E test_ng_engine_history.py::test_ac42_release_waits_delay_and_repeat_scans`; `E test_ng_engine_history.py::test_ac42_super_delayed_fill_after_reuse_freezes_and_goes_to_old_cycle_only_by_audit` (withheld execution → nothing applied, FROZEN → audited `ack_late_evidence` commits it to the OLD cycle + `acknowledge_late_evidence` + audited cumulative + conflicts resolved in one tx → history complete, NORMAL → ordinary TP of the old cycle resolves; survives restart) | C (predicate), B, A |
| AC-43 | `E test_ng_engine_risk.py::test_ac43_cid_collision_fails_closed_without_new_id` (latched CID failure → DEGRADED, nothing sent); `E test_ng_engine_risk.py::test_ac43_cid_map_binds_full_leg_identity_durably` | A (logic), B (durable, exhaustion) |
| AC-44 | `E test_ng_engine_risk.py::test_ac44_full_cap_tp_priority_cancels_cap_consuming_entry` (budget = `SlotBudget.from_plan`, headroom hard ceiling); `E test_ng_engine_risk.py::test_ac44_tp_tp_conflict_is_fifo_without_netting` | A |
| AC-45 | `E test_ng_engine_risk.py::test_ac45_stable_bootstrap_cut_once_and_never_recaptured`; `E test_ng_engine_risk.py::test_ac45_observed_position_must_equal_confirmed_b` | B |
| AC-46..50 | — | WS-E `test/web/neutral_grid/**` |
| AC-51 | `E test_ng_engine_cycles.py::test_ac51_gtt_renewal_only_after_terminal_with_exact_remainder` | — |
| AC-52 | `E test_ng_engine_ops.py::test_ac52_running_grid_dimensions_cannot_change_and_db_is_not_reset` | B (audited migration) |
| AC-53 | `E test_ng_engine_history.py::test_ac53_retention_gap_blocks_until_audited_manual_reconcile` (durable per-stream gap via `mark_retention_gap`, cleared only by `resolved_retention_gaps`) | C, B |
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
6. **Late evidence is audit-driven (orchestrator semantics).** C never offers a conflicted key; the engine applies
   NO quantity for it and freezes (AC-40): the scan conflict marks manual reconcile (FROZEN), history is
   incomplete (entries blocked). Late executions the store sees directly (order row outside the walk) are stored
   by B as `LATE_FILL` (FROZEN as well). The operator's `ack_late_evidence` is the only acceptance, in ONE store
   transaction: (1) withheld executions exactly attributed to a *settled* own leg (TERMINAL / REJECTED_ZERO_FILL,
   single payload, same side/exchange id — `_withheld_late_trades`) are committed via
   `apply_history_batch(tx, ...)` to their OLD cycle; (2) WS-A `CellLedger.acknowledge_late_evidence(gen)` for
   every cycle with late evidence; (3) the audited cumulative of every corrected leg is persisted
   (`om:<cid>.audited_cumulative`; it supersedes the stale venue order row on load); (4)
   `record_manual_reconciliation(resolved_conflict_ids=LATE_FILL + CUMULATIVE_EXCEEDS_ORDER, evidence=accepted
   trades + corrected legs + audited cycles)` → B `late_evidence = 2`, so B accepts an ordinary TP of the old
   cycle. Payload conflicts are never late evidence (`ack_history_conflict` applies nothing). A later walk whose
   only conflicts are audited `trades_exceed_order_cumulative` strings AND whose withheld rows are all committed
   with identical payloads carries no new evidence and counts as complete (`_only_audited_conflicts`); without a
   high-water the walk floor moves to that walk's start (overlap covers lag). Anything new keeps it incomplete.
7. **Wake-ups.** Only executions/terminal events wake the scanner (acceptance acks do not); coalesced to one
   extra scan per `min_wake_interval_s` (2 s). The account poll keeps its own cadence (`poll_interval_s`).
   Weight ledger: 16 200/min default (90 % of the 18 000 Standard pool); exhaustion makes history stale, which
   blocks entries (never skips a proof).
8. **Venue cap / router budget.** Always `router.SlotBudget.from_plan(plan)`: `headroom = cap - actual` is a hard
   ceiling whatever the reservations say, so after a cap reduction a TP obtains a real slot only through the
   router's emergency entry-cancel path (AC-44). The earlier engine-side "no budget" workaround is removed.
   Admission gets `CellAdmission(entry_live=...)` (a non-final entry in the cell's open cycles), so spare slots of
   live-entry cells are trimmed first.
9. **Unknown order at start** is a per-process gate (`normal_since_start`): after every (re)start an unknown
   active order blocks everything until it disappears; while running it blocks entries only.
10. **State mapping.** DEGRADED = persistence failure / fail-closed store / latched CID allocation failure /
    stale or unknown data / market not tradable; FROZEN = open history conflict or late evidence; RISK_BLOCKED =
    store entry blockers (incl. a durable retention gap), unknown order at start, cap/margin;
    PAUSED = operator pause or automatic entry freeze (e.g. OUTSIDE_BOUNDS) with reasons; STOP outcomes are
    durable and STOP_UNCERTAIN keeps reconciling.
11. **Explicit confirmation (CLI).** The launcher refuses unless `enabled: true` and
    `live_start_confirmation == "START <grid_id> ON lighter_perpetual_robinhood LIT-USDG WITH B=<B>"`; only then
    are `operator_confirmed_start/baseline` set, and the engine still requires the observed stable position to
    equal B exactly.
12. **Connector CID ownership.** CID orders are engine-owned, so Hummingbot's generic stop/exit `cancel_all` and
    lost-order paths no longer cancel them. Before the connector's polling loops start (launcher `__init__`), every
    durable CID that may have reached transport (all legs except a never-dispatched INTENT and a proven-unsent
    REJECTED_UNSENT; a PENDING intent must NOT be registered or its same-CID dispatch would be refused) is
    registered via `register_history_reconciled_order` (`executor.register_durable_cids`, read-only ledger, no
    lock); `executor.on_start` repeats it idempotently and sets `engine.leg_final_hook =
    release_history_reconciled_order`, called once per CID after its leg is proven final (stop tracking only).
    The launcher's `on_stop` → `drain_neutral_executors` is therefore the ONLY stop path of those orders; it uses
    the live executor objects (the cached controller reports go stale during `on_stop`).
13. **No catch-and-commit.** A command whose handler raises (store guard after a write, ledger refusal) rolls back
    the whole command transaction; memory is rebuilt from the store and the command is completed REJECTED in a
    new transaction. A DISPATCHED cancel row (dead process) is re-sent with `resend_same_cid=True` (same order;
    cancel is idempotent); submits are never re-dispatched once DISPATCHED.
14. **Tradability.** `trading_rules().supports_limit/post_only=False` (C reports it unless the market is tradable)
    blocks new exposure (`MARKET_NOT_TRADABLE`, `POST_ONLY_UNSUPPORTED`), TPs when LIMIT is unsupported, and
    `_pre_send_blocker` refuses such a request before transport.
15. **Aggregate TP.** `TpDispatchItem.allocation` is passed to `CellLedger.add_tp_intent(allocation=)` and
    `store.record_intent(allocations=)` in the intent transaction; on load `Leg.allocation` comes from
    `LegRecord.allocation` and the projected cycles are linked (`CellLedger._link`) so every generation sees its
    share; the TP FIFO sequence is the oldest covered obligation. Snapshot legs expose `allocation` and
    `audited_cumulative` (additive fields).
16. **Retention gap.** A scanner `retention_gap:<stream>:...` conflict is recorded durably with
    `store.mark_retention_gap(stream, required_boundary, oldest_available)`; while a gap is open the engine never
    writes `complete=True` for that stream; `ack_retention_gap` passes `resolved_retention_gaps=[open streams]`.

## Requests to other workstreams / integrator

* **R1 (integrator, blocking for a clean V2 restart).** Register the executor natively:
  `executor_orchestrator.ExecutorOrchestrator._executor_mapping["neutral_grid_executor"] = NeutralGridExecutor`
  and add `NeutralGridExecutorConfig` to `executors_info.AnyExecutorConfig`. Until then
  `executor.register_executor_type()` adds the mapping at import, `executor_info` falls back to
  `ExecutorInfo.model_construct`, and the launcher detaches the executor before the orchestrator persists
  executors (a persisted row of an unregistered type would fail validation on the next Hummingbot start).
* **R2 (WS-C, liveness).** A walk that still contains a withheld conflicted row never completes and returns no
  high-water, e.g. a stale settled order row stays in every walk while any unresolved order anchors the window.
  The engine treats walks whose only conflicts are audited and whose withheld rows are already committed
  identically as complete and advances its own walk floor (decision 6). An API to mark audited conflict keys (so
  they stop invalidating walks and the high-water advances) would make that workaround unnecessary.
* **R3 (WS-A, optional).** Accept an initial generation (or start at 1) so the genesis sentinel is unnecessary; a
  public constructor of a `CellLedger` from projected cycles would replace the engine's call of `_link()`.
* **R4 (WS-C, optional).** `LighterExchangePort` does not forward `register/release_history_reconciled_order`; the
  executor calls the connector directly (the fake exchange implements the same names).

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
* Rejected-leg late execution: a leg B stored as REJECTED_ZERO_FILL that later shows executions keeps that state
  in B (a final leg cannot be re-resolved); after the audit the engine projects it as TERMINAL with the audited
  cumulative. It is covered by the same audit path but has no dedicated fake-exchange scenario (the fake cannot
  both reject and land one submit).
* Audited-only walks do not advance the durable cursor high-water (the walk floor lives in `engine_meta`); until a
  genuinely complete walk, the high-water-row retention check is skipped for those streams (the horizon check of
  C still applies).
* If the launcher cannot read the ledger at construction (corrupt/unreadable DB), nothing is registered; the
  engine then fails closed, but Hummingbot's generic cancel paths could touch orders of a previous run until the
  connector restores its own tracking marker (logged as an error).

## Commands run (code at `5f0137c85`; `PY=$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python`)

| Gate | Command | Result |
|---|---|---|
| WS-D tests | `$PY -m pytest test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine test/controllers/generic/test_neutral_grid.py -q` | 186 passed |
| Heavy property sweep | `NG_PROPERTY_SEEDS=60 NG_PROPERTY_STEPS=120 $PY -m pytest .../engine/test_ng_engine_properties.py -q` | 61 passed |
| Lighter connector | `$PY -m pytest test/hummingbot/connector/derivative/lighter_perpetual/test_lighter_perpetual_derivative.py -q` | 85 passed, 8 subtests passed |
| Committed neutral/risk | `$PY -m pytest test/scripts/test_lighter_robinhood_neutral_grid.py test/scripts/test_lighter_robinhood_grid_risk.py -q` | 73 passed |
| Controller/executor regressions | `$PY -m pytest test/hummingbot/strategy_v2/executors/grid_executor test/controllers/generic test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py test/hummingbot/strategy_v2/executors/test_executor_base.py -q` | 135 passed |
| Merged WS-A/B/C suites | `$PY -m pytest test/.../neutral_grid_executor/core test/.../neutral_grid_executor/store test/.../neutral_grid_executor/history test/hummingbot/connector/derivative/lighter_perpetual/test_lighter_perpetual_history_pagination.py -q` | 351 passed, 268 subtests passed |
| Compile/import | `$PY -m py_compile <9 WS-D modules>` + import of engine/executor/fake_exchange/commands/snapshot/data_types/controller/script | OK |
| Lint | `$PY -m flake8 <WS-D modules> test/.../engine test/controllers/generic/test_neutral_grid.py` | clean |
| Whitespace | `git diff --check a18cb37d4 HEAD` | clean |
