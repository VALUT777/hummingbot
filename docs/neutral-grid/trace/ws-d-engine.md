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

## Review package 1 (engine @ `bcd2eebef`: lifecycle / risk / router / dispatch + web contract)

Merged first: `codex/ng-engine` fast-forwarded to `codex/ng-web` @ `b693ac615`, which already contains
`codex/neutral-grid-implementation` @ `bb49f434e`, `codex/ng-store` @ `3d0a4d162` and this branch.
Red evidence: the tests were committed at `16b837428` against the unchanged engine and
`test_ng_engine_review1.py` fails there **26/28** for the reported reasons (the two passing ones are the #14
test-gap item and a harness contract check). The final test file was re-run against the `16b837428` engine
in a scratch worktree with the same result, and it passes 28/28 with the fixes. Test-gap items are proven by
mutation: (#14) "release ignores the position condition" is killed by the #14 test; (#15) "an in-progress walk
counts as complete" is killed by the rewritten restart test and survived the old one.

| # | Finding | Fix | Test(s) (red before → green after) |
|---|---|---|---|
| 1 | TP SLO measured with the tick-start clock, anchored to the first (below-minimum) fill; transports awaited before later TP intents; entry cancels before TPs | SLO clock starts at the history commit of the fill that made the obligation dispatchable (`_cell_fill_commit_ms`); latency stamped at the actual intent commit; every router-approved TP intent is committed before any transport (`_commit_intent`), and TP transports go before withdrawals, resumed intents, cancels and entries | `E test_ng_engine_review1.py::test_r01_every_tp_intent_commits_within_slo_although_each_transport_takes_time` (fake transport latency 0.4 s: red 4 400 ms > 2 000 ms); `E test_ng_engine_review1.py::test_r01_tp_transports_are_sent_before_entry_cancels`; `E test_ng_engine_review1.py::test_r01_slo_clock_starts_when_the_obligation_becomes_dispatchable` (red 60.0 s) |
| 2 | One cell's history conflict / any store-ledger refusal withheld every cell's TP and the outside-bounds cancels | TP blocking scoped per cell (`tp_blocked_cells`: store conflicts naming a CID; under LEDGER_INVARIANT only cells failing `check_invariants`/`verify_ledger`); risk-reducing cancels (outside bounds, cancel retries, unsent cancels) flow in every non-persistence-failure state; entries stay blocked globally (FROZEN) | `E test_ng_engine_review1.py::test_r02_one_cells_history_conflict_does_not_withhold_other_cells_tps_or_risk_cancels`; `E test_ng_engine_review1.py::test_r02_ledger_invariant_freeze_keeps_tps_of_exact_cells_and_outside_bounds_cancels` |
| 3 + 17 | Drift judged on a position read that predates the same tick's fill commit; release used last tick's flag | Reads and commits carry a causal sequence (`position_seq`, `active_seq`, `fills_commit_seq`) and request-time stamps; drift and NG-CELL-001 (5) only on a position requested after the latest fill commit; `_settle` recomputes reconciliation; when a release waits only on (5) the position is re-read after the commit (weight-budgeted) | `E test_ng_engine_review1.py::test_r03_position_read_before_the_same_ticks_history_commit_is_not_drift` (red: false "position drift", RISK_BLOCKED); `E test_ng_engine_review1.py::test_r17_release_needs_a_position_read_after_the_closing_fill_commit` (red: released at tick 5 with read 0 while P = 10) |
| 4 | Failing rules refresh retried every tick, starving history | Exponential backoff (2 s → 60 s); the rules read is lower priority (only with one page of each stream + one account poll left) and bounded to what the poll/scan cadence leaves of the budget (≥ 1 read/min) | `E test_ng_engine_review1.py::test_r04_failing_rules_refresh_backs_off_and_does_not_starve_history` (red: 46 rules calls / 120 s) |
| 5 | Runtime minimum decrease: small partial fills split into many TPs → emergency cancels of other cells' entries | Entry intent stores the arming-time minimum TP (`OrderMeta.arming_min_tp`); while the entry is live a smaller TP accumulates inside the cell's reservation (`TP_ACCUMULATING`); once the entry is final everything is dispatched; emergency cancels remain only for real cap drops / hard risk | `E test_ng_engine_review1.py::test_r05_runtime_minimum_decrease_never_emergency_cancels_other_cells_entries` (red: `SLOT_EMERGENCY_TP_PRIORITY`) |
| 6 | Rejected intents re-issued with a new CID every tick, no blocker | Off-tick grid prices blocked in admission/TP planning (`PRICE_NOT_ON_TICK`); pre-send and definitive venue rejects latch the cell/role (`meta.reject_latches`) until the rules/config fingerprint changes or an exponential backoff elapses (first venue reject: one immediate retry, AC-56); blocker visible per cell | `E test_ng_engine_review1.py::test_r06_off_tick_prices_latch_a_blocker_instead_of_dead_legs_every_tick` (red: 30 dead TP legs); `E test_ng_engine_review1.py::test_r06_definitive_venue_reject_is_latched_with_backoff_not_resent_every_tick` |
| 7 | STOP never dispatched a committed-but-unsent CANCEL | The stop branch and the blocked branch dispatch CANCEL_PENDING legs with a PENDING cancel row | `E test_ng_engine_review1.py::test_r07_stop_drain_dispatches_a_committed_but_unsent_cancel` (red: STOPPING forever) |
| 8 | `resolve_unknown_submit` accepted a missing/stale active list | Requires an active list requested after the dispatch and fresh, plus a complete walk started after the dispatch (`_absence_not_proven`) | `E test_ng_engine_review1.py::test_r08_resolve_unknown_submit_is_refused_without_an_active_list`; `E test_ng_engine_review1.py::test_r08_resolve_unknown_submit_is_refused_with_an_active_list_older_than_the_dispatch` |
| 9 | STOPPED with baseline B ≠ 0 / unknown position | Stop outcome only with a fresh position requested after the latest fill; B ≠ 0 counts as inventory; otherwise STOPPING → STOP_UNCERTAIN after the timeout | `E test_ng_engine_review1.py::test_r09_stop_is_never_stopped_with_an_unknown_position_and_counts_the_baseline` |
| 10 | Steady-state walks re-read all history since the oldest LIVE entry | Lookback only for SUBMIT_UNKNOWN / TERMINAL_UNKNOWN / CANCEL_* / terminal-row-pending legs | `E test_ng_engine_review1.py::test_r10_proven_live_orders_do_not_extend_the_history_lookback` |
| 11 | Active rows read before the same tick's terminal commit recorded as evidence → store refusal loop | No live evidence for a CID whose terminal row is committed; a row older than that commit (`active_seq`) is ignored, a newer one for a final leg is a manual-reconcile contradiction | `E test_ng_engine_review1.py::test_r11_active_row_older_than_the_terminal_commit_is_ignored_not_a_store_refusal` (red: LEDGER_INVARIANT "cumulative filled regressed") |
| 12 | Phantom history lag for a WS signal of an already committed trade | Committed trade ids are skipped in `wake`; client-id-only signals expire after a complete walk that started after them (trade-id signals stay until committed) | `E test_ng_engine_review1.py::test_r12_ws_signal_for_an_already_committed_trade_is_not_history_lag` |
| 13 | RISK_BLOCKED TP without per-cell blocker, only the first reason kept | Per-cell blocker + `meta.risk_blocked` (TP key → reason) in the freeze detail; cleared by `ack_risk_blocked` | `E test_ng_engine_review1.py::test_r13_risk_blocked_tps_name_their_cells_and_every_reason` |
| 14 | No engine test for release condition (5) | test gap | `E test_ng_engine_review1.py::test_r14_release_waits_until_the_account_position_equals_the_ledger` (mutation-proven) |
| 15 | Restart test never observed RECONCILING; `hasattr` tautology | test rewritten: own-fill backlog > one bounded scanner step → RECONCILING, nothing sent; NORMAL only after the walk completed | `E test_ng_engine_ops.py::test_status_states_are_honest_during_bootstrap_and_reconcile` (mutation-proven) |
| 16 | LEDGER_INVARIANT freeze lost on reload | Re-applied and persisted by `_reload` (`_sticky_freezes`); only `ack_history_conflict` clears it | `E test_ng_engine_review1.py::test_r16_ledger_invariant_freeze_survives_reload_and_restart_until_acknowledged` |
| W1 | A lone CONFIRM_BASELINE went live | START requires `risk_acknowledged=true` + a 24-hex `preview_id` (and B equal to the config when given); CONFIRM_BASELINE requires an applied START and the enabled/offline gate; the launcher START carries the same fields (preview id = digest of the confirmed grid id + B) | `E test_ng_engine_review1.py::test_w1_confirm_baseline_requires_an_applied_acknowledged_start`; `E test_ng_engine_review1.py::test_w1_confirm_baseline_applies_the_enabled_gate` |
| W2 | Web needs the running config and rules bounds | `summary.engine_config` (every GridConfig field, decimals as strings, + core fingerprint), `summary.started`, `runtime_rules.supports_limit/supports_post_only/fetched_at/max_age_s` (`max(rules_max_age_s, 3 × rules_refresh_s)`) | `E test_ng_engine_review1.py::test_w2_snapshot_publishes_engine_config_started_and_runtime_rules_bounds` (parsed by `web.neutral_grid.runtime.engine_config_from_snapshot`) |
| W3 | UI cannot see persistence failures | `<db_path>.health.json` atomically written (tmp + fsync + replace + dir fsync) whenever `(persistence_error, fatal_reason)` changes, incl. recovery and fail-closed open; `engine.health()` in process | `E test_ng_engine_review1.py::test_w3_health_sidecar_reports_persistence_failure_and_recovery`; `E test_ng_engine_review1.py::test_w3_health_sidecar_reports_a_fail_closed_engine` |
| W4 | Per-tick DEGRADED↔NORMAL flapping churns engine_revision | Back to NORMAL only after `normal_hysteresis_ticks` (3) clean ticks; meanwhile DEGRADED with `STABILIZING` (no new entries); `engine_revision` bumps only on committed state changes | `E test_ng_engine_review1.py::test_w4_no_per_tick_degraded_normal_flapping` (red: 11 flips / 12 ticks) |

Adapted existing tests: `test_ng_engine_risk.py::test_ac43_*` (DEGRADED, from the previous package), the harness sends
the acknowledged START payload (a bare `{}` START is now refused), the AC-56 new-revision behaviour is kept by the
one immediate retry after a first definitive venue reject.

## Review package 2 (engine @ `bcd2eebef`: persistence / restart / ops / launcher)

Red evidence: the tests were committed at `969003ef4` on the unchanged package-1 engine; the final test files fail
there **15/44** (every fix test, for the reported reason; the other 29 are unchanged existing CTL tests and the J
test-honesty items) — re-run in a scratch worktree of `969003ef4` with the final test files. Green at `0399975ae`
(+ `f96d33c58`, merge of the one test-only `codex/ng-web` commit `85359342d`). Items already fixed in package 1:
cancel committed-but-unsent during STOP (#7), LEDGER_INVARIANT durability (#16), STOPPED with baseline/unknown
position (#9), resolve_unknown_submit on stale evidence (#8), drift same-tick timestamps (#3).

| Item | Finding | Fix | Test(s) |
|---|---|---|---|
| A (HIGH) | Hummingbot-stop STOP lost when its queued row becomes CONFLICT; drain logged an unpersisted STOP_UNCERTAIN | Executor tracks its STOP row: CONFLICT/REJECTED → re-enqueued with fresh revisions and a NEW key (the operator's own CLI stop intent; web commands keep the strict 409); `_stop_command_sent` only once APPLIED. Drain timeout = `stop_uncertain_after_s + 30` s (150 s); the drain reports the durable outcome (`last_drain_outcome`: `STOP_NOT_APPLIED` / `STOPPING` / committed outcome) | `CTL::test_a_hummingbot_stop_is_resent_after_a_conflict_until_the_engine_applies_it` (red: CONFLICT, never re-sent, 10 orders live); `CTL::test_a_drain_reports_the_durable_outcome_and_waits_at_least_the_uncertain_bound` |
| B | Launcher's automatic START (new random key every process) cleared a durable STOP / STOP_UNCERTAIN | A `source=launcher` START is refused (`DURABLE_STOP_ACTIVE`) while `stop_requested_ms` is set unless it carries `resume_stop_ms` equal to that stop; the launcher sets it only when `resume_after_stop_confirmation == "RESUME <grid_id> AFTER STOP <stop_ms>"` (read-only ledger read at launch); an explicit operator (web) START still resumes; the executor surfaces a refused START | `E test_ng_engine_review2.py::test_b_automatic_launcher_start_never_overrides_a_durable_stop` (red: NORMAL, 9 new submits); `E test_ng_engine_review2.py::test_b_launcher_resume_must_name_the_durable_stop_it_resumes`; `CTL::test_b_e_launcher_confirmations_bind_resume_and_migration_to_explicit_phrases` |
| C | Pre-send checks only after the intent commit | Tick and quantity checks in planning (idle eligibility, TP planning) before any open_cycle/CID/intent; a `PRESEND` latch waits for a rules/config fingerprint change (no backoff retry); a blocked TP obligation keeps its SLO/queue-age clock; the cell blocker is persisted with the cell row | `E test_ng_engine_review2.py::test_c_off_tick_prices_are_durable_visible_blockers_without_any_intent_or_cid` (red: queue age None) |
| D | Baseline audit absorbed an own fill history had not delivered (B=11 instead of 7) | Audit refused (`AUDIT_EVIDENCE_NOT_SETTLED`, retryable) unless the position was requested after the latest fill commit, no WS trade signal is pending (lag 0), a complete walk started after the position read and no own order is unresolved | `E test_ng_engine_review2.py::test_d_baseline_audit_is_refused_while_an_own_fill_may_be_missing_from_history` (red: APPLIED with B=11) |
| E | No path to a new grid on the same account/market | Launcher-confirmed migration (`migrate_grid_confirmation == "MIGRATE lighter_perpetual_robinhood LIT-USDG TO <grid_id>"`): the store opens without the fingerprint (`open_engine(allow_grid_migration=True)`), the engine stays frozen (CONFIG_MISMATCH) and the audited `migrate_grid` action verifies `grid_mutation_blockers() == []`, a new grid id and fresh market data, then calls `store.migrate_grid` in the command transaction (old grid RETIRED, cycles kept, baseline/fills/cursors untouched). The controller's default ledger is the store's per-account/market path (`engine_db_path`), so migration is the only route | `E test_ng_engine_review2.py::test_e_quiescent_grid_is_migrated_by_an_audited_command_keeping_old_cycles`; `E test_ng_engine_review2.py::test_e_migration_is_refused_while_the_old_grid_has_obligations`; `CTL::test_e_controller_default_database_is_per_account_and_market` |
| F | CID collision latched FREEZE_CID forever | Colliding CID remembered (`meta.colliding_cid`, sticky across reload); audited `retire_colliding_cid` calls `store.retire_cid` and clears the freeze in the command transaction | `E test_ng_engine_review2.py::test_f_cid_collision_is_recovered_by_an_audited_retire` |
| G | CLI status lacked cursor progress / TP remaining | `format_status` renders trades/orders cursors, pages read, walk progress/backoff and `remaining` per TP child | `E test_ng_engine_review2.py::test_g_cli_status_shows_cursor_progress_and_tp_remaining` |
| H | 1000 LIT caps enforced as a product ceiling | Caps validated finite positive; a difference from the profile default is a warning (`profile_warnings`) | `CTL::test_h_profile_caps_are_defaults_not_a_product_ceiling`; `CTL::test_profile_and_confirmation_policy` (adapted) |
| I | `operator_confirmed_*` loadable from YAML | Removed as config fields (a YAML key is rejected by `extra="forbid"`); launcher-only private attributes via `mark_operator_confirmed` | `CTL::test_i_operator_confirmations_cannot_be_loaded_from_yaml` |
| J | Test honesty | AC-12 idle eligible cell + page fault → no entry while incomplete, resumption after; AC-42 delay 20 s asserted at tick granularity; AC-55 faults at the intent / cancel-intent / dispatch-mark writes; AC-17/21 exact `P_min`/`P_max` incl. the unknown leg + slot accounting; STOPPING during a drain | `E test_ng_engine_review2.py::test_j_ac12_*`, `E test_ng_engine_review2.py::test_j_ac42_*`, `E test_ng_engine_review2.py::test_j_ac55_intent_write_failure_sends_nothing`, `E test_ng_engine_review2.py::test_j_ac55_dispatch_mark_failure_sends_nothing`, `E test_ng_engine_review2.py::test_j_ac55_cancel_intent_failure_sends_no_cancel`, `E test_ng_engine_review2.py::test_j_ac17_*`, `E test_ng_engine_review2.py::test_j_ac21_*`, `E test_ng_engine_review2.py::test_j_honest_stopping_state_during_a_drain`; mutation-proven: "entries ignore HISTORY_* blockers", "settlement delay 0", "submit without a committed intent", "transport despite a failed dispatch mark", "cancel before its intent", "UNKNOWN legs dropped from the risk endpoints", "STOPPING reported as STOP_UNCERTAIN" — each kills its test; the unmutated code passes |

Adapted existing tests: `CTL::test_controller_creates_exactly_one_executor_and_never_recreates` (default ledger is
None = per account/market), `CTL::test_profile_and_confirmation_policy` (caps are user limits),
`test_ng_engine_review1.py::test_r13_*` (the audit is retried until its evidence is settled). The new
`retire_colliding_cid` / `migrate_grid` actions are `commands.EXTENDED_AUDIT_ACTIONS` (accepted by the engine,
launcher/CLI path); `commands.AUDIT_ACTIONS` stays the web contract (`test_ngweb_commands.py::test_audit_actions_match_engine`).

## Round 3 (fix verification + critics, integration `203e31370`)

Merged first: `codex/neutral-grid-implementation` @ `203e31370` (fast-forward), then `codex/ng-store` @ `1c8ec8be4`
(B-09, m0004), `codex/ng-connector` @ `a01d679e4` (WS-C: `AuditedResolution`, type-only port read errors; the
`lighter_port.py` conflict with my own C1 port change was resolved in WS-C's favour, my port test passes against it)
and `codex/ng-web` @ `5a91b1fe2` (WS-E round 3: display redaction, stale sidecar = unknown, E-09 web check, UI for
`EXTENDED_AUDIT_ACTIONS` — payload shapes unchanged: `retire_colliding_cid` takes an optional `cid`,
`migrate_grid` takes `note`/`actor`). Red evidence: the tests were committed at `2fbb338e7` on the package-2 tip
and **19** of them fail there for the reported reasons (the test files are unchanged since); the D1-11/D1-17/D2-07
test-gap tests pass and are proven by mutation (in a scratch worktree of `4c0fe8d46`: "no RESUME_PENDING_CANCEL"
kills both D1-11 tests, "`_settle` uses the previous tick's flag" kills both D1-17 variants, "audit ignores the
position/fill order" kills D2-07; C3's wiring: "serve no audited resolutions" kills the C3 test).

| Item | Fix | Test(s) |
|---|---|---|
| C1 secret leak | `engine.redact()` on every operator-visible text: `_error`, transport details, persistence errors, freeze details, incomplete-history reasons and cursor reasons (URL query strings cut; auth/token/signature/key values and long hex replaced; length capped). Port reads raise type-only errors without a cause (WS-C's `LighterPortRequestError`) | `E test_ng_engine_review3.py::test_c1_auth_token_never_reaches_snapshot_errors_status_logs_or_the_ledger` (snapshot, errors, CLI, log capture, health, raw SQLite+WAL bytes); `E test_ng_engine_review3.py::test_c1_lighter_port_wraps_non_history_errors_with_type_only_detail` |
| C2 withheld conflicts | `_withheld_conflict_cells`: rows in `scanner.last_conflicted_rows` are mapped to cells by own CID / exchange order id and block that cell's TPs (entries are blocked by incomplete history); an unattributable row adds `HISTORY_CONFLICT_UNATTRIBUTED` to the global TP/entry blockers (fail closed); audited rows do not block | `E test_ng_engine_review3.py::test_c2_scanner_withheld_conflict_blocks_the_contradicted_cells_tp` (red: TP 5 LIVE from the contradicted 3); `E test_ng_engine_review3.py::test_c2_unattributable_withheld_conflict_blocks_every_new_tp_fail_closed` |
| C3 audited payloads | `ack_history_conflict` records, for every withheld key with a committed payload, `accepted` = committed fingerprint and `noise` = the other versions seen now (`meta.audited_payloads`, in the command transaction; also in the manual-reconciliation evidence). `_CursorView.audited_resolutions()` serves them as WS-C `AuditedResolution`s, so walks skip audited noise and complete; a version never seen at the audit stays a conflict; a never-committed key is not audited (no accepted payload is guessed) | `E test_ng_engine_review3.py::test_c3_audited_payload_conflict_restores_completeness_and_stop_finishes_honestly` (conflict → ack → complete walk → survives restart → STOP ends STOPPED_WITH_INVENTORY) |
| C4 START CONFLICT | `_track_start` re-sends a CONFLICTed (or never-inserted) launcher START with fresh revisions and a new key; REJECTED is surfaced as `start_error` | `CTL::test_c4_launcher_start_that_turns_conflict_is_resent_and_the_resume_applies` |
| C5 + D1-07 lag | Executor forwards only this trading pair's grid CIDs as hints. The engine ignores signals for committed trade ids and for settled own legs (replays); a trade-id label not tied to a live own order expires after a complete walk started ≥ `settlement_delay_s` after it. A label of our live order stays until committed, so real lag stays visible (AC-13; the web demo's 30 s lag test) | `E test_ng_engine_review3.py::test_c5_trade_signal_that_is_never_committed_expires_after_a_settled_covering_walk`; `CTL::test_c5_executor_forwards_only_this_markets_grid_fills_as_hints` |
| C6 older schema | `executor.open_ledger_readonly` reads a ledger of an older schema with the migrations it has (the writer upgrades it at start); the launcher's `ledger_preflight` refuses the start with a clear message if the ledger cannot be read (never a silent continue) | `CTL::test_c6_launcher_reads_an_older_schema_ledger_before_the_engine_upgrades_it`; `CTL::test_c6_launcher_refuses_to_start_when_the_ledger_cannot_be_read` |
| C7 example | documents `<data>/neutral_grid/neutral_grid.<domain>.<account>.<pair>.sqlite3` | `CTL::test_c7_example_config_documents_the_real_default_ledger_path` |
| D1-02 / D2-04 | Active-orders evidence recorded one order per transaction: a refused row is isolated to its cell (`evidence_conflicts`, TP blocked, manual reconcile "history conflict …" → FROZEN). Any recurring store/ledger refusal: freeze persisted, reload, then TPs of exact cells and risk-reducing cancels still run (unless the refusal came from acting) and the FROZEN state + snapshot are committed in their own transactions | `E test_ng_engine_review3.py::test_d102_recurring_active_row_contradiction_is_isolated_and_frozen_is_committed`; `E test_ng_engine_review3.py::test_d204_any_recurring_store_refusal_still_commits_frozen_and_keeps_risk_reducing_work` |
| D1-16 | The LEDGER_INVARIANT freeze is merged into the stored engine meta in its own transaction inside the handler | `E test_ng_engine_review3.py::test_d116_invariant_freeze_survives_an_immediate_crash` |
| D1-11 / D1-17 / D2-07 | test gaps | `E test_ng_engine_review3.py::test_d111_committed_unsent_cancel_is_resumed_on_the_normal_path`, `E test_ng_engine_review3.py::test_d111_committed_unsent_cancel_is_resumed_while_tps_are_blocked`, `E test_ng_engine_review3.py::test_d117_release_never_uses_a_stale_position_flag[weight/manual]`, `E test_ng_engine_review3.py::test_d207_audit_refused_when_the_position_read_predates_a_committed_fill` (mutation-proven) |
| D2-01 | A STOP whose insert failed (no row) is re-sent; `_send_stop` and the START enqueue never raise out of the executor; the drain logs "STOP was NOT applied … resumes trading on its next start" when no durable stop exists | `CTL::test_d201_stop_whose_insert_failed_is_resent_until_applied` |
| D2-15 | `resolve_unknown_submit` additionally needs: `settlement_delay_s` since the dispatch, no pending WS signal for that CID, an active list read after dispatch + delay and `settlement_scans` complete walks started after dispatch + delay | `E test_ng_engine_review3.py::test_d215_resolve_unknown_submit_never_accepts_while_the_order_is_live` (active lag 7 s) |
| D2-18 | Baseline audit also refused while a LIVE own order's active row shows more execution than history proves, and until the venue position was stable (no fill committed since) from a read at least `settlement_delay_s` before a complete walk started | `E test_ng_engine_review3.py::test_d218_audit_refused_while_an_active_row_shows_executions_history_lacks` (no WS event at all) |
| E-09 | START stores the full-config fingerprint (all GridConfig fields but `enabled`); CONFIRM_BASELINE before bootstrap refuses a changed config (`START_CONFIG_CHANGED`) and resets `started` | `E test_ng_engine_review3.py::test_e09_confirm_baseline_refuses_a_config_changed_after_start` |
| E-03 | Health sidecar rewritten at least every `health_heartbeat_s` (10 s) even when healthy; a sidecar older than that is "unknown" (WS-E reader) | `E test_ng_engine_review3.py::test_e03_health_sidecar_heartbeat_even_when_healthy` |

Web round-3 contract (orchestrator follow-ups, red at `f48ed9490`, green at `99cb39468`):
`summary.colliding_cid` (str|null) and `summary.grid_mutation_blockers` (list[str]: the store's list when the engine
is durably stopped or opened for a confirmed migration, else a non-empty "stop first" blocker, so a running grid
never pays the query); the engine accepts exactly the web's extended-audit payloads (`action`, `note`,
`acknowledge`, typed `confirmation`, and `cid` for `retire_colliding_cid`) and retires **only** the CID it recorded
as colliding (`NOT_THE_COLLIDING_CID` otherwise); START's `material_id` is persisted with the binding and in the
start audit (informational; `start_config_fingerprint` is the authority) —
`E test_ng_engine_review3.py::test_web_retire_payload_is_accepted_only_for_the_recorded_colliding_cid`,
`::test_web_migrate_payload_and_published_grid_mutation_blockers`, `::test_web_start_material_id_is_persisted_with_the_start_binding`.
Schema v4 (`m0004_dispatch_owner`, merged `d82942c12`): the launcher's pre-start reads were checked against v1, v2
and v3 ledgers (`open_ledger_readonly`); the engine records NOT_SENT only for never-dispatched PENDING rows, so the
store's B-09 owner rule never changes an engine outcome. Ledger corrections (`ledger_correction_required`): an
engine audit always accepts the committed payload, so it never produces one; should the scanner report one, history
stays incomplete (entries blocked, reason visible) — a store-audited correction path is requested (R6).

Deviation, recorded: C5's "expire trade-id labels after a covering walk" is applied only to labels that are not
tied to one of our live orders; expiring our own live order's label would hide genuine history lag (AC-13), which
the merged web demo test (`test_ngweb_demo_engine.py::test_demo_engine_full_operator_flow`) asserts.

## Round 4 (last engine round, integration `5f266ca83`)

Merged first: `codex/neutral-grid-implementation` @ `5f266ca83` (fast-forward; tree identical to `624d7e643`).
Red evidence: the round-4 tests (committed at `eea457d5a`, fix at `d0eef1804`) fail on `5f266ca83` for the reported reasons (scratch worktree of that tip with the
new test files: **23 failed**; the two intended passes are the unattributable-conflict guard and the D2-18 pin);
test-gap items are proven by mutation in a scratch copy of the fixed tree (`mutate4.py`, one guard removed per run).

| Item | Fix | Test(s) |
|---|---|---|
| H1 (D2-15 + critic) | `resolve_unknown_submit`: refused while ANY pending WS label maps to the CID (trade-id labels via `_ws_label_cid`, not only a CID label); evidence must be taken after `unknown_resolution_delay_s` (EngineOptions / executor / controller config, default 120 s, never below `settlement_delay_s`): an active list read after dispatch + delay and `settlement_scans` complete walks started after it. A REJECTED_ZERO_FILL leg (venue reject or audited "never landed") is final but never "settled" for WS signals: a later trade signal for it is recorded and stays pending (lag visible, rebase refused) until history commits it (then the store's LATE_FILL path applies) | `E test_ng_engine_review4.py::test_h1_ws_trade_signal_of_the_cid_blocks_a_not_landed_resolution` (red: APPLIED at +12 s, second TP); `::test_h1_active_list_lag_beyond_settlement_never_yields_a_second_tp[15/30]` (red: APPLIED at +16 s); `::test_h1_not_landed_submit_is_resolvable_only_after_the_unknown_resolution_delay` (liveness); `::test_h1_a_ws_fill_of_an_audit_resolved_order_stays_visible_as_lag` |
| H2 (CR-4 variant) | Executor: once Hummingbot asked to stop, the launcher START intent is dropped and never (re)sent (`early_stop` clears it; `_track_start` returns). Engine: every APPLIED STOP records `last_stop_applied_ms`; a launcher resume must name the LATEST applied STOP (a STOP while STOPPING keeps `stop_requested_ms` but supersedes a resume phrase typed before it) → `DURABLE_STOP_ACTIVE` with `latest_stop_ms`. `durable_stop_ms()` (launcher phrase) returns that latest stop | `CTL::test_h2_hummingbot_stop_at_launch_is_never_undone_by_a_resent_resume_start` (exact 3-tick race; red: stop cleared); `E test_ng_engine_review4.py::test_h2_launcher_resume_naming_a_stop_that_a_later_stop_reaffirmed_is_refused` |
| M1 (conflict binding, CR-3) | Snapshot `summary.history_conflicts` = list of `{stream, key, cell_id, versions: [{fingerprint, summary (strings), committed}]}` and `summary.conflict_set_id` (32-hex digest of streams/keys/fingerprints). Entries: durable payload contradictions (M2), open store conflicts (`stream="store_conflict"`, key = conflict id), the manual-reconcile reason, a LEDGER_INVARIANT freeze, refused active-row evidence. `ack_history_conflict` must carry `conflict_set_id` (`CONFLICT_SET_ID_REQUIRED` / `CONFLICT_SET_CHANGED` otherwise) and may carry `accepted: {key: fingerprint}`; it audits only that set: a committed key accepts the committed version (a different choice → `ACCEPTED_INVALID` `LEDGER_CORRECTION_NOT_SUPPORTED`, R6), a never-committed key needs a choice (`ACCEPTED_CHOICE_REQUIRED`; the scanner then commits the chosen version as ordinary evidence), noise = every other version seen by any walk, merged with earlier noise of the same accepted payload. An empty set → `NOTHING_TO_AUDIT` | `E test_ng_engine_review4.py::test_m1_snapshot_publishes_the_history_conflict_set`, `::test_m1_ack_is_bound_to_the_reviewed_conflict_set` (probe P1), `::test_m1_ack_with_nothing_to_audit_is_refused`, `::test_m1_never_committed_duplicate_needs_an_accepted_choice_then_history_completes` (CR-3a), `::test_m1_flapping_key_converges_after_one_ack` (CR-3b) |
| M2 (CR-2 transient) | Every finished walk records its unaudited payload contradictions (a key served with a payload other than the committed one, or with several payloads) with all versions in `engine_meta.history_conflicts` (own transaction, survives reload/restart) until `ack_history_conflict`. Each recorded key blocks its cell's TPs (own CID / exchange id; unattributable → global TP/entry block) and keeps the engine FROZEN | `E test_ng_engine_review4.py::test_m2_transient_contradiction_keeps_the_cell_blocked_until_acknowledged` (red: TP 5 from the unaudited quantity; also across a restart) |
| M3 (D1-02 restart) | `startup_scoped`: a finished walk whose only problems are row contradictions attributed to own cells (`duplicate_key_payload_mismatch`, `committed_payload_mismatch`, `trades_exceed_order_cumulative`, `inactive_order_not_terminal`), or a store-refused history batch whose rows are all attributed (`history_refused_cells`), lifts RECONCILING from the TP blockers only: exits of unaffected cells and risk-reducing cancels run; RECONCILING stays an entry blocker; affected cells stay TP-blocked. Boundary/retention/schema problems or an unattributable row keep the global block | `E test_ng_engine_review4.py::test_m3_restart_with_a_cell_attributed_conflict_keeps_other_cells_exits_and_cancels`, `::test_m3_restart_with_a_recurring_history_refusal_keeps_unaffected_exits_and_cancels` (red: tp_blockers ['RECONCILING'], 0 cancels), `::test_m3_scoped_startup_keeps_risk_reducing_cancels_under_another_tp_blocker` (MARKET_NOT_TRADABLE also blocks TPs; red: 0 cancels); guard `::test_m3_unattributable_conflict_after_restart_keeps_the_global_block` |
| M4 (D2-18 + D2-07) | Baseline audit also refused while active rows are unknown or older than the position read, and while ANY history row (own fill, unmatched manual trade, order row) was committed after the position read (`history_rows_commit_seq`) | `E test_ng_engine_review4.py::test_m4_audit_never_rebases_while_active_orders_are_unknown[1000/3]` (red: APPLIED with 11), `::test_m4_audit_refused_when_a_manual_trade_was_committed_after_the_position_read` (red: APPLIED with the stale 7), `::test_m4_audit_rebases_only_after_history_delivers_the_fill_the_active_row_shows` (D2-18 pin, loops past the settlement window; applies with 7) |
| L1 (E-09) | Before bootstrap a START whose config fingerprint differs from the acknowledged one re-binds it (APPLIED `rebound: true`, audit `rebound_from`); after bootstrap it stays `already_started`. Executor: `START_CONFIG_CHANGED` on the baseline re-sends the launcher START (operator confirmed this config at launch) and retries the baseline instead of latching | `E test_ng_engine_review4.py::test_l1_start_before_bootstrap_rebinds_a_changed_config[operator/launcher]`; `CTL::test_l1_start_config_changed_makes_the_launcher_rebind_and_retry_the_baseline` |

Mutation proofs (scratch copy of the fixed tree, one guard removed per run; 16/16 killed):

| Mutation (one guard removed) | Killed by |
|---|---|
| WS gate checks only a CID label (`str(cid) in ws_pending`) | `test_h1_ws_trade_signal_of_the_cid_blocks_a_not_landed_resolution` |
| unknown-resolution delay = settlement delay | `test_h1_active_list_lag_beyond_settlement_never_yields_a_second_tp[15/30]`, `test_h1_not_landed_submit_is_resolvable_only_after_the_unknown_resolution_delay` |
| REJECTED_ZERO_FILL counted as settled for WS labels | `test_h1_a_ws_fill_of_an_audit_resolved_order_stays_visible_as_lag` |
| engine: resume may name `stop_requested_ms` (not the latest applied STOP) | `test_h2_launcher_resume_naming_a_stop_that_a_later_stop_reaffirmed_is_refused` |
| executor: START (re)sent after the Hummingbot stop | `CTL::test_h2_hummingbot_stop_at_launch_is_never_undone_by_a_resent_resume_start` |
| ack not bound to `conflict_set_id` | `test_m1_ack_is_bound_to_the_reviewed_conflict_set` |
| contradiction record keeps only the latest walk's versions | `test_m1_flapping_key_converges_after_one_ack` |
| APPLIED with an empty set | `test_m1_ack_with_nothing_to_audit_is_refused` |
| durable record does not block its cells | `test_m2_transient_contradiction_keeps_the_cell_blocked_until_acknowledged` |
| scoped startup never lifts RECONCILING from the TP blockers | both `test_m3_restart_*` tests |
| risk-reducing cancels (TP-blocked branch) need a full reconciliation | `test_m3_scoped_startup_keeps_risk_reducing_cancels_under_another_tp_blocker` |
| only the "active ahead" audit check removed | `test_m4_audit_rebases_only_after_history_delivers_the_fill_the_active_row_shows` |
| only the active-list freshness audit check removed | `test_m4_audit_never_rebases_while_active_orders_are_unknown[1000/3]` |
| only the history-row ordering audit check removed | `test_m4_audit_refused_when_a_manual_trade_was_committed_after_the_position_read` |
| engine: no pre-bootstrap rebind | `test_l1_start_before_bootstrap_rebinds_a_changed_config[operator/launcher]` |
| executor: START_CONFIG_CHANGED latches | `CTL::test_l1_start_config_changed_makes_the_launcher_rebind_and_retry_the_baseline` |

Contract for WS-E (M1): render `summary.history_conflicts` (all version summaries are strings; `committed` marks
the ledger's version) and send `ack_history_conflict` with the viewed `summary.conflict_set_id` and, for keys whose
versions are all `committed: false`, `accepted: {<key>: <fingerprint>}`. The web normalisation currently drops both
fields, so a web ack is refused with `CONFLICT_SET_ID_REQUIRED` until WS-E passes them through (R7, since done).

M1 web parity (orchestrator follow-up; `codex/ng-web` @ `6fdf45d76` merged as the fast-forward to `d82f5992e`; red
`adcc254eb`, green `fa4d0289f`). The web accepts only opaque tokens `[A-Za-z0-9_:.-]{1,160}` for keys, fingerprints and
the set id, and the engine had published the raw typed-JSON dedupe label and the raw canonical-JSON payload
fingerprint. Canonical form, identical on both sides (the engine publishes it, the web echoes it verbatim):

* `key`: `trade:<trade_id>:<own_side>:<own_exchange_order_id>` / `order:<exchange_order_id>` (domain, account,
  market are fixed per engine); a part outside `[A-Za-z0-9_.-]` or a key over 160 chars becomes
  `<trade|order>:sha:<32 hex of the typed key>`; single-version entries are namespaced
  `store_conflict:<id>`, `manual_reconcile:reason`, `freeze:LEDGER_INVARIANT`, `active_evidence:<cell>` (unique keys).
* `fingerprint`: first 32 lowercase hex of sha256(UTF-8 of the scanner's canonical payload fingerprint,
  `history.trade_payload_fingerprint` / `order_payload_fingerprint`); single-version entries: 32-hex digest of
  their summary. The engine maps an `accepted` fingerprint back to the raw one (only among that key's versions).
* `conflict_set_id`: first 32 hex of sha256 over the canonical JSON of `[[stream, key, [fingerprints…]], …]`.
* `stream` values are the scanner's (`trades`, `inactive_orders`) plus the single-entry kinds above.

The web ack `{action, note, acknowledge, conflict_set_id, accepted, confirmation}` is accepted as is (extra fields
ignored) — `E test_ng_engine_review4.py::test_m1_web_parity_keys_fingerprints_and_the_normalized_ack_apply` runs
the real `web.neutral_grid.commands.CommandService` normalisation and conflict-set gate on a real engine snapshot
(one never-committed duplicate with the web's pick + one committed contradiction) and applies the normalized
payload; `::test_m1_web_parity_changed_set_and_empty_set_are_refused_by_the_engine` pins CONFLICT_SET_CHANGED and
NOTHING_TO_AUDIT for web-normalized payloads. Mutation "accepted fingerprint not mapped back" kills the first.

Semantics changed by this round (older tests updated): `resolve_unknown_submit` waits `unknown_resolution_delay_s`
(`test_ng_engine_crash.py::test_ac16_crash_after_dispatch_commit_is_unknown_not_proven_absent` retries until then); `ack_history_conflict`
carries the reviewed `conflict_set_id` (`review1::test_r16_ledger_invariant_freeze_survives_reload_and_restart_until_acknowledged`, `review3::test_c3_audited_payload_conflict_restores_completeness_and_stop_finishes_honestly`).

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
* **R5 (WS-E, optional).** Offer the launcher-path audited actions `retire_colliding_cid` and `migrate_grid`
  (`commands.EXTENDED_AUDIT_ACTIONS`) in the web UI, and a resume flow for `DURABLE_STOP_ACTIVE` (an explicit web
  START already resumes; the launcher needs the `RESUME <grid_id> AFTER STOP <stop_ms>` phrase).
* **R6 (WS-B, optional).** An audited store API to replace a committed payload/quantity (`LedgerCorrection` from
  the scanner) in one transaction; until then the engine audits accept only the committed version.
* **R7 (WS-E) — done** in `codex/ng-web` @ `6fdf45d76` (merged): the web passes `conflict_set_id`/`accepted` and
  renders `summary.history_conflicts`; parity pinned by the two `test_m1_web_parity_*` tests.
* **R4 (WS-C, optional).** `LighterExchangePort` does not forward `register/release_history_reconciled_order`; the
  executor calls the connector directly (the fake exchange implements the same names).

## Open risks

* Weight: with `poll_interval_s = 5` the steady-state cost (account 300 + active 300 + trades 600 + inactive 100
  per poll) is ~15 600/min, close to the 18 000 Standard pool that the Hummingbot connector's own polling also
  uses. The budget makes history go stale (entries blocked) rather than overspend; live tuning of
  `poll_interval_s`/`history_freshness_s` is needed before production.
* SUBMIT_UNKNOWN whose request never landed stays unresolved until an operator audits it (by design: no venue
  idempotence is documented); STOP then ends as STOP_UNCERTAIN. The audit ("never landed") is accepted only
  `unknown_resolution_delay_s` (default 120 s) after the dispatch, with no pending WS execution signal for the CID.
  Absence is never provable: a venue whose active list lags longer than that delay AND whose history shows neither
  a fill nor a terminal row for the order would let an operator audit a live order as "never landed" (a second TP
  may follow). This is an operator-audit residual risk (check the venue UI/export before acking); a later
  execution of that order is visible as lag and ends as LATE_FILL evidence (audited path).
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
* Rules refresh is bounded to what the poll/scan cadence leaves of the weight budget (at least one read per
  minute; ~2/min with the defaults), so `rules_refresh_s` below ~30 s is effectively capped; a venue rules change
  is picked up within that cadence (entries of off-tick/invalid cells stay blocked meanwhile).
* Anti-flap: after any DEGRADED blip new entries wait `normal_hysteresis_ticks` (3) clean ticks (TPs never wait).
* The release re-read after a fill commit costs one extra account read (300 weight) in such ticks; when the budget
  is short the release simply waits for the next regular poll.
* TP accumulation after a runtime minimum decrease applies to entries created by this version
  (`arming_min_tp`); older live entries keep the previous per-fill dispatch.
* The launcher START has no web preview: its `preview_id` is a digest of the confirmed grid id + B + executor id.
* A durable STOP is never resumed automatically: after any stop the launcher needs the per-stop resume phrase (or
  a web START). The Hummingbot-stop re-send targets the operator's own CLI intent only; a STOP that keeps
  conflicting (state churn) is re-sent every tick until applied.
* Baseline audits are refused while evidence is unsettled (retryable): an operator may need a few seconds and a
  second attempt after activity; the reason list says why.
* Grid migration needs fresh market data at apply time and a new grid id; the old grid stays in the ledger
  (RETIRED). The controller's default ledger moved from a per-grid-id file to the store's per-account/market
  path: a pre-existing per-grid file is not picked up automatically (set `db_path` explicitly to keep using it).
* If the launcher cannot read the ledger at construction (corrupt/unreadable DB), nothing is registered; the
  engine then fails closed, but Hummingbot's generic cancel paths could touch orders of a previous run until the
  connector restores its own tracking marker (logged as an error).

## Commands run (code at `fa4d0289f`; `PY=$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python`)

| Gate | Command | Result |
|---|---|---|
| WS-D tests | `$PY -m pytest test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine test/controllers/generic/test_neutral_grid.py -q` | 291 passed |
| Heavy property sweep | `NG_PROPERTY_SEEDS=60 NG_PROPERTY_STEPS=120 $PY -m pytest .../engine/test_ng_engine_properties.py -q` | 61 passed |
| Lighter connector | `$PY -m pytest test/hummingbot/connector/derivative/lighter_perpetual/test_lighter_perpetual_derivative.py -q` | 85 passed, 8 subtests passed |
| Committed neutral/risk | `$PY -m pytest test/scripts/test_lighter_robinhood_neutral_grid.py test/scripts/test_lighter_robinhood_grid_risk.py -q` | 73 passed |
| Controller/executor regressions | `$PY -m pytest test/hummingbot/strategy_v2/executors/grid_executor test/controllers/generic test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py test/hummingbot/strategy_v2/executors/test_executor_base.py -q` | 150 passed |
| Merged WS-A/B/C suites | `$PY -m pytest test/.../neutral_grid_executor/core test/.../neutral_grid_executor/store test/.../neutral_grid_executor/history test/hummingbot/connector/derivative/lighter_perpetual/test_lighter_perpetual_history_pagination.py -q` | 369 passed, 277 subtests passed |
| Web suite (WS-E, merged `d82f5992e`) | `$PY -m pytest test/web/neutral_grid -q` (incl. browser tests) | 149 passed |
| Round 4 web parity | `$PY -m pytest .../engine/test_ng_engine_review4.py -k parity` at `adcc254eb` / at `fa4d0289f` | 1 failed (keys not web-safe), 1 passed / 2 passed |
| Round 4 red/green | `$PY -m pytest .../engine/test_ng_engine_review4.py` + the new `CTL` tests at the `5f266ca83` code (tests of `eea457d5a`) / at `d0eef1804` | 23 failed (+2 intended passes) / all passed; mutations 16/16 killed (`mutate4.py`) |
| Round 3 red/green | `$PY -m pytest .../engine/test_ng_engine_review3.py test/controllers/generic/test_neutral_grid.py -q` at the `2fbb338e7` code / at `f654952ae` | 19 failed, 32 passed / 51 passed (+3 web-contract tests: red at `f48ed9490`, green at `99cb39468`) |
| Review package 2 red/green | `$PY -m pytest .../engine/test_ng_engine_review2.py test/controllers/generic/test_neutral_grid.py -q` at the `969003ef4` code / at `f96d33c58` | 15 failed, 29 passed / 44 passed |
| Review package 1 red/green | `$PY -m pytest .../engine/test_ng_engine_review1.py -q` at the `16b837428` engine / at `93c80b299` | 26 failed, 2 passed / 28 passed |
| Compile/import | `$PY -m py_compile <9 WS-D modules>` + import of engine/executor/fake_exchange/commands/snapshot/data_types/controller/script | OK |
| Lint | `$PY -m flake8 <WS-D modules> test/.../engine test/controllers/generic/test_neutral_grid.py` | clean |
| Whitespace | `git diff --check a18cb37d4 HEAD` | clean |
