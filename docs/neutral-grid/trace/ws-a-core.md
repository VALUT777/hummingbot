# WS-A core — traceability

Branch `codex/ng-core` (from `a18cb37d4`). Pure Python, no IO/asyncio/sqlite, `Decimal` for every price/quantity,
ids `int`/`str` only. Modules: `hummingbot/strategy_v2/executors/neutral_grid_executor/{grid,cells,risk,admission,router,cid,dust}.py`.

Test prefix used below: `T = test/hummingbot/strategy_v2/executors/neutral_grid_executor/core/`
(full node id = `T` + the shown suffix, e.g.
`test/hummingbot/strategy_v2/executors/neutral_grid_executor/core/test_cells.py::TestPartialEntry::test_ac05_partial_entry_2_3_5_with_floor_5`).

Run: `$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python -m pytest test/hummingbot/strategy_v2/executors/neutral_grid_executor/core -q`

Status legend: **done** = requirement fully proven at core level; **core part** = the pure logic is proven here,
orchestration/persistence/connector part belongs to another WS (see handoffs).

## Requirements → tests

| Req | Component | Test node ids (prefix `T`) | Status |
|---|---|---|---|
| NG-GRID-001 integer-tick grid, reject non-multiple/collapse/T<N | `grid.build_grid`, `build_grid_ticks`, `to_ticks` | `test_grid.py::TestIntegerTickGrid::test_sample_explicit_arithmetic_with_tick_0_0001`; `::test_non_multiple_bounds_are_rejected_not_quantized`; `::test_collapse_and_insufficient_ticks_are_rejected`; `::test_bad_types_are_rejected` | done |
| NG-GRID-002 anchor clamp, first side, fixed prices | `grid.compute_anchor`, `assign_cells`, `contracts.CellSpec` | `test_grid.py::TestIntegerTickGrid::test_anchor_is_clamped_to_bounds`; `::test_cell_sides_entry_and_tp_prices`; `test_cells.py::TestNormalCycles::test_buy_cell_cycle_rearms_buy_at_same_fixed_prices`; `test_properties.py::TestCoreProperties::test_random_orderings_preserve_invariants` (fixed side/price under random mid moves) | done |
| NG-GRID-003 Q step multiple, never round up, fees ignored, DUST, same side+target aggregation with per-cell allocation in the ledger | `grid.validate_config`, `quantize_down/up`, `grid.partition_valid`, `cells.tp_obligation_to_dispatch`, `cells.add_tp_intent(allocation=)`, `cells.refresh_dust`, `dust.*` | `test_grid.py::TestValidateConfig::test_q_not_step_multiple_is_rejected`; `test_grid.py::TestIntegerTickGrid::test_quantize_never_rounds_quantity_up`; `test_cells.py::TestPartialEntry::test_step_remainder_is_held_while_entry_live_and_never_rounded_up`; `test_cells.py::TestReleaseAndDust::test_ac33_dust_is_durable_visible_and_blocks_reset`; `test_cells.py::TestReleaseAndDust::test_max_base_split_is_balanced_so_no_avoidable_dust`; `test_dust.py::TestGrouping::*`; `test_dust.py::TestLedgerAggregateTp::*` | done |
| NG-ORD-001 no MARKET, allowed subset, post-only support | `grid.validate_order_types`, `cells.submit_request` | `test_grid.py::TestValidateConfig::test_market_and_unsupported_order_types_are_forbidden`; `test_cells.py::TestNormalCycles::test_market_order_type_cannot_be_built` | core part (GTT renewal timing: WS-D) |
| NG-ORD-002 virtual cells, `reduce_only=false` | `cells.submit_request` | `test_cells.py::TestNormalCycles::test_submit_request_is_exact_and_never_reduce_only`; `test_risk.py::TestCaps::test_ac27_virtual_tp_crosses_zero_non_reduce_only` | done |
| NG-ORD-003 self-trade router, TP priority, TP–TP FIFO, no netting | `router.plan_submits`, `router.crosses` | all of `test_router.py::TestSelfTrade::*`, `test_router.py::TestFifo::*` | done |
| NG-CELL-001 whole-cell lock, release only when (1)–(5) | `cells.CellLedger.can_release/release` | `test_cells.py::TestReleaseAndDust::test_release_requires_each_ng_cell_001_condition`; `::test_ac33_dust_is_durable_visible_and_blocks_reset`; `test_cells.py::TestTerminalPartialEntryAndLateFills::test_late_fill_after_release_goes_to_old_cycle_and_requires_audit`; property test (closed cycle ⇒ E=X, all legs final, no dust) | core part ((5) position reconciliation is an input from WS-D) |
| NG-CELL-002 partial entry, TP per confirmed share, ENTRY_LIVE+TP_LIVE, E/X buckets, invariant, unknown TP keeps reservation | `cells.apply_fill`, `tp_obligation_to_dispatch`, `add_tp_intent`, `Cycle.buckets`, `check_invariants` | `test_cells.py::TestPartialEntry::test_ac05_partial_entry_2_3_5_with_floor_5`; `::test_entry_live_and_tp_live_are_simultaneous_leg_states`; `::test_normal_partial_fill_never_cancels_entry`; `test_cells.py::TestUnknownOutcomes::test_unknown_tp_keeps_reservation_and_no_duplicate_tp`; property test (bucket partition + invariant after every event) | core part (≤2 s dispatch SLO: WS-D) |
| NG-CELL-003 partial TP, terminal partial entry, late fill after cancel | `cells.confirm_terminal`, `apply_fill`, `acknowledge_late_evidence` | `test_cells.py::TestPartialTp::*`; `test_cells.py::TestTerminalPartialEntryAndLateFills::*`; `test_cells.py::TestLateEvidenceResolution::*` | done |
| NG-CELL-004 distinct durable states, concurrent legs | `OrderState` per leg, `cells.state_flags/primary_state`, `_TRANSITIONS` | `test_cells.py::TestPartialEntry::test_entry_live_and_tp_live_are_simultaneous_leg_states`; `test_cells.py::TestUnknownOutcomes::test_illegal_transitions_raise`; `::test_restart_marks_intent_unknown_and_keeps_cid` | done |
| NG-RISK-002 P, P_min/P_max, gross, caps, TP priority, RISK_BLOCKED only when confirmed + non-entry orders exceed cap | `risk.endpoints*`, `check_submit`, `obligation_totals`, `with_obligations`, `plan_tp_headroom`, router | `test_risk.py::TestEndpoints::*`; `test_risk.py::TestCaps::*`; `test_risk.py::TestTpHeadroom::*` (incl. `::test_submit_unknown_same_side_entry_is_pending_not_risk_blocked`); `test_router.py::TestCapsAndHeadroom::*`; `test_router.py::TestTpPriorityHeadroom::*`; property test | done |
| NG-RISK-004 margin advisory, unknown data blocks | `risk.margin_advisory`, `required_margin_estimate`, `exposure_blockers` | `test_risk.py::TestMarginAdvisory::*` | core part (fetching/freshness: WS-D) |
| NG-RISK-005 bootstrap full-Q check, slots formula, slot ledger, head-of-line queue, runtime floors, effective cap reduction | `grid.validate_full_q`, `admission.*`, `router.SlotBudget` | `test_grid.py::TestValidateConfig::test_full_q_below_minimum_at_any_entry_or_tp_price_is_rejected_per_cell`; all of `test_admission.py::*` (incl. `TestHeadOfLine`, `TestCapReduction`); `test_router.py::TestTpCapacity::*`; property test (random cap drops) | done |
| NG-HIST-004 CID allocation logic (incl. latch on malformed durable row) | `cid.CidAllocator`, `validate_cid` | all of `test_cid.py::TestCid::*` | core part (durable map/high-water in same tx: WS-B) |

## Acceptance scenarios → tests

| AC | What is proven here | Test node ids (prefix `T`) | Status |
|---|---|---|---|
| AC-01 / AC-02 | BUY cell: BUY@low → SELL TP@high → next cycle BUY; SELL cell mirrored (ledger level) | `test_cells.py::TestNormalCycles::test_buy_cell_cycle_rearms_buy_at_same_fixed_prices`; `::test_sell_cell_cycle_rearms_sell` | core part (history/GTT: WS-D) |
| AC-03 | N+1 lines, 22/33 sample sides (ticks 0.01…0.00001, explicit arithmetic at 0.0001), non-multiple bounds/step and full-Q-below-minimum at any entry or TP price rejected; fixed prices under mid moves | `test_grid.py::TestIntegerTickGrid::test_sample_5_6_n55_anchor_5_4_gives_56_lines_22_buy_33_sell_for_every_plausible_tick`; `::test_sample_explicit_arithmetic_with_tick_0_0001`; `test_grid.py::TestValidateConfig::test_full_q_below_minimum_at_any_entry_or_tp_price_is_rejected_per_cell`; `::test_q_not_step_multiple_is_rejected`; `test_admission.py::TestSimultaneousPartialFills::test_ac57_many_minimum_partial_fills_use_reserved_slots_no_cancel_and_queue_progresses` (re-armed cells keep fixed price after mid move) | done |
| AC-05 | 2 held below-min, +3 → TP 5, +5 → TP 5, total 10, allocation kept | `test_cells.py::TestPartialEntry::test_ac05_partial_entry_2_3_5_with_floor_5` | done |
| AC-06 | partial TP never unlocks/reposts, exact target kept | `test_cells.py::TestPartialTp::test_ac06_partial_tp_does_not_unlock_and_keeps_exact_target` | done |
| AC-07 | canceled partial entry closes exactly actual qty, next cycle full Q | `test_cells.py::TestTerminalPartialEntryAndLateFills::test_ac07_terminal_partial_entry_closes_actual_qty_then_full_q` | done |
| AC-08 | cancel event does not unlock; late history fill adds obligation to same cycle, covered by TP; no terminal without history | `test_cells.py::TestTerminalPartialEntryAndLateFills::test_ac08_late_fill_after_cancel_event_increases_obligation_of_same_cycle` | core part (WS-D owns end-to-end) |
| AC-16 (ledger part) | restart turns INTENT into SUBMIT_UNKNOWN, keeps CID, cell stays locked | `test_cells.py::TestUnknownOutcomes::test_restart_marks_intent_unknown_and_keeps_cid` | core part |
| AC-21 (ledger/risk part) | cancel timeout keeps order, full remainder, slot; cell locked | `test_risk.py::TestEndpoints::test_cancel_timeout_keeps_full_remainder_reserved_and_cell_locked` | core part |
| AC-22 (client id part) | CID int within 48 bits, no coercion/truncation | `test_cid.py::TestCid::test_validate_never_truncates_hashes_or_coerces` | core part |
| AC-23 | never round up; fees not an input to base quantity | `test_cells.py::TestIdempotencyAndConflicts::test_fees_are_not_an_input_to_base_quantities`; `test_cells.py::TestPartialEntry::test_step_remainder_is_held_while_entry_live_and_never_rounded_up`; `test_grid.py::TestIntegerTickGrid::test_quantize_never_rounds_quantity_up` | done |
| AC-24 | BUY rejected when `P_max` would exceed +cap (also inside a router plan) | `test_risk.py::TestCaps::test_ac24_pending_buy_rejected_when_p_max_exceeds_cap`; `test_router.py::TestCapsAndHeadroom::test_entries_respect_net_and_gross_caps_across_the_plan` | done |
| AC-25 | SELL rejected when `P_min` would go below −cap; shorts inside cap work | `test_risk.py::TestCaps::test_ac25_pending_sell_rejected_when_p_min_below_minus_cap_shorts_within_cap_work`; `test_router.py::TestCapsAndHeadroom::test_entries_respect_net_and_gross_caps_across_the_plan` | done |
| AC-26 | offsetting cells at venue net 0 still count gross | `test_risk.py::TestCaps::test_ac26_gross_cap_with_venue_net_zero` | done |
| AC-27 | SELL TP of long cell at net 0 → net short, non-reduce-only, ledger consistent | `test_risk.py::TestCaps::test_ac27_virtual_tp_crosses_zero_non_reduce_only` | done |
| AC-28 | B = 0 → [−330, +220]; B = 330 → [0, +550]; B = −200 accepted; no seed/TP for B | `test_risk.py::TestEndpoints::test_sample_reachable_intervals_for_baselines`; `test_grid.py::TestPreview::test_sample_preview_counts_range_and_slots`; `::test_preview_with_baseline_330` | done (bootstrap confirmation: WS-D) |
| AC-30 (risk part) | drift beyond caps detected as hard-risk conflict | `test_risk.py::TestCaps::test_cap_violations_detect_drift` | core part |
| AC-31 | every submit checked vs live/pending/unknown; conflicting entry cancel-requested once, TP waits for history terminal; sample-grid geometry | `test_router.py::TestSelfTrade::test_ac31_tp_cancels_conflicting_entry_and_waits_for_history_terminal`; `::test_all_live_pending_and_unknown_states_are_checked`; `::test_sample_grid_geometry_tp_of_lower_buy_cell_vs_resting_upper_buy_entry`; `::test_unsent_entry_intent_is_withdrawn_unknown_submit_is_only_waited_for` | done |
| AC-33 | dust visible, durable (record round trip), blocks reset, merge only same side+target | `test_cells.py::TestReleaseAndDust::test_ac33_dust_is_durable_visible_and_blocks_reset`; `::test_dust_merges_with_returned_tp_remainder_same_side_and_target`; `test_dust.py::TestDustVisibility::test_ac33_collect_shows_durable_dust_per_cell_cycle`; `test_dust.py::TestGrouping::*` | done |
| AC-34 | higher floor blocks new entries/invalid submits without resize, TP blocker visible, full Q invalid only at the TP price refused by admission and ledger; lower floor grows exit reservations before entries; runtime `max_base` split balanced | `test_admission.py::TestRuntimeMinimumChange::*` (incl. `::test_ac34_runtime_floor_invalid_only_at_tp_price_blocks_arming`); `test_cells.py::TestReleaseAndDust::test_max_base_split_is_balanced_so_no_avoidable_dust` | done |
| AC-37 | finite shortfall → warning only; unknown/NaN/negative/float/str → block | `test_risk.py::TestMarginAdvisory::test_ac37_known_shortfall_warns_only`; `::test_ac37_unknown_or_malformed_blocks`; `::test_exposure_gate_unknown_data_blocks_margin_shortfall_only_warns` | core part |
| AC-38 (core part) | no MARKET policy/fallback can be built | `test_cells.py::TestNormalCycles::test_market_order_type_cannot_be_built`; `test_grid.py::TestValidateConfig::test_market_and_unsupported_order_types_are_forbidden` | core part |
| AC-39 | one aggregate TP leg in `CellLedger` (per-generation allocation persisted in `to_record`, shown in `to_view`) splits partial fills exactly between cycle obligations, idempotent, order independent, closes and releases | `test_dust.py::TestLedgerAggregateTp::test_ac39_ledger_dispatches_one_aggregate_and_splits_fills_exactly`; `::test_aggregate_intent_is_validated_against_each_cycle`; `::test_aggregate_leg_is_visible_and_checked_by_invariants`; `test_properties.py::TestCoreProperties::test_aggregate_tp_random_splits_are_exact_and_idempotent`; helper level: `test_dust.py::TestAggregateFills::*`, `test_dust.py::TestGrouping::*` | done |
| AC-40 (ledger part) | same key different payload, overfill, unknown leg, trade cumulative > order cumulative → conflict, not applied | `test_cells.py::TestIdempotencyAndConflicts::*` | core part |
| AC-42 (ledger part) | late evidence after reuse attributed to old cycle, flags audit, blocks new cycle; after the audit an ordinary TP closes it without re-latching and the cell keeps cycling | `test_cells.py::TestTerminalPartialEntryAndLateFills::test_late_fill_after_release_goes_to_old_cycle_and_requires_audit`; `test_cells.py::TestLateEvidenceResolution::test_resolution_tp_fills_do_not_relatch_and_later_cycles_release`; `test_cells.py::TestLateEvidenceResolution::test_audited_execution_of_a_rejected_leg_becomes_terminal`; `test_properties.py::TestCoreProperties::test_late_evidence_is_resolved_by_ordinary_tps_and_never_latches` | core part (settlement/scan: WS-C/D) |
| AC-43 (logic) | monotonic 48-bit CID, same identity same CID (restart via durable lookup), collision/exhaustion/malformed durable row fail closed and latch | `test_cid.py::TestCid::*` | core part (durable: WS-B) |
| AC-44 | full or reduced cap: stale reservations trimmed, TP never sent over the cap, cancels one cap-consuming entry (unfilled, farthest), waits, no spam; own reservation first; TP–TP FIFO; a slot-starved TP does not starve a crossing TP that owns a slot | `test_router.py::TestTpCapacity::*`; `test_admission.py::TestCapReduction::*`; `test_router.py::TestFifo::test_tp_tp_conflicts_are_fifo_and_later_ones_cannot_overtake` | done |
| AC-46 (data part) | preview numbers: 56/55, 22/33, 40 armed/15 queued, slots 120/0, reachable interval | `test_grid.py::TestPreview::*` | core part (UI: WS-E) |
| AC-51 (ledger part) | renewal only after proven terminal, exact remainder, same target, no duplicate | `test_cells.py::TestPartialTp::test_tp_terminal_with_remainder_renews_exact_remainder_same_target` | core part |
| AC-56 (ledger part) | NOT_SENT only for unsent zero-fill intent; definitive reject only zero fill; UNKNOWN keeps reservation, no new CID/revision | `test_cells.py::TestUnknownOutcomes::test_rejections_require_zero_fill_and_proper_state`; `::test_unknown_tp_keeps_reservation_and_no_duplicate_tp`; `::test_zero_fill_rejected_entry_gets_new_revision_in_same_generation` | core part |
| AC-57 | many minimum partials in one batch: all TPs from reserved slots, no cancels, no oversubscription, steady state no spam, queue head progresses deterministically; head-of-line with heterogeneous slot needs (no skip-ahead starvation) | `test_admission.py::TestSimultaneousPartialFills::test_ac57_many_minimum_partial_fills_use_reserved_slots_no_cancel_and_queue_progresses`; `test_admission.py::TestHeadOfLine::test_head_needing_more_slots_blocks_cells_behind_and_arms_first` | done |
| Property tests, late evidence | late executions of final entries (incl. released cycles) → audit → resolved by ordinary TPs; no permanent latch; cells cycle again | `test_properties.py::TestCoreProperties::test_late_evidence_is_resolved_by_ordinary_tps_and_never_latches` | done |
| Property tests (spec §13.3) | `P_min ≤ actual ≤ P_max`, window never widens, caps incl. owed exits never violated, never RISK_BLOCKED without external cause, actual orders never exceed the effective cap under random cap drops (no submit at/above cap, unused reservations ≤ cap − actual), monotone confirmations, no reset with obligation/unknown/dust, idempotent replay + exact round trip, fixed prices, no MARKET/reduce-only, drain liveness | `test_properties.py::TestCoreProperties::test_random_orderings_preserve_invariants` (60 seeds × 70 steps by default; `NG_CORE_PROPERTY_SEEDS=400 NG_CORE_PROPERTY_STEPS=150` also run) | done |

## Design decisions

- **Sample tick.** The 22/33 split does not depend on the tick as long as `T ≥ N`: `floor(i·T/55) < 0.4·T ⇔ i < 22`
  for T = 100…100000, and `P[22]` equals the anchor 5.4, so cell 22 is SELL (the `P[i] < anchor` rule).
  The explicit test uses tick `0.0001` (T = 10000; cell widths 181/182 ticks).
- **Exact arithmetic.** Multiple-of-step, floors and `min_valid_TP_qty` are computed with `fractions.Fraction`, not
  Decimal-context division, so a boundary cannot be moved by rounding. `quantize_up` is only used for floors.
- **Aggregate state is derived.** Each leg has its own `OrderState`; `state_flags()` returns every cell state that
  holds (e.g. `{ENTRY_LIVE, TP_LIVE}`). `SUBMIT_UNKNOWN`/`CANCEL_UNKNOWN` show up as `ENTRY_INTENT`/`TP_INTENT` and
  `*_LIVE` flags; the difference stays visible in the leg state (`to_view()`).
- **TERMINAL only through `confirm_terminal`**: the ledger itself enforces venue cumulative = applied executions
  (`MISSING_FILLS` while trades lag, `CONFLICT` if applied > cumulative). The caller asserts exact row + full
  scan + settlement. A history execution on an INTENT/SUBMIT_UNKNOWN leg promotes it to LIVE (it proves acceptance).
- **TP dispatch.** The whole unassigned obligation (floored to step) is dispatched as soon as it is ≥
  `min_valid_TP_qty` (split by `max_base` if needed); below-min stays pending while the entry may fill and becomes
  DUST only once every entry revision is final. DUST merges with later returned TP remainders of the same cycle.
- **TP priority on net headroom (found by the property sweep).** Entries are admitted against order endpoints *plus
  owed exits* (`risk.obligation_totals` → `router.plan_submits(owed=...)`). Without it, a filled SELL entry frees
  `P_max` room that BUY entries can take, and the owed BUY TP later becomes RISK_BLOCKED. The TP check itself
  uses the spec formula (orders only) and, when it fails (drift/cap change), cancels same-side entries
  farthest from mid first; if confirmed position leaves no room → `BLOCKED` (RISK_BLOCKED). A RISK_BLOCKED TP no
  longer holds the TP FIFO head (another exit may be what frees the room); entries never cross it.
- **Slots.** Arm-time reservation is `1 + ceil(Q/min_valid)`. For open cycles the engine may pass
  `slot_need_from_ledger` (non-final legs + future TP children; an entry fully executed per history cannot fill
  again), so finished entries return their slot. Open cycles are reserved before any entry; idle cells are armed
  head-of-line by (distance of fixed entry price to mid, cell_id).
- **Router semantics.** Candidates are *pre-commit* (SUBMIT ⇒ the engine commits intent+CID+reservation, then
  sends). Only LIVE entries are cancel-requested; an unsent INTENT entry is WITHDRAWN (NOT_SENT); SUBMIT_UNKNOWN
  and cancel-in-flight orders are waited for. TPs and unknown-role orders are never cancelled by the router.
- **Late evidence.** An execution on a leg already proven final is `LATE_EVIDENCE` (applied to that leg's own
  cycle, audit flag set, new cycles blocked). `acknowledge_late_evidence(gen)` (operator audit) makes the executed
  quantity the proven cumulative (a "rejected" leg that executed becomes TERMINAL); the remaining obligation is
  closed by ordinary TP legs on that cycle, whose fills are normal. `can_release` names every blocker (DUST,
  LATE_EVIDENCE_AUDIT, OLD_CYCLE_OBLIGATION) even when the cell has no current cycle.
- **Dust geometry and aggregation.** With fixed sides no two different cells share (TP side, target) (asserted in
  `test_dust.py::TestGrouping::test_geometry_no_two_cells_share_tp_side_and_target`), so aggregation only ever
  concerns several cycles of one cell (late evidence re-opening an old cycle). The ledger supports it: a TP leg
  may carry `allocation` (generation → share), hosted by the newest allocated cycle; each cycle's X, buckets,
  reservations and invariants use its share, fills split by water-filling (idempotent, order independent).
- **Balanced splits.** `grid.partition_valid` splits an obligation into step-multiple chunks in
  `[min_valid, max_base]`, balanced so an exact split is used whenever one exists (10 with max 6/min 5 → 5+5);
  only the unavoidable remainder is pending/DUST.
- **Effective cap reduction.** `admission.plan` trims unused reservations when `cap − actual` no longer covers them
  (entry-side spare slots first, exit-only cells last, farthest first) and reports `shortfall`;
  `router.SlotBudget.headroom = cap − actual` is a hard ceiling, so a stale reservation can never put an order
  over the cap. A TP without a slot uses the AC-44 path (cancel one LIVE entry, wait for history terminal).
  TPs that cannot be placed for a non-conflict reason (RISK_BLOCKED, no obtainable slot) do not hold the TP FIFO
  head (the cap-drop property sweep found that deadlock); entries never cross them.
- **Unknown submits in TP headroom.** Same-side SUBMIT_UNKNOWN entries count as pending headroom (WAIT); only
  confirmed position + non-entry orders beyond the cap produce RISK_BLOCKED.

## Additive core API changes after the first stable API

`CellAdmission.entry_live` (default `False`), `SlotBudget.headroom` (default `None`), `TpDispatchItem.allocation`
(default `None`), `Leg.allocation` (default `None`), `add_tp_intent(..., allocation=None)`,
`router.plan_submits(..., owed=(0, 0))`, `grid.partition_valid`, `risk.obligation_totals` / `with_obligations`,
`acknowledge_late_evidence` now returns the corrected leg ids. No existing signature or default changed.

## Contract changes (`contracts.py`)

- `GridConfig.fingerprint()` body now delegates to `grid.config_fingerprint` (it was documented "Implemented by
  core" and raised `NotImplementedError`). No field, enum member or signature changed. Pre-existing flake8 E704
  warnings in `contracts.py` (protocol stubs) are untouched.

## Handoffs

**WS-D (engine)**
1. Pass `owed=risk.obligation_totals(ledgers)` to `router.plan_submits`; the default `(0, 0)` exists only for
   compatibility and loses TP priority on net headroom.
2. Router TP candidate `seq` must be stable across ticks and persisted (e.g. commit sequence of the history fill
   that created the obligation); otherwise TP–TP FIFO is not FIFO.
3. On `SUBMIT`: `ident = ledger.next_entry_identity()` / `next_tp_identity(gen)` → `cid = allocator.allocate(ident)`
   → `begin_entry` / `add_tp_intent` + outbox + reservation in one store transaction, then transport, then
   `record_transport`. On `CANCEL`: `set_state(CANCEL_PENDING)` in the cancel-intent transaction. On `WITHDRAW`:
   `record_transport(NOT_SENT)` only with proof transport was never invoked. On `BLOCKED`: engine → RISK_BLOCKED.
4. History batch: `apply_fill(trade.dedupe_key(domain), identity_from_cid, size, price, own_side)`;
   `CONFLICT_*`/`UNKNOWN_LEG` → pause/freeze; `LATE_EVIDENCE` → FROZEN (audit, `acknowledge_late_evidence`);
   `confirm_terminal` only after exact terminal row + full scan + settlement delay + repeat scan; `MISSING_FILLS`
   → keep waiting; `CONFLICT` → freeze.
5. Each tick: `refresh_dust(rules)` (persist), admission with `CellAdmission(slot_need=slot_need_from_ledger(...),
   entry_live=<any non-final entry in the cell's open cycles>)` for open cycles, `SlotBudget.from_plan(plan)` for
   the router (never hand-built without `headroom`), aggregate items dispatched with
   `add_tp_intent(item.qty, cid, rules, generation=item.generation, allocation=item.allocation)`, persisted `plan.reservations`, entries only for `plan.newly_armed`, `entries_allowed=False` for
   pause/drift/unknown data/out-of-bounds/stop; release via `can_release(position_reconciled)` (NG-CELL-001 (5)).
6. Restart: `mark_intents_unknown()` unless the store proves the transport call never happened (AC-15/16).
7. NG-RISK-004 gate: `risk.exposure_blockers(ExposureInputs(...))`; margin via `required_margin_estimate` +
   `margin_advisory` (warning only when both are known).
8. Cross-side TP–TP conflicts are universal (every BUY-cell TP ≤ anchor line crosses every SELL-cell TP ≥ it);
   FIFO serialization can delay exits in fast two-sided moves. Surface `WAIT_TP_FIFO` + queue age (AC-14).
9. `CellLedger.to_view()` matches the CONTRACTS cell schema (plus `state_flags`, `unassigned`); the snapshot adds
   `blocker`/`queue_age_s`. `grid.build_preview` gives the AC-46 preview numbers.

**WS-B (store)**
1. Persist `CellLedger.to_record()` (JSON, Decimals/ids as strings, fill keys as nested tuples of str/int) or an
   equivalent schema that keeps `cycle.dust`, `late_evidence`, leg `seq`, `terminal_cumulative`, aggregate leg
   `allocation` (generation → share), and every fill key.
2. CID: durable `(identity → cid)` map and high-water written with the intent; give the allocator
   `lookup=identity→cid` and `exists=cid→bool`; a latched `CidError` must drive the engine to DEGRADED.

**WS-C (connector/history)**
1. Fill keys are `ExchangeTradeRow.dedupe_key(domain)` tuples; leg attribution only via CID/exchange id mapping,
   never price/size/time. Self-trade two-leg rows are two distinct keys (different `own_side`).

## Open risks

1. **Tail DUST from immediate dispatch.** Q = 10, floor 5, fills 2, 4, 4: TP 6 is dispatched at the second fill
   (spec: dispatch as soon as valid), the last 4 becomes DUST and the cell stays locked until an operator acts.
   Holding back would violate the dispatch rule; needs a spec decision if unacceptable.
2. **DUST locks the cell indefinitely.** The spec requires visibility and reset blocking but defines no operator
   resolution; only a rules change (lower floor) or a merge with returned TP remainder can clear it automatically.
3. Entry admission with owed exits is stricter than the spec's order-only `P_max` formula (a conservative
   superset); at idle the preview numbers are unchanged.
4. Late evidence after a remainder was already re-dispatched can make `X + reserved > E`; the ledger records the
   truth, `check_invariants()` reports it, and the engine must freeze (no automatic repair; the audit
   acknowledgement corrects cumulative values but cannot undo an over-exit).
5. `hypothesis` is not installed in the prepared env; property tests are seeded randomized loops (fast default
   in CI, heavier sweep run manually and documented below).

## Independent reviews

1. Read-only review (separate agent) against spec §§2–6: one defect, fixed in `b5cd6c292` — fills of the
   resolution TP of a released cycle were classified as late evidence and re-latched the audit flag forever.
2. Orchestrator adversarial review of `c2b7c6ed9` (8 confirmed): #1 = the same late-evidence defect (fixed in
   `b5cd6c292`; `test_resolution_tp_fills_do_not_relatch_and_later_cycles_release` covers late fill → ack →
   resolution TP fills → release → next entry with `check_invariants() == []`). #2 cap drop, #3 SUBMIT_UNKNOWN
   headroom, #4 greedy `max_base` split, #8 CID latch fixed in `0bbf71c76` (each test failed before the fix;
   the cap-drop sweep additionally found and fixed the slot-starved TP FIFO deadlock). #6/#7 test gaps closed in
   `35b895fd8` (the new tests kill the reviewer's FULLQ_ENTRY_ONLY, BEGIN_ENTRY_ENTRY_ONLY and SKIP_AHEAD_QUEUE
   mutations). #5 AC-39 implemented in the ledger in `4311049fa` (end-to-end and randomized tests).

## Commands run (latest)

- `python -m pytest test/hummingbot/strategy_v2/executors/neutral_grid_executor/core -q` → 127 passed, 177 subtests
- `NG_CORE_PROPERTY_SEEDS=400 NG_CORE_PROPERTY_STEPS=150 python -m pytest …/core/test_properties.py -q` → 3 passed, 1000 subtests
- `python -m pytest test/hummingbot/strategy_v2/executors/neutral_grid_executor test/hummingbot/strategy_v2/executors/grid_executor -q` → 137 passed (no package clash)
- `python -m flake8 hummingbot/strategy_v2/executors/neutral_grid_executor/{grid,cells,risk,admission,router,cid,dust}.py test/hummingbot/strategy_v2/executors/neutral_grid_executor/core/` → clean
- `git diff --check` → clean
