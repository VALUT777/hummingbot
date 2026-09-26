# Neutral Grid External Entry Adoption Design

## Objective

Atomically extend the active `lit-neutral-fixed-v3` grid from `4.9–5.9` to `4.8–5.9` and adopt the operator's
already executed manual 100 LIT BUY as generation 1 of new cell `10` (`4.8–4.9`). The resulting ledger must keep
the original baseline `B=0`, explain the venue position `P=500`, preserve the four existing 100 LIT BUY cycles and
their TPs at `5.0`, `5.1`, `5.2`, and `5.3`, and create a 100 LIT TP at `4.9` for cell `10`.

No synthetic client ID, owned leg, exchange order, or fill may be created. The manual BUY is represented by its
real exchange history evidence. The operation remains stopped after commit; START is a separate reviewed action.

The real manual exchange order ID and trade IDs are deliberately absent from this document. The engine discovers
them from the complete unmatched history after native access is restored and binds them into the candidate proof.
The operator cannot type or override those identifiers in the command.

## Why existing operations are insufficient

`baseline_audit` would reconcile the extra 100 LIT by changing the effective baseline to `B=100`. It does not
create a cycle, so the extra inventory would have no TP at `4.9`. `settle_external_close` only applies a fully final
reduce-only order to existing open obligations while the observed position is flat. It cannot create a long cycle.

The existing `extend_grid` candidate also requires venue position to equal the current ledger position. After the
manual BUY, venue position is 500 while the old ledger explains 400, so extension and adoption must be one atomic
operation. A temporary baseline rebase followed by later reclassification is prohibited.

## Operator contract

Add one `baseline_audit` action, `extend_grid_with_external_entry`. Its request contains only:

```json
{
  "action": "extend_grid_with_external_entry",
  "proof_id": "<64 lowercase hexadecimal characters>",
  "note": "<non-empty operator note>",
  "acknowledge": true,
  "confirmation": "ADOPT EXTERNAL BUY 100 LIT INTO lit-neutral-fixed-v3 CELL 10 TP 4.9"
}
```

The server derives the target config, cell, quantity, order ID, trade IDs, prices, and history inbox IDs. The
request cannot supply any accounting value. Normal command idempotency applies, and the proof is unique in the
adoption table.

The snapshot publishes `summary.grid_external_entry_candidate` with `proof_id`, `blockers`, source and target
windows and full config payloads, the added cell, proposed cycle, retained obligations, old ledger position,
observed venue position, manual order, all trade fragments, coherent-cut timestamps/high-water marks, and the
post-commit ledger/risk preview. Web review shows these values and submits only the proof-bound request above.

## Candidate and execution gates

The preview and command execution independently require all of the following. The command re-reads every value in
the same transaction and rejects a stale proof.

- The engine has a durable `STOPPED` or `STOPPED_WITH_INVENTORY` outcome. There are no active own or foreign
  orders, pending WebSocket executions, unresolved outbox items, reservations, history conflicts, retention gaps,
  late evidence, dust, or non-final legs.
- Position, active-order, and complete history reads form the same stable causal cut: position postdates every
  committed fill/history row, active orders are at least as new as position, history lag is zero, and a complete
  walk began after the position had remained stable for the settlement delay.
- The source and target satisfy the generic add-only extension rules. For this reviewed case the stored source is
  grid `lit-neutral-fixed-v3`, window `4.9–5.9`, ten 100 LIT cells, baseline `0`, and anchor `5.23215`; the target is
  `4.8–5.9` with eleven cells. Grid ID, pair, connector/domain, account, quantity, exact step, anchor, leverage,
  baseline, and financial caps are unchanged. Old boundaries are an exact contiguous slice of the target. As in
  the existing extension contract, the proof may bind an operational `max_active_orders` change (the live durable
  source revision stores 120 while the approved runtime target is 210); it must not rewrite the source revision or
  permit any other policy change.
- The candidate derives the sole missing interval, next stable cell ID, side, generation, Q, and TP from the source
  ledger and target config; none is hardcoded in runtime logic. In this case those derived values are cell `10`,
  interval `4.8–4.9`, BUY entry, SELL TP, generation 1, Q 100, and TP `4.9`. The target has no prior cell, cycle,
  leg, fill, reservation, or adoption record.
- The four retained cycles are proof-bound by cell ID, generation, entry quantity, exit quantity, external
  settlement quantity, open obligation, and TP price. They total 400 LIT at TPs `5.0–5.3`.
- There is exactly one unresolved manual exchange order for this account and market that explains the position
  delta. It is BUY, non-reduce-only, fully terminal, filled exactly 100 LIT, absent from active orders, and has one
  terminal order row. Every associated trade fragment is present, BUY, belongs to that order, totals exactly 100,
  and is selected; no partial or ambiguous evidence set is allowed. Every fill price must be at or below the cell's
  `4.8` entry price. The evidence has not been resolved or used by another audit/adoption.
- The old ledger position is exactly 400 and the coherent observed position is exactly 500. The proposed external
  BUY makes the post-commit ledger position exactly 500 without changing `B=0`.
- Post-adoption current gross is 500 and the five SELL TPs pass net and slot checks. The normal admission engine,
  rather than this command, controls later empty-cell entries under the unchanged caps.

Any extra manual order, missing fragment, price above 4.8, quantity mismatch, position change, new history row,
config/revision change, or evidence ambiguity changes the proof or adds a blocker. The operator must review a new
candidate.

## Schema and accounting

Schema v8 adds append-only `external_entries`, `external_entry_evidence`, and shared
`manual_evidence_claims` tables. The shared claims prevent an inbox row from being used by both adoption and
external-close settlement; migration backfills claims for prior settlements.

`external_entries` stores a unique proof ID; grid/cell/generation; entry side and quantity; observed position;
expected config and engine revisions; position, active-order, and history-scan timestamps/high-water marks; actor,
reason, audit-event ID, and creation time. It has a foreign key to the target cycle and permits one adoption for a
cycle. `external_entry_evidence` maps unique history inbox rows to that adoption as `TRADE` or `TERMINAL_ORDER`,
with allocated quantity on trade rows. All three tables have update/delete rejection triggers. Opening a database
whose applied v8 schema lacks any table, required column, constraint, or trigger fails closed.

The atomic store operation performs these writes in one transaction:

1. Revalidate source/target config, clean durable stop, evidence, expected revisions, proof uniqueness, and all
   adoption invariants before the first write.
2. Append the config/window revision and immutable cell `10` using the existing extension rules.
3. Open cell `10` generation 1 with planned amount 100 and config revision v8's target revision.
4. Append the external entry and evidence mappings, resolve the selected inbox rows to this adoption, credit the
   cycle's `entry_filled` by 100, and place the cell in its exit-obligation state.
5. Append one audit event containing the full proof-bound summary. Preserve STOP metadata, baseline adjustments,
   existing cells/cycles/fills/legs/CIDs, freezes unrelated to the expected config mismatch, and reject latches.

`entry_filled` remains the cycle's total proven entry execution. Ledger verification changes from “owned ENTRY
fills equal `entry_filled`” to “owned ENTRY fills plus append-only external-entry allocations equal
`entry_filled`.” Position accounting adds external-entry BUY quantity once, independently of cycle projection.
Thus post-commit `P = B0 + owned BUY400 + external BUY100 = 500`, and cell `10` has `E=100, X=0, S=0`, open
obligation 100. The external-entry execution must not also enter the existing external-close totals.

Failures at any fault hook roll back the window, config revision, cell, cycle, evidence resolution, accounting, and
audit together. A successful retry of the same idempotency key returns the recorded result. A different command
using the same proof or evidence is rejected. On restart, the v8 window overlays genesis, cell order is
`10,0,…,9`, the external BUY reconstructs both cycle E and position P, and no entry is re-created for occupied
cell `10`.

## START behavior and approved directional outside-bounds policy

The operator has approved directional entry handling in addition to the adoption:

- With a fresh book below the lower bound (`ask < 4.8`), allow only SELL entries. Block and cancel BUY entries.
- With a fresh book above the upper bound (`bid > 5.9`), allow only BUY entries. Block and cancel SELL entries.
- Inside the window, preserve normal two-sided admission. If the book is unknown, stale, crossed, or cannot be
  classified safely, block all new entries.
- Outside-bounds direction never blocks TPs. Existing persistence, history, position, rules, leverage, margin,
  self-trade, net/gross cap, reservation, slot, and throttle checks remain mandatory. TP priority and terminal
  cancellation proof are unchanged.

After adoption and a separately confirmed START while the book is below 4.8, the engine first submits five SELL
TP obligations at `4.9`, `5.0`, `5.1`, `5.2`, and `5.3`. The six empty upper cells propose SELL entries at
`5.4–5.9`. Current open gross is 500 and `max_gross_position=1000`, so admission may reserve at most five of those
six 100 LIT entries; at least one remains queued. `max_active_orders=210` is not limiting. The scheduler decides
which equally valid entry queues using its existing deterministic priority; the adoption operation does not choose
or bypass it.

Direction changes withdraw pending intents and request cancellation of live entries that are no longer allowed.
Allowed opposite-direction entries are not submitted until terminal evidence for a conflicting/crossing order is
known. TPs remain ahead of entries in routing, and self-trade prevention may cancel or delay entries to protect a
TP. Neutral-grid orders remain `reduce_only=false` under the existing contract.

## Verification

Required tests include:

- A representative restart from the real shape: old cells 0–3 each have `E=100`; cell 2's TP obligation is split
  into children `73.22 + 26.78`; venue position becomes 500 through one manual BUY with multiple trade fragments;
  active orders are empty; source extension plus adoption commits atomically.
- Post-commit assertions: `B=0`, `P=500`, unchanged old fills/CIDs/cycles, cell order `10,0,…,9`, cell 10 generation
  1 has `E100/X0/S0`, and obligations/TPs are exactly 100 at `4.9–5.3`.
- START/restart assertions: five SELL TPs are recreated at the original prices, cell 10 never submits an ENTRY,
  and no baseline adjustment or synthetic fill/leg exists.
- Candidate and direct-store rejection for wrong side, reduce-only manual order, non-final order, partial or reused
  evidence, multiple viable manual orders, quantity/price/market/account mismatch, position 499/501, stale or
  causally inconsistent reads, active orders, pending WS/outbox/reservations, forbidden config changes, occupied
  target cell, stale revisions/proof, and cap violation.
- Fault injection before and after each write class proves full rollback and an unchanged v7 source. Dropped v8
  structures fail closed; a valid v7 database upgrades and restarts.
- Directional bounds tests below, inside, and above the window prove allowed-side entry submission, disallowed-side
  cancellation through terminal proof, no entries on stale/unknown/crossed books, five-TP priority, self-trade
  safety, unchanged caps, five-of-six SELL-entry admission below 4.8, and deterministic recovery across restart.

No production database, venue order, configuration, or credential is changed by this design document.
