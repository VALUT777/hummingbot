# Neutral Grid External-Close Recovery Design

## Problem and required outcome

The stopped `lit-neutral-fixed-v1` ledger has cell 11 generation 2 with one owned BUY entry filled for 10 LIT and no owned TP fill (`E=10`, `X=0`). The venue is flat because the operator used manual reduce-only SELL order `1688849917028503` (client ID `1062647180040`), filled by trades `998583326` for 4.38 and `998583325` for 5.62 LIT. These are retained as unresolved inbox rows 154–156. Treating the trades as a baseline rebase would set `B=-10` while leaving the cell obligation open, so a restart would submit another SELL 10 and could open an unintended short.

The recovery must retain the original owned fill, cancelled TP evidence, manual order/trades, audit history, and CID allocator. It must not invent a TP fill, CID, order, or grid profit. It must leave the durable STOP in force; only a later explicit operator `START` may arm entries.

## Chosen approach

Extend the existing ledger with an append-only **external settlement**. The settlement says that specific unmatched account trades economically closed a specific retained cell obligation outside the grid. `E` and exchange-confirmed TP `X` remain unchanged. A new exact quantity `S` records externally settled base quantity, so the open obligation is `E - X - S`.

For the current evidence, `E=10`, `X=0`, `S=10`. The manual SELL executions count in account net reconciliation, so `B=0 + owned BUY 10 - external SELL 10 = 0`. They do not appear in `fills`, do not attach to a TP leg, and do not contribute to grid P&L.

This is safer and smaller than the alternatives:

- A baseline audit alone is unsafe because it preserves `E-X=10` and recreates a SELL obligation.
- Copying/renaming the database and bootstrapping a fresh one splits the audit chain, complicates crash-atomic cutover and prior-run markers, and risks CID reuse unless allocator state is hand-seeded.
- A general archive/session model is useful only when arbitrary unresolved inventory must be abandoned. Here authoritative manual executions exactly close the retained obligation, so an explicit settlement is sufficient.

## Durable schema and invariants

Schema migration 6 adds three append-only tables:

```sql
external_settlements(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  proof_id TEXT NOT NULL UNIQUE,
  grid_id TEXT NOT NULL,
  settlement_side TEXT NOT NULL CHECK (settlement_side IN ('BUY','SELL')),
  observed_position TEXT NOT NULL,
  actor TEXT NOT NULL,
  reason TEXT NOT NULL,
  expected_config_revision INTEGER NOT NULL,
  expected_engine_revision INTEGER NOT NULL,
  position_observed_at_ms INTEGER NOT NULL,
  active_observed_at_ms INTEGER NOT NULL,
  history_scan_started_at_ms INTEGER NOT NULL,
  history_scan_completed_at_ms INTEGER NOT NULL,
  trades_high_water TEXT,
  orders_high_water TEXT,
  audit_event_id INTEGER NOT NULL REFERENCES audit_events(id),
  created_at_ms INTEGER NOT NULL
)

external_settlement_cycles(
  settlement_id INTEGER NOT NULL REFERENCES external_settlements(id),
  grid_id TEXT NOT NULL,
  cell_id INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  quantity TEXT NOT NULL,
  PRIMARY KEY (settlement_id, grid_id, cell_id, generation),
  UNIQUE (grid_id, cell_id, generation),
  FOREIGN KEY (grid_id, cell_id, generation) REFERENCES cycles(grid_id, cell_id, generation)
)

external_settlement_evidence(
  settlement_id INTEGER NOT NULL REFERENCES external_settlements(id),
  inbox_id INTEGER NOT NULL REFERENCES history_inbox(id),
  evidence_role TEXT NOT NULL CHECK (evidence_role IN ('TRADE','TERMINAL_ORDER')),
  allocated_quantity TEXT,
  PRIMARY KEY (settlement_id, inbox_id),
  UNIQUE (inbox_id)
)
```

All three tables reject update/delete through triggers. Decimal validation remains in the store because SQLite cannot express canonical decimal text or cross-row sums. The first implementation settles exactly one cycle from exactly one terminal manual reduce-only order and all of that order's trade fragments. It refuses multiple cycles/orders, mixed-side, partial, over-, under-, or ambiguous allocation; it never guesses how trades map to obligations.

The store exposes these frozen interfaces:

```python
@dataclass(frozen=True)
class ExternalSettlementCycle:
    grid_id: str
    cell_id: int
    generation: int
    quantity: Decimal

@dataclass(frozen=True)
class ExternalSettlementEvidence:
    inbox_id: int
    evidence_role: str
    allocated_quantity: Optional[Decimal] = None

@dataclass(frozen=True)
class ExternalSettlementRequest:
    proof_id: str
    grid_id: str
    settlement_side: Side
    observed_position: Decimal
    actor: str
    reason: str
    expected_config_revision: int
    expected_engine_revision: int
    position_observed_at_ms: int
    active_observed_at_ms: int
    history_scan_started_at_ms: int
    history_scan_completed_at_ms: int
    trades_high_water: Optional[str]
    orders_high_water: Optional[str]
    cycles: Sequence[ExternalSettlementCycle]
    evidence: Sequence[ExternalSettlementEvidence]

NeutralGridStore.record_external_settlement(tx, request) -> int
NeutralGridStore.external_settled_quantity(grid_id, cell_id, generation) -> Decimal
NeutralGridStore.external_position_totals(grid_id: Optional[str] = None) -> Tuple[Decimal, Decimal]
```

`CycleRecord` and in-memory `Cycle` gain `external_settled: Decimal`. `E` remains owned entry fills and `X` remains owned TP fills. New derived properties are `effective_exit = X + external_settled` and `open_obligation = E - effective_exit`. `Buckets` gains `external_settled`; its `X` field keeps the exchange-fill meaning.

Every `E-X` consumer must use the correct concept:

- TP planning, dust, cell lock/release, migration blockers, late-obligation checks, gross risk, STOP inventory, and cell state use `E-X-S`.
- TP reservation/overfill checks remain bounded by `X + S + reserved <= E` so an externally settled quantity can never receive a new TP.
- Fill verification continues to prove `entry_filled` and `exit_filled` from `fills`; it separately proves settlement rows from unresolved `history_inbox` trades and their exact sums.
- Net reconciliation and later baseline audits include the real manual BUY/SELL totals from settlement evidence.
- Snapshots and CLI show `E`, `X exchange`, `S external`, and `open`; no P&L field treats `S` as a TP fill.

## Candidate and proof binding

The engine publishes `summary.external_close_candidate` only when it can construct exactly one allocation without inference. The object contains the old grid/cycle keys, `E`, `X`, `S proposed`, trade inbox IDs and exact sides/quantities/prices, terminal order evidence, observed flat position, scan/high-water timestamps, blockers, and `proof_id`.

`proof_id` is the SHA-256 of canonical JSON containing account/domain/market, config/engine revisions, grid and cycle `E/X/S`, the exact selected inbox IDs/payload hashes/quantities, observed position value, and the empty active-order fingerprint. It deliberately excludes volatile observation timestamps and scan start/completion times so the proof does not change every polling tick while the operator reviews and types the confirmation. Secrets and raw authenticated payloads are excluded. History cursors are excluded unless they encode a semantic change to the selected evidence; freshness timestamps and the latest high-waters are revalidated at execution and recorded in the audit/settlement row, not used as proof identity.

The `baseline_audit` subtype is `settle_external_close`. Its normalized payload is:

```json
{
  "action": "settle_external_close",
  "proof_id": "<published hash>",
  "confirmation": "SETTLE EXTERNAL CLOSE lit-neutral-fixed-v1 AT FLAT 0",
  "note": "manual reduce-only close after test",
  "acknowledge": true
}
```

The web intake binds it to the committed candidate and expected config/engine revisions. The engine recomputes the candidate inside command handling and rejects a changed proof. Application requires all of the following:

1. Durable outcome is `STOPPED` or `STOPPED_WITH_INVENTORY`; `STOP_UNCERTAIN` is refused.
2. Authenticated position is fresh, stable through the settlement delay, and exactly zero.
3. A fresh active-order read taken after that position is empty for the entire account/market; owned and unknown/foreign active counts are zero.
4. The existing stable-cut predicate succeeds: position was unchanged for the settlement delay before a complete private trades and inactive-orders walk began, and that walk covers the manual trades and all owned cancellation/terminal evidence. This uses the engine's stable position-read window and completed-scan record; it does not require the newest continuously refreshed position timestamp to precede the scan.
5. Every owned leg is final, every outbox row is done, every reservation is released, and there are no open history conflicts, retention gaps, late evidence, unknown submit/cancel results, pending WS executions, or unallocated fills.
6. Every selected trade/order inbox row is unresolved `UNMATCHED`, belongs to the exact account/market, and is present in the proof. The terminal order is reduce-only, final, filled exactly to the selected trades, and the trades have the obligation-closing side.
7. Selected trade quantities exactly equal selected `E-X-S`; partial or ambiguous sets are rejected.

Application is one existing engine transaction: insert audit, settlement header/cycles/evidence, mark the selected inbox rows resolved as `external_settlement:<id>`, close eligible cycles and clear their cell dust/blocker/reservations, complete the command, and bump the engine revision. A crash is therefore either wholly before or wholly after settlement. Unique `proof_id`, command idempotency key, cycle uniqueness, and inbox uniqueness make replay a no-op or a clean rejection.

The transaction does not clear `engine_meta.stop_requested_ms`, `stop_outcome`, or `stop_reason`, and it performs no connector call. After reload the engine remains stopped with ledger net 0 and no open obligation.

## Operator flow and later grid-size change

The stopped engine must be attached in maintenance mode to obtain fresh authenticated reads. Start the native runner with the existing 55-cell/10-LIT config, the ordinary launch confirmation present, and **without** `resume_after_stop_confirmation`. Existing durable-stop behavior creates the executor for reconciliation but rejects the launcher's automatic START; the engine stays stopped while it refreshes position, active orders, and private history.

When the candidate is ready, the UI offers one explicit action: “Confirm manual close and prepare restart.” It shows the 4.38 + 5.62 SELL evidence, the 10-LIT cycle, `E=10 / X exchange=0 / S external=10 / open=0`, the flat position, and the typed phrase. After it applies, the UI says “Prepared and still stopped” and offers the existing separate Start control.

If evidence spans more than one cycle or manual order, is partial, or has more than one possible allocation, the action is unavailable and the UI states that this bounded recovery supports exactly one cycle closed by one terminal reduce-only order. It must not present the feature as a general grid reset.

No configuration change is part of settlement. Keep the original 55-cell/10-LIT configuration until settlement is committed. The confirmed next grid is `grid_id: lit-neutral-fixed-v2`, range 4.9…5.9, 24 cells (25 boundaries), 20 LIT per cell, leverage 5, `max_active_orders: 120`, and both 1000-LIT caps unchanged. At the 5-LIT minimum TP size, a 20-LIT cell reserves five slots, so 120 slots admit all 24 cells exactly; runtime rules and preview must still confirm this.

After settlement, stop the maintenance process, apply those dimensions, relaunch with the existing migration phrase and no resume phrase, apply the existing audited `migrate_grid`, and review the new preview. Only the operator's final explicit START arms the 24×20 grid.

## Acceptance tests

- The exact retained case (owned BUY 10; manual reduce-only SELL trades 4.38 and 5.62; terminal manual order; flat position; no active orders) settles to `E=10, X=0, S=10, open=0`, closes the cycle, leaves baseline 0, ledger net 0, and leaves durable STOP set.
- `fills`, owned leg fill totals, CIDs, the TP leg, and grid P&L inputs are byte-for-byte unchanged by settlement.
- Same idempotency key replays the applied command; a new key with the same proof cannot reuse cycle or inbox rows.
- Stale proof/revisions, nonzero/stale position, any active/unknown order, incomplete/stale scan, non-final owned leg, unresolved outbox/reservation/conflict, wrong side, non-reduce-only order, quantity mismatch, partial allocation, mixed sides, or already-resolved inbox row is refused with no writes.
- Fault injection before commit rolls everything back; after commit, reopen shows one settlement, resolved evidence, a released cycle, net 0, and STOP still active.
- Restart/reload reconstructs `S`; no TP is submitted while stopped or after explicit START for the settled generation. A later generation may open normally.
- Late owned entry evidence after settlement reopens a positive `E-X-S` obligation under the existing late-evidence freeze and cannot dispatch a TP until audited. Late owned TP evidence that makes `X+S>E` is a history/ledger conflict and fails closed; settlement never causes the row to be ignored or silently reduces `S`.
- Existing normal TP fills still produce `S=0`; all pre-existing store/core/engine/web tests remain green.
- Existing audited grid migration accepts the quiescent settled grid and still preserves all old cycles, settlement rows, CIDs, and evidence.
- An end-to-end offline test settles the old 10-LIT obligation, migrates to `lit-neutral-fixed-v2` at 4.9…5.9 with 24×20, proves 25 boundaries and 120 reserved slots, then applies explicit START without creating any TP for the old generation.

## Explicit non-goals

This change does not provide a generic “forget history,” delete/reset database, fabricate fills, assign manual-trade P&L, auto-flatten, auto-resume, or automatically choose among ambiguous trades/cycles. A case without exact retained evidence remains manual-reconciliation blocked.
