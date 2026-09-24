# Multi-cycle external settlement plan

1. Extend core settlement preview/confirmation so the engine deterministically forms the pinned set from all eligible outstanding cycles on the required same side, then presents that complete set for UI confirmation with one final manual reduce-only order. Reuse existing per-cycle guards and reject mixed side, total mismatch, stale proof/revisions, blocked cycle, non-flat/unknown position, active/unknown orders, and incomplete history.
2. Apply evidence and `S` allocations for all selected cycles in one store transaction. Bind evidence to the full set and order/history proof, consume it once, retain `X`, fills, CIDs, history, and Stop, and preserve rollback/idempotence behavior.
3. Expose the same proof and rejection semantics through the web flow. Keep settings configuration-managed rather than GUI-editable.
4. Add core and web coverage for four BUY cycles of 20 LIT settled by one SELL order of 80 LIT; retain one-cycle compatibility; cover rollback, idempotence, evidence reuse, stale proof, and each rejection boundary.
5. Obtain independent review. In maintenance mode, verify evidence settlement before migrating to v3: 10 × 100 LIT, range `4.9…5.9`, 5x, baseline `0`; require flat position and no old obligations, then leave the result `STOPPED` for an explicit user Start.
