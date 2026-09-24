# Multi-cycle external settlement design

## Goal

Allow one complete final manual reduce-only order to settle several old-cycle obligations at once, without rewriting exchange fills, TP accounting, CIDs, history, or durable Stop.

## Eligibility and proof

The engine deterministically selects **all** eligible outstanding old cycles on the required same side with positive open obligations `E - X - S`; the UI presents and confirms that full set rather than allowing an arbitrary subset. Every selected cycle must pass the existing settlement guards. The exact sum of all selected obligations must equal the complete exact trade sum of **one** final manual reduce-only order, on the required opposite closing side.

The proof pins the complete selected set, expected revisions, the order identity, and the complete matching order/history evidence. It is rejected when the set mixes sides, the total differs, proof or revisions are stale, a cycle is blocked, the authenticated position is non-zero or unknown, active/unknown orders remain, or history is incomplete. Evidence is single-use and cannot be reused by another settlement.

## Atomic result

One transaction records the settlement evidence and an `S` allocation for every selected cycle. The allocation consumes each cycle's evidence exactly once and leaves `X`, confirmed fills, CIDs, history, and durable Stop unchanged. Retry of the same idempotent request returns the established result; a rollback leaves no partial cycle allocation.

Existing v6 per-cycle settlement allocation rows represent the per-cycle allocation. No schema change is expected.

## Migration boundary

After maintenance restart and successful evidence settlement, a new v3 configuration may migrate to 10 cells × 100 LIT over `4.9…5.9` at 5x with baseline `0` only when the authenticated position is flat and all old obligations are cleared. The migrated engine stays `STOPPED`; a user performs any later Start explicitly.

Settings remain configuration-managed. The GUI displays configuration and proof, but does not edit grid settings.
