# Neutral Grid Add-Only Extension Design

Extend the active LIT grid from `4.9–5.9` to `4.8–5.9` without changing its grid ID, anchor, baseline, existing cells, cycles, fills, legs, CIDs, or TP prices. Add cell `10` for `4.8–4.9`; the active price order becomes `10,0,…,9`.

The immutable `grids` row remains the genesis definition. Schema v7 adds append-only window revisions. The effective grid view overlays the latest revision, and active cells are resolved by matching every adjacent effective price pair to exactly one immutable cell. Extension is permitted only from a durable clean stop with fresh, coherent account evidence and no unresolved orders, outbox, reservations, drift, history conflicts, dust, or late evidence. Proven open cycle obligations may remain.

The command is `baseline_audit` with `action=extend_grid`, a 64-character lowercase hexadecimal `proof_id`, a note, and `acknowledge=true`. The snapshot publishes `grid_extension_candidate` containing the proof, blockers, source and target windows, added cells, and retained obligations. The engine rechecks the proof and all blockers when applying the command.

All ledgers remain loaded for history, risk, release, and TP recovery. Entry admission and display follow active price order. Extension never rebases inventory or fabricates settlement: `B=0`, the existing fills explain the 400 LIT position, and replacement TPs use each cycle's immutable target.
