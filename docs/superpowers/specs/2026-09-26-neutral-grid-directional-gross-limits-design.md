# Neutral Grid Directional Gross Limits Design

## Goal

Allow independently bounded BUY-entry and SELL-entry exposure while retaining the existing signed net-position cap.
The mode is opt-in and uses `max_gross_position` as the cap for each direction. Legacy configurations retain the
current aggregate-gross behavior.

## Risk model

`RiskEndpoints` continues to publish `P`, `P_min`, `P_max`, and `gross_worst`, and adds:

- `long_entry_worst`: open obligation of BUY-entry cycles plus every possibly executable BUY ENTRY or
  unknown-role remainder.
- `short_entry_worst`: open obligation of SELL-entry cycles plus every possibly executable SELL ENTRY or
  unknown-role remainder.
- `gross_worst = long_entry_worst + short_entry_worst` remains an informational aggregate.

TP order remainders affect the reachable net interval but never consume the opposite directional entry budget.
A partial entry contributes its filled open obligation plus its still-possibly-executable remainder, so its total
reservation does not shrink early. `INTENT`, `SUBMIT_UNKNOWN`, cancellation-in-flight, and terminal-unknown entry
remainders remain reserved until terminal evidence proves otherwise. External entry quantity contributes through
the cycle's open obligation; TP fills and audited external closes reduce that cycle's original-side exposure.

With `directional_gross_limits=false`, entry admission and violation checks continue to enforce
`gross_worst <= max_gross_position`. With it active, they enforce both
`long_entry_worst <= max_gross_position` and `short_entry_worst <= max_gross_position`. Net checks remain
`P_max <= max_abs_net_position` and `P_min >= -max_abs_net_position` in both modes.

For the approved live case, existing BUY cycles consume 400 long, six SELL entries consume 600 short, and the new
BUY 100 raises long to 500. Aggregate gross 1100 is informational; the reachable net interval `[-600, 500]` stays
inside the unchanged ±1000 net cap.

## Configuration and audit

Add `directional_gross_limits: bool = False` to core, executor, and controller configuration. Serialization omits
false so legacy canonical configuration JSON and full START fingerprints remain unchanged. True is serialized,
shown in snapshots, stored in a config revision, and included in future extension proofs.

Grid identity remains unchanged because its fingerprint covers only dimensions, market identity, and Q. On startup,
the engine reads the latest persisted config revision, treating a missing field as false. When persisted and requested
policy differ, new entries are blocked by `RISK_POLICY_REVIEW_REQUIRED`; TP dispatch, reconciliation, and risk-reducing
cancellation continue. The requested policy does not become active before review.

An explicit operator START with the normal current-revision, preview, and risk acknowledgements may apply the sole
policy difference. In the command transaction it calls the existing `record_config_revision`, updates the START full
configuration fingerprint, audits the transition, clears the entry-only blocker, and reloads. A launcher START and
the existing `already_started` shortcut cannot approve the change. Any unrelated configuration difference is rejected.

## Snapshot contract

Summary adds `directional_gross_limits_active`, `directional_gross_limits_requested`, `long_entry_worst`, and
`short_entry_worst`. Existing `gross_worst`, `max_gross_position`, `P`, `P_min`, `P_max`, and
`max_abs_net_position` remain. During a policy mismatch, the active field reflects persisted policy and the requested
field reflects the controller target.

## Deployment

No schema migration, grid migration, reset, rebase, or synthetic fill is used. Deploying code with the default false
does not change legacy behavior. The production profile is changed to true only after verification. The native
Hummingbot shutdown path cancels owned orders, so rollout uses one normal graceful stop and password restart; ledger
obligations reconstruct and restore TPs. Force-kill and hot patching are excluded.

## Tests

Tests must prove legacy aggregate behavior, independent long/short admission, unchanged net enforcement, partial and
unknown entry reservations, TP exclusion from the opposite budget, external-entry accounting, default-false canonical
compatibility, reviewed START persistence/restart, launcher refusal, entry-only mismatch blocking, and the concrete
400-long plus six-SELL plus BUY-100 case.
