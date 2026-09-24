# Make margin sufficiency advisory

The user explicitly requested that the estimated margin sufficiency check warn without blocking launch or trading. This replaces the earlier full-capacity free-margin gate. The calculation remains `upper price × position cap / leverage + additional reserve`, but its result is informational.

- Preflight emits `WARN` for a known, finite, nonnegative USDG balance below that estimate. Readiness accepts this specific advisory warning while retaining every other failure/incomplete gate.
- Runtime quoting reports insufficient estimated margin in status and logs, without pausing, draining or suppressing otherwise eligible quotes solely for that reason. Repeated snapshots must not spam identical warnings; the advisory clears when resolved.
- Missing, malformed, nonfinite or negative margin data remains a data-validity failure. Account/key verification, active-order ownership, exposure limits, leverage, exchange increments/minima, freshness and durable order recovery remain enforced.
- The wizard and runbook call the amount an estimate and explain that the reserve is an additional amount used in that estimate. Existing user configuration values are preserved.
- Verification covers advisory-only readiness and continued quoting, other blockers remaining effective, invalid data, warning status/log transitions and clearing. Tests use synthetic accounts and balances; no actual account operations are performed.

Ownership: runtime agent owns preflight/report tests, the small wizard wording changes and the runbook; grid agent owns runtime strategy/warning tests. Parent handles acceptance and the existing draft PR.
