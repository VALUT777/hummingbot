# Neutral-grid submit recovery

## Observed failures

Two production gaps had different transport symptoms. One submission hit the engine's 10-second
outer timeout while Lighter's shared rate bucket was exhausted. That timing is consistent with a
gate wait, but the old logs cannot prove the exact cancellation boundary. A later submission
returned an SDK signer error in 432 ms. The installed SDK collapses local signing failures and HTTP
`BadRequestException` responses into the same `(None, None, error)` shape, so the second failure's
raw text cannot prove whether the transaction reached the venue.

## Bounded pre-send gate

`submit_with_client_id` now has a five-second pre-send deadline covering only acquisition of the
SEND_TX throttler context and connector transaction lock. The connector claims the CID before its
first gate wait. Concurrent calls for that CID return `UNKNOWN` and never invoke the signer twice.

If the deadline expires for the sole CID owner, the connector releases every acquired context and
returns `NOT_SENT`: the signer has not been invoked, no in-flight order was registered, and the
engine may apply its existing proven-unsent retry policy with a new durable CID. Once both gates are
held, tracking is registered synchronously and the signer call remains under the engine's outer
timeout. Cancellation or any failure after that boundary remains `UNKNOWN`. A caller with an outer
timeout shorter than five seconds also remains conservatively `UNKNOWN`.

## Safe diagnostics

SDK error strings are never stored or published. Returned signer errors use only the allowlisted
categories `invalid_nonce`, `order_not_found`, `rate_limited`, or `unclassified`. Raised transport
exceptions use only `timeout`, `connection`, or `unclassified`. Categories are diagnostic: they do
not turn an SDK error into `NOT_SENT`, do not trigger automatic unknown resolution, and do not
weaken the active-order/history evidence gates.

## Verification and limits

Regression tests cover unique-CID gate timeout, partial acquisition cleanup, invalid timeout
values, external cancellation during gate and signer execution, a shorter outer timeout, both
same-CID races, a single signer invocation, recognized categories mixed with secret-like text, and
the existing AC-56 unknown/no-retry behavior. Venue-side ambiguity still requires the existing
audited resolution after the configured delay and authoritative absence proof.
