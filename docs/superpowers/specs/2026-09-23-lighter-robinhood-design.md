# Lighter on Robinhood Chain: connector and bounded neutral grid

Date: 2026-09-23. Status: implementation design under the user's explicit authorization to plan and then proceed autonomously. Live trading is not authorized by this preparation task.

## Outcome and user choices

The official `hummingbot/hummingbot` repository is forked into the user's personal GitHub account, `VALUT777`, with upstream v2.17.0 commit `9af100d6822da7d2d0291a906c730ef172284ee2` checked out on `codex/lighter-robinhood`. The separate SuperTerminal workspace is out of scope. Prepare this Mac first; document portable server deployment without silently installing a background trading service.

The user selected an existing Robinhood Lighter account, eligible country, the LIT perpetual, a neutral grid, leverage **5**, and a hard maximum absolute net position of **1,000 LIT**. This is a position cap, not a collateral balance, not an order size, and not a USDG spending budget. The user will enter explicit lower and upper price bounds before starting. Do not choose those bounds or infer deposited funds. API credentials are not available during development.

Acceptance means the fork, code, offline tests, public connectivity checks, and a disabled, validated setup are ready. It does not mean funded trading or rewards eligibility has been demonstrated.

## Evidence and assumptions

As checked on 2026-09-23:

- Robinhood documents a distinct Lighter domain with its own execution and liquidity: [domain documentation](https://docs.robinhood.com/chain/lighter-domains/). Mainnet API is `https://api.rh.lighter.xyz`; WebSocket is `wss://api.rh.lighter.xyz/stream`.
- [Official SDK source](https://github.com/elliottech/lighter-python/blob/main/lighter/signer_client.py) and the published `lighter-sdk==1.1.4` wheel support explicit signing `chain_id=466324`. This is distinct from the underlying Robinhood EVM chain ID. The existing `1.0.8` wheel chooses 304 for any URL containing `api`; merely changing the URL would silently sign for the wrong chain.
- [Public market details](https://api.rh.lighter.xyz/api/v1/orderBookDetails) report an active LIT perpetual, market ID 5, size increment 0.01 LIT, price increment 0.0001, minimum size 5 LIT, and minimum notional 10. The initial margin floor is 2000 basis points, consistent with maximum leverage 5. Runtime discovery must refresh these values; fixtures are dated observations, not eternal constants.
- [Public asset details](https://api.rh.lighter.xyz/api/v1/assetDetails) identify USDG as asset ID 3 with six decimals and enabled margin. Perpetual `quote_asset_id=0` is not a valid USDG asset reference; do not infer settlement from that field or label Robinhood balances USDC. Other margin assets exist; the initial bot requires adequate USDG and does not deliberately borrow against stock-token collateral.
- Robinhood testnet responds publicly but currently labels asset ID 3 USDC. This release adds Robinhood **mainnet only**, preserving Core mainnet and Core testnet. A separate testnet implementation requires its own verified configuration.
- Upstream v2.17.0 pins `lighter-sdk==1.0.8` in several dependency sources. The new SDK carries native signers for macOS arm64 and amd64. This Mac is Darwin arm64 with uv 0.11.28, system Python 3.14.6, no conda/mamba and Docker 29.6 with its Colima daemon stopped. Prefer an isolated uv-managed Python 3.12 runtime, falling back to the existing container tooling only if native build proves infeasible. Record native imports and dependency resolution after setup. Use an isolated runtime; do not alter a global Python environment. The repository's dependency constraints, including current pandas-ta, may need Python 3.12 even though setup metadata advertises an older minimum.

## Architecture choice

**Extend the existing perpetual connector with an alternate domain**, named `lighter_perpetual_robinhood`. Centralize exact per-domain REST, WebSocket, signing chain, quote and collateral settings. Reject unknown domains instead of falling back to Core. Preserve `lighter_perpetual` and `lighter_perpetual_testnet` behavior. Register independent connector credentials using existing Hummingbot `OTHER_DOMAINS` conventions. Carry the domain through every market parser and public/private data source.

Alternatives rejected: cloning the entire connector would duplicate order/authentication behavior; treating Robinhood as only a custom base URL would miss signing and settlement differences. Add a separate connector only if implementation discovers a concrete incompatibility that cannot be expressed through these domain settings.

Use SDK 1.1.4 with explicit chain ID rather than patching opaque native signing internals. Regress both Core domains and any spot connector using this shared package. User authentication consists of a public wallet address/account index, API key index and API private key. Expose a secure Robinhood credential field; never request a seed phrase or wallet private key. If wallet lookup yields multiple accounts, require an explicit account index; never silently choose the first unrelated account. Creating or binding an API key through an L1 wallet signature is a user operation, outside this implementation.

## Neutral grid behavior

Upstream Grid Strike manages one directional grid. PMM Simple maintains mid-relative levels and independently managed position executors without an aggregate pending-order cap. Neither satisfies this task unchanged. Add a small, dedicated `StrategyV2Base` script with a separately testable risk helper. Avoid changes to generic controller/executor behavior.

The script generates fixed, evenly spaced levels in the user-supplied interval. It quotes the nearest eligible buy level below the live best ask and the nearest eligible sell level above the live best bid. Use post-only `LIMIT_MAKER`, one-way position mode, and ordinary one-way buy/sell orders; opposite fills naturally reduce or reverse the single net position. Do not implement two hedged long/short legs. Skip levels that cross the book, duplicate after quantization, fail exchange minimums, or exceed exposure/margin capacity. With initial `max_open_orders=2`, there is at most one live bid and one live ask. More simultaneous levels are not necessary for the first safe release.

Defaults: `enabled=false`; connector `lighter_perpetual_robinhood`; pair `LIT-USDG`; leverage `5`; position mode `ONEWAY`; `max_position_base=1000`; `order_amount_base=10`; `grid_levels=21`; `max_open_orders=2`; `refresh_seconds=30`; `max_data_age_seconds=10`; no automatic recentering; no automatic market liquidation. Lower and upper bounds have no tradeable default. Ten LIT is a conservative editable per-order size, not an assertion about suitable investment size. Enforce exchange minimum notional at runtime. The sample remains disabled and intentionally fails live readiness until explicit bounds and credentials are supplied.

A fill or refresh initiates cancellation/draining before the next quoting cycle. A price outside the interval cancels resting orders and pauses new submissions. Existing inventory remains; this is not a stop-loss or a guarantee against liquidation. Operator stop cancels this bot's orders and reports remaining inventory. It does not close unknown/manual positions. Market closures, reduce-only markets, stale data, insufficient margin, unknown submission state, disconnected private updates and unresolved restart reconciliation pause submissions.

## Exposure invariant and finality

Maintain one signed, reconciled baseline position `p0` per quoting epoch. Atomically reserve every submitted buy quantity in `B` and every submitted sell quantity in `S` **before** scheduling network submission. Admit only orders satisfying:

```text
p0 + B <= 1000
p0 - S >= -1000
```

Equivalently, reserve the entire interval `[p0-S, p0+B]`. This covers any ordering of partial fills on either side. Quantize amounts downward before reserving; never round a capped amount upward. An accepted transaction is not a confirmed order or fill. A cancellation request is not a terminal cancellation. Keep full original reservations for the epoch, including filled/cancel-pending/uncertain orders; do not refund them merely because local tracking disappears. This intentionally sacrifices a little capacity for a simple verifiable bound.

Reset the baseline and reservations only after every epoch order has authoritative terminal state and final cumulative filled quantity, the account's active-order list is empty, there are no local pending submissions, and the signed sum of final fills plus `p0` equals a fresh account net position at exchange precision. Require consecutive consistent observations before resuming. A mismatch or timeout freezes new orders and provides actionable status. REST/WS duplicate fills must not double count. The runtime must retain enough order identities to reconcile an accepted-but-timed-out submission.

On startup, reject unexplained open orders, exposure already beyond 1,000 LIT, or ambiguous account identity. Require exclusive control of the selected account and market while running; manual orders/other bots can defeat any client-side cap. Unexpected external orders/position changes trigger a pause. No mechanism can guarantee an account-wide bound against concurrent uncontrolled trading or exchange misbehavior; document this practical boundary explicitly.

## Read-only readiness and deployment

Provide a read-only preflight command with a public mode needing no key and an optional authenticated mode that reads a private key from secure local input/environment without echoing or persisting it. It never submits, cancels, adjusts leverage, changes keys, transfers or signs a trade. Public mode validates endpoint identity, LIT metadata, USDG metadata, book freshness, grid quantization/minimums and native SDK compatibility. Authenticated mode verifies account/key association, available USDG margin, positions, existing orders and private-stream readiness. Unknown required conditions are failures or incomplete checks, never green readiness.

Leverage configuration is a signed account mutation, so apply and confirm it only during an explicitly enabled future live startup. During this task, preflight may verify that leverage 5 is allowed but must not change the account. Estimate full-cap initial margin from the user range and leverage, plus an explicit configurable reserve for fees/funding; this is a conservative requirement, not the user's deposited budget. Reject inadequate available margin. Report current/required margin and untested private checks clearly.

Use a dedicated local environment and command documented from actual successful verification. If a container runtime is already available, a locally built image can be an optional portable path; never point at an upstream stock image that omits fork changes. Store secrets only through Hummingbot's encrypted connector configuration or local ignored inputs. Do not commit populated config, key material, authentication tokens, or user account snapshots. No auto-start service, launch agent, scheduled trading, or live orders in delivery.

## Verification and completion boundary

Required automated coverage: exact routing for all domains; chain ID propagation; secure config/remapping; USDG symbol/collateral/balance conversion; active-perpetual filtering; market precision; maker/reduce-only semantics; account selection; order finality; funding/positions; Core regression; neutral-level generation; reservation invariants under arbitrary fill/cancel ordering; submission timeout; cancellation races; stale/missing data; restart/unknown orders; disabled configuration; margin and bounds failures; redacted output.

Required live public checks: RH REST metadata/order book and a short WebSocket subscription with real updates. Never convert these into a funded trade probe. Authenticated readiness, a minimal funded end-to-end order lifecycle, and operational live acceptance remain unverified until the user supplies an API key and explicitly starts trading.

Robinhood rewards are a separate program. Wallet orders have documented wallet-specific attribution, while API eligibility/multiplier is unconfirmed. Make no rewards promise, fabricate no attribution and add no artificial-volume/self-trading behavior. The bot's purpose is the user's bounded neutral market strategy.
