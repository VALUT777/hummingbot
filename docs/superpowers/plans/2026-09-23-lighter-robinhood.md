# Lighter Robinhood Connector and Neutral Grid Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prepare a fork of Hummingbot with correct Robinhood Lighter support and a disabled fixed-range neutral LIT grid whose absolute net exposure, including pending fills, is bounded at 1,000 LIT with leverage 5.

**Architecture:** Add `lighter_perpetual_robinhood` as an alternate domain of the existing perpetual connector. Add a dedicated `StrategyV2Base` script and a pure reservation helper instead of modifying generic grid controllers. Read-only preflight and runtime instructions establish readiness without executing trades.

**Tech Stack:** Hummingbot v2.17.0 (`9af100d6822da7d2d0291a906c730ef172284ee2`), Python/Pydantic 2/asyncio/Decimal, official `lighter-sdk==1.1.4`, pytest, existing native connector build, optional locally built Docker image.

---

Read the paired design at `docs/superpowers/specs/2026-09-23-lighter-robinhood-design.md`. The user authorized autonomous implementation after the plan. No further design-approval gate is needed. Missing private credentials and price bounds block live trading, not code, public verification or packaging.

## Ownership and dependencies

| Workstream | Owned paths | Dependencies |
|---|---|---|
| Repository/runtime coordinator | Git remotes/branch; isolated environment; `setup.py`, `setup/environment.yml`, `setup/pip_packages.txt`, relevant Docker dependency lock | Existing setup agent; coordinate SDK pin changes with connector worker |
| Connector worker | `hummingbot/connector/derivative/lighter_perpetual/`; corresponding `test/hummingbot/connector/derivative/lighter_perpetual/`; minimal shared-domain callers if required | Upstream checkout |
| Neutral-grid worker | `scripts/lighter_robinhood_neutral_grid.py`, `scripts/lighter_robinhood_grid_risk.py`, `test/scripts/test_lighter_robinhood_neutral_grid.py`, `test/scripts/test_lighter_robinhood_grid_risk.py` | Stable domain contract; can implement with mocks before connector finishes |
| Readiness/docs worker | `bin/lighter_robinhood_preflight.py`, `test/bin/test_lighter_robinhood_preflight.py`, `conf/scripts/lighter_robinhood_neutral_grid.yml.example`, `docs/lighter-robinhood.md` | Domain contract and script config fields agreed; final public/auth checks depend on connector |
| Planner | Only this plan and paired design | No production ownership |

Parallel workers must not edit one another's files. Tests stay with each component owner; a separate reviewer reads the integrated diff. Run independent units first, then integrated checks. Do not add a separate Robinhood spot connector or Robinhood testnet in this change.

## Task 1: Pin the base and prepare a working isolated runtime

- [ ] Record fork URL, upstream remote, tag/commit, `codex/lighter-robinhood`, OS/architecture and interpreter in the runbook. Preserve the separate SuperTerminal checkout.
- [x] Inspected `AGENTS.md`, build metadata and available runtimes. Installed official micromamba 2.9.0 privately under `$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d`, created an isolated Python 3.12.14 environment from `setup/environment.yml`, aligned the current channel solver's NumPy/Numba/cryptography constraints, and built the native arm64 extensions. System Python and shell profiles were not changed. The exact tested commands are in `docs/lighter-robinhood-macos.md`.
- [ ] Change every authoritative `lighter-sdk==1.0.8` dependency pin to `lighter-sdk==1.1.4`, without blanket unrelated upgrades. Check the new SDK's API call signatures, model names and error returns used by both Lighter connectors.
- [ ] Build/import the necessary Hummingbot native extensions and native SDK signer. Record actual successful commands. If unavailable, distinguish source/unit checks from full runtime checks and keep live readiness blocked.
- [ ] Verify the package version and signer constructor accepts explicit `chain_id`; smoke-test native library loading without a real private key. Commit runtime/pin changes separately.

Verification: `python -m pip check`; `python -c "import lighter; import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_derivative"`; existing Lighter spot tests as well as perpetual tests after Task 2. A platform/package failure is not a passing readiness result.

## Task 2: Add domain-safe connector configuration

Files: `lighter_perpetual_constants.py`, `lighter_perpetual_utils.py`, `lighter_perpetual_web_utils.py`, `lighter_perpetual_derivative.py`, plus `test_lighter_perpetual_utils.py` and new `test_lighter_perpetual_web_utils.py` in their existing directories.

- [ ] First add failing routing/config tests for this exact table:

| Domain | REST origin | Signing chain | Quote/collateral |
|---|---|---:|---|
| `lighter_perpetual` | `https://mainnet.zklighter.elliot.ai` | 304 | USDC |
| `lighter_perpetual_testnet` | `https://testnet.zklighter.elliot.ai` | 300 | USDC |
| `lighter_perpetual_robinhood` | `https://api.rh.lighter.xyz` | 466324 | USDG |

WebSocket origins are corresponding `wss://…/stream`. Assert an unknown domain raises instead of routing to Core. Assert both public and private requests and the signer use the selected entry.

- [ ] Implement one immutable domain settings lookup, preserving backward-compatible Core constants if existing callers import them. Pass explicit chain ID into SDK `SignerClient`. Do not rely on SDK substring inference.
- [ ] Register Robinhood in `OTHER_DOMAINS`, parameters, examples, fees and keys, with `LIT-USDG` as example. Add independent correctly prefixed API fields, private key `SecretStr` with `is_secure=True`, nonnegative index validation, and account-limit validation/default Standard. Verify Hummingbot's alternate-domain key-to-constructor remapping actually works.
- [ ] Support an explicit account index where needed, without breaking existing config. With L1 lookup, accept only an exact unambiguous result; zero or multiple candidates require action. Test account index 0 explicitly (truthiness must not lose it).
- [ ] Run `python -m pytest test/hummingbot/connector/derivative/lighter_perpetual/test_lighter_perpetual_utils.py test/hummingbot/connector/derivative/lighter_perpetual/test_lighter_perpetual_web_utils.py -q`, then commit.

## Task 3: Route market, account and stream semantics through the domain

Files: `lighter_perpetual_api_utils.py`, `lighter_perpetual_derivative.py`, `lighter_perpetual_api_order_book_data_source.py`, `lighter_perpetual_user_stream_data_source.py`, `lighter_perpetual_auth.py` only if needed; corresponding existing test files. Inspect `hummingbot/data_feed/candles_feed/lighter_perpetual_candles/` and shared spot helper call sites; change only actual dependencies of the new domain.

- [ ] Add dated public RH fixtures with active LIT market ID 5, USDG asset ID 3, precision and minimums. Keep synthetic private account/position/funding/order fixtures; never capture real authenticated user data into Git.
- [ ] Change the market parser to accept a domain or explicit quote token with Core as its backward-compatible default. Pass it at **every** derivative/public-data call site. Filter hidden/inactive/non-perpetual markets and produce LIT-USDG. Do not use RH perp `quote_asset_id=0` to resolve collateral.
- [ ] Make trading-rule collateral and available-balance selection domain-aware. Test USDG `available_balance`, locked assets, missing fields, stale assets, position signs, quote-valued funding and fees. Validate returned asset metadata; no silent USDC fallback for RH.
- [ ] Mock REST/WS account authentication, subscribe/reconnect and funding updates for RH. Check account tier limits against Robinhood API docs; default to conservative Standard and do not promise Core premium allowances apply unchanged.
- [ ] Verify maker orders, partial fills, rejection, asynchronous cancellation, accepted-but-not-filled transactions, duplicate fills and client-to-exchange order identity. An uncertain submission/cancel must remain reconcilable; fix only failures relevant to safe operation.
- [ ] Ensure the new grid does not request a Core candle feed for RH. It does not need candles; document unsupported RH candle routing rather than silently returning another domain's data.
- [ ] Run the full existing perpetual suite and the Lighter spot suite; commit the minimal connector patch.

## Task 4: Implement a pure exposure reservation model

Create `scripts/lighter_robinhood_grid_risk.py` and `test/scripts/test_lighter_robinhood_grid_risk.py`. This module must not import network clients or mutate the exchange.

- [ ] Write a Decimal-based epoch model with baseline position, limit, per-client-order side/amount reservation, terminal cumulative fill state and pending/uncertain status. Define methods for admission, reservation, terminal observation and reconciled reset. The admission formula is the contract:

```python
from decimal import Decimal

def capacity(baseline: Decimal, buy_reserved: Decimal,
             sell_reserved: Decimal, limit: Decimal, side: str) -> Decimal:
    if side == "BUY":
        return max(Decimal("0"), limit - baseline - buy_reserved)
    if side == "SELL":
        return max(Decimal("0"), limit + baseline - sell_reserved)
    raise ValueError("Unsupported order side")
```

- [ ] Add invariant tests: at p0=950, at most 50 additional buys; at p0=-950, at most 50 additional sells; pending orders reduce capacity; partial/full fills and cancel requests do not refund reservations; duplicate events are idempotent; invalid/NaN/negative amounts fail; account positions outside cap fail startup.
- [ ] Enumerate or property-test all execution interleavings of a small mixed buy/sell batch and assert every realized signed position stays inside `[p0-S,p0+B]` and the cap. Include cancelled/rejected/unknown orders, rounding to 0.01 and boundary equality.
- [ ] Implement reset only when every order has final status and cumulative fill, account active orders are empty, pending submissions are zero, and `p0 + buys_filled - sells_filled` matches fresh net position at market precision. Require consecutive consistent observations. Test position-lag mismatch, lost/unknown order IDs, premature cancellation responses and external position changes freeze reset.
- [ ] Run `python -m pytest test/scripts/test_lighter_robinhood_grid_risk.py -q`; commit. Review this invariant before connecting it to live order submission code.

## Task 5: Implement the disabled neutral grid script

Create `scripts/lighter_robinhood_neutral_grid.py` and `test/scripts/test_lighter_robinhood_neutral_grid.py`. Follow current `scripts/simple_pmm.py` imports from `hummingbot.strategy.strategy_v2_base`; do not depend on removed `ScriptStrategyBase`.

- [ ] Define a Pydantic config using the design's exact defaults. Missing bounds are allowed only for a disabled template; live validation requires finite positive `lower_price < upper_price`, `enabled=True`, exact RH connector/pair, leverage 5, ONEWAY and cap no larger than 1,000. Validate order size/level count/refresh/staleness settings. Never derive collateral budget from 1,000 LIT.
- [ ] Implement states `DISABLED`, `RECONCILING`, `QUOTING`, `DRAINING`, `PAUSED`. Disabled state never calls buy/sell, changes leverage or cancels manual orders. Startup requires authoritative account/order reconciliation and fresh public/private state.
- [ ] Build 21 evenly spaced inclusive static levels; quantize prices to market ticks; choose nearest eligible buy below best ask and sell above best bid with at most two open orders total. Skip crossing/duplicate/dust levels. Quantize size downward to min(order size, side capacity), then enforce base/notional minimums and available margin.
- [ ] Reserve atomically before each buy/sell scheduling call; use LIMIT_MAKER and ONEWAY. Any unexpected submission result moves to reconciliation with the reservation retained. Confirm leverage through the connector only at enabled startup, never during tests/preflight.
- [ ] On fill/refresh/outside-range/stale-feed/stop, drain only owned orders; keep reservations until authoritative final reconciliation. Never create replacements concurrently with pending cancellation. Resume only inside bounds with consistent fresh state. Report inventory retained at stop or out-of-range; do not silently market-close it.
- [ ] Test mocked no-order startup, range validation, quantized fixed levels, side selection, ONEWAY 5, cap enforcement through scheduling, timeout/cancel races, stale book/private feed, insufficient funds, external orders, restart and disabled behavior. Ensure test doubles fail immediately on any unexpected signed exchange mutation.
- [ ] Run `python -m pytest test/scripts/test_lighter_robinhood_neutral_grid.py test/scripts/test_lighter_robinhood_grid_risk.py -q`; commit.

## Task 6: Read-only preflight, sample and Mac runbook

Create `bin/lighter_robinhood_preflight.py`, `test/bin/test_lighter_robinhood_preflight.py`, `conf/scripts/lighter_robinhood_neutral_grid.yml.example`, `docs/lighter-robinhood.md`. Keep example suffixed `.example` so it is not an automatically runnable strategy config.

- [ ] Provide `--public-only` mode and optional authenticated reads. The command must have no reachable trade/cancel/transfer/key-binding/leverage-write operation. Private key input must be redacted, not placed in command-line argv. Test accidental mutation methods with fail-fast stubs.
- [ ] Check domain/chain/SDK, USDG asset3/decimals, active LIT market, size/tick/minimums, maximum leverage, public REST+WS freshness, bounds/grid quantization and default disabled status. Exit nonzero for failed required checks; distinguish `public checks passed` from `live ready`.
- [ ] When authenticated, verify account/key association, available USDG margin, net position within cap, no unexplained active orders and private subscription. Compute a conservative full-cap margin requirement using the explicit range, leverage5 and a separately named configurable reserve; do not call this a user deposit/budget. Report unavailable checks as incomplete.
- [ ] Write Mac install/start/stop instructions using tested runtime commands, encrypted Hummingbot connector setup and the required inputs: bound prices, API key/account identifiers, sufficient funded USDG. Explain API key binding via user's own wallet without requesting its private key. Do not claim the Wallet UI has a key-creation button unless verified.
- [ ] Describe exclusive account control, post-only rejection, funding, range pause retaining exposure, clean restart, safe cancellation and manual review after unknown order outcomes. State that API rewards eligibility/multiplier is unconfirmed and this is not a points-farming system.
- [ ] Add optional server migration instructions only for the built fork, with secrets outside Git, persistent encrypted config and manual startup. Do not install or enable an auto-start service.
- [ ] Run `python -m pytest test/bin/test_lighter_robinhood_preflight.py -q` and public-only preflight. Keep evidence concise and secret-free; commit.

## Task 7: Integrated verification and independent acceptance

- [ ] Inspect the actual integrated diff and new paths. Verify no unexpected source refactor, unrelated dependency updates, private data, populated enabled config or live autostart.
- [ ] Run the full Lighter perpetual test directory, Lighter spot regression directory, both new script test files and the preflight tests in the built runtime. Run repository-required style/static checks against changed files. Fix failures with the owning worker.
- [ ] Perform public RH REST and a short WebSocket subscription to LIT order book. Verify the data is from the RH host and that live metadata matches config. No order smoke test during this task.
- [ ] Exercise the sample's validation to prove it cannot trade while disabled or missing bounds, and run a mocked integration cycle covering submit → partial fill → cancel → late fill → terminal reconcile → replacement without exceeding 1,000 LIT.
- [ ] Review the finality/risk code independently, prioritizing reservation release, account snapshot lag, unexpected order ownership and reconnects. The reviewer must reproduce the cap invariant tests rather than relying only on worker summaries.
- [ ] Commit and push the prepared branch to the personal fork. Report exact commit, tests, public check results and any runtime/private-test limitations. If a PR is created, attach it to the task. Leave no process capable of submitting orders running.

## Verification matrix and remaining live inputs

| Area | Offline evidence | Public live evidence | Requires user before live |
|---|---|---|---|
| Domain/signing | All 3 domains, explicit chain ID, native load | RH REST/WS | None |
| LIT/USDG metadata | Dated fixture/parser/minimum tests | Active market5, USDG3, precision, max leverage | Bounds entered by user |
| Credentials/account | Remapping, secret masking, ambiguity tests | None | API private key, API key index, wallet/account index |
| Balances/margin/funding | Synthetic conversion/reconciliation tests | Public funding allowed | Sufficient USDG; authenticated reads |
| Neutral exposure cap | Exhaustive/property interleavings and mocked integration | No live orders | Exclusive account control, clean account state |
| Grid lifecycle | Disabled, pending, cancel/fill/timeout/restart tests | Book freshness | Explicit future live start, eventual minimal funded lifecycle |
| Runtime | Imports, build, dependency checks | Network reachability | Private auth/native signer real key confirmation |

The user's country eligibility is already confirmed. Do not ask again. Bounds and API credentials are intentionally outstanding. Funds cannot be assumed. API points/rewards remain unconfirmed and are not a launch criterion or promised result.
