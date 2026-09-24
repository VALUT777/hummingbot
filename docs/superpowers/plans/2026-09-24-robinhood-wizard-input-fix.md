# Robinhood wizard input repair

**Observed cause:** the wizard printed an instruction to enter an API Private Key immediately before asking for Account Index. Following that instruction put the key into an integer field, then a validation exception terminated the wizard. This was reproduced using dummy data. The user's actual API Key Index is 4, which is accepted; changing the key-index range is not part of this repair.

**User-approved behavior:** prompt on separate numbered lines for Your Account Index, API Key Index and hidden API Private Key, followed by the price range, order size in LIT, number of grid levels and explicit USDG reserve. Public Key is not required. Put the local Hummingbot storage password in a clearly separate later step for newly entered credentials. Preserve the keystore created during the first attempt. Saved credentials may be reused through an explicit choice. Invalid field values must repeat only that field without losing already valid inputs; allow harmless surrounding whitespace from copying.

The user clarified that “number of iterations” means grid levels, not a limit on completed trades. Make order size and grid levels configurable with defaults 10 LIT and 21 levels, retaining the 1000 LIT absolute net-position cap, 5× leverage and two simultaneous orders. Level count must be an exact integer of at least two and fit the distinct exchange price ticks within the range; order size must be finite, positive, no greater than the position cap and satisfy existing exchange increment/minimum checks. Keep exact START confirmation, read-only checks, durable order recovery and launch identity checks.

- [x] Runtime agent: reproduce the prompt bug, then repair prompts, retry handling, password order and dynamic parameter collection, with focused regression tests and updated runbook text.
- [x] Grid agent: adapt the fixed level-count validation in the strategy and preflight, with their tests; guard grid allocation against insufficient exchange price ticks and preserve all exposure and exchange checks. No risk-ledger or journal changes.
- [x] Independent review: check prompt order, retry/cancel boundaries, credential secrecy and consistency between the wizard's configuration and preflight.
- [x] Parent acceptance: inspect the changes, run focused verification and a real launcher input/cancel smoke without actual keys, update the existing draft PR and reopen the wizard.

## Verification

The parent ran the full related suite: 522 passed and 8 subtests passed, with 16 known upstream warnings. The final wizard-only check passed 31 tests. Lint and whitespace checks passed. Independent scoped review accepted the changes. A real Desktop launcher in a pseudo-terminal confirmed account-first input, same-field retries for invalid account/API indexes, whitespace around index 4, and hidden private-key entry third. Cancellation occurred before keystore unlock or network checks; the existing user keystore and working configuration were unchanged. No actual API credentials or exchange operations were used.
