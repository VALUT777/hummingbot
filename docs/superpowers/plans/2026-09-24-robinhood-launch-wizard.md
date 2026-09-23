# Robinhood launch wizard implementation plan

> **For agentic workers:** Use subagent-driven-development to implement this bounded runtime extension. Keep all changes on `codex/lighter-robinhood`; the parent serializes Git operations.

**Goal:** Let the user double-click a Mac launcher, paste API data locally and reach a checked, explicitly requested bot launch.

**Architecture:** A Russian terminal wizard reuses native Hummingbot encrypted credentials and existing preflight classes in-process. A small `.command` launcher resolves this checkout and its private Python runtime. No connector or strategy behavior changes.

**Tech Stack:** Python 3.12, existing Hummingbot config/security and CLI modules, PyYAML, POSIX shell.

---

### Task 1: Implement and test the bounded wizard

Files: create `bin/lighter_robinhood_setup.py`, `test/bin/test_lighter_robinhood_setup.py`, and `Запустить LIT бота.command`; modify `docs/lighter-robinhood-macos.md` with the simple launch path. The parent separately owns the small `Статус LIT бота.command` and `Остановить LIT бота.command` wrappers for native status and graceful stop.

- [x] Confirm actual native keystore/start APIs, runtime paths and active-instance detection before implementation.
- [x] Add tests first for strict indexes/decimal bounds, no echo fallback, no secret argv/output/plaintext persistence, existing credential reuse and read-only preflight gating.
- [x] Implement the design in `docs/superpowers/specs/2026-09-24-robinhood-launch-wizard-design.md` with dependency injection for terminal/network/start boundaries.
- [x] Cover cancellation and failures leaving the working config disabled; prevent modifying a running instance and bind startup to the verified account/configuration.
- [x] Add the executable double-click wrapper and concise Russian instructions, including hidden input behavior, Maker Only off, and position-preserving stop.
- [x] Run targeted wizard/preflight tests and shell syntax checks in the existing private Python environment; do not use actual credentials or place orders.

### Task 2: Review and deliver

- [x] Independently review the actual diff against the specification, then credential secrecy and launch-gate correctness; return defects to the implementer.
- [x] Verify a pseudo-terminal dry run stops at input/cancel and never starts trading, plus regression tests appropriate to changed runtime boundaries.
- [ ] Commit/push the verified changes and update/attach existing draft PR #1. Preserve local user config and unrelated workspace.
- [ ] Make the launcher easy to find locally, open it only to the initial input prompt, and deliver concise field-by-field instructions. Do not read secret input/output after the user begins entering credentials.

## Acceptance evidence

The parent verified 110 targeted tests, lint, compilation, shell syntax and whitespace checks. Independent review found no remaining findings. An actual Desktop-launcher pseudo-terminal run from an external directory reached hidden password input and canceled without changing the working configuration. No credentials, authenticated exchange calls or orders were used; the local configuration remains disabled. Desktop launch/status/stop shortcuts are prepared in `~/Desktop/LIT бот`.
