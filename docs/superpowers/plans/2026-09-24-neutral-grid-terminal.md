# Neutral Grid Terminal Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one bounded, read-only terminal API backed by real public candles and snapshot-consistent ledger data.

**Architecture:** A focused `terminal.py` owns market allowlisting, candle caching, and response composition. `StoreGateway` adds a bounded confirmed-fill read; `server.py` validates query parameters and serves the composed response. Runtime injects either the public provider or an offline demo fixture.

**Tech Stack:** Python 3.12, aiohttp, SQLite, Decimal, pytest/pytest-asyncio.

---

### Task 1: Contract and ledger composition

**Files:**
- Create: `test/web/neutral_grid/test_ngweb_terminal.py`
- Create: `web/neutral_grid/terminal.py`
- Modify: `web/neutral_grid/gateway.py`

- [ ] Write tests for empty state, exact strings, nonfinal order filtering, fill cutoff, limit-plus-one truncation, and demo candles.
- [ ] Run the terminal test file and confirm failure because the module/API does not exist.
- [ ] Implement the minimal response composer and bounded gateway fill query.
- [ ] Re-run the terminal tests and confirm they pass.

### Task 2: Public candle provider

**Files:**
- Modify: `test/web/neutral_grid/test_ngweb_terminal.py`
- Modify: `web/neutral_grid/terminal.py`

- [ ] Add failing tests for strict identity allowlisting, exact decimal strings, timeout/error degradation, TTL cache, and concurrent single flight.
- [ ] Implement a server-owned Robinhood candle provider using the fixed host, path, pair, and market ID.
- [ ] Re-run the focused tests and confirm they pass.

### Task 3: HTTP and runtime wiring

**Files:**
- Modify: `test/web/neutral_grid/test_ngweb_terminal.py`
- Modify: `web/neutral_grid/server.py`
- Modify: `web/neutral_grid/runtime.py`

- [ ] Add failing API tests for authentication, default query values, strict bounds, normal composition, and HTTP-200 market degradation.
- [ ] Add `GET /api/terminal`, inject demo/public providers, and keep all output behind `_json`.
- [ ] Re-run the focused tests and then the neutral-grid backend regression suite.

### Task 4: Review and verification

**Files:**
- Review all changed backend and documentation files.

- [ ] Check the diff for writes outside the command path, arbitrary network targets, secret-bearing inputs, unbounded queries, floats in financial fields, and frontend-file overlap.
- [ ] Run terminal, state, attach, security, preview, command, truncation, and store-gateway tests.
- [ ] Report results without committing; the user explicitly requested no commit yet.
