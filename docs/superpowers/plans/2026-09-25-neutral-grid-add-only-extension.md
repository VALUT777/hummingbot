# Neutral Grid Add-Only Extension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an audited append-only lower grid extension from `4.9–5.9` to `4.8–5.9` while preserving every existing ledger identity and TP obligation.

**Architecture:** Store effective grid windows as append-only revisions over the immutable genesis grid. Resolve active cells by price interval, keep all ledgers loaded, and restrict entry admission and rendering to the effective price order. A proof-bound command applies the extension only from a clean stopped state.

**Tech Stack:** Python 3.12, SQLite STRICT tables, pytest, Decimal arithmetic.

---

### Task 1: Append-only store window revision

**Files:** `migrations/m0007_grid_window_revisions.py`, `migrations/__init__.py`, `store.py`, `store/test_ng_store_grid_extension.py`

- [ ] Write tests proving a lower extension adds one stable cell, overlays the effective grid, preserves all old rows and baseline, survives restart, and rejects contraction/repricing or unsafe durable state.
- [ ] Run the focused store test and confirm it fails because the extension API is absent.
- [ ] Add schema v7, `GridExtension`, effective-window lookup, active-cell resolution, and the atomic extension transaction.
- [ ] Run the focused store suite and existing bootstrap/migration tests.

### Task 2: Engine proof, command, and active ordering

**Files:** `engine.py`, `commands.py`, `snapshot.py`, `engine/test_ng_engine_grid_extension.py`

- [ ] Write tests for the candidate contract, stale proof rejection, stopped-state application, retained obligations, and active price order `10,0,…,9`.
- [ ] Run the focused engine test and confirm the missing candidate/action failures.
- [ ] Implement the candidate digest, command gate, append-only store call, price-ordered admission, and snapshot fields.
- [ ] Run focused engine, command, risk, and snapshot tests.

### Task 3: Regression verification

**Files:** all changed files above

- [ ] Run store and engine neutral-grid suites, controller tests, compilation, and whitespace checks.
- [ ] Review the diff for production paths, credentials, resets, rebases, or settlement fabrication; none are allowed.
