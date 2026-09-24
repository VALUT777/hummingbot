"""Schema v1 of the neutral grid durable store (spec NG-DB-001).

Rules enforced by the schema itself:

* every table is ``STRICT``; declared column types are only ``TEXT`` and ``INTEGER`` (no ``REAL`` and
  no ``NUMERIC`` affinity, which would silently coerce decimal strings into lossy numbers);
* decimals are canonical TEXT strings, exchange/trade ids are TEXT, client order ids are INTEGER;
* append-only facts (``cid_map``, ``dedupe_keys``, ``fills``, ``audit_events``, ``schema_migrations``)
  are protected by triggers that abort any UPDATE/DELETE;
* bootstrap fields (baseline, cut) and fixed grid dimensions are protected by triggers once written.

This file is frozen once released: never edit it, add ``m0002_*`` instead (checksums are verified).
"""

VERSION = 1
NAME = "initial"

SQL = r"""
CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at_ms INTEGER NOT NULL
) STRICT;

CREATE TRIGGER schema_migrations_append_only_u BEFORE UPDATE ON schema_migrations
BEGIN
    SELECT RAISE(ABORT, 'schema_migrations is append-only');
END;

CREATE TRIGGER schema_migrations_append_only_d BEFORE DELETE ON schema_migrations
BEGIN
    SELECT RAISE(ABORT, 'schema_migrations is append-only');
END;

CREATE TABLE engine (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    db_uuid TEXT NOT NULL,
    engine_id TEXT NOT NULL,
    connector_name TEXT NOT NULL,
    connector_domain TEXT NOT NULL,
    account_index INTEGER NOT NULL CHECK (account_index >= 0),
    trading_pair TEXT NOT NULL,
    market_id INTEGER,
    cid_epoch INTEGER NOT NULL CHECK (cid_epoch BETWEEN 1 AND 255),
    created_at_ms INTEGER NOT NULL,
    recovered_from_loss INTEGER NOT NULL DEFAULT 0 CHECK (recovered_from_loss IN (0, 1)),
    bootstrapped_at_ms INTEGER,
    initial_grid_id TEXT,
    initial_baseline TEXT,
    bootstrap_trades_cut TEXT,
    bootstrap_orders_cut TEXT,
    bootstrap_cut_ts_ms INTEGER,
    bootstrap_actor TEXT,
    bootstrap_confirmation TEXT,
    current_grid_id TEXT,
    config_revision INTEGER NOT NULL DEFAULT 0,
    engine_revision INTEGER NOT NULL DEFAULT 0,
    reconciliation_revision INTEGER NOT NULL DEFAULT 0,
    engine_state TEXT NOT NULL DEFAULT 'BOOTSTRAPPING',
    state_reason TEXT,
    pause_reason TEXT,
    stop_reason TEXT,
    manual_reconcile_required INTEGER NOT NULL DEFAULT 0 CHECK (manual_reconcile_required IN (0, 1)),
    manual_reconcile_reason TEXT,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE TRIGGER engine_identity_immutable
BEFORE UPDATE OF db_uuid, engine_id, connector_name, connector_domain, account_index, trading_pair,
    cid_epoch, created_at_ms, recovered_from_loss ON engine
BEGIN
    SELECT RAISE(ABORT, 'engine identity is immutable');
END;

CREATE TRIGGER engine_bootstrap_immutable
BEFORE UPDATE OF bootstrapped_at_ms, initial_grid_id, initial_baseline, bootstrap_trades_cut,
    bootstrap_orders_cut, bootstrap_cut_ts_ms, bootstrap_actor, bootstrap_confirmation, market_id ON engine
WHEN OLD.bootstrapped_at_ms IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'bootstrap record (baseline, cut, market) is immutable');
END;

CREATE TRIGGER engine_no_delete BEFORE DELETE ON engine
BEGIN
    SELECT RAISE(ABORT, 'engine row cannot be deleted');
END;

CREATE TABLE engine_owner (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    owner_token TEXT NOT NULL,
    hostname TEXT NOT NULL,
    pid INTEGER NOT NULL,
    lock_key TEXT NOT NULL,
    acquired_at_ms INTEGER NOT NULL,
    released_at_ms INTEGER
) STRICT;

CREATE TABLE config_revisions (
    revision INTEGER PRIMARY KEY CHECK (revision >= 1),
    grid_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    config_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    actor TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
) STRICT;

CREATE TRIGGER config_revisions_append_only_u BEFORE UPDATE ON config_revisions
BEGIN
    SELECT RAISE(ABORT, 'config_revisions is append-only');
END;

CREATE TRIGGER config_revisions_append_only_d BEFORE DELETE ON config_revisions
BEGIN
    SELECT RAISE(ABORT, 'config_revisions is append-only');
END;

CREATE TABLE grids (
    grid_id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    config_revision INTEGER NOT NULL,
    lower_price TEXT NOT NULL,
    upper_price TEXT NOT NULL,
    cell_count INTEGER NOT NULL CHECK (cell_count > 0),
    order_amount_base TEXT NOT NULL,
    prices_json TEXT NOT NULL,
    anchor TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'RETIRED')),
    migrated_from TEXT,
    created_at_ms INTEGER NOT NULL,
    retired_at_ms INTEGER
) STRICT;

CREATE TRIGGER grids_dimensions_immutable
BEFORE UPDATE OF grid_id, fingerprint, config_revision, lower_price, upper_price, cell_count,
    order_amount_base, prices_json, anchor, migrated_from, created_at_ms ON grids
BEGIN
    SELECT RAISE(ABORT, 'grid dimensions, Q and anchor are immutable');
END;

CREATE TRIGGER grids_no_delete BEFORE DELETE ON grids
BEGIN
    SELECT RAISE(ABORT, 'grids cannot be deleted');
END;

CREATE TABLE cells (
    grid_id TEXT NOT NULL REFERENCES grids(grid_id),
    cell_id INTEGER NOT NULL CHECK (cell_id >= 0),
    low_price TEXT NOT NULL,
    high_price TEXT NOT NULL,
    entry_side TEXT NOT NULL CHECK (entry_side IN ('BUY', 'SELL')),
    generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
    state TEXT NOT NULL DEFAULT 'IDLE',
    blocker TEXT,
    reserved_slots INTEGER NOT NULL DEFAULT 0 CHECK (reserved_slots >= 0),
    queued_at_ms INTEGER,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (grid_id, cell_id)
) STRICT;

CREATE TRIGGER cells_prices_immutable
BEFORE UPDATE OF grid_id, cell_id, low_price, high_price, entry_side ON cells
BEGIN
    SELECT RAISE(ABORT, 'cell prices and entry side are immutable');
END;

CREATE TRIGGER cells_no_delete BEFORE DELETE ON cells
BEGIN
    SELECT RAISE(ABORT, 'cells cannot be deleted');
END;

CREATE TABLE cycles (
    grid_id TEXT NOT NULL,
    cell_id INTEGER NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 1),
    entry_side TEXT NOT NULL CHECK (entry_side IN ('BUY', 'SELL')),
    entry_price TEXT NOT NULL,
    tp_price TEXT NOT NULL,
    planned_amount TEXT NOT NULL,
    config_revision INTEGER NOT NULL,
    entry_filled TEXT NOT NULL DEFAULT '0',
    exit_filled TEXT NOT NULL DEFAULT '0',
    dust TEXT NOT NULL DEFAULT '0',
    state TEXT NOT NULL CHECK (state IN ('OPEN', 'COMPLETE')),
    late_evidence INTEGER NOT NULL DEFAULT 0 CHECK (late_evidence IN (0, 1, 2)),
    opened_at_ms INTEGER NOT NULL,
    closed_at_ms INTEGER,
    close_reason TEXT,
    PRIMARY KEY (grid_id, cell_id, generation),
    FOREIGN KEY (grid_id, cell_id) REFERENCES cells(grid_id, cell_id)
) STRICT;

CREATE TRIGGER cycles_no_delete BEFORE DELETE ON cycles
BEGIN
    SELECT RAISE(ABORT, 'cycles cannot be deleted');
END;

CREATE TABLE cid_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    cid_epoch INTEGER NOT NULL CHECK (cid_epoch BETWEEN 1 AND 255),
    next_seq INTEGER NOT NULL CHECK (next_seq >= 1)
) STRICT;

CREATE TABLE cid_map (
    cid INTEGER PRIMARY KEY CHECK (cid > 0 AND cid <= 281474976710655),
    grid_id TEXT NOT NULL,
    cell_id INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('ENTRY', 'TP')),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    allocated_at_ms INTEGER NOT NULL,
    UNIQUE (grid_id, cell_id, generation, role, revision)
) STRICT;

CREATE TRIGGER cid_map_append_only_u BEFORE UPDATE ON cid_map
BEGIN
    SELECT RAISE(ABORT, 'cid_map is append-only');
END;

CREATE TRIGGER cid_map_append_only_d BEFORE DELETE ON cid_map
BEGIN
    SELECT RAISE(ABORT, 'cid_map is append-only');
END;

CREATE TABLE retired_cids (
    cid INTEGER PRIMARY KEY CHECK (cid > 0 AND cid <= 281474976710655),
    reason TEXT NOT NULL,
    actor TEXT NOT NULL,
    at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE foreign_cids (
    cid INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    first_seen_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE legs (
    cid INTEGER PRIMARY KEY REFERENCES cid_map(cid),
    grid_id TEXT NOT NULL,
    cell_id INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('ENTRY', 'TP')),
    revision INTEGER NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price TEXT NOT NULL,
    amount TEXT NOT NULL,
    order_type TEXT NOT NULL,
    reduce_only INTEGER NOT NULL CHECK (reduce_only IN (0, 1)),
    expiry_ms INTEGER,
    state TEXT NOT NULL,
    filled TEXT NOT NULL DEFAULT '0',
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (grid_id, cell_id, generation, role, revision),
    FOREIGN KEY (grid_id, cell_id, generation) REFERENCES cycles(grid_id, cell_id, generation)
) STRICT;

CREATE TRIGGER legs_request_immutable
BEFORE UPDATE OF cid, grid_id, cell_id, generation, role, revision, side, price, amount, order_type,
    reduce_only, expiry_ms, created_at_ms ON legs
BEGIN
    SELECT RAISE(ABORT, 'leg request is immutable; use a new revision');
END;

CREATE TRIGGER legs_no_delete BEFORE DELETE ON legs
BEGIN
    SELECT RAISE(ABORT, 'legs cannot be deleted');
END;

CREATE TABLE orders (
    cid INTEGER PRIMARY KEY REFERENCES legs(cid),
    exchange_order_id TEXT,
    order_index TEXT,
    nonce TEXT,
    submission_state TEXT NOT NULL,
    cancel_state TEXT NOT NULL DEFAULT 'NONE',
    venue_status TEXT,
    venue_filled TEXT,
    venue_remaining TEXT,
    venue_final INTEGER NOT NULL DEFAULT 0 CHECK (venue_final IN (0, 1)),
    venue_row_json TEXT,
    last_evidence_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX orders_by_exchange_id ON orders(exchange_order_id);

CREATE TABLE reservations (
    cid INTEGER PRIMARY KEY REFERENCES legs(cid),
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    amount TEXT NOT NULL,
    slots INTEGER NOT NULL CHECK (slots >= 0),
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'RELEASED')),
    created_at_ms INTEGER NOT NULL,
    released_at_ms INTEGER,
    release_reason TEXT
) STRICT;

CREATE TABLE allocations (
    cid INTEGER NOT NULL REFERENCES legs(cid),
    grid_id TEXT NOT NULL,
    cell_id INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    amount TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (cid, grid_id, cell_id, generation),
    FOREIGN KEY (grid_id, cell_id, generation) REFERENCES cycles(grid_id, cell_id, generation)
) STRICT;

CREATE TABLE outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL CHECK (kind IN ('SUBMIT', 'CANCEL')),
    cid INTEGER NOT NULL REFERENCES legs(cid),
    request_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'DISPATCHED', 'DONE')),
    attempts INTEGER NOT NULL DEFAULT 0,
    prev_leg_state TEXT,
    outcome TEXT,
    outcome_detail TEXT,
    created_at_ms INTEGER NOT NULL,
    dispatched_at_ms INTEGER,
    result_at_ms INTEGER
) STRICT;

CREATE UNIQUE INDEX outbox_one_submit_per_cid ON outbox(cid) WHERE kind = 'SUBMIT';
CREATE UNIQUE INDEX outbox_one_open_cancel_per_cid ON outbox(cid) WHERE kind = 'CANCEL' AND status != 'DONE';

CREATE TRIGGER outbox_no_delete BEFORE DELETE ON outbox
BEGIN
    SELECT RAISE(ABORT, 'outbox rows cannot be deleted');
END;

CREATE TABLE history_inbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream TEXT NOT NULL CHECK (stream IN ('TRADES', 'INACTIVE_ORDERS')),
    dedupe_key TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('APPLIED', 'UNMATCHED', 'CONFLICT', 'PRE_CUT')),
    cid INTEGER,
    detail TEXT,
    batch_id TEXT,
    received_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    resolution TEXT,
    UNIQUE (dedupe_key, payload_hash)
) STRICT;

CREATE INDEX history_inbox_open ON history_inbox(status, resolved_at_ms);

CREATE TRIGGER history_inbox_no_delete BEFORE DELETE ON history_inbox
BEGIN
    SELECT RAISE(ABORT, 'history_inbox rows cannot be deleted');
END;

CREATE TABLE history_conflicts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conflict_key TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    inbox_id INTEGER REFERENCES history_inbox(id),
    cid INTEGER,
    detail TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    resolution TEXT
) STRICT;

CREATE TRIGGER history_conflicts_no_delete BEFORE DELETE ON history_conflicts
BEGIN
    SELECT RAISE(ABORT, 'history_conflicts rows cannot be deleted');
END;

CREATE TABLE dedupe_keys (
    dedupe_key TEXT PRIMARY KEY,
    stream TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    first_inbox_id INTEGER NOT NULL REFERENCES history_inbox(id),
    first_seen_ms INTEGER NOT NULL
) STRICT;

CREATE TRIGGER dedupe_keys_append_only_u BEFORE UPDATE ON dedupe_keys
BEGIN
    SELECT RAISE(ABORT, 'dedupe_keys is append-only');
END;

CREATE TRIGGER dedupe_keys_append_only_d BEFORE DELETE ON dedupe_keys
BEGIN
    SELECT RAISE(ABORT, 'dedupe_keys is append-only');
END;

CREATE TABLE fills (
    dedupe_key TEXT PRIMARY KEY REFERENCES dedupe_keys(dedupe_key),
    domain TEXT NOT NULL,
    account_index INTEGER NOT NULL,
    market_id INTEGER NOT NULL,
    trade_id_str TEXT NOT NULL,
    own_side TEXT NOT NULL CHECK (own_side IN ('BUY', 'SELL')),
    own_exchange_order_id TEXT,
    cid INTEGER NOT NULL REFERENCES legs(cid),
    grid_id TEXT NOT NULL,
    cell_id INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('ENTRY', 'TP')),
    size TEXT NOT NULL,
    price TEXT NOT NULL,
    is_maker INTEGER,
    timestamp_ms INTEGER NOT NULL,
    inbox_id INTEGER NOT NULL REFERENCES history_inbox(id),
    late INTEGER NOT NULL DEFAULT 0 CHECK (late IN (0, 1)),
    applied_at_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX fills_by_cid ON fills(cid);

CREATE TRIGGER fills_append_only_u BEFORE UPDATE ON fills
BEGIN
    SELECT RAISE(ABORT, 'fills is append-only');
END;

CREATE TRIGGER fills_append_only_d BEFORE DELETE ON fills
BEGIN
    SELECT RAISE(ABORT, 'fills is append-only');
END;

CREATE TABLE fill_allocations (
    dedupe_key TEXT NOT NULL REFERENCES fills(dedupe_key),
    grid_id TEXT NOT NULL,
    cell_id INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    amount TEXT NOT NULL,
    PRIMARY KEY (dedupe_key, grid_id, cell_id, generation)
) STRICT;

CREATE TABLE cursors (
    stream TEXT PRIMARY KEY,
    cursor TEXT,
    high_water TEXT,
    high_water_ts_ms INTEGER,
    required_boundary_ts_ms INTEGER,
    required_boundary_marker TEXT,
    oldest_available_ts_ms INTEGER,
    complete INTEGER NOT NULL DEFAULT 0 CHECK (complete IN (0, 1)),
    incomplete_reason TEXT,
    last_full_scan_ms INTEGER,
    revision INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    expected_config_revision INTEGER NOT NULL,
    expected_engine_revision INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('QUEUED', 'APPLIED', 'REJECTED', 'CONFLICT')),
    result_json TEXT,
    created_at_ms INTEGER NOT NULL,
    claimed_at_ms INTEGER,
    claim_count INTEGER NOT NULL DEFAULT 0,
    applied_at_ms INTEGER
) STRICT;

CREATE INDEX commands_queue ON commands(status, id);

CREATE TABLE audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    actor TEXT NOT NULL,
    engine_revision INTEGER,
    payload_json TEXT NOT NULL
) STRICT;

CREATE TRIGGER audit_events_append_only_u BEFORE UPDATE ON audit_events
BEGIN
    SELECT RAISE(ABORT, 'audit_events is append-only');
END;

CREATE TRIGGER audit_events_append_only_d BEFORE DELETE ON audit_events
BEGIN
    SELECT RAISE(ABORT, 'audit_events is append-only');
END;

CREATE TABLE state_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at_ms INTEGER NOT NULL,
    entity TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason TEXT
) STRICT;

CREATE TABLE snapshot_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_version INTEGER NOT NULL
) STRICT;

CREATE TABLE snapshots (
    snapshot_version INTEGER PRIMARY KEY,
    config_revision INTEGER NOT NULL,
    engine_revision INTEGER NOT NULL,
    committed_at_ms INTEGER NOT NULL,
    engine_state TEXT NOT NULL,
    snapshot_json TEXT NOT NULL
) STRICT;

CREATE TABLE baseline_adjustments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at_ms INTEGER NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    old_baseline TEXT NOT NULL,
    new_baseline TEXT NOT NULL,
    observed_position TEXT NOT NULL,
    audit_event_id INTEGER NOT NULL REFERENCES audit_events(id)
) STRICT;

CREATE TRIGGER baseline_adjustments_append_only_u BEFORE UPDATE ON baseline_adjustments
BEGIN
    SELECT RAISE(ABORT, 'baseline_adjustments is append-only');
END;

CREATE TRIGGER baseline_adjustments_append_only_d BEFORE DELETE ON baseline_adjustments
BEGIN
    SELECT RAISE(ABORT, 'baseline_adjustments is append-only');
END;

CREATE TABLE engine_kv (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;
"""
