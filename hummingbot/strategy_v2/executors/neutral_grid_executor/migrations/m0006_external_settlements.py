"""Schema v6: append-only proof of manual executions that settled one retained cycle obligation."""

VERSION = 6
NAME = "external_settlements"

SQL = r"""
CREATE TABLE external_settlements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proof_id TEXT NOT NULL UNIQUE,
    grid_id TEXT NOT NULL,
    settlement_side TEXT NOT NULL CHECK (settlement_side IN ('BUY', 'SELL')),
    observed_position TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    expected_config_revision INTEGER NOT NULL,
    expected_engine_revision INTEGER NOT NULL,
    position_observed_at_ms INTEGER NOT NULL,
    active_observed_at_ms INTEGER NOT NULL,
    history_scan_started_at_ms INTEGER NOT NULL,
    history_scan_completed_at_ms INTEGER NOT NULL,
    trades_high_water TEXT,
    orders_high_water TEXT,
    audit_event_id INTEGER NOT NULL REFERENCES audit_events(id),
    created_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE external_settlement_cycles (
    settlement_id INTEGER NOT NULL REFERENCES external_settlements(id),
    grid_id TEXT NOT NULL,
    cell_id INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    quantity TEXT NOT NULL,
    PRIMARY KEY (settlement_id, grid_id, cell_id, generation),
    UNIQUE (grid_id, cell_id, generation),
    FOREIGN KEY (grid_id, cell_id, generation) REFERENCES cycles(grid_id, cell_id, generation)
) STRICT;

CREATE TABLE external_settlement_evidence (
    settlement_id INTEGER NOT NULL REFERENCES external_settlements(id),
    inbox_id INTEGER NOT NULL REFERENCES history_inbox(id),
    evidence_role TEXT NOT NULL CHECK (evidence_role IN ('TRADE', 'TERMINAL_ORDER')),
    allocated_quantity TEXT,
    PRIMARY KEY (settlement_id, inbox_id),
    UNIQUE (inbox_id)
) STRICT;

CREATE TRIGGER external_settlements_append_only_u BEFORE UPDATE ON external_settlements
BEGIN SELECT RAISE(ABORT, 'external_settlements is append-only'); END;
CREATE TRIGGER external_settlements_append_only_d BEFORE DELETE ON external_settlements
BEGIN SELECT RAISE(ABORT, 'external_settlements is append-only'); END;
CREATE TRIGGER external_settlement_cycles_append_only_u BEFORE UPDATE ON external_settlement_cycles
BEGIN SELECT RAISE(ABORT, 'external_settlement_cycles is append-only'); END;
CREATE TRIGGER external_settlement_cycles_append_only_d BEFORE DELETE ON external_settlement_cycles
BEGIN SELECT RAISE(ABORT, 'external_settlement_cycles is append-only'); END;
CREATE TRIGGER external_settlement_evidence_append_only_u BEFORE UPDATE ON external_settlement_evidence
BEGIN SELECT RAISE(ABORT, 'external_settlement_evidence is append-only'); END;
CREATE TRIGGER external_settlement_evidence_append_only_d BEFORE DELETE ON external_settlement_evidence
BEGIN SELECT RAISE(ABORT, 'external_settlement_evidence is append-only'); END;
"""
