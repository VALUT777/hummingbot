"""Schema v8: real external entry executions adopted into exact cycles."""

VERSION = 8
NAME = "external_entries"

SQL = r"""
CREATE TABLE external_entries (
    id INTEGER PRIMARY KEY,
    proof_id TEXT NOT NULL UNIQUE,
    grid_id TEXT NOT NULL,
    cell_id INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    entry_side TEXT NOT NULL CHECK (entry_side IN ('BUY','SELL')),
    quantity TEXT NOT NULL,
    observed_position TEXT NOT NULL,
    expected_config_revision INTEGER NOT NULL,
    expected_engine_revision INTEGER NOT NULL,
    position_observed_at_ms INTEGER NOT NULL,
    active_observed_at_ms INTEGER NOT NULL,
    history_scan_started_at_ms INTEGER NOT NULL,
    history_scan_completed_at_ms INTEGER NOT NULL,
    trades_high_water TEXT,
    orders_high_water TEXT,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    audit_event_id INTEGER NOT NULL REFERENCES audit_events(id),
    created_at_ms INTEGER NOT NULL,
    UNIQUE (grid_id, cell_id, generation),
    FOREIGN KEY (grid_id, cell_id, generation) REFERENCES cycles(grid_id, cell_id, generation)
) STRICT;

CREATE TABLE external_entry_evidence (
    adoption_id INTEGER NOT NULL REFERENCES external_entries(id),
    inbox_id INTEGER NOT NULL UNIQUE REFERENCES history_inbox(id),
    evidence_role TEXT NOT NULL CHECK (evidence_role IN ('TRADE','TERMINAL_ORDER')),
    allocated_quantity TEXT,
    PRIMARY KEY (adoption_id, inbox_id)
) STRICT;

CREATE TABLE manual_evidence_claims (
    inbox_id INTEGER PRIMARY KEY REFERENCES history_inbox(id),
    owner_kind TEXT NOT NULL CHECK (owner_kind IN ('EXTERNAL_CLOSE','EXTERNAL_ENTRY')),
    owner_id INTEGER NOT NULL,
    claimed_at_ms INTEGER NOT NULL
) STRICT;

INSERT INTO manual_evidence_claims(inbox_id, owner_kind, owner_id, claimed_at_ms)
SELECT e.inbox_id, 'EXTERNAL_CLOSE', e.settlement_id, s.created_at_ms
FROM external_settlement_evidence e JOIN external_settlements s ON s.id=e.settlement_id;

CREATE TRIGGER external_entries_append_only_u BEFORE UPDATE ON external_entries
BEGIN SELECT RAISE(ABORT, 'external_entries is append-only'); END;
CREATE TRIGGER external_entries_append_only_d BEFORE DELETE ON external_entries
BEGIN SELECT RAISE(ABORT, 'external_entries is append-only'); END;
CREATE TRIGGER external_entry_evidence_append_only_u BEFORE UPDATE ON external_entry_evidence
BEGIN SELECT RAISE(ABORT, 'external_entry_evidence is append-only'); END;
CREATE TRIGGER external_entry_evidence_append_only_d BEFORE DELETE ON external_entry_evidence
BEGIN SELECT RAISE(ABORT, 'external_entry_evidence is append-only'); END;
CREATE TRIGGER manual_evidence_claims_append_only_u BEFORE UPDATE ON manual_evidence_claims
BEGIN SELECT RAISE(ABORT, 'manual_evidence_claims is append-only'); END;
CREATE TRIGGER manual_evidence_claims_append_only_d BEFORE DELETE ON manual_evidence_claims
BEGIN SELECT RAISE(ABORT, 'manual_evidence_claims is append-only'); END;
"""
