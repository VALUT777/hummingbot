"""Schema v7: append-only effective windows for additive extensions of an immutable grid."""

VERSION = 7
NAME = "grid_window_revisions"

SQL = r"""
CREATE TABLE grid_window_revisions (
    grid_id TEXT NOT NULL REFERENCES grids(grid_id),
    config_revision INTEGER NOT NULL REFERENCES config_revisions(revision),
    fingerprint TEXT NOT NULL,
    lower_price TEXT NOT NULL,
    upper_price TEXT NOT NULL,
    cell_count INTEGER NOT NULL CHECK (cell_count > 0),
    order_amount_base TEXT NOT NULL,
    prices_json TEXT NOT NULL,
    anchor TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    proof_id TEXT NOT NULL UNIQUE,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (grid_id, config_revision)
) STRICT;

CREATE TRIGGER grid_window_revisions_append_only_u BEFORE UPDATE ON grid_window_revisions
BEGIN SELECT RAISE(ABORT, 'grid_window_revisions is append-only'); END;
CREATE TRIGGER grid_window_revisions_append_only_d BEFORE DELETE ON grid_window_revisions
BEGIN SELECT RAISE(ABORT, 'grid_window_revisions is append-only'); END;
"""
