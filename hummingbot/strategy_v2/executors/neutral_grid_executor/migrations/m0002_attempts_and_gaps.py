"""Schema v2 (review fixes 4 and 10).

* ``outbox_attempts``: one append-only row per transport attempt of an outbox row. A resend of the same CID no longer
  erases the earlier attempt's outcome, and a first-attempt-only rule can be enforced for releasing outcomes.
* ``cursors.retention_gap_open``: a durable per-stream retention-gap flag. It is set when the required overlap
  boundary is older than the oldest available history, and cleared only by a manual reconciliation that names the
  stream.

Frozen once released; add ``m0003_*`` for further changes.
"""

VERSION = 2
NAME = "attempts_and_gaps"

SQL = r"""
CREATE TABLE outbox_attempts (
    outbox_id INTEGER NOT NULL REFERENCES outbox(id),
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    dispatched_at_ms INTEGER NOT NULL,
    outcome TEXT,
    outcome_detail TEXT,
    result_at_ms INTEGER,
    PRIMARY KEY (outbox_id, attempt)
) STRICT;

CREATE TRIGGER outbox_attempts_no_delete BEFORE DELETE ON outbox_attempts
BEGIN
    SELECT RAISE(ABORT, 'outbox_attempts is append-only');
END;

CREATE TRIGGER outbox_attempts_outcome_once
BEFORE UPDATE OF outcome ON outbox_attempts
WHEN OLD.outcome IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'an attempt outcome is recorded once');
END;

INSERT INTO outbox_attempts(outbox_id, attempt, dispatched_at_ms, outcome, outcome_detail, result_at_ms)
SELECT id, attempts, COALESCE(dispatched_at_ms, created_at_ms), outcome, outcome_detail, result_at_ms
FROM outbox WHERE attempts > 0;

ALTER TABLE cursors ADD COLUMN retention_gap_open INTEGER NOT NULL DEFAULT 0 CHECK (retention_gap_open IN (0, 1));

UPDATE cursors SET retention_gap_open = 1
WHERE required_boundary_ts_ms IS NOT NULL AND oldest_available_ts_ms IS NOT NULL
    AND required_boundary_ts_ms < oldest_available_ts_ms;
"""
