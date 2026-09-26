"""Schema v4 (review B-09): the owner token of the store that committed each dispatch attempt.

A NOT_SENT / zero-fill reject may only finalise a DISPATCHED submit when it is reported by the same store instance
(owner token) that committed the dispatch mark -- only that process can know the transport was not invoked. Rows
dispatched before this migration have no owner (NULL) and are treated as dispatched by another process.
Frozen once released; add ``m0005_*`` for further changes.
"""

VERSION = 4
NAME = "dispatch_owner"

SQL = r"""
ALTER TABLE outbox_attempts ADD COLUMN dispatch_owner TEXT;
"""
