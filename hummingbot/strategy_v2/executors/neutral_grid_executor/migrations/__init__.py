"""Ordered, checksummed schema migrations for the neutral grid store.

A database is only opened when every applied migration recorded in ``schema_migrations`` is known to this
code with an identical checksum and no recorded version is newer than :data:`LATEST_VERSION`. Anything
else (unknown version, newer version, gap, edited migration) is refused fail-closed by the store.
"""
import hashlib
from dataclasses import dataclass
from typing import Tuple

from hummingbot.strategy_v2.executors.neutral_grid_executor.migrations import (
    m0001_initial,
    m0002_attempts_and_gaps,
)


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


MIGRATIONS: Tuple[Migration, ...] = (
    Migration(m0001_initial.VERSION, m0001_initial.NAME, m0001_initial.SQL),
    Migration(m0002_attempts_and_gaps.VERSION, m0002_attempts_and_gaps.NAME, m0002_attempts_and_gaps.SQL),
)

LATEST_VERSION = MIGRATIONS[-1].version


def validate_registry(migrations: Tuple[Migration, ...]) -> None:
    """Versions must be 1..N contiguous and strictly increasing."""
    for expected, migration in enumerate(migrations, start=1):
        if migration.version != expected:
            raise ValueError(f"migration registry is not contiguous at version {migration.version}")


validate_registry(MIGRATIONS)
