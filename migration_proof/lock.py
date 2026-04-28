from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol


class PgConnection(Protocol):
    def execute(self, sql: str, parameters: tuple[Any, ...] = ...) -> Any:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...


@dataclass(frozen=True)
class MigrationLock:
    lock_key: int = 32000071

    def acquire(self, conn: PgConnection) -> None:
        conn.execute("SELECT pg_advisory_lock(%s)", (self.lock_key,))

    def release(self, conn: PgConnection) -> None:
        conn.execute("SELECT pg_advisory_unlock(%s)", (self.lock_key,))

    def record_start(self, conn: PgConnection, migration_id: str) -> None:
        conn.execute(
            """
            INSERT INTO schema_migration_runs (migration_id, status, started_at)
            VALUES (%s, 'running', %s)
            """,
            (migration_id, datetime.now(timezone.utc)),
        )

    def record_finish(self, conn: PgConnection, migration_id: str, status: str, error: str | None = None) -> None:
        conn.execute(
            """
            UPDATE schema_migration_runs
            SET status = %s, error = %s, finished_at = %s
            WHERE migration_id = %s AND status = 'running'
            """,
            (status, error, datetime.now(timezone.utc), migration_id),
        )

