from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol

from .lock import MigrationLock


class PgConnection(Protocol):
    def execute(self, sql: str, parameters: tuple[Any, ...] = ...) -> Any:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...


@dataclass(frozen=True)
class MigrationPlan:
    migration_id: str
    up_sql: str
    down_sql: str | None = None
    source_path: str | None = None


@dataclass(frozen=True)
class MigrationResult:
    migration_id: str
    status: str
    dry_run: bool
    source_path: str | None = None
    error: str | None = None


class PostgreSqlMigrationRunner:
    def __init__(self, conn: PgConnection, lock: MigrationLock | None = None):
        self.conn = conn
        self.lock = lock or MigrationLock()

    def ensure_registry(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                migration_key text PRIMARY KEY,
                description text NOT NULL DEFAULT '',
                checksum text NOT NULL DEFAULT '',
                applied_at timestamptz NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migration_runs (
                id bigserial PRIMARY KEY,
                migration_id text NOT NULL,
                status text NOT NULL,
                error text,
                started_at timestamptz NOT NULL,
                finished_at timestamptz
            )
            """
        )
        self.conn.commit()

    def applied(self) -> set[str]:
        cursor = self.conn.execute(f"SELECT {self._migration_key_column()} FROM schema_migrations")
        return {str(row[0]) for row in cursor.fetchall()}

    def _migration_key_column(self) -> str:
        cursor = self.conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'schema_migrations'
              AND column_name IN ('migration_key', 'migration_id')
            ORDER BY CASE column_name WHEN 'migration_key' THEN 0 ELSE 1 END
            """
        )
        row = cursor.fetchone()
        return str(row[0]) if row else 'migration_key'

    def pending(self, plans: Iterable[MigrationPlan]) -> list[MigrationPlan]:
        applied = self.applied()
        return [plan for plan in plans if plan.migration_id not in applied]

    def dry_run(self, plans: Iterable[MigrationPlan]) -> list[MigrationResult]:
        return [
            MigrationResult(plan.migration_id, "pending", True, plan.source_path)
            for plan in self.pending(plans)
        ]

    def apply(self, plans: Iterable[MigrationPlan], dry_run: bool = False) -> list[MigrationResult]:
        self.ensure_registry()
        pending = self.pending(plans)
        if dry_run:
            return self.dry_run(pending)

        results: list[MigrationResult] = []
        self.lock.acquire(self.conn)
        try:
            for plan in pending:
                self.lock.record_start(self.conn, plan.migration_id)
                try:
                    self.conn.execute(plan.up_sql)
                    self._record_migration(plan)
                    self.lock.record_finish(self.conn, plan.migration_id, "applied")
                    self.conn.commit()
                    results.append(MigrationResult(plan.migration_id, "applied", False, plan.source_path))
                except Exception as exc:
                    self.conn.rollback()
                    self.lock.record_finish(self.conn, plan.migration_id, "failed", str(exc))
                    self.conn.commit()
                    results.append(MigrationResult(plan.migration_id, "failed", False, plan.source_path, str(exc)))
                    raise
        finally:
            self.lock.release(self.conn)
            self.conn.commit()
        return results

    def _record_migration(self, plan: MigrationPlan) -> None:
        applied_at = datetime.now(timezone.utc)
        if self._migration_key_column() == 'migration_id':
            self.conn.execute(
                """
                INSERT INTO schema_migrations (migration_id, source_path, applied_at)
                VALUES (%s, %s, %s)
                """,
                (plan.migration_id, plan.source_path, applied_at),
            )
            return
        checksum = hashlib.sha256(plan.up_sql.encode('utf-8')).hexdigest()
        self.conn.execute(
            """
            INSERT INTO schema_migrations (migration_key, description, checksum, applied_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (migration_key) DO NOTHING
            """,
            (plan.migration_id, plan.source_path or plan.migration_id, checksum, applied_at),
        )

    def rollback(self, plan: MigrationPlan, dry_run: bool = False) -> MigrationResult:
        if not plan.down_sql:
            return MigrationResult(plan.migration_id, "rollback_unavailable", dry_run, plan.source_path)
        if dry_run:
            return MigrationResult(plan.migration_id, "rollback_pending", True, plan.source_path)
        self.lock.acquire(self.conn)
        try:
            self.conn.execute(plan.down_sql)
            self.conn.execute(f"DELETE FROM schema_migrations WHERE {self._migration_key_column()} = %s", (plan.migration_id,))
            self.conn.commit()
            return MigrationResult(plan.migration_id, "rolled_back", False, plan.source_path)
        except Exception as exc:
            self.conn.rollback()
            return MigrationResult(plan.migration_id, "rollback_failed", False, plan.source_path, str(exc))
        finally:
            self.lock.release(self.conn)
            self.conn.commit()


def load_sql_plans(directory: str | Path) -> list[MigrationPlan]:
    root = Path(directory)
    plans: list[MigrationPlan] = []
    for up_path in sorted(root.glob("*.up.sql")):
        migration_id = up_path.name.removesuffix(".up.sql")
        down_path = root / f"{migration_id}.down.sql"
        plans.append(
            MigrationPlan(
                migration_id=migration_id,
                up_sql=up_path.read_text(encoding="utf-8"),
                down_sql=down_path.read_text(encoding="utf-8") if down_path.exists() else None,
                source_path=str(up_path),
            )
        )
    return plans
