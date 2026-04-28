from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Protocol


class PgConnection(Protocol):
    def execute(self, sql: str, parameters: tuple[Any, ...] = ...) -> Any:
        ...


@dataclass(frozen=True)
class SchemaObject:
    object_type: str
    name: str


class SchemaDriftChecker:
    def expected_objects(self, migrations: Iterable[str]) -> set[SchemaObject]:
        expected: set[SchemaObject] = {
            SchemaObject("table", "schema_migrations"),
            SchemaObject("table", "schema_migration_runs"),
        }
        for migration_id in migrations:
            expected.add(SchemaObject("migration", migration_id))
        return expected

    def applied_migrations(self, conn: PgConnection) -> set[str]:
        cursor = conn.execute(f"SELECT {self._migration_key_column(conn)} FROM schema_migrations")
        return {str(row[0]) for row in cursor.fetchall()}

    def _migration_key_column(self, conn: PgConnection) -> str:
        cursor = conn.execute(
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

    def missing_migrations(self, conn: PgConnection, expected_migration_ids: Iterable[str]) -> list[str]:
        applied = self.applied_migrations(conn)
        return [migration_id for migration_id in expected_migration_ids if migration_id not in applied]

    def extra_migrations(self, conn: PgConnection, expected_migration_ids: Iterable[str]) -> list[str]:
        expected = set(expected_migration_ids)
        return sorted(self.applied_migrations(conn) - expected)
