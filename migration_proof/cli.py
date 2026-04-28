from __future__ import annotations

import argparse
import os

try:
    import psycopg
except Exception:  # pragma: no cover - dependency is environment-specific
    psycopg = None

from .drift import SchemaDriftChecker
from .runner import PostgreSqlMigrationRunner, load_sql_plans


def main() -> int:
    parser = argparse.ArgumentParser(description="muFinances PostgreSQL migration proof runner")
    parser.add_argument("command", choices=("dry-run", "apply", "drift"))
    parser.add_argument("--migrations", default="schema/postgresql")
    parser.add_argument("--database-url", default=os.environ.get("MUFINANCES_DATABASE_URL"))
    args = parser.parse_args()

    if psycopg is None:
        raise SystemExit("psycopg is required for PostgreSQL migration proof")
    if not args.database_url:
        raise SystemExit("MUFINANCES_DATABASE_URL or --database-url is required")

    plans = load_sql_plans(args.migrations)
    with psycopg.connect(args.database_url) as conn:
        runner = PostgreSqlMigrationRunner(conn)
        if args.command == "dry-run":
            for result in runner.apply(plans, dry_run=True):
                print(f"{result.migration_id}: {result.status}")
        elif args.command == "apply":
            for result in runner.apply(plans):
                print(f"{result.migration_id}: {result.status}")
        elif args.command == "drift":
            checker = SchemaDriftChecker()
            for migration_id in checker.missing_migrations(conn, [plan.migration_id for plan in plans]):
                print(f"missing: {migration_id}")
            for migration_id in checker.extra_migrations(conn, [plan.migration_id for plan in plans]):
                print(f"extra: {migration_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
