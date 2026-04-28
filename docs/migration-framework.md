# muFinances Migration Framework

## Managed SQL

New database changes should be added under `app/schema_files/` as one SQL file
per migration. The filename stem is the migration key, for example
`0054_real_migration_framework.sql`.

## Rollback Scripts

Every managed migration should have a matching rollback script under
`app/schema_files/rollback/`. Rollback plans are exposed for operators, while
live rollback execution remains an explicit deployment decision.

## Runtime Controls

The migration runner exposes:

- `GET /api/migrations/status`
- `POST /api/migrations/dry-run`
- `POST /api/migrations/run`
- `GET /api/migrations/runs`
- `GET /api/migrations/rollback-plan/{migration_key}`

The runner records every validation/run attempt in `migration_runs`, uses
`migration_locks` to prevent concurrent schema changes, and translates managed
SQL through the PostgreSQL compatibility path during dry-run checks.

## Current Compatibility

`app.services.foundation.ensure_foundation_ready()` still registers historical
batch migrations for compatibility with existing local databases. New database
work should use the managed SQL flow first, then register the migration key in
the foundation registry.
