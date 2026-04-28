# PostgreSQL Migration Proof

B75 adds a PostgreSQL-specific migration proof framework. The goal is to prove that muFinances can build a fresh PostgreSQL database from migrations, detect drift, and safely roll back migrations that provide rollback SQL.

## Commands

Dry-run pending migrations:

```powershell
python -m migration_proof.cli dry-run --migrations schema/postgresql --database-url $env:MUFINANCES_DATABASE_URL
```

Apply migrations:

```powershell
python -m migration_proof.cli apply --migrations schema/postgresql --database-url $env:MUFINANCES_DATABASE_URL
```

Check drift:

```powershell
python -m migration_proof.cli drift --migrations schema/postgresql --database-url $env:MUFINANCES_DATABASE_URL
```

## Migration File Naming

Use paired SQL files:

```text
schema/postgresql/0001_foundation.up.sql
schema/postgresql/0001_foundation.down.sql
```

Rollback files are required for migrations that can be safely reversed. Destructive production rollbacks should be written as forward repair migrations unless the rollback has been tested.

## Required Proof

B75 is complete when these have been run against PostgreSQL:

1. Empty database migration from zero to current schema.
2. Dry-run showing no pending migrations after apply.
3. Drift check showing no missing or extra migrations.
4. Rollback validation for reversible migrations.
5. PostgreSQL-native test run in CI or on the internal deployment machine.

## Files

- `migration_proof/runner.py`
- `migration_proof/lock.py`
- `migration_proof/drift.py`
- `migration_proof/cli.py`
- `tests/test_migration_proof.py`

