# PostgreSQL Schema Migrations

Place PostgreSQL migrations here using paired files:

```text
0001_foundation.up.sql
0001_foundation.down.sql
```

Run the proof runner with:

```powershell
python -m migration_proof.cli dry-run --migrations schema/postgresql
python -m migration_proof.cli apply --migrations schema/postgresql
python -m migration_proof.cli drift --migrations schema/postgresql
```

