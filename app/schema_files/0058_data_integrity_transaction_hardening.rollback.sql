-- SQLite cannot drop columns safely in-place for this local runtime.
-- Rollback removes the idempotency index and migration marker.
DROP INDEX IF EXISTS idx_planning_ledger_idempotency;
DELETE FROM schema_migrations WHERE migration_key = '0058_data_integrity_transaction_hardening';
