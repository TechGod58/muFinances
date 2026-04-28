-- SQLite cannot drop columns safely in-place for this local runtime.
-- Rollback removes the migration marker only.
DELETE FROM schema_migrations WHERE migration_key = '0059_connector_productionization';
