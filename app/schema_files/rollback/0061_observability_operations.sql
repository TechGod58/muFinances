-- Rollback removes B61 observability records and migration marker only.
DROP TABLE IF EXISTS backup_restore_drill_runs;
DROP TABLE IF EXISTS alert_events;
DROP TABLE IF EXISTS health_probe_runs;
DROP TABLE IF EXISTS observability_metrics;
DELETE FROM schema_migrations WHERE migration_key = '0061_observability_operations';
