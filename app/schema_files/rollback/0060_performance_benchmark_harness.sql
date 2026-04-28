-- Rollback removes benchmark harness records and migration marker only.
DROP TABLE IF EXISTS performance_benchmark_metrics;
DROP TABLE IF EXISTS performance_benchmark_runs;
DELETE FROM schema_migrations WHERE migration_key = '0060_performance_benchmark_harness';
