DROP TABLE IF EXISTS parallel_cubed_partitions;
DROP TABLE IF EXISTS parallel_cubed_runs;
DELETE FROM schema_migrations WHERE migration_key = '0065_parallel_cubed_multi_core_execution_engine';
