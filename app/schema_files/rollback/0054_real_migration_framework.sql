-- B53 rollback script.
-- Keep this conservative: rollback evidence is documented and available, but
-- live rollback execution must be explicitly approved by operations.

DROP TABLE IF EXISTS migration_runs;
DROP TABLE IF EXISTS migration_locks;
