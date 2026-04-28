-- Rollback removes the B62 documentation milestone marker only.
DELETE FROM schema_migrations WHERE migration_key = '0062_documentation_freeze_operator_readiness';
