-- B56 has no physical table changes. Rollback removes the migration marker only.
DELETE FROM schema_migrations WHERE migration_key = '0057_formula_modeling_safety';
