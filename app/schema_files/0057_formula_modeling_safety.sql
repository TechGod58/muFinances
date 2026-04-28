-- B56 records the application-level formula safety boundary.
-- Formula parsing, linting, sandbox limits, and calculation traces are enforced
-- in app/services/formula_engine.py and model recalculation metadata.

INSERT INTO schema_migrations (migration_key, description, checksum, applied_at)
VALUES (
    '0057_formula_modeling_safety',
    'Create dedicated formula parser/evaluator controls, formula linting, sandbox limits, circular dependency stress coverage, and calculation trace output.',
    'managed-by-runtime',
    CURRENT_TIMESTAMP
);
