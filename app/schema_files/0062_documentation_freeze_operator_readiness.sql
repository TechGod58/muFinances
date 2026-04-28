-- B62 documentation freeze and operator readiness.
-- Documentation artifacts are versioned in docs/guides and registered as runbooks
-- during application startup. This migration records the release milestone.

INSERT INTO schema_migrations (migration_key, description, checksum, applied_at)
VALUES (
    '0062_documentation_freeze_operator_readiness',
    'Freeze operator-ready admin, planner, controller, integration, data dictionary, close process, production operations, and release checklist documentation.',
    'builtin-0062',
    datetime('now')
)
ON CONFLICT(migration_key) DO NOTHING;
