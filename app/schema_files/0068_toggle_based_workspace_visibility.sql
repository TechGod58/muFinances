-- B68 is a frontend layout-control batch. The persistent state is stored per
-- signed-in browser profile so the server schema only records migration status.
INSERT OR IGNORE INTO schema_migrations (migration_key, description, checksum, applied_at)
VALUES (
    '0068_toggle_based_workspace_visibility',
    'Create closed-by-default toggle-based workspace visibility, active toggle state, persistent per-user layout state, close buttons, and keyboard-accessible section controls.',
    'builtin-0068',
    CURRENT_TIMESTAMP
);
