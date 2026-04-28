-- B60 performance benchmark harness.

CREATE TABLE IF NOT EXISTS performance_benchmark_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_key TEXT NOT NULL UNIQUE,
    scenario_id INTEGER DEFAULT NULL,
    dataset_key TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    backend TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    thresholds_json TEXT NOT NULL DEFAULT '{}',
    results_json TEXT NOT NULL DEFAULT '{}',
    query_plans_json TEXT NOT NULL DEFAULT '{}',
    indexes_json TEXT NOT NULL DEFAULT '[]',
    regression_failures_json TEXT NOT NULL DEFAULT '[]',
    created_by TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT DEFAULT NULL,
    FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS performance_benchmark_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    metric_key TEXT NOT NULL,
    metric_type TEXT NOT NULL,
    elapsed_ms INTEGER NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    threshold_ms INTEGER DEFAULT NULL,
    status TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES performance_benchmark_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_planning_ledger_scenario_period_account
ON planning_ledger (scenario_id, period, account_code, reversed_at);

CREATE INDEX IF NOT EXISTS idx_planning_ledger_import_batch
ON planning_ledger (import_batch_id);

CREATE INDEX IF NOT EXISTS idx_import_batches_scenario_connector
ON import_batches (scenario_id, connector_key, created_at);

CREATE INDEX IF NOT EXISTS idx_connector_sync_logs_connector_created
ON connector_sync_logs (connector_key, created_at);

CREATE INDEX IF NOT EXISTS idx_audit_logs_entity_created
ON audit_logs (entity_type, entity_id, created_at);

CREATE INDEX IF NOT EXISTS idx_performance_benchmark_metrics_run
ON performance_benchmark_metrics (run_id, metric_key);

INSERT INTO schema_migrations (migration_key, description, checksum, applied_at)
VALUES (
    '0060_performance_benchmark_harness',
    'Create large realistic dataset seeding, query-plan checks, hot-path indexes, benchmark metrics, and regression thresholds.',
    'builtin-0060',
    datetime('now')
)
ON CONFLICT(migration_key) DO NOTHING;
