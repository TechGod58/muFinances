-- B61 observability and operations.

CREATE TABLE IF NOT EXISTS observability_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_key TEXT NOT NULL,
    metric_type TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT NOT NULL DEFAULT 'count',
    labels_json TEXT NOT NULL DEFAULT '{}',
    trace_id TEXT NOT NULL DEFAULT '',
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS health_probe_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    probe_key TEXT NOT NULL,
    status TEXT NOT NULL,
    latency_ms INTEGER NOT NULL DEFAULT 0,
    detail_json TEXT NOT NULL DEFAULT '{}',
    trace_id TEXT NOT NULL DEFAULT '',
    checked_by TEXT NOT NULL DEFAULT 'system',
    checked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alert_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_key TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    message TEXT NOT NULL,
    source TEXT NOT NULL,
    trace_id TEXT NOT NULL DEFAULT '',
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    acknowledged_by TEXT DEFAULT NULL,
    acknowledged_at TEXT DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS backup_restore_drill_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    drill_key TEXT NOT NULL UNIQUE,
    backup_key TEXT NOT NULL,
    status TEXT NOT NULL,
    backup_size_bytes INTEGER NOT NULL DEFAULT 0,
    validation_json TEXT NOT NULL DEFAULT '{}',
    trace_id TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_observability_metrics_key_time
ON observability_metrics (metric_key, recorded_at);

CREATE INDEX IF NOT EXISTS idx_health_probe_runs_key_time
ON health_probe_runs (probe_key, checked_at);

CREATE INDEX IF NOT EXISTS idx_alert_events_status_severity
ON alert_events (status, severity, created_at);

INSERT INTO schema_migrations (migration_key, description, checksum, applied_at)
VALUES (
    '0061_observability_operations',
    'Create structured trace-aware logs, metrics, health probes, alerts, drill records, and diagnostic checks.',
    'builtin-0061',
    datetime('now')
)
ON CONFLICT(migration_key) DO NOTHING;
