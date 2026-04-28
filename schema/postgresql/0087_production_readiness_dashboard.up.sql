CREATE TABLE IF NOT EXISTS production_dashboard_snapshots (
    snapshot_id text PRIMARY KEY,
    overall_status text NOT NULL,
    components_json text NOT NULL,
    created_by text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS production_alerts (
    alert_id text PRIMARY KEY,
    severity text NOT NULL,
    source text NOT NULL,
    message text NOT NULL,
    status text NOT NULL DEFAULT 'open',
    metadata_json text NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz
);

CREATE INDEX IF NOT EXISTS ix_production_dashboard_status_created
    ON production_dashboard_snapshots (overall_status, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_production_alerts_status_severity
    ON production_alerts (status, severity, created_at DESC);

