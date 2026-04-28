CREATE TABLE IF NOT EXISTS chart_render_artifacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_id INTEGER NOT NULL,
    scenario_id INTEGER NOT NULL,
    render_key TEXT NOT NULL UNIQUE,
    render_format TEXT NOT NULL,
    renderer TEXT NOT NULL DEFAULT 'mu-chart-renderer-v1',
    file_name TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    content_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    width INTEGER NOT NULL DEFAULT 960,
    height INTEGER NOT NULL DEFAULT 540,
    visual_hash TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (chart_id) REFERENCES report_charts(id) ON DELETE CASCADE,
    FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_chart_render_artifacts_chart
    ON chart_render_artifacts(chart_id, created_at);

CREATE INDEX IF NOT EXISTS idx_chart_render_artifacts_scenario
    ON chart_render_artifacts(scenario_id, render_format);

CREATE TABLE IF NOT EXISTS dashboard_chart_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scenario_id INTEGER NOT NULL,
    chart_id INTEGER DEFAULT NULL,
    widget_id INTEGER DEFAULT NULL,
    render_id INTEGER DEFAULT NULL,
    snapshot_key TEXT NOT NULL UNIQUE,
    snapshot_type TEXT NOT NULL DEFAULT 'dashboard_chart',
    status TEXT NOT NULL DEFAULT 'retained',
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
    FOREIGN KEY (chart_id) REFERENCES report_charts(id) ON DELETE SET NULL,
    FOREIGN KEY (widget_id) REFERENCES dashboard_widgets(id) ON DELETE SET NULL,
    FOREIGN KEY (render_id) REFERENCES chart_render_artifacts(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_dashboard_chart_snapshots_scenario
    ON dashboard_chart_snapshots(scenario_id, created_at);

INSERT OR IGNORE INTO schema_migrations (migration_key, description, checksum, applied_at)
VALUES (
    '0064_real_chart_rendering_export_engine',
    'Create rendered chart artifacts, PNG/SVG exports, dashboard chart snapshots, and chart image hooks for PDF, PowerPoint, and board package output.',
    'builtin-0064',
    datetime('now')
);
