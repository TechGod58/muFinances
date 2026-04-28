-- B63 tax classification and compliance watch.

CREATE TABLE IF NOT EXISTS tax_activity_classifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    classification_key TEXT NOT NULL UNIQUE,
    scenario_id INTEGER NOT NULL,
    ledger_entry_id INTEGER DEFAULT NULL,
    activity_name TEXT NOT NULL,
    tax_status TEXT NOT NULL,
    activity_tag TEXT NOT NULL,
    income_type TEXT NOT NULL,
    ubit_code TEXT DEFAULT NULL,
    regularly_carried_on INTEGER NOT NULL DEFAULT 0,
    substantially_related INTEGER NOT NULL DEFAULT 1,
    debt_financed INTEGER NOT NULL DEFAULT 0,
    amount REAL NOT NULL DEFAULT 0,
    expense_offset REAL NOT NULL DEFAULT 0,
    net_ubti REAL NOT NULL DEFAULT 0,
    form990_part TEXT DEFAULT NULL,
    form990_line TEXT DEFAULT NULL,
    form990_column TEXT DEFAULT NULL,
    review_status TEXT NOT NULL DEFAULT 'draft',
    reviewer TEXT DEFAULT NULL,
    reviewed_at TEXT DEFAULT NULL,
    notes TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
    FOREIGN KEY (ledger_entry_id) REFERENCES planning_ledger(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS tax_rule_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_key TEXT NOT NULL UNIQUE,
    jurisdiction TEXT NOT NULL DEFAULT 'US',
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    rule_area TEXT NOT NULL,
    latest_known_version TEXT NOT NULL DEFAULT '',
    check_frequency_days INTEGER NOT NULL DEFAULT 30,
    last_checked_at TEXT DEFAULT NULL,
    next_check_at TEXT DEFAULT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    notes TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tax_update_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    check_key TEXT NOT NULL UNIQUE,
    source_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    detected_change INTEGER NOT NULL DEFAULT 0,
    previous_version TEXT NOT NULL DEFAULT '',
    detected_version TEXT NOT NULL DEFAULT '',
    detail_json TEXT NOT NULL DEFAULT '{}',
    checked_by TEXT NOT NULL,
    checked_at TEXT NOT NULL,
    FOREIGN KEY (source_id) REFERENCES tax_rule_sources(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tax_change_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_key TEXT NOT NULL UNIQUE,
    source_id INTEGER NOT NULL,
    severity TEXT NOT NULL DEFAULT 'warning',
    status TEXT NOT NULL DEFAULT 'open',
    message TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    acknowledged_by TEXT DEFAULT NULL,
    acknowledged_at TEXT DEFAULT NULL,
    resolved_by TEXT DEFAULT NULL,
    resolved_at TEXT DEFAULT NULL,
    FOREIGN KEY (source_id) REFERENCES tax_rule_sources(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tax_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_key TEXT NOT NULL UNIQUE,
    classification_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    decision TEXT NOT NULL DEFAULT 'review',
    reviewer TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    evidence_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    FOREIGN KEY (classification_id) REFERENCES tax_activity_classifications(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS form990_support_fields (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    support_key TEXT NOT NULL UNIQUE,
    scenario_id INTEGER NOT NULL,
    period TEXT NOT NULL,
    form_part TEXT NOT NULL,
    line_number TEXT NOT NULL,
    column_code TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL,
    amount REAL NOT NULL DEFAULT 0,
    basis_json TEXT NOT NULL DEFAULT '{}',
    review_status TEXT NOT NULL DEFAULT 'draft',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tax_classifications_scenario_status
ON tax_activity_classifications (scenario_id, tax_status, review_status);

CREATE INDEX IF NOT EXISTS idx_tax_sources_next_check
ON tax_rule_sources (status, next_check_at);

CREATE INDEX IF NOT EXISTS idx_tax_alerts_status
ON tax_change_alerts (status, severity, created_at);

INSERT INTO schema_migrations (migration_key, description, checksum, applied_at)
VALUES (
    '0063_tax_classification_compliance_watch',
    'Create NPO/taxable income classification, UBIT tracking, Form 990 support, tax source registry, update checks, alerts, and review workflow.',
    'builtin-0063',
    datetime('now')
)
ON CONFLICT(migration_key) DO NOTHING;
