CREATE TABLE IF NOT EXISTS export_artifact_validations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_id INTEGER NOT NULL,
    validation_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    checks_json TEXT NOT NULL DEFAULT '{}',
    issues_json TEXT NOT NULL DEFAULT '[]',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (artifact_id) REFERENCES export_artifacts(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_export_artifact_validations_artifact
    ON export_artifact_validations(artifact_id, created_at);

INSERT OR IGNORE INTO schema_migrations (migration_key, description, checksum, applied_at)
VALUES (
    '0069_production_pdf_board_artifact_completion',
    'Create production PDF and board artifact validation, downloadable artifact controls, embedded chart evidence, footnotes, and page break completion checks.',
    'builtin-0069',
    CURRENT_TIMESTAMP
);
