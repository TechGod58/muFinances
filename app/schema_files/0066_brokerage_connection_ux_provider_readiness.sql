ALTER TABLE brokerage_connections ADD COLUMN credential_type TEXT NOT NULL DEFAULT '';
ALTER TABLE brokerage_connections ADD COLUMN provider_environment TEXT NOT NULL DEFAULT 'sandbox';
ALTER TABLE brokerage_connections ADD COLUMN auth_flow_status TEXT NOT NULL DEFAULT 'not_started';
ALTER TABLE brokerage_connections ADD COLUMN auth_url TEXT NOT NULL DEFAULT '';
ALTER TABLE brokerage_connections ADD COLUMN consent_status TEXT NOT NULL DEFAULT 'not_requested';
ALTER TABLE brokerage_connections ADD COLUMN read_only_ack INTEGER NOT NULL DEFAULT 0;
ALTER TABLE brokerage_connections ADD COLUMN sync_warning TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS brokerage_consent_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    consent_version TEXT NOT NULL,
    read_only_ack INTEGER NOT NULL DEFAULT 0,
    real_money_trading_ack INTEGER NOT NULL DEFAULT 0,
    data_scope_ack INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    consent_text TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (connection_id) REFERENCES brokerage_connections(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

INSERT OR IGNORE INTO schema_migrations (migration_key, description, checksum, applied_at)
VALUES (
    '0066_brokerage_connection_ux_provider_readiness',
    'Create brokerage provider selection, sandbox/live readiness, credential/OAuth setup flow, read-only consent records, sync warnings, and brokerage audit trail surfaces.',
    'builtin-0066',
    datetime('now')
);
