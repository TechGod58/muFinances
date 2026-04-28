CREATE TABLE IF NOT EXISTS university_agent_clients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    shared_secret_hash TEXT NOT NULL,
    scopes_json TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'active',
    callback_url TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS university_agent_tools (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tool_key TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    required_scope TEXT NOT NULL,
    action_type TEXT NOT NULL,
    approval_required INTEGER NOT NULL DEFAULT 1,
    enabled INTEGER NOT NULL DEFAULT 1,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS university_agent_policies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    policy_key TEXT NOT NULL UNIQUE,
    client_key TEXT NOT NULL,
    tool_key TEXT NOT NULL,
    allowed_actions_json TEXT NOT NULL DEFAULT '[]',
    max_amount REAL DEFAULT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (client_key) REFERENCES university_agent_clients(client_key) ON DELETE CASCADE,
    FOREIGN KEY (tool_key) REFERENCES university_agent_tools(tool_key) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS university_agent_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_key TEXT NOT NULL UNIQUE,
    client_key TEXT NOT NULL,
    tool_key TEXT NOT NULL,
    scenario_id INTEGER DEFAULT NULL,
    signature_status TEXT NOT NULL,
    policy_status TEXT NOT NULL,
    approval_status TEXT NOT NULL DEFAULT 'not_required',
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    result_json TEXT NOT NULL DEFAULT '{}',
    callback_url TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    completed_at TEXT DEFAULT NULL,
    FOREIGN KEY (client_key) REFERENCES university_agent_clients(client_key) ON DELETE CASCADE,
    FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS university_agent_callbacks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id INTEGER NOT NULL,
    callback_url TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    payload_json TEXT NOT NULL DEFAULT '{}',
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    delivered_at TEXT DEFAULT NULL,
    FOREIGN KEY (request_id) REFERENCES university_agent_requests(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS university_agent_audit_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id INTEGER DEFAULT NULL,
    client_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    FOREIGN KEY (request_id) REFERENCES university_agent_requests(id) ON DELETE SET NULL
);

INSERT OR IGNORE INTO schema_migrations (migration_key, description, checksum, applied_at)
VALUES (
    '0067_university_agent_integration_layer',
    'Create signed external University Agent API, tool registry, scoped permissions, allowed-action policies, audit logs, approval-before-posting enforcement, and callback support.',
    'builtin-0067',
    datetime('now')
);
