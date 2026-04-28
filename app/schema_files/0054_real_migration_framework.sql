-- B53 real migration framework.
-- Runtime tables are also present in app.db.init_db so existing databases are
-- upgraded safely before this managed SQL runner is fully adopted.

CREATE TABLE IF NOT EXISTS migration_locks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lock_key TEXT NOT NULL UNIQUE,
    owner TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS migration_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    migration_key TEXT NOT NULL,
    direction TEXT NOT NULL DEFAULT 'up',
    status TEXT NOT NULL,
    dry_run INTEGER NOT NULL DEFAULT 0,
    checksum TEXT NOT NULL DEFAULT '',
    sql_path TEXT NOT NULL DEFAULT '',
    rollback_path TEXT NOT NULL DEFAULT '',
    postgres_sql_json TEXT NOT NULL DEFAULT '[]',
    message TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL,
    completed_at TEXT DEFAULT NULL
);
