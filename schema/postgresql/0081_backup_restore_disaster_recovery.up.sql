CREATE TABLE IF NOT EXISTS backup_manifests (
    backup_id text PRIMARY KEY,
    storage_uri text NOT NULL,
    database_engine text NOT NULL,
    byte_size bigint NOT NULL,
    checksum text NOT NULL,
    status text NOT NULL,
    includes_files boolean NOT NULL DEFAULT false,
    includes_database boolean NOT NULL DEFAULT true,
    metadata_json text NOT NULL DEFAULT '{}',
    created_by text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS restore_drills (
    drill_id text PRIMARY KEY,
    backup_id text NOT NULL REFERENCES backup_manifests(backup_id),
    status text NOT NULL,
    checks_json text NOT NULL DEFAULT '{}',
    issues_json text NOT NULL DEFAULT '[]',
    duration_seconds integer,
    executed_by text,
    executed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS disaster_recovery_simulations (
    scenario_id text PRIMARY KEY,
    failure_type text NOT NULL,
    expected_steps_json text NOT NULL DEFAULT '[]',
    status text NOT NULL,
    created_by text,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);

CREATE INDEX IF NOT EXISTS ix_backup_manifests_status
    ON backup_manifests (status, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_restore_drills_status
    ON restore_drills (status, executed_at DESC);

