CREATE TABLE IF NOT EXISTS background_jobs (
    job_id text PRIMARY KEY,
    job_type text NOT NULL,
    status text NOT NULL,
    payload_json text NOT NULL DEFAULT '{}',
    result_json text,
    attempts integer NOT NULL DEFAULT 0,
    priority integer NOT NULL DEFAULT 0,
    run_after timestamptz NOT NULL DEFAULT now(),
    worker_id text,
    leased_at timestamptz,
    completed_at timestamptz,
    failed_at timestamptz,
    last_error text,
    cancellation_reason text,
    created_by text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS background_job_logs (
    id bigserial PRIMARY KEY,
    job_id integer NOT NULL REFERENCES background_jobs(id),
    event_type text NOT NULL,
    message text NOT NULL,
    details_json text NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS background_worker_heartbeats (
    id bigserial PRIMARY KEY,
    worker_id text NOT NULL,
    metadata_json text NOT NULL DEFAULT '{}',
    seen_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_background_jobs_queue
    ON background_jobs (status, scheduled_for, priority DESC, queued_at);

CREATE INDEX IF NOT EXISTS ix_background_job_logs_job
    ON background_job_logs (job_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_worker_heartbeats_seen
    ON background_worker_heartbeats (worker_id, seen_at DESC);
