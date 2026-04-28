CREATE TABLE IF NOT EXISTS export_artifacts (
    artifact_id text PRIMARY KEY,
    export_type text NOT NULL,
    file_name text NOT NULL,
    content_type text NOT NULL,
    byte_size bigint NOT NULL,
    checksum text NOT NULL,
    page_count integer,
    sheet_count integer,
    slide_count integer,
    metadata_json text NOT NULL,
    validation_json text NOT NULL,
    valid boolean NOT NULL,
    created_by text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS export_regression_runs (
    id text PRIMARY KEY,
    export_type text NOT NULL,
    baseline_artifact_id integer,
    candidate_artifact_id integer NOT NULL REFERENCES export_artifacts(id),
    status text NOT NULL,
    diff_json text NOT NULL DEFAULT '{}',
    created_by text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_export_artifacts_type_created
    ON export_artifacts (artifact_type, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_export_regression_status
    ON export_regression_runs (status, export_type);
