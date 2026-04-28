CREATE TABLE IF NOT EXISTS import_mapping_versions (
    mapping_key text PRIMARY KEY,
    mapping_id text NOT NULL,
    version integer NOT NULL,
    source_system text NOT NULL,
    fields_json text NOT NULL,
    active boolean NOT NULL DEFAULT true,
    created_by text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (mapping_id, version)
);

CREATE TABLE IF NOT EXISTS import_batches (
    id text PRIMARY KEY,
    source_system text NOT NULL,
    file_name text NOT NULL,
    mapping_key text REFERENCES import_mapping_versions(mapping_key),
    source_ref text,
    status text NOT NULL,
    created_by text,
    created_at timestamptz NOT NULL DEFAULT now(),
    approved_by text,
    approved_at timestamptz,
    rejection_reason text,
    rollback_reason text
);

CREATE TABLE IF NOT EXISTS import_staged_rows (
    batch_id integer NOT NULL REFERENCES import_batches(id),
    row_number integer NOT NULL,
    row_hash text NOT NULL,
    row_json text NOT NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, row_number)
);

CREATE TABLE IF NOT EXISTS import_rejections (
    id bigserial PRIMARY KEY,
    batch_id integer NOT NULL REFERENCES import_batches(id),
    row_number integer NOT NULL,
    severity text NOT NULL,
    field_name text NOT NULL,
    message text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_import_staged_rows_hash
    ON import_staged_rows (batch_id, row_hash);

CREATE INDEX IF NOT EXISTS ix_import_batches_status
    ON import_batches (status, connector_key);
