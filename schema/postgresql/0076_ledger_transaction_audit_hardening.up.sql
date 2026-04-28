CREATE TABLE IF NOT EXISTS idempotency_keys (
    key text PRIMARY KEY,
    operation text NOT NULL,
    request_hash text NOT NULL,
    status text NOT NULL,
    response_ref text,
    metadata_json text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_chain (
    entity_type text NOT NULL,
    entity_id text NOT NULL,
    sequence integer NOT NULL,
    previous_hash text NOT NULL,
    record_hash text NOT NULL,
    payload_json text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, entity_id, sequence)
);

CREATE INDEX IF NOT EXISTS ix_audit_chain_entity
    ON audit_chain (entity_type, entity_id, sequence);

ALTER TABLE planning_ledger
    ADD COLUMN IF NOT EXISTS immutable boolean NOT NULL DEFAULT false;

ALTER TABLE planning_ledger
    ADD COLUMN IF NOT EXISTS posted_by text;

ALTER TABLE planning_ledger
    ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'working';
