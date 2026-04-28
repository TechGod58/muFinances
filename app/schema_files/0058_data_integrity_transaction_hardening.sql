-- B57 adds ledger posting integrity controls.

ALTER TABLE planning_ledger ADD COLUMN idempotency_key TEXT DEFAULT NULL;
ALTER TABLE planning_ledger ADD COLUMN posted_checksum TEXT DEFAULT NULL;
ALTER TABLE planning_ledger ADD COLUMN immutable_posting INTEGER NOT NULL DEFAULT 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_planning_ledger_idempotency
ON planning_ledger (idempotency_key)
WHERE idempotency_key IS NOT NULL AND idempotency_key <> '';

INSERT INTO schema_migrations (migration_key, description, checksum, applied_at)
VALUES (
    '0058_data_integrity_transaction_hardening',
    'Create explicit ledger posting transaction boundaries, idempotency keys, immutable posting checksums, restore safeguards, and stronger audit-chain enforcement.',
    'managed-by-runtime',
    CURRENT_TIMESTAMP
);
