ALTER TABLE planning_ledger
    DROP COLUMN IF EXISTS status;

ALTER TABLE planning_ledger
    DROP COLUMN IF EXISTS posted_by;

ALTER TABLE planning_ledger
    DROP COLUMN IF EXISTS immutable;

DROP INDEX IF EXISTS ix_audit_chain_entity;

DROP TABLE IF EXISTS audit_chain;

DROP TABLE IF EXISTS idempotency_keys;
