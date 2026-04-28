# Ledger Transaction And Audit Hardening

B76 adds safety primitives for posting financial records.

## Rules

- Ledger posting must run inside an explicit transaction boundary.
- Every posting request requires an idempotency key.
- Reusing an idempotency key with a different request is rejected.
- Posted, locked, or closed ledger lines are immutable.
- Ledger posting appends an audit-chain record.
- Audit-chain verification must detect tampered source-to-report records.

## Files

- `services/transactions.py`
- `services/idempotency.py`
- `services/audit_chain.py`
- `services/ledger.py`
- `tests/test_ledger_transaction_audit.py`

## Route Migration

When routes are migrated to `LedgerService.post_line`, callers must pass:

- `id`
- `scenario_id`
- `department_code`
- `account_code`
- `fiscal_period`
- `amount`
- `idempotency_key`

The route should not perform direct ledger inserts.

