# Security Review And Production Readiness

B82 adds production-readiness review checks before muFinances is deployed internally.

## Reviewed Areas

- Required production secrets.
- Unsafe default secret values.
- Required response security headers.
- Production HSTS requirement.
- Permission registry completeness.
- Sensitive-field masking policy.
- Audit/admin separation of duties.
- Production fail-fast behavior.

## Files

- `services/production_readiness.py`
- `tests/test_production_readiness.py`
- `schema/postgresql/0082_security_production_readiness.up.sql`
- `schema/postgresql/0082_security_production_readiness.down.sql`

## Production Rule

When `MUFINANCES_MODE=production`, blocker findings must prevent startup. Warnings can be shown in the admin readiness dashboard, but blockers must stop deployment.

