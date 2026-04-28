# muFinances Deployment Guide

## Deployment Checklist

- Set `MUFINANCES_MODE=production`.
- Configure PostgreSQL connection.
- Configure secret vault values.
- Configure SSO provider and AD/OU policy.
- Configure allowed Manchester domain/network rules.
- Apply database migrations.
- Start web and worker processes.
- Verify health probes.
- Confirm backup job and restore drill schedule.

## Required Verification

```powershell
python -m pytest
npx playwright test
python -m migration_proof.cli dry-run --migrations schema/postgresql
python -m migration_proof.cli drift --migrations schema/postgresql
```

## Production Blockers

- Missing required secrets.
- Unsafe default secrets.
- Failed migrations.
- Failed health probes.
- No verified backup.
- Dead-letter worker jobs.
- Missing SSO/AD policy.

