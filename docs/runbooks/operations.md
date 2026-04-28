# muFinances Operations Runbook

## Health Checks

Run:

```powershell
.\deploy\health-check.ps1
```

## Daily Checks

- Confirm `/api/health` returns `ok`.
- Confirm the latest operational check is `pass`.
- Confirm a current backup exists.
- Confirm the latest restore test is `pass`.
- Review failed imports and rejected automation recommendations.

## Recovery

Start with the latest passing restore test and use the pre-restore snapshot created by the restore endpoint if rollback is needed.
