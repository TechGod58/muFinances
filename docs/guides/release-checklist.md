# Release Checklist

## Purpose And Scope

This checklist is used before promoting a muFinances release to a shared campus server or production environment. It ties documentation, backup, migration, diagnostics, smoke testing, and rollback decisions into one operator-ready procedure.

## Pre-Release Freeze

Complete these items before code or configuration promotion:

- Confirm release scope and release owner.
- Confirm no unrelated local changes are included.
- Confirm operator guides pass `/api/production-ops/documentation-readiness`.
- Confirm open critical alerts are resolved or formally accepted.
- Confirm scheduled imports and background jobs will not conflict with the maintenance window.
- Notify finance, controller, integration owners, and IT operations.

## Migration And Backup

1. Create a pre-release backup.
2. Run a backup/restore drill and confirm validation passes.
3. Run migration dry-run checks when applicable.
4. Apply migrations.
5. Confirm `/api/foundation/migrations` shows the expected latest migration.
6. Store backup key, drill key, trace ID, and migration result with the release note.

## Smoke Tests

Run these after deployment:

- `/api/health/live`
- `/api/health/ready`
- `/api/observability/health-probes/run`
- Login and sign out.
- Bootstrap load for the active scenario.
- Ledger summary read.
- Report workspace read.
- Connector health check for at least one configured connector.
- Backup drill or restore test, depending on maintenance window length.

If any smoke test fails, pause the release and evaluate rollback.

## Rollback Decision

Rollback is required when:

- Authentication or access guard blocks valid users.
- Migrations fail or leave the database in an unknown state.
- Ledger posting, reporting, or close workflows fail critical smoke tests.
- Data integrity, audit chain, or backup validation fails.
- A critical alert remains open without an approved workaround.

Rollback steps:

1. Stop worker and app processes.
2. Preserve logs and trace IDs.
3. Restore the pre-release backup or redeploy the prior build.
4. Run health probes.
5. Notify stakeholders.
6. Record the final status in release notes.

## Release Signoff

Release signoff requires:

- Tests passed.
- Health probes passed.
- Backup and restore drill passed.
- Latest migration confirmed.
- Documentation readiness passed.
- Known issues documented.
- Controller or budget office acceptance recorded when user-facing workflows changed.

After signoff, publish release notes and keep the release checklist with operational readiness evidence.
