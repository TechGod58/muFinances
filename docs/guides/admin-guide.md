# Admin Guide

## Purpose And Scope

This guide is for muFinances system administrators who operate the localhost or internal campus deployment, manage access, review operational health, and support finance users. It covers routine checks, security administration, incident response, and release support.

Administrators should use named accounts only. Shared administrator sessions are not acceptable for production operation because audit reports, access reviews, and approval logs depend on actor identity.

## Daily Checks

Perform these checks every business day during rollout and at least twice per week after stabilization:

1. Confirm `/api/health/live` and `/api/health/ready` return healthy responses.
2. Review `/api/observability/workspace` for failed health probes, open alerts, and backup drill status.
3. Review `/api/production-ops/status` for database, pooling, TLS, secrets, and guide readiness.
4. Review `/api/deployment-governance/workspace` for open readiness items and recent diagnostics.
5. Confirm the newest backup and restore drill are current enough for the campus recovery objective.
6. Review connector sync failures, rejected import rows, and dead-lettered background jobs.

Record exceptions in the operational log with a trace ID and a short remediation note.

## User And Access Control

Create and maintain users through the security workspace or security API. Use the least-privilege role that supports the workflow:

- `finance.admin`: system administration, security, operations, and release control.
- `budget.office`: budget office setup, approvals, reporting, and scenario management.
- `department.planner`: department-scoped entry, comments, and submission workflow.
- `controller`: close, reconciliation, consolidation, and audit packet workflows.
- `executive`: read-focused dashboards, board packages, and approved reporting.

Use dimension access for department-scoped planners. Grant `row_access.all` only to central finance, controller, and system administrators. Review inactive users monthly and remove access when employment, role, or department ownership changes.

## Security Operations

Production should require SSO and AD/OU verification. Validate these settings before go-live:

- Domain/VPN guard is enabled and only accepts `manchester.edu` or approved campus/VPN ranges.
- AD OU guard points at the approved finance access group or OU.
- Local development secrets are not accepted in production mode.
- Admin accounts are not using first-run or temporary passwords.
- Sensitive fields remain masked unless the user has `sensitive.read`.

Run an access review certification at the start of every fiscal quarter and before major budget planning launches. Keep evidence of SSO, AD/OU, and SoD review in the access review record.

## Audit Reporting

Use `/api/production-ops/admin-audit-report` to review:

- Recent security events.
- Activity by actor.
- Activity by entity type.
- Authentication and access changes.
- Recent high-impact changes to ledger, dimensions, scenarios, connectors, reports, and close controls.

Export or snapshot audit reports before close signoff, before production releases, after emergency access changes, and after any restore operation.

## Backup And Restore Administration

Backups can be created from Operations or the Foundation backup API. Restore actions are protected by integrity checks and pre-restore snapshots.

Minimum operator routine:

1. Confirm a successful backup exists before a release, migration, or large import.
2. Run a restore drill after the backup and verify `integrity_check` is `ok`.
3. Keep the drill record and trace ID with the release or operational ticket.
4. Do not run a destructive restore without written approval from finance leadership and IT operations.

## Incident Response

When an alert, failed probe, or failed sync appears:

1. Capture the trace ID, endpoint, user, and time.
2. Review application logs, audit logs, connector sync logs, and job logs.
3. Decide whether the incident is user workflow, data quality, connector, infrastructure, or security.
4. If data may be incorrect, pause imports/postings for the affected scenario or period.
5. Communicate status to the budget office or controller.
6. Record the resolution in application logs and attach evidence where appropriate.

Security incidents should be escalated to campus IT security immediately. Do not delete logs or audit records.

## Release Support

Before a release, run the release checklist, create a backup, run a restore drill, run diagnostics, and verify all B62 documentation readiness checks pass. After release, run health probes and one user login smoke test.
