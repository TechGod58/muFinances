# muFinances Recovery Guide

## Purpose

This guide locks the disaster recovery process for muFinances. Recovery must protect ledger integrity, audit history, evidence retention, connector lineage, and release governance records.

## Recovery Triggers

Start recovery when the database fails integrity checks, the server loses durable storage, migrations corrupt the schema, a release blocks login or posting, backups cannot be validated, or a production incident affects financial correctness.

## Backup Sources

Use the newest verified backup that has passed a restore drill. The backup record should include backup key, path, size, integrity check result, table count, actor, and timestamp. Do not restore from unverified files unless the operational signoff explicitly accepts the risk.

## Restore Procedure

1. Stop web and worker processes.
2. Preserve current logs, alerts, and failed jobs in a support bundle.
3. Restore the selected backup to an isolated validation location.
4. Run integrity checks and table count validation.
5. Restore to the active runtime only after validation passes.
6. Restart the API process first, then the worker.
7. Run health probes, login smoke, connector health, and a report smoke test.

## Data Integrity

After restore, verify audit-chain status, immutable ledger posting checks, migration status, and source-to-report lineage. Locked fiscal periods must remain locked. Dead-letter jobs should be reviewed before replaying.

## Communication And Signoff

Record the incident, backup key, restore drill result, release version, rollback plan, operator, and final signoff. Finance users should receive a short note stating whether any data entry window needs to be repeated.

## Post-Recovery Follow-Up

Create an operator-facing issue report, attach logs and the support bundle manifest, and schedule a follow-up review to decide whether monitoring, backups, or deployment gates need to be tightened.
