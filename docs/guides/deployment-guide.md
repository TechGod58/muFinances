# muFinances Deployment Guide

## Purpose

This guide locks the operator-facing deployment procedure for muFinances. It is written for the internal server deployment path where the application runs on localhost or an internal Manchester host and is protected by the Manchester network, SSO, AD/OU mapping, row-level access, and production readiness checks.

## Runtime Layout

The application runtime has three layers: the web/API process, the background worker process, and the database runtime. The server may use MS SQL, PostgreSQL, or the local SQLite development mode. Production deployments should use a real DSN, connection pooling, encrypted secrets, and a field key loaded from a protected file or server secret store.

## Deployment Steps

1. Confirm the target environment is recorded in deployment governance.
2. Export the current configuration snapshot.
3. Create a verified backup and run a restore drill.
4. Apply the release package to staging first.
5. Run migrations and schema drift checks.
6. Run health probes, worker diagnostics, connector checks, and login smoke tests.
7. Promote staging to production only after the readiness checklist is signed.

## Service Startup

Start the API process and worker process independently so worker failures do not take the web interface down. The worker should be configured with retry/backoff, dead-letter handling, and job logs visible from the production readiness dashboard.

## Verification

After deployment, verify `/api/health/ready`, the production readiness dashboard, migration status, worker status, backup status, application logs, and open alerts. Run one import test in connector test mode and one report export smoke test before releasing the system to finance users.

## Rollback

Rollback requires a verified backup, the previous release package, and a rollback plan tied to the release version. Stop workers, restore the database, redeploy the previous package, run health probes, and record the rollback decision in deployment governance.
