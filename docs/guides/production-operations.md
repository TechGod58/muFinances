# Production Operations Guide

## Runtime Modes

muFinances runs in local SQLite mode by default:

```text
CAMPUS_FPM_DB_BACKEND=sqlite
CAMPUS_FPM_DB_PATH=/app/data/campus_fpm.db
CAMPUS_FPM_DB_POOL_SIZE=5
```

For a campus server, use the PostgreSQL deployment target and set:

```text
CAMPUS_FPM_DB_BACKEND=postgres
CAMPUS_FPM_POSTGRES_DSN=postgresql://mufinances@postgres:5432/mufinances
CAMPUS_FPM_DB_SSL_MODE=require
```

For a Manchester-hosted server that standardizes on Microsoft SQL Server, use
the SQL Server deployment target and set:

```text
CAMPUS_FPM_DB_BACKEND=mssql
CAMPUS_FPM_MSSQL_DSN=Driver={ODBC Driver 18 for SQL Server};Server=tcp:sql-server-name,1433;Database=muFinances;Encrypt=yes;TrustServerCertificate=no;Authentication=ActiveDirectoryIntegrated;
```

Install the Microsoft ODBC Driver for SQL Server on the application server
before using `mssql` mode. The app uses `pyodbc` and validates SQL Server
translation through `/api/database-runtime/status`.

Keep SQLite for single-user local desktop runs. Use PostgreSQL or SQL Server for shared campus server deployments, load testing, and production operations.

## TLS

Terminate TLS at the campus reverse proxy or load balancer. Required settings:

- HTTPS certificate issued by the campus certificate authority or public CA.
- HTTP to HTTPS redirect.
- `Secure`, `HttpOnly`, and same-site cookie settings when cookie auth is introduced.
- OIDC redirect URI updated to the HTTPS hostname.
- PostgreSQL `sslmode=require`, `verify-ca`, or `verify-full` for remote database connections, or SQL Server `Encrypt=yes` with certificate validation enabled.

## Manchester Network And AD OU Guard

For production, put muFinances behind the campus reverse proxy and enable the Manchester access guard. The guard accepts traffic from Manchester hostnames or configured on-prem/VPN CIDR ranges. Localhost remains enabled only for desktop or server console checks.

```text
CAMPUS_FPM_DOMAIN_GUARD_ENABLED=true
CAMPUS_FPM_ALLOWED_HOST_SUFFIXES=manchester.edu
CAMPUS_FPM_ALLOWED_CLIENT_CIDRS=10.0.0.0/8,172.16.0.0/12,192.168.0.0/16
CAMPUS_FPM_ALLOW_LOCALHOST=false
```

Enable AD OU verification after LDAP service credentials and the allowed OU are ready. Use `ldaps://`, store the bind password in approved secret storage, and validate `/api/security/access-guard/status`.

## Secrets

Do not place production secrets in source files. Use Docker secrets, Windows service environment variables, or campus vault tooling:

```text
CAMPUS_FPM_FIELD_KEY_FILE=/run/secrets/mufinances_field_key
POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password
```

Rotate `CAMPUS_FPM_FIELD_KEY` only through a controlled maintenance window because it protects encrypted field values and credential references.

## Logs

Operational evidence is split into:

- Application logs: `/api/production-ops/application-logs`
- Job history: `/api/performance/jobs` and `/api/performance/job-logs`
- Connector sync logs: `/api/integrations/sync-logs`
- Audit trail: `/api/production-ops/admin-audit-report`
- Alert events: `/api/observability/alerts`

Review production logs daily during rollout and weekly after stabilization.

## Observability

Use `/api/observability/workspace` for metrics, health probes, alerts, and backup drill records. Every request returns `X-Trace-Id`; include that value in tickets, release notes, and incident records.

Run health probes before and after releases. Open alert events should be acknowledged only after the root cause and remediation are recorded.

## Backup And Restore Drills

Before release, migration, large import, or production configuration change:

1. Create a backup.
2. Run a backup/restore drill.
3. Confirm SQLite integrity or PostgreSQL validation status.
4. Record the trace ID and drill key in the release checklist.
5. Keep the backup until the release is accepted and the retention policy allows disposal.

Do not perform a destructive restore unless the controller, IT operations, and system administrator approve the action.

## Operator Handoff

For each production support handoff, provide current release version, open alerts, most recent backup key, most recent restore drill, latest migration, known connector issues, and current period lock state.
