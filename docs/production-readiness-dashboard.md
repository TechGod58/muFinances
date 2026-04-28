# Production Readiness Dashboard

B87 adds a production readiness dashboard.

## Dashboard Areas

- Database mode.
- Migration status.
- Auth mode.
- Worker status.
- Backup status.
- Health checks.
- Logs.
- Alerts.

## Files

- `services/production_dashboard.py`
- `static/js/production-readiness-dashboard.js`
- `tests/test_production_dashboard_release_governance.py`
- `schema/postgresql/0087_production_readiness_dashboard.up.sql`

## UI

The app adds a `Production` button to the command bar. It opens a right-side dashboard panel. Until API routes are wired, it shows local fallback readiness cards and labels pending server endpoints.

