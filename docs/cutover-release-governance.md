# Cutover And Release Governance

B88 adds release and cutover governance.

## Capabilities

- Environment promotion records.
- Config export/import with secret redaction.
- Release notes.
- Rollback plans.
- Operational signoffs.
- Pilot deployment checklist.

## Required Promotion Checks

- Release notes.
- Migration dry-run.
- Rollback plan.
- Financial correctness tests.
- UI smoke tests.
- Verified backup.
- Security readiness.
- Pilot support path.

## Files

- `services/release_governance.py`
- `tests/test_production_dashboard_release_governance.py`
- `schema/postgresql/0088_cutover_release_governance.up.sql`

