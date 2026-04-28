# Backup Restore And Disaster Recovery

B81 adds backup/restore drill tracking and disaster-recovery evidence.

## Capabilities

- Backup manifest creation.
- Backup manifest validation.
- Backup audit records.
- Restore drill records.
- Restore validation checks.
- Failure simulation planning.
- Production restore approval guard.

## Files

- `services/backup_restore.py`
- `tests/test_backup_restore.py`
- `schema/postgresql/0081_backup_restore_disaster_recovery.up.sql`
- `schema/postgresql/0081_backup_restore_disaster_recovery.down.sql`

## Recovery Drill Checklist

1. Create backup.
2. Register manifest.
3. Validate manifest checksum, size, storage URI, and database engine.
4. Restore into a non-production target.
5. Run migration drift check.
6. Run application health check.
7. Record restore drill result.
8. Review audit trail.

## Production Rule

Production restore requires explicit approval. The service layer blocks unapproved production restore requests.

