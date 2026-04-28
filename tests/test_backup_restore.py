import pytest

from services.backup_restore import BackupRestoreService, BackupStatus, RestoreStatus
from services.base import ServiceContext, ValidationError


class FakeCursor:
    description = []

    def fetchall(self):
        return []


class FakeDb:
    def __init__(self):
        self.statements = []

    def execute(self, sql, parameters=()):
        self.statements.append((sql, parameters))
        return FakeCursor()

    def executemany(self, sql, parameters):
        self.statements.append((sql, list(parameters)))

    def commit(self):
        pass

    def rollback(self):
        pass


def test_manifest_validation_passes_for_complete_backup():
    service = BackupRestoreService(FakeDb())
    manifest = service.build_manifest("backup-1", "file:///backups/backup-1.dump", "postgresql", b"backup")

    validation = service.validate_manifest(manifest)

    assert validation.status is RestoreStatus.PASSED
    assert validation.issues == ()


def test_manifest_validation_fails_for_empty_backup():
    service = BackupRestoreService(FakeDb())
    manifest = service.build_manifest("backup-1", "file:///backups/backup-1.dump", "postgresql", b"")

    validation = service.validate_manifest(manifest)

    assert validation.status is RestoreStatus.FAILED
    assert "has_bytes" in validation.issues


def test_register_backup_records_verified_status():
    db = FakeDb()
    service = BackupRestoreService(db)
    context = ServiceContext(user_id="admin", roles=("admin",))
    manifest = service.build_manifest("backup-1", "file:///backups/backup-1.dump", "postgresql", b"backup")

    status = service.register_backup(context, manifest)

    assert status is BackupStatus.VERIFIED
    assert any("insert into backup_manifests" in " ".join(sql.lower().split()) for sql, _ in db.statements)


def test_production_restore_requires_approval():
    service = BackupRestoreService(FakeDb())

    with pytest.raises(ValidationError):
        service.assert_restore_allowed("production", approved=False)


def test_failure_simulation_requires_steps():
    service = BackupRestoreService(FakeDb())
    context = ServiceContext(user_id="admin", roles=("admin",))

    with pytest.raises(ValidationError):
        service.simulate_failure(context, "scenario-1", "database_loss", [])

