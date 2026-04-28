from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Sequence

from .audit import AuditService
from .base import DatabaseConnection, ServiceContext, ValidationError, require_fields


class BackupStatus(str, Enum):
    CREATED = "created"
    VERIFIED = "verified"
    FAILED = "failed"
    RESTORED = "restored"


class RestoreStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    BLOCKED = "blocked"


@dataclass(frozen=True)
class BackupManifest:
    backup_id: str
    storage_uri: str
    database_engine: str
    byte_size: int
    checksum: str
    created_at: str
    includes_files: bool = False
    includes_database: bool = True
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RestoreValidation:
    backup_id: str
    status: RestoreStatus
    checks: Mapping[str, bool]
    issues: tuple[str, ...] = ()


def checksum_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class BackupRestoreService:
    def __init__(self, db: DatabaseConnection, audit: AuditService | None = None):
        self.db = db
        self.audit = audit or AuditService(db)

    def build_manifest(
        self,
        backup_id: str,
        storage_uri: str,
        database_engine: str,
        content: bytes,
        metadata: Mapping[str, Any] | None = None,
    ) -> BackupManifest:
        return BackupManifest(
            backup_id=backup_id,
            storage_uri=storage_uri,
            database_engine=database_engine,
            byte_size=len(content),
            checksum=checksum_bytes(content),
            created_at=datetime.now(timezone.utc).isoformat(),
            includes_files=bool((metadata or {}).get("includes_files", False)),
            includes_database=bool((metadata or {}).get("includes_database", True)),
            metadata=dict(metadata or {}),
        )

    def validate_manifest(self, manifest: BackupManifest) -> RestoreValidation:
        checks = {
            "has_backup_id": bool(manifest.backup_id),
            "has_storage_uri": bool(manifest.storage_uri),
            "has_database": manifest.includes_database,
            "has_bytes": manifest.byte_size > 0,
            "has_checksum": bool(manifest.checksum),
            "has_engine": bool(manifest.database_engine),
        }
        issues = tuple(name for name, passed in checks.items() if not passed)
        return RestoreValidation(
            backup_id=manifest.backup_id,
            status=RestoreStatus.PASSED if not issues else RestoreStatus.FAILED,
            checks=checks,
            issues=issues,
        )

    def register_backup(self, context: ServiceContext, manifest: BackupManifest) -> BackupStatus:
        validation = self.validate_manifest(manifest)
        status = BackupStatus.VERIFIED if validation.status is RestoreStatus.PASSED else BackupStatus.FAILED
        self.db.execute(
            """
            INSERT INTO backup_manifests (
                backup_id, storage_uri, database_engine, byte_size, checksum, status,
                includes_files, includes_database, metadata_json, created_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                manifest.backup_id,
                manifest.storage_uri,
                manifest.database_engine,
                manifest.byte_size,
                manifest.checksum,
                status.value,
                manifest.includes_files,
                manifest.includes_database,
                json.dumps(dict(manifest.metadata), default=str, sort_keys=True),
                context.user_id,
                manifest.created_at,
            ),
        )
        self.audit.record(
            context,
            "backup.register",
            "backup_manifest",
            manifest.backup_id,
            {"status": status.value, "issues": validation.issues},
        )
        return status

    def record_restore_drill(
        self,
        context: ServiceContext,
        drill_id: str,
        manifest: BackupManifest,
        validation: RestoreValidation,
        duration_seconds: int | None = None,
    ) -> RestoreStatus:
        require_fields({"drill_id": drill_id}, ("drill_id",))
        self.db.execute(
            """
            INSERT INTO restore_drills (
                drill_id, backup_id, status, checks_json, issues_json, duration_seconds,
                executed_by, executed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                drill_id,
                manifest.backup_id,
                validation.status.value,
                json.dumps(dict(validation.checks), default=str, sort_keys=True),
                json.dumps(list(validation.issues), default=str, sort_keys=True),
                duration_seconds,
                context.user_id,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.audit.record(
            context,
            "backup.restore_drill",
            "restore_drill",
            drill_id,
            {"backup_id": manifest.backup_id, "status": validation.status.value},
        )
        return validation.status

    def simulate_failure(
        self,
        context: ServiceContext,
        scenario_id: str,
        failure_type: str,
        expected_steps: Sequence[str],
    ) -> str:
        if not expected_steps:
            raise ValidationError("Failure simulation requires expected recovery steps")
        self.db.execute(
            """
            INSERT INTO disaster_recovery_simulations (
                scenario_id, failure_type, expected_steps_json, status, created_by, created_at
            ) VALUES (?, ?, ?, 'planned', ?, ?)
            """,
            (
                scenario_id,
                failure_type,
                json.dumps(list(expected_steps), default=str),
                context.user_id,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.audit.record(
            context,
            "backup.failure_simulation.plan",
            "disaster_recovery_simulation",
            scenario_id,
            {"failure_type": failure_type},
        )
        return scenario_id

    def assert_restore_allowed(self, target_environment: str, approved: bool) -> None:
        if target_environment.lower() == "production" and not approved:
            raise ValidationError("Production restore requires explicit approval")

