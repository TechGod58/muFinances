from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Sequence

from .audit import AuditService
from .base import DatabaseConnection, ServiceContext, ValidationError, require_fields


class PromotionStatus(str, Enum):
    DRAFT = "draft"
    READY = "ready"
    BLOCKED = "blocked"
    PROMOTED = "promoted"
    ROLLED_BACK = "rolled_back"


@dataclass(frozen=True)
class EnvironmentPromotion:
    promotion_id: str
    source_environment: str
    target_environment: str
    release_version: str
    status: PromotionStatus
    checklist: Mapping[str, bool] = field(default_factory=dict)


@dataclass(frozen=True)
class OperationalSignoff:
    role: str
    user_id: str
    approved: bool
    notes: str = ""


class ReleaseGovernanceService:
    REQUIRED_CHECKS = (
        "release_notes",
        "migration_dry_run",
        "rollback_plan",
        "financial_tests",
        "ui_smoke_tests",
        "backup_verified",
        "security_readiness",
        "pilot_support",
    )

    REQUIRED_SIGNOFF_ROLES = (
        "admin",
        "controller",
        "integration_owner",
        "security_owner",
    )

    def __init__(self, db: DatabaseConnection, audit: AuditService | None = None):
        self.db = db
        self.audit = audit or AuditService(db)

    def evaluate_checklist(self, checklist: Mapping[str, bool]) -> PromotionStatus:
        missing = [name for name in self.REQUIRED_CHECKS if not checklist.get(name)]
        return PromotionStatus.READY if not missing else PromotionStatus.BLOCKED

    def create_promotion(self, context: ServiceContext, payload: Mapping[str, Any]) -> EnvironmentPromotion:
        require_fields(payload, ("promotion_id", "source_environment", "target_environment", "release_version"))
        checklist = dict(payload.get("checklist", {}))
        status = self.evaluate_checklist(checklist)
        promotion = EnvironmentPromotion(
            promotion_id=str(payload["promotion_id"]),
            source_environment=str(payload["source_environment"]),
            target_environment=str(payload["target_environment"]),
            release_version=str(payload["release_version"]),
            status=status,
            checklist=checklist,
        )
        self.db.execute(
            """
            INSERT INTO environment_promotions (
                promotion_id, source_environment, target_environment, release_version,
                status, checklist_json, created_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                promotion.promotion_id,
                promotion.source_environment,
                promotion.target_environment,
                promotion.release_version,
                promotion.status.value,
                json.dumps(checklist, default=str, sort_keys=True),
                context.user_id,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.audit.record(context, "release.promotion.create", "environment_promotion", promotion.promotion_id, payload)
        return promotion

    def export_config(self, config: Mapping[str, Any]) -> str:
        blocked_keys = {"password", "secret", "token", "api_key", "client_secret"}
        sanitized = {
            key: ("<redacted>" if any(blocked in key.lower() for blocked in blocked_keys) else value)
            for key, value in config.items()
        }
        return json.dumps(sanitized, default=str, sort_keys=True, indent=2)

    def import_config(self, config_json: str) -> dict[str, Any]:
        config = json.loads(config_json)
        if not isinstance(config, dict):
            raise ValidationError("Imported config must be a JSON object")
        return config

    def record_release_notes(self, context: ServiceContext, release_version: str, notes_markdown: str) -> None:
        if not notes_markdown.strip():
            raise ValidationError("Release notes cannot be empty")
        self.db.execute(
            """
            INSERT INTO release_notes (release_version, notes_markdown, created_by, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (release_version, notes_markdown, context.user_id, datetime.now(timezone.utc).isoformat()),
        )
        self.audit.record(context, "release.notes.record", "release_notes", release_version, {})

    def record_rollback_plan(self, context: ServiceContext, release_version: str, plan: Mapping[str, Any]) -> None:
        required = ("trigger", "steps", "owner")
        require_fields(plan, required)
        self.db.execute(
            """
            INSERT INTO rollback_plans (release_version, plan_json, created_by, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (release_version, json.dumps(dict(plan), default=str, sort_keys=True), context.user_id, datetime.now(timezone.utc).isoformat()),
        )
        self.audit.record(context, "release.rollback_plan.record", "rollback_plan", release_version, plan)

    def signoff_complete(self, signoffs: Sequence[OperationalSignoff]) -> bool:
        approvals = {signoff.role for signoff in signoffs if signoff.approved}
        return all(role in approvals for role in self.REQUIRED_SIGNOFF_ROLES)

    def record_signoff(self, context: ServiceContext, promotion_id: str, signoff: OperationalSignoff) -> None:
        self.db.execute(
            """
            INSERT INTO operational_signoffs (
                promotion_id, role_name, user_id, approved, notes, signed_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                promotion_id,
                signoff.role,
                signoff.user_id,
                signoff.approved,
                signoff.notes,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.audit.record(
            context,
            "release.signoff.record",
            "environment_promotion",
            promotion_id,
            {"role": signoff.role, "approved": signoff.approved},
        )

    def pilot_checklist(self, pilot_users: Sequence[str], support_owner: str, monitoring_enabled: bool) -> dict[str, bool]:
        return {
            "pilot_users_identified": bool(pilot_users),
            "support_owner_assigned": bool(support_owner),
            "monitoring_enabled": monitoring_enabled,
            "feedback_path_ready": bool(pilot_users and support_owner),
        }

