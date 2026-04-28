from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Sequence

from .audit import AuditService
from .base import DatabaseConnection, ServiceContext, ValidationError


class ExportType(str, Enum):
    PDF = "pdf"
    EXCEL = "excel"
    POWERPOINT = "powerpoint"
    CHART_PNG = "chart_png"
    CHART_SVG = "chart_svg"
    BOARD_PACKAGE = "board_package"
    BI_API = "bi_api"


@dataclass(frozen=True)
class ExportArtifact:
    artifact_id: str
    export_type: ExportType
    file_name: str
    content_type: str
    byte_size: int
    checksum: str
    page_count: int | None = None
    sheet_count: int | None = None
    slide_count: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExportValidationIssue:
    severity: str
    code: str
    message: str


@dataclass(frozen=True)
class ExportValidationResult:
    artifact_id: str
    valid: bool
    issues: tuple[ExportValidationIssue, ...]


EXPECTED_CONTENT_TYPES = {
    ExportType.PDF: "application/pdf",
    ExportType.EXCEL: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ExportType.POWERPOINT: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ExportType.CHART_PNG: "image/png",
    ExportType.CHART_SVG: "image/svg+xml",
    ExportType.BOARD_PACKAGE: "application/zip",
    ExportType.BI_API: "application/json",
}


def checksum_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class ExportValidationService:
    def __init__(self, db: DatabaseConnection, audit: AuditService | None = None):
        self.db = db
        self.audit = audit or AuditService(db)

    def build_artifact(
        self,
        artifact_id: str,
        export_type: ExportType,
        file_name: str,
        content_type: str,
        content: bytes,
        metadata: Mapping[str, Any] | None = None,
    ) -> ExportArtifact:
        metadata = metadata or {}
        return ExportArtifact(
            artifact_id=artifact_id,
            export_type=export_type,
            file_name=file_name,
            content_type=content_type,
            byte_size=len(content),
            checksum=checksum_bytes(content),
            page_count=metadata.get("page_count"),
            sheet_count=metadata.get("sheet_count"),
            slide_count=metadata.get("slide_count"),
            metadata=dict(metadata),
        )

    def validate(self, artifact: ExportArtifact) -> ExportValidationResult:
        issues: list[ExportValidationIssue] = []
        expected = EXPECTED_CONTENT_TYPES.get(artifact.export_type)
        if expected and artifact.content_type != expected:
            issues.append(
                ExportValidationIssue(
                    "error",
                    "content_type",
                    f"Expected {expected}, got {artifact.content_type}",
                )
            )
        if artifact.byte_size <= 0:
            issues.append(ExportValidationIssue("error", "empty_artifact", "Export artifact is empty"))
        if artifact.export_type in {ExportType.PDF, ExportType.BOARD_PACKAGE} and not artifact.page_count:
            issues.append(ExportValidationIssue("warning", "pagination_missing", "Page count was not captured"))
        if artifact.export_type is ExportType.EXCEL and not artifact.sheet_count:
            issues.append(ExportValidationIssue("warning", "sheet_count_missing", "Sheet count was not captured"))
        if artifact.export_type is ExportType.POWERPOINT and not artifact.slide_count:
            issues.append(ExportValidationIssue("warning", "slide_count_missing", "Slide count was not captured"))
        if artifact.export_type in {ExportType.CHART_PNG, ExportType.CHART_SVG} and not artifact.metadata.get("chart_spec_hash"):
            issues.append(ExportValidationIssue("warning", "chart_spec_missing", "Chart spec hash was not captured"))
        return ExportValidationResult(
            artifact.artifact_id,
            not any(issue.severity == "error" for issue in issues),
            tuple(issues),
        )

    def record_artifact(self, context: ServiceContext, artifact: ExportArtifact, result: ExportValidationResult) -> None:
        self.db.execute(
            """
            INSERT INTO export_artifacts (
                artifact_id, export_type, file_name, content_type, byte_size, checksum,
                page_count, sheet_count, slide_count, metadata_json, validation_json,
                valid, created_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact.artifact_id,
                artifact.export_type.value,
                artifact.file_name,
                artifact.content_type,
                artifact.byte_size,
                artifact.checksum,
                artifact.page_count,
                artifact.sheet_count,
                artifact.slide_count,
                json.dumps(dict(artifact.metadata), default=str, sort_keys=True),
                json.dumps([issue.__dict__ for issue in result.issues], default=str, sort_keys=True),
                result.valid,
                context.user_id,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.audit.record(
            context,
            "export.artifact.validate",
            "export_artifact",
            artifact.artifact_id,
            {"valid": result.valid, "issues": [issue.__dict__ for issue in result.issues]},
        )

    def validate_and_record(self, context: ServiceContext, artifact: ExportArtifact) -> ExportValidationResult:
        result = self.validate(artifact)
        self.record_artifact(context, artifact, result)
        return result

    def validate_board_package(self, context: ServiceContext, package_id: str, artifacts: Sequence[ExportArtifact]) -> ExportValidationResult:
        issues: list[ExportValidationIssue] = []
        if not artifacts:
            issues.append(ExportValidationIssue("error", "empty_package", "Board package has no artifacts"))
        required = {ExportType.PDF, ExportType.EXCEL}
        present = {artifact.export_type for artifact in artifacts}
        for missing in sorted(required - present, key=lambda item: item.value):
            issues.append(ExportValidationIssue("error", "missing_required_artifact", f"Missing {missing.value} artifact"))
        for artifact in artifacts:
            result = self.validate(artifact)
            issues.extend(
                ExportValidationIssue(issue.severity, f"{artifact.artifact_id}.{issue.code}", issue.message)
                for issue in result.issues
            )
        package_result = ExportValidationResult(package_id, not any(issue.severity == "error" for issue in issues), tuple(issues))
        self.audit.record(
            context,
            "export.board_package.validate",
            "board_package",
            package_id,
            {"valid": package_result.valid, "artifact_count": len(artifacts)},
        )
        return package_result

