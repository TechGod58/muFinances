from __future__ import annotations

import csv
import hashlib
import io
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator, Mapping, Sequence

from .audit import AuditService
from .base import DatabaseConnection, ServiceContext, fetch_all, require_fields
from .security import SecurityService
from .transactions import TransactionManager


@dataclass(frozen=True)
class MappingField:
    source: str
    target: str
    required: bool = False
    default: Any = None


@dataclass(frozen=True)
class ImportMappingVersion:
    mapping_id: str
    version: int
    source_system: str
    fields: tuple[MappingField, ...]
    active: bool = True


@dataclass(frozen=True)
class ValidationIssue:
    row_number: int
    severity: str
    field: str
    message: str


@dataclass(frozen=True)
class ImportPreview:
    batch_id: str
    accepted_rows: int
    rejected_rows: int
    issues: tuple[ValidationIssue, ...] = field(default_factory=tuple)


def stable_hash(value: Mapping[str, Any] | Sequence[Any] | str) -> str:
    payload = value if isinstance(value, str) else json.dumps(value, default=str, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class ImportPipelineService:
    def __init__(
        self,
        db: DatabaseConnection,
        audit: AuditService | None = None,
        security: SecurityService | None = None,
        transactions: TransactionManager | None = None,
    ):
        self.db = db
        self.audit = audit or AuditService(db)
        self.security = security or SecurityService()
        self.transactions = transactions or TransactionManager(db)

    def create_mapping_version(self, context: ServiceContext, payload: Mapping[str, Any]) -> str:
        self.security.require_permission(context, "imports.approve")
        require_fields(payload, ("mapping_id", "version", "source_system", "fields"))
        mapping_key = f"{payload['mapping_id']}@{payload['version']}"
        self.db.execute(
            """
            INSERT INTO import_mapping_versions (
                mapping_key, mapping_id, version, source_system, fields_json, active, created_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                mapping_key,
                payload["mapping_id"],
                int(payload["version"]),
                payload["source_system"],
                json.dumps(payload["fields"], default=str, sort_keys=True),
                bool(payload.get("active", True)),
                context.user_id,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.audit.record(context, "import.mapping_version.create", "import_mapping_version", mapping_key, payload)
        return mapping_key

    def stream_csv_rows(self, content: bytes | str, chunk_size: int = 1000) -> Iterator[list[dict[str, str]]]:
        text = content.decode("utf-8-sig") if isinstance(content, bytes) else content
        reader = csv.DictReader(io.StringIO(text))
        chunk: list[dict[str, str]] = []
        for row in reader:
            chunk.append(dict(row))
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    def validate_rows(
        self,
        mapping: ImportMappingVersion,
        rows: Iterable[Mapping[str, Any]],
        start_row: int = 1,
    ) -> tuple[list[dict[str, Any]], list[ValidationIssue]]:
        accepted: list[dict[str, Any]] = []
        issues: list[ValidationIssue] = []
        for offset, row in enumerate(rows):
            row_number = start_row + offset
            transformed: dict[str, Any] = {}
            rejected = False
            for field in mapping.fields:
                value = row.get(field.source, field.default)
                if field.required and value in (None, ""):
                    issues.append(ValidationIssue(row_number, "error", field.source, f"{field.source} is required"))
                    rejected = True
                transformed[field.target] = value
            if not rejected:
                accepted.append(transformed)
        return accepted, issues

    def register_batch(
        self,
        context: ServiceContext,
        batch_id: str,
        source_system: str,
        file_name: str,
        mapping_key: str,
        source_ref: str | None = None,
    ) -> str:
        require_fields(
            {
                "batch_id": batch_id,
                "source_system": source_system,
                "file_name": file_name,
                "mapping_key": mapping_key,
            },
            ("batch_id", "source_system", "file_name", "mapping_key"),
        )
        self.db.execute(
            """
            INSERT INTO import_batches (
                id, source_system, file_name, mapping_key, source_ref, status, created_by, created_at
            ) VALUES (?, ?, ?, ?, ?, 'staged', ?, ?)
            """,
            (batch_id, source_system, file_name, mapping_key, source_ref, context.user_id, datetime.now(timezone.utc).isoformat()),
        )
        self.audit.record(context, "import.batch.register", "import_batch", batch_id, {"source_ref": source_ref})
        return batch_id

    def persist_preview(
        self,
        context: ServiceContext,
        batch_id: str,
        accepted_rows: Sequence[Mapping[str, Any]],
        issues: Sequence[ValidationIssue],
    ) -> ImportPreview:
        with self.transactions.boundary():
            for index, row in enumerate(accepted_rows, start=1):
                row_hash = stable_hash(row)
                self.db.execute(
                    """
                    INSERT INTO import_staged_rows (
                        batch_id, row_number, row_hash, row_json, status
                    ) VALUES (?, ?, ?, ?, 'accepted')
                    """,
                    (batch_id, index, row_hash, json.dumps(dict(row), default=str, sort_keys=True)),
                )
            for issue in issues:
                self.db.execute(
                    """
                    INSERT INTO import_rejections (
                        batch_id, row_number, severity, field_name, message
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (batch_id, issue.row_number, issue.severity, issue.field, issue.message),
                )
            status = "rejected" if issues and not accepted_rows else "validated"
            self.db.execute("UPDATE import_batches SET status = ? WHERE id = ?", (status, batch_id))
            self.audit.record(
                context,
                "import.batch.preview",
                "import_batch",
                batch_id,
                {"accepted": len(accepted_rows), "rejected": len(issues)},
            )
        return ImportPreview(batch_id, len(accepted_rows), len(issues), tuple(issues))

    def approve_batch(self, context: ServiceContext, batch_id: str) -> None:
        self.security.require_permission(context, "imports.approve")
        with self.transactions.boundary():
            self.db.execute(
                """
                UPDATE import_batches
                SET status = 'approved', approved_by = ?, approved_at = ?
                WHERE id = ? AND status IN ('validated', 'staged')
                """,
                (context.user_id, datetime.now(timezone.utc).isoformat(), batch_id),
            )
            self.audit.record(context, "import.batch.approve", "import_batch", batch_id, {})

    def reject_batch(self, context: ServiceContext, batch_id: str, reason: str) -> None:
        self.security.require_permission(context, "imports.approve")
        with self.transactions.boundary():
            self.db.execute(
                """
                UPDATE import_batches
                SET status = 'rejected', rejection_reason = ?
                WHERE id = ?
                """,
                (reason, batch_id),
            )
            self.audit.record(context, "import.batch.reject", "import_batch", batch_id, {"reason": reason})

    def rollback_batch(self, context: ServiceContext, batch_id: str, reason: str) -> None:
        self.security.require_permission(context, "imports.approve")
        with self.transactions.boundary():
            self.db.execute("UPDATE import_staged_rows SET status = 'rolled_back' WHERE batch_id = ?", (batch_id,))
            self.db.execute(
                """
                UPDATE import_batches
                SET status = 'rolled_back', rollback_reason = ?
                WHERE id = ?
                """,
                (reason, batch_id),
            )
            self.audit.record(context, "import.batch.rollback", "import_batch", batch_id, {"reason": reason})

    def drill_back(self, context: ServiceContext, batch_id: str, row_hash: str | None = None) -> list[dict[str, Any]]:
        sql = """
            SELECT batch_id, row_number, row_hash, row_json, status
            FROM import_staged_rows
            WHERE batch_id = ? AND (? IS NULL OR row_hash = ?)
            ORDER BY row_number
        """
        return fetch_all(self.db.execute(sql, (batch_id, row_hash, row_hash)))
