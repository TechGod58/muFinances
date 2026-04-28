from __future__ import annotations

from typing import Any, Mapping

from .audit import AuditService
from .base import DatabaseConnection, ServiceContext, fetch_all, require_fields


class ReportService:
    def __init__(self, db: DatabaseConnection, audit: AuditService | None = None):
        self.db = db
        self.audit = audit or AuditService(db)

    def saved_reports(self, context: ServiceContext) -> list[dict[str, Any]]:
        cursor = self.db.execute("SELECT * FROM saved_reports ORDER BY updated_at DESC")
        return fetch_all(cursor)

    def create_snapshot(self, context: ServiceContext, payload: Mapping[str, Any]) -> str:
        require_fields(payload, ("id", "report_id", "content_type", "status"))
        snapshot_id = str(payload["id"])
        self.db.execute(
            """
            INSERT INTO report_snapshots (id, report_id, content_type, status, created_by)
            VALUES (?, ?, ?, ?, ?)
            """,
            (snapshot_id, payload["report_id"], payload["content_type"], payload["status"], context.user_id),
        )
        self.audit.record(context, "report.create_snapshot", "report_snapshot", snapshot_id, payload)
        return snapshot_id

