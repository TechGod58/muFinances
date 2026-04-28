from __future__ import annotations

from typing import Any, Mapping

from .audit import AuditService
from .base import DatabaseConnection, ServiceContext, fetch_all, require_fields


class ImportService:
    def __init__(self, db: DatabaseConnection, audit: AuditService | None = None):
        self.db = db
        self.audit = audit or AuditService(db)

    def mappings(self, context: ServiceContext) -> list[dict[str, Any]]:
        cursor = self.db.execute("SELECT * FROM import_mappings ORDER BY name")
        return fetch_all(cursor)

    def register_batch(self, context: ServiceContext, payload: Mapping[str, Any]) -> str:
        require_fields(payload, ("id", "source_system", "file_name", "status"))
        batch_id = str(payload["id"])
        self.db.execute(
            """
            INSERT INTO import_batches (id, source_system, file_name, status, created_by)
            VALUES (?, ?, ?, ?, ?)
            """,
            (batch_id, payload["source_system"], payload["file_name"], payload["status"], context.user_id),
        )
        self.audit.record(context, "import.register_batch", "import_batch", batch_id, payload)
        return batch_id

