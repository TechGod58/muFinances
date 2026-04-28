from __future__ import annotations

from typing import Any, Mapping

from .audit import AuditService
from .base import DatabaseConnection, ServiceContext, fetch_all, require_fields
from .security import SecurityService


class WorkflowService:
    def __init__(
        self,
        db: DatabaseConnection,
        audit: AuditService | None = None,
        security: SecurityService | None = None,
    ):
        self.db = db
        self.audit = audit or AuditService(db)
        self.security = security or SecurityService()

    def tasks(self, context: ServiceContext, status: str | None = None) -> list[dict[str, Any]]:
        cursor = self.db.execute(
            """
            SELECT *
            FROM workflow_tasks
            WHERE (? IS NULL OR status = ?)
            ORDER BY due_date, title
            """,
            (status, status),
        )
        return fetch_all(cursor)

    def approve(self, context: ServiceContext, payload: Mapping[str, Any]) -> str:
        self.security.require_role(context, "admin", "controller", "approver")
        require_fields(payload, ("task_id", "decision"))
        task_id = str(payload["task_id"])
        self.db.execute(
            """
            UPDATE workflow_tasks
            SET status = ?, approved_by = ?
            WHERE id = ?
            """,
            (payload["decision"], context.user_id, task_id),
        )
        self.audit.record(context, "workflow.approve", "workflow_task", task_id, payload)
        return task_id

