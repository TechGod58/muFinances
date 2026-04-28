from __future__ import annotations

from typing import Any, Mapping

from .audit import AuditService
from .base import DatabaseConnection, ServiceContext, ValidationError, fetch_all, require_fields


class BudgetService:
    def __init__(self, db: DatabaseConnection, audit: AuditService | None = None):
        self.db = db
        self.audit = audit or AuditService(db)

    def submissions(self, context: ServiceContext, department_code: str | None = None) -> list[dict[str, Any]]:
        cursor = self.db.execute(
            """
            SELECT *
            FROM budget_submissions
            WHERE (? IS NULL OR department_code = ?)
            ORDER BY fiscal_period DESC, department_code
            """,
            (department_code, department_code),
        )
        return fetch_all(cursor)

    def save_submission(self, context: ServiceContext, payload: Mapping[str, Any]) -> str:
        require_fields(payload, ("id", "department_code", "fiscal_period", "status"))
        submission_id = str(payload["id"])
        self.db.execute(
            """
            INSERT OR REPLACE INTO budget_submissions (
                id, department_code, fiscal_period, status, submitted_by
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                submission_id,
                payload["department_code"],
                payload["fiscal_period"],
                payload["status"],
                context.user_id,
            ),
        )
        self.audit.record(context, "budget.save_submission", "budget_submission", submission_id, payload)
        return submission_id

