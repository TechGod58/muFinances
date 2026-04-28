from __future__ import annotations

from typing import Any, Mapping

from .audit import AuditService
from .base import DatabaseConnection, ServiceContext, fetch_all, require_fields


class ForecastService:
    def __init__(self, db: DatabaseConnection, audit: AuditService | None = None):
        self.db = db
        self.audit = audit or AuditService(db)

    def scenarios(self, context: ServiceContext) -> list[dict[str, Any]]:
        cursor = self.db.execute("SELECT * FROM scenarios ORDER BY updated_at DESC")
        return fetch_all(cursor)

    def clone_scenario(self, context: ServiceContext, payload: Mapping[str, Any]) -> str:
        require_fields(payload, ("source_scenario_id", "target_scenario_id", "name"))
        target_id = str(payload["target_scenario_id"])
        self.db.execute(
            """
            INSERT INTO scenarios (id, name, source_scenario_id, status, created_by)
            VALUES (?, ?, ?, 'working', ?)
            """,
            (target_id, payload["name"], payload["source_scenario_id"], context.user_id),
        )
        self.audit.record(context, "forecast.clone_scenario", "scenario", target_id, payload)
        return target_id

