from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping

from .base import DatabaseConnection, ServiceContext


class AuditService:
    def __init__(self, db: DatabaseConnection):
        self.db = db

    def record(
        self,
        context: ServiceContext,
        action: str,
        entity_type: str,
        entity_id: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        self.db.execute(
            """
            INSERT INTO audit_log (
                action, entity_type, entity_id, user_id, request_id, details_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                action,
                entity_type,
                entity_id,
                context.user_id,
                context.request_id,
                "{}" if details is None else json.dumps(dict(details), default=str, sort_keys=True),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
