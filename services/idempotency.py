from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from .base import DatabaseConnection, ValidationError, fetch_all


@dataclass(frozen=True)
class IdempotencyResult:
    key: str
    status: str
    response_ref: str | None = None


class IdempotencyService:
    def __init__(self, db: DatabaseConnection):
        self.db = db

    def require_key(self, key: str | None) -> str:
        if not key or not key.strip():
            raise ValidationError("Idempotency key is required")
        return key.strip()

    def claim(self, key: str | None, operation: str, request_hash: str) -> IdempotencyResult:
        clean_key = self.require_key(key)
        cursor = self.db.execute(
            """
            SELECT key, status, response_ref, request_hash
            FROM idempotency_keys
            WHERE key = ?
            """,
            (clean_key,),
        )
        rows = fetch_all(cursor)
        if rows:
            row = rows[0]
            if row.get("request_hash") != request_hash:
                raise ValidationError("Idempotency key was reused with a different request")
            return IdempotencyResult(clean_key, str(row["status"]), row.get("response_ref"))

        self.db.execute(
            """
            INSERT INTO idempotency_keys (key, operation, request_hash, status, created_at)
            VALUES (?, ?, ?, 'claimed', ?)
            """,
            (clean_key, operation, request_hash, datetime.now(timezone.utc).isoformat()),
        )
        return IdempotencyResult(clean_key, "claimed")

    def complete(self, key: str, response_ref: str, metadata: Mapping[str, Any] | None = None) -> None:
        self.db.execute(
            """
            UPDATE idempotency_keys
            SET status = 'completed', response_ref = ?, metadata_json = ?
            WHERE key = ?
            """,
            (response_ref, "{}" if metadata is None else str(dict(metadata)), key),
        )

