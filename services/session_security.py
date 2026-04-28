from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping

from .base import PermissionDenied, ValidationError


@dataclass(frozen=True)
class SessionRecord:
    session_id: str
    user_id: str
    token_hash: str
    issued_at: datetime
    expires_at: datetime
    client_host: str | None = None
    user_agent: str | None = None
    sso_subject: str | None = None


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class SessionSecurityService:
    def __init__(self, ttl_minutes: int = 480):
        if ttl_minutes < 5:
            raise ValidationError("Session TTL must be at least five minutes")
        self.ttl = timedelta(minutes=ttl_minutes)

    def issue(self, user_id: str, metadata: Mapping[str, object] | None = None) -> tuple[str, SessionRecord]:
        if not user_id:
            raise ValidationError("user_id is required")
        token = secrets.token_urlsafe(48)
        now = datetime.now(timezone.utc)
        metadata = metadata or {}
        record = SessionRecord(
            session_id=secrets.token_hex(16),
            user_id=user_id,
            token_hash=hash_token(token),
            issued_at=now,
            expires_at=now + self.ttl,
            client_host=str(metadata.get("client_host") or "") or None,
            user_agent=str(metadata.get("user_agent") or "") or None,
            sso_subject=str(metadata.get("sso_subject") or "") or None,
        )
        return token, record

    def validate(self, token: str, record: SessionRecord, now: datetime | None = None) -> None:
        if not token:
            raise PermissionDenied("Missing session token")
        if hash_token(token) != record.token_hash:
            raise PermissionDenied("Invalid session token")
        if (now or datetime.now(timezone.utc)) >= record.expires_at:
            raise PermissionDenied("Session has expired")

