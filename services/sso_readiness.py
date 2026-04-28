from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .base import ValidationError


@dataclass(frozen=True)
class SsoConfiguration:
    provider: str
    issuer: str
    client_id: str
    callback_url: str
    group_claim: str = "groups"
    email_claim: str = "email"
    subject_claim: str = "sub"
    enabled: bool = False


class SsoReadinessService:
    REQUIRED_FIELDS = ("provider", "issuer", "client_id", "callback_url")

    def validate_configuration(self, config: SsoConfiguration) -> list[str]:
        issues: list[str] = []
        for field in self.REQUIRED_FIELDS:
            if not getattr(config, field):
                issues.append(f"Missing SSO field: {field}")
        if config.enabled and issues:
            raise ValidationError("; ".join(issues))
        return issues

    def identity_from_claims(self, config: SsoConfiguration, claims: Mapping[str, object]) -> dict[str, object]:
        subject = claims.get(config.subject_claim)
        email = claims.get(config.email_claim)
        groups = claims.get(config.group_claim) or []
        if not subject:
            raise ValidationError("SSO subject claim is missing")
        if not email:
            raise ValidationError("SSO email claim is missing")
        return {
            "sso_subject": str(subject),
            "email": str(email).lower(),
            "groups": list(groups) if isinstance(groups, (list, tuple, set)) else [str(groups)],
        }

