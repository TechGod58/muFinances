from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Sequence

from .base import ValidationError


class ReadinessSeverity(str, Enum):
    BLOCKER = "blocker"
    WARNING = "warning"
    INFO = "info"


@dataclass(frozen=True)
class ReadinessFinding:
    code: str
    severity: ReadinessSeverity
    message: str
    remediation: str


@dataclass(frozen=True)
class SecurityHeaderPolicy:
    required_headers: Mapping[str, str] = field(
        default_factory=lambda: {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "Referrer-Policy": "same-origin",
            "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
        }
    )


@dataclass(frozen=True)
class ProductionReadinessPolicy:
    production_mode: bool
    required_secret_names: tuple[str, ...] = (
        "MUFINANCES_SECRET_KEY",
        "MUFINANCES_DATABASE_URL",
    )
    forbidden_secret_patterns: tuple[str, ...] = (
        "password",
        "changeme",
        "dev-secret",
        "localhost-secret",
    )
    required_permissions: tuple[str, ...] = (
        "ledger.read",
        "ledger.post",
        "budget.submit",
        "budget.approve",
        "imports.approve",
        "audit.read",
        "security.admin",
    )
    masking_required_fields: tuple[str, ...] = (
        "password",
        "token",
        "secret",
        "api_key",
        "ssn",
        "account_number",
    )
    required_sod_pairs: tuple[tuple[str, str], ...] = (
        ("budget.submit", "budget.approve"),
        ("ledger.post", "audit.read"),
        ("security.admin", "admin.impersonate"),
    )


class ProductionReadinessService:
    def __init__(
        self,
        policy: ProductionReadinessPolicy | None = None,
        header_policy: SecurityHeaderPolicy | None = None,
    ):
        self.policy = policy or ProductionReadinessPolicy(
            production_mode=str(os.environ.get("MUFINANCES_MODE", "development")).lower() == "production"
        )
        self.header_policy = header_policy or SecurityHeaderPolicy()

    def review_environment(self, env: Mapping[str, str] | None = None) -> list[ReadinessFinding]:
        env = env or os.environ
        findings: list[ReadinessFinding] = []
        if not self.policy.production_mode:
            findings.append(
                ReadinessFinding(
                    "runtime.not_production",
                    ReadinessSeverity.INFO,
                    "Production fail-fast checks are running in non-production mode.",
                    "Set MUFINANCES_MODE=production before deployment validation.",
                )
            )
        for name in self.policy.required_secret_names:
            value = env.get(name, "")
            if not value:
                findings.append(
                    ReadinessFinding(
                        f"secret.missing.{name.lower()}",
                        ReadinessSeverity.BLOCKER,
                        f"Required secret {name} is missing.",
                        f"Set {name} using the production secret vault.",
                    )
                )
                continue
            if self._looks_unsafe_secret(value):
                findings.append(
                    ReadinessFinding(
                        f"secret.unsafe.{name.lower()}",
                        ReadinessSeverity.BLOCKER,
                        f"Required secret {name} appears to contain a default or unsafe value.",
                        f"Rotate {name} and load it from the production secret vault.",
                    )
                )
        return findings

    def review_headers(self, headers: Mapping[str, str]) -> list[ReadinessFinding]:
        normalized = {key.lower(): value for key, value in headers.items()}
        findings: list[ReadinessFinding] = []
        for name, expected in self.header_policy.required_headers.items():
            actual = normalized.get(name.lower())
            if actual != expected:
                findings.append(
                    ReadinessFinding(
                        f"header.{name.lower()}",
                        ReadinessSeverity.BLOCKER,
                        f"Security header {name} is not set to {expected}.",
                        f"Set {name}: {expected} on all application responses.",
                    )
                )
        if self.policy.production_mode and "strict-transport-security" not in normalized:
            findings.append(
                ReadinessFinding(
                    "header.hsts",
                    ReadinessSeverity.BLOCKER,
                    "HSTS is missing in production mode.",
                    "Set Strict-Transport-Security after TLS is enabled.",
                )
            )
        return findings

    def review_permissions(self, permission_names: Sequence[str]) -> list[ReadinessFinding]:
        available = set(permission_names)
        findings = []
        for permission in self.policy.required_permissions:
            if permission not in available:
                findings.append(
                    ReadinessFinding(
                        f"permission.missing.{permission}",
                        ReadinessSeverity.BLOCKER,
                        f"Required permission {permission} is not registered.",
                        "Register the permission and map it to approved roles.",
                    )
                )
        return findings

    def review_masking(self, masked_fields: Sequence[str]) -> list[ReadinessFinding]:
        masked = {field.lower() for field in masked_fields}
        findings = []
        for field in self.policy.masking_required_fields:
            if field not in masked:
                findings.append(
                    ReadinessFinding(
                        f"masking.missing.{field}",
                        ReadinessSeverity.BLOCKER,
                        f"Sensitive field {field} is not listed as masked.",
                        "Add the field to the masking policy before production.",
                    )
                )
        return findings

    def review_sod(self, sod_pairs: Sequence[tuple[str, str]]) -> list[ReadinessFinding]:
        configured = {tuple(pair) for pair in sod_pairs}
        findings = []
        for pair in self.policy.required_sod_pairs:
            if pair not in configured:
                findings.append(
                    ReadinessFinding(
                        f"sod.missing.{pair[0]}.{pair[1]}",
                        ReadinessSeverity.WARNING,
                        f"SoD rule {pair[0]} versus {pair[1]} is not configured.",
                        "Add the SoD pair to the access review policy.",
                    )
                )
        return findings

    def fail_fast(self, findings: Sequence[ReadinessFinding]) -> None:
        blockers = [finding for finding in findings if finding.severity is ReadinessSeverity.BLOCKER]
        if self.policy.production_mode and blockers:
            codes = ", ".join(finding.code for finding in blockers)
            raise ValidationError(f"Production readiness blockers: {codes}")

    def _looks_unsafe_secret(self, value: str) -> bool:
        lowered = value.lower()
        if len(value) < 24:
            return True
        return any(re.search(pattern.lower(), lowered) for pattern in self.policy.forbidden_secret_patterns)
