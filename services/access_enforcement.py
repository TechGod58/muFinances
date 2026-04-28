from __future__ import annotations

from dataclasses import dataclass, field
from ipaddress import ip_address, ip_network
from typing import Mapping, Sequence

from .base import PermissionDenied, ServiceContext, ValidationError


@dataclass(frozen=True)
class RoleRule:
    permission: str
    allowed_roles: tuple[str, ...]
    description: str


@dataclass(frozen=True)
class AccessPolicy:
    allowed_domains: tuple[str, ...] = ("manchester.edu",)
    allowed_networks: tuple[str, ...] = ()
    required_ou_paths: tuple[str, ...] = ()
    role_rules: tuple[RoleRule, ...] = field(default_factory=tuple)
    production_mode: bool = False


DEFAULT_ROLE_RULES = (
    RoleRule("ledger.read", ("admin", "controller", "budget_office", "department_planner", "executive"), "Read ledger data"),
    RoleRule("ledger.post", ("admin", "controller", "budget_office"), "Post ledger lines and adjustments"),
    RoleRule("budget.submit", ("admin", "budget_office", "department_planner"), "Submit department budget data"),
    RoleRule("budget.approve", ("admin", "budget_office", "controller"), "Approve budget submissions"),
    RoleRule("forecast.publish", ("admin", "budget_office", "controller"), "Publish forecast/scenario outputs"),
    RoleRule("close.manage", ("admin", "controller"), "Manage close and reconciliation workflows"),
    RoleRule("imports.approve", ("admin", "controller", "integration_admin"), "Approve staged imports"),
    RoleRule("security.admin", ("admin", "security_admin"), "Manage users, roles, SSO, and access policy"),
    RoleRule("audit.read", ("admin", "controller", "auditor"), "Read audit records and evidence"),
)


def default_access_policy(production_mode: bool = False) -> AccessPolicy:
    return AccessPolicy(role_rules=DEFAULT_ROLE_RULES, production_mode=production_mode)


def normalize_email_domain(value: str | None) -> str:
    if not value or "@" not in value:
        return ""
    return value.rsplit("@", 1)[1].strip().lower()


def normalize_roles(roles: Sequence[str] | None) -> tuple[str, ...]:
    return tuple(sorted({str(role).strip().lower() for role in roles or () if str(role).strip()}))


class AccessEnforcementService:
    def __init__(self, policy: AccessPolicy | None = None):
        self.policy = policy or default_access_policy()

    def require_permission(self, context: ServiceContext, permission: str) -> None:
        roles = set(normalize_roles(context.roles))
        for rule in self.policy.role_rules:
            if rule.permission == permission:
                if roles.intersection(rule.allowed_roles):
                    return
                raise PermissionDenied(f"Permission denied for {permission}")
        raise ValidationError(f"Unknown permission: {permission}")

    def require_domain(self, identity: Mapping[str, object]) -> None:
        email = str(identity.get("email") or identity.get("username") or "")
        domain = normalize_email_domain(email)
        allowed = {item.lower() for item in self.policy.allowed_domains}
        if allowed and domain not in allowed:
            raise PermissionDenied(f"User domain is not allowed: {domain or 'unknown'}")

    def require_ou_membership(self, identity: Mapping[str, object]) -> None:
        required = tuple(path.lower() for path in self.policy.required_ou_paths)
        if not required:
            return
        user_ous = tuple(str(path).lower() for path in identity.get("ou_paths", ()) or ())
        if not any(required_path in user_ou for required_path in required for user_ou in user_ous):
            raise PermissionDenied("User is not in an allowed Active Directory OU")

    def require_network(self, client_host: str | None) -> None:
        if not self.policy.allowed_networks:
            return
        if not client_host:
            raise PermissionDenied("Client network cannot be verified")
        client_ip = ip_address(client_host)
        if not any(client_ip in ip_network(network, strict=False) for network in self.policy.allowed_networks):
            raise PermissionDenied("Client is outside the allowed campus/VPN network")

    def validate_login_context(self, identity: Mapping[str, object], client_host: str | None = None) -> None:
        self.require_domain(identity)
        self.require_ou_membership(identity)
        self.require_network(client_host)

    def role_matrix(self) -> list[dict[str, object]]:
        return [
            {
                "permission": rule.permission,
                "allowed_roles": list(rule.allowed_roles),
                "description": rule.description,
            }
            for rule in self.policy.role_rules
        ]

