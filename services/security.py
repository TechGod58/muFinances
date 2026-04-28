from __future__ import annotations

from .access_enforcement import AccessEnforcementService, default_access_policy, normalize_roles
from .base import PermissionDenied, ServiceContext


class SecurityService:
    def __init__(self, access: AccessEnforcementService | None = None):
        self.access = access or AccessEnforcementService(default_access_policy())

    def require_role(self, context: ServiceContext, *allowed_roles: str) -> None:
        if not allowed_roles:
            return
        roles = set(normalize_roles(context.roles))
        allowed = set(normalize_roles(allowed_roles))
        if not roles.intersection(allowed):
            raise PermissionDenied(f"Requires one of: {', '.join(allowed_roles)}")

    def require_permission(self, context: ServiceContext, permission: str) -> None:
        self.access.require_permission(context, permission)

    def can_post_ledger(self, context: ServiceContext) -> bool:
        return bool(set(normalize_roles(context.roles)).intersection({"admin", "controller", "budget_office"}))

    def can_approve_workflow(self, context: ServiceContext) -> bool:
        return bool(set(normalize_roles(context.roles)).intersection({"admin", "controller", "approver"}))
