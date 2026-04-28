import pytest

from services.access_enforcement import AccessEnforcementService, AccessPolicy, RoleRule
from services.base import PermissionDenied, ServiceContext


def test_domain_enforcement_allows_manchester_email():
    service = AccessEnforcementService(AccessPolicy(allowed_domains=("manchester.edu",)))

    service.validate_login_context({"email": "person@manchester.edu"})


def test_domain_enforcement_rejects_external_email():
    service = AccessEnforcementService(AccessPolicy(allowed_domains=("manchester.edu",)))

    with pytest.raises(PermissionDenied):
        service.validate_login_context({"email": "person@example.com"})


def test_permission_matrix_checks_roles():
    service = AccessEnforcementService(
        AccessPolicy(
            role_rules=(
                RoleRule("ledger.post", ("controller",), "Post ledger"),
            )
        )
    )

    service.require_permission(ServiceContext(user_id="u1", roles=("controller",)), "ledger.post")

    with pytest.raises(PermissionDenied):
        service.require_permission(ServiceContext(user_id="u2", roles=("department_planner",)), "ledger.post")


def test_network_enforcement_accepts_allowed_cidr():
    service = AccessEnforcementService(AccessPolicy(allowed_domains=(), allowed_networks=("10.10.0.0/16",)))

    service.validate_login_context({"email": "person@manchester.edu"}, client_host="10.10.4.20")


def test_ou_enforcement_requires_allowed_path():
    service = AccessEnforcementService(
        AccessPolicy(allowed_domains=(), required_ou_paths=("OU=Finance",))
    )

    service.validate_login_context({"email": "person@manchester.edu", "ou_paths": ["OU=Finance,DC=manchester,DC=edu"]})

    with pytest.raises(PermissionDenied):
        service.validate_login_context({"email": "person@manchester.edu", "ou_paths": ["OU=Students,DC=manchester,DC=edu"]})

