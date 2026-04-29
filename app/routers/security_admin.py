from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app.routers.deps import require
from app.schemas import (
    ADOUGroupMappingCreate,
    AdminImpersonationCreate,
    DomainVPNCheckCreate,
    SecurityActivationCertificationRunCreate,
    SoDPolicyCreate,
    SSOProductionSettingCreate,
    UserAccessReviewCreate,
    UserAccessReviewDecision,
    UserCreate,
    UserDimensionAccessCreate,
)
from app.services.access_guard import access_guard_status
from app.services.security import (
    activate_security_controls,
    certify_access_review,
    create_access_review,
    create_user,
    end_impersonation,
    enterprise_admin_status,
    enterprise_admin_workspace,
    grant_dimension_access,
    list_access_reviews,
    list_ad_ou_group_mappings,
    list_domain_vpn_checks,
    list_impersonation_sessions,
    list_sso_production_settings,
    list_users,
    record_domain_vpn_check,
    security_status,
    start_impersonation,
    upsert_ad_ou_group_mapping,
    upsert_sod_policy,
    upsert_sso_production_setting,
)
from app.services.manchester_identity_live_proof import (
    list_runs as list_manchester_identity_runs,
    run_live_proof as run_manchester_identity_live_proof,
    status as manchester_identity_live_proof_status,
)
from app.services.security_activation_certification import (
    list_runs as list_security_activation_certification_runs,
    run_certification as run_security_activation_certification,
    status as security_activation_certification_status,
)

router = APIRouter(tags=['security'])


@router.get('/api/security/status')
def security_status_endpoint() -> dict[str, Any]:
    return security_status()


@router.get('/api/security/enterprise-status')
def security_enterprise_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return enterprise_admin_status()


@router.get('/api/security/enterprise-workspace')
def security_enterprise_workspace_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return enterprise_admin_workspace()


@router.post('/api/security/activation/run')
def security_activation_run_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return activate_security_controls(request.state.user)


@router.get('/api/security/activation-certification/status')
def security_activation_certification_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return security_activation_certification_status()


@router.get('/api/security/activation-certification/runs')
def security_activation_certification_runs_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    rows = list_security_activation_certification_runs()
    return {'count': len(rows), 'activation_certification_runs': rows}


@router.post('/api/security/activation-certification/run')
def security_activation_certification_run_endpoint(payload: SecurityActivationCertificationRunCreate, request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return run_security_activation_certification(payload.model_dump(), request.state.user)


@router.get('/api/security/access-guard/status')
def security_access_guard_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return access_guard_status()


@router.get('/api/security/users')
def security_users(request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    rows = list_users()
    return {'count': len(rows), 'users': rows}


@router.post('/api/security/users')
def security_create_user(payload: UserCreate, request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return create_user(payload.model_dump(), actor=request.state.user['email'])


@router.post('/api/security/users/{user_id}/dimension-access')
def security_grant_dimension_access(user_id: int, payload: UserDimensionAccessCreate, request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return grant_dimension_access(user_id, payload.model_dump(), actor=request.state.user['email'])


@router.get('/api/security/sso-production-settings')
def security_sso_production_settings(request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    rows = list_sso_production_settings()
    return {'count': len(rows), 'sso_production_settings': rows}


@router.post('/api/security/sso-production-settings')
def security_upsert_sso_production_setting(payload: SSOProductionSettingCreate, request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return upsert_sso_production_setting(payload.model_dump(), request.state.user)


@router.get('/api/security/ad-ou-group-mappings')
def security_ad_ou_group_mappings(request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    rows = list_ad_ou_group_mappings()
    return {'count': len(rows), 'ad_ou_group_mappings': rows}


@router.post('/api/security/ad-ou-group-mappings')
def security_upsert_ad_ou_group_mapping(payload: ADOUGroupMappingCreate, request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return upsert_ad_ou_group_mapping(payload.model_dump(), request.state.user)


@router.get('/api/security/domain-vpn-checks')
def security_domain_vpn_checks(request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    rows = list_domain_vpn_checks()
    return {'count': len(rows), 'domain_vpn_checks': rows}


@router.post('/api/security/domain-vpn-checks')
def security_record_domain_vpn_check(payload: DomainVPNCheckCreate, request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return record_domain_vpn_check(payload.model_dump(), request.state.user)


@router.get('/api/security/manchester-identity-live-proof/status')
def security_manchester_identity_live_proof_status(request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return manchester_identity_live_proof_status()


@router.get('/api/security/manchester-identity-live-proof/runs')
def security_manchester_identity_live_proof_runs(request: Request, limit: int = 50) -> dict[str, Any]:
    require(request, 'security.manage')
    rows = list_manchester_identity_runs(limit)
    return {'count': len(rows), 'runs': rows}


@router.post('/api/security/manchester-identity-live-proof/run')
def security_run_manchester_identity_live_proof(payload: dict[str, Any], request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return run_manchester_identity_live_proof(payload, request.state.user)


@router.get('/api/security/impersonations')
def security_impersonations(request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    rows = list_impersonation_sessions()
    return {'count': len(rows), 'impersonations': rows}


@router.post('/api/security/impersonations')
def security_start_impersonation(payload: AdminImpersonationCreate, request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    try:
        return start_impersonation(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post('/api/security/impersonations/{impersonation_id}/end')
def security_end_impersonation(impersonation_id: int, request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return end_impersonation(impersonation_id, request.state.user)


@router.post('/api/security/sod-policies')
def security_upsert_sod_policy(payload: SoDPolicyCreate, request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return upsert_sod_policy(payload.model_dump(), request.state.user)


@router.get('/api/security/access-reviews')
def security_access_reviews(request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    rows = list_access_reviews()
    return {'count': len(rows), 'access_reviews': rows}


@router.post('/api/security/access-reviews')
def security_create_access_review(payload: UserAccessReviewCreate, request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return create_access_review(payload.model_dump(), request.state.user)


@router.post('/api/security/access-reviews/{review_id}/certify')
def security_certify_access_review(review_id: int, payload: UserAccessReviewDecision, request: Request) -> dict[str, Any]:
    require(request, 'security.manage')
    return certify_access_review(review_id, payload.model_dump(), request.state.user)
