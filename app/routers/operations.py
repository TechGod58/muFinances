from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from app.routers.deps import require
from app.services.deployment_operations import list_restore_tests, list_runbooks, status as deployment_status
from app.services.observability_operations import status as observability_status
from app.services.operations_readiness import status as readiness_status
from app.services.performance_reliability import status as performance_status
from app.services.mssql_live_proof import list_runs as list_mssql_proof_runs
from app.services.mssql_live_proof import run_proof as run_mssql_proof
from app.services.mssql_live_proof import status as mssql_live_proof_status
from app.services.production_database_live_proof import (
    list_runs as list_production_database_live_proof_runs,
    run_live_proof as run_production_database_live_proof,
    status as production_database_live_proof_status,
)
from app.services.prophix_parity_pilot_signoff import (
    list_runs as list_prophix_parity_pilot_signoff_runs,
    run_signoff as run_prophix_parity_pilot_signoff,
    status as prophix_parity_pilot_signoff_status,
)
from app.services.secure_audit_log_certification import (
    list_runs as list_secure_audit_log_certification_runs,
    run_certification as run_secure_audit_log_certification,
    status as secure_audit_log_certification_status,
)
from app.services.secure_audit_operations import (
    create_auditor_packet,
    create_backup_verification,
    operational_policy,
    status as secure_audit_status,
    tamper_check_report,
    verification_dashboard,
)

router = APIRouter(tags=['operations'])


@router.get('/api/operations/status')
def operations_status_endpoint() -> dict[str, Any]:
    return deployment_status()


@router.get('/api/operations/restore-tests')
def operations_restore_tests(request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    rows = list_restore_tests()
    return {'count': len(rows), 'restore_tests': rows}


@router.get('/api/operations/runbooks')
def operations_runbooks(request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    rows = list_runbooks()
    return {'count': len(rows), 'runbooks': rows}


@router.get('/api/operations-readiness/status')
def operations_readiness_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return readiness_status()


@router.get('/api/observability/status')
def observability_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return observability_status()


@router.get('/api/performance/status')
def performance_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return performance_status()


@router.get('/api/secure-audit-operations/status')
def secure_audit_operations_status(request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return secure_audit_status()


@router.get('/api/secure-audit-operations/dashboard')
def secure_audit_operations_dashboard(request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return verification_dashboard()


@router.post('/api/secure-audit-operations/backup-verification')
def secure_audit_operations_backup_verification(request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return create_backup_verification(request.state.user)


@router.post('/api/secure-audit-operations/auditor-packets')
def secure_audit_operations_auditor_packet(request: Request, limit: int = 250) -> dict[str, Any]:
    require(request, 'operations.manage')
    return create_auditor_packet(request.state.user, limit)


@router.get('/api/secure-audit-operations/tamper-report')
def secure_audit_operations_tamper_report(request: Request, limit: int = 5000) -> dict[str, Any]:
    require(request, 'operations.manage')
    return tamper_check_report(limit)


@router.get('/api/secure-audit-operations/policy')
def secure_audit_operations_policy(request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return operational_policy()


@router.get('/api/mssql-live-proof/status')
def mssql_live_proof_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return mssql_live_proof_status()


@router.get('/api/mssql-live-proof/runs')
def mssql_live_proof_runs(request: Request, limit: int = 50) -> dict[str, Any]:
    require(request, 'operations.manage')
    rows = list_mssql_proof_runs(limit)
    return {'count': len(rows), 'runs': rows}


@router.post('/api/mssql-live-proof/run')
def mssql_live_proof_run(payload: dict[str, Any], request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return run_mssql_proof(payload, request.state.user)


@router.get('/api/production-database-live-proof/status')
def production_database_live_proof_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return production_database_live_proof_status()


@router.get('/api/production-database-live-proof/runs')
def production_database_live_proof_runs(request: Request, limit: int = 50) -> dict[str, Any]:
    require(request, 'operations.manage')
    rows = list_production_database_live_proof_runs(limit)
    return {'count': len(rows), 'runs': rows}


@router.post('/api/production-database-live-proof/run')
def production_database_live_proof_run(payload: dict[str, Any], request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return run_production_database_live_proof(payload, request.state.user)


@router.get('/api/secure-audit-log-certification/status')
def secure_audit_log_certification_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return secure_audit_log_certification_status()


@router.get('/api/secure-audit-log-certification/runs')
def secure_audit_log_certification_runs(request: Request, limit: int = 50) -> dict[str, Any]:
    require(request, 'operations.manage')
    rows = list_secure_audit_log_certification_runs(limit)
    return {'count': len(rows), 'runs': rows}


@router.post('/api/secure-audit-log-certification/run')
def secure_audit_log_certification_run(payload: dict[str, Any], request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return run_secure_audit_log_certification(payload, request.state.user)


@router.get('/api/prophix-parity-pilot-signoff/status')
def prophix_parity_pilot_signoff_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return prophix_parity_pilot_signoff_status()


@router.get('/api/prophix-parity-pilot-signoff/runs')
def prophix_parity_pilot_signoff_runs(request: Request, limit: int = 50) -> dict[str, Any]:
    require(request, 'operations.manage')
    rows = list_prophix_parity_pilot_signoff_runs(limit)
    return {'count': len(rows), 'runs': rows}


@router.post('/api/prophix-parity-pilot-signoff/run')
def prophix_parity_pilot_signoff_run(payload: dict[str, Any], request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return run_prophix_parity_pilot_signoff(payload, request.state.user)
