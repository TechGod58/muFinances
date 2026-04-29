from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.routers.deps import require
from app.schemas import ReportDefinitionCreate
from app.services.reporting import create_report_definition, list_report_definitions, run_report, status
from app.services.reporting_artifact_certification import (
    list_runs as list_reporting_artifact_certification_runs,
    run_certification as run_reporting_artifact_certification,
    status as reporting_artifact_certification_status,
)

router = APIRouter(tags=['reporting'])


@router.get('/api/reporting/status')
def reporting_status_endpoint() -> dict[str, Any]:
    return status()


@router.get('/api/reporting/reports')
def reporting_reports(request: Request) -> dict[str, Any]:
    require(request, 'reporting.manage')
    rows = list_report_definitions()
    return {'count': len(rows), 'reports': rows}


@router.post('/api/reporting/reports')
def reporting_create_report(payload: ReportDefinitionCreate, request: Request) -> dict[str, Any]:
    require(request, 'reporting.manage')
    return create_report_definition(payload.model_dump(), request.state.user)


@router.get('/api/reporting/reports/{report_id}/run')
def reporting_run_report(report_id: int, request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    require(request, 'reporting.manage')
    try:
        return run_report(report_id, scenario_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/api/reporting/artifact-certification/status')
def reporting_artifact_certification_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'reporting.manage')
    return reporting_artifact_certification_status()


@router.get('/api/reporting/artifact-certification/runs')
def reporting_artifact_certification_runs_endpoint(request: Request, limit: int = 50) -> dict[str, Any]:
    require(request, 'reporting.manage')
    rows = list_reporting_artifact_certification_runs(limit)
    return {'count': len(rows), 'runs': rows}


@router.post('/api/reporting/artifact-certification/run')
def reporting_artifact_certification_run_endpoint(payload: dict[str, Any], request: Request) -> dict[str, Any]:
    require(request, 'reporting.manage')
    return run_reporting_artifact_certification(payload, request.state.user)
