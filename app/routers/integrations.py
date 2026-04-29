from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.routers.deps import require
from app.schemas import ConnectorCreate, ImportBatchCreate
from app.services.campus_integrations import list_connectors, list_import_batches, run_import, status, upsert_connector
from app.services.real_campus_data_reconciliation import (
    list_runs as list_real_campus_data_reconciliation_runs,
    run_reconciliation as run_real_campus_data_reconciliation,
    status as real_campus_data_reconciliation_status,
)

router = APIRouter(tags=['integrations'])


@router.get('/api/integrations/status')
def integrations_status_endpoint() -> dict[str, Any]:
    return status()


@router.get('/api/integrations/connectors')
def integrations_connectors(request: Request) -> dict[str, Any]:
    require(request, 'integrations.manage')
    rows = list_connectors()
    return {'count': len(rows), 'connectors': rows}


@router.post('/api/integrations/connectors')
def integrations_upsert_connector(payload: ConnectorCreate, request: Request) -> dict[str, Any]:
    require(request, 'integrations.manage')
    return upsert_connector(payload.model_dump(), request.state.user)


@router.get('/api/integrations/imports')
def integrations_imports(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    require(request, 'integrations.manage')
    rows = list_import_batches(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'imports': rows}


@router.post('/api/integrations/imports')
def integrations_run_import(payload: ImportBatchCreate, request: Request) -> dict[str, Any]:
    require(request, 'integrations.manage')
    try:
        return run_import(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get('/api/integrations/real-campus-data-reconciliation/status')
def real_campus_data_reconciliation_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'integrations.manage')
    return real_campus_data_reconciliation_status()


@router.get('/api/integrations/real-campus-data-reconciliation/runs')
def real_campus_data_reconciliation_runs_endpoint(request: Request, limit: int = 50) -> dict[str, Any]:
    require(request, 'integrations.manage')
    rows = list_real_campus_data_reconciliation_runs(limit)
    return {'count': len(rows), 'runs': rows}


@router.post('/api/integrations/real-campus-data-reconciliation/run')
def real_campus_data_reconciliation_run_endpoint(payload: dict[str, Any], request: Request) -> dict[str, Any]:
    require(request, 'integrations.manage')
    return run_real_campus_data_reconciliation(payload, request.state.user)
