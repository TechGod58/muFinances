from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.routers.deps import require
from app.schemas import ConnectorCreate, ImportBatchCreate
from app.services.campus_integrations import list_connectors, list_import_batches, run_import, status, upsert_connector

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
