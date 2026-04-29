from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.routers.deps import require
from app.schemas import AccountReconciliationCreate, ConsolidationRunCreate, PeriodLockAction
from app.services.close_consolidation import (
    create_reconciliation,
    list_checklist_items,
    list_consolidation_runs,
    run_consolidation,
    set_period_lock,
    status,
)

router = APIRouter(tags=['close'])


@router.get('/api/close/status')
def close_status_endpoint() -> dict[str, Any]:
    return status()


@router.get('/api/close/checklists')
def close_checklists(request: Request, scenario_id: int = Query(..., ge=1), period: str | None = None) -> dict[str, Any]:
    require(request, 'close.manage')
    rows = list_checklist_items(scenario_id, period)
    return {'scenario_id': scenario_id, 'period': period, 'count': len(rows), 'checklists': rows}


@router.post('/api/close/reconciliations')
def close_create_reconciliation(payload: AccountReconciliationCreate, request: Request) -> dict[str, Any]:
    require(request, 'close.manage')
    try:
        return create_reconciliation(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get('/api/close/consolidation-runs')
def close_consolidation_runs(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    require(request, 'close.manage')
    rows = list_consolidation_runs(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'runs': rows}


@router.post('/api/close/consolidation-runs')
def close_run_consolidation(payload: ConsolidationRunCreate, request: Request) -> dict[str, Any]:
    require(request, 'close.manage')
    try:
        return run_consolidation(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post('/api/close/calendar/{period}/lock')
def close_set_period_lock(period: str, payload: PeriodLockAction, request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    require(request, 'close.manage')
    return set_period_lock(scenario_id, period, payload.lock_state, request.state.user)
