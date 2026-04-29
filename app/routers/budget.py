from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.routers.deps import require
from app.schemas import ApprovalAction, BudgetAssumptionCreate, BudgetSubmissionCreate, BudgetTransferCreate, OperatingBudgetLineCreate
from app.services.operating_budget import (
    add_budget_line,
    approve_submission,
    approve_transfer,
    create_assumption,
    create_submission,
    list_assumptions,
    list_submissions,
    list_transfers,
    reject_submission,
    request_transfer,
    status as operating_budget_status,
    submit_submission,
)

router = APIRouter(tags=['budget'])


@router.get('/api/operating-budget/status')
def operating_budget_status_endpoint() -> dict[str, Any]:
    return operating_budget_status()


@router.get('/api/operating-budget/submissions')
def operating_budget_submissions(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    require(request, 'operating_budget.manage')
    rows = list_submissions(scenario_id, request.state.user)
    return {'scenario_id': scenario_id, 'count': len(rows), 'submissions': rows}


@router.post('/api/operating-budget/submissions')
def operating_budget_create_submission(payload: BudgetSubmissionCreate, request: Request) -> dict[str, Any]:
    require(request, 'operating_budget.manage')
    try:
        return create_submission(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post('/api/operating-budget/submissions/{submission_id}/lines')
def operating_budget_add_line(submission_id: int, payload: OperatingBudgetLineCreate, request: Request) -> dict[str, Any]:
    require(request, 'operating_budget.manage')
    try:
        return add_budget_line(submission_id, payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post('/api/operating-budget/submissions/{submission_id}/submit')
def operating_budget_submit(submission_id: int, request: Request) -> dict[str, Any]:
    require(request, 'operating_budget.manage')
    try:
        return submit_submission(submission_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post('/api/operating-budget/submissions/{submission_id}/approve')
def operating_budget_approve(submission_id: int, payload: ApprovalAction, request: Request) -> dict[str, Any]:
    require(request, 'operating_budget.approve')
    try:
        return approve_submission(submission_id, request.state.user, note=payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post('/api/operating-budget/submissions/{submission_id}/reject')
def operating_budget_reject(submission_id: int, payload: ApprovalAction, request: Request) -> dict[str, Any]:
    require(request, 'operating_budget.approve')
    try:
        return reject_submission(submission_id, request.state.user, note=payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/api/operating-budget/assumptions')
def operating_budget_assumptions(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    require(request, 'operating_budget.manage')
    rows = list_assumptions(scenario_id, request.state.user)
    return {'scenario_id': scenario_id, 'count': len(rows), 'assumptions': rows}


@router.post('/api/operating-budget/assumptions')
def operating_budget_create_assumption(payload: BudgetAssumptionCreate, request: Request) -> dict[str, Any]:
    require(request, 'operating_budget.manage')
    return create_assumption(payload.model_dump(), request.state.user)


@router.get('/api/operating-budget/transfers')
def operating_budget_transfers(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    rows = list_transfers(scenario_id, request.state.user)
    return {'scenario_id': scenario_id, 'count': len(rows), 'transfers': rows}


@router.post('/api/operating-budget/transfers')
def operating_budget_request_transfer(payload: BudgetTransferCreate, request: Request) -> dict[str, Any]:
    require(request, 'operating_budget.manage')
    return request_transfer(payload.model_dump(), request.state.user)


@router.post('/api/operating-budget/transfers/{transfer_id}/approve')
def operating_budget_approve_transfer(transfer_id: int, request: Request) -> dict[str, Any]:
    require(request, 'operating_budget.approve')
    try:
        return approve_transfer(transfer_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
