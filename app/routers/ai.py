from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request

from app.routers.deps import require
from app.services.ai_explainability import list_explanations, status as explainability_status
from app.services.governed_automation import list_agent_actions, status as automation_status

router = APIRouter(tags=['ai'])


@router.get('/api/ai-explainability/status')
def ai_explainability_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'automation.manage')
    return explainability_status()


@router.get('/api/ai-explainability/explanations')
def ai_explanations_endpoint(request: Request, scenario_id: int = Query(..., ge=1), status: str | None = None) -> dict[str, Any]:
    require(request, 'automation.manage')
    rows = list_explanations(scenario_id, status)
    return {'scenario_id': scenario_id, 'count': len(rows), 'explanations': rows}


@router.get('/api/automation/status')
def automation_status_endpoint() -> dict[str, Any]:
    return automation_status()


@router.get('/api/automation/agent-actions')
def automation_agent_actions(request: Request, scenario_id: int | None = Query(None, ge=1), status: str | None = None) -> dict[str, Any]:
    require(request, 'automation.manage')
    rows = list_agent_actions(scenario_id, status)
    return {'scenario_id': scenario_id, 'count': len(rows), 'actions': rows}
