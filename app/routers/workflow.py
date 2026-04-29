from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request

from app.routers.deps import require
from app.services.workflow_designer import list_tasks, status

router = APIRouter(tags=['workflow'])


@router.get('/api/workflow-designer/status')
def workflow_designer_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'scenario.manage')
    return status()


@router.get('/api/workflow-designer/tasks')
def workflow_designer_tasks(request: Request, scenario_id: int | None = Query(None, ge=1), status: str | None = None) -> dict[str, Any]:
    require(request, 'scenario.manage')
    rows = list_tasks(scenario_id, status)
    return {'scenario_id': scenario_id, 'count': len(rows), 'tasks': rows}
