from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from app.contracts.financial import contract_status
from app.routers.deps import require

router = APIRouter(tags=['contracts'])


@router.get('/api/contracts/financial/status')
def financial_contract_status_endpoint(request: Request) -> dict[str, Any]:
    require(request, 'operations.manage')
    return contract_status()
