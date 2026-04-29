from __future__ import annotations

from fastapi import HTTPException, Request

from app.services.security import require_permission


def require(request: Request, permission: str) -> None:
    try:
        require_permission(request.state.user, permission)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
