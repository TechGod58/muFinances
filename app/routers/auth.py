from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app.schemas import AuthLogin, PasswordChangeCreate
from app.services.access_guard import assert_ad_ou_allowed
from app.services.security import (
    authenticate,
    build_sso_authorization_url,
    change_password,
    sso_config,
)

router = APIRouter(prefix='/api/auth', tags=['auth'])


@router.get('/bootstrap')
def auth_bootstrap() -> dict[str, Any]:
    return {
        'auth_required': True,
        'login_endpoint': '/api/auth/login',
        'sso': sso_config(),
        'default_admin_email': 'admin@mufinances.local',
    }


@router.post('/login')
def auth_login(payload: AuthLogin) -> dict[str, Any]:
    result = authenticate(payload.email, payload.password)
    if result is None:
        raise HTTPException(status_code=401, detail='Invalid email or password.')
    try:
        assert_ad_ou_allowed(result['user'])
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return result


@router.post('/password')
def auth_change_password(payload: PasswordChangeCreate, request: Request) -> dict[str, Any]:
    try:
        user = change_password(request.state.user, payload.current_password, payload.new_password)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {'changed': True, 'user': user}


@router.get('/sso/config')
def auth_sso_config() -> dict[str, Any]:
    return sso_config()


@router.get('/sso/login')
def auth_sso_login() -> dict[str, Any]:
    return build_sso_authorization_url()


@router.get('/sso/callback')
def auth_sso_callback() -> dict[str, Any]:
    raise HTTPException(
        status_code=501,
        detail='SSO callback is reserved for server deployment. Configure issuer, client secret, and token validation before enabling code exchange.',
    )


@router.get('/me')
def auth_me(request: Request) -> dict[str, Any]:
    return request.state.user
