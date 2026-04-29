from __future__ import annotations

from urllib.parse import parse_qs
from typing import Any

import secrets

from fastapi import APIRouter, HTTPException, Query, Request, Response

from app.schemas import AuthLogin, PasswordChangeCreate
from app.services.access_guard import assert_ad_ou_allowed
from app.services.security import (
    APP_ENV,
    CSRF_COOKIE_NAME,
    SESSION_HOURS,
    SESSION_COOKIE_MODE,
    SESSION_COOKIE_NAME,
    authenticate,
    build_sso_authorization_url,
    change_password,
    complete_saml_callback,
    complete_sso_callback,
    logout_session,
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
def auth_login(payload: AuthLogin, request: Request, response: Response) -> dict[str, Any]:
    try:
        result = authenticate(payload.email, payload.password, request.client.host if request.client else '')
    except PermissionError as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=401, detail='Invalid email or password.')
    try:
        assert_ad_ou_allowed(result['user'])
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    _apply_session_cookies(response, result)
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
def auth_sso_callback(
    code: str | None = Query(None),
    state: str | None = Query(None),
    id_token: str | None = Query(None),
    error: str | None = Query(None),
    error_description: str | None = Query(None),
) -> dict[str, Any]:
    if error:
        raise HTTPException(status_code=400, detail=error_description or error)
    try:
        return complete_sso_callback({'code': code, 'state': state, 'id_token': id_token})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/sso/callback')
async def auth_sso_callback_post(request: Request) -> dict[str, Any]:
    body = (await request.body()).decode('utf-8')
    payload = {key: values[-1] for key, values in parse_qs(body, keep_blank_values=True).items()}
    try:
        if payload.get('SAMLResponse'):
            return complete_saml_callback(payload)
        return complete_sso_callback(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/logout')
def auth_logout(request: Request, response: Response) -> dict[str, Any]:
    authorization = request.headers.get('authorization', '')
    token = authorization.removeprefix('Bearer ').strip() if authorization.lower().startswith('bearer ') else ''
    if not token:
        token = request.cookies.get(SESSION_COOKIE_NAME, '')
    result = logout_session(token, actor=request.state.user.get('email', 'api.user'))
    response.delete_cookie(SESSION_COOKIE_NAME, path='/')
    response.delete_cookie(CSRF_COOKIE_NAME, path='/')
    return result


@router.get('/me')
def auth_me(request: Request) -> dict[str, Any]:
    return request.state.user


def _apply_session_cookies(response: Response, result: dict[str, Any]) -> None:
    if not SESSION_COOKIE_MODE:
        result['session_mode'] = 'bearer'
        return
    csrf_token = secrets.token_urlsafe(32)
    secure = APP_ENV in {'prod', 'production'}
    response.set_cookie(
        SESSION_COOKIE_NAME,
        result['token'],
        httponly=True,
        secure=secure,
        samesite='strict',
        max_age=SESSION_HOURS * 60 * 60,
        path='/',
    )
    response.set_cookie(
        CSRF_COOKIE_NAME,
        csrf_token,
        httponly=False,
        secure=secure,
        samesite='strict',
        max_age=SESSION_HOURS * 60 * 60,
        path='/',
    )
    result['session_mode'] = 'cookie'
    result['csrf_token'] = csrf_token
    if APP_ENV in {'prod', 'production'}:
        result.pop('token', None)
