from __future__ import annotations

from fastapi import APIRouter

from app import db

router = APIRouter(tags=['health'])


@router.get('/api/health')
def health() -> dict[str, str]:
    return {'status': 'ok', 'app': 'campus-fpm-base'}


@router.get('/api/health/live')
def live() -> dict[str, str]:
    return {'status': 'live', 'app': 'campus-fpm-base'}


@router.get('/api/health/ready')
def ready() -> dict[str, object]:
    database_ok = db.fetch_one('SELECT COUNT(*) AS count FROM scenarios') is not None
    return {
        'status': 'ready' if database_ok else 'not_ready',
        'checks': {'database': database_ok},
        'database': db.database_runtime(),
    }
