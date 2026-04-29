from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_backend_router_decomposition.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def owner_for(path: str, method: str = 'GET') -> str:
    for route in app.routes:
        methods = getattr(route, 'methods', set()) or set()
        if getattr(route, 'path', None) == path and method.upper() in methods:
            return route.endpoint.__module__
    raise AssertionError(f'No route found for {method} {path}')


def test_api_runtime_route_table_has_no_duplicate_method_paths() -> None:
    seen: set[tuple[str, str]] = set()
    duplicates: list[tuple[str, str]] = []
    for route in app.routes:
        methods = getattr(route, 'methods', set()) or set()
        path = getattr(route, 'path', '')
        if not path.startswith('/api'):
            continue
        for method in methods - {'HEAD', 'OPTIONS'}:
            key = (method, path)
            if key in seen:
                duplicates.append(key)
            seen.add(key)
    assert duplicates == []
    assert app.state.api_route_deduplication['removed_count'] > 0


def test_core_financial_domains_are_mounted_from_router_modules() -> None:
    assert owner_for('/api/security/status') == 'app.routers.security_admin'
    assert owner_for('/api/security/users') == 'app.routers.security_admin'
    assert owner_for('/api/operating-budget/status') == 'app.routers.budget'
    assert owner_for('/api/operating-budget/submissions') == 'app.routers.budget'
    assert owner_for('/api/foundation/ledger') == 'app.routers.ledger'
    assert owner_for('/api/ledger-depth/journals') == 'app.routers.ledger'
    assert owner_for('/api/reporting/status') == 'app.routers.reporting'
    assert owner_for('/api/close/status') == 'app.routers.close'
    assert owner_for('/api/integrations/status') == 'app.routers.integrations'
    assert owner_for('/api/operations/status') == 'app.routers.operations'
    assert owner_for('/api/ai-explainability/status') == 'app.routers.ai'
    assert owner_for('/api/workflow-designer/status') == 'app.routers.workflow'


def test_decomposed_router_paths_preserve_runtime_behavior() -> None:
    headers = admin_headers()
    security = client.get('/api/security/status', headers=headers)
    assert security.status_code == 200
    assert security.json()['batch'] == 'B02'
    assert security.json()['checks']['api_auth_gate_ready'] is True

    budget = client.get('/api/operating-budget/status', headers=headers)
    assert budget.status_code == 200
    assert budget.json()['batch'] == 'B03'

    scenarios = client.get('/api/scenarios', headers=headers)
    assert scenarios.status_code == 200
    scenario_id = scenarios.json()[0]['id']

    ledger = client.get(f'/api/foundation/ledger?scenario_id={scenario_id}', headers=headers)
    assert ledger.status_code == 200
    assert ledger.json()['scenario_id'] == scenario_id

    for path in (
        '/api/reporting/status',
        '/api/close/status',
        '/api/integrations/status',
        '/api/operations/status',
        '/api/ai-explainability/status',
        '/api/workflow-designer/status',
    ):
        response = client.get(path, headers=headers)
        assert response.status_code == 200, path
        assert response.json()['complete'] is True
