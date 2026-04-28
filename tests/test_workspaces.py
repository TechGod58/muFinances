from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_workspaces.db'
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


def scenario_id() -> int:
    scenarios = client.get('/api/scenarios', headers=admin_headers()).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def test_admin_gets_all_role_specific_workspaces() -> None:
    headers = admin_headers()
    sid = scenario_id()
    response = client.get(f'/api/workspaces?scenario_id={sid}', headers=headers)
    assert response.status_code == 200
    keys = {workspace['key'] for workspace in response.json()['workspaces']}
    assert {'budget_office', 'department_planner', 'controller', 'grants', 'executive'} <= keys
    executive = next(workspace for workspace in response.json()['workspaces'] if workspace['key'] == 'executive')
    assert any(metric['label'] == 'Net position' for metric in executive['metrics'])


def test_department_planner_workspace_visibility_and_status() -> None:
    headers = admin_headers()
    created = client.post(
        '/api/security/users',
        headers=headers,
        json={
            'email': 'planner.workspace@mufinances.local',
            'display_name': 'Planner Workspace',
            'password': 'PlannerPass!3200',
            'role_keys': ['department.planner'],
        },
    )
    assert created.status_code == 200
    planner_login = client.post(
        '/api/auth/login',
        json={'email': 'planner.workspace@mufinances.local', 'password': 'PlannerPass!3200'},
    )
    assert planner_login.status_code == 200
    planner_headers = {'Authorization': f"Bearer {planner_login.json()['token']}"}
    changed = client.post(
        '/api/auth/password',
        headers=planner_headers,
        json={'current_password': 'PlannerPass!3200', 'new_password': 'PlannerPass!3200-Changed'},
    )
    assert changed.status_code == 200
    sid = scenario_id()
    response = client.get(f'/api/workspaces?scenario_id={sid}', headers=planner_headers)
    assert response.status_code == 200
    keys = {workspace['key'] for workspace in response.json()['workspaces']}
    assert 'department_planner' in keys
    assert 'budget_office' not in keys

    status = client.get('/api/workspaces/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B11'
    assert payload['complete'] is True
    assert payload['checks']['executive_dashboard_ready'] is True
