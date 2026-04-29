from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_multi_user_pilot_cycle.db'
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


def test_multi_user_pilot_cycle_executes_full_finance_cycle_with_roles() -> None:
    headers = admin_headers()

    status = client.get('/api/multi-user-pilot-cycle/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B127'
    assert status.json()['complete'] is True

    run = client.post('/api/multi-user-pilot-cycle/run', headers=headers, json={'run_key': 'b127-pilot-cycle'})
    assert run.status_code == 200, run.text
    payload = run.json()
    assert payload['batch'] == 'B127'
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert all(payload['checks'].values())
    assert {item['role_key'] for item in payload['role_participants']} == {
        'budget_office',
        'controller',
        'department_planner',
        'grants',
        'executive',
        'it_admin',
    }
    assert all(item['status'] == 'signed' for item in payload['role_participants'])
    assert set(payload['cycle_steps']) == {'budget', 'forecast', 'close', 'consolidation', 'reporting', 'board_package'}
    assert all(step['status'] == 'passed' for step in payload['cycle_steps'].values())
    assert payload['pilot']['status'] == 'passed'
    assert payload['consolidation']['status'] == 'passed'

    rows = client.get('/api/multi-user-pilot-cycle/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
