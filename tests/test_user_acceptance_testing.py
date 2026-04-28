from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_user_acceptance_testing.db'
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


def test_user_acceptance_testing_scripts_failures_fixes_and_signoff() -> None:
    headers = admin_headers()

    status = client.get('/api/user-acceptance/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B107'
    assert status.json()['complete'] is True

    run = client.post('/api/user-acceptance/run', headers=headers, json={'run_key': 'b107-regression'})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['summary']['role_count'] == 6
    assert payload['summary']['script_count'] == 6
    assert payload['summary']['result_count'] == 6
    assert payload['summary']['failure_count'] == 1
    assert payload['summary']['verified_failure_count'] == 1
    assert payload['summary']['signoff_count'] == 6

    roles = {script['role_key'] for script in payload['scripts']}
    assert roles == {'budget_office', 'controller', 'department_planner', 'grants', 'executive', 'it_admin'}
    assert all(len(script['steps']) >= 4 for script in payload['scripts'])
    assert all(result['status'] == 'passed' for result in payload['results'])
    assert payload['failures'][0]['status'] == 'verified'
    assert payload['failures'][0]['fix_summary']
    assert all(signoff['status'] == 'signed' for signoff in payload['signoffs'])

    detail = client.get(f"/api/user-acceptance/runs/{payload['id']}", headers=headers)
    assert detail.status_code == 200
    assert detail.json()['run_key'] == 'b107-regression'

    rows = client.get('/api/user-acceptance/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1

    refreshed = client.get('/api/user-acceptance/status', headers=headers)
    assert refreshed.json()['latest_run']['run_key'] == 'b107-regression'
    assert refreshed.json()['counts']['open_failures'] == 0
