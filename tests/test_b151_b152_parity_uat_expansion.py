from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_b151_b152_parity_uat_expansion.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200, response.text
    return {'Authorization': f"Bearer {response.json()['token']}"}


def test_b151_minimum_viable_prophix_parity_matrix_records_pass_fail_evidence() -> None:
    headers = admin_headers()

    status = client.get('/api/parity/minimum-viable/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B151'
    assert status.json()['counts']['workflow_rows'] == 13

    run = client.post('/api/parity/minimum-viable/run', headers=headers, json={'run_key': 'b151-regression'})
    assert run.status_code == 200, run.text
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['summary']['total'] == 13
    assert payload['summary']['failed'] == 0
    assert payload['summary']['passed'] == 13

    keys = {row['workflow_key'] for row in payload['matrix']}
    assert keys == {
        'budgeting', 'forecasting', 'reporting', 'close', 'consolidation',
        'intercompany', 'integrations', 'security', 'workflow', 'ai',
        'excel_office', 'audit', 'operations',
    }
    assert all(row['result'] in {'pass', 'fail'} for row in payload['matrix'])
    assert all(row['evidence_count'] >= 3 for row in payload['matrix'])
    assert next(row for row in payload['matrix'] if row['workflow_key'] == 'consolidation')['result'] == 'pass'

    failed = client.post(
        '/api/parity/minimum-viable/run',
        headers=headers,
        json={'run_key': 'b151-forced-gap', 'overrides': {'integrations': {'result': 'fail', 'notes': 'Connector credentials not supplied.'}}},
    )
    assert failed.status_code == 200
    assert failed.json()['status'] == 'failed'
    assert failed.json()['summary']['failed'] == 1


def test_b152_uat_expansion_covers_roles_failures_retests_and_signoff() -> None:
    headers = admin_headers()

    status = client.get('/api/user-acceptance/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B152'
    assert status.json()['checks']['auditor_script_ready'] is True
    assert status.json()['checks']['integration_admin_script_ready'] is True
    assert status.json()['checks']['retest_tracking_ready'] is True

    run = client.post('/api/user-acceptance/run', headers=headers, json={'run_key': 'b152-regression'})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['summary']['role_count'] == 8
    assert payload['summary']['retest_count'] == 1
    assert payload['summary']['checks']['retests_recorded'] is True

    roles = {script['role_key'] for script in payload['scripts']}
    assert {'auditor', 'integration_admin'} <= roles
    assert payload['failures'][0]['status'] == 'verified'
    assert payload['retests'][0]['status'] == 'passed'
    assert len(payload['signoffs']) == 8
    assert all(signoff['status'] == 'signed' for signoff in payload['signoffs'])

