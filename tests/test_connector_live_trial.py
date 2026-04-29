from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_connector_live_trial.db'
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


def test_connector_live_trial_exercises_gl_sis_hr_payroll_grants_and_banking() -> None:
    headers = admin_headers()

    status = client.get('/api/integrations/connector-live-trial/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B122'
    assert status.json()['complete'] is True

    run = client.post(
        '/api/integrations/connector-live-trial/run',
        headers=headers,
        json={
            'run_key': 'b122-live-trial-regression',
            'live_mode': True,
            'credential_refs': {
                'gl': 'gl-live-token',
                'sis': 'sis-oauth-token',
                'hr_payroll': 'hr-payroll-token',
                'grants': 'grants-oauth-token',
                'banking': 'banking-api-token',
            },
        },
    )
    assert run.status_code == 200, run.text
    payload = run.json()
    assert payload['batch'] == 'B122'
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['mode'] == 'live_ready'
    assert len(payload['connectors']) == 5
    assert {row['slot'] for row in payload['connectors']} == {'gl', 'sis', 'hr_payroll', 'grants', 'banking'}
    assert {row['auth_type'] for row in payload['connectors']} == {'api_key', 'oauth_client'}
    assert all(row['auth_status'] == 'ready' for row in payload['connectors'])
    assert all(row['sync_status'] == 'complete' for row in payload['connectors'])
    assert all(row['retry_status'] == 'retry_scheduled' for row in payload['connectors'])
    assert payload['checks']['oauth_and_api_key_flows_ready'] is True
    assert payload['checks']['rejection_queue_ready'] is True
    assert payload['checks']['drillback_ready'] is True
    assert payload['evidence']['rejections'] >= 1
    assert payload['evidence']['sync_logs'] >= 5
    assert all(source == 'payload' for source in payload['evidence']['credential_sources'].values())

    rows = client.get('/api/integrations/connector-live-trial/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
