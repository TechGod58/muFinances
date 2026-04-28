from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_release_candidate_batches.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}", 'X-Trace-Id': 'test-trace-b110-b112'}


def test_b110_pilot_deployment_runs_complete_cycle() -> None:
    headers = admin_headers()

    status = client.get('/api/pilot-deployment/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B110'
    assert status.json()['complete'] is True

    run = client.post(
        '/api/pilot-deployment/run',
        headers=headers,
        json={'run_key': 'b110-regression', 'release_version': 'B110.regression'},
    )
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['internal_server_deployment_recorded'] is True
    assert payload['checks']['real_identity_connected'] is True
    assert payload['checks']['real_test_data_loaded'] is True
    assert payload['checks']['budget_cycle_completed'] is True
    assert payload['checks']['forecast_cycle_completed'] is True
    assert payload['checks']['close_cycle_completed'] is True
    assert payload['checks']['reporting_cycle_completed'] is True
    assert payload['checks']['selected_user_signoff_recorded'] is True
    assert payload['signoff']['status'] == 'signed'

    detail = client.get(f"/api/pilot-deployment/runs/{payload['id']}", headers=headers)
    assert detail.status_code == 200
    assert detail.json()['run_key'] == 'b110-regression'


def test_b111_parity_gap_review_requires_and_uses_pilot_evidence() -> None:
    headers = admin_headers()

    pilot = client.post('/api/pilot-deployment/run', headers=headers, json={'run_key': 'b111-pilot'})
    assert pilot.status_code == 200

    run = client.post(
        '/api/parity-gap-review/run',
        headers=headers,
        json={'run_key': 'b111-regression', 'pilot_run_id': pilot.json()['id']},
    )
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'reviewed_with_gaps'
    assert payload['complete'] is True
    assert payload['checks']['pilot_use_reviewed'] is True
    assert payload['checks']['feature_matrix_completed'] is True
    assert {'Prophix', 'Workday Adaptive Planning', 'Planful', 'Anaplan'} == set(payload['vendor_sources'])
    assert len(payload['matrix']) >= 8
    assert any(row['mufinances_status'] == 'met' for row in payload['matrix'])
    assert any(gap['gap_key'] == 'vendor_ecosystem_support' for gap in payload['gaps'])
    assert all(gap['pilot_run_id'] == pilot.json()['id'] for gap in payload['gaps'])

    status = client.get('/api/parity-gap-review/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['latest_run']['run_key'] == 'b111-regression'


def test_b112_release_candidate_builds_release_gate_evidence() -> None:
    headers = admin_headers()

    pilot = client.post('/api/pilot-deployment/run', headers=headers, json={'run_key': 'b112-pilot'})
    assert pilot.status_code == 200
    parity = client.post(
        '/api/parity-gap-review/run',
        headers=headers,
        json={'run_key': 'b112-parity', 'pilot_run_id': pilot.json()['id']},
    )
    assert parity.status_code == 200

    run = client.post(
        '/api/production-release-candidate/run',
        headers=headers,
        json={
            'run_key': 'b112-regression',
            'release_version': 'B112.regression',
            'pilot_run_id': pilot.json()['id'],
            'parity_run_id': parity.json()['id'],
        },
    )
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['scope_frozen'] is True
    assert payload['checks']['pilot_defects_fixed'] is True
    assert payload['checks']['full_regression_recorded'] is True
    assert payload['checks']['backup_restore_passed'] is True
    assert payload['checks']['security_review_passed'] is True
    assert payload['checks']['performance_review_passed'] is True
    assert payload['checks']['finance_signoff_recorded'] is True
    assert payload['checks']['it_signoff_recorded'] is True
    assert payload['checks']['operator_evidence_ready'] is True
    assert payload['signoffs']['finance']['status'] == 'signed'
    assert payload['signoffs']['it']['status'] == 'signed'
    assert payload['artifacts']['release_note']['status'] == 'published'

    rows = client.get('/api/production-release-candidate/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
