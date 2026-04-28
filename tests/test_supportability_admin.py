from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_supportability_admin.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_login() -> tuple[dict[str, str], dict[str, object]]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    payload = response.json()
    return {'Authorization': f"Bearer {payload['token']}"}, payload['user']


def test_supportability_run_proves_troubleshooting_bundle_replay_and_diagnostics() -> None:
    headers, user = admin_login()

    status = client.get('/api/supportability/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B109A'
    assert status.json()['complete'] is True

    run = client.post('/api/supportability/run', headers=headers, json={'run_key': 'b109a-regression'})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['admin_troubleshooting_tools_ready'] is True
    assert payload['checks']['support_bundle_export_ready'] is True
    assert payload['checks']['error_replay_ids_ready'] is True
    assert payload['checks']['failed_job_replay_ready'] is True
    assert payload['checks']['connector_test_mode_ready'] is True
    assert payload['checks']['user_session_diagnostics_ready'] is True
    assert payload['checks']['permission_simulation_ready'] is True
    assert payload['checks']['operator_issue_reports_ready'] is True

    artifacts = payload['artifacts']
    assert artifacts['bundle']['replay_id'] == 'replay-b109a-regression'
    assert artifacts['bundle']['manifest']['diagnostics']['redaction'] == 'secrets omitted'
    assert artifacts['failed_job']['status'] == 'dead_letter'
    assert artifacts['failed_job_replay']['status'] == 'queued'
    assert artifacts['connector_test']['mode'] == 'test'
    assert artifacts['session_diagnostic']['active_sessions'] >= 1
    assert artifacts['permission_simulation']['allowed'] is True
    assert artifacts['issue_report']['status'] == 'open'
    assert artifacts['issue_report']['replay_id'] == 'replay-b109a-regression'

    detail = client.get(f"/api/supportability/runs/{payload['id']}", headers=headers)
    assert detail.status_code == 200
    assert detail.json()['run_key'] == 'b109a-regression'

    rows = client.get('/api/supportability/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1

    session = client.get(f"/api/supportability/users/{user['id']}/sessions", headers=headers)
    assert session.status_code == 200
    assert session.json()['user_id'] == user['id']


def test_supportability_individual_tools_create_operator_evidence() -> None:
    headers, user = admin_login()

    bundle = client.post('/api/supportability/bundles', headers=headers, json={'bundle_key': 'manual-support-bundle', 'replay_id': 'manual-replay'})
    assert bundle.status_code == 200
    assert bundle.json()['manifest']['database']['backend'] in {'sqlite', 'postgres', 'mssql'}

    permission = client.post(
        '/api/supportability/permissions/simulate',
        headers=headers,
        json={'simulation_key': 'manual-permission', 'user_id': user['id'], 'permission_key': 'operations.manage'},
    )
    assert permission.status_code == 200
    assert permission.json()['allowed'] is True

    connector = client.post(
        '/api/supportability/connectors/test-mode',
        headers=headers,
        json={'test_key': 'manual-connector-test', 'connector_key': 'manual-support-connector', 'system_type': 'file', 'adapter_key': 'erp_gl'},
    )
    assert connector.status_code == 200
    assert connector.json()['mode'] == 'test'

    issue = client.post(
        '/api/supportability/issues',
        headers=headers,
        json={'issue_key': 'manual-issue', 'title': 'Manual support issue', 'severity': 'low', 'replay_id': 'manual-replay', 'detail': {'bundle': bundle.json()['bundle_key']}},
    )
    assert issue.status_code == 200
    assert issue.json()['status'] == 'open'
