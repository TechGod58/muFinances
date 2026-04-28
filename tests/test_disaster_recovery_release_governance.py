from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_disaster_recovery_release_governance.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}", 'X-Trace-Id': 'test-trace-b106'}


def test_disaster_recovery_release_governance_run_builds_cutover_evidence() -> None:
    headers = admin_headers()

    status = client.get('/api/disaster-recovery-release/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B106'
    assert status.json()['complete'] is True

    run = client.post(
        '/api/disaster-recovery-release/run',
        headers=headers,
        json={'run_key': 'b106-regression', 'release_version': 'B106.regression'},
    )
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['restore_drills_ready'] is True
    assert payload['checks']['rollback_plans_ready'] is True
    assert payload['checks']['release_notes_ready'] is True
    assert payload['checks']['environment_promotion_ready'] is True
    assert payload['checks']['config_export_import_ready'] is True
    assert payload['checks']['operational_signoff_checklist_ready'] is True

    artifacts = payload['artifacts']
    assert artifacts['restore_drill']['status'] == 'pass'
    assert artifacts['rollback_plan']['status'] == 'approved'
    assert artifacts['release_note']['status'] == 'published'
    assert artifacts['promotion']['status'] == 'approved'
    assert artifacts['config_export']['direction'] == 'export'
    assert artifacts['config_import']['direction'] == 'import'
    assert all(item['status'] == 'ready' for item in artifacts['readiness_items'])
    assert {item['environment_key'] for item in artifacts['environments']} == {'staging', 'production'}

    rows = client.get('/api/disaster-recovery-release/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1

    workspace = client.get('/api/deployment-governance/workspace', headers=headers)
    assert workspace.status_code == 200
    workspace_payload = workspace.json()
    assert any(row['release_version'] == 'B106.regression' for row in workspace_payload['release_notes'])
    assert any(row['release_version'] == 'B106.regression' for row in workspace_payload['promotions'])
    assert any(row['item_key'] == 'operational-signoff-complete' and row['status'] == 'ready' for row in workspace_payload['readiness_items'])


def test_disaster_recovery_release_status_reports_latest_run() -> None:
    headers = admin_headers()
    client.post('/api/disaster-recovery-release/run', headers=headers, json={'run_key': 'b106-status', 'release_version': 'B106.status'})

    status = client.get('/api/disaster-recovery-release/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B106'
    assert payload['latest_run']['run_key'] == 'b106-status'
    assert payload['counts']['restore_drills'] >= 1
    assert payload['counts']['rollback_plans'] >= 1
    assert payload['counts']['config_snapshots'] >= 2
