from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_fpa_workflow_certification.db'
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


def test_fpa_workflow_certification_runs_end_to_end_with_finance_signoff() -> None:
    headers = admin_headers()

    status = client.get('/api/fpa-workflow-certification/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B93'
    assert status.json()['complete'] is True

    run = client.post('/api/fpa-workflow-certification/run', headers=headers, json={})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['operating_budget_approved'] is True
    assert payload['checks']['forecast_posted'] is True
    assert payload['checks']['scenario_compare_has_variance'] is True
    assert payload['checks']['variance_workflow_approved'] is True
    assert payload['checks']['ai_narrative_approved'] is True
    assert payload['checks']['scenario_published_locked'] is True
    assert payload['checks']['board_package_and_pdf_ready'] is True
    assert payload['checks']['finance_signoff_recorded'] is True
    assert payload['finance_signoff']['signoff_type'] == 'finance_certification'
    assert payload['artifacts']['board_package']['status'] == 'assembled'

    rows = client.get('/api/fpa-workflow-certification/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
