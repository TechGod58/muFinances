from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_reporting_artifact_certification.db'
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


def scenario_id(headers: dict[str, str]) -> int:
    scenarios = client.get('/api/scenarios', headers=headers).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def test_b158_reporting_artifact_certification_validates_artifacts_pagination_retention_and_accuracy() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/reporting/artifact-certification/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B158'
    assert payload['complete'] is True
    assert payload['checks']['pdf_artifact_certification_ready'] is True
    assert payload['checks']['chart_export_certification_ready'] is True

    run = client.post(
        '/api/reporting/artifact-certification/run',
        headers=headers,
        json={'scenario_id': sid, 'run_key': 'b158-regression', 'signed_by': 'controller@manchester.edu'},
    )
    assert run.status_code == 200, run.text
    proof = run.json()
    assert proof['batch'] == 'B158'
    assert proof['status'] == 'passed'
    assert proof['complete'] is True
    assert proof['checks']['pdf_artifacts_validated'] is True
    assert proof['checks']['excel_artifacts_validated'] is True
    assert proof['checks']['powerpoint_artifacts_validated'] is True
    assert proof['checks']['email_artifacts_validated'] is True
    assert proof['checks']['chart_artifacts_validated'] is True
    assert proof['checks']['board_package_pagination_validated'] is True
    assert proof['checks']['retention_validated'] is True
    assert proof['checks']['downloadable_files_validated'] is True
    assert proof['checks']['statement_accuracy_validated'] is True

    artifact_types = {item['artifact_type'] for item in proof['artifact_manifest']}
    assert {'pdf', 'excel', 'pptx', 'email', 'png'} <= artifact_types
    assert all(item['validation_status'] == 'passed' for item in proof['artifact_manifest'])
    assert all(item['file_exists'] for item in proof['artifact_manifest'])
    assert any(item['chart_image_embeds'] >= 1 for item in proof['artifact_manifest'])
    assert proof['signoff']['signed_by'] == 'controller@manchester.edu'
    assert proof['signoff']['all_checks_passed'] is True

    rows = client.get('/api/reporting/artifact-certification/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
