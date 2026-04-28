from __future__ import annotations

import os
import sys
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_reporting_output_completion.db'
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


def test_reporting_output_completion_generates_real_artifacts_and_distribution() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    response = client.post(f'/api/reporting/output-completion/run?scenario_id={sid}', headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload['complete'] is True
    assert payload['checks']['pdf_ready'] is True
    assert payload['checks']['excel_ready'] is True
    assert payload['checks']['powerpoint_ready'] is True
    assert payload['checks']['scheduled_distribution_ready'] is True
    assert payload['checks']['visual_regression_tests_ready'] is True

    artifacts = {item['artifact_type']: item for item in payload['artifacts']}
    xlsx_path = Path(artifacts['excel']['storage_path'])
    pptx_path = Path(artifacts['pptx']['storage_path'])
    assert zipfile.is_zipfile(xlsx_path)
    assert zipfile.is_zipfile(pptx_path)
    with zipfile.ZipFile(xlsx_path) as archive:
        assert 'xl/workbook.xml' in archive.namelist()
        assert b'Financial Statement' in archive.read('xl/worksheets/sheet1.xml')
    with zipfile.ZipFile(pptx_path) as archive:
        assert 'ppt/presentation.xml' in archive.namelist()
        assert any(name.startswith('ppt/media/chart') for name in archive.namelist())

    assert artifacts['pdf']['metadata']['page_count'] >= 1
    assert artifacts['email']['content_type'] == 'message/rfc822'
    assert all(validation['status'] == 'passed' for validation in payload['validations'])


def test_reporting_output_completion_status_surface() -> None:
    headers = admin_headers()
    status = client.get('/api/reporting/output-completion/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'Reporting Output Completion'
    assert status.json()['checks']['real_excel_artifacts_ready'] is True
