from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_excel_adoption_certification.db'
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
    response = client.post(
        '/api/scenarios',
        headers=headers,
        json={'name': 'B94 Excel Certification Scenario', 'version': 'b94', 'start_period': '2026-08', 'end_period': '2027-07'},
    )
    assert response.status_code == 200
    return int(response.json()['id'])


def test_excel_adoption_certification_proves_roundtrip_rejections_comments_and_powerpoint() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/office/excel-certification/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B94'
    assert status.json()['complete'] is True

    run = client.post(f'/api/office/excel-certification/run?scenario_id={sid}', headers=headers)
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['protected_template_metadata_ready'] is True
    assert payload['checks']['named_ranges_ready'] is True
    assert payload['checks']['refresh_button_ready'] is True
    assert payload['checks']['publish_button_ready'] is True
    assert payload['checks']['offline_edit_accepted'] is True
    assert payload['checks']['roundtrip_rejected_rows_ready'] is True
    assert payload['checks']['cell_comments_ready'] is True
    assert payload['checks']['workbook_package_ready'] is True
    assert payload['checks']['powerpoint_refresh_ready'] is True
    assert payload['detail']['roundtrip_import']['accepted_rows'] == 1
    assert payload['detail']['roundtrip_import']['rejected_rows'] == 1

    rows = client.get('/api/office/excel-certification/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
