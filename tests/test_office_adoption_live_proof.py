from __future__ import annotations

import os
import sys
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_office_adoption_live_proof.db'
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
        json={'name': 'Office Live Proof Scenario', 'version': 'b123', 'start_period': '2026-07', 'end_period': '2027-06'},
    )
    assert response.status_code == 200
    return int(response.json()['id'])


def test_office_live_proof_records_excel_roundtrip_rejections_comments_and_powerpoint_refresh() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/office/live-proof/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B123'
    assert status.json()['complete'] is True

    run = client.post('/api/office/live-proof/run', headers=headers, json={'scenario_id': sid, 'run_key': 'b123-office-live-proof'})
    assert run.status_code == 200, run.text
    payload = run.json()
    assert payload['batch'] == 'B123'
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert all(payload['checks'].values())
    assert payload['artifacts']['roundtrip_import']['accepted_rows'] == 1
    assert payload['artifacts']['roundtrip_import']['rejected_rows'] == 1
    assert payload['artifacts']['comment']['status'] == 'open'
    assert {'LedgerInput.Amount', 'LedgerInput.Period', 'Variance.ActualVsBudget'} <= {
        row['range_name'] for row in payload['artifacts']['named_ranges']
    }

    template_path = Path(payload['artifacts']['certification_template']['storage_path'])
    deck_path = Path(payload['artifacts']['certification_powerpoint_deck']['storage_path'])
    package_path = Path(payload['artifacts']['certification_workbook_package']['storage_path'])
    assert zipfile.is_zipfile(template_path)
    assert zipfile.is_zipfile(deck_path)
    assert zipfile.is_zipfile(package_path)
    with zipfile.ZipFile(deck_path, 'r') as archive:
        assert 'ppt/presentation.xml' in archive.namelist()
        assert any(name.startswith('ppt/media/chart') for name in archive.namelist())

    rows = client.get('/api/office/live-proof/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
