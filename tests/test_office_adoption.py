from __future__ import annotations

import os
import sys
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_office_adoption.db'
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
        json={'name': 'Office Adoption Proof Scenario', 'version': 'proof', 'start_period': '2026-07', 'end_period': '2027-06'},
    )
    assert response.status_code == 200
    return int(response.json()['id'])


def test_office_adoption_proof_validates_excel_roundtrip_and_powerpoint_artifacts() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/office/adoption/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'Office Adoption'
    assert status.json()['checks']['offline_workbook_reconciliation_testable'] is True

    proof = client.post(f'/api/office/adoption/proof?scenario_id={sid}', headers=headers, json={})
    assert proof.status_code == 200
    payload = proof.json()
    assert payload['complete'] is True
    assert all(payload['checks'].values())
    assert payload['ledger_reconciliation']['after'] >= payload['ledger_reconciliation']['before'] + 1

    template_path = Path(payload['template']['storage_path'])
    assert zipfile.is_zipfile(template_path)
    with zipfile.ZipFile(template_path, 'r') as archive:
        assert 'xl/workbook.xml' in archive.namelist()
        assert 'xl/worksheets/sheet2.xml' in archive.namelist()

    deck_path = Path(payload['powerpoint_deck']['storage_path'])
    assert zipfile.is_zipfile(deck_path)
    with zipfile.ZipFile(deck_path, 'r') as archive:
        names = archive.namelist()
        assert 'ppt/presentation.xml' in names
        assert 'ppt/slides/slide1.xml' in names
        assert any(name.startswith('ppt/media/chart') for name in names)

    actions = client.get(f'/api/office/workspace-actions?scenario_id={sid}', headers=headers)
    assert actions.status_code == 200
    assert any(action['action_type'] == 'office_adoption_proof' for action in actions.json()['actions'])
