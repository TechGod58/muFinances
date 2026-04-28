from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_consolidation_certification.db'
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


def test_consolidation_certification_proves_advanced_consolidation_stack() -> None:
    headers = admin_headers()

    status = client.get('/api/close/consolidation-certification/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B97'
    assert status.json()['complete'] is True

    run = client.post('/api/close/consolidation-certification/run', headers=headers, json={})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['multi_entity_hierarchy_created'] is True
    assert payload['checks']['ownership_chain_calculated'] is True
    assert payload['checks']['minority_interest_recorded'] is True
    assert payload['checks']['intercompany_matched'] is True
    assert payload['checks']['elimination_approved'] is True
    assert payload['checks']['currency_translation_recorded'] is True
    assert payload['checks']['cta_journal_recorded'] is True
    assert payload['checks']['multi_gaap_mapping_applied'] is True
    assert payload['checks']['statutory_schedules_assembled'] is True
    assert payload['artifacts']['statutory_pack']['status'] == 'assembled'
    assert len(payload['artifacts']['supplemental_schedules']) >= 3

    rows = client.get('/api/close/consolidation-certification/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
