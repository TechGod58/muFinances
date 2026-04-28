from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_consolidation_advanced_controls.db'
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


def scenario_id() -> int:
    scenarios = client.get('/api/scenarios', headers=admin_headers()).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def test_consolidation_advanced_controls_and_elimination_review() -> None:
    headers = admin_headers()
    sid = scenario_id()
    period = '2026-11'

    parent = client.post('/api/close/consolidation-entities', headers=headers, json={'entity_code': 'CAMPUS', 'entity_name': 'Campus', 'base_currency': 'USD', 'gaap_basis': 'US_GAAP'})
    child = client.post('/api/close/consolidation-entities', headers=headers, json={'entity_code': 'FOUNDATION', 'entity_name': 'Foundation', 'parent_entity_code': 'CAMPUS', 'base_currency': 'USD', 'gaap_basis': 'US_GAAP'})
    assert parent.status_code == 200
    assert child.status_code == 200
    entities = client.get('/api/close/consolidation-entities', headers=headers)
    assert any(row['entity_code'] == 'FOUNDATION' and row['parent_entity_code'] == 'CAMPUS' for row in entities.json()['entities'])

    ownership = client.post('/api/close/entity-ownerships', headers=headers, json={'scenario_id': sid, 'parent_entity_code': 'CAMPUS', 'child_entity_code': 'FOUNDATION', 'ownership_percent': 100, 'effective_period': period})
    assert ownership.status_code == 200
    assert ownership.json()['ownership_percent'] == 100

    setting = client.post('/api/close/consolidation-settings', headers=headers, json={'scenario_id': sid, 'gaap_basis': 'US_GAAP', 'reporting_currency': 'USD', 'translation_method': 'placeholder', 'enabled': True})
    assert setting.status_code == 200

    elimination = client.post('/api/close/eliminations', headers=headers, json={'scenario_id': sid, 'period': period, 'entity_code': 'CAMPUS', 'account_code': 'TRANSFER', 'amount': -25000, 'reason': 'Eliminate internal support'})
    assert elimination.status_code == 200
    assert elimination.json()['review_status'] == 'draft'

    submitted = client.post(f"/api/close/eliminations/{elimination.json()['id']}/submit", headers=headers, json={'note': 'Ready for review.'})
    assert submitted.status_code == 200
    assert submitted.json()['review_status'] == 'pending_review'
    approved = client.post(f"/api/close/eliminations/{elimination.json()['id']}/approve", headers=headers, json={'note': 'Approved.'})
    assert approved.status_code == 200
    assert approved.json()['review_status'] == 'approved'

    run = client.post('/api/close/consolidation-runs', headers=headers, json={'scenario_id': sid, 'period': period})
    assert run.status_code == 200
    assert run.json()['audit_report']['contents']['controls']['multi_gaap'] == 'placeholder'
    assert run.json()['audit_report']['contents']['controls']['currency_translation'] == 'placeholder'

    reports = client.get(f'/api/close/consolidation-audit-reports?scenario_id={sid}', headers=headers)
    assert reports.status_code == 200
    assert reports.json()['audit_reports']


def test_close_status_reports_b20_complete() -> None:
    response = client.get('/api/close/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B20'
    assert payload['complete'] is True
    assert payload['checks']['entity_hierarchy_ready'] is True
    assert payload['checks']['elimination_review_workflow_ready'] is True
