from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_close_depth.db'
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


def test_close_depth_templates_dependencies_workflow_confirmations_and_locks() -> None:
    headers = admin_headers()
    sid = scenario_id()
    period = '2026-10'

    calendar = client.post(
        '/api/close/calendar',
        headers=headers,
        json={'scenario_id': sid, 'period': period, 'close_start': '2026-11-01', 'close_due': '2026-11-05'},
    )
    assert calendar.status_code == 200
    assert calendar.json()['lock_state'] == 'open'

    base_template = client.post(
        '/api/close/templates',
        headers=headers,
        json={'template_key': 'ledger-review', 'title': 'Review ledger', 'owner_role': 'Controller', 'due_day_offset': 0, 'dependency_keys': [], 'active': True},
    )
    assert base_template.status_code == 200
    dependent_template = client.post(
        '/api/close/templates',
        headers=headers,
        json={'template_key': 'subledger-review', 'title': 'Review subledger', 'owner_role': 'Controller', 'due_day_offset': 0, 'dependency_keys': ['ledger-review'], 'active': True},
    )
    assert dependent_template.status_code == 200

    generated = client.post(f'/api/close/templates/instantiate?scenario_id={sid}&period={period}', headers=headers, json={})
    assert generated.status_code == 200
    assert generated.json()['count'] == 2
    dependent = next(item for item in generated.json()['items'] if item['checklist_key'] == 'subledger-review')
    assert dependent['dependency_status'] == 'blocked'

    blocked_complete = client.post(f"/api/close/checklists/{dependent['id']}/complete", headers=headers, json={'evidence': {}})
    assert blocked_complete.status_code == 409

    base = next(item for item in generated.json()['items'] if item['checklist_key'] == 'ledger-review')
    completed_base = client.post(f"/api/close/checklists/{base['id']}/complete", headers=headers, json={'evidence': {'reviewed': True}})
    assert completed_base.status_code == 200
    completed_dependent = client.post(f"/api/close/checklists/{dependent['id']}/complete", headers=headers, json={'evidence': {'reviewed': True}})
    assert completed_dependent.status_code == 200

    reconciliation = client.post(
        '/api/close/reconciliations',
        headers=headers,
        json={'scenario_id': sid, 'period': period, 'entity_code': 'CAMPUS', 'account_code': 'TUITION', 'source_balance': 1, 'owner': 'Controller', 'notes': 'Material variance'},
    )
    assert reconciliation.status_code == 200
    assert reconciliation.json()['status'] == 'exception'

    exceptions = client.get(f'/api/close/reconciliation-exceptions?scenario_id={sid}', headers=headers)
    assert exceptions.status_code == 200
    assert exceptions.json()['exceptions']

    submitted = client.post(f"/api/close/reconciliations/{reconciliation.json()['id']}/submit", headers=headers, json={'note': 'Prepared.'})
    assert submitted.status_code == 200
    assert submitted.json()['status'] == 'pending_review'
    reviewed = client.post(f"/api/close/reconciliations/{reconciliation.json()['id']}/approve", headers=headers, json={'note': 'Reviewed.'})
    assert reviewed.status_code == 200
    assert reviewed.json()['status'] == 'reviewed'
    assert reviewed.json()['reviewer'] == 'admin@mufinances.local'

    confirmation = client.post(
        '/api/close/entity-confirmations',
        headers=headers,
        json={'scenario_id': sid, 'period': period, 'entity_code': 'CAMPUS', 'confirmation_type': 'balance'},
    )
    assert confirmation.status_code == 200
    assert confirmation.json()['status'] == 'requested'
    confirmed = client.post(f"/api/close/entity-confirmations/{confirmation.json()['id']}/confirm", headers=headers, json={'response': {'confirmed': True}})
    assert confirmed.status_code == 200
    assert confirmed.json()['status'] == 'confirmed'

    locked = client.post(f'/api/close/calendar/{period}/lock?scenario_id={sid}', headers=headers, json={'lock_state': 'locked'})
    assert locked.status_code == 200
    assert locked.json()['lock_state'] == 'locked'

    blocked_reconciliation = client.post(
        '/api/close/reconciliations',
        headers=headers,
        json={'scenario_id': sid, 'period': period, 'entity_code': 'CAMPUS', 'account_code': 'SUPPLIES', 'source_balance': 0, 'owner': 'Controller', 'notes': 'Should block'},
    )
    assert blocked_reconciliation.status_code == 409

    unlocked = client.post(f'/api/close/calendar/{period}/lock?scenario_id={sid}', headers=headers, json={'lock_state': 'open'})
    assert unlocked.status_code == 200
    assert unlocked.json()['lock_state'] == 'open'


def test_close_status_reports_b19_complete() -> None:
    response = client.get('/api/close/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] in {'B19', 'B20'}
    assert payload['complete'] is True
    assert payload['checks']['period_lock_enforcement_ready'] is True
    assert payload['checks']['entity_confirmations_ready'] is True
