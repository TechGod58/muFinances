from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_close_consolidation.db'
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


def test_close_reconciliation_consolidation_and_audit_packet() -> None:
    headers = admin_headers()
    sid = scenario_id()

    checklist = client.post(
        '/api/close/checklists',
        headers=headers,
        json={
            'scenario_id': sid,
            'period': '2026-08',
            'checklist_key': 'ledger-review',
            'title': 'Review planning ledger',
            'owner': 'Controller',
            'due_date': '2026-09-05',
        },
    )
    assert checklist.status_code == 200
    complete = client.post(
        f"/api/close/checklists/{checklist.json()['id']}/complete",
        headers=headers,
        json={'evidence': {'file': 'ledger-review.pdf'}},
    )
    assert complete.status_code == 200
    assert complete.json()['status'] == 'complete'

    reconciliation = client.post(
        '/api/close/reconciliations',
        headers=headers,
        json={
            'scenario_id': sid,
            'period': '2026-08',
            'entity_code': 'CAMPUS',
            'account_code': 'TUITION',
            'source_balance': 1125000,
            'owner': 'Controller',
            'notes': 'Ties to student receivable detail',
        },
    )
    assert reconciliation.status_code == 200
    assert reconciliation.json()['status'] in {'reconciled', 'variance', 'prepared', 'exception'}

    match = client.post(
        '/api/close/intercompany-matches',
        headers=headers,
        json={
            'scenario_id': sid,
            'period': '2026-08',
            'source_entity_code': 'CAMPUS',
            'target_entity_code': 'FOUNDATION',
            'account_code': 'TRANSFER',
            'source_amount': 25000,
            'target_amount': -25000,
        },
    )
    assert match.status_code == 200
    assert match.json()['status'] == 'matched'

    elimination = client.post(
        '/api/close/eliminations',
        headers=headers,
        json={
            'scenario_id': sid,
            'period': '2026-08',
            'entity_code': 'CAMPUS',
            'account_code': 'TRANSFER',
            'amount': -25000,
            'reason': 'Eliminate internal campus transfer',
        },
    )
    assert elimination.status_code == 200
    assert elimination.json()['ledger_entry_id'] is not None

    run = client.post(
        '/api/close/consolidation-runs',
        headers=headers,
        json={'scenario_id': sid, 'period': '2026-08'},
    )
    assert run.status_code == 200
    assert run.json()['status'] == 'complete'
    assert run.json()['audit_packet']['status'] == 'sealed'
    assert run.json()['audit_packet']['contents']['totals']['eliminations'] == -25000


def test_close_status_reports_b08_complete() -> None:
    response = client.get('/api/close/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] in {'B08', 'B19', 'B20'}
    assert payload['complete'] is True
    assert payload['checks']['audit_packets_ready'] is True
