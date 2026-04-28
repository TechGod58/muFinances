from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_secure_financial_audit.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app import db
from app.main import app

client = TestClient(app)


def login(email: str = 'admin@mufinances.local', password: str = 'ChangeMe!3200') -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': email, 'password': password})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def scenario_id(headers: dict[str, str]) -> int:
    response = client.get('/api/scenarios', headers=headers)
    assert response.status_code == 200
    return int(response.json()[0]['id'])


def test_financial_ledger_post_writes_secure_hash_chained_audit_log() -> None:
    headers = login()
    before = db.fetch_one('SELECT COUNT(*) AS count FROM secure_financial_audit_logs')
    sid = scenario_id(headers)

    response = client.post(
        '/api/foundation/ledger',
        headers=headers,
        json={
            'scenario_id': sid,
            'department_code': 'SCI',
            'fund_code': 'GEN',
            'account_code': 'SUPPLIES',
            'period': '2026-08',
            'amount': 1234.56,
            'source': 'manual',
            'ledger_type': 'actual',
            'ledger_basis': 'actual',
            'notes': 'Secure financial audit proof.',
        },
    )
    assert response.status_code == 200
    ledger_id = str(response.json()['id'])

    after = db.fetch_one('SELECT COUNT(*) AS count FROM secure_financial_audit_logs')
    assert int(after['count']) == int(before['count']) + 1
    secure_row = db.fetch_one('SELECT * FROM secure_financial_audit_logs ORDER BY id DESC LIMIT 1')
    assert secure_row['entity_type'] == 'planning_ledger'
    assert secure_row['entity_id'] == ledger_id
    assert secure_row['action'] == 'posted'
    assert secure_row['detail_checksum']
    assert secure_row['row_hash']

    verification = db.verify_secure_financial_audit_chain()
    assert verification['valid'] is True
    assert verification['checked'] >= 1


def test_secure_financial_audit_has_no_user_list_endpoint_and_general_audit_is_ops_only() -> None:
    admin_headers = login()
    created = client.post(
        '/api/security/users',
        headers=admin_headers,
        json={
            'email': 'planner.audit@mufinances.local',
            'display_name': 'Planner Audit User',
            'password': 'PlannerPass!3200',
            'role_keys': ['department.planner'],
        },
    )
    assert created.status_code == 200
    planner_headers = login('planner.audit@mufinances.local', 'PlannerPass!3200')

    general_audit = client.get('/api/audit-logs', headers=planner_headers)
    assert general_audit.status_code == 403

    secure_audit = client.get('/api/secure-financial-audit-logs', headers=admin_headers)
    assert secure_audit.status_code == 404
