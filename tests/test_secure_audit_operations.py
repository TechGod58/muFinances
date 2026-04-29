from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_secure_audit_operations.db'
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
    response = client.get('/api/scenarios', headers=headers)
    assert response.status_code == 200
    return int(response.json()[0]['id'])


def seed_secure_financial_event(headers: dict[str, str]) -> None:
    sid = scenario_id(headers)
    response = client.post(
        '/api/foundation/ledger',
        headers=headers,
        json={
            'scenario_id': sid,
            'department_code': 'SCI',
            'fund_code': 'GEN',
            'account_code': 'B116_AUDIT',
            'period': '2026-11',
            'amount': 1160,
            'source': 'b116_test',
            'ledger_type': 'actual',
            'ledger_basis': 'actual',
            'notes': 'B116 secure audit operations seed.',
        },
    )
    assert response.status_code == 200, response.text


def test_secure_audit_operations_dashboard_backup_packet_and_policy() -> None:
    headers = admin_headers()
    seed_secure_financial_event(headers)

    status = client.get('/api/secure-audit-operations/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B116'
    assert status.json()['complete'] is True
    assert status.json()['checks']['tamper_check_reporting_ready'] is True

    dashboard = client.get('/api/secure-audit-operations/dashboard', headers=headers)
    assert dashboard.status_code == 200
    assert dashboard.json()['chain']['valid'] is True
    assert dashboard.json()['retention']['covered'] is True
    assert dashboard.json()['policy']['policy_key'] == 'secure-financial-audit-operations'

    backup = client.post('/api/secure-audit-operations/backup-verification', headers=headers)
    assert backup.status_code == 200
    assert backup.json()['status'] == 'pass'
    assert backup.json()['result']['secure_financial_audit_present'] is True

    packet = client.post('/api/secure-audit-operations/auditor-packets?limit=25', headers=headers)
    assert packet.status_code == 200
    payload = packet.json()
    assert payload['export_type'] == 'auditor_packet'
    assert payload['packet_checksum']
    assert payload['packet']['chain']['valid'] is True
    assert payload['packet']['record_count'] >= 1

    tamper = client.get('/api/secure-audit-operations/tamper-report', headers=headers)
    assert tamper.status_code == 200
    assert tamper.json()['status'] == 'pass'
    assert tamper.json()['finding_count'] == 0
