from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_secure_audit_log_certification.db'
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


def test_b159_secure_audit_log_certification_proves_chain_backup_packet_and_restricted_access() -> None:
    headers = admin_headers()

    status = client.get('/api/secure-audit-log-certification/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B159'
    assert payload['complete'] is True
    assert payload['checks']['secure_log_table_not_user_exposed'] is True

    run = client.post(
        '/api/secure-audit-log-certification/run',
        headers=headers,
        json={'run_key': 'b159-regression', 'signed_by': 'audit.owner@manchester.edu', 'packet_limit': 50},
    )
    assert run.status_code == 200, run.text
    proof = run.json()
    assert proof['batch'] == 'B159'
    assert proof['status'] == 'passed'
    assert proof['complete'] is True
    assert proof['checks']['audit_compliance_certified'] is True
    assert proof['checks']['secure_financial_audit_chain_valid'] is True
    assert proof['checks']['tamper_report_clean'] is True
    assert proof['checks']['backup_contains_secure_audit_log'] is True
    assert proof['checks']['auditor_packet_exported'] is True
    assert proof['checks']['secure_log_table_not_exposed_in_packet'] is True
    assert proof['auditor_packet']['packet']['record_count'] >= 1
    assert len(proof['auditor_packet']['packet_checksum']) == 64
    assert proof['signoff']['signed_by'] == 'audit.owner@manchester.edu'
    assert proof['signoff']['all_checks_passed'] is True

    rows = client.get('/api/secure-audit-log-certification/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
