from __future__ import annotations

import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_auditor_access_model.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app import db
from app.main import app

client = TestClient(app)


def login(email: str, password: str) -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': email, 'password': password})
    assert response.status_code == 200, response.text
    return {'Authorization': f"Bearer {response.json()['token']}"}


def admin_headers() -> dict[str, str]:
    return login('admin@mufinances.local', 'ChangeMe!3200')


def create_auditor(headers: dict[str, str]) -> dict[str, str]:
    stamp = int(time.time() * 1000)
    email = f'b126-auditor-{stamp}@mufinances.local'
    initial_password = 'Audit!3200'
    changed_password = 'Audit!3200-Changed'
    response = client.post(
        '/api/security/users',
        headers=headers,
        json={'email': email, 'display_name': 'B126 Auditor', 'password': initial_password, 'role_keys': ['auditor']},
    )
    assert response.status_code == 200, response.text
    auditor_headers = login(email, initial_password)
    changed = client.post('/api/auth/password', headers=auditor_headers, json={'current_password': initial_password, 'new_password': changed_password})
    assert changed.status_code == 200, changed.text
    return login(email, changed_password)


def seed_evidence(headers: dict[str, str]) -> None:
    ledger = client.post(
        '/api/foundation/ledger',
        headers=headers,
        json={
            'scenario_id': 1,
            'department_code': 'SCI',
            'fund_code': 'GEN',
            'account_code': 'B126_AUDIT',
            'period': '2026-12',
            'amount': 1260,
            'source': 'b126_test',
            'ledger_type': 'actual',
            'ledger_basis': 'actual',
            'notes': 'B126 secure audit seed.',
        },
    )
    assert ledger.status_code == 200, ledger.text
    close = client.post('/api/close/certification/run', headers=headers, json={'run_key': 'b126-close'})
    assert close.status_code == 200, close.text
    db.execute("UPDATE close_task_templates SET active = 0 WHERE template_key LIKE 'b126-close-%'")
    consolidation = client.post('/api/close/consolidation-certification/run', headers=headers, json={'run_key': 'b126-consolidation'})
    assert consolidation.status_code == 200, consolidation.text


def test_controlled_auditor_access_exposes_safe_records_not_secure_log_table() -> None:
    admin = admin_headers()
    seed_evidence(admin)
    auditor = create_auditor(admin)

    status = client.get('/api/auditor-access/status', headers=auditor)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B126'
    assert status.json()['checks']['secure_internal_audit_log_table_not_exposed'] is True

    workspace = client.get('/api/auditor-access/workspace', headers=auditor)
    assert workspace.status_code == 200, workspace.text
    payload = workspace.json()
    assert payload['batch'] == 'B126'
    assert payload['audit_packet']['records_exposed'] is False
    assert payload['audit_packet']['record_count'] >= 1
    assert payload['secure_log_policy']['direct_table_access_allowed'] is False
    assert payload['counts']['close_evidence'] >= 1
    assert payload['counts']['consolidation_reports'] >= 1
    assert payload['counts']['exportable_audit_records'] >= 1

    export = client.post('/api/auditor-access/export', headers=auditor)
    assert export.status_code == 200, export.text
    exported = export.json()
    assert exported['secure_log_table_exposed'] is False
    assert exported['checksum']
    assert 'secure_financial_audit_logs' not in str(exported['payload']['records'])

    direct = client.get('/api/secure-financial-audit-logs', headers=auditor)
    assert direct.status_code == 404

    records = client.get('/api/auditor-access/records', headers=admin)
    assert records.status_code == 200
    assert records.json()['count'] >= 2
