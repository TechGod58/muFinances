from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_security_activation_certification.db'
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


def test_security_activation_certification_proves_manchester_controls() -> None:
    headers = admin_headers()

    status = client.get('/api/security/activation-certification/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B100'
    assert status.json()['complete'] is True

    run = client.post('/api/security/activation-certification/run', headers=headers, json={})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['manchester_sso_ready'] is True
    assert payload['checks']['mfa_handoff_ready'] is True
    assert payload['checks']['ad_ou_group_mapping_ready'] is True
    assert payload['checks']['domain_vpn_enforcement_ready'] is True
    assert payload['checks']['row_level_access_ready'] is True
    assert payload['checks']['masking_ready'] is True
    assert payload['checks']['session_hardening_ready'] is True
    assert payload['checks']['sod_policy_enforcement_ready'] is True
    assert payload['activation']['complete'] is True
    assert payload['evidence']['network_probe']['external_host_blocked'] is True
    assert payload['evidence']['masking_probe']['restricted_user']['ssn'] == 'masked'
    assert 'SCI' in payload['evidence']['row_level_access']['department_codes'] or payload['evidence']['row_level_access']['department_codes'] == ['*']

    rows = client.get('/api/security/activation-certification/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
