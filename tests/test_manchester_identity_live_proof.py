from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_manchester_identity_live_proof.db'
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


def test_manchester_identity_live_proof_records_sso_mfa_ad_domain_and_mapping_signoff() -> None:
    headers = admin_headers()
    status = client.get('/api/security/manchester-identity-live-proof/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B118'
    assert status.json()['complete'] is True

    run = client.post(
        '/api/security/manchester-identity-live-proof/run',
        headers=headers,
        json={'signed_by': 'it.security@manchester.edu', 'notes': 'B118 live identity proof signoff.'},
    )
    assert run.status_code == 200, run.text
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['production_sso_ready'] is True
    assert payload['checks']['mfa_handoff_ready'] is True
    assert payload['checks']['ad_ou_validation_ready'] is True
    assert payload['checks']['domain_vpn_enforcement_ready'] is True
    assert payload['checks']['access_review_evidence_ready'] is True
    assert payload['checks']['role_group_mapping_signoff_ready'] is True
    assert payload['evidence']['sso']['metadata_url'].startswith('https://login.microsoftonline.com/manchester.edu/')
    assert payload['evidence']['ad_ou']['required_ou'] == 'OU=muFinances Users,OU=Finance,DC=manchester,DC=edu'
    assert payload['evidence']['network']['external_blocked'] is True
    assert payload['signoff']['signed_by'] == 'it.security@manchester.edu'
    assert payload['signoff']['role_group_mapping_signed_off'] is True

    runs = client.get('/api/security/manchester-identity-live-proof/runs', headers=headers)
    assert runs.status_code == 200
    assert runs.json()['count'] >= 1
