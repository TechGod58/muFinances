from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_manchester_identity_activation_proof.db'
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


def test_b156_manchester_identity_activation_proof_records_signed_activation_evidence() -> None:
    headers = admin_headers()

    status = client.get('/api/security/manchester-identity-activation-proof/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B156'
    assert payload['complete'] is True
    assert payload['checks']['production_sso_activation_ready'] is True
    assert payload['checks']['domain_vpn_enforcement_active'] is True

    run = client.post(
        '/api/security/manchester-identity-activation-proof/run',
        headers=headers,
        json={
            'run_key': 'b156-regression',
            'signed_by': 'it.security@manchester.edu',
            'activation_scope': 'Manchester production identity activation',
        },
    )
    assert run.status_code == 200, run.text
    proof = run.json()
    assert proof['status'] == 'passed'
    assert proof['complete'] is True
    assert proof['checks']['production_sso_activation_ready'] is True
    assert proof['checks']['mfa_claim_handoff_ready'] is True
    assert proof['checks']['ad_ou_group_mapping_ready'] is True
    assert proof['checks']['access_review_certification_ready'] is True
    assert proof['checks']['audit_evidence_ready'] is True
    assert proof['live_proof']['evidence']['sso']['metadata_url'].startswith('https://login.microsoftonline.com/manchester.edu/')
    assert proof['signoff']['signed_by'] == 'it.security@manchester.edu'
    assert proof['signoff']['required_ou'] == 'OU=muFinances Users,OU=Finance,DC=manchester,DC=edu'
    assert proof['signoff']['external_network_blocked'] is True
    assert proof['signoff']['all_checks_passed'] is True

    runs = client.get('/api/security/manchester-identity-activation-proof/runs', headers=headers)
    assert runs.status_code == 200
    assert runs.json()['count'] >= 1
