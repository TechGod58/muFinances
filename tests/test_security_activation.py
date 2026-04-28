from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_security_activation.db'
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


def test_security_activation_proves_sso_ad_domain_sessions_and_certifications() -> None:
    headers = admin_headers()

    response = client.post('/api/security/activation/run', headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload['complete'] is True
    assert payload['checks']['sso_ready'] is True
    assert payload['checks']['ad_ou_mapping_ready'] is True
    assert payload['checks']['manchester_domain_enforcement_ready'] is True
    assert payload['checks']['vpn_enforcement_ready'] is True
    assert payload['checks']['session_controls_ready'] is True
    assert payload['checks']['access_review_certified'] is True
    assert payload['checks']['sod_certified'] is True
    assert payload['sso']['metadata_url'].startswith('https://login.microsoftonline.com/manchester.edu/')
    assert payload['ad_ou_mapping']['allowed_ou_dn'] == 'OU=muFinances Users,OU=Finance,DC=manchester,DC=edu'
    assert payload['domain_check']['allowed'] is True
    assert payload['vpn_check']['allowed'] is True

    workspace = client.get('/api/security/enterprise-workspace', headers=headers)
    assert workspace.status_code == 200
    assert workspace.json()['access_reviews'][0]['status'] == 'certified'
