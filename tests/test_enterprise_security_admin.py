from __future__ import annotations

import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_enterprise_security_admin.db'
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
    scenarios = client.get('/api/scenarios', headers=headers).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def test_enterprise_security_admin_workflow() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    me = client.get('/api/auth/me', headers=headers).json()

    user = client.post(
        '/api/security/users',
        headers=headers,
        json={'email': f'b46-{int(time.time() * 1000)}@mufinances.local', 'display_name': 'B46 Reviewer', 'password': 'Review!3200', 'role_keys': ['auditor']},
    )
    assert user.status_code == 200

    sso = client.post(
        '/api/security/sso-production-settings',
        headers=headers,
        json={'provider_key': 'campus-sso', 'environment': 'production', 'metadata_url': 'https://login.microsoftonline.com/manchester.edu/.well-known/openid-configuration', 'required_claim': 'email', 'group_claim': 'groups', 'jit_provisioning': True, 'status': 'ready'},
    )
    assert sso.status_code == 200
    assert sso.json()['status'] == 'ready'

    mapping = client.post(
        '/api/security/ad-ou-group-mappings',
        headers=headers,
        json={'mapping_key': 'finance-budget-office', 'ad_group_dn': 'CN=Finance Budget,OU=Groups,DC=manchester,DC=edu', 'allowed_ou_dn': 'OU=muFinances Users,OU=Finance,DC=manchester,DC=edu', 'role_key': 'budget.office', 'dimension_kind': 'department', 'dimension_code': 'SCI'},
    )
    assert mapping.status_code == 200
    assert mapping.json()['active'] is True

    guard = client.post(
        '/api/security/domain-vpn-checks',
        headers=headers,
        json={'host': 'mufinances.manchester.edu', 'client_host': '10.30.44.12', 'forwarded_host': 'mufinances.manchester.edu', 'forwarded_for': '10.30.44.12'},
    )
    assert guard.status_code == 200
    assert guard.json()['allowed'] is True

    impersonation = client.post('/api/security/impersonations', headers=headers, json={'target_user_id': user.json()['id'], 'reason': 'B46 support validation'})
    assert impersonation.status_code == 200
    assert impersonation.json()['status'] == 'issued'
    assert impersonation.json()['impersonation_token']

    ended = client.post(f"/api/security/impersonations/{impersonation.json()['id']}/end", headers=headers)
    assert ended.status_code == 200
    assert ended.json()['status'] == 'ended'

    sod = client.post(
        '/api/security/sod-policies',
        headers=headers,
        json={'rule_key': 'b46-admin-auditor', 'name': 'Admin and auditor conflict', 'conflict_type': 'role_pair', 'left_value': 'finance.admin', 'right_value': 'auditor', 'severity': 'medium', 'active': True},
    )
    assert sod.status_code == 200
    assert sod.json()['rule_key'] == 'b46-admin-auditor'

    review = client.post(
        '/api/security/access-reviews',
        headers=headers,
        json={'review_key': 'b46-access-review', 'reviewer_user_id': me['id'], 'scenario_id': sid, 'scope': {'roles': True, 'dimensions': True}},
    )
    assert review.status_code == 200
    assert review.json()['status'] == 'open'
    assert review.json()['findings']

    certified = client.post(f"/api/security/access-reviews/{review.json()['id']}/certify", headers=headers, json={'findings': review.json()['findings']})
    assert certified.status_code == 200
    assert certified.json()['status'] == 'certified'

    workspace = client.get('/api/security/enterprise-workspace', headers=headers)
    assert workspace.status_code == 200
    payload = workspace.json()
    assert payload['status']['batch'] == 'B46'
    assert payload['sso_production_settings']
    assert payload['ad_ou_group_mappings']
    assert payload['domain_vpn_checks']
    assert payload['impersonation_sessions']
    assert payload['access_reviews']


def test_enterprise_security_status_reports_b46_complete() -> None:
    response = client.get('/api/security/enterprise-status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B46'
    assert payload['complete'] is True
    assert payload['checks']['admin_impersonation_controls_ready'] is True
