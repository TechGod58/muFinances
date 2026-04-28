from __future__ import annotations

import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_security.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post(
        '/api/auth/login',
        json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'},
    )
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def login_headers(email: str, password: str) -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': email, 'password': password})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def change_password(headers: dict[str, str], current_password: str, new_password: str) -> dict[str, str]:
    response = client.post(
        '/api/auth/password',
        headers=headers,
        json={'current_password': current_password, 'new_password': new_password},
    )
    assert response.status_code == 200
    return headers


def seeded_scenario_id() -> int:
    scenarios = client.get('/api/scenarios', headers=admin_headers()).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def test_api_data_requires_authentication() -> None:
    response = client.get('/api/scenarios')
    assert response.status_code == 401
    assert response.json()['detail'] == 'Authentication required.'


def test_login_and_me_return_profile() -> None:
    headers = admin_headers()
    response = client.get('/api/auth/me', headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload['email'] == 'admin@mufinances.local'
    assert 'finance.admin' in payload['roles']
    assert 'security.manage' in payload['permissions']


def test_row_level_access_and_sensitive_masking() -> None:
    headers = admin_headers()
    email = f'planner-{int(time.time() * 1000)}@mufinances.local'
    password = 'Planner!3200'
    user = client.post(
        '/api/security/users',
        headers=headers,
        json={
            'email': email,
            'display_name': 'Science Planner',
            'password': password,
            'role_keys': ['department.planner'],
        },
    )
    assert user.status_code == 200
    user_id = user.json()['id']

    grant = client.post(
        f'/api/security/users/{user_id}/dimension-access',
        headers=headers,
        json={'dimension_kind': 'department', 'code': 'SCI'},
    )
    assert grant.status_code == 200

    scenario_id = seeded_scenario_id()
    created = client.post(
        '/api/foundation/ledger',
        headers=headers,
        json={
            'scenario_id': scenario_id,
            'department_code': 'SCI',
            'fund_code': 'GEN',
            'account_code': 'SALARY',
            'period': '2026-08',
            'amount': -5000,
            'notes': 'Sensitive compensation test',
            'metadata': {'salary': '5000', 'batch': 'B02'},
        },
    )
    assert created.status_code == 200
    assert created.json()['metadata']['salary'] == '5000'

    planner_headers = login_headers(email, password)
    blocked = client.get(f'/api/foundation/ledger?scenario_id={scenario_id}', headers=planner_headers)
    assert blocked.status_code == 403
    assert blocked.json()['code'] == 'password_change_required'
    planner_headers = change_password(planner_headers, password, 'Planner!3200-Changed')
    ledger = client.get(f'/api/foundation/ledger?scenario_id={scenario_id}', headers=planner_headers)
    assert ledger.status_code == 200
    entries = ledger.json()['entries']
    assert entries
    assert {entry['department_code'] for entry in entries} == {'SCI'}
    sensitive_entry = next(item for item in entries if item['notes'] == 'Sensitive compensation test')
    assert sensitive_entry['metadata']['salary'] == 'masked'


def test_security_status_reports_b02_complete() -> None:
    response = client.get('/api/security/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B02'
    assert payload['complete'] is True
    assert payload['checks']['api_auth_gate_ready'] is True
    assert payload['checks']['sso_ready'] is True
    assert payload['checks']['first_login_password_change_ready'] is True


def test_sso_config_and_login_endpoint_are_ready_for_server_config() -> None:
    config = client.get('/api/auth/sso/config')
    assert config.status_code == 200
    payload = config.json()
    assert payload['provider_key'] == 'campus-sso'
    assert payload['login_endpoint'] == '/api/auth/sso/login'
    assert payload['callback_endpoint'] == '/api/auth/sso/callback'

    login = client.get('/api/auth/sso/login')
    assert login.status_code == 200
    assert login.json()['enabled'] is False
    assert login.json()['reason'] == 'SSO provider is not configured.'
