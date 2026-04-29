from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_b133_b135_database_ci_sso.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app import db
from app.main import app
from app.services import security

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def _b64url(payload: bytes) -> str:
    return base64.urlsafe_b64encode(payload).decode('ascii').rstrip('=')


def _hs256_token(claims: dict[str, object]) -> str:
    header = {'typ': 'JWT', 'alg': 'HS256'}
    signing_input = f"{_b64url(json.dumps(header, separators=(',', ':')).encode())}.{_b64url(json.dumps(claims, separators=(',', ':')).encode())}"
    signature = hmac.new(b'mufinances-test-secret', signing_input.encode('ascii'), hashlib.sha256).digest()
    return f'{signing_input}.{_b64url(signature)}'


def test_b133_database_runtime_statuses_split_not_configured_from_failed() -> None:
    headers = admin_headers()

    for path in [
        '/api/database-runtime/status',
        '/api/postgres-runtime/status',
        '/api/production-data-platform/status',
        '/api/performance/status',
    ]:
        response = client.get(path, headers=headers)
        assert response.status_code == 200
        payload = response.json()
        assert payload['complete'] is True
        assert payload['database']['active_backend_status'] == 'ready'
        assert payload['database']['postgres_status'] in {'ready', 'not_configured', 'not_available'}
        assert payload['database']['mssql_status'] in {'ready', 'not_configured', 'not_available'}

    operations = client.post('/api/operations-readiness/run', headers=headers, json={'run_key': 'b133-ops-readiness'})
    assert operations.status_code == 200
    status = client.get('/api/operations-readiness/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['complete'] is True
    assert status.json()['checks']['database_runtime_classified'] is True
    assert status.json()['checks']['active_database_backend_ready'] is True


def test_b134_database_ci_matrix_defines_sqlite_postgres_and_mssql_ready_jobs() -> None:
    workflow = (PROJECT_ROOT / '.github' / 'workflows' / 'database-ci.yml').read_text(encoding='utf-8')
    assert 'sqlite:' in workflow
    assert 'postgres:' in workflow
    assert 'mssql-ready:' in workflow
    assert 'postgres:16' in workflow
    assert 'CAMPUS_FPM_DB_BACKEND: postgres' in workflow
    assert 'CAMPUS_FPM_MSSQL_DSN' in workflow
    assert 'tests/test_migration_proof.py' in workflow
    assert 'tests/test_production_data_platform_cutover.py' in workflow


def test_b135_real_sso_callback_maps_claims_roles_and_blocks_replay(monkeypatch) -> None:
    monkeypatch.setattr(security, 'SSO_ISSUER_URL', 'https://login.manchester.edu')
    monkeypatch.setattr(security, 'SSO_AUTHORIZE_URL', 'https://login.manchester.edu/oauth2/v2.0/authorize')
    monkeypatch.setattr(security, 'SSO_TOKEN_URL', 'https://login.manchester.edu/oauth2/v2.0/token')
    monkeypatch.setattr(security, 'SSO_JWKS_URL', 'https://login.manchester.edu/discovery/keys')
    monkeypatch.setattr(security, 'SSO_CLIENT_ID', 'mufinances-test-client')
    monkeypatch.setattr(security, 'SSO_CLIENT_SECRET', 'mufinances-test-secret')
    now = time.strftime('%Y-%m-%dT%H:%M:%S+00:00', time.gmtime())
    db.execute(
        '''
        INSERT INTO sso_providers (
            provider_key, name, protocol, issuer_url, authorize_url, token_url,
            jwks_url, client_id, enabled, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        ON CONFLICT(provider_key) DO UPDATE SET
            issuer_url = excluded.issuer_url,
            authorize_url = excluded.authorize_url,
            token_url = excluded.token_url,
            jwks_url = excluded.jwks_url,
            client_id = excluded.client_id,
            enabled = 1
        ''',
        (
            'campus-sso',
            'Campus SSO',
            'oidc',
            'https://login.manchester.edu',
            'https://login.manchester.edu/oauth2/v2.0/authorize',
            'https://login.manchester.edu/oauth2/v2.0/token',
            'https://login.manchester.edu/discovery/keys',
            'mufinances-test-client',
            now,
        ),
    )
    headers = admin_headers()
    setting = client.post(
        '/api/security/sso-production-settings',
        headers=headers,
        json={
            'provider_key': 'campus-sso',
            'environment': 'production',
            'metadata_url': 'https://login.manchester.edu/.well-known/openid-configuration',
            'required_claim': 'email',
            'group_claim': 'groups',
            'jit_provisioning': True,
            'status': 'enabled',
        },
    )
    assert setting.status_code == 200
    mapping = client.post(
        '/api/security/ad-ou-group-mappings',
        headers=headers,
        json={
            'mapping_key': 'b135-finance-sso',
            'ad_group_dn': 'CN=Finance Users,OU=Finance,DC=manchester,DC=edu',
            'allowed_ou_dn': 'OU=Finance,DC=manchester,DC=edu',
            'role_key': 'budget.office',
            'dimension_kind': 'department',
            'dimension_code': 'SCI',
            'active': True,
        },
    )
    assert mapping.status_code == 200

    login = client.get('/api/auth/sso/login')
    assert login.status_code == 200
    login_payload = login.json()
    assert login_payload['enabled'] is True
    query = parse_qs(urlparse(login_payload['authorization_url']).query)
    nonce = query['nonce'][0]

    claims = {
        'iss': 'https://login.manchester.edu',
        'aud': 'mufinances-test-client',
        'sub': 'A12345',
        'email': 'finance.user@manchester.edu',
        'name': 'Finance User',
        'nonce': nonce,
        'exp': int(time.time()) + 600,
        'groups': ['CN=Finance Users,OU=Finance,DC=manchester,DC=edu'],
        'dn': 'CN=Finance User,OU=Finance,DC=manchester,DC=edu',
    }
    callback = client.get(
        '/api/auth/sso/callback',
        params={'state': login_payload['state'], 'id_token': _hs256_token(claims)},
    )
    assert callback.status_code == 200
    payload = callback.json()
    assert payload['auth_method'] == 'sso_oidc'
    assert payload['user']['email'] == 'finance.user@manchester.edu'
    assert 'budget.office' in payload['user']['roles']
    assert {'dimension_kind': 'department', 'code': 'SCI'} in payload['user']['dimension_access']

    replay = client.get(
        '/api/auth/sso/callback',
        params={'state': login_payload['state'], 'id_token': _hs256_token(claims)},
    )
    assert replay.status_code == 400
    assert 'already been used' in replay.json()['detail']

    me = client.get('/api/auth/me', headers={'Authorization': f"Bearer {payload['token']}"})
    assert me.status_code == 200
    assert me.json()['email'] == 'finance.user@manchester.edu'

    logout = client.post('/api/auth/logout', headers={'Authorization': f"Bearer {payload['token']}"})
    assert logout.status_code == 200
    assert logout.json()['logged_out'] is True
    assert client.get('/api/auth/me', headers={'Authorization': f"Bearer {payload['token']}"}).status_code == 401

    db.execute("UPDATE sso_providers SET enabled = 0, authorize_url = '', token_url = '', jwks_url = '', client_id = '' WHERE provider_key = 'campus-sso'")
    db.execute("DELETE FROM ad_ou_group_mappings WHERE mapping_key = 'b135-finance-sso'")
    db.execute("DELETE FROM sso_production_settings WHERE provider_key = 'campus-sso'")
