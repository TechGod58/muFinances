from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_brokerage_connectors.db'
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


def test_brokerage_status_reports_read_only_framework_complete() -> None:
    response = client.get('/api/brokerage/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B36'
    assert payload['complete'] is True
    assert payload['checks']['real_money_trading_blocked'] is True
    assert payload['trading_enabled'] is False


def test_provider_catalog_keeps_trading_disabled() -> None:
    response = client.get('/api/brokerage/providers', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['trading_enabled'] is False
    assert any(item['provider_key'] == 'generic_sandbox' for item in payload['providers'])
    assert all(item['supports_trading'] is False for item in payload['providers'])


def test_sandbox_connection_test_and_holdings_sync() -> None:
    headers = admin_headers()
    create = client.post(
        '/api/brokerage/connections',
        headers=headers,
        json={'provider_key': 'generic_sandbox', 'connection_name': 'Sandbox brokerage', 'mode': 'sandbox'},
    )
    assert create.status_code == 200
    connection = create.json()
    assert connection['status'] == 'sandbox_ready'
    assert connection['trading_enabled'] is False

    test = client.post(f"/api/brokerage/connections/{connection['id']}/test", headers=headers)
    assert test.status_code == 200
    assert test.json()['connection']['status'] == 'ok'

    sync = client.post(f"/api/brokerage/connections/{connection['id']}/sync", headers=headers)
    assert sync.status_code == 200
    payload = sync.json()
    assert payload['accounts'][0]['account_name'].startswith('Sandbox brokerage')
    assert len(payload['holdings']) >= 1
    assert payload['holdings'][0]['market_value'] > 0
    assert payload['sync_runs'][0]['status'] == 'synced'


def test_live_provider_requires_credential_reference_before_sync() -> None:
    headers = admin_headers()
    create = client.post(
        '/api/brokerage/connections',
        headers=headers,
        json={'provider_key': 'alpaca', 'connection_name': 'Alpaca read only', 'mode': 'read_only'},
    )
    assert create.status_code == 200
    connection = create.json()
    assert connection['status'] == 'needs_credentials'

    test = client.post(f"/api/brokerage/connections/{connection['id']}/test", headers=headers)
    assert test.status_code == 200
    assert test.json()['connection']['status'] == 'needs_credentials'

    sync = client.post(f"/api/brokerage/connections/{connection['id']}/sync", headers=headers)
    assert sync.status_code == 409


def test_b66_brokerage_provider_readiness_consent_credentials_and_audit() -> None:
    headers = admin_headers()
    status = client.get('/api/brokerage/provider-readiness/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B66'
    assert status.json()['checks']['provider_selection_ready'] is True

    providers = client.get('/api/brokerage/providers', headers=headers)
    assert providers.status_code == 200
    schwab = next(item for item in providers.json()['providers'] if item['provider_key'] == 'schwab')
    assert schwab['auth_type'] == 'oauth'
    assert schwab['live_ready'] is True
    assert 'real_money_trading_disabled' in schwab['required_acknowledgements']

    create = client.post(
        '/api/brokerage/connections',
        headers=headers,
        json={
            'provider_key': 'schwab',
            'connection_name': 'Schwab read-only',
            'mode': 'live',
            'provider_environment': 'live',
            'read_only_ack': False,
        },
    )
    assert create.status_code == 200
    connection = create.json()
    assert connection['status'] == 'needs_consent'
    assert connection['provider_environment'] == 'live'
    assert connection['warnings'][0].startswith('Read-only live brokerage sync')

    consent = client.post(
        f"/api/brokerage/connections/{connection['id']}/consent",
        headers=headers,
        json={'read_only_ack': True, 'real_money_trading_ack': True, 'data_scope_ack': True},
    )
    assert consent.status_code == 200
    assert consent.json()['consent']['status'] == 'accepted'

    credentials = client.post(
        f"/api/brokerage/connections/{connection['id']}/credential-setup",
        headers=headers,
        json={'credential_type': 'oauth_client', 'redirect_uri': '/oauth/brokerage/callback'},
    )
    assert credentials.status_code == 200
    assert credentials.json()['auth_url'].startswith('/oauth/brokerage/schwab')
    assert credentials.json()['connection']['auth_flow_status'] == 'oauth_pending'

    credentials_ready = client.post(
        f"/api/brokerage/connections/{connection['id']}/credential-setup",
        headers=headers,
        json={'credential_type': 'oauth_client', 'credential_ref': 'vault://schwab/read-only-demo'},
    )
    assert credentials_ready.status_code == 200
    assert credentials_ready.json()['connection']['status'] == 'configured'

    sync = client.post(f"/api/brokerage/connections/{connection['id']}/sync", headers=headers)
    assert sync.status_code == 200
    assert sync.json()['sync_runs'][0]['detail']['warning'].startswith('Read-only live brokerage sync')

    audit = client.get('/api/brokerage/audit-trail', headers=headers)
    assert audit.status_code == 200
    actions = {item['entity_type'] for item in audit.json()['audit_trail']}
    assert {'brokerage_connection', 'brokerage_consent', 'brokerage_credential_flow'} <= actions


def test_b66_ui_surface_exists() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')
    assert 'id="brokerageProviderSelect"' in index
    assert 'Connect brokerage account' in index
    assert 'id="brokerageCredentialButton"' in index
    assert 'id="brokerageConsentTable"' in index
    assert 'handleBrokerageCredentialSetup' in app_js
    assert '/api/brokerage/connections/${connection.id}/consent' in app_js
