from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_access_guard.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app
from app.services.access_guard import NetworkGuardConfig, dn_is_under_ou, is_network_allowed

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post(
        '/api/auth/login',
        json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'},
    )
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def test_manchester_hostname_is_allowed_when_domain_guard_is_enabled() -> None:
    config = NetworkGuardConfig(
        enabled=True,
        allowed_host_suffixes=('manchester.edu',),
        allowed_client_cidrs=(),
        allow_localhost=False,
    )

    assert is_network_allowed('mufinances.manchester.edu', '203.0.113.10', {}, config) is True
    assert is_network_allowed('finance.example.edu', '203.0.113.10', {}, config) is False


def test_on_prem_or_vpn_cidr_is_allowed_when_hostname_is_external() -> None:
    config = NetworkGuardConfig(
        enabled=True,
        allowed_host_suffixes=('manchester.edu',),
        allowed_client_cidrs=('10.30.0.0/16',),
        allow_localhost=False,
    )

    assert is_network_allowed('finance.example.edu', '10.30.44.12', {}, config) is True
    assert is_network_allowed('finance.example.edu', '192.0.2.42', {}, config) is False


def test_forwarded_headers_can_prove_reverse_proxy_host_and_client_network() -> None:
    config = NetworkGuardConfig(
        enabled=True,
        allowed_host_suffixes=('manchester.edu',),
        allowed_client_cidrs=('172.20.0.0/16',),
        allow_localhost=False,
    )

    assert is_network_allowed(
        'internal-proxy',
        '127.0.0.1',
        {'x-forwarded-host': 'budget.manchester.edu'},
        config,
    ) is True
    assert is_network_allowed(
        'internal-proxy',
        '127.0.0.1',
        {'x-forwarded-for': '172.20.8.4'},
        config,
    ) is True


def test_ad_distinguished_name_must_be_inside_allowed_ou() -> None:
    allowed = 'OU=muFinances Users,OU=Finance,DC=manchester,DC=edu'
    assert dn_is_under_ou('CN=Jane Planner,OU=muFinances Users,OU=Finance,DC=manchester,DC=edu', allowed) is True
    assert dn_is_under_ou('CN=Jane Planner,OU=Other Apps,OU=Finance,DC=manchester,DC=edu', allowed) is False


def test_access_guard_status_endpoint_reports_configurable_controls() -> None:
    response = client.get('/api/security/access-guard/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['title'] == 'Manchester Domain And AD OU Access Guard'
    assert payload['network_guard']['allowed_host_suffixes'] == ['manchester.edu']
    assert payload['checks']['network_guard_configurable'] is True
    assert payload['checks']['ad_ou_guard_configurable'] is True
