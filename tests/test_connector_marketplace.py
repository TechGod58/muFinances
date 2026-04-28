from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_connector_marketplace.db'
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


def test_connector_marketplace_status_and_adapter_coverage() -> None:
    response = client.get('/api/integrations/marketplace/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B40'
    assert payload['complete'] is True
    assert payload['checks']['erp_sis_hr_payroll_grants_banking_brokerage_ready'] is True

    adapters = client.get('/api/integrations/adapters', headers=admin_headers())
    assert adapters.status_code == 200
    system_types = {item['system_type'] for item in adapters.json()['adapters']}
    assert {'erp', 'sis', 'hr', 'payroll', 'grants', 'banking', 'brokerage'} <= system_types


def test_auth_health_mapping_preset_and_source_drillback() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    connector = client.post(
        '/api/integrations/connectors',
        headers=headers,
        json={'connector_key': 'erp-market', 'name': 'ERP Marketplace', 'system_type': 'erp', 'direction': 'inbound', 'config': {'adapter_key': 'erp_gl'}},
    )
    assert connector.status_code == 200
    assert connector.json()['config']['adapter_key'] == 'erp_gl'

    auth = client.post(
        '/api/integrations/auth-flows',
        headers=headers,
        json={'connector_key': 'erp-market', 'adapter_key': 'erp_gl', 'credential_ref': 'vault://erp-market/api-token'},
    )
    assert auth.status_code == 200
    assert auth.json()['status'] == 'ready'

    health = client.post('/api/integrations/connectors/erp-market/health', headers=headers)
    assert health.status_code == 200
    assert health.json()['status'] == 'healthy'

    preset = client.post(
        '/api/integrations/mapping-presets/apply',
        headers=headers,
        json={'connector_key': 'erp-market', 'preset_key': 'erp-ledger-standard', 'template_key': 'erp-market-standard'},
    )
    assert preset.status_code == 200
    assert preset.json()['mapping']['acct'] == 'account_code'

    imported = client.post(
        '/api/integrations/imports',
        headers=headers,
        json={
            'scenario_id': sid,
            'connector_key': 'erp-market',
            'source_format': 'csv',
            'import_type': 'ledger',
            'rows': [{'dept': 'SCI', 'fund': 'GEN', 'acct': 'SUPPLIES', 'fiscal_period': '2026-08', 'amount': -150, 'source_record_id': 'ERP-ROW-1'}],
        },
    )
    assert imported.status_code == 200
    assert imported.json()['accepted_rows'] == 1

    drillback = client.get('/api/integrations/source-drillbacks/erp-market/ERP-ROW-1', headers=headers)
    assert drillback.status_code == 200
    assert drillback.json()['target_type'] == 'planning_ledger'
    assert drillback.json()['source_payload']['acct'] == 'SUPPLIES'

    workspace = client.get('/api/integrations/marketplace', headers=headers)
    assert workspace.status_code == 200
    assert workspace.json()['auth_flows'][0]['connector_key'] == 'erp-market'
    assert workspace.json()['source_drillbacks'][0]['source_record_id'] == 'ERP-ROW-1'


def test_connector_marketplace_ui_contract() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')

    assert 'id="adapterTable"' in index
    assert 'id="connectorHealthTable"' in index
    assert 'id="authFlowTable"' in index
    assert 'id="mappingPresetTable"' in index
    assert 'id="sourceDrillbackTable"' in index
    assert 'handleConnectorAuthStart' in app_js
    assert 'handleConnectorHealthRun' in app_js
    assert 'handleMappingPresetApply' in app_js
