from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_connector_productionization.db'
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


def test_connector_production_status_contracts_vault_streaming_mapping_and_drillback() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/integrations/production/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B59'
    assert status.json()['checks']['adapter_contracts_ready'] is True

    contracts = client.get('/api/integrations/adapter-contracts?system_type=erp', headers=headers)
    assert contracts.status_code == 200
    contract = contracts.json()['contracts'][0]
    assert 'stream_import' in contract['contract']['required_methods']
    assert contract['credential_schema']['storage'] == 'vault_ref_only'

    connector = client.post(
        '/api/integrations/connectors',
        headers=headers,
        json={'connector_key': 'b59-erp', 'name': 'B59 ERP', 'system_type': 'erp', 'direction': 'inbound', 'config': {'adapter_key': 'erp_gl'}},
    )
    assert connector.status_code == 200

    credential = client.post(
        '/api/integrations/credentials',
        headers=headers,
        json={'connector_key': 'b59-erp', 'credential_key': 'api-token', 'secret_value': 'fixture-credential-value', 'secret_type': 'api_key', 'expires_at': '2027-01-01'},
    )
    assert credential.status_code == 200
    assert credential.json()['secret_type'] == 'api_key'
    assert credential.json()['masked_value'] != 'fixture-credential-value'

    auth = client.post(
        '/api/integrations/auth-flows',
        headers=headers,
        json={'connector_key': 'b59-erp', 'adapter_key': 'erp_gl', 'credential_ref': credential.json()['secret_ref']},
    )
    assert auth.status_code == 200
    assert auth.json()['status'] == 'ready'

    first_mapping = client.post(
        '/api/integrations/mapping-templates',
        headers=headers,
        json={'template_key': 'b59-map', 'connector_key': 'b59-erp', 'import_type': 'ledger', 'mapping': {'dept': 'department_code', 'acct': 'account_code'}, 'active': True},
    )
    assert first_mapping.status_code == 200
    assert first_mapping.json()['version'] == 1
    second_mapping = client.post(
        '/api/integrations/mapping-templates',
        headers=headers,
        json={'template_key': 'b59-map', 'connector_key': 'b59-erp', 'import_type': 'ledger', 'mapping': {'dept': 'department_code', 'acct': 'account_code', 'src': 'source_record_id'}, 'active': True},
    )
    assert second_mapping.status_code == 200
    assert second_mapping.json()['version'] == 2

    imported = client.post(
        '/api/integrations/imports',
        headers=headers,
        json={
            'scenario_id': sid,
            'connector_key': 'b59-erp',
            'source_format': 'csv',
            'import_type': 'ledger',
            'source_name': 'large-erp-export.csv',
            'stream_chunk_size': 2,
            'rows': [
                {'dept': 'SCI', 'fund_code': 'GEN', 'acct': 'SUPPLIES', 'period': '2026-08', 'amount': -100, 'src': 'B59-1'},
                {'dept': 'SCI', 'fund_code': 'GEN', 'acct': 'SUPPLIES', 'period': '2026-09', 'amount': -200, 'src': 'B59-2'},
                {'dept': 'SCI', 'fund_code': 'GEN', 'acct': 'SUPPLIES', 'period': '2026-10', 'amount': -300, 'src': 'B59-3'},
            ],
        },
    )
    assert imported.status_code == 200
    payload = imported.json()
    assert payload['accepted_rows'] == 3
    assert payload['stream_chunks'] == 2
    assert payload['mapping_version'] == 2
    assert payload['contract_validated'] == 1

    drillback = client.get('/api/integrations/source-drillbacks/b59-erp/B59-1', headers=headers)
    assert drillback.status_code == 200
    assert drillback.json()['validation_status'] == 'valid'
    validated = client.post(f"/api/integrations/source-drillbacks/{drillback.json()['id']}/validate", headers=headers)
    assert validated.status_code == 200
    assert validated.json()['validation']['status'] == 'valid'

    logs = client.get('/api/integrations/sync-logs?connector_key=b59-erp', headers=headers)
    assert any(row['event_type'] == 'import_stream_chunk' for row in logs.json()['sync_logs'])


def test_real_connector_proof_exercises_all_adapter_flows() -> None:
    headers = admin_headers()
    proof = client.post('/api/integrations/connector-proof/run', headers=headers)
    assert proof.status_code == 200
    payload = proof.json()
    assert payload['complete'] is True
    assert payload['checks']['all_adapters_exercised'] is True
    assert payload['checks']['credential_flows_ready'] is True
    assert payload['checks']['source_drillbacks_ready'] is True
    assert len(payload['results']) == 7
    assert {row['connector']['system_type'] for row in payload['results']} >= {'erp', 'sis', 'hr', 'payroll', 'grants', 'banking', 'brokerage'}

    rejections = client.get('/api/integrations/rejections', headers=headers)
    assert rejections.status_code == 200
    assert rejections.json()['count'] >= 1

    retry_events = client.get('/api/integrations/retry-events', headers=headers)
    assert retry_events.status_code == 200
    assert retry_events.json()['count'] >= 7
