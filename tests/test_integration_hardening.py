from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_integration_hardening.db'
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


def scenario_id() -> int:
    scenarios = client.get('/api/scenarios', headers=admin_headers()).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def test_integration_hardening_controls_and_new_import_types() -> None:
    headers = admin_headers()
    sid = scenario_id()

    for connector_key, system_type in [('bank-cash', 'banking'), ('crm-pipeline', 'crm')]:
        response = client.post(
            '/api/integrations/connectors',
            headers=headers,
            json={'connector_key': connector_key, 'name': connector_key, 'system_type': system_type, 'direction': 'inbound', 'config': {'mode': 'test'}},
        )
        assert response.status_code == 200

    mapping = client.post(
        '/api/integrations/mapping-templates',
        headers=headers,
        json={'template_key': 'bank-map', 'connector_key': 'bank-cash', 'import_type': 'banking_cash', 'mapping': {'txn_amount': 'amount'}, 'active': True},
    )
    assert mapping.status_code == 200
    assert mapping.json()['mapping']['txn_amount'] == 'amount'

    rule = client.post(
        '/api/integrations/validation-rules',
        headers=headers,
        json={'rule_key': 'cash-amount-numeric', 'import_type': 'banking_cash', 'field_name': 'amount', 'operator': 'numeric', 'severity': 'error', 'active': True},
    )
    assert rule.status_code == 200

    credential = client.post(
        '/api/integrations/credentials',
        headers=headers,
        json={'connector_key': 'bank-cash', 'credential_key': 'api-token', 'secret_value': 'super-secret-token'},
    )
    assert credential.status_code == 200
    assert credential.json()['masked_value'] != 'super-secret-token'
    assert credential.json()['secret_ref'].startswith('vault://')

    bank_import = client.post(
        '/api/integrations/imports',
        headers=headers,
        json={
            'scenario_id': sid,
            'connector_key': 'bank-cash',
            'source_format': 'csv',
            'import_type': 'banking_cash',
            'rows': [{'bank_account': 'OPERATING', 'transaction_date': '2026-08-15', 'txn_amount': 4500, 'description': 'Wire deposit'}],
        },
    )
    assert bank_import.status_code == 200
    assert bank_import.json()['accepted_rows'] == 1
    cash_rows = client.get(f'/api/integrations/banking-cash-imports?scenario_id={sid}', headers=headers)
    assert cash_rows.json()['cash_imports'][0]['amount'] == 4500

    crm_import = client.post(
        '/api/integrations/imports',
        headers=headers,
        json={
            'scenario_id': sid,
            'connector_key': 'crm-pipeline',
            'source_format': 'xlsx',
            'import_type': 'crm_enrollment',
            'rows': [{'pipeline_stage': 'accepted', 'term': '2026-FA', 'headcount': 125, 'yield_rate': 0.42}],
        },
    )
    assert crm_import.status_code == 200
    assert crm_import.json()['accepted_rows'] == 1

    retry = client.post(
        '/api/integrations/retry-events',
        headers=headers,
        json={'connector_key': 'bank-cash', 'operation_type': 'sync', 'error_message': 'Timeout', 'attempts': 2},
    )
    assert retry.status_code == 200
    assert retry.json()['status'] == 'retry_scheduled'

    sync = client.post('/api/integrations/sync-jobs', headers=headers, json={'connector_key': 'bank-cash', 'job_type': 'banking_sync'})
    assert sync.status_code == 200
    logs = client.get('/api/integrations/sync-logs?connector_key=bank-cash', headers=headers)
    assert logs.status_code == 200
    assert logs.json()['sync_logs']


def test_integrations_status_reports_b21_complete() -> None:
    response = client.get('/api/integrations/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B21'
    assert payload['complete'] is True
    assert payload['checks']['credential_vault_ready'] is True
    assert payload['checks']['crm_enrollment_pipeline_import_ready'] is True
