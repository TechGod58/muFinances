from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_production_readiness_evidence_dashboard.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)
os.environ['CAMPUS_FPM_DB_POOL_SIZE'] = '4'

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}", 'X-Trace-Id': 'test-trace-b119'}


def scenario_id(headers: dict[str, str]) -> int:
    response = client.get('/api/scenarios', headers=headers)
    assert response.status_code == 200
    return int(response.json()[0]['id'])


def seed_secure_financial_event(headers: dict[str, str]) -> None:
    response = client.post(
        '/api/foundation/ledger',
        headers=headers,
        json={
            'scenario_id': scenario_id(headers),
            'department_code': 'OPS',
            'fund_code': 'GEN',
            'account_code': 'B119_AUDIT',
            'period': '2026-12',
            'amount': 1190,
            'source': 'b119_test',
            'ledger_type': 'actual',
            'ledger_basis': 'actual',
            'notes': 'B119 production readiness evidence seed.',
        },
    )
    assert response.status_code == 200, response.text


def seed_connector_evidence(headers: dict[str, str]) -> None:
    connector = client.post(
        '/api/integrations/connectors',
        headers=headers,
        json={
            'connector_key': 'b119-erp',
            'name': 'B119 ERP Evidence',
            'system_type': 'erp',
            'direction': 'inbound',
            'config': {'adapter_key': 'erp_gl'},
        },
    )
    assert connector.status_code == 200, connector.text
    health = client.post('/api/integrations/connectors/b119-erp/health', headers=headers)
    assert health.status_code == 200, health.text


def test_production_readiness_dashboard_rolls_up_b119_evidence() -> None:
    headers = admin_headers()
    seed_secure_financial_event(headers)
    seed_connector_evidence(headers)

    secure_backup = client.post('/api/secure-audit-operations/backup-verification', headers=headers)
    assert secure_backup.status_code == 200
    mssql = client.post(
        '/api/mssql-live-proof/run',
        headers=headers,
        json={'allow_rehearsal_without_dsn': True, 'attempt_live_connection': False, 'create_backup': True, 'run_restore_validation': True},
    )
    assert mssql.status_code == 200, mssql.text
    identity = client.post(
        '/api/security/manchester-identity-live-proof/run',
        headers=headers,
        json={'signed_by': 'it.security@manchester.edu', 'notes': 'B119 dashboard identity evidence.'},
    )
    assert identity.status_code == 200, identity.text
    readiness = client.post('/api/operations-readiness/run', headers=headers, json={'run_key': 'b119-dashboard-evidence'})
    assert readiness.status_code == 200, readiness.text

    response = client.get('/api/admin/production-readiness-dashboard', headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B105'
    assert payload['evidence_batch'] == 'B119'
    assert payload['evidence']['secure_audit']['checks']['secure_financial_audit_chain_ready'] is True
    assert payload['evidence']['secure_audit']['latest_backup_verification']['status'] == 'pass'
    assert payload['evidence']['mssql']['checks']['mssql_driver_ready'] is True
    assert payload['evidence']['identity']['checks']['production_sso_ready'] is True
    assert payload['evidence']['connectors']['checks']['connector_health_dashboard_ready'] is True
    assert payload['evidence']['connectors']['counts']['adapters'] >= 7
    assert payload['evidence']['connectors']['counts']['connectors'] >= 1
    assert payload['evidence']['workers']['status'] in {'ok', 'warning', 'blocked'}
    assert sum(payload['evidence']['workers']['counts'].values()) >= 1
    assert payload['evidence']['backup']['latest_drill']['status'] == 'pass'
    assert payload['evidence']['migration']['registered_count'] >= 1
    assert payload['evidence']['alerts']['open_count'] >= 0

    component_names = {component['name'] for component in payload['components']}
    assert {
        'Secure audit verification',
        'MS SQL status',
        'Identity status',
        'Connector health',
        'Alert evidence',
    } <= component_names
