from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_campus_integrations.db'
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


def test_connectors_import_rejections_sync_and_powerbi_export() -> None:
    headers = admin_headers()
    sid = scenario_id()

    connector = client.post(
        '/api/integrations/connectors',
        headers=headers,
        json={
            'connector_key': 'erp-gl',
            'name': 'ERP General Ledger',
            'system_type': 'erp',
            'direction': 'inbound',
            'config': {'mode': 'local-file-drop'},
        },
    )
    assert connector.status_code == 200
    assert connector.json()['connector_key'] == 'erp-gl'

    import_run = client.post(
        '/api/integrations/imports',
        headers=headers,
        json={
            'scenario_id': sid,
            'connector_key': 'erp-gl',
            'source_format': 'csv',
            'import_type': 'ledger',
            'rows': [
                {
                    'department_code': 'SCI',
                    'fund_code': 'GEN',
                    'account_code': 'SUPPLIES',
                    'period': '2026-08',
                    'amount': -1250,
                    'notes': 'ERP import row',
                },
                {
                    'department_code': 'SCI',
                    'fund_code': 'GEN',
                    'account_code': '',
                    'period': '2026-08',
                    'amount': 'bad',
                },
            ],
        },
    )
    assert import_run.status_code == 200
    payload = import_run.json()
    assert payload['accepted_rows'] == 1
    assert payload['rejected_rows'] == 1
    assert payload['status'] == 'accepted_with_rejections'
    assert payload['rejections'][0]['reason'].startswith('Missing required fields')

    sync = client.post(
        '/api/integrations/sync-jobs',
        headers=headers,
        json={'connector_key': 'erp-gl', 'job_type': 'erp_sync'},
    )
    assert sync.status_code == 200
    assert sync.json()['status'] == 'complete'

    export = client.post(
        '/api/integrations/powerbi-exports',
        headers=headers,
        json={'scenario_id': sid, 'dataset_name': 'FY27 Operating Plan'},
    )
    assert export.status_code == 200
    assert export.json()['status'] == 'ready'
    assert 'planning_ledger' in export.json()['manifest']['tables']


def test_integrations_status_reports_b09_complete() -> None:
    response = client.get('/api/integrations/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] in {'B09', 'B21'}
    assert payload['complete'] is True
    assert payload['checks']['powerbi_export_ready'] is True
