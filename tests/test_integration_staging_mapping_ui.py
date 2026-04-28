from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_integration_staging_mapping_ui.db'
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


def test_staging_preview_reject_approve_and_drillback() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    connector = client.post(
        '/api/integrations/connectors',
        headers=headers,
        json={'connector_key': 'stage-gl', 'name': 'Stage GL', 'system_type': 'erp', 'direction': 'inbound', 'config': {'mode': 'stage'}},
    )
    assert connector.status_code == 200

    mapping = client.post(
        '/api/integrations/mapping-templates',
        headers=headers,
        json={'template_key': 'stage-gl-map', 'connector_key': 'stage-gl', 'import_type': 'ledger', 'mapping': {'dept': 'department_code', 'acct': 'account_code'}, 'active': True},
    )
    assert mapping.status_code == 200

    rule = client.post(
        '/api/integrations/validation-rules',
        headers=headers,
        json={'rule_key': 'stage-amount-numeric', 'import_type': 'ledger', 'field_name': 'amount', 'operator': 'numeric', 'severity': 'error', 'active': True},
    )
    assert rule.status_code == 200

    preview = client.post(
        '/api/integrations/staging/preview',
        headers=headers,
        json={
            'scenario_id': sid,
            'connector_key': 'stage-gl',
            'source_format': 'csv',
            'import_type': 'ledger',
            'source_name': 'controller-upload.csv',
            'rows': [
                {'dept': 'SCI', 'fund_code': 'GEN', 'acct': 'SUPPLIES', 'period': '2026-08', 'amount': -1250, 'notes': 'Preview valid'},
                {'dept': 'SCI', 'fund_code': 'GEN', 'acct': '', 'period': '2026-08', 'amount': 'bad', 'notes': 'Preview invalid'},
            ],
        },
    )
    assert preview.status_code == 200
    payload = preview.json()
    assert payload['status'] == 'needs_review'
    assert payload['valid_rows'] == 1
    assert payload['rejected_rows'] == 1
    valid_row = next(row for row in payload['rows'] if row['status'] == 'valid')
    rejected_row = next(row for row in payload['rows'] if row['status'] == 'rejected')
    assert valid_row['mapped']['department_code'] == 'SCI'
    assert rejected_row['validation']

    rejected = client.post(f"/api/integrations/staging/rows/{rejected_row['id']}/reject", headers=headers, json={'note': 'Bad account and amount.'})
    assert rejected.status_code == 200
    assert rejected.json()['rejected_rows'] == 1

    approved = client.post(f"/api/integrations/staging/{payload['id']}/approve", headers=headers, json={'note': 'Valid row approved.'})
    assert approved.status_code == 200
    approved_payload = approved.json()
    assert approved_payload['status'] == 'approved'
    assert approved_payload['approved_rows'] == 1
    assert approved_payload['import_batch']['accepted_rows'] == 1
    approved_row = next(row for row in approved_payload['rows'] if row['status'] == 'approved')

    drillback = client.get(f"/api/integrations/staging/rows/{approved_row['id']}/drillback", headers=headers)
    assert drillback.status_code == 200
    trace = drillback.json()
    assert trace['source_trace']['source_name'] == 'controller-upload.csv'
    assert trace['import_batch']['id'] == approved_payload['import_batch']['id']
    assert trace['ledger_entries'][0]['import_batch_id'] == approved_payload['import_batch']['id']


def test_integration_staging_status_and_ui_contract() -> None:
    headers = admin_headers()
    response = client.get('/api/integrations/staging/status', headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B31'
    assert payload['complete'] is True
    assert payload['checks']['preview_imports_ready'] is True
    assert payload['checks']['drillback_ready'] is True

    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')
    assert 'id="previewImportButton"' in index
    assert 'id="stagingBatchTable"' in index
    assert 'id="stagingRowTable"' in index
    assert '/api/integrations/staging/preview' in app_js
    assert 'handleImportPreview' in app_js
