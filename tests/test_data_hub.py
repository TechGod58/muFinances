from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_data_hub.db'
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


def test_data_hub_status_reports_b39_complete() -> None:
    response = client.get('/api/data-hub/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B39'
    assert payload['complete'] is True
    assert payload['checks']['chart_of_accounts_governance_ready'] is True
    assert payload['checks']['source_to_report_lineage_ready'] is True


def test_master_data_change_mapping_metadata_and_lineage_workflow() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    change = client.post(
        '/api/data-hub/change-requests',
        headers=headers,
        json={
            'dimension_kind': 'account',
            'code': 'B39_FEES',
            'name': 'B39 Governed Fees',
            'change_type': 'create',
            'effective_from': '2026-08',
            'metadata': {'account_group': 'Revenue'},
        },
    )
    assert change.status_code == 200
    assert change.json()['status'] == 'pending'

    approved = client.post(f"/api/data-hub/change-requests/{change.json()['id']}/approve", headers=headers)
    assert approved.status_code == 200
    assert approved.json()['status'] == 'approved'

    hierarchy = client.get('/api/foundation/dimensions/hierarchy', headers=headers)
    assert any(item['code'] == 'B39_FEES' for item in hierarchy.json()['account'])

    mapping = client.post(
        '/api/data-hub/mappings',
        headers=headers,
        json={
            'mapping_key': 'erp-b39-fees',
            'source_system': 'ERP',
            'source_dimension': 'account',
            'source_code': '40010',
            'target_dimension': 'account',
            'target_code': 'B39_FEES',
            'effective_from': '2026-08',
            'active': True,
        },
    )
    assert mapping.status_code == 200
    assert mapping.json()['target_code'] == 'B39_FEES'
    assert mapping.json()['active'] is True

    metadata = client.post(
        '/api/data-hub/metadata-approvals',
        headers=headers,
        json={'entity_type': 'dimension_member', 'entity_id': 'account:B39_FEES', 'metadata': {'steward': 'Controller'}},
    )
    assert metadata.status_code == 200
    approved_metadata = client.post(f"/api/data-hub/metadata-approvals/{metadata.json()['id']}/approve", headers=headers)
    assert approved_metadata.status_code == 200
    assert approved_metadata.json()['status'] == 'approved'

    ledger = client.post(
        '/api/foundation/ledger',
        headers=headers,
        json={'scenario_id': sid, 'department_code': 'ART', 'fund_code': 'GEN', 'account_code': 'B39_FEES', 'period': '2026-08', 'amount': 900, 'source': 'erp_import', 'source_version': 'batch-39'},
    )
    assert ledger.status_code == 200

    lineage = client.post(
        f'/api/data-hub/lineage/build?scenario_id={sid}&target_type=report&target_id=financial_statement',
        headers=headers,
    )
    assert lineage.status_code == 200
    assert any(item['source_id'] == 'erp_import:batch-39' for item in lineage.json()['lineage'])

    workspace = client.get(f'/api/data-hub/workspace?scenario_id={sid}', headers=headers)
    assert workspace.status_code == 200
    assert workspace.json()['change_requests'][0]['status'] == 'approved'
    assert workspace.json()['mappings'][0]['mapping_key'] == 'erp-b39-fees'
    assert workspace.json()['lineage'][0]['target_id'] == 'financial_statement'


def test_data_hub_ui_contract() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')

    assert 'id="data-hub"' in index
    assert 'id="requestMasterDataChangeButton"' in index
    assert 'id="masterDataChangeTable"' in index
    assert 'id="masterDataMappingTable"' in index
    assert 'id="metadataApprovalTable"' in index
    assert 'id="dataLineageTable"' in index
    assert 'renderDataHub' in app_js
    assert 'handleMasterDataChangeRequest' in app_js
    assert 'handleDataLineageBuild' in app_js
