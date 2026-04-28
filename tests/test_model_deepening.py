from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_model_deepening.db'
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


def create_model(headers: dict[str, str]) -> int:
    sid = scenario_id(headers)
    model = client.post(
        '/api/model-builder/models',
        headers=headers,
        json={
            'scenario_id': sid,
            'model_key': 'b92-enterprise-model',
            'name': 'B92 Enterprise Model',
            'description': 'Deepening proof model.',
            'status': 'active',
        },
    )
    assert model.status_code == 200
    model_id = int(model.json()['id'])
    formula = client.post(
        '/api/model-builder/formulas',
        headers=headers,
        json={
            'model_id': model_id,
            'formula_key': 'net_growth',
            'label': 'Net growth',
            'expression': 'ACCOUNT_TUITION * 0.03',
            'target_account_code': 'B92_GROWTH',
            'target_department_code': 'MODEL',
            'target_fund_code': 'GEN',
            'period_start': '2026-08',
            'period_end': '2026-10',
            'active': True,
        },
    )
    assert formula.status_code == 200
    return model_id


def test_multidimensional_modeling_deepening_proves_optimization_publish_and_branching() -> None:
    headers = admin_headers()
    model_id = create_model(headers)

    status = client.get('/api/model-builder/deepening/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B92'
    assert status.json()['complete'] is True

    proof = client.post(f'/api/model-builder/models/{model_id}/deepening-proof', headers=headers)
    assert proof.status_code == 200
    payload = proof.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['cube_has_sparse_dense_strategy'] is True
    assert payload['checks']['model_version_published'] is True
    assert payload['checks']['dependency_invalidations_recorded'] is True
    assert payload['checks']['formula_ordering_available'] is True
    assert payload['checks']['scenario_branch_created'] is True
    assert payload['optimization']['strategy']['storage_mode'] == 'sparse_fact_table_with_dense_dimension_cache'
    assert payload['version']['status'] == 'published'
    assert payload['branch']['status'] == 'open'

    profiles = client.get(f'/api/model-builder/models/{model_id}/cube/optimization-profiles', headers=headers)
    assert profiles.status_code == 200
    assert profiles.json()['count'] >= 1

    branches = client.get(f'/api/model-builder/models/{model_id}/scenario-branches', headers=headers)
    assert branches.status_code == 200
    assert branches.json()['count'] >= 1
