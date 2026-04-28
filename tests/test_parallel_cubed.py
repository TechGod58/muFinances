from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_parallel_cubed.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def auth_headers() -> dict[str, str]:
    response = client.post(
        '/api/auth/login',
        json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'},
    )
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def test_parallel_cubed_status_loads_finance_genome() -> None:
    response = client.get('/api/parallel-cubed/status', headers=auth_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['ready'] is True
    assert payload['genomeId'] == 'mufinances-parallel-cubed'
    assert any(region['id'] == 'foundation' for region in payload['regions'])


def test_parallel_cubed_route_activates_bound_regions() -> None:
    response = client.post(
        '/api/parallel-cubed/route',
        headers=auth_headers(),
        json={
            'seedRegion': 'foundation',
            'intent': 'Build the planning ledger backbone',
            'feedback': 0.5,
            'entropy': 0.1,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload['seedRegion'] == 'foundation'
    assert 'planning' in payload['activeRegions']
    assert 'planning_ledger' in payload['bindings']['foundation']


def test_parallel_cubed_guard_checkpoints_elevated_finance_risk() -> None:
    response = client.post(
        '/api/parallel-cubed/guard',
        headers=auth_headers(),
        json={
            'validationErrors': 1,
            'missingControls': 1,
            'staleInputs': 0,
            'openExceptions': 0,
            'highVarianceCount': 0,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload['decision']['action'] == 'checkpoint'
    assert payload['decision']['level'] == 'elevated'


def test_parallel_cubed_batches_follow_action_plan_order() -> None:
    response = client.get('/api/parallel-cubed/batches', headers=auth_headers())
    assert response.status_code == 200
    batches = response.json()['batches']
    assert batches[0]['id'] == 'B01'
    assert batches[0]['seedRegion'] == 'foundation'
    assert batches[-1]['id'] == 'B12'
    assert batches[-1]['seedRegion'] == 'operations'
