from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_performance_proof.db'
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


def active_scenario_id(headers: dict[str, str]) -> int:
    response = client.post(
        '/api/scenarios',
        headers=headers,
        json={'name': 'Performance Proof Scenario', 'version': 'proof', 'start_period': '2026-07', 'end_period': '2027-06'},
    )
    assert response.status_code == 200
    return int(response.json()['id'])


def test_performance_proof_exercises_campus_scale_workloads_and_parallel_cubed() -> None:
    headers = admin_headers()
    scenario_id = active_scenario_id(headers)

    status = client.get('/api/performance/proof/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'Performance Proof'
    assert status.json()['checks']['parallel_cubed_multicore_proof_ready'] is True

    run = client.post(
        '/api/performance/proof/run',
        headers=headers,
        json={
            'scenario_id': scenario_id,
            'dataset_key': 'campus-scale-proof-test',
            'row_count': 180,
            'backend': 'runtime',
            'thresholds': {
                'apply_indexes': 60000,
                'seed_large_dataset': 60000,
                'summary_query': 60000,
                'financial_statement': 60000,
                'streaming_import': 60000,
                'query_plan': 60000,
                'formula_recalculation': 60000,
                'allocation_run': 60000,
                'consolidation_run': 60000,
                'parallel_cubed_multi_core': 60000,
            },
            'include_import': True,
            'include_reports': True,
        },
    )
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['regression_failures'] == []

    metric_keys = {metric['metric_key'] for metric in payload['metrics']}
    assert {
        'summary_query',
        'streaming_import',
        'financial_statement',
        'formula_recalculation',
        'allocation_run',
        'consolidation_run',
        'parallel_cubed_multi_core',
    } <= metric_keys

    proof = payload['results']['performance_proof']
    assert proof['checks']['ledger_queries_completed'] is True
    assert proof['checks']['imports_completed'] is True
    assert proof['checks']['reports_completed'] is True
    assert proof['checks']['formulas_completed'] is True
    assert proof['checks']['allocations_completed'] is True
    assert proof['checks']['consolidation_completed'] is True
    assert proof['checks']['parallel_cubed_completed'] is True
    assert proof['checks']['parallel_reduce_verified'] is True
    assert proof['parallel_cubed_run']['benchmark']['worker_count'] >= 1
