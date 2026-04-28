from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_performance_benchmark_harness.db'
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
    response = client.get('/api/bootstrap', headers=headers)
    assert response.status_code == 200
    return int(response.json()['activeScenario']['id'])


def test_b60_benchmark_harness_seeds_measures_plans_and_thresholds() -> None:
    headers = admin_headers()
    scenario_id = active_scenario_id(headers)

    status = client.get('/api/performance/benchmarks/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B60'
    assert status.json()['complete'] is True

    run = client.post(
        '/api/performance/benchmarks/run',
        headers=headers,
        json={
            'scenario_id': scenario_id,
            'dataset_key': 'campus-realistic-benchmark-test',
            'row_count': 120,
            'backend': 'runtime',
            'thresholds': {
                'apply_indexes': 10000,
                'seed_large_dataset': 10000,
                'summary_query': 10000,
                'financial_statement': 10000,
                'streaming_import': 10000,
                'query_plan': 10000,
            },
            'include_import': True,
            'include_reports': True,
        },
    )
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['results']['seed']['seeded_rows'] >= 120
    assert payload['regression_failures'] == []
    assert len(payload['indexes']) >= 5
    assert payload['query_plans']['backend'] in {'sqlite', 'postgres'}
    assert 'planning_ledger' in payload['query_plans']['postgres_sql']

    metric_keys = {metric['metric_key'] for metric in payload['metrics']}
    assert {'apply_indexes', 'seed_large_dataset', 'summary_query', 'financial_statement', 'streaming_import', 'query_plan'} <= metric_keys

    detail = client.get(f"/api/performance/benchmarks/{payload['id']}", headers=headers)
    assert detail.status_code == 200
    assert detail.json()['metrics'][0]['status'] == 'passed'

    listing = client.get(f'/api/performance/benchmarks?scenario_id={scenario_id}', headers=headers)
    assert listing.status_code == 200
    assert listing.json()['count'] >= 1

    workspace = client.get(f'/api/performance/workspace?scenario_id={scenario_id}', headers=headers)
    assert workspace.status_code == 200
    assert workspace.json()['benchmark_status']['batch'] == 'B60'
    assert workspace.json()['benchmark_runs']


def test_b60_migration_and_ui_surface_exist() -> None:
    headers = admin_headers()
    migrations = client.get('/api/foundation/migrations', headers=headers)
    assert migrations.status_code == 200
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0060_performance_benchmark_harness' in keys

    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')
    assert 'id="runBenchmarkHarnessButton"' in index
    assert 'id="benchmarkHarnessTable"' in index
    assert '/api/performance/benchmarks/run' in app_js
