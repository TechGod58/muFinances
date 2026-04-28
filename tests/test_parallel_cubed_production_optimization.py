from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_parallel_cubed_production_optimization.db'
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
    response = client.get('/api/bootstrap', headers=headers)
    assert response.status_code == 200
    return int(response.json()['activeScenario']['id'])


def test_parallel_cubed_production_optimization_tunes_strategies_and_records_dashboard() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/performance/parallel-cubed/optimization/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B108'
    assert status.json()['complete'] is True
    assert status.json()['cpu']['logical_cores'] >= 1

    run = client.post(
        '/api/performance/parallel-cubed/optimization/run',
        headers=headers,
        json={'run_key': 'b108-regression', 'scenario_id': sid, 'row_count': 96, 'max_workers': 4},
    )
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['best_strategy'] in {'balanced', 'department', 'account', 'period'}
    assert payload['checks']['cpu_core_detection_under_load_ready'] is True
    assert payload['checks']['partition_strategy_tuning_ready'] is True
    assert payload['checks']['safe_merge_reduce_ready'] is True
    assert payload['checks']['parallel_imports_ready'] is True
    assert payload['checks']['parallel_report_generation_ready'] is True
    assert payload['checks']['benchmark_dashboard_ready'] is True
    assert payload['load_profile']['logical_cores'] >= 1
    assert payload['load_profile']['seeded_rows'] >= 0
    assert len(payload['strategy_results']) == 4

    strategies = {item['strategy'] for item in payload['strategy_results']}
    assert strategies == {'balanced', 'department', 'account', 'period'}
    for item in payload['strategy_results']:
        assert item['reduce_status'] == 'matched'
        assert item['reduce_matches_serial'] is True
        assert item['import_status'] in {'accepted', 'accepted_with_rejections'}
        assert item['import_accepted_rows'] >= 1
        assert item['report_sections'] >= 1
        assert {'calculation', 'import', 'report'} <= set(item['work_types'])
        assert item['partition_count'] >= item['worker_count']
        assert item['throughput_per_second'] > 0

    detail = client.get(f"/api/performance/parallel-cubed/optimization/runs/{payload['id']}", headers=headers)
    assert detail.status_code == 200
    assert detail.json()['run_key'] == 'b108-regression'

    rows = client.get('/api/performance/parallel-cubed/optimization/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1

    partitions = client.get('/api/performance/parallel-cubed/partitions', headers=headers)
    assert partitions.status_code == 200
    assert partitions.json()['count'] >= 1
