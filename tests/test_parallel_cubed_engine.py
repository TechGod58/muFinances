from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_parallel_cubed_engine.db'
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


def unlocked_scenario_id(headers: dict[str, str]) -> int:
    response = client.post(
        '/api/scenarios',
        headers=headers,
        json={
            'name': 'B65 Parallel Cubed Test',
            'version': 'v1',
            'start_period': '2027-01',
            'end_period': '2027-06',
        },
    )
    assert response.status_code == 200
    return int(response.json()['id'])


def test_parallel_cubed_detects_cores_partitions_reduces_and_records_benchmark() -> None:
    headers = admin_headers()
    scenario_id = unlocked_scenario_id(headers)

    cpu = client.get('/api/performance/parallel-cubed/cpu', headers=headers)
    assert cpu.status_code == 200
    assert cpu.json()['logical_cores'] >= 1

    run = client.post(
        '/api/performance/parallel-cubed/run',
        headers=headers,
        json={
            'scenario_id': scenario_id,
            'work_type': 'mixed',
            'partition_strategy': 'department',
            'max_workers': 4,
            'row_count': 240,
            'include_import': True,
            'include_reports': True,
        },
    )
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['worker_count'] >= 1
    assert payload['logical_cores'] >= payload['worker_count']
    assert payload['partition_count'] >= payload['worker_count']
    assert payload['reduce_status'] == 'matched'
    assert payload['result']['calculation']['reduce_matches_serial'] is True
    assert payload['result']['import']['accepted_rows'] >= 1
    assert payload['result']['report']['generated_sections'] >= 1
    assert payload['benchmark']['core_coverage_percent'] > 0
    assert {partition['work_type'] for partition in payload['partitions']} >= {'calculation', 'import', 'report'}

    detail = client.get(f"/api/performance/parallel-cubed/runs/{payload['id']}", headers=headers)
    assert detail.status_code == 200
    assert detail.json()['benchmark']['executor_kind'] == 'thread_pool'

    workspace = client.get(f'/api/performance/parallel-cubed/workspace?scenario_id={scenario_id}', headers=headers)
    assert workspace.status_code == 200
    assert workspace.json()['status']['batch'] == 'B65'
    assert workspace.json()['runs']
    assert workspace.json()['partitions']


def test_parallel_cubed_status_migration_and_ui_surface() -> None:
    headers = admin_headers()
    status = client.get('/api/performance/parallel-cubed/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B65'
    assert payload['complete'] is True
    assert payload['checks']['multi_core_worker_pool_ready'] is True

    migrations = client.get('/api/foundation/migrations', headers=headers)
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0065_parallel_cubed_multi_core_execution_engine' in keys

    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')
    assert 'id="runParallelCubedButton"' in index
    assert 'id="parallelCubedRunTable"' in index
    assert 'id="parallelCubedPartitionTable"' in index
    assert '/api/performance/parallel-cubed/run' in app_js
    assert 'handleParallelCubedRun' in app_js
