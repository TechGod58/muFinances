from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_performance_reliability.db'
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


def active_scenario_id() -> int:
    response = client.get('/api/bootstrap', headers=admin_headers())
    assert response.status_code == 200
    return int(response.json()['activeScenario']['id'])


def test_performance_benchmark_index_cache_jobs_and_restore_automation() -> None:
    headers = admin_headers()
    scenario_id = active_scenario_id()

    indexes = client.post('/api/performance/index-recommendations/seed', headers=headers)
    assert indexes.status_code == 200
    assert indexes.json()['count'] >= 5

    load_test = client.post(
        '/api/performance/load-tests',
        headers=headers,
        json={
            'scenario_id': scenario_id,
            'test_type': 'calculation_benchmark',
            'row_count': 1000,
            'backend': 'runtime',
        },
    )
    assert load_test.status_code == 200
    assert load_test.json()['status'] == 'completed'
    assert load_test.json()['throughput_per_second'] > 0

    invalidation = client.post(
        '/api/performance/cache-invalidations',
        headers=headers,
        json={'cache_key': f'scenario:{scenario_id}', 'scope': 'scenario', 'reason': 'test invalidation'},
    )
    assert invalidation.status_code == 200
    assert invalidation.json()['status'] == 'invalidated'

    job = client.post(
        '/api/performance/jobs',
        headers=headers,
        json={
            'job_type': 'large_import_stress',
            'priority': 10,
            'payload': {'scenario_id': scenario_id, 'row_count': 1200},
        },
    )
    assert job.status_code == 200
    assert job.json()['status'] == 'queued'

    ran = client.post('/api/performance/jobs/run-next', headers=headers)
    assert ran.status_code == 200
    assert ran.json()['ran'] is True
    assert ran.json()['job']['status'] == 'completed'

    backup = client.post('/api/operations/backups', headers=headers)
    assert backup.status_code == 200
    restore_auto = client.post(
        '/api/performance/restore-automations',
        headers=headers,
        json={'backup_key': backup.json()['backup_key'], 'verify_only': True},
    )
    assert restore_auto.status_code == 200
    assert restore_auto.json()['status'] == 'passed'
    assert restore_auto.json()['result']['integrity_check'] == 'ok'

    workspace = client.get(f'/api/performance/workspace?scenario_id={scenario_id}', headers=headers)
    assert workspace.status_code == 200
    payload = workspace.json()
    assert payload['status']['batch'] == 'B47'
    assert payload['load_tests']
    assert payload['background_jobs']
    assert payload['restore_automations']


def test_performance_status_and_migration_are_registered() -> None:
    headers = admin_headers()
    client.post('/api/performance/index-recommendations/seed', headers=headers)

    status = client.get('/api/performance/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B47'
    assert payload['complete'] is True
    assert payload['checks']['background_job_queue_ready'] is True

    migrations = client.get('/api/foundation/migrations', headers=headers)
    assert migrations.status_code == 200
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0048_performance_scale_reliability' in keys


def test_performance_reliability_ui_surface_exists() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')

    assert 'id="performance-reliability"' in index
    assert 'id="runPerformanceLoadTestButton"' in index
    assert 'id="indexStrategyTable"' in index
    assert 'id="backgroundJobTable"' in index
    assert 'renderPerformanceReliability' in app_js
    assert '/api/performance/workspace' in app_js
