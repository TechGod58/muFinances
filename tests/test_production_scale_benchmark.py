from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_production_scale_benchmark.db'
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


def test_production_scale_benchmark_runs_full_stack_profile() -> None:
    headers = admin_headers()
    status = client.get('/api/performance/production-scale/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B125'
    assert status.json()['complete'] is True

    run = client.post(
        '/api/performance/production-scale/run',
        headers=headers,
        json={
            'run_key': 'b125-production-scale',
            'years': 5,
            'scenario_count': 6,
            'department_count': 40,
            'grant_count': 25,
            'employee_count': 500,
            'account_count': 80,
            'ledger_row_count': 12000,
            'benchmark_row_count': 12000,
            'user_count': 24,
        },
    )
    assert run.status_code == 200, run.text
    payload = run.json()
    assert payload['batch'] == 'B125'
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert all(payload['checks'].values())
    assert payload['profile']['years'] == 5
    assert payload['enterprise_run']['seed']['period_count'] >= 60
    assert payload['enterprise_run']['seed']['ledger_rows_seeded'] >= 12000
    metrics = {item['metric_key']: item for item in payload['enterprise_run']['benchmark']['metrics']}
    assert metrics['streaming_import']['status'] == 'passed'
    assert metrics['financial_statement']['status'] == 'passed'
    assert metrics['consolidation_run']['status'] == 'passed'
    assert metrics['allocation_run']['status'] == 'passed'
    assert metrics['parallel_cubed_multi_core']['status'] == 'passed'

    rows = client.get('/api/performance/production-scale/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
