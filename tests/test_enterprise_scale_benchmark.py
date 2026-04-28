from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_enterprise_scale_benchmark.db'
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


def test_enterprise_scale_benchmark_runs_full_performance_stack() -> None:
    headers = admin_headers()

    status = client.get('/api/performance/enterprise-scale/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B91'
    assert status.json()['complete'] is True

    run = client.post(
        '/api/performance/enterprise-scale/run',
        headers=headers,
        json={
            'years': 5,
            'scenario_count': 3,
            'department_count': 10,
            'grant_count': 5,
            'employee_count': 50,
            'account_count': 20,
            'ledger_row_count': 1000,
            'benchmark_row_count': 1000,
            'thresholds': {
                'apply_indexes': 20000,
                'seed_large_dataset': 20000,
                'summary_query': 20000,
                'financial_statement': 20000,
                'streaming_import': 20000,
                'query_plan': 20000,
                'formula_recalculation': 20000,
                'allocation_run': 20000,
                'consolidation_run': 20000,
                'parallel_cubed_multi_core': 20000,
            },
        },
    )
    assert run.status_code == 200
    payload = run.json()
    assert payload['batch'] if 'batch' in payload else True
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['profile']['years'] == 5
    assert payload['seed']['period_count'] == 60
    assert payload['seed']['scenario_count'] == 3
    assert payload['seed']['department_count'] == 10
    assert payload['checks']['report_benchmark_completed'] is True
    assert payload['checks']['import_benchmark_completed'] is True
    assert payload['checks']['formula_benchmark_completed'] is True
    assert payload['checks']['allocation_benchmark_completed'] is True
    assert payload['checks']['consolidation_benchmark_completed'] is True
    assert payload['checks']['parallel_cubed_benchmark_completed'] is True

    rows = client.get('/api/performance/enterprise-scale/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
