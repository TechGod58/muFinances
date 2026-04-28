from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_advanced_forecasting.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app import db
from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def scenario_id() -> int:
    scenarios = client.get('/api/scenarios', headers=admin_headers()).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def test_seasonal_historical_actuals_variance_and_driver_graph() -> None:
    headers = admin_headers()
    sid = scenario_id()

    seasonal = client.post(
        '/api/scenario-engine/forecast-runs',
        headers=headers,
        json={
            'scenario_id': sid,
            'method_key': 'seasonal',
            'account_code': 'TUITION',
            'department_code': 'SCI',
            'period_start': '2026-08',
            'period_end': '2026-08',
            'confidence': 0.85,
        },
    )
    assert seasonal.status_code == 200
    assert seasonal.json()['method_key'] == 'seasonal'

    trend = client.post(
        '/api/scenario-engine/forecast-runs',
        headers=headers,
        json={
            'scenario_id': sid,
            'method_key': 'historical_trend',
            'account_code': 'TUITION',
            'department_code': 'SCI',
            'period_start': '2026-09',
            'period_end': '2026-09',
            'confidence': 0.85,
        },
    )
    assert trend.status_code == 200
    assert trend.json()['method_key'] == 'historical_trend'

    actuals = client.post(
        '/api/scenario-engine/actuals',
        headers=headers,
        json={
            'scenario_id': sid,
            'source_version': 'actuals-test',
            'rows': [
                {
                    'scenario_id': sid,
                    'department_code': 'SCI',
                    'fund_code': 'GEN',
                    'account_code': 'TUITION',
                    'period': '2026-08',
                    'amount': 100000,
                    'notes': 'Actual tuition',
                }
            ],
        },
    )
    assert actuals.status_code == 200
    assert actuals.json()['entries'][0]['ledger_basis'] == 'actual'

    variance = client.post(f'/api/scenario-engine/forecast-actual-variances/run?scenario_id={sid}', headers=headers)
    assert variance.status_code == 200
    assert variance.json()['count'] >= 1

    client.post(
        '/api/scenario-engine/planning-drivers',
        headers=headers,
        json={'scenario_id': sid, 'driver_key': 'a', 'label': 'A', 'expression': 'b + 1', 'unit': 'count'},
    )
    client.post(
        '/api/scenario-engine/planning-drivers',
        headers=headers,
        json={'scenario_id': sid, 'driver_key': 'b', 'label': 'B', 'expression': 'a + 1', 'unit': 'count'},
    )
    graph = client.get(f'/api/scenario-engine/driver-graph?scenario_id={sid}', headers=headers)
    assert graph.status_code == 200
    assert graph.json()['has_cycles'] is True
    assert graph.json()['cycles']
    db.execute("DELETE FROM drivers WHERE scenario_id = ? AND driver_key IN ('a', 'b')", (sid,))


def test_advanced_forecasting_status_reports_b15_complete() -> None:
    response = client.get('/api/scenario-engine/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B15'
    assert payload['complete'] is True
    assert payload['checks']['circular_dependency_detection_ready'] is True
