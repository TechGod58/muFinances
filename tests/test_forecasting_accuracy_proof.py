from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_forecasting_accuracy_proof.db'
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


def test_forecasting_accuracy_proof_runs_predictive_stack_end_to_end() -> None:
    headers = admin_headers()

    status = client.get('/api/scenario-engine/forecasting-accuracy-proof/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B95'
    assert status.json()['complete'] is True

    run = client.post('/api/scenario-engine/forecasting-accuracy-proof/run', headers=headers, json={})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['historical_actuals_ingested'] is True
    assert payload['checks']['seasonal_forecast_posted'] is True
    assert payload['checks']['historical_trend_forecast_posted'] is True
    assert payload['checks']['confidence_intervals_recorded'] is True
    assert payload['checks']['backtest_scored'] is True
    assert payload['checks']['recommendation_comparison_ready'] is True
    assert payload['checks']['forecast_actual_variance_ready'] is True
    assert payload['checks']['explainable_drivers_ready'] is True
    assert payload['artifacts']['backtest']['status'] == 'scored'
    assert len(payload['artifacts']['seasonal_forecast']['created_lines']) == 3
    assert payload['artifacts']['driver_explanation']['count'] >= 1

    rows = client.get('/api/scenario-engine/forecasting-accuracy-proof/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
