from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_predictive_forecasting_studio.db'
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


def scenario_id() -> int:
    scenarios = client.get('/api/scenarios', headers=admin_headers()).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def seed_history(headers: dict[str, str], sid: int) -> None:
    client.post(
        '/api/scenario-engine/drivers',
        headers=headers,
        json={
            'scenario_id': sid,
            'driver_key': 'tuition_growth',
            'label': 'Tuition growth',
            'driver_type': 'ratio',
            'unit': 'ratio',
            'value': 0.04,
            'locked': False,
        },
    )
    response = client.post(
        '/api/scenario-engine/actuals',
        headers=headers,
        json={
            'scenario_id': sid,
            'source_version': 'predictive-test',
            'rows': [
                {'scenario_id': sid, 'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': 'TUITION', 'period': '2026-05', 'amount': 98000, 'notes': 'History'},
                {'scenario_id': sid, 'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': 'TUITION', 'period': '2026-06', 'amount': 99000, 'notes': 'History'},
                {'scenario_id': sid, 'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': 'TUITION', 'period': '2026-07', 'amount': 101000, 'notes': 'History'},
                {'scenario_id': sid, 'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': 'TUITION', 'period': '2026-08', 'amount': 104000, 'notes': 'Holdout'},
                {'scenario_id': sid, 'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': 'TUITION', 'period': '2026-09', 'amount': 105000, 'notes': 'Holdout'},
            ],
        },
    )
    assert response.status_code == 200


def test_predictive_forecasting_studio_workflow() -> None:
    headers = admin_headers()
    sid = scenario_id()
    seed_history(headers, sid)

    choice = client.post(
        '/api/scenario-engine/model-choices',
        headers=headers,
        json={
            'scenario_id': sid,
            'choice_key': 'tuition-seasonal-choice',
            'account_code': 'TUITION',
            'department_code': 'SCI',
            'selected_method': 'seasonal',
            'seasonality_mode': 'monthly',
            'confidence_level': 0.86,
        },
    )
    assert choice.status_code == 200
    choice_id = choice.json()['id']

    tuning = client.post(
        '/api/scenario-engine/tuning-profiles',
        headers=headers,
        json={
            'choice_id': choice_id,
            'seasonality_strength': 1.2,
            'confidence_level': 0.9,
            'confidence_spread': 0.1,
            'driver_weights': {'tuition_growth': 0.8},
        },
    )
    assert tuning.status_code == 200
    assert tuning.json()['driver_weights']['tuition_growth'] == 0.8

    backtest = client.post(
        '/api/scenario-engine/backtests',
        headers=headers,
        json={'choice_id': choice_id, 'period_start': '2026-08', 'period_end': '2026-09'},
    )
    assert backtest.status_code == 200
    assert backtest.json()['status'] == 'scored'
    assert backtest.json()['accuracy_score'] >= 0

    recommendations = client.post(
        '/api/scenario-engine/recommendations/compare',
        headers=headers,
        json={
            'scenario_id': sid,
            'account_code': 'TUITION',
            'department_code': 'SCI',
            'methods': ['straight_line', 'rolling_average', 'seasonal', 'historical_trend'],
        },
    )
    assert recommendations.status_code == 200
    assert recommendations.json()['recommended_method'] in {'straight_line', 'rolling_average', 'seasonal', 'historical_trend'}
    assert recommendations.json()['comparison']['methods']

    explanations = client.post(
        f'/api/scenario-engine/driver-explanations/run?scenario_id={sid}&account_code=TUITION&department_code=SCI',
        headers=headers,
    )
    assert explanations.status_code == 200
    assert explanations.json()['count'] >= 1

    workspace = client.get(f'/api/scenario-engine/predictive-workspace?scenario_id={sid}', headers=headers)
    assert workspace.status_code == 200
    payload = workspace.json()
    assert payload['status']['batch'] == 'B42'
    assert payload['model_choices']
    assert payload['backtests']
    assert payload['recommendations']
    assert payload['driver_explanations']


def test_predictive_forecasting_status_reports_b42_complete() -> None:
    response = client.get('/api/scenario-engine/predictive-status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B42'
    assert payload['complete'] is True
    assert payload['checks']['recommendation_comparison_ready'] is True
