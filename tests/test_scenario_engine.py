from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_scenario_engine.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app
from app.services.forecast_engine import safe_eval

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def scenario_id() -> int:
    scenarios = client.get('/api/scenarios', headers=admin_headers()).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def test_typed_driver_clone_compare_and_forecast_lineage() -> None:
    headers = admin_headers()
    sid = scenario_id()
    driver = client.post(
        '/api/scenario-engine/drivers',
        headers=headers,
        json={
            'scenario_id': sid,
            'driver_key': 'tuition_growth',
            'label': 'Tuition growth',
            'driver_type': 'ratio',
            'unit': 'ratio',
            'value': 0.05,
            'locked': False,
        },
    )
    assert driver.status_code == 200
    assert driver.json()['driver_key'] == 'tuition_growth'

    forecast = client.post(
        '/api/scenario-engine/forecast-runs',
        headers=headers,
        json={
            'scenario_id': sid,
            'method_key': 'growth_rate',
            'account_code': 'TUITION',
            'department_code': 'SCI',
            'period_start': '2026-08',
            'period_end': '2026-09',
            'driver_key': 'tuition_growth',
            'confidence': 0.85,
        },
    )
    assert forecast.status_code == 200
    payload = forecast.json()
    assert payload['status'] == 'posted'
    assert len(payload['created_lines']) == 2
    assert payload['created_lines'][0]['confidence_low'] < payload['created_lines'][0]['ledger_entry']['amount']
    run_id = payload['id']

    lineage = client.get(f'/api/scenario-engine/forecast-runs/{run_id}/lineage', headers=headers)
    assert lineage.status_code == 200
    assert lineage.json()['count'] == 2
    assert lineage.json()['lineage'][0]['driver_key'] == 'tuition_growth'

    clone = client.post(
        f'/api/scenario-engine/scenarios/{sid}/clone',
        headers=headers,
        json={'name': 'FY27 Operating Plan Upside', 'version': 'upside'},
    )
    assert clone.status_code == 200
    clone_id = clone.json()['id']
    assert clone_id != sid

    compare = client.get(
        f'/api/scenario-engine/compare?base_scenario_id={sid}&compare_scenario_id={clone_id}',
        headers=headers,
    )
    assert compare.status_code == 200
    assert compare.json()['base_scenario_id'] == sid
    assert compare.json()['compare_scenario_id'] == clone_id


def test_scenario_engine_status_reports_b06_complete() -> None:
    response = client.get('/api/scenario-engine/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] in {'B06', 'B15'}
    assert payload['complete'] is True
    assert payload['checks']['driver_lineage_ready'] is True


def test_forecast_safe_eval_uses_formula_engine_without_eval_escape() -> None:
    assert safe_eval('max(student_growth, 0.03) + 0.01', {'student_growth': 0.02}) == 0.04
    try:
        safe_eval('__import__("os").system("dir")', {})
    except ValueError:
        pass
    else:
        raise AssertionError('unsafe driver expression was not blocked')
