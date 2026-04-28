from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_app.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app import db
from app.main import app

client = TestClient(app)


def auth_headers() -> dict[str, str]:
    response = client.post(
        '/api/auth/login',
        json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'},
    )
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def test_health() -> None:
    response = client.get('/api/health')
    assert response.status_code == 200
    assert response.json()['status'] == 'ok'


def test_seeded_scenarios_exist() -> None:
    response = client.get('/api/scenarios', headers=auth_headers())
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert data[0]['name'] == 'FY27 Operating Plan'


def test_create_scenario() -> None:
    response = client.post(
        '/api/scenarios',
        headers=auth_headers(),
        json={
            'name': 'FY28 Operating Plan',
            'version': 'v1',
            'start_period': '2027-07',
            'end_period': '2027-12',
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload['name'] == 'FY28 Operating Plan'
    assert payload['status'] == 'draft'


def test_run_forecast_creates_rows() -> None:
    headers = auth_headers()
    scenarios = client.get('/api/scenarios', headers=headers).json()
    seeded = next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')
    scenario_id = seeded['id']
    before = client.get(f'/api/scenarios/{scenario_id}/line-items', headers=headers).json()
    response = client.post(f'/api/scenarios/{scenario_id}/forecast/run', headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload['scenario_id'] == scenario_id
    assert payload['resolved_drivers']['student_growth'] == 0.035
    assert len(payload['created_line_items']) > 0
    after = client.get(f'/api/scenarios/{scenario_id}/line-items', headers=headers).json()
    assert len(after) > len(before)


def test_forecast_rerun_preserves_reversed_ledger_history() -> None:
    headers = auth_headers()
    scenarios = client.get('/api/scenarios', headers=headers).json()
    seeded = next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')
    scenario_id = seeded['id']

    first = client.post(f'/api/scenarios/{scenario_id}/forecast/run', headers=headers)
    assert first.status_code == 200
    first_count = len(first.json()['created_line_items'])

    second = client.post(f'/api/scenarios/{scenario_id}/forecast/run', headers=headers)
    assert second.status_code == 200
    assert len(second.json()['created_line_items']) == first_count

    reversed_rows = db.fetch_all(
        '''
        SELECT id
        FROM planning_ledger
        WHERE scenario_id = ? AND source = 'forecast' AND reversed_at IS NOT NULL
        ''',
        (scenario_id,),
    )
    active_rows = client.get(f'/api/scenarios/{scenario_id}/line-items', headers=headers).json()

    assert len(reversed_rows) >= first_count
    assert all(row['source'] != 'forecast' or row['id'] not in {item['id'] for item in reversed_rows} for row in active_rows)


def test_summary_report_has_expected_shape() -> None:
    headers = auth_headers()
    scenarios = client.get('/api/scenarios', headers=headers).json()
    seeded = next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')
    scenario_id = seeded['id']
    response = client.get(f'/api/reports/summary?scenario_id={scenario_id}', headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload['scenario_id'] == scenario_id
    assert 'SCI' in payload['by_department']
    assert 'TUITION' in payload['by_account']
