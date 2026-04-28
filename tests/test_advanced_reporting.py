from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_advanced_reporting.db'
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


def test_advanced_reporting_package_outputs() -> None:
    headers = admin_headers()
    sid = scenario_id()

    rollups = client.get(f'/api/reporting/account-rollups?scenario_id={sid}&period_start=2026-07&period_end=2026-12', headers=headers)
    assert rollups.status_code == 200
    assert any(row['group'] == 'Revenue' for row in rollups.json()['rollups'])

    period = client.get(f'/api/reporting/period-range?scenario_id={sid}&period_start=2026-07&period_end=2026-12&dimension=department_code', headers=headers)
    assert period.status_code == 200
    assert period.json()['dimension'] == 'department_code'

    variance = client.get(f'/api/reporting/actual-budget-forecast-variance?scenario_id={sid}', headers=headers)
    assert variance.status_code == 200
    assert variance.json()['rows']

    for path in ['balance-sheet', 'cash-flow', 'fund-report', 'grant-report', 'departmental-pl']:
        response = client.get(f'/api/reporting/{path}?scenario_id={sid}', headers=headers)
        assert response.status_code == 200

    package = client.post(
        '/api/reporting/board-packages',
        headers=headers,
        json={'scenario_id': sid, 'package_name': 'FY27 Board Package', 'period_start': '2026-07', 'period_end': '2026-12'},
    )
    assert package.status_code == 200
    payload = package.json()
    assert payload['status'] == 'assembled'
    assert 'cash_flow' in payload['contents']
    assert 'departmental_pl' in payload['contents']


def test_reporting_status_reports_b16_complete() -> None:
    response = client.get('/api/reporting/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] in {'B16', 'B17', 'B18'}
    assert payload['complete'] is True
    assert payload['checks']['board_package_ready'] is True
