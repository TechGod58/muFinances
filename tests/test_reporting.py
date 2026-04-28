from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_reporting.db'
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


def test_report_builder_dashboard_statement_variance_and_exports() -> None:
    headers = admin_headers()
    sid = scenario_id()
    report = client.post(
        '/api/reporting/reports',
        headers=headers,
        json={
            'name': 'Department by Account',
            'report_type': 'ledger_matrix',
            'row_dimension': 'department_code',
            'column_dimension': 'account_code',
            'filters': {},
        },
    )
    assert report.status_code == 200
    report_id = report.json()['id']

    run = client.get(f'/api/reporting/reports/{report_id}/run?scenario_id={sid}', headers=headers)
    assert run.status_code == 200
    assert any(row['row'] == 'SCI' and row['column'] == 'TUITION' for row in run.json()['rows'])

    widget = client.post(
        '/api/reporting/widgets',
        headers=headers,
        json={'name': 'Net Position', 'widget_type': 'metric', 'metric_key': 'net_total', 'scenario_id': sid, 'config': {}},
    )
    assert widget.status_code == 200
    summary = client.get(f'/api/reports/summary?scenario_id={sid}', headers=headers)
    assert widget.json()['value'] == summary.json()['net_total']

    statement = client.get(f'/api/reporting/financial-statement?scenario_id={sid}', headers=headers)
    assert statement.status_code == 200
    assert statement.json()['sections'][-1]['label'] == 'Change in net position'

    clone = client.post(
        f'/api/scenario-engine/scenarios/{sid}/clone',
        headers=headers,
        json={'name': 'FY27 Reporting Compare', 'version': 'compare'},
    )
    assert clone.status_code == 200
    variance = client.get(
        f"/api/reporting/variance?base_scenario_id={sid}&compare_scenario_id={clone.json()['id']}",
        headers=headers,
    )
    assert variance.status_code == 200
    assert variance.json()['rows']

    export = client.post(
        '/api/reporting/exports',
        headers=headers,
        json={
            'report_definition_id': report_id,
            'scenario_id': sid,
            'export_format': 'json',
            'schedule_cron': '0 7 * * 1',
            'destination': 'board-pack',
        },
    )
    assert export.status_code == 200
    assert export.json()['status'] == 'scheduled'


def test_reporting_status_reports_b07_complete() -> None:
    response = client.get('/api/reporting/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] in {'B07', 'B16', 'B17', 'B18'}
    assert payload['complete'] is True
    assert payload['checks']['scheduled_exports_ready'] is True
