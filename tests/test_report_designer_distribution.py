from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_report_designer_distribution.db'
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


def scenario_id(headers: dict[str, str]) -> int:
    scenarios = client.get('/api/scenarios', headers=headers).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def test_report_designer_layout_book_charts_bursting_and_recurring_package() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    report = client.post(
        '/api/reporting/reports',
        headers=headers,
        json={
            'name': 'B29 Designer Report',
            'report_type': 'ledger_matrix',
            'row_dimension': 'department_code',
            'column_dimension': 'account_code',
            'filters': {},
        },
    )
    assert report.status_code == 200

    layout = client.post(
        '/api/reporting/layouts',
        headers=headers,
        json={
            'scenario_id': sid,
            'report_definition_id': report.json()['id'],
            'name': 'Controller landscape layout',
            'layout': {'orientation': 'landscape', 'sections': ['summary', 'variance'], 'density': 'compact'},
        },
    )
    assert layout.status_code == 200
    assert layout.json()['layout']['orientation'] == 'landscape'

    chart = client.post(
        '/api/reporting/charts',
        headers=headers,
        json={
            'scenario_id': sid,
            'name': 'Department trend',
            'chart_type': 'bar',
            'dataset_type': 'period_range',
            'config': {'dimension': 'department_code', 'period_start': '2026-07', 'period_end': '2026-12'},
        },
    )
    assert chart.status_code == 200
    assert chart.json()['dataset']['dimension'] == 'department_code'

    book = client.post(
        '/api/reporting/report-books',
        headers=headers,
        json={
            'scenario_id': sid,
            'name': 'B29 Monthly Book',
            'layout_id': layout.json()['id'],
            'period_start': '2026-07',
            'period_end': '2026-12',
            'report_definition_ids': [report.json()['id']],
            'chart_ids': [chart.json()['id']],
        },
    )
    assert book.status_code == 200
    assert book.json()['status'] == 'assembled'
    assert book.json()['contents']['reports'][0]['definition']['name'] == 'B29 Designer Report'
    assert book.json()['contents']['charts'][0]['name'] == 'Department trend'

    burst = client.post(
        '/api/reporting/burst-rules',
        headers=headers,
        json={
            'scenario_id': sid,
            'book_id': book.json()['id'],
            'burst_dimension': 'department_code',
            'recipients': ['budget.office@example.edu', 'science.dean@example.edu'],
            'export_format': 'pdf',
        },
    )
    assert burst.status_code == 200
    assert burst.json()['active'] is True
    assert len(burst.json()['recipients']) == 2

    recurring = client.post(
        '/api/reporting/recurring-packages',
        headers=headers,
        json={
            'scenario_id': sid,
            'book_id': book.json()['id'],
            'schedule_cron': '0 8 1 * *',
            'destination': 'controller-email-package',
            'next_run_at': '2026-05-01T08:00:00Z',
        },
    )
    assert recurring.status_code == 200
    assert recurring.json()['status'] == 'scheduled'

    run = client.post(f"/api/reporting/recurring-packages/{recurring.json()['id']}/run", headers=headers)
    assert run.status_code == 200
    assert run.json()['status'] == 'complete'
    assert run.json()['recipient_count'] == 2
    assert run.json()['artifact_id'] is not None
    assert run.json()['detail']['book']['name'] == 'B29 Monthly Book'


def test_report_designer_distribution_status_reports_b29_complete() -> None:
    response = client.get('/api/reporting/designer-distribution/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B29'
    assert payload['complete'] is True
    assert payload['checks']['saved_layouts_ready'] is True
    assert payload['checks']['report_books_ready'] is True
    assert payload['checks']['charts_ready'] is True
    assert payload['checks']['bursting_ready'] is True
    assert payload['checks']['recurring_packages_ready'] is True
