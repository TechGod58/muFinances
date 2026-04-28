from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_production_reporting_polish.db'
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


def create_book(headers: dict[str, str], sid: int) -> tuple[dict[str, object], dict[str, object]]:
    report = client.post(
        '/api/reporting/reports',
        headers=headers,
        json={'name': 'B49 Statement', 'report_type': 'ledger_matrix', 'row_dimension': 'department_code', 'column_dimension': 'account_code', 'filters': {}},
    ).json()
    layout = client.post(
        '/api/reporting/layouts',
        headers=headers,
        json={'scenario_id': sid, 'report_definition_id': report['id'], 'name': 'B49 Pixel Layout', 'layout': {'unit': 'px', 'grid': {'x': 72, 'y': 72, 'width': 468}}},
    ).json()
    chart = client.post(
        '/api/reporting/charts',
        headers=headers,
        json={'scenario_id': sid, 'name': 'B49 Chart', 'chart_type': 'bar', 'dataset_type': 'period_range', 'config': {'dimension': 'department_code', 'period_start': '2026-07', 'period_end': '2026-12'}},
    ).json()
    book = client.post(
        '/api/reporting/report-books',
        headers=headers,
        json={'scenario_id': sid, 'name': 'B49 Binder', 'layout_id': layout['id'], 'period_start': '2026-07', 'period_end': '2026-12', 'report_definition_ids': [report['id']], 'chart_ids': [chart['id']]},
    ).json()
    return book, chart


def test_production_reporting_polish_end_to_end() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    book, chart = create_book(headers, sid)

    profile = client.post(
        '/api/reporting/pagination-profiles',
        headers=headers,
        json={'scenario_id': sid, 'name': 'Board PDF', 'page_size': 'Letter', 'orientation': 'landscape', 'margin_top': 0.5, 'margin_right': 0.5, 'margin_bottom': 0.5, 'margin_left': 0.5, 'rows_per_page': 28},
    )
    assert profile.status_code == 200
    assert profile.json()['orientation'] == 'landscape'

    footnote = client.post(
        '/api/reporting/footnotes',
        headers=headers,
        json={'scenario_id': sid, 'target_type': 'financial_statement', 'marker': 'A', 'footnote_text': 'Rounded to whole dollars.', 'display_order': 1},
    )
    assert footnote.status_code == 200
    assert footnote.json()['marker'] == 'A'

    page_break = client.post(
        '/api/reporting/page-breaks',
        headers=headers,
        json={'report_book_id': book['id'], 'section_key': 'variance', 'page_number': 2, 'break_before': True},
    )
    assert page_break.status_code == 200
    assert page_break.json()['break_before'] is True

    formatted = client.post(
        f"/api/reporting/charts/{chart['id']}/format",
        headers=headers,
        json={'format': {'palette': ['#7df0c6', '#f6c453'], 'show_data_labels': True, 'currency_axis': True}},
    )
    assert formatted.status_code == 200
    assert formatted.json()['config']['format']['show_data_labels'] is True

    recurring = client.post(
        '/api/reporting/recurring-packages',
        headers=headers,
        json={'scenario_id': sid, 'book_id': book['id'], 'schedule_cron': '0 8 1 * *', 'destination': 'board-release'},
    )
    assert recurring.status_code == 200

    requested = client.post(f"/api/reporting/recurring-packages/{recurring.json()['id']}/release-request", headers=headers)
    assert requested.status_code == 200
    assert requested.json()['status'] == 'pending_approval'

    approved = client.post(f"/api/reporting/recurring-packages/{recurring.json()['id']}/approve-release", headers=headers, json={'note': 'Approved for board release.'})
    assert approved.status_code == 200
    assert approved.json()['status'] == 'approved'

    released = client.post(f"/api/reporting/recurring-packages/{recurring.json()['id']}/release", headers=headers, json={'note': 'Released.'})
    assert released.status_code == 200
    assert released.json()['status'] == 'released'

    pixel = client.get(f'/api/reporting/pixel-financial-statement?scenario_id={sid}', headers=headers)
    assert pixel.status_code == 200
    assert pixel.json()['page']['orientation'] == 'landscape'
    assert pixel.json()['rows'][0]['x'] == 72
    assert pixel.json()['footnotes'][0]['footnote_text'] == 'Rounded to whole dollars.'

    workspace = client.get(f'/api/reporting/production-polish/workspace?scenario_id={sid}', headers=headers)
    assert workspace.status_code == 200
    payload = workspace.json()
    assert payload['status']['batch'] == 'B49'
    assert payload['page_breaks']
    assert payload['release_reviews'][0]['status'] == 'released'


def test_production_reporting_status_and_migration_are_registered() -> None:
    headers = admin_headers()
    status = client.get('/api/reporting/production-polish/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B49'
    assert payload['complete'] is True
    assert payload['checks']['pdf_pagination_ready'] is True

    migrations = client.get('/api/foundation/migrations', headers=headers)
    assert migrations.status_code == 200
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0050_production_reporting_polish' in keys


def test_production_reporting_ui_surface_exists() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')

    assert 'id="pixelStatementTable"' in index
    assert 'id="paginationProfileTable"' in index
    assert 'id="reportFootnoteTable"' in index
    assert 'id="boardReleaseTable"' in index
    assert 'id="releaseBoardPackageButton"' in index
    assert '/api/reporting/production-polish/workspace' in app_js
    assert 'handleBoardPackageReleaseFlow' in app_js
