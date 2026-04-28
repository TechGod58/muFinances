from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_reporting_pixel_polish_certification.db'
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


def test_reporting_pixel_polish_certification_builds_controller_grade_outputs() -> None:
    headers = admin_headers()

    status = client.get('/api/reporting/pixel-polish-certification/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B98'
    assert status.json()['complete'] is True

    run = client.post('/api/reporting/pixel-polish-certification/run', headers=headers, json={})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['income_statement_has_sections'] is True
    assert payload['checks']['balance_sheet_has_net_position'] is True
    assert payload['checks']['cash_flow_has_net_cash_flow'] is True
    assert payload['checks']['fund_report_has_rows'] is True
    assert payload['checks']['grant_report_has_rows'] is True
    assert payload['checks']['departmental_pl_has_rows'] is True
    assert payload['checks']['pixel_layout_and_pagination_ready'] is True
    assert payload['checks']['footnotes_ready'] is True
    assert payload['checks']['charts_rendered_and_exported'] is True
    assert payload['checks']['board_package_pdf_ready'] is True
    assert payload['artifacts']['pdf_artifact']['status'] == 'ready'
    assert payload['artifacts']['chart_render']['render_format'] == 'svg'

    rows = client.get('/api/reporting/pixel-polish-certification/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
