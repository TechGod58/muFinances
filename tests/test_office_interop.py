from __future__ import annotations

import base64
import os
import sys
import zipfile
from io import BytesIO
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_office_interop.db'
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


def test_excel_template_roundtrip_and_workbook_package() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    template = client.post(f'/api/office/excel-template?scenario_id={sid}', headers=headers, json={})
    assert template.status_code == 200
    payload = template.json()
    assert payload['workbook_type'] == 'excel_template'
    assert payload['file_name'].endswith('.xlsx')
    assert payload['metadata']['roundtrip_sheet'] == 'LedgerInput'

    raw = base64.b64decode(payload['workbook_base64'])
    with zipfile.ZipFile(BytesIO(raw), 'r') as archive:
        assert 'xl/workbook.xml' in archive.namelist()
        assert 'xl/worksheets/sheet2.xml' in archive.namelist()

    imported = client.post(
        '/api/office/excel-import',
        headers=headers,
        json={'scenario_id': sid, 'workbook_key': payload['workbook_key'], 'workbook_base64': payload['workbook_base64'], 'sheet_name': 'LedgerInput'},
    )
    assert imported.status_code == 200
    assert imported.json()['accepted_rows'] == 1
    assert imported.json()['rejected_rows'] == 0
    assert imported.json()['ledger_entries'][0]['source'] == 'excel_roundtrip'

    package = client.post(f'/api/office/workbook-package?scenario_id={sid}', headers=headers, json={})
    assert package.status_code == 200
    assert package.json()['workbook_type'] == 'workbook_package'
    assert 'FinancialStatement' in package.json()['metadata']['sheets']
    assert 'Ledger' in package.json()['metadata']['sheets']

    workbooks = client.get(f'/api/office/workbooks?scenario_id={sid}', headers=headers)
    assert workbooks.status_code == 200
    assert workbooks.json()['count'] == 2

    imports = client.get(f'/api/office/roundtrip-imports?scenario_id={sid}', headers=headers)
    assert imports.status_code == 200
    assert imports.json()['imports'][0]['accepted_rows'] == 1


def test_office_status_reports_b26_complete() -> None:
    response = client.get('/api/office/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B26'
    assert payload['complete'] is True
    assert payload['checks']['excel_template_export_ready'] is True
    assert payload['checks']['excel_template_import_ready'] is True
    assert payload['checks']['round_trip_editing_ready'] is True
    assert payload['checks']['workbook_package_generation_ready'] is True


def test_excel_native_workspace_refresh_publish_comments_and_deck_refresh() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/office/native-status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B37'
    assert status.json()['checks']['named_ranges_ready'] is True
    assert status.json()['checks']['powerpoint_refresh_ready'] is True

    template = client.post(f'/api/office/excel-template?scenario_id={sid}', headers=headers, json={})
    assert template.status_code == 200
    workbook_key = template.json()['workbook_key']
    assert 'LedgerInput.Amount' in template.json()['metadata']['named_ranges']
    assert 'Variance.ActualVsBudget' in template.json()['metadata']['variance_formulas']

    ranges = client.get(f'/api/office/named-ranges?scenario_id={sid}', headers=headers)
    assert ranges.status_code == 200
    assert any(item['range_name'] == 'LedgerInput.Amount' for item in ranges.json()['named_ranges'])
    assert any(item['protected'] == 1 for item in ranges.json()['named_ranges'])

    refresh = client.post(f'/api/office/workbooks/{workbook_key}/refresh', headers=headers, json={})
    assert refresh.status_code == 200
    assert refresh.json()['action_type'] == 'refresh'
    assert refresh.json()['status'] == 'refreshed'

    comment = client.post(
        '/api/office/cell-comments',
        headers=headers,
        json={'scenario_id': sid, 'workbook_key': workbook_key, 'sheet_name': 'LedgerInput', 'cell_ref': 'E2', 'comment_text': 'Explain variance before publishing.'},
    )
    assert comment.status_code == 200
    assert comment.json()['cell_ref'] == 'E2'

    publish = client.post(f'/api/office/workbooks/{workbook_key}/publish', headers=headers, json={})
    assert publish.status_code == 200
    assert publish.json()['status'] == 'published'

    deck = client.post(f'/api/office/powerpoint-refresh?scenario_id={sid}', headers=headers, json={})
    assert deck.status_code == 200
    assert deck.json()['workbook_type'] == 'powerpoint_deck'

    workspace = client.get(f'/api/office/native-workspace?scenario_id={sid}', headers=headers)
    assert workspace.status_code == 200
    assert workspace.json()['cell_comments'][0]['comment_text'].startswith('Explain variance')
    assert any(action['action_type'] == 'powerpoint_refresh' for action in workspace.json()['actions'])


def test_office_interop_ui_contract() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')

    assert 'id="officeInterop"' in index
    assert 'Excel and Office interop' in index
    assert 'id="createExcelTemplateButton"' in index
    assert 'id="refreshExcelWorkbookButton"' in index
    assert 'id="publishExcelWorkbookButton"' in index
    assert 'id="refreshPowerPointButton"' in index
    assert 'id="officeNamedRangeTable"' in index
    assert 'id="officeCellCommentTable"' in index
    assert 'id="importExcelRoundtripButton"' in index
    assert 'id="createWorkbookPackageButton"' in index
    assert 'id="excelRoundtripDialog"' in index
    assert 'handleExcelTemplateCreate' in app_js
    assert 'handleOfficeWorkbookRefresh' in app_js
    assert 'handleOfficeWorkbookPublish' in app_js
    assert 'handlePowerPointRefresh' in app_js
    assert 'handleExcelRoundtripImport' in app_js
    assert 'handleWorkbookPackageCreate' in app_js
