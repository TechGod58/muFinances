from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_export_distribution.db'
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


def test_export_distribution_artifacts_snapshots_extracts_and_manifest() -> None:
    headers = admin_headers()
    sid = scenario_id()
    report = client.post(
        '/api/reporting/reports',
        headers=headers,
        json={
            'name': 'B17 Distribution Report',
            'report_type': 'ledger_matrix',
            'row_dimension': 'department_code',
            'column_dimension': 'account_code',
            'filters': {},
        },
    )
    assert report.status_code == 200
    report_id = report.json()['id']

    package = client.post(
        '/api/reporting/board-packages',
        headers=headers,
        json={'scenario_id': sid, 'package_name': 'B17 Board Package', 'period_start': '2026-07', 'period_end': '2026-12'},
    )
    assert package.status_code == 200
    package_id = package.json()['id']

    excel = client.post(
        '/api/reporting/artifacts',
        headers=headers,
        json={'scenario_id': sid, 'artifact_type': 'excel', 'file_name': 'board-package', 'package_id': package_id, 'retention_until': '2033-06-30'},
    )
    assert excel.status_code == 200
    assert excel.json()['artifact_type'] == 'excel'
    assert 'spreadsheetml' in excel.json()['content_type']
    assert excel.json()['status'] == 'ready'

    pdf = client.post(
        '/api/reporting/artifacts',
        headers=headers,
        json={'scenario_id': sid, 'artifact_type': 'pdf', 'file_name': 'variance-report', 'report_definition_id': report_id},
    )
    assert pdf.status_code == 200
    assert pdf.json()['content_type'] == 'application/pdf'

    snapshot = client.post(
        '/api/reporting/snapshots',
        headers=headers,
        json={'scenario_id': sid, 'snapshot_type': 'board_package', 'retention_until': '2033-06-30'},
    )
    assert snapshot.status_code == 200
    assert snapshot.json()['retention_until'] == '2033-06-30'
    assert snapshot.json()['payload']['bi_api_manifest']['schema_version'] == '2026.04.b17'

    scheduled = client.post(
        '/api/reporting/exports',
        headers=headers,
        json={
            'report_definition_id': report_id,
            'scenario_id': sid,
            'export_format': 'xlsx',
            'schedule_cron': '0 7 * * 1',
            'destination': 'bi-api-drop',
        },
    )
    assert scheduled.status_code == 200

    extract = client.post(
        '/api/reporting/scheduled-extract-runs',
        headers=headers,
        json={'scenario_id': sid, 'export_id': scheduled.json()['id'], 'destination': 'bi-api-drop'},
    )
    assert extract.status_code == 200
    assert extract.json()['status'] == 'complete'
    assert extract.json()['row_count'] > 0
    assert extract.json()['artifact_id'] is not None

    manifest = client.get(f'/api/reporting/bi-api-manifest?scenario_id={sid}', headers=headers)
    assert manifest.status_code == 200
    assert manifest.json()['schema_version'] == '2026.04.b17'
    assert manifest.json()['row_count'] > 0
    assert any(endpoint['name'] == 'export_artifacts' for endpoint in manifest.json()['endpoints'])


def test_reporting_status_reports_b17_complete() -> None:
    response = client.get('/api/reporting/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] in {'B17', 'B18'}
    assert payload['complete'] is True
    assert payload['checks']['excel_export_ready'] is True
    assert payload['checks']['scheduled_extract_history_ready'] is True
