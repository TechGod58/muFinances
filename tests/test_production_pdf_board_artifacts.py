from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_production_pdf_board_artifacts.db'
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


def test_b69_production_pdf_board_artifact_download_and_validation() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    chart = client.post(
        '/api/reporting/charts',
        headers=headers,
        json={
            'scenario_id': sid,
            'name': 'B69 Board Chart',
            'chart_type': 'bar',
            'dataset_type': 'period_range',
            'config': {'dimension': 'department_code', 'period_start': '2026-07', 'period_end': '2026-12'},
        },
    )
    assert chart.status_code == 200
    footnote = client.post(
        '/api/reporting/footnotes',
        headers=headers,
        json={'scenario_id': sid, 'target_type': 'board_package', 'marker': 'A', 'footnote_text': 'Board packet dollars are rounded.', 'display_order': 1},
    )
    assert footnote.status_code == 200
    package = client.post(
        '/api/reporting/board-packages',
        headers=headers,
        json={'scenario_id': sid, 'package_name': 'B69 Board Packet', 'period_start': '2026-07', 'period_end': '2026-12'},
    )
    assert package.status_code == 200

    pdf = client.post(
        '/api/reporting/artifacts',
        headers=headers,
        json={'scenario_id': sid, 'artifact_type': 'pdf', 'file_name': 'b69-board-packet', 'package_id': package.json()['id']},
    )
    assert pdf.status_code == 200
    payload = pdf.json()
    assert payload['content_type'] == 'application/pdf'
    assert payload['download_url'] == f"/api/reporting/artifacts/{payload['id']}/download"
    assert payload['metadata']['validation_status'] == 'passed'
    assert payload['metadata']['page_count'] >= 2
    assert payload['metadata']['chart_image_embeds'] >= 1

    body = Path(payload['storage_path']).read_bytes()
    assert body.startswith(b'%PDF-1.4')
    assert body.rstrip().endswith(b'%%EOF')
    assert b'B69 Board Packet' in body
    assert b'Chart:' in body
    assert b'Footnotes' in body
    assert body.count(b'/Type /Page ') == payload['metadata']['page_count']

    download = client.get(payload['download_url'], headers=headers)
    assert download.status_code == 200
    assert download.headers['content-type'].startswith('application/pdf')
    assert download.content == body

    validations = client.get(f'/api/reporting/artifact-validations?artifact_id={payload["id"]}', headers=headers)
    assert validations.status_code == 200
    validation = validations.json()['validations'][0]
    assert validation['status'] == 'passed'
    assert validation['checks']['pdf_header'] is True
    assert validation['checks']['pdf_page_count_matches_metadata'] is True


def test_b69_status_migration_and_ui_surface() -> None:
    headers = admin_headers()
    status = client.get('/api/reporting/production-pdf/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B69'
    assert payload['complete'] is True
    assert payload['checks']['downloadable_artifacts_ready'] is True

    migrations = client.get('/api/foundation/migrations', headers=headers)
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0069_production_pdf_board_artifact_completion' in keys

    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')
    assert 'id="productionPdfArtifactTable"' in index
    assert 'id="exportValidationTable"' in index
    assert '/api/reporting/production-pdf/workspace' in app_js
    assert 'downloadArtifact' in app_js
