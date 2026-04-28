from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_production_reporting_renderer.db'
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


def test_pdf_and_email_artifacts_use_renderer_not_placeholder() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    package = client.post(
        '/api/reporting/board-packages',
        headers=headers,
        json={'scenario_id': sid, 'package_name': 'B54 Board Package', 'period_start': '2026-07', 'period_end': '2026-12'},
    )
    assert package.status_code == 200

    book = client.post(
        '/api/reporting/report-books',
        headers=headers,
        json={'scenario_id': sid, 'name': 'B54 Binder', 'period_start': '2026-07', 'period_end': '2026-12', 'report_definition_ids': [], 'chart_ids': []},
    )
    assert book.status_code == 200
    page_break = client.post(
        '/api/reporting/page-breaks',
        headers=headers,
        json={'report_book_id': book.json()['id'], 'section_key': 'variance', 'page_number': 2, 'break_before': True},
    )
    assert page_break.status_code == 200

    pdf = client.post(
        '/api/reporting/artifacts',
        headers=headers,
        json={'scenario_id': sid, 'artifact_type': 'pdf', 'file_name': 'b54-board', 'package_id': package.json()['id']},
    )
    assert pdf.status_code == 200
    payload = pdf.json()
    assert payload['content_type'] == 'application/pdf'
    assert payload['metadata']['renderer'] == 'mu-html-pdf-v1'
    assert payload['metadata']['page_count'] >= 2
    assert payload['metadata']['visual_hash']
    body = Path(payload['storage_path']).read_bytes()
    assert body.startswith(b'%PDF-1.4')
    assert b'placeholder' not in body.lower()
    assert b'B54 Board Package' in body

    email = client.post(
        '/api/reporting/artifacts',
        headers=headers,
        json={'scenario_id': sid, 'artifact_type': 'email', 'file_name': 'b54-board-email', 'package_id': package.json()['id']},
    )
    assert email.status_code == 200
    email_body = Path(email.json()['storage_path']).read_text(encoding='utf-8')
    assert 'Content-Type: multipart/alternative' in email_body
    assert '<!doctype html>' in email_body
    assert 'B54 Board Package' in email_body


def test_b54_migration_registered() -> None:
    headers = admin_headers()
    migrations = client.get('/api/foundation/migrations', headers=headers)
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0055_production_reporting_renderer' in keys
