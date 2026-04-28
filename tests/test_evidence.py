from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_evidence.db'
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


def test_comments_attachments_and_packet_evidence_links() -> None:
    headers = admin_headers()
    sid = scenario_id()
    checklist = client.post(
        '/api/close/checklists',
        headers=headers,
        json={
            'scenario_id': sid,
            'period': '2026-08',
            'checklist_key': 'evidence-review',
            'title': 'Review evidence links',
            'owner': 'Controller',
            'due_date': '2026-09-05',
        },
    )
    assert checklist.status_code == 200
    close_id = str(checklist.json()['id'])

    comment = client.post(
        '/api/evidence/comments',
        headers=headers,
        json={
            'entity_type': 'close_task',
            'entity_id': close_id,
            'comment_text': 'Evidence reviewed by controller.',
            'visibility': 'audit',
        },
    )
    assert comment.status_code == 200
    assert comment.json()['comment_text'].startswith('Evidence reviewed')

    attachment = client.post(
        '/api/evidence/attachments',
        headers=headers,
        json={
            'entity_type': 'close_task',
            'entity_id': close_id,
            'file_name': 'close-evidence.pdf',
            'storage_path': 'evidence/close-evidence.pdf',
            'content_type': 'application/pdf',
            'size_bytes': 2048,
            'retention_until': '2033-06-30',
            'metadata': {'source': 'controller'},
        },
    )
    assert attachment.status_code == 200
    assert attachment.json()['metadata']['source'] == 'controller'

    run = client.post(
        '/api/close/consolidation-runs',
        headers=headers,
        json={'scenario_id': sid, 'period': '2026-08'},
    )
    assert run.status_code == 200
    links = run.json()['audit_packet']['contents']['evidence_links']
    assert links['close_task_comments'][0]['entity_id'] == close_id
    assert links['close_task_attachments'][0]['file_name'] == 'close-evidence.pdf'


def test_evidence_status_reports_b14_complete() -> None:
    response = client.get('/api/evidence/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B14'
    assert payload['complete'] is True
    assert payload['checks']['audit_packet_evidence_links_ready'] is True
