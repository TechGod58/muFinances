from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_financial_close_certification.db'
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


def test_financial_close_certification_builds_close_packet_and_signoff() -> None:
    headers = admin_headers()

    status = client.get('/api/close/certification/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B96'
    assert status.json()['complete'] is True

    run = client.post('/api/close/certification/run', headers=headers, json={})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['month_end_calendar_created'] is True
    assert payload['checks']['close_task_dependencies_created'] is True
    assert payload['checks']['dependency_task_completed_before_review'] is True
    assert payload['checks']['reconciliation_prepared_submitted_reviewed'] is True
    assert payload['checks']['evidence_attachments_retained'] is True
    assert payload['checks']['entity_confirmation_completed'] is True
    assert payload['checks']['audit_packet_generated'] is True
    assert payload['checks']['close_signoff_locked_period'] is True
    assert payload['close_signoff']['signoff_type'] == 'month_end_close'
    assert payload['close_signoff']['lock_state'] == 'locked'
    assert payload['artifacts']['reconciliation']['status'] == 'reviewed'
    assert payload['artifacts']['consolidation']['audit_packet']['status'] == 'sealed'

    rows = client.get('/api/close/certification/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
