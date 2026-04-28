from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_collaboration_layer_hardening.db'
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
    response = client.get('/api/bootstrap', headers=headers)
    assert response.status_code == 200
    return int(response.json()['activeScenario']['id'])


def test_collaboration_layer_hardening_status_and_run() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/collaboration/hardening/status', headers=headers)
    assert status.status_code == 200
    status_payload = status.json()
    assert status_payload['batch'] == 'B104'
    assert status_payload['complete'] is True
    assert all(status_payload['checks'].values())

    run = client.post('/api/collaboration/hardening/run', headers=headers, json={'run_key': 'b104-regression', 'scenario_id': sid})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['chat_notifications_ready'] is True
    assert payload['checks']['user_presence_ready'] is True
    assert payload['checks']['comments_ready'] is True
    assert payload['checks']['attachments_ready'] is True
    assert payload['checks']['evidence_links_ready'] is True
    assert payload['checks']['mentions_ready'] is True
    assert payload['checks']['task_notifications_ready'] is True
    assert payload['checks']['delayed_delivery_ready'] is True

    artifacts = payload['artifacts']
    assert artifacts['chat_message']['delivery_status'] == 'pending_delivery'
    assert artifacts['delivered_message']['delivery_status'] == 'delivered'
    assert artifacts['recipient_summary']['unread_count'] >= 1
    assert artifacts['comment']['comment_text'].startswith('@')
    assert artifacts['attachment']['retention_until'] == '2034-06-30'
    assert artifacts['linked_evidence']['comments']
    assert artifacts['linked_evidence']['attachments']
    assert artifacts['mentions']
    assert artifacts['task_notification']['status'] == 'unread'
    assert artifacts['task_queue']

    rows = client.get('/api/collaboration/hardening/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1


def test_chat_presence_and_notification_endpoints_expose_collaboration_state() -> None:
    headers = admin_headers()

    presence = client.get('/api/chat/presence', headers=headers)
    assert presence.status_code == 200
    presence_payload = presence.json()
    assert presence_payload['current_user']['status'] == 'online'
    assert presence_payload['current_user']['last_seen_at']

    mentions = client.get('/api/chat/mentions', headers=headers)
    assert mentions.status_code == 200
    assert mentions.json()['count'] == 0

    task_queue = client.get('/api/chat/task-notifications', headers=headers)
    assert task_queue.status_code == 200
    assert task_queue.json()['status'] is None
    assert task_queue.json()['count'] == 0
