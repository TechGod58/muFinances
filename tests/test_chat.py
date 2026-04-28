from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_chat.db'
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


def create_planner(headers: dict[str, str]) -> dict[str, object]:
    response = client.post(
        '/api/security/users',
        headers=headers,
        json={
            'email': 'planner.chat@mufinances.local',
            'display_name': 'Planner Chat',
            'password': 'PlannerChat!3200',
            'role_keys': ['department.planner'],
        },
    )
    assert response.status_code == 200
    return response.json()


def planner_headers() -> dict[str, str]:
    login = client.post('/api/auth/login', json={'email': 'planner.chat@mufinances.local', 'password': 'PlannerChat!3200'})
    assert login.status_code == 200
    headers = {'Authorization': f"Bearer {login.json()['token']}"}
    change = client.post(
        '/api/auth/password',
        headers=headers,
        json={'current_password': 'PlannerChat!3200', 'new_password': 'PlannerChat!3200Next'},
    )
    assert change.status_code == 200
    login = client.post('/api/auth/login', json={'email': 'planner.chat@mufinances.local', 'password': 'PlannerChat!3200Next'})
    assert login.status_code == 200
    return {'Authorization': f"Bearer {login.json()['token']}"}


def test_direct_chat_creates_unread_message_and_notification() -> None:
    admin = admin_headers()
    planner = create_planner(admin)

    users = client.get('/api/chat/users', headers=admin)
    assert users.status_code == 200
    assert any(user['id'] == planner['id'] for user in users.json()['users'])

    sent = client.post(
        '/api/chat/messages',
        headers=admin,
        json={'recipient_user_id': planner['id'], 'body': 'Budget review is ready.'},
    )
    assert sent.status_code == 200
    assert sent.json()['direction'] == 'sent'
    assert sent.json()['notification_id']

    recipient_headers = planner_headers()
    summary = client.get('/api/chat/summary', headers=recipient_headers)
    assert summary.status_code == 200
    assert summary.json()['unread_count'] == 1
    assert summary.json()['latest_unread']['body'] == 'Budget review is ready.'

    notifications = client.get('/api/ux/notifications', headers=recipient_headers)
    assert notifications.status_code == 200
    assert any(row['notification_type'] == 'chat' and row['status'] == 'unread' for row in notifications.json()['notifications'])

    messages = client.get(f"/api/chat/messages?peer_user_id={sent.json()['sender_user_id']}", headers=recipient_headers)
    assert messages.status_code == 200
    assert messages.json()['messages'][0]['direction'] == 'received'

    read = client.post('/api/chat/messages/read', headers=recipient_headers, json={'peer_user_id': sent.json()['sender_user_id']})
    assert read.status_code == 200
    assert read.json()['unread_count'] == 0

    notifications = client.get('/api/ux/notifications', headers=recipient_headers)
    assert all(row['status'] == 'read' for row in notifications.json()['notifications'] if row['notification_type'] == 'chat')


def test_chat_ui_assets_are_registered() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    assert 'id="chatButton"' in index
    assert 'id="chatSatellite"' in index
    assert '/static/js/chat-satellite.js?v=4' in index
    assert (PROJECT_ROOT / 'static' / 'chat-window.html').exists()
    assert (PROJECT_ROOT / 'static' / 'js' / 'chat-window.js').exists()
