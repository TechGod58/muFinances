from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_ux_productivity.db'
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


def test_ux_productivity_profile_notifications_grid_bulk_and_reviews() -> None:
    headers = admin_headers()
    sid = scenario_id()

    profile = client.post(
        '/api/ux/profile',
        headers=headers,
        json={'display_name': 'Admin User', 'default_scenario_id': sid, 'default_period': '2026-08', 'preferences': {'compact_grid': True}},
    )
    assert profile.status_code == 200
    assert profile.json()['default_period'] == '2026-08'

    notification = client.post(
        '/api/ux/notifications',
        headers=headers,
        json={'scenario_id': sid, 'notification_type': 'review', 'title': 'Review submissions', 'message': 'SCI is ready.', 'severity': 'info', 'link': '#operating-budget'},
    )
    assert notification.status_code == 200
    assert notification.json()['status'] == 'unread'
    read = client.post(f"/api/ux/notifications/{notification.json()['id']}/read", headers=headers, json={})
    assert read.status_code == 200
    assert read.json()['status'] == 'read'

    invalid = client.post(
        '/api/ux/grids/validate',
        headers=headers,
        json={'scenario_id': sid, 'rows': [{'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': '', 'period': '2026-08', 'amount': 'bad'}]},
    )
    assert invalid.status_code == 200
    assert invalid.json()['valid'] is False

    bulk = client.post(
        '/api/ux/bulk-paste',
        headers=headers,
        json={'scenario_id': sid, 'paste_text': 'department_code\tfund_code\taccount_code\tperiod\tamount\tnotes\nART\tGEN\tSUPPLIES\t2026-08\t-1250\tPaste row'},
    )
    assert bulk.status_code == 200
    assert bulk.json()['accepted_rows'] == 1
    assert bulk.json()['rejected_rows'] == 0

    missing = client.get(f'/api/ux/missing-submissions?scenario_id={sid}', headers=headers)
    assert missing.status_code == 200
    assert any(row['department_code'] == 'SCI' for row in missing.json()['rows'])

    comparison = client.get(f'/api/ux/department-comparison?scenario_id={sid}', headers=headers)
    assert comparison.status_code == 200
    assert comparison.json()['rows']

    bootstrap = client.get(f'/api/ux/bootstrap?scenario_id={sid}', headers=headers)
    assert bootstrap.status_code == 200
    assert bootstrap.json()['profile']['default_period'] == '2026-08'


def test_ux_status_reports_b22_complete() -> None:
    response = client.get('/api/ux/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B22'
    assert payload['complete'] is True
    assert payload['checks']['bulk_paste_import_ui_ready'] is True
    assert payload['checks']['department_comparison_ready'] is True
