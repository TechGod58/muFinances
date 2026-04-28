from __future__ import annotations

import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_workflow_designer.db'
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


def admin_user(headers: dict[str, str]) -> dict[str, object]:
    response = client.get('/api/auth/me', headers=headers)
    assert response.status_code == 200
    return response.json()


def test_workflow_designer_chains_delegation_escalation_and_notifications() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    admin = admin_user(headers)
    now = datetime.now(UTC)

    delegation = client.post(
        '/api/workflow-designer/delegations',
        headers=headers,
        json={
            'from_user_id': admin['id'],
            'to_user_id': admin['id'],
            'starts_at': (now - timedelta(hours=1)).isoformat(),
            'ends_at': (now + timedelta(hours=1)).isoformat(),
            'reason': 'Controller coverage',
        },
    )
    assert delegation.status_code == 200
    assert delegation.json()['active'] is True

    template = client.post(
        '/api/workflow-designer/templates',
        headers=headers,
        json={
            'name': 'B30 Budget Approval Chain',
            'entity_type': 'budget_submission',
            'steps': [
                {
                    'step_key': 'department-review',
                    'label': 'Department review',
                    'approver_user_id': admin['id'],
                    'escalation_hours': 24,
                    'escalation_user_id': admin['id'],
                    'notification_template': 'Department review is ready.',
                },
                {
                    'step_key': 'budget-office-review',
                    'label': 'Budget office review',
                    'approver_role': 'budget.office',
                    'approver_user_id': admin['id'],
                    'escalation_hours': 0,
                    'escalation_user_id': admin['id'],
                    'notification_template': 'Budget office review is ready.',
                },
            ],
        },
    )
    assert template.status_code == 200
    assert len(template.json()['steps']) == 2

    instance = client.post(
        '/api/workflow-designer/instances',
        headers=headers,
        json={'template_id': template.json()['id'], 'scenario_id': sid, 'subject_type': 'budget_submission', 'subject_id': 'SCI-FY27'},
    )
    assert instance.status_code == 200
    assert instance.json()['status'] == 'active'
    first_task = instance.json()['tasks'][0]
    assert first_task['delegated_from_user_id'] == admin['id']

    decision = client.post(
        f"/api/workflow-designer/tasks/{first_task['id']}/decision",
        headers=headers,
        json={'decision': 'approved', 'note': 'Department review complete.'},
    )
    assert decision.status_code == 200
    assert decision.json()['current_step_key'] == 'budget-office-review'
    assert decision.json()['status'] == 'active'

    escalated = client.post(f'/api/workflow-designer/escalations/run?scenario_id={sid}', headers=headers, json={})
    assert escalated.status_code == 200
    assert escalated.json()['escalated'] == 1

    tasks = client.get(f'/api/workflow-designer/tasks?scenario_id={sid}&status=escalated', headers=headers)
    assert tasks.status_code == 200
    assert tasks.json()['count'] == 1

    notifications = client.get(f'/api/ux/notifications?scenario_id={sid}', headers=headers)
    assert notifications.status_code == 200
    assert any(row['notification_type'] == 'workflow' and 'Workflow escalation' in row['title'] for row in notifications.json()['notifications'])


def test_workflow_designer_status_reports_b30_complete() -> None:
    response = client.get('/api/workflow-designer/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] in {'B30', 'B45'}
    assert payload['complete'] is True
    assert payload['checks']['configurable_approval_chains_ready'] is True
    assert payload['checks']['escalations_ready'] is True
    assert payload['checks']['delegation_ready'] is True
    assert payload['checks']['notifications_ready'] is True


def test_workflow_process_orchestration_depth() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    admin = admin_user(headers)
    now = datetime.now(UTC)
    period = '2026-08'

    template = client.post(
        '/api/workflow-designer/templates',
        headers=headers,
        json={
            'template_key': 'b45-close-process',
            'name': 'B45 Close Process',
            'entity_type': 'close_campaign',
            'steps': [
                {'step_key': 'prepare', 'label': 'Prepare', 'approver_user_id': admin['id'], 'escalation_hours': 1, 'escalation_user_id': admin['id']},
                {'step_key': 'certify', 'label': 'Certify', 'approver_user_id': admin['id'], 'escalation_hours': 2, 'escalation_user_id': admin['id']},
            ],
        },
    )
    assert template.status_code == 200

    visual = client.post(
        '/api/workflow-designer/visual-designs',
        headers=headers,
        json={'template_id': template.json()['id'], 'layout': {'nodes': [{'id': 'prepare', 'x': 50}], 'edges': [{'from': 'prepare', 'to': 'certify'}]}},
    )
    assert visual.status_code == 200
    assert visual.json()['layout']['nodes'][0]['id'] == 'prepare'

    calendar = client.post(
        '/api/workflow-designer/process-calendars',
        headers=headers,
        json={'scenario_id': sid, 'calendar_key': 'b45-close-cal', 'process_type': 'close', 'period': period, 'milestones': [{'key': 'review', 'offset_days': 2}], 'status': 'active'},
    )
    assert calendar.status_code == 200
    assert calendar.json()['milestones'][0]['key'] == 'review'

    substitute = client.post(
        '/api/workflow-designer/substitute-approvers',
        headers=headers,
        json={'original_user_id': admin['id'], 'substitute_user_id': admin['id'], 'process_type': 'close', 'starts_at': (now - timedelta(hours=1)).isoformat(), 'ends_at': (now + timedelta(days=1)).isoformat(), 'active': True},
    )
    assert substitute.status_code == 200
    assert substitute.json()['active'] is True

    checklist = client.post('/api/close/checklists', headers=headers, json={'scenario_id': sid, 'period': period, 'checklist_key': 'b45-close-task', 'title': 'B45 close task', 'owner': 'Controller', 'due_date': '2026-08-10'})
    assert checklist.status_code == 200

    monitor = client.post('/api/workflow-designer/campaign-monitors', headers=headers, json={'scenario_id': sid, 'process_type': 'close', 'period': period})
    assert monitor.status_code == 200
    assert monitor.json()['total_items'] >= 1
    assert monitor.json()['status'] == 'monitoring'

    packet = client.post('/api/workflow-designer/certification-packets', headers=headers, json={'scenario_id': sid, 'process_type': 'close', 'period': period})
    assert packet.status_code == 200
    assert packet.json()['contents']['campaign_monitor']['total_items'] >= 1

    workspace = client.get(f'/api/workflow-designer/workspace?scenario_id={sid}', headers=headers)
    assert workspace.status_code == 200
    payload = workspace.json()
    assert payload['status']['batch'] == 'B45'
    assert payload['visual_designs']
    assert payload['process_calendars']
    assert payload['substitute_approvers']
    assert payload['certification_packets']
    assert payload['campaign_monitors']
