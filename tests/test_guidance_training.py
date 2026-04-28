from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_guidance_training.db'
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


def active_scenario_id(headers: dict[str, str]) -> int:
    response = client.get('/api/bootstrap', headers=headers)
    assert response.status_code == 200
    return int(response.json()['activeScenario']['id'])


def test_guidance_workspace_task_progress_and_training_mode() -> None:
    headers = admin_headers()
    scenario_id = active_scenario_id(headers)

    workspace = client.get(f'/api/guidance/workspace?scenario_id={scenario_id}', headers=headers)
    assert workspace.status_code == 200
    payload = workspace.json()
    assert payload['status']['batch'] == 'B48'
    assert payload['recommended_role'] == 'admin'
    assert len(payload['checklists']) >= 3
    assert len(payload['field_help']) >= 6
    assert len(payload['playbooks']) >= 4

    first_checklist = payload['checklists'][0]
    first_task = first_checklist['tasks'][0]
    completed = client.post(
        '/api/guidance/tasks/complete',
        headers=headers,
        json={'checklist_key': first_checklist['checklist_key'], 'task_key': first_task['task_key']},
    )
    assert completed.status_code == 200
    assert completed.json()['status'] == 'completed'

    training = client.post(
        '/api/guidance/training/start',
        headers=headers,
        json={'mode_key': 'controller', 'scenario_id': scenario_id},
    )
    assert training.status_code == 200
    assert training.json()['mode_key'] == 'controller'
    assert training.json()['status'] == 'active'

    refreshed = client.get(f'/api/guidance/workspace?scenario_id={scenario_id}', headers=headers).json()
    assert refreshed['training_sessions']
    assert any(task['status'] == 'completed' for checklist in refreshed['checklists'] for task in checklist['tasks'])


def test_guidance_status_and_migration_are_registered() -> None:
    headers = admin_headers()

    status = client.get('/api/guidance/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B48'
    assert payload['complete'] is True
    assert payload['checks']['training_mode_ready'] is True

    migrations = client.get('/api/foundation/migrations', headers=headers)
    assert migrations.status_code == 200
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0049_in_app_guidance_finance_training' in keys


def test_guidance_training_ui_and_sign_out_surface_exists() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')

    assert 'id="guidance-training"' in index
    assert 'id="completeGuidanceTaskButton"' in index
    assert 'id="startAdminTrainingButton"' in index
    assert 'id="footerSignOutButton"' in index
    assert 'Sign out' in index
    assert 'renderGuidanceTraining' in app_js
    assert '/api/guidance/workspace' in app_js
    assert 'function signOut()' in app_js
