from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_ux_finance_user_polish.db'
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


def test_ux_finance_user_polish_status_and_run() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/ux/finance-polish/status', headers=headers)
    assert status.status_code == 200
    status_payload = status.json()
    assert status_payload['batch'] == 'B103'
    assert status_payload['complete'] is True
    assert all(status_payload['checks'].values())

    run = client.post('/api/ux/finance-polish/run', headers=headers, json={'run_key': 'b103-regression', 'scenario_id': sid})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['workspace_toggles_ready'] is True
    assert payload['checks']['dock_undock_ready'] is True
    assert payload['checks']['guided_entry_ready'] is True
    assert payload['checks']['inline_validation_ready'] is True
    assert payload['checks']['keyboard_accessibility_ready'] is True
    assert payload['checks']['training_mode_ready'] is True
    assert payload['artifacts']['validation']['valid'] is False
    assert payload['artifacts']['bulk_paste']['accepted_rows'] == 1
    assert payload['artifacts']['training']['status'] == 'active'

    rows = client.get('/api/ux/finance-polish/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1


def test_ux_finance_user_polish_ui_assets_are_discoverable_and_keyboard_ready() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')
    workspace_js = (PROJECT_ROOT / 'static' / 'js' / 'workspace-button-fallback.js').read_text(encoding='utf-8')
    dock_js = (PROJECT_ROOT / 'static' / 'js' / 'dockable-sections.js').read_text(encoding='utf-8')
    styles = (PROJECT_ROOT / 'static' / 'styles.css').read_text(encoding='utf-8')

    assert 'id="heroImportButton"' in index
    assert 'id="heroExportButton"' in index
    assert 'id="guidedStart"' in index
    assert 'id="gridValidationMessage"' in index
    assert 'skip-link' in index
    assert 'tabindex="0"' in index
    assert 'handleGuidedManualSave' in app_js
    assert 'handleGuidedImportRun' in app_js
    assert 'handleGuidedExportRun' in app_js
    assert 'workspaceMenuButton' in workspace_js
    assert 'workspace-section-toggle' in workspace_js
    assert 'aria-pressed' in workspace_js
    assert 'dock-toggle-button' in dock_js
    assert 'window.open' in dock_js
    assert '[aria-invalid="true"]' in styles
    assert ':focus-visible' in styles
