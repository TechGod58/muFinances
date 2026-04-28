from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_guided_entry.db'
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


def test_guided_entry_status_reports_b25_complete() -> None:
    response = client.get('/api/guided-entry/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B25'
    assert payload['complete'] is True
    assert payload['checks']['start_here_panel_ready'] is True
    assert payload['checks']['manual_entry_wizard_ready'] is True
    assert payload['checks']['import_wizard_ready'] is True
    assert payload['checks']['export_wizard_ready'] is True


def test_guided_entry_ui_has_non_expert_starting_points() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')

    assert 'id="guidedStart"' in index
    assert 'What do you want to do first?' in index
    assert 'No software knowledge needed' in index
    assert 'id="guidedManualButton"' in index
    assert 'id="guidedImportButton"' in index
    assert 'id="guidedExportButton"' in index
    assert 'id="guidedManualDialog"' in index
    assert 'id="guidedImportDialog"' in index
    assert 'id="guidedExportDialog"' in index
    assert 'handleGuidedManualSave' in app_js
    assert 'handleGuidedImportRun' in app_js
    assert 'handleGuidedExportRun' in app_js
