from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_workspace_visibility.db'
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


def test_b68_toggle_workspace_visibility_ui_and_migration() -> None:
    headers = admin_headers()
    migrations = client.get('/api/foundation/migrations', headers=headers)
    assert migrations.status_code == 200
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0068_toggle_based_workspace_visibility' in keys

    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')
    workspace_js = (PROJECT_ROOT / 'static' / 'js' / 'workspace-button-fallback.js').read_text(encoding='utf-8')
    index_html = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')

    assert 'function toggleCommandDeck()' in app_js
    assert "$('#commandDeckToggle').addEventListener('click', toggleCommandDeck)" in app_js
    assert 'function initWorkspaceVisibility' not in app_js
    assert 'WORKSPACE_VISIBILITY_STYLE_ID' not in app_js
    assert 'wireExistingCommandDeckToggles' not in app_js
    assert '__MUFINANCES_APP_JS_OWNS_WORKSPACE_TOGGLES__' not in app_js
    assert 'mufinances.commandDeck.activeSections.v2' not in app_js
    assert 'mufinances.workspace.visibleSections' not in app_js

    assert 'workspaceToggleTray' in workspace_js
    assert 'workspaceMenuButton' in workspace_js
    assert 'workspaceEmptyState' in workspace_js
    assert 'workspace-section-toggle' in workspace_js
    assert 'workspace-toggle-category' in workspace_js
    assert 'mufinances.workspace.fallback.activeNumbers.v2' in workspace_js
    assert 'activeNumbers' in workspace_js
    assert 'setPanelVisible' in workspace_js
    assert 'showAllWorkspacePanels' in workspace_js
    assert 'Show all workspaces' in workspace_js
    assert 'aria-pressed' in workspace_js
    assert 'workspace-toggle-active' in workspace_js

    assert '/static/js/workspace-button-fallback.js?v=89' in index_html
