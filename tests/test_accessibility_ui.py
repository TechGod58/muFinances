from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_accessibility_ui.db'
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


def test_accessibility_status_reports_b23_complete() -> None:
    response = client.get('/api/accessibility/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B23'
    assert payload['complete'] is True
    assert payload['checks']['keyboard_navigation_ready'] is True
    assert payload['checks']['screen_reader_labels_ready'] is True
    assert payload['checks']['mobile_tablet_review_layout_ready'] is True
    assert payload['checks']['high_contrast_table_checks_ready'] is True
    assert payload['checks']['playwright_ui_smoke_tests_ready'] is True


def test_static_ui_has_accessibility_contracts() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    styles = (PROJECT_ROOT / 'static' / 'styles.css').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')

    assert 'class="skip-link"' in index
    assert 'aria-label="Primary sections"' in index
    assert 'role="status" aria-live="polite"' in index
    assert 'aria-label="Grid amount"' in index
    assert '<caption class="sr-only">' in app_js
    assert 'class ApiError extends Error' in app_js
    assert 'const escapeHtml' in app_js
    assert 'function applyRouteFromHash' in app_js
    assert 'id="appStatus"' in index
    assert 'frontendReliability' in (PROJECT_ROOT / 'static' / 'modules' / 'registry.js').read_text(encoding='utf-8')
    assert ':focus-visible' in styles
    assert '.app-status' in styles
    assert 'nav a[aria-current="true"]' in styles
    assert '@media (max-width: 760px)' in styles
    assert '@media (forced-colors: active)' in styles
