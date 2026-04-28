from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_security_hardening_b51.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app
from app.services import security

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def test_login_form_has_no_prefilled_credentials() -> None:
    index = Path('static/index.html').read_text(encoding='utf-8')
    assert 'value="Admin"' not in index
    assert 'name="password" type="password" required autocomplete="current-password"' in index
    assert 'id="passwordChangeDialog"' in index


def test_security_headers_and_b51_migration_are_registered() -> None:
    response = client.get('/api/scenarios')
    assert response.status_code == 401
    assert response.headers['x-content-type-options'] == 'nosniff'
    assert response.headers['x-frame-options'] == 'DENY'
    assert response.headers['cache-control'] == 'no-store'

    migrations = client.get('/api/foundation/migrations', headers=admin_headers())
    assert migrations.status_code == 200
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0052_security_cleanup_first_run_hardening' in keys


def test_production_security_readiness_blocks_dev_secrets(monkeypatch) -> None:
    monkeypatch.setattr(security, 'APP_ENV', 'production')
    monkeypatch.setattr(security, 'DEFAULT_ADMIN_PASSWORD', security.DEV_DEFAULT_ADMIN_PASSWORD)
    monkeypatch.setattr(security, 'FIELD_KEY', security.DEV_DEFAULT_FIELD_KEY)
    try:
        security.assert_production_security_ready()
    except RuntimeError as exc:
        assert 'Production security readiness failed' in str(exc)
    else:
        raise AssertionError('Production readiness should reject default development secrets.')
