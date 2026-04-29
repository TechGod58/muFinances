from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services import security


def test_b139_production_blocks_default_admin_and_local_dev_secrets(monkeypatch) -> None:
    monkeypatch.setattr(security, 'APP_ENV', 'production')
    monkeypatch.setattr(security, 'DEFAULT_ADMIN_EMAIL', security.DEV_DEFAULT_ADMIN_EMAIL)
    monkeypatch.setattr(security, 'DEFAULT_ADMIN_PASSWORD', security.DEV_DEFAULT_ADMIN_PASSWORD)
    monkeypatch.setattr(security, 'FIELD_KEY', security.DEV_DEFAULT_FIELD_KEY)
    monkeypatch.setattr(security, 'FIELD_KEY_FILE', '')
    monkeypatch.delenv('CAMPUS_FPM_ALLOWED_ORIGINS', raising=False)

    blockers = security.production_security_blockers()

    assert any('ADMIN_EMAIL' in blocker for blocker in blockers)
    assert any('ADMIN_PASSWORD' in blocker for blocker in blockers)
    assert any('FIELD_KEY' in blocker for blocker in blockers)
    assert any('ALLOWED_ORIGINS' in blocker for blocker in blockers)
    try:
        security.assert_production_security_ready()
    except RuntimeError as exc:
        assert 'Production security readiness failed' in str(exc)
    else:
        raise AssertionError('Production startup must fail fast on unsafe defaults.')


def test_b139_blocks_known_unsafe_admin_password(monkeypatch, tmp_path: Path) -> None:
    key_file = tmp_path / 'field-key.txt'
    key_file.write_text('safe-production-field-key', encoding='utf-8')
    monkeypatch.setattr(security, 'APP_ENV', 'production')
    monkeypatch.setattr(security, 'DEFAULT_ADMIN_EMAIL', 'finance-admin@manchester.edu')
    monkeypatch.setattr(security, 'DEFAULT_ADMIN_PASSWORD', 'sup3rB@D')
    monkeypatch.setattr(security, 'FIELD_KEY', security.DEV_DEFAULT_FIELD_KEY)
    monkeypatch.setattr(security, 'FIELD_KEY_FILE', str(key_file))
    monkeypatch.setenv('CAMPUS_FPM_ALLOWED_ORIGINS', 'https://mufinances.manchester.edu')

    blockers = security.production_security_blockers()

    assert any('unsafe default password blocklist' in blocker for blocker in blockers)


def test_b140_frontend_controller_split_is_registered_and_fallback_not_loaded() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    registry = (PROJECT_ROOT / 'static' / 'modules' / 'registry.js').read_text(encoding='utf-8')

    assert '/static/js/controllers/workspace-controller.js' in index
    assert '/static/js/controllers/command-bar-controller.js' in index
    assert '/static/js/controllers/import-export-controller.js' in index
    assert 'src="/static/js/workspace-button-fallback.js' not in index
    assert 'frontendMonolithSplit' in registry


def test_b141_html_shell_uses_component_templates_for_chat() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    shell_templates = (PROJECT_ROOT / 'static' / 'js' / 'components' / 'shell-templates.js').read_text(encoding='utf-8')

    assert '/static/js/components/shell-templates.js' in index
    assert '<aside id="chatSatellite"' not in index
    assert 'chatSatellite' in shell_templates
