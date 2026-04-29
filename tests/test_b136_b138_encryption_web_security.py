from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_b136_b138_security.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app
from app.services import security

client = TestClient(app)


def test_authenticated_field_encryption_round_trips_and_rejects_tampering() -> None:
    encrypted = security.encrypt_value('salary-bank-account-1234')

    assert encrypted.startswith('enc:v2:')
    assert security.decrypt_value(encrypted) == 'salary-bank-account-1234'

    tampered = encrypted[:-2] + ('AA' if not encrypted.endswith('AA') else 'BB')
    try:
        security.decrypt_value(tampered)
    except ValueError as exc:
        assert 'authentication failed' in str(exc)
    else:
        raise AssertionError('Tampered encrypted value should not decrypt.')


def test_legacy_v1_values_remain_readable_and_migratable() -> None:
    legacy = security._legacy_encrypt_for_migration_test('legacy-tax-id')

    migrated = security.migrate_encrypted_value(legacy)

    assert legacy.startswith('enc:v1:')
    assert migrated.startswith('enc:v2:')
    assert security.decrypt_value(migrated) == 'legacy-tax-id'


def test_field_key_file_satisfies_production_secret_check(monkeypatch, tmp_path: Path) -> None:
    key_file = tmp_path / 'field_key.txt'
    key_file.write_text('production-field-key-from-mounted-secret', encoding='utf-8')

    monkeypatch.setattr(security, 'APP_ENV', 'production')
    monkeypatch.setattr(security, 'DEFAULT_ADMIN_EMAIL', 'finance-admin@manchester.edu')
    monkeypatch.setattr(security, 'DEFAULT_ADMIN_PASSWORD', 'changed-production-admin-password')
    monkeypatch.setattr(security, 'FIELD_KEY', security.DEV_DEFAULT_FIELD_KEY)
    monkeypatch.setattr(security, 'FIELD_KEY_FILE', str(key_file))
    monkeypatch.setenv('CAMPUS_FPM_ALLOWED_ORIGINS', 'https://mufinances.manchester.edu')

    security.assert_production_security_ready()

    status = security.encryption_status()
    assert status['authenticated_encryption_ready'] is True
    assert status['field_key_file_supported'] is True
    assert status['field_key_loaded'] is True


def test_login_lockout_blocks_repeated_bad_passwords(monkeypatch) -> None:
    monkeypatch.setattr(security, 'LOGIN_LOCKOUT_THRESHOLD', 2)
    monkeypatch.setattr(security, 'LOGIN_LOCKOUT_MINUTES', 10)
    host = 'lockout-test-host'

    assert security.authenticate('admin@mufinances.local', 'bad-password-1', host) is None
    assert security.authenticate('admin@mufinances.local', 'bad-password-2', host) is None
    try:
        security.authenticate('admin@mufinances.local', 'ChangeMe!3200', host)
    except PermissionError as exc:
        assert 'Login temporarily locked' in str(exc)
    else:
        raise AssertionError('Login should be locked after repeated failures.')


def test_cors_is_restricted_and_security_headers_are_present() -> None:
    response = client.options(
        '/api/auth/bootstrap',
        headers={
            'Origin': 'http://localhost:3200',
            'Access-Control-Request-Method': 'GET',
        },
    )

    assert response.headers.get('access-control-allow-origin') == 'http://localhost:3200'
    assert response.headers.get('access-control-allow-origin') != '*'

    unauthenticated = client.get('/api/scenarios')
    assert unauthenticated.status_code == 401
    assert unauthenticated.headers['cross-origin-opener-policy'] == 'same-origin'
    assert unauthenticated.headers['cross-origin-resource-policy'] == 'same-origin'
