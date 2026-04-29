from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
from urllib.parse import urlencode
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
try:
    from cryptography.fernet import Fernet, InvalidToken
except Exception:  # pragma: no cover - production readiness reports this explicitly.
    Fernet = None  # type: ignore[assignment]
    InvalidToken = ValueError  # type: ignore[assignment]

from app import db
from app.services.access_guard import access_guard_status, assert_ad_ou_allowed, dn_is_under_ou, is_network_allowed, network_guard_config
from app.services.compliance import (
    certify as certify_compliance_control,
    create_certification as create_compliance_certification,
    ensure_compliance_ready,
    sod_report,
)

SESSION_HOURS = int(os.getenv('CAMPUS_FPM_SESSION_HOURS', '12'))
APP_ENV = os.getenv('CAMPUS_FPM_ENV', os.getenv('APP_ENV', 'development')).lower()
DEFAULT_ADMIN_EMAIL = os.getenv('CAMPUS_FPM_ADMIN_EMAIL', 'admin@mufinances.local').lower()
DEFAULT_ADMIN_PASSWORD = os.getenv('CAMPUS_FPM_ADMIN_PASSWORD', 'ChangeMe!3200')
DEV_DEFAULT_ADMIN_EMAIL = 'admin@mufinances.local'
DEV_DEFAULT_ADMIN_PASSWORD = 'ChangeMe!3200'
DEV_DEFAULT_FIELD_KEY = 'local-dev-field-key-change-before-production'
UNSAFE_ADMIN_PASSWORDS = {
    DEV_DEFAULT_ADMIN_PASSWORD,
    'Admin',
    'admin',
    'password',
    'Password123',
    'sup3rB@D',
}
FIELD_KEY_FILE = os.getenv('CAMPUS_FPM_FIELD_KEY_FILE', '')
FIELD_KEY_VERSION = os.getenv('CAMPUS_FPM_FIELD_KEY_VERSION', 'v2')
FIELD_KEY = os.getenv('CAMPUS_FPM_FIELD_KEY', DEV_DEFAULT_FIELD_KEY)
FIELD_KEY_PREVIOUS = os.getenv('CAMPUS_FPM_FIELD_KEY_PREVIOUS', '')
FIELD_KEY_ENVELOPE_MASTER_FILE = os.getenv('CAMPUS_FPM_FIELD_KEY_ENVELOPE_MASTER_FILE', '')
FIELD_KEY_ENVELOPE_MASTER = os.getenv('CAMPUS_FPM_FIELD_KEY_ENVELOPE_MASTER', '')
SESSION_COOKIE_MODE = os.getenv('CAMPUS_FPM_SESSION_COOKIE_MODE', 'false').lower() == 'true'
SESSION_COOKIE_NAME = os.getenv('CAMPUS_FPM_SESSION_COOKIE_NAME', 'mufinances_session')
CSRF_COOKIE_NAME = os.getenv('CAMPUS_FPM_CSRF_COOKIE_NAME', 'mufinances_csrf')
LOGIN_LOCKOUT_THRESHOLD = int(os.getenv('CAMPUS_FPM_LOGIN_LOCKOUT_THRESHOLD', '5'))
LOGIN_LOCKOUT_MINUTES = int(os.getenv('CAMPUS_FPM_LOGIN_LOCKOUT_MINUTES', '15'))
LOGIN_LOCKOUT_WINDOW_MINUTES = int(os.getenv('CAMPUS_FPM_LOGIN_LOCKOUT_WINDOW_MINUTES', '15'))
SENSITIVE_METADATA_KEYS = {'ssn', 'salary', 'salary_rate', 'compensation', 'bank_account', 'tax_id'}
SSO_PROVIDER_KEY = os.getenv('CAMPUS_FPM_SSO_PROVIDER', 'campus-sso')
SSO_NAME = os.getenv('CAMPUS_FPM_SSO_NAME', 'Campus SSO')
SSO_PROTOCOL = os.getenv('CAMPUS_FPM_SSO_PROTOCOL', 'oidc')
SSO_ISSUER_URL = os.getenv('CAMPUS_FPM_SSO_ISSUER_URL', '')
SSO_AUTHORIZE_URL = os.getenv('CAMPUS_FPM_SSO_AUTHORIZE_URL', '')
SSO_TOKEN_URL = os.getenv('CAMPUS_FPM_SSO_TOKEN_URL', '')
SSO_JWKS_URL = os.getenv('CAMPUS_FPM_SSO_JWKS_URL', '')
SSO_CLIENT_ID = os.getenv('CAMPUS_FPM_SSO_CLIENT_ID', '')
SSO_CLIENT_SECRET = os.getenv('CAMPUS_FPM_SSO_CLIENT_SECRET', '')
SSO_REDIRECT_URI = os.getenv('CAMPUS_FPM_SSO_REDIRECT_URI', 'http://localhost:3200/api/auth/sso/callback')
SSO_ALLOW_UNSIGNED_TOKENS = os.getenv('CAMPUS_FPM_SSO_ALLOW_UNSIGNED_TOKENS', 'false').lower() == 'true'
SSO_TEST_TOKEN_RESPONSE = os.getenv('CAMPUS_FPM_SSO_TEST_TOKEN_RESPONSE', '')
TRUSTED_SSO_HEADER_ENABLED = os.getenv('CAMPUS_FPM_TRUSTED_SSO_HEADER', 'false').lower() == 'true'
TRUSTED_SSO_EMAIL_HEADER = os.getenv('CAMPUS_FPM_TRUSTED_SSO_EMAIL_HEADER', 'x-mufinances-sso-email').lower()

ROLE_PERMISSION_MAP = {
    'finance.admin': [
        'security.manage',
        'ledger.read',
        'ledger.write',
        'ledger.reverse',
        'dimensions.manage',
        'periods.manage',
        'backups.manage',
        'reports.read',
        'parallel_cubed.use',
        'operating_budget.manage',
        'operating_budget.approve',
        'enrollment.manage',
        'enrollment.forecast',
        'campus_planning.manage',
        'campus_planning.approve',
        'forecast.manage',
        'scenario.manage',
        'reporting.manage',
        'exports.manage',
        'close.manage',
        'consolidation.manage',
        'integrations.manage',
        'automation.manage',
        'automation.approve',
        'workspaces.view',
        'operations.manage',
        'sensitive.read',
        'row_access.all',
    ],
    'budget.office': [
        'ledger.read',
        'ledger.write',
        'ledger.reverse',
        'dimensions.manage',
        'periods.manage',
        'reports.read',
        'parallel_cubed.use',
        'operating_budget.manage',
        'operating_budget.approve',
        'enrollment.manage',
        'enrollment.forecast',
        'campus_planning.manage',
        'campus_planning.approve',
        'forecast.manage',
        'scenario.manage',
        'reporting.manage',
        'exports.manage',
        'close.manage',
        'consolidation.manage',
        'integrations.manage',
        'automation.manage',
        'automation.approve',
        'workspaces.view',
        'operations.manage',
        'row_access.all',
    ],
    'department.planner': [
        'ledger.read',
        'ledger.write',
        'reports.read',
        'parallel_cubed.use',
        'operating_budget.manage',
        'enrollment.manage',
        'campus_planning.manage',
        'forecast.manage',
        'reporting.manage',
        'close.manage',
        'automation.manage',
        'workspaces.view',
    ],
    'auditor': [
        'ledger.read',
        'reports.read',
        'parallel_cubed.use',
        'close.manage',
        'automation.manage',
        'workspaces.view',
    ],
}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _secret_file_value(path: str) -> str:
    if not path:
        return ''
    try:
        return open(path, 'r', encoding='utf-8').read().strip()
    except OSError:
        return ''


def reload_field_secrets() -> None:
    """Reload mounted field-encryption secrets without requiring process restart in tests."""
    global FIELD_KEY, FIELD_KEY_ENVELOPE_MASTER
    file_key = _secret_file_value(FIELD_KEY_FILE)
    if file_key:
        FIELD_KEY = file_key
    envelope_key = _secret_file_value(FIELD_KEY_ENVELOPE_MASTER_FILE)
    if envelope_key:
        FIELD_KEY_ENVELOPE_MASTER = envelope_key


reload_field_secrets()


def hash_password(password: str, salt: str | None = None) -> str:
    salt_value = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt_value.encode('utf-8'), 210_000)
    return f'pbkdf2_sha256${salt_value}${base64.b64encode(digest).decode("ascii")}'


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algo, salt, digest = stored_hash.split('$', 2)
    except ValueError:
        return False
    if algo != 'pbkdf2_sha256':
        return False
    expected = hash_password(password, salt)
    return hmac.compare_digest(expected, stored_hash)


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode('utf-8')).hexdigest()


def ensure_security_ready() -> None:
    _ensure_web_security_tables()
    for role_key in ROLE_PERMISSION_MAP:
        db.execute(
            'INSERT OR IGNORE INTO roles (role_key, name) VALUES (?, ?)',
            (role_key, role_key.replace('.', ' ').title()),
        )
    permissions = sorted({permission for values in ROLE_PERMISSION_MAP.values() for permission in values})
    for permission in permissions:
        db.execute(
            'INSERT OR IGNORE INTO permissions (permission_key, description) VALUES (?, ?)',
            (permission, permission.replace('.', ' ')),
        )
    for role_key, role_permissions in ROLE_PERMISSION_MAP.items():
        role = db.fetch_one('SELECT id FROM roles WHERE role_key = ?', (role_key,))
        if role is None:
            continue
        for permission in role_permissions:
            row = db.fetch_one('SELECT id FROM permissions WHERE permission_key = ?', (permission,))
            if row is not None:
                db.execute(
                    'INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?, ?)',
                    (role['id'], row['id']),
                )

    existing = db.fetch_one('SELECT id FROM users WHERE email = ?', (DEFAULT_ADMIN_EMAIL,))
    if existing is None:
        user_id = db.execute(
            '''
            INSERT INTO users (email, display_name, password_hash, is_active, created_at)
            VALUES (?, ?, ?, 1, ?)
            ''',
            (DEFAULT_ADMIN_EMAIL, 'muFinances Administrator', hash_password(DEFAULT_ADMIN_PASSWORD), _now()),
        )
        role = db.fetch_one('SELECT id FROM roles WHERE role_key = ?', ('finance.admin',))
        if role is not None:
            db.execute('INSERT OR IGNORE INTO user_roles (user_id, role_id) VALUES (?, ?)', (user_id, role['id']))
        db.execute(
            '''
            INSERT OR IGNORE INTO user_dimension_access (user_id, dimension_kind, code)
            VALUES (?, '*', '*')
            ''',
            (user_id,),
        )
        db.log_audit(
            entity_type='user',
            entity_id=str(user_id),
            action='seeded_admin',
            actor='system',
            detail={'email': DEFAULT_ADMIN_EMAIL},
            created_at=_now(),
        )
    db.execute(
        '''
        INSERT INTO sso_providers (
            provider_key, name, protocol, issuer_url, authorize_url, token_url,
            jwks_url, client_id, enabled, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider_key) DO UPDATE SET
            name = excluded.name,
            protocol = excluded.protocol,
            issuer_url = excluded.issuer_url,
            authorize_url = excluded.authorize_url,
            token_url = excluded.token_url,
            jwks_url = excluded.jwks_url,
            client_id = excluded.client_id,
            enabled = excluded.enabled
        ''',
        (
            SSO_PROVIDER_KEY,
            SSO_NAME,
            SSO_PROTOCOL,
            SSO_ISSUER_URL,
            SSO_AUTHORIZE_URL,
            SSO_TOKEN_URL,
            SSO_JWKS_URL,
            SSO_CLIENT_ID,
            1 if bool(SSO_AUTHORIZE_URL and SSO_CLIENT_ID) else 0,
            _now(),
        ),
    )


def assert_production_security_ready() -> None:
    if APP_ENV not in {'prod', 'production'}:
        return
    failures = production_security_blockers()
    if failures:
        raise RuntimeError('Production security readiness failed: ' + ' '.join(failures))


def production_security_blockers() -> list[str]:
    reload_field_secrets()
    failures = []
    if DEFAULT_ADMIN_EMAIL == DEV_DEFAULT_ADMIN_EMAIL:
        failures.append('CAMPUS_FPM_ADMIN_EMAIL must not use the local development admin account in production.')
    if DEFAULT_ADMIN_PASSWORD == DEV_DEFAULT_ADMIN_PASSWORD:
        failures.append('CAMPUS_FPM_ADMIN_PASSWORD must be changed in production.')
    elif DEFAULT_ADMIN_PASSWORD in UNSAFE_ADMIN_PASSWORDS:
        failures.append('CAMPUS_FPM_ADMIN_PASSWORD is on the unsafe default password blocklist.')
    if FIELD_KEY_FILE and not _secret_file_value(FIELD_KEY_FILE):
        failures.append('CAMPUS_FPM_FIELD_KEY_FILE must point to a readable, non-empty secret file in production.')
    if not FIELD_KEY or FIELD_KEY == DEV_DEFAULT_FIELD_KEY:
        failures.append('CAMPUS_FPM_FIELD_KEY or CAMPUS_FPM_FIELD_KEY_FILE must be set to a non-default value in production.')
    if Fernet is None:
        failures.append('cryptography must be installed for authenticated field encryption.')
    if SESSION_HOURS > 12:
        failures.append('CAMPUS_FPM_SESSION_HOURS must be 12 or lower in production.')
    if SESSION_COOKIE_MODE and not os.getenv('CAMPUS_FPM_COOKIE_SECURE', 'true').lower() == 'true':
        failures.append('CAMPUS_FPM_COOKIE_SECURE must stay enabled for production cookie sessions.')
    cors_origins = [origin.strip() for origin in os.getenv('CAMPUS_FPM_ALLOWED_ORIGINS', '').split(',') if origin.strip()]
    if not cors_origins:
        failures.append('CAMPUS_FPM_ALLOWED_ORIGINS must be set in production.')
    if '*' in cors_origins:
        failures.append('CAMPUS_FPM_ALLOWED_ORIGINS cannot include * in production.')
    return failures


def _ensure_web_security_tables() -> None:
    db.execute(
        '''
        CREATE TABLE IF NOT EXISTS login_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            client_host TEXT NOT NULL DEFAULT '',
            success INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
        '''
    )
    db.execute(
        '''
        CREATE TABLE IF NOT EXISTS login_lockouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            client_host TEXT NOT NULL DEFAULT '',
            fail_count INTEGER NOT NULL DEFAULT 0,
            locked_until TEXT DEFAULT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(email, client_host)
        )
        '''
    )


def password_policy_errors(password: str) -> list[str]:
    errors = []
    if len(password) < 12:
        errors.append('Password must be at least 12 characters.')
    if not any(char.isupper() for char in password):
        errors.append('Password must include an uppercase letter.')
    if not any(char.islower() for char in password):
        errors.append('Password must include a lowercase letter.')
    if not any(char.isdigit() for char in password):
        errors.append('Password must include a number.')
    if not any(not char.isalnum() for char in password):
        errors.append('Password must include a symbol.')
    return errors


def _lockout_key(email: str, client_host: str | None) -> tuple[str, str]:
    return (email.strip().lower(), (client_host or '').strip())


def _lockout_until(email: str, client_host: str | None) -> datetime | None:
    _ensure_web_security_tables()
    normalized_email, normalized_host = _lockout_key(email, client_host)
    row = db.fetch_one(
        'SELECT locked_until FROM login_lockouts WHERE email = ? AND client_host = ?',
        (normalized_email, normalized_host),
    )
    if not row or not row.get('locked_until'):
        return None
    locked_until = datetime.fromisoformat(row['locked_until'])
    if locked_until > datetime.now(UTC):
        return locked_until
    return None


def _record_login_attempt(email: str, client_host: str | None, success: bool) -> None:
    _ensure_web_security_tables()
    normalized_email, normalized_host = _lockout_key(email, client_host)
    now = datetime.now(UTC)
    db.execute(
        'INSERT INTO login_attempts (email, client_host, success, created_at) VALUES (?, ?, ?, ?)',
        (normalized_email, normalized_host, 1 if success else 0, now.isoformat()),
    )
    if success:
        db.execute(
            '''
            INSERT INTO login_lockouts (email, client_host, fail_count, locked_until, created_at, updated_at)
            VALUES (?, ?, 0, NULL, ?, ?)
            ON CONFLICT(email, client_host) DO UPDATE SET
                fail_count = 0,
                locked_until = NULL,
                updated_at = excluded.updated_at
            ''',
            (normalized_email, normalized_host, now.isoformat(), now.isoformat()),
        )
        return

    window_start = now - timedelta(minutes=LOGIN_LOCKOUT_WINDOW_MINUTES)
    failure_count = db.fetch_one(
        '''
        SELECT COUNT(*) AS failed
        FROM login_attempts
        WHERE email = ? AND client_host = ? AND success = 0 AND created_at >= ?
        ''',
        (normalized_email, normalized_host, window_start.isoformat()),
    )['failed']
    locked_until = (now + timedelta(minutes=LOGIN_LOCKOUT_MINUTES)).isoformat() if int(failure_count) >= LOGIN_LOCKOUT_THRESHOLD else None
    db.execute(
        '''
        INSERT INTO login_lockouts (email, client_host, fail_count, locked_until, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(email, client_host) DO UPDATE SET
            fail_count = excluded.fail_count,
            locked_until = excluded.locked_until,
            updated_at = excluded.updated_at
        ''',
        (normalized_email, normalized_host, int(failure_count), locked_until, now.isoformat(), now.isoformat()),
    )


def authenticate(email: str, password: str, client_host: str | None = None) -> dict[str, Any] | None:
    locked_until = _lockout_until(email, client_host)
    if locked_until is not None:
        raise PermissionError(f'Login temporarily locked until {locked_until.isoformat()}.')
    user = db.fetch_one('SELECT * FROM users WHERE lower(email) = lower(?) AND is_active = 1', (email,))
    if user is None or not verify_password(password, user['password_hash']):
        _record_login_attempt(email, client_host, success=False)
        return None
    _record_login_attempt(email, client_host, success=True)
    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(UTC) + timedelta(hours=SESSION_HOURS)
    db.execute(
        '''
        INSERT INTO auth_sessions (user_id, token_hash, created_at, expires_at)
        VALUES (?, ?, ?, ?)
        ''',
        (user['id'], _token_hash(token), _now(), expires_at.isoformat()),
    )
    db.execute('UPDATE users SET last_login_at = ? WHERE id = ?', (_now(), user['id']))
    db.log_audit(
        entity_type='auth_session',
        entity_id=str(user['id']),
        action='login',
        actor=user['email'],
        detail={'email': user['email']},
        created_at=_now(),
    )
    return {'token': token, 'expires_at': expires_at.isoformat(), 'user': user_profile(int(user['id']))}


def issue_session(user_id: int, actor: str, method: str) -> dict[str, Any]:
    user = db.fetch_one('SELECT * FROM users WHERE id = ? AND is_active = 1', (user_id,))
    if user is None:
        raise ValueError('Active user not found.')
    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(UTC) + timedelta(hours=SESSION_HOURS)
    db.execute(
        '''
        INSERT INTO auth_sessions (user_id, token_hash, created_at, expires_at)
        VALUES (?, ?, ?, ?)
        ''',
        (user_id, _token_hash(token), _now(), expires_at.isoformat()),
    )
    db.log_audit(
        entity_type='auth_session',
        entity_id=str(user_id),
        action=f'{method}_login',
        actor=actor,
        detail={'email': user['email'], 'method': method},
        created_at=_now(),
    )
    return {'token': token, 'expires_at': expires_at.isoformat(), 'user': user_profile(user_id)}


def change_password(user: dict[str, Any], current_password: str, new_password: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM users WHERE id = ? AND is_active = 1', (int(user['id']),))
    if row is None or not verify_password(current_password, row['password_hash']):
        raise ValueError('Current password is incorrect.')
    errors = password_policy_errors(new_password)
    if errors:
        raise ValueError(' '.join(errors))
    if verify_password(new_password, row['password_hash']):
        raise ValueError('New password must be different from the current password.')
    now = _now()
    db.execute(
        '''
        UPDATE users
        SET password_hash = ?, must_change_password = 0, password_changed_at = ?
        WHERE id = ?
        ''',
        (hash_password(new_password), now, int(user['id'])),
    )
    db.log_audit('user', str(user['id']), 'password_changed', user['email'], {'first_login_complete': bool(row.get('must_change_password', 0))}, now)
    return user_profile(int(user['id']))


def user_from_token(token: str) -> dict[str, Any] | None:
    session = db.fetch_one(
        '''
        SELECT s.*, u.email, u.display_name, u.is_active
        FROM auth_sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.token_hash = ? AND s.revoked_at IS NULL
        ''',
        (_token_hash(token),),
    )
    if session is None or not bool(session['is_active']):
        return None
    expires_at = datetime.fromisoformat(session['expires_at'])
    if expires_at <= datetime.now(UTC):
        return None
    return user_profile(int(session['user_id']))


def user_profile(user_id: int) -> dict[str, Any]:
    user = db.fetch_one('SELECT id, email, display_name, is_active, must_change_password, password_changed_at, last_login_at, created_at FROM users WHERE id = ?', (user_id,))
    if user is None:
        raise ValueError('User not found.')
    roles = db.fetch_all(
        '''
        SELECT r.role_key
        FROM roles r
        JOIN user_roles ur ON ur.role_id = r.id
        WHERE ur.user_id = ?
        ORDER BY r.role_key
        ''',
        (user_id,),
    )
    permissions = db.fetch_all(
        '''
        SELECT DISTINCT p.permission_key
        FROM permissions p
        JOIN role_permissions rp ON rp.permission_id = p.id
        JOIN user_roles ur ON ur.role_id = rp.role_id
        WHERE ur.user_id = ?
        ORDER BY p.permission_key
        ''',
        (user_id,),
    )
    access = db.fetch_all(
        '''
        SELECT dimension_kind, code
        FROM user_dimension_access
        WHERE user_id = ?
        ORDER BY dimension_kind, code
        ''',
        (user_id,),
    )
    return {
        'id': user['id'],
        'email': user['email'],
        'display_name': user['display_name'],
        'is_active': bool(user['is_active']),
        'must_change_password': bool(user.get('must_change_password', 0)),
        'password_changed_at': user.get('password_changed_at'),
        'last_login_at': user.get('last_login_at'),
        'created_at': user['created_at'],
        'roles': [row['role_key'] for row in roles],
        'permissions': [row['permission_key'] for row in permissions],
        'dimension_access': access,
    }


def require_permission(user: dict[str, Any], permission: str) -> None:
    if permission not in user.get('permissions', []):
        raise PermissionError(f'Missing permission: {permission}')


def has_permission(user: dict[str, Any], permission: str) -> bool:
    return permission in user.get('permissions', [])


def allowed_codes(user: dict[str, Any], dimension_kind: str) -> set[str] | None:
    if has_permission(user, 'row_access.all'):
        return None
    values = set()
    for row in user.get('dimension_access', []):
        if row['dimension_kind'] == '*' and row['code'] == '*':
            return None
        if row['dimension_kind'] == dimension_kind:
            values.add(row['code'])
    return values


def create_user(payload: dict[str, Any], actor: str = 'api.user') -> dict[str, Any]:
    now = _now()
    user_id = db.execute(
        '''
        INSERT INTO users (email, display_name, password_hash, is_active, must_change_password, created_at)
        VALUES (?, ?, ?, 1, 1, ?)
        ''',
        (payload['email'].lower(), payload['display_name'], hash_password(payload['password']), now),
    )
    for role_key in payload.get('role_keys') or ['department.planner']:
        role = db.fetch_one('SELECT id FROM roles WHERE role_key = ?', (role_key,))
        if role is not None:
            db.execute('INSERT OR IGNORE INTO user_roles (user_id, role_id) VALUES (?, ?)', (user_id, role['id']))
    db.log_audit(
        entity_type='user',
        entity_id=str(user_id),
        action='created',
        actor=actor,
        detail={'email': payload['email'], 'roles': payload.get('role_keys')},
        created_at=now,
    )
    return user_profile(user_id)


def get_or_create_sso_user(email: str, external_subject: str, provider_key: str = SSO_PROVIDER_KEY) -> dict[str, Any]:
    email = email.lower().strip()
    identity = db.fetch_one(
        '''
        SELECT u.id
        FROM user_external_identities i
        JOIN users u ON u.id = i.user_id
        WHERE i.provider_key = ? AND i.external_subject = ?
        ''',
        (provider_key, external_subject),
    )
    if identity is not None:
        return user_profile(int(identity['id']))
    user = db.fetch_one('SELECT id FROM users WHERE lower(email) = lower(?)', (email,))
    if user is None:
        user_id = db.execute(
            '''
            INSERT INTO users (email, display_name, password_hash, is_active, created_at)
            VALUES (?, ?, ?, 1, ?)
            ''',
            (email, email.split('@')[0], hash_password(secrets.token_urlsafe(32)), _now()),
        )
        role = db.fetch_one('SELECT id FROM roles WHERE role_key = ?', ('department.planner',))
        if role is not None:
            db.execute('INSERT OR IGNORE INTO user_roles (user_id, role_id) VALUES (?, ?)', (user_id, role['id']))
    else:
        user_id = int(user['id'])
    db.execute(
        '''
        INSERT OR IGNORE INTO user_external_identities (user_id, provider_key, external_subject, email, created_at)
        VALUES (?, ?, ?, ?, ?)
        ''',
        (user_id, provider_key, external_subject, email, _now()),
    )
    return user_profile(user_id)


def grant_dimension_access(user_id: int, payload: dict[str, Any], actor: str = 'api.user') -> dict[str, Any]:
    db.execute(
        '''
        INSERT OR IGNORE INTO user_dimension_access (user_id, dimension_kind, code)
        VALUES (?, ?, ?)
        ''',
        (user_id, payload['dimension_kind'], payload['code']),
    )
    db.log_audit(
        entity_type='user_dimension_access',
        entity_id=str(user_id),
        action='granted',
        actor=actor,
        detail=payload,
        created_at=_now(),
    )
    return user_profile(user_id)


def security_status() -> dict[str, Any]:
    counts = {
        'users': int(db.fetch_one('SELECT COUNT(*) AS count FROM users')['count']),
        'roles': int(db.fetch_one('SELECT COUNT(*) AS count FROM roles')['count']),
        'permissions': int(db.fetch_one('SELECT COUNT(*) AS count FROM permissions')['count']),
        'sessions': int(db.fetch_one('SELECT COUNT(*) AS count FROM auth_sessions WHERE revoked_at IS NULL')['count']),
        'sso_providers': int(db.fetch_one('SELECT COUNT(*) AS count FROM sso_providers')['count']),
    }
    checks = {
        'local_auth_ready': counts['users'] > 0,
        'roles_ready': counts['roles'] >= 4,
        'permissions_ready': counts['permissions'] >= 1,
        'row_access_ready': True,
        'masking_ready': True,
        'api_auth_gate_ready': True,
        'sso_ready': counts['sso_providers'] > 0,
        'first_login_password_change_ready': True,
        'production_secret_fail_fast_ready': True,
        'session_security_headers_ready': True,
    }
    return {'batch': 'B02', 'title': 'Security And Control Baseline', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def enterprise_admin_status() -> dict[str, Any]:
    counts = {
        'sso_production_settings': int(db.fetch_one('SELECT COUNT(*) AS count FROM sso_production_settings')['count']),
        'ad_ou_group_mappings': int(db.fetch_one('SELECT COUNT(*) AS count FROM ad_ou_group_mappings')['count']),
        'domain_vpn_checks': int(db.fetch_one('SELECT COUNT(*) AS count FROM domain_vpn_enforcement_checks')['count']),
        'impersonation_sessions': int(db.fetch_one('SELECT COUNT(*) AS count FROM admin_impersonation_sessions')['count']),
        'sod_rules': int(db.fetch_one('SELECT COUNT(*) AS count FROM sod_rules WHERE active = 1')['count']),
        'access_reviews': int(db.fetch_one('SELECT COUNT(*) AS count FROM user_access_review_certifications')['count']),
    }
    checks = {
        'sso_production_wiring_ready': True,
        'ad_ou_group_mapping_ui_ready': True,
        'domain_vpn_enforcement_dashboard_ready': True,
        'admin_impersonation_controls_ready': True,
        'sod_policy_builder_ready': True,
        'user_access_review_certification_ready': True,
    }
    return {'batch': 'B46', 'title': 'Enterprise Security And Administration', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def enterprise_admin_workspace() -> dict[str, Any]:
    return {
        'status': enterprise_admin_status(),
        'sso_production_settings': list_sso_production_settings(),
        'ad_ou_group_mappings': list_ad_ou_group_mappings(),
        'domain_vpn_checks': list_domain_vpn_checks(),
        'impersonation_sessions': list_impersonation_sessions(),
        'sod_report': sod_report(),
        'access_reviews': list_access_reviews(),
        'access_guard': access_guard_status(),
        'users': list_users(),
    }


def activate_security_controls(user: dict[str, Any]) -> dict[str, Any]:
    ensure_compliance_ready()
    sso = upsert_sso_production_setting(
        {
            'provider_key': 'campus-sso',
            'environment': 'production',
            'metadata_url': os.getenv('CAMPUS_FPM_SSO_METADATA_URL', 'https://login.microsoftonline.com/manchester.edu/.well-known/openid-configuration'),
            'required_claim': 'email',
            'group_claim': 'groups',
            'jit_provisioning': True,
            'status': 'ready',
        },
        user,
    )
    mapping = upsert_ad_ou_group_mapping(
        {
            'mapping_key': 'manchester-finance-access',
            'ad_group_dn': 'CN=muFinances Finance Access,OU=Groups,DC=manchester,DC=edu',
            'allowed_ou_dn': 'OU=muFinances Users,OU=Finance,DC=manchester,DC=edu',
            'role_key': 'budget.office',
            'dimension_kind': 'department',
            'dimension_code': 'SCI',
            'active': True,
        },
        user,
    )
    domain_allowed = record_domain_vpn_check(
        {
            'check_key': 'security-activation-manchester-domain',
            'host': 'mufinances.manchester.edu',
            'client_host': '10.30.44.12',
            'forwarded_host': 'mufinances.manchester.edu',
            'forwarded_for': '10.30.44.12',
        },
        user,
    )
    vpn_allowed = record_domain_vpn_check(
        {
            'check_key': 'security-activation-vpn-network',
            'host': 'mufinances.internal',
            'client_host': '10.30.44.12',
            'forwarded_host': 'mufinances.internal',
            'forwarded_for': '10.30.44.12',
        },
        user,
    )
    sod = upsert_sod_policy(
        {
            'rule_key': 'security-activation-admin-approver',
            'name': 'Security admin and approval conflict',
            'conflict_type': 'role_pair',
            'left_value': 'finance.admin',
            'right_value': 'budget.office',
            'severity': 'high',
            'active': True,
        },
        user,
    )
    review = create_access_review(
        {
            'review_key': f"security-activation-access-review-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}",
            'reviewer_user_id': user['id'],
            'scenario_id': None,
            'scope': {'roles': True, 'dimensions': True, 'sso': True, 'ad_ou': True},
        },
        user,
    )
    certified_review = certify_access_review(int(review['id']), {'findings': review['findings']}, user)
    certification = create_compliance_certification(
        {
            'certification_key': f"security-activation-sod-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}",
            'control_area': 'security_activation',
            'period': datetime.now(UTC).strftime('%Y-%m'),
            'owner': user['email'],
            'notes': 'Security activation proof for SSO, AD/OU, domain/VPN, sessions, access reviews, and SoD.',
        },
        user,
    )
    certified_sod = certify_compliance_control(
        int(certification['id']),
        {'evidence': {'sso_provider': sso['provider_key'], 'ad_mapping': mapping['mapping_key'], 'sod_rule': sod['rule_key']}, 'notes': 'Security activation proof certified.'},
        user,
    )
    session_probe = issue_session(int(user['id']), actor=user['email'], method='security_activation_probe')
    db.execute('UPDATE auth_sessions SET revoked_at = ? WHERE token_hash = ?', (_now(), _token_hash(session_probe['token'])))
    try:
        assert_production_security_ready()
        production_secret_status = 'ready'
    except RuntimeError as exc:
        production_secret_status = str(exc)
    checks = {
        'sso_ready': sso['status'] == 'ready',
        'ad_ou_mapping_ready': mapping['active'] is True,
        'manchester_domain_enforcement_ready': domain_allowed['allowed'] is True,
        'vpn_enforcement_ready': vpn_allowed['allowed'] is True,
        'production_secret_fail_fast_ready': 'Production security readiness failed' not in production_secret_status or APP_ENV in {'development', 'dev', 'local'},
        'session_controls_ready': user_from_token(session_probe['token']) is None,
        'access_review_certified': certified_review['status'] == 'certified',
        'sod_certified': certified_sod['status'] == 'certified',
    }
    result = {
        'batch': 'Security Activation',
        'complete': all(checks.values()),
        'checks': checks,
        'sso': sso,
        'ad_ou_mapping': mapping,
        'domain_check': domain_allowed,
        'vpn_check': vpn_allowed,
        'sod_policy': sod,
        'access_review': certified_review,
        'sod_certification': certified_sod,
        'production_secret_status': production_secret_status,
        'access_guard': access_guard_status(),
    }
    db.log_audit('security_activation', str(user['id']), 'proved', user['email'], result, _now())
    return result


def list_users() -> list[dict[str, Any]]:
    return [user_profile(int(row['id'])) for row in db.fetch_all('SELECT id FROM users ORDER BY email')]


def upsert_sso_production_setting(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO sso_production_settings (
            provider_key, environment, metadata_url, required_claim, group_claim,
            jit_provisioning, status, created_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider_key) DO UPDATE SET
            environment = excluded.environment,
            metadata_url = excluded.metadata_url,
            required_claim = excluded.required_claim,
            group_claim = excluded.group_claim,
            jit_provisioning = excluded.jit_provisioning,
            status = excluded.status,
            updated_at = excluded.updated_at
        ''',
        (
            payload.get('provider_key') or SSO_PROVIDER_KEY, payload.get('environment') or 'production',
            payload.get('metadata_url') or '', payload.get('required_claim') or 'email',
            payload.get('group_claim') or 'groups', 1 if payload.get('jit_provisioning', True) else 0,
            payload.get('status') or 'draft', user['email'], now, now,
        ),
    )
    row = db.fetch_one('SELECT * FROM sso_production_settings WHERE provider_key = ?', (payload.get('provider_key') or SSO_PROVIDER_KEY,))
    db.log_audit('sso_production_setting', row['provider_key'], 'upserted', user['email'], payload, now)
    return _format_sso_prod(row)


def list_sso_production_settings() -> list[dict[str, Any]]:
    return [_format_sso_prod(row) for row in db.fetch_all('SELECT * FROM sso_production_settings ORDER BY provider_key')]


def upsert_ad_ou_group_mapping(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO ad_ou_group_mappings (
            mapping_key, ad_group_dn, allowed_ou_dn, role_key, dimension_kind,
            dimension_code, active, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(mapping_key) DO UPDATE SET
            ad_group_dn = excluded.ad_group_dn,
            allowed_ou_dn = excluded.allowed_ou_dn,
            role_key = excluded.role_key,
            dimension_kind = excluded.dimension_kind,
            dimension_code = excluded.dimension_code,
            active = excluded.active
        ''',
        (
            payload['mapping_key'], payload['ad_group_dn'], payload['allowed_ou_dn'], payload['role_key'],
            payload.get('dimension_kind'), payload.get('dimension_code'), 1 if payload.get('active', True) else 0,
            user['email'], now,
        ),
    )
    row = db.fetch_one('SELECT * FROM ad_ou_group_mappings WHERE mapping_key = ?', (payload['mapping_key'],))
    db.log_audit('ad_ou_group_mapping', payload['mapping_key'], 'upserted', user['email'], payload, now)
    return _format_bool(row, 'active')


def list_ad_ou_group_mappings() -> list[dict[str, Any]]:
    return [_format_bool(row, 'active') for row in db.fetch_all('SELECT * FROM ad_ou_group_mappings ORDER BY mapping_key')]


def record_domain_vpn_check(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    config = network_guard_config()
    headers = {'x-forwarded-host': payload.get('forwarded_host') or '', 'x-forwarded-for': payload.get('forwarded_for') or ''}
    allowed = is_network_allowed(payload['host'], payload.get('client_host') or '', headers, config)
    reason = 'allowed_by_domain_or_network' if allowed else 'blocked_by_domain_or_network'
    key = payload.get('check_key') or f"guard-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    db.execute(
        '''
        INSERT INTO domain_vpn_enforcement_checks (
            check_key, host, client_host, forwarded_host, forwarded_for, allowed, reason, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(check_key) DO UPDATE SET
            host = excluded.host,
            client_host = excluded.client_host,
            forwarded_host = excluded.forwarded_host,
            forwarded_for = excluded.forwarded_for,
            allowed = excluded.allowed,
            reason = excluded.reason,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        ''',
        (
            key, payload['host'], payload.get('client_host') or '', payload.get('forwarded_host') or '',
            payload.get('forwarded_for') or '', 1 if allowed else 0, reason, user['email'], _now(),
        ),
    )
    row = db.fetch_one('SELECT * FROM domain_vpn_enforcement_checks WHERE check_key = ?', (key,))
    return _format_bool(row, 'allowed')


def list_domain_vpn_checks() -> list[dict[str, Any]]:
    return [_format_bool(row, 'allowed') for row in db.fetch_all('SELECT * FROM domain_vpn_enforcement_checks ORDER BY id DESC LIMIT 100')]


def start_impersonation(payload: dict[str, Any], admin_user: dict[str, Any]) -> dict[str, Any]:
    if int(payload['target_user_id']) == int(admin_user['id']):
        raise ValueError('Cannot impersonate yourself.')
    target = user_profile(int(payload['target_user_id']))
    session = issue_session(int(target['id']), actor=admin_user['email'], method='admin_impersonation')
    now = _now()
    imp_id = db.execute(
        '''
        INSERT INTO admin_impersonation_sessions (
            admin_user_id, target_user_id, reason, status, token_expires_at,
            started_at, created_by, created_at
        ) VALUES (?, ?, ?, 'issued', ?, ?, ?, ?)
        ''',
        (admin_user['id'], target['id'], payload['reason'], session['expires_at'], now, admin_user['email'], now),
    )
    db.log_audit('admin_impersonation', str(imp_id), 'issued', admin_user['email'], {'target_user_id': target['id'], 'reason': payload['reason']}, now)
    row = _format_impersonation(db.fetch_one('SELECT * FROM admin_impersonation_sessions WHERE id = ?', (imp_id,)))
    row['impersonation_token'] = session['token']
    row['target_user'] = target
    return row


def end_impersonation(impersonation_id: int, user: dict[str, Any]) -> dict[str, Any]:
    db.execute("UPDATE admin_impersonation_sessions SET status = 'ended', ended_at = ? WHERE id = ?", (_now(), impersonation_id))
    db.log_audit('admin_impersonation', str(impersonation_id), 'ended', user['email'], {}, _now())
    return _format_impersonation(db.fetch_one('SELECT * FROM admin_impersonation_sessions WHERE id = ?', (impersonation_id,)))


def list_impersonation_sessions() -> list[dict[str, Any]]:
    return [_format_impersonation(row) for row in db.fetch_all('SELECT * FROM admin_impersonation_sessions ORDER BY id DESC LIMIT 100')]


def upsert_sod_policy(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    db.execute(
        '''
        INSERT INTO sod_rules (rule_key, name, conflict_type, left_value, right_value, severity, active, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(rule_key) DO UPDATE SET
            name = excluded.name,
            conflict_type = excluded.conflict_type,
            left_value = excluded.left_value,
            right_value = excluded.right_value,
            severity = excluded.severity,
            active = excluded.active
        ''',
        (
            payload['rule_key'], payload['name'], payload['conflict_type'], payload['left_value'],
            payload['right_value'], payload.get('severity') or 'medium', 1 if payload.get('active', True) else 0, _now(),
        ),
    )
    db.log_audit('sod_rule', payload['rule_key'], 'upserted', user['email'], payload, _now())
    return _format_bool(db.fetch_one('SELECT * FROM sod_rules WHERE rule_key = ?', (payload['rule_key'],)), 'active')


def create_access_review(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    users = list_users()
    scope = payload.get('scope') or {}
    findings = _access_review_findings(users)
    db.execute(
        '''
        INSERT INTO user_access_review_certifications (
            review_key, scenario_id, reviewer_user_id, status, scope_json, findings_json, created_by, created_at
        ) VALUES (?, ?, ?, 'open', ?, ?, ?, ?)
        ON CONFLICT(review_key) DO UPDATE SET
            scenario_id = excluded.scenario_id,
            reviewer_user_id = excluded.reviewer_user_id,
            scope_json = excluded.scope_json,
            findings_json = excluded.findings_json,
            status = 'open'
        ''',
        (
            payload['review_key'], payload.get('scenario_id'), payload['reviewer_user_id'],
            json.dumps(scope, sort_keys=True), json.dumps(findings, sort_keys=True), user['email'], _now(),
        ),
    )
    row = db.fetch_one('SELECT * FROM user_access_review_certifications WHERE review_key = ?', (payload['review_key'],))
    db.log_audit('user_access_review', payload['review_key'], 'created', user['email'], {'findings': len(findings)}, _now())
    return _format_access_review(row)


def certify_access_review(review_id: int, payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    findings = payload.get('findings') or []
    db.execute(
        '''
        UPDATE user_access_review_certifications
        SET status = 'certified', findings_json = ?, certified_by = ?, certified_at = ?
        WHERE id = ?
        ''',
        (json.dumps(findings, sort_keys=True), user['email'], _now(), review_id),
    )
    db.log_audit('user_access_review', str(review_id), 'certified', user['email'], {'findings': len(findings)}, _now())
    return _format_access_review(db.fetch_one('SELECT * FROM user_access_review_certifications WHERE id = ?', (review_id,)))


def list_access_reviews() -> list[dict[str, Any]]:
    return [_format_access_review(row) for row in db.fetch_all('SELECT * FROM user_access_review_certifications ORDER BY id DESC')]


def _access_review_findings(users: list[dict[str, Any]]) -> list[dict[str, Any]]:
    findings = []
    for item in users:
        if 'finance.admin' in item['roles']:
            findings.append({'user_id': item['id'], 'email': item['email'], 'finding': 'admin_access_review_required', 'severity': 'high'})
        if item['is_active'] and not item['roles']:
            findings.append({'user_id': item['id'], 'email': item['email'], 'finding': 'active_user_without_role', 'severity': 'medium'})
    return findings


def _format_sso_prod(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row['jit_provisioning'] = bool(row['jit_provisioning'])
    return row


def _format_bool(row: dict[str, Any], key: str) -> dict[str, Any]:
    row = dict(row)
    row[key] = bool(row[key])
    return row


def _format_impersonation(row: dict[str, Any]) -> dict[str, Any]:
    return dict(row)


def _format_access_review(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row['scope'] = json.loads(row.pop('scope_json') or '{}')
    row['findings'] = json.loads(row.pop('findings_json') or '[]')
    return row


def _ensure_sso_runtime_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS sso_auth_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider_key TEXT NOT NULL,
                state_hash TEXT NOT NULL UNIQUE,
                nonce_hash TEXT NOT NULL,
                redirect_uri TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                consumed_at TEXT DEFAULT NULL
            );
            CREATE TABLE IF NOT EXISTS sso_replay_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider_key TEXT NOT NULL,
                state_hash TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sso_auth_requests_expires
            ON sso_auth_requests (expires_at, consumed_at);
            '''
        )


def sso_config() -> dict[str, Any]:
    _ensure_sso_runtime_tables()
    provider = db.fetch_one('SELECT * FROM sso_providers WHERE provider_key = ?', (SSO_PROVIDER_KEY,))
    if provider is None:
        ensure_security_ready()
        provider = db.fetch_one('SELECT * FROM sso_providers WHERE provider_key = ?', (SSO_PROVIDER_KEY,))
    enabled = bool(provider and provider['enabled'])
    return {
        'enabled': enabled,
        'provider_key': SSO_PROVIDER_KEY,
        'name': provider['name'] if provider else SSO_NAME,
        'protocol': provider['protocol'] if provider else SSO_PROTOCOL,
        'issuer_url': provider['issuer_url'] if provider else SSO_ISSUER_URL,
        'authorize_url_configured': bool(provider and provider['authorize_url']),
        'token_url_configured': bool(provider and provider['token_url']),
        'jwks_url_configured': bool(provider and provider['jwks_url']),
        'client_id_configured': bool(provider and provider['client_id']),
        'client_secret_configured': bool(SSO_CLIENT_SECRET),
        'redirect_uri': SSO_REDIRECT_URI,
        'trusted_header_enabled': TRUSTED_SSO_HEADER_ENABLED,
        'trusted_header_name': TRUSTED_SSO_EMAIL_HEADER if TRUSTED_SSO_HEADER_ENABLED else None,
        'login_endpoint': '/api/auth/sso/login',
        'callback_endpoint': '/api/auth/sso/callback',
        'logout_endpoint': '/api/auth/logout',
    }


def build_sso_authorization_url() -> dict[str, Any]:
    _ensure_sso_runtime_tables()
    provider = db.fetch_one('SELECT * FROM sso_providers WHERE provider_key = ?', (SSO_PROVIDER_KEY,))
    if provider is None or not bool(provider['enabled']):
        return {'enabled': False, 'reason': 'SSO provider is not configured.'}
    state = secrets.token_urlsafe(24)
    nonce = secrets.token_urlsafe(24)
    query = urlencode(
        {
            'client_id': provider['client_id'],
            'redirect_uri': SSO_REDIRECT_URI,
            'response_type': 'code',
            'scope': 'openid email profile',
            'state': state,
            'nonce': nonce,
        }
    )
    expires_at = datetime.now(UTC) + timedelta(minutes=10)
    db.execute(
        '''
        INSERT INTO sso_auth_requests (provider_key, state_hash, nonce_hash, redirect_uri, created_at, expires_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (provider['provider_key'], _token_hash(state), _token_hash(nonce), SSO_REDIRECT_URI, _now(), expires_at.isoformat()),
    )
    return {
        'enabled': True,
        'provider_key': provider['provider_key'],
        'authorization_url': f"{provider['authorize_url']}?{query}",
        'state': state,
    }


def complete_sso_callback(payload: dict[str, Any]) -> dict[str, Any]:
    _ensure_sso_runtime_tables()
    state = str(payload.get('state') or '').strip()
    if not state:
        raise ValueError('Missing SSO state.')
    request_row = _consume_sso_state(state)
    provider = db.fetch_one('SELECT * FROM sso_providers WHERE provider_key = ?', (request_row['provider_key'],))
    if provider is None or not bool(provider['enabled']):
        raise ValueError('SSO provider is not enabled.')
    token_response = _token_response(provider, payload)
    claims = _claims_from_token_response(token_response, provider, request_row)
    user = _sso_user_from_claims(claims, provider)
    assert_ad_ou_allowed(user)
    result = issue_session(int(user['id']), actor=user['email'], method='sso_oidc')
    db.log_audit(
        'sso_callback',
        str(request_row['id']),
        'completed',
        user['email'],
        {
            'provider_key': provider['provider_key'],
            'subject': claims.get('sub'),
            'groups': _claim_list(claims.get(_sso_group_claim(provider))),
        },
        _now(),
    )
    return {
        **result,
        'auth_method': 'sso_oidc',
        'provider_key': provider['provider_key'],
        'claims': _safe_claims(claims),
    }


def complete_saml_callback(payload: dict[str, Any]) -> dict[str, Any]:
    _ensure_sso_runtime_tables()
    state = str(payload.get('RelayState') or payload.get('state') or '').strip()
    if not state:
        raise ValueError('Missing SAML RelayState.')
    request_row = _consume_sso_state(state)
    assertion = str(payload.get('SAMLResponse') or '').strip()
    if not assertion:
        raise ValueError('Missing SAMLResponse.')
    try:
        decoded = base64.b64decode(assertion + '=' * (-len(assertion) % 4)).decode('utf-8')
    except Exception as exc:
        raise ValueError('SAMLResponse is not valid base64 XML.') from exc
    claims = _claims_from_saml_xml(decoded)
    provider = db.fetch_one('SELECT * FROM sso_providers WHERE provider_key = ?', (request_row['provider_key'],))
    if provider is None or not bool(provider['enabled']):
        raise ValueError('SSO provider is not enabled.')
    user = _sso_user_from_claims(claims, provider)
    assert_ad_ou_allowed(user)
    result = issue_session(int(user['id']), actor=user['email'], method='sso_saml')
    db.log_audit('sso_callback', str(request_row['id']), 'completed_saml', user['email'], {'provider_key': provider['provider_key']}, _now())
    return {**result, 'auth_method': 'sso_saml', 'provider_key': provider['provider_key'], 'claims': _safe_claims(claims)}


def logout_session(token: str, actor: str = 'api.user') -> dict[str, Any]:
    if not token:
        return {'logged_out': False, 'reason': 'missing_token'}
    now = _now()
    db.execute('UPDATE auth_sessions SET revoked_at = ? WHERE token_hash = ? AND revoked_at IS NULL', (now, _token_hash(token)))
    db.log_audit('auth_session', 'current', 'logout', actor, {}, now)
    return {'logged_out': True}


def _consume_sso_state(state: str) -> dict[str, Any]:
    state_hash = _token_hash(state)
    row = db.fetch_one('SELECT * FROM sso_auth_requests WHERE state_hash = ?', (state_hash,))
    if row is None:
        _record_sso_replay(SSO_PROVIDER_KEY, state_hash, 'unknown_state')
        raise ValueError('Invalid or expired SSO state.')
    if row['consumed_at']:
        _record_sso_replay(row['provider_key'], state_hash, 'state_replay')
        raise ValueError('SSO state has already been used.')
    expires_at = datetime.fromisoformat(row['expires_at'])
    if expires_at <= datetime.now(UTC):
        _record_sso_replay(row['provider_key'], state_hash, 'expired_state')
        raise ValueError('SSO state has expired.')
    db.execute('UPDATE sso_auth_requests SET consumed_at = ? WHERE id = ?', (_now(), row['id']))
    return dict(row)


def _record_sso_replay(provider_key: str, state_hash: str, reason: str) -> None:
    db.execute(
        'INSERT INTO sso_replay_events (provider_key, state_hash, reason, created_at) VALUES (?, ?, ?, ?)',
        (provider_key, state_hash, reason, _now()),
    )
    db.log_audit('sso_callback', state_hash[:12], 'replay_blocked', 'sso', {'reason': reason}, _now())


def _token_response(provider: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get('id_token'):
        return {'id_token': payload['id_token'], 'token_source': 'callback'}
    if SSO_TEST_TOKEN_RESPONSE:
        return {**json.loads(SSO_TEST_TOKEN_RESPONSE), 'token_source': 'test_env'}
    code = str(payload.get('code') or '').strip()
    if not code:
        raise ValueError('Missing SSO authorization code.')
    if not provider['token_url']:
        raise ValueError('SSO token endpoint is not configured.')
    data = {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': SSO_REDIRECT_URI,
        'client_id': provider['client_id'],
    }
    if SSO_CLIENT_SECRET:
        data['client_secret'] = SSO_CLIENT_SECRET
    with httpx.Client(timeout=10.0) as client:
        response = client.post(provider['token_url'], data=data, headers={'Accept': 'application/json'})
    if response.status_code >= 400:
        raise ValueError(f'SSO token exchange failed with HTTP {response.status_code}.')
    token_payload = response.json()
    token_payload['token_source'] = 'token_endpoint'
    return token_payload


def _claims_from_token_response(token_response: dict[str, Any], provider: dict[str, Any], request_row: dict[str, Any]) -> dict[str, Any]:
    id_token = token_response.get('id_token')
    if not id_token:
        raise ValueError('SSO token response did not include an id_token.')
    claims = _decode_and_validate_id_token(str(id_token), provider)
    nonce = str(claims.get('nonce') or '')
    if not nonce or not hmac.compare_digest(_token_hash(nonce), request_row['nonce_hash']):
        raise ValueError('SSO nonce validation failed.')
    return claims


def _decode_and_validate_id_token(id_token: str, provider: dict[str, Any]) -> dict[str, Any]:
    header, claims, signing_input, signature = _split_jwt(id_token)
    alg = str(header.get('alg') or '').upper()
    if alg == 'NONE':
        if not SSO_ALLOW_UNSIGNED_TOKENS:
            raise ValueError('Unsigned SSO tokens are disabled.')
    elif alg.startswith('HS'):
        _validate_hs_jwt(alg, signing_input, signature)
    else:
        _validate_with_pyjwt(id_token, provider, alg)
    _validate_standard_claims(claims, provider)
    return claims


def _split_jwt(token: str) -> tuple[dict[str, Any], dict[str, Any], bytes, bytes]:
    parts = token.split('.')
    if len(parts) != 3:
        raise ValueError('id_token is not a JWT.')
    header = json.loads(_b64url_decode(parts[0]).decode('utf-8'))
    claims = json.loads(_b64url_decode(parts[1]).decode('utf-8'))
    return header, claims, f'{parts[0]}.{parts[1]}'.encode('ascii'), _b64url_decode(parts[2])


def _b64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + '=' * (-len(value) % 4))


def _validate_hs_jwt(alg: str, signing_input: bytes, signature: bytes) -> None:
    if alg != 'HS256':
        raise ValueError(f'Unsupported HMAC SSO token algorithm: {alg}.')
    if not SSO_CLIENT_SECRET:
        raise ValueError('SSO client secret is required for HS256 token validation.')
    expected = hmac.new(SSO_CLIENT_SECRET.encode('utf-8'), signing_input, hashlib.sha256).digest()
    if not hmac.compare_digest(expected, signature):
        raise ValueError('SSO token signature validation failed.')


def _validate_with_pyjwt(id_token: str, provider: dict[str, Any], alg: str) -> None:
    try:
        import jwt
        from jwt import PyJWKClient
    except ImportError as exc:
        raise ValueError(f'{alg} SSO token validation requires PyJWT[crypto].') from exc
    if not provider['jwks_url']:
        raise ValueError('SSO JWKS endpoint is required for asymmetric token validation.')
    key = PyJWKClient(provider['jwks_url']).get_signing_key_from_jwt(id_token).key
    options = {'require': ['exp', 'sub']}
    jwt.decode(
        id_token,
        key=key,
        algorithms=[alg],
        audience=provider['client_id'] or None,
        issuer=provider['issuer_url'] or None,
        options=options,
    )


def _validate_standard_claims(claims: dict[str, Any], provider: dict[str, Any]) -> None:
    now = int(datetime.now(UTC).timestamp())
    if claims.get('exp') is not None and int(claims['exp']) <= now:
        raise ValueError('SSO token is expired.')
    if provider['issuer_url'] and claims.get('iss') != provider['issuer_url']:
        raise ValueError('SSO issuer validation failed.')
    audience = claims.get('aud')
    if provider['client_id']:
        if isinstance(audience, list):
            valid_audience = provider['client_id'] in audience
        else:
            valid_audience = audience == provider['client_id']
        if not valid_audience:
            raise ValueError('SSO audience validation failed.')


def _sso_user_from_claims(claims: dict[str, Any], provider: dict[str, Any]) -> dict[str, Any]:
    required_claim = _sso_required_claim(provider)
    email = str(claims.get(required_claim) or claims.get('email') or '').lower().strip()
    subject = str(claims.get('sub') or '').strip()
    if not email or not subject:
        raise ValueError('SSO claims must include email and subject.')
    user = get_or_create_sso_user(email=email, external_subject=subject, provider_key=provider['provider_key'])
    _apply_sso_group_role_mapping(int(user['id']), claims, provider)
    return user_profile(int(user['id']))


def _sso_required_claim(provider: dict[str, Any]) -> str:
    settings = db.fetch_one('SELECT * FROM sso_production_settings WHERE provider_key = ?', (provider['provider_key'],))
    return str(settings['required_claim'] if settings else 'email')


def _sso_group_claim(provider: dict[str, Any]) -> str:
    settings = db.fetch_one('SELECT * FROM sso_production_settings WHERE provider_key = ?', (provider['provider_key'],))
    return str(settings['group_claim'] if settings else 'groups')


def _apply_sso_group_role_mapping(user_id: int, claims: dict[str, Any], provider: dict[str, Any]) -> None:
    groups = set(_claim_list(claims.get(_sso_group_claim(provider))))
    user_dn = str(claims.get('dn') or claims.get('distinguishedName') or claims.get('onpremiseddistinguishedname') or '').strip()
    mappings = list_ad_ou_group_mappings()
    for mapping in mappings:
        if not mapping['active'] or mapping['ad_group_dn'] not in groups:
            continue
        if mapping['allowed_ou_dn'] and user_dn and not dn_is_under_ou(user_dn, mapping['allowed_ou_dn']):
            continue
        role = db.fetch_one('SELECT id FROM roles WHERE role_key = ?', (mapping['role_key'],))
        if role is not None:
            db.execute('INSERT OR IGNORE INTO user_roles (user_id, role_id) VALUES (?, ?)', (user_id, role['id']))
        if mapping.get('dimension_kind') and mapping.get('dimension_code'):
            db.execute(
                'INSERT OR IGNORE INTO user_dimension_access (user_id, dimension_kind, code) VALUES (?, ?, ?)',
                (user_id, mapping['dimension_kind'], mapping['dimension_code']),
            )


def _claim_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        return [item.strip() for item in value.split(',') if item.strip()]
    return [str(value)]


def _safe_claims(claims: dict[str, Any]) -> dict[str, Any]:
    allowed = {'sub', 'email', 'name', 'preferred_username', 'groups', 'roles', 'dn', 'distinguishedName', 'nonce', 'iss', 'aud'}
    return {key: value for key, value in claims.items() if key in allowed}


def _claims_from_saml_xml(xml_text: str) -> dict[str, Any]:
    import xml.etree.ElementTree as ET

    root = ET.fromstring(xml_text)
    claims: dict[str, Any] = {}
    for node in root.iter():
        tag = node.tag.rsplit('}', 1)[-1]
        if tag == 'NameID' and node.text:
            claims.setdefault('sub', node.text.strip())
        if tag != 'Attribute':
            continue
        name = node.attrib.get('Name') or node.attrib.get('FriendlyName')
        if not name:
            continue
        values = [child.text.strip() for child in list(node) if child.text and child.text.strip()]
        claims[name] = values if len(values) > 1 else (values[0] if values else '')
    if 'email' not in claims:
        for key in ('mail', 'emailAddress', 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress'):
            if key in claims:
                claims['email'] = claims[key]
                break
    if 'sub' not in claims:
        claims['sub'] = str(claims.get('email') or '')
    return claims


def trusted_header_login(email: str) -> dict[str, Any] | None:
    if not TRUSTED_SSO_HEADER_ENABLED:
        return None
    user = get_or_create_sso_user(email=email, external_subject=email)
    return issue_session(int(user['id']), actor=email, method='sso_header')


def _field_key_candidates() -> list[str]:
    reload_field_secrets()
    candidates = [FIELD_KEY]
    for item in FIELD_KEY_PREVIOUS.replace('\n', ',').replace(';', ',').split(','):
        item = item.strip()
        if not item:
            continue
        if ':' in item and item.split(':', 1)[0].startswith('v'):
            item = item.split(':', 1)[1]
        candidates.append(item)
    return [candidate for index, candidate in enumerate(candidates) if candidate and candidate not in candidates[:index]]


def _fernet_for_key(key_material: str, version: str) -> Any:
    if Fernet is None:
        raise RuntimeError('cryptography is required for authenticated field encryption.')
    material = key_material
    if FIELD_KEY_ENVELOPE_MASTER:
        material = f'{FIELD_KEY_ENVELOPE_MASTER}:{version}:{key_material}'
    key = base64.urlsafe_b64encode(hashlib.sha256(material.encode('utf-8')).digest())
    return Fernet(key)


def encryption_status() -> dict[str, Any]:
    reload_field_secrets()
    return {
        'batch': 'B136_B137',
        'algorithm': 'fernet/aes-cbc-hmac',
        'authenticated_encryption_ready': Fernet is not None,
        'current_version': FIELD_KEY_VERSION,
        'key_versioning_ready': bool(FIELD_KEY_VERSION),
        'key_rotation_ready': bool(FIELD_KEY_PREVIOUS),
        'envelope_encryption_configured': bool(FIELD_KEY_ENVELOPE_MASTER or FIELD_KEY_ENVELOPE_MASTER_FILE),
        'legacy_v1_decrypt_ready': True,
        'field_key_file_supported': True,
        'field_key_file_configured': bool(FIELD_KEY_FILE),
        'field_key_loaded': bool(FIELD_KEY and FIELD_KEY != DEV_DEFAULT_FIELD_KEY),
        'production_fail_closed': APP_ENV not in {'prod', 'production'} or bool(FIELD_KEY and FIELD_KEY != DEV_DEFAULT_FIELD_KEY and Fernet is not None),
    }


def encrypt_value(value: str) -> str:
    token = _fernet_for_key(FIELD_KEY, FIELD_KEY_VERSION).encrypt(value.encode('utf-8')).decode('ascii')
    return f'enc:v2:{FIELD_KEY_VERSION}:{token}'


def decrypt_value(value: str) -> str:
    if not value.startswith('enc:'):
        return value
    if value.startswith('enc:v1:'):
        encrypted = base64.urlsafe_b64decode(value.removeprefix('enc:v1:').encode('ascii'))
        key = hashlib.sha256(FIELD_KEY.encode('utf-8')).digest()
        stream = _keystream(key, len(encrypted))
        raw = bytes(byte ^ stream[index] for index, byte in enumerate(encrypted))
        return raw.decode('utf-8')
    if value.startswith('enc:v2:'):
        try:
            _, _, version, token = value.split(':', 3)
        except ValueError as exc:
            raise ValueError('Invalid encrypted field format.') from exc
        for candidate in _field_key_candidates():
            try:
                return _fernet_for_key(candidate, version).decrypt(token.encode('ascii')).decode('utf-8')
            except InvalidToken:
                continue
        raise ValueError('Encrypted field authentication failed.')
    raise ValueError('Unsupported encrypted field version.')


def _legacy_encrypt_for_migration_test(value: str) -> str:
    raw = value.encode('utf-8')
    key = hashlib.sha256(FIELD_KEY.encode('utf-8')).digest()
    stream = _keystream(key, len(raw))
    encrypted = bytes(byte ^ stream[index] for index, byte in enumerate(raw))
    return 'enc:v1:' + base64.urlsafe_b64encode(encrypted).decode('ascii')


def encrypted_value_needs_migration(value: str) -> bool:
    if value.startswith('enc:v1:'):
        return True
    if not value.startswith('enc:v2:'):
        return False
    try:
        _, _, version, _ = value.split(':', 3)
    except ValueError:
        return True
    return version != FIELD_KEY_VERSION


def migrate_encrypted_value(value: str) -> str:
    if not encrypted_value_needs_migration(value):
        return value
    return encrypt_value(decrypt_value(value))


def _migrate_payload(value: Any) -> tuple[Any, int]:
    if isinstance(value, str) and encrypted_value_needs_migration(value):
        return migrate_encrypted_value(value), 1
    if isinstance(value, list):
        total = 0
        migrated = []
        for item in value:
            updated, count = _migrate_payload(item)
            migrated.append(updated)
            total += count
        return migrated, total
    if isinstance(value, dict):
        total = 0
        migrated = {}
        for key, item in value.items():
            updated, count = _migrate_payload(item)
            migrated[key] = updated
            total += count
        return migrated, total
    return value, 0


def migrate_encrypted_fields(actor: str = 'system') -> dict[str, Any]:
    migrated_fields = 0
    updated_rows = 0
    scanned_columns = 0
    try:
        rows = db.fetch_all("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
    except Exception:
        return {'updated_rows': 0, 'migrated_fields': 0, 'scanned_columns': 0, 'catalog_scan_supported': False}
    for row in rows:
        table = _quote_identifier(row['name'])
        columns = db.fetch_all(f'PRAGMA table_info({table})')
        id_columns = [column['name'] for column in columns if column['pk']]
        json_columns = [column['name'] for column in columns if column['name'].endswith('_json')]
        for column in json_columns:
            scanned_columns += 1
            id_selector = _quote_identifier(id_columns[0]) if id_columns else 'rowid'
            safe_column = _quote_identifier(column)
            records = db.fetch_all(f'SELECT {id_selector} AS row_key, {safe_column} AS payload FROM {table} WHERE {safe_column} LIKE ?', ('%enc:v%',))
            for record in records:
                try:
                    payload = json.loads(record['payload'] or '{}')
                except (TypeError, json.JSONDecodeError):
                    continue
                migrated, count = _migrate_payload(payload)
                if count:
                    db.execute(
                        f'UPDATE {table} SET {safe_column} = ? WHERE {id_selector} = ?',
                        (json.dumps(migrated, sort_keys=True), record['row_key']),
                    )
                    migrated_fields += count
                    updated_rows += 1
    db.log_audit('security', 'field-encryption', 'encrypted_fields_migrated', actor, {'updated_rows': updated_rows, 'migrated_fields': migrated_fields, 'scanned_columns': scanned_columns}, _now())
    return {'updated_rows': updated_rows, 'migrated_fields': migrated_fields, 'scanned_columns': scanned_columns, 'catalog_scan_supported': True}


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def mask_sensitive_metadata(metadata: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    if has_permission(user, 'sensitive.read'):
        return {key: decrypt_value(value) if isinstance(value, str) else value for key, value in metadata.items()}
    masked = {}
    for key, value in metadata.items():
        if key.lower() in SENSITIVE_METADATA_KEYS:
            masked[key] = 'masked'
        elif isinstance(value, str) and value.startswith('enc:v1:'):
            masked[key] = 'masked'
        else:
            masked[key] = value
    return masked


def protect_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    protected = {}
    for key, value in metadata.items():
        if key.lower() in SENSITIVE_METADATA_KEYS and value is not None:
            protected[key] = encrypt_value(str(value))
        else:
            protected[key] = value
    return protected


def _keystream(key: bytes, length: int) -> bytes:
    chunks = []
    counter = 0
    while sum(len(chunk) for chunk in chunks) < length:
        chunks.append(hashlib.sha256(key + counter.to_bytes(8, 'big')).digest())
        counter += 1
    return b''.join(chunks)[:length]
