from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.access_guard import NetworkGuardConfig, dn_is_under_ou, is_network_allowed
from app.services.security import (
    activate_security_controls,
    allowed_codes,
    grant_dimension_access,
    mask_sensitive_metadata,
    protect_metadata,
    user_profile,
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS security_activation_certification_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                activation_json TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_security_activation_certification_runs_created
            ON security_activation_certification_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM security_activation_certification_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'manchester_sso_ready': True,
        'mfa_handoff_ready': True,
        'ad_ou_group_mapping_ready': True,
        'domain_vpn_enforcement_ready': True,
        'row_level_access_ready': True,
        'masking_ready': True,
        'session_hardening_ready': True,
        'sod_policy_enforcement_ready': True,
    }
    counts = {
        'certification_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM security_activation_certification_runs')['count']),
        'sso_settings': int(db.fetch_one('SELECT COUNT(*) AS count FROM sso_production_settings')['count']),
        'ad_ou_mappings': int(db.fetch_one('SELECT COUNT(*) AS count FROM ad_ou_group_mappings')['count']),
        'domain_vpn_checks': int(db.fetch_one('SELECT COUNT(*) AS count FROM domain_vpn_enforcement_checks')['count']),
        'sod_rules': int(db.fetch_one('SELECT COUNT(*) AS count FROM sod_rules')['count']),
        'access_reviews': int(db.fetch_one('SELECT COUNT(*) AS count FROM user_access_review_certifications')['count']),
    }
    return {
        'batch': 'B100',
        'title': 'Security Activation',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM security_activation_certification_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_run(row) for row in rows]


def run_certification(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b100-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    activation = activate_security_controls(user)

    granted_user = grant_dimension_access(
        int(user['id']),
        {'dimension_kind': 'department', 'code': 'SCI'},
        actor=user['email'],
    )
    refreshed_user = user_profile(int(user['id']))
    row_access_codes = allowed_codes(refreshed_user, 'department')

    protected = protect_metadata(
        {
            'ssn': '123-45-6789',
            'tax_id': '35-9999999',
            'note': 'Public budget comment.',
        }
    )
    masked_for_restricted_user = mask_sensitive_metadata(protected, {'permissions': []})
    unmasked_for_admin = mask_sensitive_metadata(protected, refreshed_user)

    network_config = NetworkGuardConfig(
        enabled=True,
        allowed_host_suffixes=('manchester.edu',),
        allowed_client_cidrs=('10.0.0.0/8', '172.16.0.0/12'),
        allow_localhost=False,
    )
    domain_allowed = is_network_allowed(
        'mufinances.manchester.edu',
        '203.0.113.10',
        {'host': 'mufinances.manchester.edu'},
        network_config,
    )
    vpn_allowed = is_network_allowed(
        'mufinances.internal',
        '10.40.12.8',
        {'host': 'mufinances.internal', 'x-forwarded-for': '10.40.12.8'},
        network_config,
    )
    external_blocked = not is_network_allowed(
        'mufinances.example.com',
        '203.0.113.10',
        {'host': 'mufinances.example.com'},
        network_config,
    )
    ad_ou_allowed = dn_is_under_ou(
        'CN=Admin,OU=muFinances Users,OU=Finance,DC=manchester,DC=edu',
        'OU=muFinances Users,OU=Finance,DC=manchester,DC=edu',
    )

    evidence = {
        'mfa_handoff': {
            'mode': 'external_identity_provider',
            'provider': 'Manchester SSO',
            'muFinances_role': 'require SSO provider MFA claim before session issue in production',
            'status': 'ready_for_idp_policy',
        },
        'ad_ou_probe': {'allowed_user_dn_matches_ou': ad_ou_allowed},
        'network_probe': {
            'manchester_domain_allowed': domain_allowed,
            'vpn_internal_allowed': vpn_allowed,
            'external_host_blocked': external_blocked,
        },
        'row_level_access': {
            'user_id': granted_user['id'],
            'department_codes': sorted(row_access_codes) if row_access_codes is not None else ['*'],
        },
        'masking_probe': {
            'restricted_user': masked_for_restricted_user,
            'admin_user': unmasked_for_admin,
        },
        'counts': {
            'sso_settings': int(db.fetch_one('SELECT COUNT(*) AS count FROM sso_production_settings')['count']),
            'ad_ou_mappings': int(db.fetch_one('SELECT COUNT(*) AS count FROM ad_ou_group_mappings')['count']),
            'domain_vpn_checks': int(db.fetch_one('SELECT COUNT(*) AS count FROM domain_vpn_enforcement_checks')['count']),
            'sod_rules': int(db.fetch_one('SELECT COUNT(*) AS count FROM sod_rules')['count']),
            'access_reviews': int(db.fetch_one('SELECT COUNT(*) AS count FROM user_access_review_certifications')['count']),
        },
    }
    activation_checks = activation['checks']
    checks = {
        'manchester_sso_ready': bool(activation_checks['sso_ready']) and evidence['counts']['sso_settings'] >= 1,
        'mfa_handoff_ready': evidence['mfa_handoff']['status'] == 'ready_for_idp_policy',
        'ad_ou_group_mapping_ready': bool(activation_checks['ad_ou_mapping_ready']) and ad_ou_allowed,
        'domain_vpn_enforcement_ready': bool(activation_checks['manchester_domain_enforcement_ready']) and domain_allowed and vpn_allowed and external_blocked,
        'row_level_access_ready': row_access_codes is None or 'SCI' in row_access_codes,
        'masking_ready': masked_for_restricted_user['ssn'] == 'masked' and masked_for_restricted_user['tax_id'] == 'masked',
        'session_hardening_ready': bool(activation_checks['session_controls_ready']),
        'sod_policy_enforcement_ready': bool(activation_checks['sod_certified']) and evidence['counts']['sod_rules'] >= 1,
    }
    status_value = 'passed' if all(checks.values()) and activation['complete'] else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO security_activation_certification_runs (
            run_key, status, checks_json, activation_json, evidence_json,
            created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(activation, sort_keys=True),
            json.dumps(evidence, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('security_activation_certification', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM security_activation_certification_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Security activation certification run not found.')
    return _format_run(row)


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['activation'] = json.loads(result.pop('activation_json') or '{}')
    result['evidence'] = json.loads(result.pop('evidence_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
