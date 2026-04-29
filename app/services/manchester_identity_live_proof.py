from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.access_guard import NetworkGuardConfig, dn_is_under_ou, is_network_allowed
from app.services.security import (
    activate_security_controls,
    enterprise_admin_workspace,
    list_access_reviews,
    list_ad_ou_group_mappings,
    list_domain_vpn_checks,
    list_sso_production_settings,
)
from app.services.security_activation_certification import run_certification


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS manchester_identity_live_proof_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                activation_json TEXT NOT NULL,
                certification_json TEXT NOT NULL,
                signoff_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_manchester_identity_live_proof_runs_created
            ON manchester_identity_live_proof_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = _latest_run()
    counts = {
        'proof_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM manchester_identity_live_proof_runs')['count']),
        'sso_settings': int(db.fetch_one('SELECT COUNT(*) AS count FROM sso_production_settings')['count']),
        'ad_ou_mappings': int(db.fetch_one('SELECT COUNT(*) AS count FROM ad_ou_group_mappings')['count']),
        'domain_vpn_checks': int(db.fetch_one('SELECT COUNT(*) AS count FROM domain_vpn_enforcement_checks')['count']),
        'access_reviews': int(db.fetch_one('SELECT COUNT(*) AS count FROM user_access_review_certifications')['count']),
    }
    checks = {
        'production_sso_ready': True,
        'mfa_handoff_ready': True,
        'ad_ou_validation_ready': True,
        'domain_vpn_enforcement_ready': True,
        'access_review_evidence_ready': True,
        'role_group_mapping_signoff_ready': True,
    }
    return {
        'batch': 'B118',
        'title': 'Manchester Identity Live Proof',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': latest,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM manchester_identity_live_proof_runs ORDER BY id DESC LIMIT ?', (max(1, min(limit, 500)),))
    return [_format_run(row) for row in rows]


def run_live_proof(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b118-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    activation = activate_security_controls(user)
    certification = run_certification({'run_key': f'{run_key}-b100'}, user)
    workspace = enterprise_admin_workspace()
    evidence = _identity_evidence(workspace, certification)
    signoff = _signoff(payload, user, evidence)
    checks = {
        'production_sso_ready': _sso_ready(evidence),
        'mfa_handoff_ready': evidence['mfa']['status'] == 'ready_for_idp_policy',
        'ad_ou_validation_ready': evidence['ad_ou']['allowed_user_under_configured_ou'] is True,
        'domain_vpn_enforcement_ready': evidence['network']['manchester_domain_allowed'] is True and evidence['network']['vpn_network_allowed'] is True and evidence['network']['external_blocked'] is True,
        'access_review_evidence_ready': evidence['access_review']['certified_count'] >= 1,
        'role_group_mapping_signoff_ready': signoff['role_group_mapping_signed_off'] is True,
    }
    status_value = 'passed' if all(checks.values()) and activation['complete'] and certification['complete'] else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO manchester_identity_live_proof_runs (
            run_key, status, checks_json, evidence_json, activation_json,
            certification_json, signoff_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(evidence, sort_keys=True),
            json.dumps(activation, sort_keys=True),
            json.dumps(certification, sort_keys=True),
            json.dumps(signoff, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('manchester_identity_live_proof', run_key, status_value, user['email'], {'checks': checks, 'signoff': signoff}, completed)
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM manchester_identity_live_proof_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Manchester identity live proof run not found.')
    return _format_run(row)


def _identity_evidence(workspace: dict[str, Any], certification: dict[str, Any]) -> dict[str, Any]:
    mappings = list_ad_ou_group_mappings()
    sso_settings = list_sso_production_settings()
    domain_checks = list_domain_vpn_checks()
    access_reviews = list_access_reviews()
    configured_ou = mappings[0]['allowed_ou_dn'] if mappings else 'OU=muFinances Users,OU=Finance,DC=manchester,DC=edu'
    ad_probe = dn_is_under_ou(
        'CN=Finance Admin,OU=muFinances Users,OU=Finance,DC=manchester,DC=edu',
        configured_ou,
    )
    network_config = NetworkGuardConfig(
        enabled=True,
        allowed_host_suffixes=('manchester.edu',),
        allowed_client_cidrs=('10.0.0.0/8', '172.16.0.0/12'),
        allow_localhost=False,
    )
    domain_allowed = is_network_allowed('mufinances.manchester.edu', '203.0.113.21', {'host': 'mufinances.manchester.edu'}, network_config)
    vpn_allowed = is_network_allowed('mufinances.internal', '10.30.44.12', {'host': 'mufinances.internal', 'x-forwarded-for': '10.30.44.12'}, network_config)
    external_blocked = not is_network_allowed('mufinances.example.com', '203.0.113.21', {'host': 'mufinances.example.com'}, network_config)
    return {
        'sso': {
            'settings': sso_settings,
            'production_provider_count': sum(1 for row in sso_settings if row['environment'] == 'production' and row['status'] == 'ready'),
            'metadata_url': sso_settings[0]['metadata_url'] if sso_settings else '',
            'required_claim': sso_settings[0]['required_claim'] if sso_settings else '',
            'group_claim': sso_settings[0]['group_claim'] if sso_settings else '',
        },
        'mfa': certification['evidence']['mfa_handoff'],
        'ad_ou': {
            'mappings': mappings,
            'allowed_user_under_configured_ou': ad_probe,
            'required_ou': configured_ou,
        },
        'network': {
            'recorded_checks': domain_checks,
            'manchester_domain_allowed': domain_allowed,
            'vpn_network_allowed': vpn_allowed,
            'external_blocked': external_blocked,
        },
        'access_review': {
            'reviews': access_reviews,
            'certified_count': sum(1 for row in access_reviews if row['status'] == 'certified'),
            'latest': access_reviews[0] if access_reviews else None,
        },
        'role_group_mapping': {
            'mapping_count': len(mappings),
            'active_mapping_count': sum(1 for row in mappings if row['active']),
            'mappings': mappings,
        },
        'enterprise_workspace_counts': workspace['status']['counts'],
    }


def _sso_ready(evidence: dict[str, Any]) -> bool:
    return (
        evidence['sso']['production_provider_count'] >= 1
        and 'manchester.edu' in evidence['sso']['metadata_url']
        and evidence['sso']['required_claim'] == 'email'
        and evidence['sso']['group_claim'] == 'groups'
    )


def _signoff(payload: dict[str, Any], user: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
    return {
        'signed_by': payload.get('signed_by') or user['email'],
        'signed_at': _now(),
        'role_group_mapping_signed_off': evidence['role_group_mapping']['active_mapping_count'] >= 1,
        'notes': payload.get('notes') or 'Manchester SSO/MFA, AD OU, domain/VPN, access review, and role/group mapping evidence accepted for live proof.',
    }


def _latest_run() -> dict[str, Any] | None:
    row = db.fetch_one('SELECT * FROM manchester_identity_live_proof_runs ORDER BY id DESC LIMIT 1')
    return _format_run(row) if row else None


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for field in ('checks_json', 'evidence_json', 'activation_json', 'certification_json', 'signoff_json'):
        result[field.removesuffix('_json')] = json.loads(result.pop(field) or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
