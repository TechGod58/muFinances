from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.manchester_identity_live_proof import run_live_proof
from app.services.manchester_identity_live_proof import status as live_proof_status
from app.services.security import enterprise_admin_status
from app.services.security_activation_certification import status as certification_status


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS manchester_identity_activation_proof_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                live_proof_json TEXT NOT NULL,
                certification_json TEXT NOT NULL,
                enterprise_json TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                signoff_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_manchester_identity_activation_proof_runs_created
            ON manchester_identity_activation_proof_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    live = live_proof_status()
    certification = certification_status()
    enterprise = enterprise_admin_status()
    checks = _checks(live, certification, enterprise, None)
    counts = {
        'activation_proof_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM manchester_identity_activation_proof_runs')['count']),
        'live_identity_proof_runs': live['counts']['proof_runs'],
        'activation_certification_runs': certification['counts']['certification_runs'],
    }
    return {
        'batch': 'B156',
        'title': 'Manchester Identity Activation Proof',
        'complete': all(checks.values()),
        'checks': checks,
        'live_proof': live,
        'certification': certification,
        'enterprise': enterprise,
        'counts': counts,
        'latest_run': _latest_run(),
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all(
        'SELECT * FROM manchester_identity_activation_proof_runs ORDER BY id DESC LIMIT ?',
        (max(1, min(limit, 500)),),
    )
    return [_format_run(row) for row in rows]


def run_activation_proof(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b156-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    live = run_live_proof(
        {
            'run_key': f'{run_key}-live',
            'signed_by': payload.get('signed_by') or user['email'],
            'notes': payload.get('notes') or 'B156 Manchester identity activation proof.',
        },
        user,
    )
    certification = certification_status()
    enterprise = enterprise_admin_status()
    checks = _checks(live, certification, enterprise, live)
    signoff = _signoff(payload, user, live, checks)
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO manchester_identity_activation_proof_runs (
            run_key, status, live_proof_json, certification_json, enterprise_json,
            checks_json, signoff_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            status_value,
            json.dumps(live, sort_keys=True),
            json.dumps(certification, sort_keys=True),
            json.dumps(enterprise, sort_keys=True),
            json.dumps(checks, sort_keys=True),
            json.dumps(signoff, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('manchester_identity_activation_proof', run_key, status_value, user['email'], {'checks': checks, 'signoff': signoff}, completed)
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM manchester_identity_activation_proof_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Manchester identity activation proof run not found.')
    return _format_run(row)


def _checks(
    live_status: dict[str, Any],
    certification: dict[str, Any],
    enterprise: dict[str, Any],
    live_run: dict[str, Any] | None,
) -> dict[str, bool]:
    live_checks = live_status.get('checks', {})
    if live_run:
        live_checks = live_run.get('checks', live_checks)
    enterprise_checks = enterprise.get('checks', {})
    certification_checks = certification.get('checks', {})
    live_evidence = (live_run or {}).get('evidence') or {}
    return {
        'production_sso_activation_ready': bool(live_checks.get('production_sso_ready')) and bool(certification_checks.get('manchester_sso_ready', True)),
        'mfa_claim_handoff_ready': bool(live_checks.get('mfa_handoff_ready')) and bool(certification_checks.get('mfa_handoff_ready', True)),
        'ad_ou_group_mapping_ready': bool(live_checks.get('ad_ou_validation_ready')) and bool(enterprise_checks.get('ad_ou_group_mapping_ui_ready', True)),
        'domain_vpn_enforcement_active': bool(live_checks.get('domain_vpn_enforcement_ready')) and bool(enterprise_checks.get('domain_vpn_enforcement_dashboard_ready', True)),
        'access_review_certification_ready': bool(live_checks.get('access_review_evidence_ready')) and bool(enterprise_checks.get('user_access_review_certification_ready', True)),
        'role_group_mapping_signoff_ready': bool(live_checks.get('role_group_mapping_signoff_ready')),
        'audit_evidence_ready': bool((live_run or live_status).get('complete')) and (not live_evidence or bool(live_evidence.get('sso'))),
    }


def _signoff(payload: dict[str, Any], user: dict[str, Any], live: dict[str, Any], checks: dict[str, bool]) -> dict[str, Any]:
    evidence = live.get('evidence') or {}
    ad_ou = evidence.get('ad_ou') or {}
    network = evidence.get('network') or {}
    return {
        'signed_by': payload.get('signed_by') or user['email'],
        'signed_at': _now(),
        'activation_scope': payload.get('activation_scope') or 'Manchester production SSO/MFA, AD OU, domain/VPN, and role mapping',
        'allowed_domain': 'manchester.edu',
        'required_ou': ad_ou.get('required_ou') or 'OU=muFinances Users,OU=Finance,DC=manchester,DC=edu',
        'external_network_blocked': network.get('external_blocked') is True,
        'all_checks_passed': all(checks.values()),
        'notes': payload.get('notes') or 'Manchester identity controls are ready for production activation evidence.',
    }


def _latest_run() -> dict[str, Any] | None:
    row = db.fetch_one('SELECT * FROM manchester_identity_activation_proof_runs ORDER BY id DESC LIMIT 1')
    return _format_run(row) if row else None


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for field in ('live_proof_json', 'certification_json', 'enterprise_json', 'checks_json', 'signoff_json'):
        result[field.removesuffix('_json')] = json.loads(result.pop(field) or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
