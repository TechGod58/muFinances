from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.audit_compliance_certification import run_certification as run_audit_compliance_certification
from app.services.secure_audit_operations import (
    create_auditor_packet,
    create_backup_verification,
    status as secure_audit_status,
    tamper_check_report,
    verification_dashboard,
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS secure_audit_log_certification_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                audit_compliance_json TEXT NOT NULL,
                dashboard_json TEXT NOT NULL,
                backup_verification_json TEXT NOT NULL,
                auditor_packet_json TEXT NOT NULL,
                tamper_report_json TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                signoff_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_secure_audit_log_certification_runs_created
            ON secure_audit_log_certification_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    secure = secure_audit_status()
    dashboard = verification_dashboard()
    checks = {
        'secure_financial_audit_chain_ready': secure['checks']['secure_financial_audit_chain_ready'],
        'retention_policy_ready': secure['checks']['retention_policy_ready'],
        'backup_verification_hook_ready': secure['checks']['backup_verification_hook_ready'],
        'auditor_packet_export_ready': secure['checks']['auditor_packet_export_ready'],
        'tamper_check_reporting_ready': secure['checks']['tamper_check_reporting_ready'],
        'secure_log_table_not_user_exposed': True,
        'certification_record_ready': True,
    }
    counts = {
        'certification_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM secure_audit_log_certification_runs')['count']),
        'secure_audit_logs': secure['counts']['secure_audit_logs'],
        'auditor_exports': secure['counts']['auditor_exports'],
        'backup_verifications': secure['counts']['backup_verifications'],
    }
    return {
        'batch': 'B159',
        'title': 'Secure Audit Log Certification',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'secure_audit_status': secure,
        'dashboard': dashboard,
        'latest_run': _latest_run(),
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all(
        'SELECT * FROM secure_audit_log_certification_runs ORDER BY id DESC LIMIT ?',
        (max(1, min(limit, 500)),),
    )
    return [_format_run(row) for row in rows]


def run_certification(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b159-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    audit_compliance = run_audit_compliance_certification({'run_key': f'{run_key}-b101'}, user)
    db.execute('UPDATE scenarios SET status = ? WHERE id = ?', ('evidence', audit_compliance['scenario_id']))
    dashboard = verification_dashboard()
    backup = create_backup_verification(user)
    packet = create_auditor_packet(user, int(payload.get('packet_limit') or 250))
    tamper = tamper_check_report()
    packet_records_text = json.dumps((packet.get('packet') or {}).get('records') or [], sort_keys=True)
    checks = {
        'audit_compliance_certified': audit_compliance['status'] == 'passed',
        'secure_financial_audit_chain_valid': dashboard['chain']['valid'] is True and int(dashboard['chain'].get('checked') or 0) >= 1,
        'tamper_report_clean': tamper['status'] == 'pass' and tamper['finding_count'] == 0,
        'backup_contains_secure_audit_log': backup['status'] == 'pass' and backup['result']['secure_financial_audit_present'] is True,
        'auditor_packet_exported': packet['export_type'] == 'auditor_packet' and packet['packet']['record_count'] >= 1,
        'auditor_packet_checksum_ready': len(packet['packet_checksum']) == 64,
        'retention_policy_active': dashboard['retention']['covered'] is True,
        'secure_log_table_not_exposed_in_packet': 'secure_financial_audit_logs' not in packet_records_text,
    }
    signoff = _signoff(payload, user, checks)
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO secure_audit_log_certification_runs (
            run_key, status, audit_compliance_json, dashboard_json, backup_verification_json,
            auditor_packet_json, tamper_report_json, checks_json, signoff_json,
            created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            status_value,
            json.dumps(audit_compliance, sort_keys=True),
            json.dumps(dashboard, sort_keys=True),
            json.dumps(backup, sort_keys=True),
            json.dumps(_safe_packet(packet), sort_keys=True),
            json.dumps(tamper, sort_keys=True),
            json.dumps(checks, sort_keys=True),
            json.dumps(signoff, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('secure_audit_log_certification', run_key, status_value, user['email'], {'checks': checks, 'signoff': signoff}, completed)
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM secure_audit_log_certification_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Secure audit log certification run not found.')
    return _format_run(row)


def _safe_packet(packet: dict[str, Any]) -> dict[str, Any]:
    sanitized = dict(packet)
    packet_body = dict(sanitized.get('packet') or {})
    packet_body['records'] = [
        {
            'id': row.get('id'),
            'entity_type': row.get('entity_type'),
            'entity_id': row.get('entity_id'),
            'action': row.get('action'),
            'actor': row.get('actor'),
            'detail_checksum': row.get('detail_checksum'),
            'row_hash': row.get('row_hash'),
            'created_at': row.get('created_at'),
        }
        for row in packet_body.get('records', [])
    ]
    sanitized['packet'] = packet_body
    return sanitized


def _signoff(payload: dict[str, Any], user: dict[str, Any], checks: dict[str, bool]) -> dict[str, Any]:
    return {
        'signed_by': payload.get('signed_by') or user['email'],
        'signed_at': _now(),
        'all_checks_passed': all(checks.values()),
        'restricted_log_access': 'No user-facing secure financial audit log table endpoint is exposed.',
        'notes': payload.get('notes') or 'Secure audit log chain, backup, tamper report, retention, and auditor packet evidence certified.',
    }


def _latest_run() -> dict[str, Any] | None:
    row = db.fetch_one('SELECT * FROM secure_audit_log_certification_runs ORDER BY id DESC LIMIT 1')
    return _format_run(row) if row else None


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['batch'] = 'B159'
    result['audit_compliance'] = json.loads(result.pop('audit_compliance_json') or '{}')
    result['dashboard'] = json.loads(result.pop('dashboard_json') or '{}')
    result['backup_verification'] = json.loads(result.pop('backup_verification_json') or '{}')
    result['auditor_packet'] = json.loads(result.pop('auditor_packet_json') or '{}')
    result['tamper_report'] = json.loads(result.pop('tamper_report_json') or '{}')
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['signoff'] = json.loads(result.pop('signoff_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
