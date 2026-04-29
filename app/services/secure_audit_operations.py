from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app import db
from app.services.compliance import list_retention_policies, retention_review, upsert_retention_policy
from app.services.foundation import create_backup, list_backups


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS secure_audit_operation_exports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                export_key TEXT NOT NULL UNIQUE,
                export_type TEXT NOT NULL,
                packet_json TEXT NOT NULL,
                packet_checksum TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_secure_audit_operation_exports_created
            ON secure_audit_operation_exports (created_at);
            CREATE TABLE IF NOT EXISTS secure_audit_backup_verifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                verification_key TEXT NOT NULL UNIQUE,
                backup_key TEXT NOT NULL,
                status TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_secure_audit_backup_verifications_created
            ON secure_audit_backup_verifications (created_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    dashboard = verification_dashboard()
    checks = {
        'secure_financial_audit_chain_ready': dashboard['chain']['valid'] is True,
        'retention_policy_ready': dashboard['retention']['covered'] is True,
        'backup_verification_hook_ready': True,
        'auditor_packet_export_ready': True,
        'tamper_check_reporting_ready': True,
        'operational_policy_ready': True,
    }
    counts = {
        'secure_audit_logs': int(db.fetch_one('SELECT COUNT(*) AS count FROM secure_financial_audit_logs')['count']),
        'auditor_exports': int(db.fetch_one('SELECT COUNT(*) AS count FROM secure_audit_operation_exports')['count']),
        'backup_verifications': int(db.fetch_one('SELECT COUNT(*) AS count FROM secure_audit_backup_verifications')['count']),
        'retention_policies': len(list_retention_policies()),
    }
    return {
        'batch': 'B116',
        'title': 'Secure Audit Operations',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_export': _latest_export(),
        'latest_backup_verification': _latest_backup_verification(),
    }


def verification_dashboard(limit: int = 5000) -> dict[str, Any]:
    _ensure_tables()
    _ensure_secure_audit_retention_policy({'email': 'system'})
    chain = db.verify_secure_financial_audit_chain(limit)
    retention = retention_review()
    tamper = tamper_check_report(limit)
    backup = _latest_backup_verification()
    latest = db.fetch_one('SELECT * FROM secure_financial_audit_logs ORDER BY id DESC LIMIT 1')
    return {
        'batch': 'B116',
        'generated_at': _now(),
        'chain': chain,
        'tamper': tamper,
        'retention': {
            'covered': any(row['entity_type'] == 'secure_financial_audit_logs' and row['active'] for row in list_retention_policies()),
            'review': retention,
        },
        'backup': {
            'latest_backup': list_backups()[0] if list_backups() else None,
            'latest_verification': backup,
        },
        'latest_secure_event': dict(latest) if latest else None,
        'policy': operational_policy(),
    }


def create_backup_verification(user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    backup = create_backup(note='B116 secure financial audit backup verification', actor=user['email'])
    result = _verify_backup_contains_secure_audit(Path(backup['path']))
    status_value = 'pass' if result['secure_financial_audit_present'] and result['integrity_check'] == 'ok' else 'fail'
    verification_key = f"secure-audit-backup-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    db.execute(
        '''
        INSERT INTO secure_audit_backup_verifications (
            verification_key, backup_key, status, result_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (verification_key, backup['backup_key'], status_value, json.dumps(result, sort_keys=True), user['email'], _now()),
    )
    db.log_audit('secure_audit_backup_verification', verification_key, status_value, user['email'], {'backup_key': backup['backup_key'], 'result': result}, _now())
    return _latest_backup_verification()


def create_auditor_packet(user: dict[str, Any], limit: int = 250) -> dict[str, Any]:
    _ensure_tables()
    rows = db.fetch_all(
        '''
        SELECT id, audit_log_id, entity_type, entity_id, action, actor,
               detail_checksum, previous_hash, row_hash, created_at, sealed_at
        FROM secure_financial_audit_logs
        ORDER BY id DESC
        LIMIT ?
        ''',
        (max(1, min(limit, 1000)),),
    )
    packet = {
        'packet_type': 'secure_financial_audit',
        'generated_at': _now(),
        'record_count': len(rows),
        'chain': db.verify_secure_financial_audit_chain(5000),
        'retention_policies': [row for row in list_retention_policies() if row['entity_type'] == 'secure_financial_audit_logs'],
        'records': rows,
        'policy': operational_policy(),
    }
    packet_json = json.dumps(packet, sort_keys=True, default=str)
    checksum = hashlib.sha256(packet_json.encode('utf-8')).hexdigest()
    export_key = f"secure-audit-packet-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    db.execute(
        '''
        INSERT INTO secure_audit_operation_exports (
            export_key, export_type, packet_json, packet_checksum, created_by, created_at
        ) VALUES (?, 'auditor_packet', ?, ?, ?, ?)
        ''',
        (export_key, packet_json, checksum, user['email'], _now()),
    )
    db.log_audit('secure_audit_operation_export', export_key, 'created', user['email'], {'record_count': len(rows), 'packet_checksum': checksum}, _now())
    return _format_export(db.fetch_one('SELECT * FROM secure_audit_operation_exports WHERE export_key = ?', (export_key,)))


def tamper_check_report(limit: int = 5000) -> dict[str, Any]:
    chain = db.verify_secure_financial_audit_chain(limit)
    orphaned = db.fetch_all(
        '''
        SELECT s.id, s.audit_log_id, s.entity_type, s.entity_id
        FROM secure_financial_audit_logs s
        LEFT JOIN audit_logs a ON a.id = s.audit_log_id
        WHERE a.id IS NULL
        ORDER BY s.id
        LIMIT 100
        '''
    )
    duplicate_audit_ids = db.fetch_all(
        '''
        SELECT audit_log_id, COUNT(*) AS count
        FROM secure_financial_audit_logs
        GROUP BY audit_log_id
        HAVING COUNT(*) > 1
        LIMIT 100
        '''
    )
    findings = list(chain.get('broken') or [])
    findings.extend({'id': row['id'], 'audit_log_id': row['audit_log_id'], 'reason': 'orphaned_audit_log'} for row in orphaned)
    findings.extend({'audit_log_id': row['audit_log_id'], 'reason': 'duplicate_secure_audit_log'} for row in duplicate_audit_ids)
    return {
        'generated_at': _now(),
        'status': 'pass' if not findings and chain['valid'] else 'fail',
        'chain': chain,
        'finding_count': len(findings),
        'findings': findings,
    }


def operational_policy() -> dict[str, Any]:
    return {
        'policy_key': 'secure-financial-audit-operations',
        'log_access': 'Only operations/security administrators can access operational audit dashboards; no user-facing secure log listing route is exposed.',
        'retention': 'Secure financial audit logs are retained for 10 years by default and placed under legal hold for financial audit review.',
        'backup': 'Every release and audit review creates a verified backup that must include secure_financial_audit_logs.',
        'tamper_response': 'Any hash-chain mismatch blocks release readiness until the packet is preserved and IT/security review signs off.',
        'auditor_export': 'Auditor packets include hashes, checksums, retention evidence, and policy text without exposing unrestricted application logs.',
    }


def _ensure_secure_audit_retention_policy(user: dict[str, Any]) -> None:
    existing = db.fetch_one(
        "SELECT id FROM retention_policies WHERE policy_key = 'secure-financial-audit-10y'"
    )
    if existing is None:
        upsert_retention_policy(
            {
                'policy_key': 'secure-financial-audit-10y',
                'entity_type': 'secure_financial_audit_logs',
                'retention_years': 10,
                'disposition_action': 'archive',
                'legal_hold': True,
                'active': True,
            },
            user,
        )


def _verify_backup_contains_secure_audit(path: Path) -> dict[str, Any]:
    result = {
        'path': str(path),
        'integrity_check': 'missing',
        'secure_financial_audit_present': False,
        'secure_financial_audit_count': 0,
        'last_hash': None,
    }
    if not path.exists():
        return result
    with sqlite3.connect(path) as conn:
        integrity = conn.execute('PRAGMA integrity_check').fetchone()
        result['integrity_check'] = integrity[0] if integrity else 'missing'
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()}
        result['secure_financial_audit_present'] = 'secure_financial_audit_logs' in tables
        if result['secure_financial_audit_present']:
            row = conn.execute('SELECT COUNT(*) AS count, MAX(row_hash) AS last_hash FROM secure_financial_audit_logs').fetchone()
            result['secure_financial_audit_count'] = int(row[0] or 0)
            result['last_hash'] = row[1]
    return result


def _latest_export() -> dict[str, Any] | None:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM secure_audit_operation_exports ORDER BY id DESC LIMIT 1')
    return _format_export(row) if row else None


def _latest_backup_verification() -> dict[str, Any] | None:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM secure_audit_backup_verifications ORDER BY id DESC LIMIT 1')
    if not row:
        return None
    result = dict(row)
    result['result'] = json.loads(result.pop('result_json') or '{}')
    result['complete'] = result['status'] == 'pass'
    return result


def _format_export(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['packet'] = json.loads(result.pop('packet_json') or '{}')
    result['complete'] = result['packet']['chain']['valid'] is True
    return result
