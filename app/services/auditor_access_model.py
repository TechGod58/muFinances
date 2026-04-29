from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.reporting import list_board_packages, list_export_artifacts
from app.services.secure_audit_operations import create_auditor_packet, status as secure_audit_status, tamper_check_report


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS auditor_access_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                access_key TEXT NOT NULL UNIQUE,
                accessor_email TEXT NOT NULL,
                access_type TEXT NOT NULL,
                target_type TEXT NOT NULL,
                target_id TEXT NOT NULL,
                status TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_auditor_access_records_created
            ON auditor_access_records (created_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    secure = secure_audit_status()
    checks = {
        'auditor_only_packet_access_ready': True,
        'close_evidence_summary_ready': True,
        'consolidation_report_summary_ready': True,
        'exportable_audit_record_ready': True,
        'secure_internal_audit_log_table_not_exposed': True,
        'auditor_access_records_ready': True,
    }
    counts = {
        'auditor_access_records': int(db.fetch_one('SELECT COUNT(*) AS count FROM auditor_access_records')['count']),
        'auditor_exports': secure['counts']['auditor_exports'],
        'secure_audit_logs': secure['counts']['secure_audit_logs'],
        'board_packages': int(db.fetch_one('SELECT COUNT(*) AS count FROM board_packages')['count']),
        'export_artifacts': int(db.fetch_one('SELECT COUNT(*) AS count FROM export_artifacts')['count']),
    }
    return {
        'batch': 'B126',
        'title': 'Controlled Auditor Access Model',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'secure_log_policy': _secure_log_policy(),
        'latest_access': _latest_access(),
    }


def auditor_workspace(user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    _require_auditor(user)
    packet = create_auditor_packet(user, limit=100)
    evidence = _auditor_evidence_summary()
    record = _record_access(
        user,
        'workspace_view',
        'auditor_workspace',
        str(packet['id']),
        {
            'packet_export_key': packet['export_key'],
            'packet_checksum': packet['packet_checksum'],
            'evidence_counts': evidence['counts'],
        },
    )
    return {
        'batch': 'B126',
        'access': record,
        'audit_packet': _safe_packet(packet),
        'close_evidence': evidence['close_evidence'],
        'consolidation_reports': evidence['consolidation_reports'],
        'exportable_audit_records': evidence['exportable_audit_records'],
        'secure_log_policy': _secure_log_policy(),
        'counts': evidence['counts'],
    }


def export_auditor_records(user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    _require_auditor(user)
    evidence = _auditor_evidence_summary()
    payload = {
        'export_type': 'auditor_safe_records',
        'generated_at': _now(),
        'records': evidence,
        'policy': _secure_log_policy(),
    }
    payload_json = json.dumps(payload, sort_keys=True, default=str)
    checksum = hashlib.sha256(payload_json.encode('utf-8')).hexdigest()
    record = _record_access(user, 'export', 'auditor_safe_records', checksum, {'checksum': checksum, 'record_count': evidence['counts']['exportable_audit_records']})
    return {
        'batch': 'B126',
        'access': record,
        'checksum': checksum,
        'payload': payload,
        'secure_log_table_exposed': False,
    }


def list_access_records(limit: int = 100) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM auditor_access_records ORDER BY id DESC LIMIT ?', (max(1, min(limit, 500)),))
    return [_format_access(row) for row in rows]


def _auditor_evidence_summary() -> dict[str, Any]:
    close_rows = db.fetch_all(
        '''
        SELECT id, scenario_id, title, status, completed_by, completed_at, evidence_json
        FROM close_checklists
        WHERE status IN ('complete', 'completed', 'reviewed', 'approved')
        ORDER BY id DESC
        LIMIT 100
        '''
    )
    consolidation_rows = db.fetch_all(
        '''
        SELECT id, scenario_id, period, status, created_by, created_at
        FROM consolidation_runs
        ORDER BY id DESC
        LIMIT 100
        '''
    )
    artifacts = list_export_artifacts()
    board_packages = list_board_packages()
    audit_packets = db.fetch_all(
        '''
        SELECT id, consolidation_run_id, packet_key, status, created_by, created_at
        FROM audit_packets
        ORDER BY id DESC
        LIMIT 100
        '''
    )
    tamper = tamper_check_report()
    close_evidence = [
        {
            'id': row['id'],
            'scenario_id': row['scenario_id'],
            'title': row['title'],
            'status': row['status'],
            'completed_by': row['completed_by'],
            'completed_at': row['completed_at'],
            'evidence_checksum': hashlib.sha256(str(row.get('evidence_json') or '{}').encode('utf-8')).hexdigest(),
        }
        for row in close_rows
    ]
    consolidation_reports = [dict(row) for row in consolidation_rows]
    exportable = [
        {
            'artifact_id': item['id'],
            'scenario_id': item['scenario_id'],
            'artifact_type': item['artifact_type'],
            'file_name': item['file_name'],
            'status': item['status'],
            'package_id': item.get('package_id'),
            'checksum': item['metadata'].get('checksum') or hashlib.sha256(str(item).encode('utf-8')).hexdigest(),
        }
        for item in artifacts
    ]
    exportable.extend(
        {
            'board_package_id': item['id'],
            'scenario_id': item['scenario_id'],
            'package_name': item['package_name'],
            'status': item['status'],
            'checksum': hashlib.sha256(json.dumps(item, sort_keys=True, default=str).encode('utf-8')).hexdigest(),
        }
        for item in board_packages
    )
    exportable.extend(
        {
            'audit_packet_id': row['id'],
            'consolidation_run_id': row['consolidation_run_id'],
            'packet_key': row['packet_key'],
            'status': row['status'],
            'created_by': row['created_by'],
            'created_at': row['created_at'],
            'checksum': hashlib.sha256(json.dumps(dict(row), sort_keys=True, default=str).encode('utf-8')).hexdigest(),
        }
        for row in audit_packets
    )
    return {
        'close_evidence': close_evidence,
        'consolidation_reports': consolidation_reports,
        'exportable_audit_records': exportable,
        'tamper_check': {'status': tamper['status'], 'finding_count': tamper['finding_count']},
        'counts': {
            'close_evidence': len(close_evidence),
            'consolidation_reports': len(consolidation_reports),
            'exportable_audit_records': len(exportable),
        },
    }


def _safe_packet(packet: dict[str, Any]) -> dict[str, Any]:
    raw_packet = packet['packet']
    return {
        'id': packet['id'],
        'export_key': packet['export_key'],
        'export_type': packet['export_type'],
        'packet_checksum': packet['packet_checksum'],
        'created_at': packet['created_at'],
        'record_count': raw_packet.get('record_count', 0),
        'chain': raw_packet.get('chain', {}),
        'retention_policies': raw_packet.get('retention_policies', []),
        'policy': raw_packet.get('policy', {}),
        'records_exposed': False,
    }


def _record_access(user: dict[str, Any], access_type: str, target_type: str, target_id: str, evidence: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    access_key = f"{access_type}-{target_type}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    row_id = db.execute(
        '''
        INSERT INTO auditor_access_records (
            access_key, accessor_email, access_type, target_type, target_id,
            status, evidence_json, created_at
        ) VALUES (?, ?, ?, ?, ?, 'granted', ?, ?)
        ''',
        (access_key, user['email'], access_type, target_type, target_id, json.dumps(evidence, sort_keys=True), now),
    )
    db.log_audit('auditor_access', access_key, 'granted', user['email'], {'target_type': target_type, 'target_id': target_id}, now)
    return _format_access(db.fetch_one('SELECT * FROM auditor_access_records WHERE id = ?', (row_id,)))


def _latest_access() -> dict[str, Any] | None:
    row = db.fetch_one('SELECT * FROM auditor_access_records ORDER BY id DESC LIMIT 1')
    return _format_access(row) if row else None


def _format_access(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['evidence'] = json.loads(result.pop('evidence_json') or '{}')
    return result


def _require_auditor(user: dict[str, Any]) -> None:
    roles = set(user.get('roles') or [])
    permissions = set(user.get('permissions') or [])
    if 'auditor' not in roles and 'reports.read' not in permissions:
        raise PermissionError('Auditor access requires the auditor role or reports.read permission.')


def _secure_log_policy() -> dict[str, Any]:
    return {
        'secure_internal_audit_log_table': 'secure_financial_audit_logs',
        'direct_table_access_allowed': False,
        'auditor_surfaces': ['audit_packet_summary', 'close_evidence_summary', 'consolidation_report_summary', 'exportable_audit_records'],
        'raw_secure_log_listing_route': None,
    }
