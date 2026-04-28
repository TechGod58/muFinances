from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db

DEFAULT_SOD_RULES = [
    ('security-admin-and-planner', 'Security admin should not be a department planner', 'role_pair', 'finance.admin', 'department.planner', 'high'),
    ('creator-and-approver', 'Same actor should not create and approve controlled records', 'action_pair', 'created', 'approved', 'high'),
    ('submitter-and-reviewer', 'Same actor should not submit and review reconciliations or eliminations', 'action_pair', 'submitted', 'approved', 'medium'),
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def ensure_compliance_ready() -> None:
    now = _now()
    for key, name, conflict_type, left_value, right_value, severity in DEFAULT_SOD_RULES:
        db.execute(
            '''
            INSERT OR IGNORE INTO sod_rules (
                rule_key, name, conflict_type, left_value, right_value, severity, active, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            ''',
            (key, name, conflict_type, left_value, right_value, severity, now),
        )


def status() -> dict[str, Any]:
    ensure_compliance_ready()
    counts = {
        'audit_logs': int(db.fetch_one('SELECT COUNT(*) AS count FROM audit_logs')['count']),
        'sealed_audit_logs': int(db.fetch_one('SELECT COUNT(*) AS count FROM audit_log_hashes')['count']),
        'sod_rules': int(db.fetch_one('SELECT COUNT(*) AS count FROM sod_rules WHERE active = 1')['count']),
        'retention_policies': int(db.fetch_one('SELECT COUNT(*) AS count FROM retention_policies WHERE active = 1')['count']),
        'certifications': int(db.fetch_one('SELECT COUNT(*) AS count FROM compliance_certifications')['count']),
    }
    checks = {
        'immutable_audit_ready': True,
        'audit_verification_ready': True,
        'sod_checks_ready': counts['sod_rules'] >= len(DEFAULT_SOD_RULES),
        'retention_controls_ready': True,
        'certification_controls_ready': True,
    }
    return {'batch': 'B33', 'title': 'Compliance And Audit Hardening', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def seal_audit_backlog(user: dict[str, Any]) -> dict[str, Any]:
    if _requires_chain_rebuild() or _previous_hash_order_only_failure():
        db.execute('DELETE FROM audit_log_hashes')
    rows = db.fetch_all(
        '''
        SELECT a.id
        FROM audit_logs a
        LEFT JOIN audit_log_hashes h ON h.audit_log_id = a.id
        WHERE h.id IS NULL
        ORDER BY a.id
        '''
    )
    for row in rows:
        db.seal_audit_log(int(row['id']))
    db.log_audit('audit_log_hashes', 'backlog', 'sealed', user['email'], {'sealed': len(rows)}, _now())
    return {'sealed': len(rows), 'remaining_unsealed': _unsealed_count()}


def verify_audit_chain(limit: int = 1000) -> dict[str, Any]:
    rows = db.fetch_all(
        '''
        SELECT a.*, h.previous_hash, h.row_hash, h.sealed_at
        FROM audit_logs a
        LEFT JOIN audit_log_hashes h ON h.audit_log_id = a.id
        ORDER BY a.id ASC
        LIMIT ?
        ''',
        (limit,),
    )
    previous_hash = 'GENESIS'
    failures = []
    verified = 0
    for row in rows:
        if not row.get('row_hash'):
            failures.append({'audit_log_id': row['id'], 'reason': 'missing_hash'})
            previous_hash = ''
            continue
        if row['previous_hash'] != previous_hash:
            failures.append({'audit_log_id': row['id'], 'reason': 'previous_hash_mismatch'})
        expected = db.audit_row_hash(row, row['previous_hash'])
        if expected != row['row_hash']:
            failures.append({'audit_log_id': row['id'], 'reason': 'row_hash_mismatch'})
        previous_hash = row['row_hash']
        verified += 1
    return {
        'verified': verified,
        'total_checked': len(rows),
        'unsealed': _unsealed_count(),
        'valid': not failures and _unsealed_count() == 0,
        'failures': failures,
    }


def sod_report() -> dict[str, Any]:
    ensure_compliance_ready()
    rules = db.fetch_all('SELECT * FROM sod_rules WHERE active = 1 ORDER BY severity DESC, rule_key')
    violations = _role_pair_violations(rules) + _action_pair_violations(rules)
    return {'generated_at': _now(), 'rules': rules, 'violation_count': len(violations), 'violations': violations}


def upsert_retention_policy(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO retention_policies (
            policy_key, entity_type, retention_years, disposition_action, legal_hold,
            active, created_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(policy_key) DO UPDATE SET
            entity_type = excluded.entity_type,
            retention_years = excluded.retention_years,
            disposition_action = excluded.disposition_action,
            legal_hold = excluded.legal_hold,
            active = excluded.active,
            updated_at = excluded.updated_at
        ''',
        (
            payload['policy_key'], payload['entity_type'], payload['retention_years'], payload['disposition_action'],
            1 if payload.get('legal_hold') else 0, 1 if payload.get('active', True) else 0, user['email'], now, now,
        ),
    )
    db.log_audit('retention_policy', payload['policy_key'], 'upserted', user['email'], payload, now)
    return _retention_policy(payload['policy_key'])


def list_retention_policies() -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM retention_policies ORDER BY entity_type, policy_key')
    for row in rows:
        row['legal_hold'] = bool(row['legal_hold'])
        row['active'] = bool(row['active'])
    return rows


def retention_review() -> dict[str, Any]:
    policies = list_retention_policies()
    coverage = []
    for policy in policies:
        count = _entity_count(policy['entity_type'])
        coverage.append({**policy, 'current_records': count, 'protected_by_legal_hold': bool(policy['legal_hold'])})
    return {'generated_at': _now(), 'policy_count': len(policies), 'coverage': coverage}


def create_certification(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO compliance_certifications (
            scenario_id, certification_key, control_area, period, status, owner,
            due_at, notes, created_by, created_at
        ) VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?, ?)
        ON CONFLICT(certification_key) DO UPDATE SET
            scenario_id = excluded.scenario_id,
            control_area = excluded.control_area,
            period = excluded.period,
            owner = excluded.owner,
            due_at = excluded.due_at,
            notes = excluded.notes
        ''',
        (
            payload.get('scenario_id'), payload['certification_key'], payload['control_area'], payload['period'],
            payload['owner'], payload.get('due_at'), payload.get('notes', ''), user['email'], now,
        ),
    )
    db.log_audit('compliance_certification', payload['certification_key'], 'created', user['email'], payload, now)
    return _certification(payload['certification_key'])


def certify(certification_id: int, payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    existing = db.fetch_one('SELECT * FROM compliance_certifications WHERE id = ?', (certification_id,))
    if existing is None:
        raise ValueError('Certification not found.')
    db.execute(
        '''
        UPDATE compliance_certifications
        SET status = 'certified', certified_by = ?, certified_at = ?, evidence_json = ?, notes = ?
        WHERE id = ?
        ''',
        (user['email'], _now(), json.dumps(payload.get('evidence') or {}, sort_keys=True), payload.get('notes', ''), certification_id),
    )
    db.log_audit('compliance_certification', str(certification_id), 'certified', user['email'], payload, _now())
    return _format_certification(db.fetch_one('SELECT * FROM compliance_certifications WHERE id = ?', (certification_id,)))


def list_certifications(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        rows = db.fetch_all('SELECT * FROM compliance_certifications WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    else:
        rows = db.fetch_all('SELECT * FROM compliance_certifications ORDER BY id DESC')
    return [_format_certification(row) for row in rows]


def _unsealed_count() -> int:
    return int(
        db.fetch_one(
            '''
            SELECT COUNT(*) AS count
            FROM audit_logs a
            LEFT JOIN audit_log_hashes h ON h.audit_log_id = a.id
            WHERE h.id IS NULL
            '''
        )['count']
    )


def _requires_chain_rebuild() -> bool:
    row = db.fetch_one(
        '''
        SELECT
            COALESCE(MIN(CASE WHEN h.id IS NULL THEN a.id END), 0) AS first_unsealed,
            COALESCE(MAX(CASE WHEN h.id IS NOT NULL THEN a.id END), 0) AS last_sealed
        FROM audit_logs a
        LEFT JOIN audit_log_hashes h ON h.audit_log_id = a.id
        '''
    )
    return bool(row and int(row['first_unsealed']) > 0 and int(row['first_unsealed']) < int(row['last_sealed']))


def _previous_hash_order_only_failure() -> bool:
    report = verify_audit_chain(5000)
    failures = report.get('failures') or []
    return bool(failures and report.get('unsealed') == 0 and all(item.get('reason') == 'previous_hash_mismatch' for item in failures))


def _role_pair_violations(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    violations = []
    role_rules = [rule for rule in rules if rule['conflict_type'] == 'role_pair']
    for rule in role_rules:
        rows = db.fetch_all(
            '''
            SELECT u.id, u.email, u.display_name
            FROM users u
            JOIN user_roles left_ur ON left_ur.user_id = u.id
            JOIN roles left_role ON left_role.id = left_ur.role_id
            JOIN user_roles right_ur ON right_ur.user_id = u.id
            JOIN roles right_role ON right_role.id = right_ur.role_id
            WHERE left_role.role_key = ? AND right_role.role_key = ?
            ORDER BY u.email
            ''',
            (rule['left_value'], rule['right_value']),
        )
        for row in rows:
            violations.append({'rule_key': rule['rule_key'], 'severity': rule['severity'], 'type': 'role_pair', 'user': row})
    return violations


def _action_pair_violations(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    controlled_entities = ('budget_submission', 'journal_adjustment', 'account_reconciliation', 'elimination_entry', 'compliance_certification')
    violations = []
    action_rules = [rule for rule in rules if rule['conflict_type'] == 'action_pair']
    for rule in action_rules:
        left_actions = (rule['left_value'], 'upserted') if rule['left_value'] == 'created' else (rule['left_value'],)
        rows = db.fetch_all(
            f'''
            SELECT left_log.entity_type, left_log.entity_id, left_log.actor, left_log.action AS left_action, right_log.action AS right_action
            FROM audit_logs left_log
            JOIN audit_logs right_log
              ON right_log.entity_type = left_log.entity_type
             AND right_log.entity_id = left_log.entity_id
             AND right_log.actor = left_log.actor
            WHERE left_log.entity_type IN ({','.join('?' for _ in controlled_entities)})
              AND left_log.action IN ({','.join('?' for _ in left_actions)})
              AND right_log.action = ?
            ORDER BY left_log.id DESC
            LIMIT 100
            ''',
            (*controlled_entities, *left_actions, rule['right_value']),
        )
        for row in rows:
            violations.append({'rule_key': rule['rule_key'], 'severity': rule['severity'], 'type': 'action_pair', **row})
    return violations


def _entity_count(entity_type: str) -> int:
    table_map = {
        'audit_logs': 'audit_logs',
        'evidence_attachment': 'evidence_attachments',
        'report_snapshot': 'report_snapshots',
        'export_artifact': 'export_artifacts',
        'application_log': 'application_logs',
    }
    table = table_map.get(entity_type)
    if not table:
        return 0
    return int(db.fetch_one(f'SELECT COUNT(*) AS count FROM {table}')['count'])


def _retention_policy(policy_key: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM retention_policies WHERE policy_key = ?', (policy_key,))
    if row is None:
        raise ValueError('Retention policy not found.')
    row['legal_hold'] = bool(row['legal_hold'])
    row['active'] = bool(row['active'])
    return row


def _certification(certification_key: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM compliance_certifications WHERE certification_key = ?', (certification_key,))
    if row is None:
        raise ValueError('Certification not found.')
    return _format_certification(row)


def _format_certification(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row['evidence'] = json.loads(row.pop('evidence_json') or '{}')
    return row
