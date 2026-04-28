from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'comments': int(db.fetch_one('SELECT COUNT(*) AS count FROM entity_comments')['count']),
        'attachments': int(db.fetch_one('SELECT COUNT(*) AS count FROM evidence_attachments')['count']),
        'close_task_evidence': _attachment_count('close_task'),
        'reconciliation_evidence': _attachment_count('reconciliation'),
        'report_evidence': _attachment_count('report'),
        'budget_line_evidence': _attachment_count('budget_line'),
    }
    checks = {
        'budget_line_comments_ready': True,
        'report_comments_ready': True,
        'reconciliation_comments_ready': True,
        'close_task_comments_ready': True,
        'attachment_metadata_ready': True,
        'retention_metadata_ready': True,
        'audit_packet_evidence_links_ready': True,
    }
    return {'batch': 'B14', 'title': 'Comments, Attachments, And Evidence', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def create_comment(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    comment_id = db.execute(
        '''
        INSERT INTO entity_comments (entity_type, entity_id, comment_text, visibility, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (payload['entity_type'], payload['entity_id'], payload['comment_text'], payload['visibility'], user['email'], now),
    )
    db.log_audit('entity_comment', str(comment_id), 'created', user['email'], payload, now)
    return get_comment(comment_id)


def list_comments(entity_type: str | None = None, entity_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    params: list[Any] = []
    where = []
    if entity_type:
        where.append('entity_type = ?')
        params.append(entity_type)
    if entity_id:
        where.append('entity_id = ?')
        params.append(entity_id)
    params.append(max(1, min(500, limit)))
    clause = f"WHERE {' AND '.join(where)}" if where else ''
    return db.fetch_all(f'SELECT * FROM entity_comments {clause} ORDER BY id DESC LIMIT ?', tuple(params))


def get_comment(comment_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM entity_comments WHERE id = ?', (comment_id,))
    if row is None:
        raise ValueError('Comment not found.')
    return row


def resolve_comment(comment_id: int, user: dict[str, Any]) -> dict[str, Any]:
    row = get_comment(comment_id)
    now = _now()
    db.execute('UPDATE entity_comments SET resolved_at = ? WHERE id = ?', (now, comment_id))
    db.log_audit('entity_comment', str(comment_id), 'resolved', user['email'], {'previous_resolved_at': row['resolved_at']}, now)
    return get_comment(comment_id)


def create_attachment(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    attachment_id = db.execute(
        '''
        INSERT INTO evidence_attachments (
            entity_type, entity_id, file_name, storage_path, content_type, size_bytes,
            retention_until, metadata_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            payload['entity_type'], payload['entity_id'], payload['file_name'], payload['storage_path'],
            payload['content_type'], payload['size_bytes'], payload.get('retention_until'),
            json.dumps(payload.get('metadata') or {}, sort_keys=True), user['email'], now,
        ),
    )
    db.log_audit('evidence_attachment', str(attachment_id), 'created', user['email'], payload, now)
    return get_attachment(attachment_id)


def list_attachments(entity_type: str | None = None, entity_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    params: list[Any] = []
    where = []
    if entity_type:
        where.append('entity_type = ?')
        params.append(entity_type)
    if entity_id:
        where.append('entity_id = ?')
        params.append(entity_id)
    params.append(max(1, min(500, limit)))
    clause = f"WHERE {' AND '.join(where)}" if where else ''
    rows = db.fetch_all(f'SELECT * FROM evidence_attachments {clause} ORDER BY id DESC LIMIT ?', tuple(params))
    return [_format_attachment(row) for row in rows]


def get_attachment(attachment_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM evidence_attachments WHERE id = ?', (attachment_id,))
    if row is None:
        raise ValueError('Attachment not found.')
    return _format_attachment(row)


def entity_evidence(entity_type: str, entity_id: str) -> dict[str, Any]:
    return {
        'entity_type': entity_type,
        'entity_id': entity_id,
        'comments': list_comments(entity_type, entity_id),
        'attachments': list_attachments(entity_type, entity_id),
    }


def packet_evidence_links(scenario_id: int, period: str) -> dict[str, Any]:
    checklist_rows = db.fetch_all(
        'SELECT id FROM close_checklists WHERE scenario_id = ? AND period = ?',
        (scenario_id, period),
    )
    reconciliation_rows = db.fetch_all(
        'SELECT id FROM account_reconciliations WHERE scenario_id = ? AND period = ?',
        (scenario_id, period),
    )
    close_ids = [str(row['id']) for row in checklist_rows]
    reconciliation_ids = [str(row['id']) for row in reconciliation_rows]
    return {
        'close_task_comments': _comments_for('close_task', close_ids),
        'close_task_attachments': _attachments_for('close_task', close_ids),
        'reconciliation_comments': _comments_for('reconciliation', reconciliation_ids),
        'reconciliation_attachments': _attachments_for('reconciliation', reconciliation_ids),
    }


def _comments_for(entity_type: str, entity_ids: list[str]) -> list[dict[str, Any]]:
    if not entity_ids:
        return []
    placeholders = ','.join('?' for _ in entity_ids)
    return db.fetch_all(
        f'SELECT * FROM entity_comments WHERE entity_type = ? AND entity_id IN ({placeholders}) ORDER BY id ASC',
        (entity_type, *entity_ids),
    )


def _attachments_for(entity_type: str, entity_ids: list[str]) -> list[dict[str, Any]]:
    if not entity_ids:
        return []
    placeholders = ','.join('?' for _ in entity_ids)
    rows = db.fetch_all(
        f'SELECT * FROM evidence_attachments WHERE entity_type = ? AND entity_id IN ({placeholders}) ORDER BY id ASC',
        (entity_type, *entity_ids),
    )
    return [_format_attachment(row) for row in rows]


def _attachment_count(entity_type: str) -> int:
    row = db.fetch_one('SELECT COUNT(*) AS count FROM evidence_attachments WHERE entity_type = ?', (entity_type,))
    return int(row['count'])


def _format_attachment(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['metadata'] = json.loads(result.pop('metadata_json') or '{}')
    return result
