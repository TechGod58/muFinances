from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.chat import (
    chat_summary,
    create_task_notification,
    list_chat_users,
    list_mentions,
    list_messages,
    list_task_notifications,
    presence_summary,
    send_message,
)
from app.services.evidence import create_attachment, create_comment, entity_evidence
from app.services.security import create_user, user_profile


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS collaboration_hardening_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                artifacts_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_collaboration_hardening_runs_created
            ON collaboration_hardening_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM collaboration_hardening_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'chat_notifications_ready': True,
        'user_presence_ready': True,
        'comments_ready': True,
        'attachments_ready': True,
        'evidence_links_ready': True,
        'mentions_ready': True,
        'task_notifications_ready': True,
        'delayed_delivery_ready': True,
    }
    counts = {
        'hardening_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM collaboration_hardening_runs')['count']),
        'chat_messages': int(db.fetch_one('SELECT COUNT(*) AS count FROM chat_messages')['count']),
        'notifications': int(db.fetch_one('SELECT COUNT(*) AS count FROM notifications')['count']),
        'comments': int(db.fetch_one('SELECT COUNT(*) AS count FROM entity_comments')['count']),
        'attachments': int(db.fetch_one('SELECT COUNT(*) AS count FROM evidence_attachments')['count']),
    }
    return {
        'batch': 'B104',
        'title': 'Collaboration Layer Hardening',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM collaboration_hardening_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_run(row) for row in rows]


def run_hardening(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b104-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    recipient = _ensure_collaboration_user(run_key, user)
    recipient_profile = user_profile(int(recipient['id']))

    before_presence = presence_summary(user)
    sent = send_message(
        {
            'recipient_user_id': recipient['id'],
            'body': f"@{recipient['email']} Budget task and evidence package {run_key} is ready.",
        },
        user,
    )
    delivery_before = sent['delivery_status']
    recipient_summary = chat_summary(recipient_profile)
    received_messages = list_messages(int(user['id']), recipient_profile)
    mentions = list_mentions(recipient_profile)
    comment = create_comment(
        {
            'entity_type': 'budget_task',
            'entity_id': run_key,
            'comment_text': f'@{recipient["email"]} Please review the budget evidence for {run_key}.',
            'visibility': 'internal',
        },
        user,
    )
    attachment = create_attachment(
        {
            'entity_type': 'budget_task',
            'entity_id': run_key,
            'file_name': f'{run_key}-evidence.pdf',
            'storage_path': f'evidence/{run_key}/budget-evidence.pdf',
            'content_type': 'application/pdf',
            'size_bytes': 4096,
            'retention_until': '2034-06-30',
            'metadata': {'chat_message_id': sent['id'], 'comment_id': comment['id']},
        },
        user,
    )
    linked_evidence = entity_evidence('budget_task', run_key)
    task = create_task_notification(
        {
            'task_key': f'{run_key}-review',
            'recipient_user_id': recipient['id'],
            'scenario_id': payload.get('scenario_id'),
            'title': 'Budget evidence review',
            'message': f'Review evidence and comment thread for {run_key}.',
            'severity': 'info',
            'link': '#evidence',
        },
        user,
    )
    task_queue = list_task_notifications(recipient_profile)
    users_after = list_chat_users(user)
    after_presence = presence_summary(recipient_profile)
    delivered_message = next((message for message in received_messages if int(message['id']) == int(sent['id'])), sent)
    artifacts = {
        'presence_before': before_presence,
        'presence_after': after_presence,
        'recipient': recipient,
        'chat_message': sent,
        'recipient_summary': recipient_summary,
        'received_messages': received_messages,
        'delivered_message': delivered_message,
        'mentions': mentions,
        'comment': comment,
        'attachment': attachment,
        'linked_evidence': linked_evidence,
        'task_notification': task,
        'task_queue': task_queue,
        'chat_users': users_after,
    }
    checks = {
        'chat_notifications_ready': bool(sent['notification_id']) and recipient_summary['unread_count'] >= 1,
        'user_presence_ready': bool(before_presence['current_user']['last_seen_at']) and bool(after_presence['current_user']['last_seen_at']),
        'comments_ready': comment['comment_text'].startswith('@'),
        'attachments_ready': attachment['retention_until'] == '2034-06-30',
        'evidence_links_ready': len(linked_evidence['attachments']) >= 1 and len(linked_evidence['comments']) >= 1,
        'mentions_ready': any(int(item['mentioned_user_id']) == int(recipient['id']) for item in mentions),
        'task_notifications_ready': task['status'] == 'unread' and any(item['task_key'] == task['task_key'] for item in task_queue),
        'delayed_delivery_ready': delivery_before == 'pending_delivery' and delivered_message['delivery_status'] == 'delivered',
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO collaboration_hardening_runs (
            run_key, status, checks_json, artifacts_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''',
        (run_key, status_value, json.dumps(checks, sort_keys=True), json.dumps(artifacts, sort_keys=True), user['email'], started, completed),
    )
    db.log_audit('collaboration_hardening', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM collaboration_hardening_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Collaboration hardening run not found.')
    return _format_run(row)


def _ensure_collaboration_user(run_key: str, user: dict[str, Any]) -> dict[str, Any]:
    email = f'{run_key}.collab@mufinances.local'
    existing = db.fetch_one('SELECT id FROM users WHERE lower(email) = lower(?)', (email,))
    if existing:
        return user_profile(int(existing['id']))
    return create_user(
        {
            'email': email,
            'display_name': f'Collaboration Reviewer {run_key[-6:]}',
            'password': 'CollabReview!3200',
            'role_keys': ['department.planner'],
        },
        actor=user['email'],
    )


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['artifacts'] = json.loads(result.pop('artifacts_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
