from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app import db


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _public_user(row: dict[str, Any], current_user_id: int) -> dict[str, Any]:
    unread = db.fetch_one(
        '''
        SELECT COUNT(*) AS count
        FROM chat_messages
        WHERE sender_user_id = ? AND recipient_user_id = ? AND read_at IS NULL
        ''',
        (row['id'], current_user_id),
    )
    return {
        'id': row['id'],
        'email': row['email'],
        'display_name': row['display_name'],
        'last_login_at': row.get('last_login_at'),
        'unread_count': int(unread['count'] if unread else 0),
    }


def list_chat_users(user: dict[str, Any]) -> list[dict[str, Any]]:
    rows = db.fetch_all(
        '''
        SELECT id, email, display_name, last_login_at
        FROM users
        WHERE is_active = 1 AND id <> ?
        ORDER BY lower(display_name), lower(email)
        ''',
        (user['id'],),
    )
    return [_public_user(row, int(user['id'])) for row in rows]


def chat_summary(user: dict[str, Any]) -> dict[str, Any]:
    unread = db.fetch_one(
        '''
        SELECT COUNT(*) AS count
        FROM chat_messages
        WHERE recipient_user_id = ? AND read_at IS NULL
        ''',
        (user['id'],),
    )
    latest = db.fetch_one(
        '''
        SELECT cm.*, su.display_name AS sender_name, su.email AS sender_email
        FROM chat_messages cm
        JOIN users su ON su.id = cm.sender_user_id
        WHERE cm.recipient_user_id = ? AND cm.read_at IS NULL
        ORDER BY cm.id DESC
        LIMIT 1
        ''',
        (user['id'],),
    )
    return {
        'unread_count': int(unread['count'] if unread else 0),
        'latest_unread': _format_message(latest, int(user['id'])) if latest else None,
    }


def list_messages(peer_user_id: int, user: dict[str, Any], limit: int = 100) -> list[dict[str, Any]]:
    _get_active_user(peer_user_id)
    rows = db.fetch_all(
        '''
        SELECT
            cm.*,
            su.display_name AS sender_name,
            su.email AS sender_email,
            ru.display_name AS recipient_name,
            ru.email AS recipient_email
        FROM chat_messages cm
        JOIN users su ON su.id = cm.sender_user_id
        JOIN users ru ON ru.id = cm.recipient_user_id
        WHERE
            (cm.sender_user_id = ? AND cm.recipient_user_id = ?)
            OR (cm.sender_user_id = ? AND cm.recipient_user_id = ?)
        ORDER BY cm.id DESC
        LIMIT ?
        ''',
        (user['id'], peer_user_id, peer_user_id, user['id'], limit),
    )
    return [_format_message(row, int(user['id'])) for row in reversed(rows)]


def send_message(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    recipient_id = int(payload['recipient_user_id'])
    if recipient_id == int(user['id']):
        raise ValueError('Choose a different recipient.')
    recipient = _get_active_user(recipient_id)
    body = str(payload['body']).strip()
    if not body:
        raise ValueError('Message cannot be blank.')
    if len(body) > 2000:
        raise ValueError('Message is too long.')

    now = _now()
    sender_name = user.get('display_name') or user['email']
    preview = body if len(body) <= 180 else f'{body[:177]}...'
    with db.transaction(immediate=True) as tx:
        notification_id = tx.execute(
            '''
            INSERT INTO notifications (
                user_id, scenario_id, notification_type, title, message, severity, status, link, created_at
            ) VALUES (?, NULL, 'chat', ?, ?, 'info', 'unread', '#chat', ?)
            ''',
            (recipient_id, f'Chat from {sender_name}', preview, now),
        ).lastrowid
        message_id = tx.execute(
            '''
            INSERT INTO chat_messages (
                sender_user_id, recipient_user_id, body, sent_at, notification_id
            ) VALUES (?, ?, ?, ?, ?)
            ''',
            (user['id'], recipient_id, body, now, notification_id),
        ).lastrowid
        db.log_audit(
            'chat_message',
            str(message_id),
            'sent',
            user['email'],
            {'recipient_user_id': recipient_id, 'notification_id': notification_id},
            now,
            conn=tx,
        )

    message = get_message(message_id, user)
    message['recipient'] = {
        'id': recipient['id'],
        'email': recipient['email'],
        'display_name': recipient['display_name'],
    }
    return message


def get_message(message_id: int, user: dict[str, Any]) -> dict[str, Any]:
    row = db.fetch_one(
        '''
        SELECT
            cm.*,
            su.display_name AS sender_name,
            su.email AS sender_email,
            ru.display_name AS recipient_name,
            ru.email AS recipient_email
        FROM chat_messages cm
        JOIN users su ON su.id = cm.sender_user_id
        JOIN users ru ON ru.id = cm.recipient_user_id
        WHERE cm.id = ? AND (cm.sender_user_id = ? OR cm.recipient_user_id = ?)
        ''',
        (message_id, user['id'], user['id']),
    )
    if row is None:
        raise ValueError('Message not found.')
    return _format_message(row, int(user['id']))


def mark_messages_read(user: dict[str, Any], peer_user_id: int | None = None) -> dict[str, Any]:
    now = _now()
    params: list[Any] = [now, user['id']]
    where = 'recipient_user_id = ? AND read_at IS NULL'
    if peer_user_id is not None:
        _get_active_user(int(peer_user_id))
        where += ' AND sender_user_id = ?'
        params.append(int(peer_user_id))
    db.execute(f'UPDATE chat_messages SET read_at = ? WHERE {where}', tuple(params))

    notification_params: list[Any] = [now, user['id'], user['id']]
    notification_where = 'recipient_user_id = ?'
    if peer_user_id is not None:
        notification_where += ' AND sender_user_id = ?'
        notification_params.append(int(peer_user_id))
    db.execute(
        f'''
        UPDATE notifications
        SET status = 'read', read_at = ?
        WHERE user_id = ? AND notification_type = 'chat' AND status = 'unread'
          AND id IN (
            SELECT notification_id
            FROM chat_messages
            WHERE {notification_where} AND notification_id IS NOT NULL
          )
        ''',
        tuple(notification_params),
    )
    return chat_summary(user)


def _get_active_user(user_id: int) -> dict[str, Any]:
    row = db.fetch_one(
        '''
        SELECT id, email, display_name, last_login_at
        FROM users
        WHERE id = ? AND is_active = 1
        ''',
        (user_id,),
    )
    if row is None:
        raise ValueError('Recipient not found.')
    return row


def _format_message(row: dict[str, Any], current_user_id: int) -> dict[str, Any]:
    return {
        'id': row['id'],
        'sender_user_id': row['sender_user_id'],
        'recipient_user_id': row['recipient_user_id'],
        'sender_name': row.get('sender_name') or row.get('sender_email') or '',
        'sender_email': row.get('sender_email') or '',
        'recipient_name': row.get('recipient_name') or row.get('recipient_email') or '',
        'recipient_email': row.get('recipient_email') or '',
        'body': row['body'],
        'sent_at': row['sent_at'],
        'read_at': row.get('read_at'),
        'notification_id': row.get('notification_id'),
        'direction': 'sent' if int(row['sender_user_id']) == current_user_id else 'received',
    }
