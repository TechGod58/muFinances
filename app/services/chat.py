from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from app import db


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS user_presence (
                user_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'offline',
                last_seen_at TEXT DEFAULT NULL,
                last_opened_chat_at TEXT DEFAULT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS chat_delivery_receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_message_id INTEGER NOT NULL UNIQUE,
                recipient_user_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                delivered_at TEXT DEFAULT NULL,
                delivery_reason TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (chat_message_id) REFERENCES chat_messages(id) ON DELETE CASCADE,
                FOREIGN KEY (recipient_user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS collaboration_mentions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                mentioned_user_id INTEGER NOT NULL,
                notification_id INTEGER DEFAULT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (mentioned_user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (notification_id) REFERENCES notifications(id) ON DELETE SET NULL
            );
            CREATE TABLE IF NOT EXISTS collaboration_task_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_key TEXT NOT NULL,
                recipient_user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                status TEXT NOT NULL,
                notification_id INTEGER DEFAULT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                read_at TEXT DEFAULT NULL,
                FOREIGN KEY (recipient_user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (notification_id) REFERENCES notifications(id) ON DELETE SET NULL
            );
            CREATE INDEX IF NOT EXISTS idx_chat_delivery_recipient
            ON chat_delivery_receipts (recipient_user_id, status, chat_message_id);
            CREATE INDEX IF NOT EXISTS idx_collaboration_mentions_user
            ON collaboration_mentions (mentioned_user_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_collaboration_task_notifications_user
            ON collaboration_task_notifications (recipient_user_id, status, created_at);
            '''
        )


def record_presence(user: dict[str, Any], status: str = 'online', opened_chat: bool = False) -> dict[str, Any]:
    _ensure_tables()
    now = _now()
    db.execute(
        '''
        INSERT INTO user_presence (user_id, status, last_seen_at, last_opened_chat_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            status = excluded.status,
            last_seen_at = excluded.last_seen_at,
            last_opened_chat_at = COALESCE(excluded.last_opened_chat_at, user_presence.last_opened_chat_at),
            updated_at = excluded.updated_at
        ''',
        (user['id'], status, now, now if opened_chat else None, now),
    )
    return _presence_for_user(int(user['id']))


def presence_summary(user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    current = record_presence(user)
    users = list_chat_users(user)
    return {'current_user': current, 'count': len(users), 'users': users}


def _public_user(row: dict[str, Any], current_user_id: int) -> dict[str, Any]:
    _ensure_tables()
    unread = db.fetch_one(
        '''
        SELECT COUNT(*) AS count
        FROM chat_messages
        WHERE sender_user_id = ? AND recipient_user_id = ? AND read_at IS NULL
        ''',
        (row['id'], current_user_id),
    )
    presence = _presence_for_user(int(row['id']))
    return {
        'id': row['id'],
        'email': row['email'],
        'display_name': row['display_name'],
        'last_login_at': row.get('last_login_at'),
        'presence': presence,
        'unread_count': int(unread['count'] if unread else 0),
    }


def list_chat_users(user: dict[str, Any]) -> list[dict[str, Any]]:
    _ensure_tables()
    record_presence(user)
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
    _ensure_tables()
    record_presence(user)
    delivered = mark_pending_deliveries(user)
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
        'delivered_on_open': delivered,
        'presence': _presence_for_user(int(user['id'])),
    }


def list_messages(peer_user_id: int, user: dict[str, Any], limit: int = 100) -> list[dict[str, Any]]:
    _ensure_tables()
    record_presence(user, opened_chat=True)
    mark_pending_deliveries(user, peer_user_id)
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
    _ensure_tables()
    record_presence(user)
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
    recipient_online = _user_has_active_session(recipient_id)
    delivery_status = 'delivered' if recipient_online else 'pending_delivery'
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
        tx.execute(
            '''
            INSERT INTO chat_delivery_receipts (
                chat_message_id, recipient_user_id, status, delivered_at, delivery_reason, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                message_id,
                recipient_id,
                delivery_status,
                now if recipient_online else None,
                'active_session' if recipient_online else 'deliver_when_recipient_signs_in',
                now,
                now,
            ),
        )
        mention_notification_ids = _create_mentions(tx, body, message_id, notification_id, user, recipient_id, now)
        db.log_audit(
            'chat_message',
            str(message_id),
            'sent',
            user['email'],
            {'recipient_user_id': recipient_id, 'notification_id': notification_id, 'mentions': mention_notification_ids},
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


def create_task_notification(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    recipient_id = int(payload['recipient_user_id'])
    _get_active_user(recipient_id)
    now = _now()
    with db.transaction(immediate=True) as tx:
        notification_id = tx.execute(
            '''
            INSERT INTO notifications (
                user_id, scenario_id, notification_type, title, message, severity, status, link, created_at
            ) VALUES (?, ?, 'workflow', ?, ?, ?, 'unread', ?, ?)
            ''',
            (
                recipient_id,
                payload.get('scenario_id'),
                payload['title'],
                payload['message'],
                payload.get('severity') or 'info',
                payload.get('link') or '#workflow',
                now,
            ),
        ).lastrowid
        task_id = tx.execute(
            '''
            INSERT INTO collaboration_task_notifications (
                task_key, recipient_user_id, title, message, status, notification_id, created_by, created_at
            ) VALUES (?, ?, ?, ?, 'unread', ?, ?, ?)
            ''',
            (payload['task_key'], recipient_id, payload['title'], payload['message'], notification_id, user['email'], now),
        ).lastrowid
        db.log_audit(
            'collaboration_task_notification',
            str(task_id),
            'created',
            user['email'],
            {'recipient_user_id': recipient_id, 'notification_id': notification_id, 'task_key': payload['task_key']},
            now,
            conn=tx,
        )
    return get_task_notification(task_id)


def list_task_notifications(user: dict[str, Any], status_filter: str | None = None) -> list[dict[str, Any]]:
    _ensure_tables()
    params: list[Any] = [user['id']]
    where = 'recipient_user_id = ?'
    if status_filter:
        where += ' AND status = ?'
        params.append(status_filter)
    rows = db.fetch_all(
        f'SELECT * FROM collaboration_task_notifications WHERE {where} ORDER BY id DESC LIMIT 100',
        tuple(params),
    )
    return rows


def get_task_notification(task_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM collaboration_task_notifications WHERE id = ?', (task_id,))
    if row is None:
        raise ValueError('Task notification not found.')
    return row


def list_mentions(user: dict[str, Any]) -> list[dict[str, Any]]:
    _ensure_tables()
    return db.fetch_all(
        'SELECT * FROM collaboration_mentions WHERE mentioned_user_id = ? ORDER BY id DESC LIMIT 100',
        (user['id'],),
    )


def mark_pending_deliveries(user: dict[str, Any], peer_user_id: int | None = None) -> dict[str, Any]:
    _ensure_tables()
    now = _now()
    count_params: list[Any] = [user['id']]
    where = "recipient_user_id = ? AND status = 'pending_delivery'"
    if peer_user_id is not None:
        where += ' AND chat_message_id IN (SELECT id FROM chat_messages WHERE sender_user_id = ?)'
        count_params.append(int(peer_user_id))
    pending = db.fetch_one(f'SELECT COUNT(*) AS count FROM chat_delivery_receipts WHERE {where}', tuple(count_params))
    params: list[Any] = [now, now, *count_params]
    db.execute(
        f'''
        UPDATE chat_delivery_receipts
        SET status = 'delivered', delivered_at = ?, delivery_reason = 'recipient_seen_after_sign_in', updated_at = ?
        WHERE {where}
        ''',
        tuple(params),
    )
    return {'delivered_count': int(pending['count'] if pending else 0), 'delivered_at': now}


def get_message(message_id: int, user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
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
    _ensure_tables()
    record_presence(user, opened_chat=True)
    mark_pending_deliveries(user, peer_user_id)
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


def _presence_for_user(user_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM user_presence WHERE user_id = ?', (user_id,))
    if row is None:
        user = db.fetch_one('SELECT id, last_login_at FROM users WHERE id = ?', (user_id,))
        return {
            'user_id': user_id,
            'status': 'offline',
            'last_seen_at': user.get('last_login_at') if user else None,
            'last_opened_chat_at': None,
            'updated_at': user.get('last_login_at') if user else None,
        }
    return row


def _user_has_active_session(user_id: int) -> bool:
    row = db.fetch_one(
        '''
        SELECT COUNT(*) AS count
        FROM auth_sessions
        WHERE user_id = ? AND revoked_at IS NULL AND expires_at > ?
        ''',
        (user_id, _now()),
    )
    return int(row['count'] if row else 0) > 0


def _create_mentions(tx: Any, body: str, message_id: int, chat_notification_id: int, user: dict[str, Any], recipient_id: int, now: str) -> list[int]:
    mentioned_users = _mentioned_users(body, recipient_id)
    notification_ids = []
    for mentioned in mentioned_users:
        mention_notification_id = chat_notification_id if int(mentioned['id']) == recipient_id else tx.execute(
            '''
            INSERT INTO notifications (
                user_id, scenario_id, notification_type, title, message, severity, status, link, created_at
            ) VALUES (?, NULL, 'chat', ?, ?, 'info', 'unread', '#chat', ?)
            ''',
            (mentioned['id'], f"Mention from {user.get('display_name') or user['email']}", body[:180], now),
        ).lastrowid
        tx.execute(
            '''
            INSERT INTO collaboration_mentions (
                source_type, source_id, mentioned_user_id, notification_id, created_by, created_at
            ) VALUES ('chat_message', ?, ?, ?, ?, ?)
            ''',
            (str(message_id), mentioned['id'], mention_notification_id, user['email'], now),
        )
        notification_ids.append(int(mention_notification_id))
    return notification_ids


def _mentioned_users(body: str, recipient_id: int) -> list[dict[str, Any]]:
    tokens = {match.group(1).strip('.,;:!?()[]{}') for match in re.finditer(r'@([A-Za-z0-9._%+\-]+(?:@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})?)', body)}
    if not tokens:
        return [_get_active_user(recipient_id)]
    rows = []
    for token in tokens:
        lookup = token.lower()
        row = db.fetch_one(
            '''
            SELECT id, email, display_name, last_login_at
            FROM users
            WHERE is_active = 1
              AND (
                lower(email) = lower(?)
                OR lower(display_name) = lower(?)
                OR lower(replace(display_name, ' ', '.')) = lower(?)
              )
            ''',
            (lookup, lookup, lookup),
        )
        if row:
            rows.append(row)
    if not any(int(row['id']) == recipient_id for row in rows):
        rows.append(_get_active_user(recipient_id))
    unique: dict[int, dict[str, Any]] = {}
    for row in rows:
        unique[int(row['id'])] = row
    return list(unique.values())


def _format_message(row: dict[str, Any], current_user_id: int) -> dict[str, Any]:
    receipt = db.fetch_one('SELECT status, delivered_at, delivery_reason FROM chat_delivery_receipts WHERE chat_message_id = ?', (row['id'],))
    mentions = db.fetch_all('SELECT mentioned_user_id, notification_id FROM collaboration_mentions WHERE source_type = ? AND source_id = ?', ('chat_message', str(row['id'])))
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
        'delivery_status': receipt['status'] if receipt else 'delivered',
        'delivered_at': receipt.get('delivered_at') if receipt else row.get('sent_at'),
        'delivery_reason': receipt.get('delivery_reason') if receipt else 'legacy_message',
        'mentions': mentions,
        'direction': 'sent' if int(row['sender_user_id']) == current_user_id else 'received',
    }
