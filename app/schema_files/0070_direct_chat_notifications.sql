CREATE TABLE IF NOT EXISTS chat_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_user_id INTEGER NOT NULL,
    recipient_user_id INTEGER NOT NULL,
    body TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    read_at TEXT DEFAULT NULL,
    notification_id INTEGER DEFAULT NULL,
    FOREIGN KEY (sender_user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (recipient_user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (notification_id) REFERENCES notifications(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_chat_messages_recipient_unread
ON chat_messages (recipient_user_id, read_at, id);

CREATE INDEX IF NOT EXISTS idx_chat_messages_thread
ON chat_messages (sender_user_id, recipient_user_id, id);

INSERT OR IGNORE INTO schema_migrations (migration_key, description, applied_at)
VALUES (
    '0070_direct_chat_notifications',
    'Create direct chat messages with unread notification handoff for signed-in and next-login recipients.',
    CURRENT_TIMESTAMP
);
