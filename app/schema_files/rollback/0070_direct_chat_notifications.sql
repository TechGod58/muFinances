DROP INDEX IF EXISTS idx_chat_messages_thread;
DROP INDEX IF EXISTS idx_chat_messages_recipient_unread;
DROP TABLE IF EXISTS chat_messages;
DELETE FROM schema_migrations WHERE migration_key = '0070_direct_chat_notifications';
