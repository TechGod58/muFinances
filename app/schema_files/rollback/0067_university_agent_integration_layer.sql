DROP TABLE IF EXISTS university_agent_audit_logs;
DROP TABLE IF EXISTS university_agent_callbacks;
DROP TABLE IF EXISTS university_agent_requests;
DROP TABLE IF EXISTS university_agent_policies;
DROP TABLE IF EXISTS university_agent_tools;
DROP TABLE IF EXISTS university_agent_clients;
DELETE FROM schema_migrations WHERE migration_key = '0067_university_agent_integration_layer';
