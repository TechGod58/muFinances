-- B59 productionizes connector contracts, vault metadata, streaming import markers,
-- mapping versions, and drill-back validation.

ALTER TABLE connector_adapters ADD COLUMN contract_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE connector_adapters ADD COLUMN credential_schema_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE connector_adapters ADD COLUMN max_stream_rows INTEGER NOT NULL DEFAULT 100000;
ALTER TABLE connector_auth_flows ADD COLUMN oauth_state TEXT NOT NULL DEFAULT '';
ALTER TABLE credential_vault ADD COLUMN secret_type TEXT NOT NULL DEFAULT 'api_key';
ALTER TABLE credential_vault ADD COLUMN expires_at TEXT DEFAULT NULL;
ALTER TABLE credential_vault ADD COLUMN rotated_at TEXT DEFAULT NULL;
ALTER TABLE import_mapping_templates ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE import_mapping_templates ADD COLUMN previous_template_key TEXT DEFAULT NULL;
ALTER TABLE import_batches ADD COLUMN source_name TEXT NOT NULL DEFAULT '';
ALTER TABLE import_batches ADD COLUMN stream_chunks INTEGER NOT NULL DEFAULT 1;
ALTER TABLE import_batches ADD COLUMN mapping_template_key TEXT DEFAULT NULL;
ALTER TABLE import_batches ADD COLUMN mapping_version INTEGER DEFAULT NULL;
ALTER TABLE import_batches ADD COLUMN contract_validated INTEGER NOT NULL DEFAULT 0;
ALTER TABLE connector_source_drillbacks ADD COLUMN validation_status TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE connector_source_drillbacks ADD COLUMN validation_json TEXT NOT NULL DEFAULT '{}';

INSERT INTO schema_migrations (migration_key, description, checksum, applied_at)
VALUES (
    '0059_connector_productionization',
    'Create production connector contracts, OAuth/API-key vault metadata, streaming import controls, mapping versions, and drill-back validation.',
    'managed-by-runtime',
    CURRENT_TIMESTAMP
);
