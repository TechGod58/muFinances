-- Rollback removes B63 tax compliance records and migration marker only.
DROP TABLE IF EXISTS form990_support_fields;
DROP TABLE IF EXISTS tax_reviews;
DROP TABLE IF EXISTS tax_change_alerts;
DROP TABLE IF EXISTS tax_update_checks;
DROP TABLE IF EXISTS tax_rule_sources;
DROP TABLE IF EXISTS tax_activity_classifications;
DELETE FROM schema_migrations WHERE migration_key = '0063_tax_classification_compliance_watch';
