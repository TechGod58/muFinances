DROP TABLE IF EXISTS dashboard_chart_snapshots;
DROP TABLE IF EXISTS chart_render_artifacts;
DELETE FROM schema_migrations WHERE migration_key = '0064_real_chart_rendering_export_engine';
