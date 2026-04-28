# Data Dictionary

## Purpose And Scope

This dictionary summarizes the main operational tables used by muFinances. It is not a replacement for database schema inspection, but it gives operators and analysts the table purpose, ownership, and retention expectations needed for support and audit work.

## Core Tables

- `planning_ledger`: dimensional planning, actual, forecast, scenario, import, adjustment, and allocation rows. Key fields include `scenario_id`, `entity_code`, `department_code`, `fund_code`, `account_code`, `period`, `amount`, `ledger_basis`, `source_record_id`, `idempotency_key`, `posted_checksum`, and `reversed_at`.
- `dimension_members`: departments, accounts, funds, entities, programs, projects, and grants. Includes parent code and metadata for hierarchy support.
- `fiscal_periods`: fiscal calendar periods, period index, and close state.
- `scenarios`: scenario header, version, status, period range, lock state, and creation time.
- `schema_migrations`: applied migration markers and descriptions.

## Planning Tables

- `budget_submissions`: department budget workflow headers.
- `operating_budget_lines`: recurring and one-time budget detail lines.
- `budget_assumptions`: scenario assumptions used by budget office and planners.
- `budget_transfers`: transfer requests and approvals.
- `typed_drivers`: typed forecast and planning drivers.
- `forecast_runs`: forecast execution history, method, confidence, and result metadata.
- `planning_models`, `model_formulas`, and related model tables: multidimensional model definitions, formulas, versions, and recalculation traces.

## Close And Reporting

- `account_reconciliations`: preparer and reviewer reconciliation records, aging, exceptions, and evidence references.
- `close_checklists`: period close task instances.
- `close_task_templates`: reusable close task templates.
- `consolidation_runs`: consolidation execution summaries.
- `elimination_entries`: elimination journals and review workflow.
- `entity_confirmations`: entity-level confirmation records.
- `report_definitions`, `report_layouts`, `report_books`, and `report_snapshots`: report builder outputs, saved layouts, binder definitions, and retained snapshots.
- `board_package_releases`: board package approval and release records.

## Security And Audit

- `users`: local user identity, password-change state, and account status.
- `roles`, `permissions`, `user_roles`: role and permission assignments.
- `user_dimension_access`: row-level access by dimension.
- `audit_logs` and `audit_log_hashes`: immutable audit history and hash chain.
- `sso_production_settings`, `ad_ou_group_mappings`, and `domain_vpn_checks`: production access guard and SSO/AD support.

## Operations

- `application_logs`: application, job, sync, admin, and security log entries with correlation or trace IDs.
- `observability_metrics`: operational metric records.
- `health_probe_runs`: health probe results and latency.
- `alert_events`: alert-ready failure events and acknowledgment state.
- `backup_records`: created backup files and metadata.
- `backup_restore_drill_runs`: B61 drill records with validation results.
- `restore_test_runs`: deployment operation restore test records.
- `runbook_records`: registered operational guides.
- `background_jobs`, `background_job_logs`, `background_dead_letters`: durable job queue and failure handling.
- `credential_vault`: masked credential references and rotation metadata.
- `connector_sync_logs`: connector-specific sync events.

## Integration Tables

- `external_connectors`: configured source or destination systems.
- `connector_adapters`: adapter contracts, credential schema, and stream limits.
- `connector_auth_flows`: OAuth/API-key flow metadata.
- `import_mapping_templates`: versioned mapping templates.
- `import_batches`, `import_rejections`, `import_staging_batches`, and `import_staging_rows`: import execution, preview, validation, and rejection handling.
- `connector_source_drillbacks`: source-to-ledger drill-back records and validation state.

## Retention Notes

Retain audit logs, close evidence, report snapshots, release records, and backup drill records according to campus finance and audit policy. Do not manually delete rows from audit, evidence, or close tables. Use retention policies and certification controls when disposal or archival is approved.
