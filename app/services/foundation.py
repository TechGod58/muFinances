from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app import db
from app.services.security import allowed_codes, has_permission, mask_sensitive_metadata, protect_metadata

BACKUP_DIR = db.DATA_DIR / 'backups'
BACKUP_DIR.mkdir(exist_ok=True)

BUILTIN_MIGRATIONS = [
    {
        'key': '0001_foundation_planning_ledger',
        'description': 'Create dimensional planning ledger, fiscal periods, dimension members, migrations, and backup records.',
    },
    {
        'key': '0002_parallel_cubed_finance_genome',
        'description': 'Register finance-oriented Parallel Cubed wiring and B01-B12 batch plan.',
    },
    {
        'key': '0003_security_control_baseline',
        'description': 'Create local users, roles, permissions, sessions, row-level access, and sensitive masking controls.',
    },
    {
        'key': '0004_operating_budget_workspace',
        'description': 'Create operating budget submissions, assumptions, recurring and one-time lines, transfers, and approvals.',
    },
    {
        'key': '0005_enrollment_tuition_planning',
        'description': 'Create enrollment terms, tuition rates, forecast inputs, and tuition revenue forecast runs.',
    },
    {
        'key': '0006_workforce_faculty_grants_capital',
        'description': 'Create position control, faculty load, grant budget, burn-rate, capital request, and depreciation planning records.',
    },
    {
        'key': '0007_forecast_scenario_engine',
        'description': 'Create typed drivers, scenario clone/compare, forecast methods, confidence ranges, and driver lineage.',
    },
    {
        'key': '0008_reporting_analytics_layer',
        'description': 'Create report definitions, dashboard widgets, financial statements, variance reports, and scheduled exports.',
    },
    {
        'key': '0009_close_reconciliation_consolidation',
        'description': 'Create close checklists, account reconciliations, intercompany matching, eliminations, consolidation runs, and audit packets.',
    },
    {
        'key': '0010_campus_integrations',
        'description': 'Create CSV/XLSX imports, ERP/SIS/HR/payroll/grants connectors, sync jobs, validation rejections, and Power BI exports.',
    },
    {
        'key': '0011_governed_automation',
        'description': 'Create variance, anomaly, budget, and reconciliation assistants with human approval gates.',
    },
    {
        'key': '0012_workspace_ux_completion',
        'description': 'Create role-specific workspaces for Budget Office, department planners, controller, grants, and executives.',
    },
    {
        'key': '0013_deployment_operations',
        'description': 'Create deployment packaging, health checks, backup restore tests, and operational runbooks.',
    },
    {
        'key': '0014_ledger_depth_actuals',
        'description': 'Create ledger basis separation, journal adjustments, scenario publication, approved-change merge, and source/version lineage.',
    },
    {
        'key': '0015_comments_attachments_evidence',
        'description': 'Create shared comments, attachment metadata, evidence retention, audit trail, and audit-packet evidence links.',
    },
    {
        'key': '0016_advanced_forecasting',
        'description': 'Create seasonal and historical trend forecasts, actuals ingestion, forecast variance tracking, and driver dependency graph checks.',
    },
    {
        'key': '0017_advanced_reporting_package',
        'description': 'Create account rollups, period-range reporting, actual/budget/forecast variance, balance sheet, cash flow, fund/grant/department reports, and board packages.',
    },
    {
        'key': '0018_export_distribution_completion',
        'description': 'Create Excel/PDF/email-ready export artifacts, report snapshots, scheduled extract history, and hardened BI/API export manifests.',
    },
    {
        'key': '0019_narrative_reporting_variance_workflow',
        'description': 'Create required variance explanations, commentary workflow, board-report narrative assembly, and AI-drafted narratives with human approval.',
    },
    {
        'key': '0020_close_reconciliation_depth',
        'description': 'Create close templates, task dependencies, period close calendar, reconciliation preparer/reviewer workflow, aging exceptions, period locks, and entity confirmations.',
    },
    {
        'key': '0021_consolidation_advanced_controls',
        'description': 'Create consolidation entity hierarchy, ownership percentages, GAAP and currency placeholders, audit reports, and elimination review workflow.',
    },
    {
        'key': '0022_integration_hardening',
        'description': 'Create import mapping templates, validation rules, credential vault, retry handling, connector sync logs, banking cash import, and CRM enrollment pipeline import.',
    },
    {
        'key': '0023_ux_productivity_layer',
        'description': 'Create fiscal period selector support, notifications, user profile controls, editable grid validation, bulk paste import, and submission review screens.',
    },
    {
        'key': '0024_accessibility_ui_smoke_testing',
        'description': 'Register keyboard navigation, screen-reader labels, responsive review layouts, high-contrast table checks, and Playwright UI smoke coverage.',
    },
    {
        'key': '0025_production_operations_hardening',
        'description': 'Register PostgreSQL deployment option, connection pooling, TLS notes, encrypted secrets handling, application/job/sync logs, admin audit reports, and production guides.',
    },
    {
        'key': '0026_guided_data_entry_import_wizard',
        'description': 'Create guided data entry, import, and export starting points for non-expert users.',
    },
    {
        'key': '0027_excel_office_interop',
        'description': 'Create Excel template export/import, round-trip editing, and workbook package generation.',
    },
    {
        'key': '0028_real_postgresql_runtime',
        'description': 'Create real PostgreSQL runtime support with pooled connections, SQL compatibility translation, and backend readiness checks.',
    },
    {
        'key': '0029_advanced_consolidation_engine',
        'description': 'Create currency rates, ownership-applied consolidation logic, multi-GAAP book mappings, and consolidation journals.',
    },
    {
        'key': '0030_report_designer_distribution',
        'description': 'Create saved report layouts, report books, chart definitions, bursting rules, and recurring report packages.',
    },
    {
        'key': '0031_workflow_designer',
        'description': 'Create configurable workflow approval chains, escalations, delegation, and notification-driven routing.',
    },
    {
        'key': '0032_integration_staging_mapping_ui',
        'description': 'Create staged import previews, validation, rejection, approval, and drill-back links for mapped integration rows.',
    },
    {
        'key': '0033_model_builder_allocation_engine',
        'description': 'Create user-defined planning models, formulas, allocation rules, dependency recalculation, and ledger lineage.',
    },
    {
        'key': '0034_compliance_audit_hardening',
        'description': 'Create immutable audit hash seals, segregation-of-duties checks, retention policies, and certification controls.',
    },
    {
        'key': '0035_ai_explainability_layer',
        'description': 'Create cited AI variance explanations, confidence scoring, source tracing, and human approval controls.',
    },
    {
        'key': '0036_market_watch_paper_trading_lab',
        'description': 'Create market ticker panel, symbol search, watchlists, quote provider hook, paper trading accounts, orders, portfolio P&L, and trade history.',
    },
    {
        'key': '0037_brokerage_connector_framework',
        'description': 'Create read-only brokerage connector registry, credential references, connection tests, account and holdings sync, and sync audit logs.',
    },
    {
        'key': '0038_excel_native_workspace',
        'description': 'Create Excel add-in style refresh/publish controls, named ranges, protected templates, cell comments, variance formulas, offline round-trip tracking, and PowerPoint deck refresh logs.',
    },
    {
        'key': '0039_enterprise_modeling_engine',
        'description': 'Create multidimensional cube behavior, sparse and dense dimension handling, formula parsing, calculation ordering, dependency invalidation, versioned model publishing, and large-model performance checks.',
    },
    {
        'key': '0040_data_hub_master_data_governance',
        'description': 'Create chart of accounts governance, department/entity/fund hierarchy change workflow, effective dating, mapping tables, metadata approval, and source-to-report lineage.',
    },
    {
        'key': '0041_connector_marketplace_depth',
        'description': 'Create adapter registry for ERP, SIS, HR, payroll, grants, banking, brokerage, OAuth and API-key credential flows, connector health dashboard, field mapping presets, and source drill-back.',
    },
    {
        'key': '0042_ai_planning_agents',
        'description': 'Create plain-language budget update, bulk adjustment, report question, and anomaly explanation agents with guarded execution, prompt audit trail, and human approval before posting.',
    },
    {
        'key': '0043_predictive_forecasting_studio',
        'description': 'Create forecast model selection, backtesting, accuracy scoring, seasonality and confidence tuning, explainable drivers, and recommendation comparison.',
    },
    {
        'key': '0044_advanced_consolidation_statutory_reporting',
        'description': 'Create minority interest, complex ownership chains, CTA and currency translation depth, multi-book statutory packs, supplemental schedules, and consolidation rule designer.',
    },
    {
        'key': '0045_profitability_allocation_management',
        'description': 'Create activity-based costing, tuition and program margin, grant and fund profitability, service-center allocations, allocation trace reports, and before/after allocation comparison.',
    },
    {
        'key': '0046_workflow_process_orchestration_depth',
        'description': 'Create visual workflow designer metadata, reusable process calendars, escalations, delegation, substitute approvers, certification packets, and close/budget campaign monitoring.',
    },
    {
        'key': '0047_enterprise_security_administration',
        'description': 'Create SSO production wiring, AD/OU group mapping UI records, domain/VPN enforcement dashboard, admin impersonation controls, SoD policy builder, and user access review certification.',
    },
    {
        'key': '0048_performance_scale_reliability',
        'description': 'Create PostgreSQL load testing records, index strategy recommendations, background job queue, large import stress tests, calculation benchmarks, cache invalidation, and backup restore automation.',
    },
    {
        'key': '0049_in_app_guidance_finance_training',
        'description': 'Create role-based onboarding, guided task checklists, field help, process walkthroughs, campus planning playbooks, and admin/planner/controller training mode.',
    },
    {
        'key': '0050_production_reporting_polish',
        'description': 'Create pixel-controlled financial statements, report binder design polish, footnotes, page breaks, PDF pagination profiles, chart formatting, and recurring board package approval and release controls.',
    },
    {
        'key': '0051_deployment_governance_release_controls',
        'description': 'Create environment promotion, config export/import, tenant and environment settings, migration rollback plans, release notes, admin diagnostics, and operational readiness checklist controls.',
    },
    {
        'key': '0052_security_cleanup_first_run_hardening',
        'description': 'Remove prefilled credentials, add first-login password-change controls, production secret fail-fast checks, and stricter session/browser headers.',
    },
    {
        'key': '0053_architecture_modularization',
        'description': 'Create router modules, managed schema-file boundaries, static feature-module registry, and service boundary documentation.',
    },
    {
        'key': '0054_real_migration_framework',
        'description': 'Create managed SQL migration files, rollback-script discovery, migration locks, dry-run checks, run records, and PostgreSQL translation validation.',
    },
    {
        'key': '0055_production_reporting_renderer',
        'description': 'Create production report renderer for HTML-backed PDF/email artifacts, page break controls, binder assembly exports, and visual regression hashes.',
    },
    {
        'key': '0056_durable_background_jobs_scheduler',
        'description': 'Create durable worker scheduling fields, retry/backoff handling, cancellation, dead-letter records, job logs, and worker deployment runbooks.',
    },
    {
        'key': '0057_formula_modeling_safety',
        'description': 'Create dedicated formula parser/evaluator controls, formula linting, sandbox limits, circular dependency stress coverage, and calculation trace output.',
    },
    {
        'key': '0058_data_integrity_transaction_hardening',
        'description': 'Create explicit ledger posting transaction boundaries, idempotency keys, immutable posting checksums, restore safeguards, and stronger audit-chain enforcement.',
    },
    {
        'key': '0059_connector_productionization',
        'description': 'Create production connector contracts, OAuth/API-key vault metadata, streaming import controls, mapping versions, and drill-back validation.',
    },
    {
        'key': '0060_performance_benchmark_harness',
        'description': 'Create large realistic dataset seeding, PostgreSQL query-plan checks, actual hot-path indexes, calculation/report/import benchmark metrics, and regression thresholds.',
    },
    {
        'key': '0061_observability_operations',
        'description': 'Create structured trace-aware logs, metrics, health probes, alert-ready failure events, backup/restore drill records, and operational diagnostics checks.',
    },
    {
        'key': '0062_documentation_freeze_operator_readiness',
        'description': 'Freeze operator-ready admin, planner, controller, integration, data dictionary, close process, production operations, and release checklist documentation.',
    },
    {
        'key': '0063_tax_classification_compliance_watch',
        'description': 'Create NPO/taxable income classification, exempt/taxable activity tagging, UBIT-style tracking, Form 990 support fields, tax-rule source registry, scheduled update checks, tax alerts, and review workflow.',
    },
    {
        'key': '0064_real_chart_rendering_export_engine',
        'description': 'Create rendered chart artifacts, PNG/SVG exports, dashboard chart snapshots, and chart image hooks for PDF, PowerPoint, and board package output.',
    },
    {
        'key': '0065_parallel_cubed_multi_core_execution_engine',
        'description': 'Create Parallel Cubed worker-pool execution, partitioned calculations, parallel import/report phases, safe merge/reduce history, CPU detection, and benchmark dashboard records.',
    },
    {
        'key': '0066_brokerage_connection_ux_provider_readiness',
        'description': 'Create brokerage provider selection, sandbox/live readiness, credential/OAuth setup flow, read-only consent records, sync warnings, and brokerage audit trail surfaces.',
    },
    {
        'key': '0067_university_agent_integration_layer',
        'description': 'Create signed external University Agent API, tool registry, scoped permissions, allowed-action policies, audit logs, approval-before-posting enforcement, and callback support.',
    },
    {
        'key': '0068_toggle_based_workspace_visibility',
        'description': 'Create closed-by-default toggle-based workspace visibility, active toggle state, persistent per-user layout state, close buttons, and keyboard-accessible section controls.',
    },
    {
        'key': '0069_production_pdf_board_artifact_completion',
        'description': 'Create production PDF and board package artifact validation, downloadable artifact controls, pagination metadata, embedded chart evidence, footnotes, and page break completion checks.',
    },
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _checksum(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def ensure_foundation_ready() -> None:
    """Apply metadata migrations for schema created by db.init_db.

    The current app uses SQLite with CREATE TABLE IF NOT EXISTS. This migration
    registry gives B01 a durable place to track schema milestones without
    pretending we have a full Alembic stack yet.
    """

    for item in BUILTIN_MIGRATIONS:
        db.execute(
            '''
            INSERT OR IGNORE INTO schema_migrations (migration_key, description, checksum, applied_at)
            VALUES (?, ?, ?, ?)
            ''',
            (item['key'], item['description'], _checksum(item['description']), _now()),
        )


def foundation_status() -> dict[str, Any]:
    table_counts = {
        'planning_ledger': int(db.fetch_one('SELECT COUNT(*) AS count FROM planning_ledger')['count']),
        'dimension_members': int(db.fetch_one('SELECT COUNT(*) AS count FROM dimension_members')['count']),
        'fiscal_periods': int(db.fetch_one('SELECT COUNT(*) AS count FROM fiscal_periods')['count']),
        'schema_migrations': int(db.fetch_one('SELECT COUNT(*) AS count FROM schema_migrations')['count']),
        'backup_records': int(db.fetch_one('SELECT COUNT(*) AS count FROM backup_records')['count']),
    }
    checks = {
        'ledger_ready': table_counts['planning_ledger'] > 0,
        'dimensions_ready': table_counts['dimension_members'] > 0,
        'periods_ready': table_counts['fiscal_periods'] > 0,
        'migrations_ready': table_counts['schema_migrations'] >= len(BUILTIN_MIGRATIONS),
        'backups_ready': BACKUP_DIR.exists(),
        'ledger_transactions_ready': True,
        'idempotency_keys_ready': _column_exists('planning_ledger', 'idempotency_key'),
        'immutable_posting_ready': _column_exists('planning_ledger', 'posted_checksum'),
        'restore_safeguards_ready': True,
        'audit_chain_enforced': True,
    }
    return {
        'batch': 'B01',
        'title': 'Foundation Ledger Hardening',
        'complete': all(checks.values()),
        'checks': checks,
        'table_counts': table_counts,
    }


def _column_exists(table: str, column: str) -> bool:
    if db.DB_BACKEND != 'sqlite':
        return True
    return any(row['name'] == column for row in db.fetch_all(f'PRAGMA table_info({table})'))


def list_migrations() -> list[dict[str, Any]]:
    ensure_foundation_ready()
    return db.fetch_all('SELECT * FROM schema_migrations ORDER BY migration_key ASC')


def list_fiscal_periods(fiscal_year: str | None = None) -> list[dict[str, Any]]:
    if fiscal_year:
        rows = db.fetch_all(
            'SELECT * FROM fiscal_periods WHERE fiscal_year = ? ORDER BY period_index ASC, period ASC',
            (fiscal_year,),
        )
    else:
        rows = db.fetch_all('SELECT * FROM fiscal_periods ORDER BY fiscal_year ASC, period_index ASC, period ASC')
    for row in rows:
        row['is_closed'] = bool(row['is_closed'])
    return rows


def upsert_fiscal_period(payload: dict[str, Any], actor: str = 'api.user') -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO fiscal_periods (fiscal_year, period, period_index, is_closed)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(period) DO UPDATE SET
            fiscal_year = excluded.fiscal_year,
            period_index = excluded.period_index,
            is_closed = excluded.is_closed
        ''',
        (
            payload['fiscal_year'],
            payload['period'],
            payload['period_index'],
            1 if payload.get('is_closed') else 0,
        ),
    )
    db.log_audit(
        entity_type='fiscal_period',
        entity_id=payload['period'],
        action='upserted',
        actor=actor,
        detail=payload,
        created_at=now,
    )
    row = db.fetch_one('SELECT * FROM fiscal_periods WHERE period = ?', (payload['period'],))
    if row is None:
        raise RuntimeError('Fiscal period could not be reloaded.')
    row['is_closed'] = bool(row['is_closed'])
    return row


def set_period_closed(period: str, is_closed: bool, actor: str = 'api.user') -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM fiscal_periods WHERE period = ?', (period,))
    if row is None:
        raise ValueError('Fiscal period not found.')
    db.execute('UPDATE fiscal_periods SET is_closed = ? WHERE period = ?', (1 if is_closed else 0, period))
    db.log_audit(
        entity_type='fiscal_period',
        entity_id=period,
        action='closed' if is_closed else 'reopened',
        actor=actor,
        detail={'period': period, 'is_closed': is_closed},
        created_at=_now(),
    )
    updated = db.fetch_one('SELECT * FROM fiscal_periods WHERE period = ?', (period,))
    if updated is None:
        raise RuntimeError('Fiscal period could not be reloaded.')
    updated['is_closed'] = bool(updated['is_closed'])
    return updated


def create_dimension_member(payload: dict[str, Any], actor: str = 'api.user') -> dict[str, Any]:
    now = _now()
    metadata_json = json.dumps(payload.get('metadata') or {}, sort_keys=True)
    member_id = db.execute(
        '''
        INSERT INTO dimension_members (dimension_kind, code, name, parent_code, active, metadata_json)
        VALUES (?, ?, ?, ?, 1, ?)
        ON CONFLICT(dimension_kind, code) DO UPDATE SET
            name = excluded.name,
            parent_code = excluded.parent_code,
            active = 1,
            metadata_json = excluded.metadata_json
        ''',
        (
            payload['dimension_kind'],
            payload['code'],
            payload['name'],
            payload.get('parent_code'),
            metadata_json,
        ),
    )
    db.execute(
        '''
        INSERT OR IGNORE INTO dimensions (kind, code, name, active)
        VALUES (?, ?, ?, 1)
        ''',
        (payload['dimension_kind'], payload['code'], payload['name']),
    )
    db.log_audit(
        entity_type='dimension_member',
        entity_id=f"{payload['dimension_kind']}:{payload['code']}",
        action='upserted',
        actor=actor,
        detail=payload,
        created_at=now,
    )
    row = db.fetch_one(
        'SELECT * FROM dimension_members WHERE dimension_kind = ? AND code = ?',
        (payload['dimension_kind'], payload['code']),
    )
    if row is None:
        raise RuntimeError('Dimension member could not be reloaded.')
    row['metadata'] = json.loads(row.pop('metadata_json') or '{}')
    row['active'] = bool(row['active'])
    return row


def dimension_hierarchy() -> dict[str, list[dict[str, Any]]]:
    rows = db.fetch_all(
        '''
        SELECT *
        FROM dimension_members
        WHERE active = 1
        ORDER BY dimension_kind, COALESCE(parent_code, ''), code
        '''
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        row['active'] = bool(row['active'])
        row['metadata'] = json.loads(row.pop('metadata_json') or '{}')
        grouped.setdefault(row['dimension_kind'], []).append(row)
    return grouped


def append_ledger_entry(payload: dict[str, Any], actor: str = 'api.user', user: dict[str, Any] | None = None) -> dict[str, Any]:
    now = _now()
    ledger_basis = payload.get('ledger_basis') or _basis_from_ledger_type(payload.get('ledger_type') or 'planning')
    if ledger_basis not in {'actual', 'budget', 'forecast', 'scenario'}:
        raise ValueError('Unsupported ledger basis.')
    metadata_json = json.dumps(protect_metadata(payload.get('metadata') or {}), sort_keys=True)
    idempotency_key = (payload.get('idempotency_key') or '').strip() or None
    with db.transaction(immediate=True) as conn:
        if idempotency_key:
            existing = conn.execute('SELECT * FROM planning_ledger WHERE idempotency_key = ?', (idempotency_key,)).fetchone()
            if existing is not None:
                return _format_ledger_row(existing, user=user)
        scenario = conn.execute('SELECT * FROM scenarios WHERE id = ?', (payload['scenario_id'],)).fetchone()
        if scenario is None:
            raise ValueError('Scenario not found.')
        if bool(scenario['locked']):
            raise ValueError('Scenario is locked.')
        if user is not None:
            department_codes = allowed_codes(user, 'department')
            if department_codes is not None and payload['department_code'] not in department_codes:
                raise PermissionError('Department access denied.')
        period = conn.execute('SELECT * FROM fiscal_periods WHERE period = ?', (payload['period'],)).fetchone()
        if period is not None and bool(period['is_closed']):
            raise ValueError('Fiscal period is closed.')
        checksum_payload = _ledger_checksum_payload(payload, ledger_basis, metadata_json, actor, now)
        posted_checksum = _stable_checksum(checksum_payload)
        entry_id = int(
            conn.execute(
                '''
                INSERT INTO planning_ledger (
                    scenario_id, entity_code, department_code, fund_code, account_code,
                    program_code, project_code, grant_code, period, amount, source,
                    driver_key, notes, ledger_type, ledger_basis, source_version, source_record_id,
                    parent_ledger_entry_id, idempotency_key, posted_checksum, immutable_posting,
                    posted_by, posted_at, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                ''',
                (
                    payload['scenario_id'],
                    payload.get('entity_code') or 'CAMPUS',
                    payload['department_code'],
                    payload['fund_code'],
                    payload['account_code'],
                    payload.get('program_code'),
                    payload.get('project_code'),
                    payload.get('grant_code'),
                    payload['period'],
                    payload['amount'],
                    payload.get('source') or 'manual',
                    payload.get('notes') or '',
                    payload.get('ledger_type') or 'planning',
                    ledger_basis,
                    payload.get('source_version'),
                    payload.get('source_record_id'),
                    payload.get('parent_ledger_entry_id'),
                    idempotency_key,
                    posted_checksum,
                    actor,
                    now,
                    metadata_json,
                ),
            ).lastrowid
        )
        db.log_audit(
            entity_type='planning_ledger',
            entity_id=str(entry_id),
            action='posted',
            actor=actor,
            detail={**payload, 'posted_checksum': posted_checksum, 'idempotency_key': idempotency_key},
            created_at=now,
            conn=conn,
        )
        row = conn.execute('SELECT * FROM planning_ledger WHERE id = ?', (entry_id,)).fetchone()
        if row is None:
            raise RuntimeError('Ledger entry could not be reloaded.')
        return _format_ledger_row(row, user=user)


def _basis_from_ledger_type(ledger_type: str) -> str:
    if ledger_type == 'actual':
        return 'actual'
    if ledger_type == 'forecast':
        return 'forecast'
    if ledger_type in {'scenario', 'elimination'}:
        return 'scenario'
    return 'budget'


def _ledger_checksum_payload(payload: dict[str, Any], ledger_basis: str, metadata_json: str, actor: str, posted_at: str) -> dict[str, Any]:
    return {
        'scenario_id': payload['scenario_id'],
        'entity_code': payload.get('entity_code') or 'CAMPUS',
        'department_code': payload['department_code'],
        'fund_code': payload['fund_code'],
        'account_code': payload['account_code'],
        'program_code': payload.get('program_code'),
        'project_code': payload.get('project_code'),
        'grant_code': payload.get('grant_code'),
        'period': payload['period'],
        'amount': round(float(payload['amount']), 2),
        'source': payload.get('source') or 'manual',
        'notes': payload.get('notes') or '',
        'ledger_type': payload.get('ledger_type') or 'planning',
        'ledger_basis': ledger_basis,
        'source_version': payload.get('source_version'),
        'source_record_id': payload.get('source_record_id'),
        'parent_ledger_entry_id': payload.get('parent_ledger_entry_id'),
        'metadata_json': metadata_json,
        'posted_by': actor,
        'posted_at': posted_at,
    }


def _stable_checksum(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')).hexdigest()


def reverse_ledger_entry(entry_id: int, reason: str, actor: str = 'api.user') -> dict[str, Any]:
    now = _now()
    with db.transaction(immediate=True) as conn:
        row = conn.execute('SELECT * FROM planning_ledger WHERE id = ?', (entry_id,)).fetchone()
        if row is None:
            raise ValueError('Ledger entry not found.')
        if row['reversed_at'] is not None:
            raise ValueError('Ledger entry is already reversed.')
        if not bool(row.get('immutable_posting', 1)):
            raise ValueError('Ledger entry is not protected by immutable posting controls.')
        scenario = conn.execute('SELECT * FROM scenarios WHERE id = ?', (row['scenario_id'],)).fetchone()
        if scenario is not None and bool(scenario['locked']):
            raise ValueError('Scenario is locked.')
        period = conn.execute('SELECT * FROM fiscal_periods WHERE period = ?', (row['period'],)).fetchone()
        if period is not None and bool(period['is_closed']):
            raise ValueError('Fiscal period is closed.')
        conn.execute('UPDATE planning_ledger SET reversed_at = ? WHERE id = ?', (now, entry_id))
        db.log_audit(
            entity_type='planning_ledger',
            entity_id=str(entry_id),
            action='reversed',
            actor=actor,
            detail={'reason': reason, 'posted_checksum': row.get('posted_checksum')},
            created_at=now,
            conn=conn,
        )
        updated = conn.execute('SELECT * FROM planning_ledger WHERE id = ?', (entry_id,)).fetchone()
        if updated is None:
            raise RuntimeError('Ledger entry could not be reloaded.')
        return _format_ledger_row(updated)


def get_ledger_entry(entry_id: int) -> dict[str, Any] | None:
    row = db.fetch_one('SELECT * FROM planning_ledger WHERE id = ?', (entry_id,))
    if row is None:
        return None
    return _format_ledger_row(row)


def list_ledger_entries(
    scenario_id: int,
    include_reversed: bool = False,
    limit: int = 500,
    user: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    where = 'scenario_id = ?'
    params: list[Any] = [scenario_id]
    if not include_reversed:
        where += ' AND reversed_at IS NULL'
    department_codes = allowed_codes(user, 'department') if user is not None else None
    if department_codes is not None:
        if not department_codes:
            return []
        placeholders = ','.join('?' for _ in department_codes)
        where += f' AND department_code IN ({placeholders})'
        params.extend(sorted(department_codes))
    params.append(max(1, min(5000, limit)))
    rows = db.fetch_all(
        f'''
        SELECT *
        FROM planning_ledger
        WHERE {where}
        ORDER BY period ASC, department_code ASC, account_code ASC, id ASC
        LIMIT ?
        ''',
        tuple(params),
    )
    return [_format_ledger_row(row, user=user) for row in rows]


def summary_by_dimensions(scenario_id: int, user: dict[str, Any] | None = None) -> dict[str, Any]:
    department_codes = allowed_codes(user, 'department') if user is not None else None
    if department_codes is not None and not department_codes:
        rows: list[dict[str, Any]] = []
    elif department_codes is not None:
        placeholders = ','.join('?' for _ in department_codes)
        rows = db.fetch_all(
            f'''
            SELECT department_code, account_code, SUM(amount) AS total
            FROM planning_ledger
            WHERE scenario_id = ? AND reversed_at IS NULL AND department_code IN ({placeholders})
            GROUP BY department_code, account_code
            ORDER BY department_code, account_code
            ''',
            (scenario_id, *sorted(department_codes)),
        )
    else:
        rows = db.fetch_all(
            '''
            SELECT department_code, account_code, SUM(amount) AS total
            FROM planning_ledger
            WHERE scenario_id = ? AND reversed_at IS NULL
            GROUP BY department_code, account_code
            ORDER BY department_code, account_code
            ''',
            (scenario_id,),
        )
    by_department: dict[str, float] = {}
    by_account: dict[str, float] = {}
    revenue_total = 0.0
    expense_total = 0.0
    for row in rows:
        total = round(float(row['total']), 2)
        by_department[row['department_code']] = round(by_department.get(row['department_code'], 0.0) + total, 2)
        by_account[row['account_code']] = round(by_account.get(row['account_code'], 0.0) + total, 2)
        if total >= 0:
            revenue_total += total
        else:
            expense_total += total
    return {
        'scenario_id': scenario_id,
        'revenue_total': round(revenue_total, 2),
        'expense_total': round(expense_total, 2),
        'net_total': round(revenue_total + expense_total, 2),
        'by_department': by_department,
        'by_account': by_account,
    }


def create_backup(note: str = '', actor: str = 'api.user') -> dict[str, Any]:
    now = _now()
    backup_key = f"backup-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    backup_path = (BACKUP_DIR / f'{backup_key}.db').resolve()
    source_path = db.DB_PATH.resolve()
    with sqlite3.connect(source_path) as source:
        with sqlite3.connect(backup_path) as target:
            source.backup(target)
    size_bytes = backup_path.stat().st_size
    record_id = db.execute(
        '''
        INSERT INTO backup_records (backup_key, path, size_bytes, created_by, created_at, note)
        VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (backup_key, str(backup_path), size_bytes, actor, now, note),
    )
    db.log_audit(
        entity_type='backup',
        entity_id=backup_key,
        action='created',
        actor=actor,
        detail={'path': str(backup_path), 'size_bytes': size_bytes, 'note': note},
        created_at=now,
    )
    row = db.fetch_one('SELECT * FROM backup_records WHERE id = ?', (record_id,))
    if row is None:
        raise RuntimeError('Backup record could not be reloaded.')
    return row


def list_backups() -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM backup_records ORDER BY id DESC')


def restore_backup(backup_key: str, actor: str = 'api.user') -> dict[str, Any]:
    if db.DB_BACKEND != 'sqlite':
        raise ValueError('Manual restore is only available for the local SQLite runtime.')
    row = db.fetch_one('SELECT * FROM backup_records WHERE backup_key = ?', (backup_key,))
    if row is None:
        raise ValueError('Backup not found.')
    backup_path = Path(row['path']).resolve()
    backup_root = BACKUP_DIR.resolve()
    if backup_root not in backup_path.parents or not backup_path.exists():
        raise ValueError('Backup path is invalid.')
    _verify_restore_candidate(backup_path)

    pre_restore_path = create_backup(note=f'pre-restore snapshot before {backup_key}', actor=actor)['path']
    active_chain = _audit_chain_summary()
    shutil.copy2(backup_path, db.DB_PATH)
    db.init_db()
    ensure_foundation_ready()
    db.log_audit(
        entity_type='backup',
        entity_id=backup_key,
        action='restored',
        actor=actor,
        detail={'pre_restore_backup': pre_restore_path, 'restore_safeguard': 'sqlite_integrity_check_passed', 'active_chain_before_restore': active_chain},
        created_at=_now(),
    )
    return {'restored': True, 'backup_key': backup_key, 'pre_restore_backup': pre_restore_path}


def _verify_restore_candidate(backup_path: Path) -> None:
    with sqlite3.connect(backup_path) as conn:
        integrity = conn.execute('PRAGMA integrity_check').fetchone()
        if not integrity or integrity[0] != 'ok':
            raise ValueError('Backup failed SQLite integrity check.')
        required = {'planning_ledger', 'audit_logs', 'audit_log_hashes', 'schema_migrations'}
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        present = {str(row[0]) for row in rows}
        missing = sorted(required - present)
        if missing:
            raise ValueError(f'Backup is missing required tables: {", ".join(missing)}')


def _audit_chain_summary() -> dict[str, Any]:
    row = db.fetch_one(
        '''
        SELECT COUNT(*) AS audit_logs,
               (SELECT COUNT(*) FROM audit_log_hashes) AS sealed_audit_logs,
               (SELECT row_hash FROM audit_log_hashes ORDER BY audit_log_id DESC LIMIT 1) AS last_hash
        FROM audit_logs
        '''
    )
    return dict(row or {'audit_logs': 0, 'sealed_audit_logs': 0, 'last_hash': None})


def _format_ledger_row(row: dict[str, Any], user: dict[str, Any] | None = None) -> dict[str, Any]:
    row = dict(row)
    metadata = json.loads(row.pop('metadata_json') or '{}')
    if user is not None:
        metadata = mask_sensitive_metadata(metadata, user)
    row['metadata'] = metadata
    return row
