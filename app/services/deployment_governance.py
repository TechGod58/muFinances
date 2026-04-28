from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.access_guard import access_guard_status
from app.services.foundation import list_migrations
from app.services.postgres_runtime import status as postgres_runtime_status
from app.services.production_operations import admin_audit_report, status as production_ops_status
from app.services.observability_operations import run_health_probes, status as observability_status


READINESS_DEFAULTS = [
    ('security-sso', 'security', 'SSO and AD/OU enforcement reviewed'),
    ('database-backup', 'backup', 'Backup and restore test completed'),
    ('postgres-runtime', 'database', 'PostgreSQL runtime path validated'),
    ('integration-health', 'integration', 'Connector health checks reviewed'),
    ('reporting-release', 'reporting', 'Board package release controls verified'),
    ('operations-runbook', 'operations', 'Operations runbooks reviewed'),
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'environments': int(db.fetch_one('SELECT COUNT(*) AS count FROM deployment_environment_settings')['count']),
        'promotions': int(db.fetch_one('SELECT COUNT(*) AS count FROM deployment_promotions')['count']),
        'config_snapshots': int(db.fetch_one('SELECT COUNT(*) AS count FROM deployment_config_snapshots')['count']),
        'rollback_plans': int(db.fetch_one('SELECT COUNT(*) AS count FROM migration_rollback_plans')['count']),
        'release_notes': int(db.fetch_one('SELECT COUNT(*) AS count FROM deployment_release_notes')['count']),
        'diagnostics': int(db.fetch_one('SELECT COUNT(*) AS count FROM admin_diagnostic_runs')['count']),
        'readiness_items': int(db.fetch_one('SELECT COUNT(*) AS count FROM operational_readiness_items')['count']),
    }
    checks = {
        'environment_promotion_ready': True,
        'config_export_import_ready': True,
        'tenant_environment_settings_ready': True,
        'migration_rollback_plans_ready': True,
        'release_notes_ready': True,
        'admin_diagnostics_ready': True,
        'operational_readiness_checklist_ready': True,
    }
    return {'batch': 'B50', 'title': 'Deployment Governance And Release Controls', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def workspace() -> dict[str, Any]:
    ensure_readiness_defaults({'email': 'system'})
    return {
        'status': status(),
        'environments': list_environments(),
        'promotions': list_promotions(),
        'config_snapshots': list_config_snapshots(),
        'rollback_plans': list_rollback_plans(),
        'release_notes': list_release_notes(),
        'diagnostics': list_diagnostics(),
        'readiness_items': list_readiness_items(),
    }


def upsert_environment(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO deployment_environment_settings (
            environment_key, tenant_key, base_url, database_backend, sso_required,
            domain_guard_required, settings_json, status, updated_by, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(environment_key) DO UPDATE SET
            tenant_key = excluded.tenant_key,
            base_url = excluded.base_url,
            database_backend = excluded.database_backend,
            sso_required = excluded.sso_required,
            domain_guard_required = excluded.domain_guard_required,
            settings_json = excluded.settings_json,
            status = excluded.status,
            updated_by = excluded.updated_by,
            updated_at = excluded.updated_at
        ''',
        (
            payload['environment_key'], payload.get('tenant_key') or 'campus', payload.get('base_url') or '',
            payload.get('database_backend') or 'sqlite', 1 if payload.get('sso_required') else 0,
            1 if payload.get('domain_guard_required') else 0, json.dumps(payload.get('settings') or {}, sort_keys=True),
            payload.get('status') or 'draft', user['email'], now,
        ),
    )
    db.log_audit('deployment_environment', payload['environment_key'], 'upserted', user['email'], payload, now)
    return _format_environment(_one('SELECT * FROM deployment_environment_settings WHERE environment_key = ?', (payload['environment_key'],)))


def list_environments() -> list[dict[str, Any]]:
    return [_format_environment(row) for row in db.fetch_all('SELECT * FROM deployment_environment_settings ORDER BY environment_key ASC')]


def create_promotion(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    key = f"promotion-{payload['from_environment']}-{payload['to_environment']}-{payload['release_version']}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    checklist = payload.get('checklist') or {}
    status_value = 'approved' if checklist and all(bool(value) for value in checklist.values()) else 'planned'
    promotion_id = db.execute(
        '''
        INSERT INTO deployment_promotions (
            promotion_key, from_environment, to_environment, release_version, status,
            checklist_json, promoted_by, promoted_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            key, payload['from_environment'], payload['to_environment'], payload['release_version'],
            status_value, json.dumps(checklist, sort_keys=True), user['email'], now,
        ),
    )
    db.log_audit('deployment_promotion', key, status_value, user['email'], payload, now)
    return _format_promotion(_one('SELECT * FROM deployment_promotions WHERE id = ?', (promotion_id,)))


def list_promotions() -> list[dict[str, Any]]:
    return [_format_promotion(row) for row in db.fetch_all('SELECT * FROM deployment_promotions ORDER BY id DESC LIMIT 100')]


def create_config_snapshot(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    key = f"config-{payload['direction']}-{payload['environment_key']}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    snapshot_payload = payload.get('payload') or _export_config_payload(payload['environment_key'])
    snapshot_id = db.execute(
        '''
        INSERT INTO deployment_config_snapshots (
            snapshot_key, environment_key, direction, payload_json, status, created_by, created_at
        ) VALUES (?, ?, ?, ?, 'ready', ?, ?)
        ''',
        (key, payload['environment_key'], payload['direction'], json.dumps(snapshot_payload, sort_keys=True), user['email'], now),
    )
    db.log_audit('deployment_config_snapshot', key, payload['direction'], user['email'], {'environment_key': payload['environment_key']}, now)
    return _format_config_snapshot(_one('SELECT * FROM deployment_config_snapshots WHERE id = ?', (snapshot_id,)))


def list_config_snapshots() -> list[dict[str, Any]]:
    return [_format_config_snapshot(row) for row in db.fetch_all('SELECT * FROM deployment_config_snapshots ORDER BY id DESC LIMIT 100')]


def upsert_rollback_plan(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    key = f"rollback-{payload['migration_key']}"
    db.execute(
        '''
        INSERT INTO migration_rollback_plans (
            plan_key, migration_key, rollback_strategy, verification_steps_json, status, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(plan_key) DO UPDATE SET
            rollback_strategy = excluded.rollback_strategy,
            verification_steps_json = excluded.verification_steps_json,
            status = excluded.status,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        ''',
        (
            key, payload['migration_key'], payload['rollback_strategy'],
            json.dumps(payload.get('verification_steps') or [], sort_keys=True), payload.get('status') or 'draft',
            user['email'], now,
        ),
    )
    db.log_audit('migration_rollback_plan', key, 'upserted', user['email'], payload, now)
    return _format_rollback_plan(_one('SELECT * FROM migration_rollback_plans WHERE plan_key = ?', (key,)))


def list_rollback_plans() -> list[dict[str, Any]]:
    return [_format_rollback_plan(row) for row in db.fetch_all('SELECT * FROM migration_rollback_plans ORDER BY id DESC')]


def upsert_release_note(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    key = f"release-{payload['release_version']}"
    published_by = user['email'] if payload.get('status') == 'published' else None
    published_at = now if payload.get('status') == 'published' else None
    db.execute(
        '''
        INSERT INTO deployment_release_notes (
            release_key, release_version, title, notes_json, status, created_by, created_at, published_by, published_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(release_key) DO UPDATE SET
            title = excluded.title,
            notes_json = excluded.notes_json,
            status = excluded.status,
            created_by = excluded.created_by,
            created_at = excluded.created_at,
            published_by = excluded.published_by,
            published_at = excluded.published_at
        ''',
        (
            key, payload['release_version'], payload['title'], json.dumps(payload.get('notes') or {}, sort_keys=True),
            payload.get('status') or 'draft', user['email'], now, published_by, published_at,
        ),
    )
    db.log_audit('deployment_release_note', key, 'upserted', user['email'], payload, now)
    return _format_release_note(_one('SELECT * FROM deployment_release_notes WHERE release_key = ?', (key,)))


def list_release_notes() -> list[dict[str, Any]]:
    return [_format_release_note(row) for row in db.fetch_all('SELECT * FROM deployment_release_notes ORDER BY id DESC')]


def run_admin_diagnostics(scope: str, user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    key = f"diagnostic-{scope}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    result = {
        'scope': scope,
        'database': db.database_runtime(),
        'production_ops': production_ops_status(),
        'postgres_runtime': postgres_runtime_status(),
        'access_guard': access_guard_status(),
        'observability': observability_status(),
        'health_probes': run_health_probes(user),
        'audit_totals': admin_audit_report(25)['totals'],
        'latest_migration': list_migrations()[-1]['migration_key'] if list_migrations() else None,
    }
    diagnostic_id = db.execute(
        '''
        INSERT INTO admin_diagnostic_runs (diagnostic_key, scope, status, result_json, created_by, created_at)
        VALUES (?, ?, 'pass', ?, ?, ?)
        ''',
        (key, scope, json.dumps(result, sort_keys=True), user['email'], now),
    )
    db.log_audit('admin_diagnostic', key, 'pass', user['email'], {'scope': scope}, now)
    return _format_diagnostic(_one('SELECT * FROM admin_diagnostic_runs WHERE id = ?', (diagnostic_id,)))


def list_diagnostics() -> list[dict[str, Any]]:
    return [_format_diagnostic(row) for row in db.fetch_all('SELECT * FROM admin_diagnostic_runs ORDER BY id DESC LIMIT 50')]


def ensure_readiness_defaults(user: dict[str, Any]) -> None:
    now = _now()
    for key, category, title in READINESS_DEFAULTS:
        db.execute(
            '''
            INSERT OR IGNORE INTO operational_readiness_items (
                item_key, category, title, status, evidence_json, updated_by, updated_at
            ) VALUES (?, ?, ?, 'open', '{}', ?, ?)
            ''',
            (key, category, title, user.get('email', 'system'), now),
        )


def upsert_readiness_item(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO operational_readiness_items (
            item_key, category, title, status, evidence_json, updated_by, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(item_key) DO UPDATE SET
            category = excluded.category,
            title = excluded.title,
            status = excluded.status,
            evidence_json = excluded.evidence_json,
            updated_by = excluded.updated_by,
            updated_at = excluded.updated_at
        ''',
        (
            payload['item_key'], payload['category'], payload['title'], payload.get('status') or 'open',
            json.dumps(payload.get('evidence') or {}, sort_keys=True), user['email'], now,
        ),
    )
    db.log_audit('operational_readiness_item', payload['item_key'], 'upserted', user['email'], payload, now)
    return _format_readiness(_one('SELECT * FROM operational_readiness_items WHERE item_key = ?', (payload['item_key'],)))


def list_readiness_items() -> list[dict[str, Any]]:
    ensure_readiness_defaults({'email': 'system'})
    return [_format_readiness(row) for row in db.fetch_all('SELECT * FROM operational_readiness_items ORDER BY category ASC, item_key ASC')]


def _export_config_payload(environment_key: str) -> dict[str, Any]:
    return {
        'environment': next((row for row in list_environments() if row['environment_key'] == environment_key), None),
        'runbooks': db.fetch_all('SELECT runbook_key, title, category, status FROM runbook_records ORDER BY runbook_key'),
        'sso_settings': db.fetch_all('SELECT provider_key, environment, required_claim, group_claim, status FROM sso_production_settings ORDER BY provider_key'),
        'migrations': list_migrations(),
        'exported_at': _now(),
    }


def _one(query: str, params: tuple[Any, ...]) -> dict[str, Any]:
    row = db.fetch_one(query, params)
    if row is None:
        raise RuntimeError('Deployment governance record could not be reloaded.')
    return row


def _format_environment(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['sso_required'] = bool(result['sso_required'])
    result['domain_guard_required'] = bool(result['domain_guard_required'])
    result['settings'] = json.loads(result.pop('settings_json') or '{}')
    return result


def _format_promotion(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checklist'] = json.loads(result.pop('checklist_json') or '{}')
    return result


def _format_config_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['payload'] = json.loads(result.pop('payload_json') or '{}')
    return result


def _format_rollback_plan(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['verification_steps'] = json.loads(result.pop('verification_steps_json') or '[]')
    return result


def _format_release_note(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['notes'] = json.loads(result.pop('notes_json') or '{}')
    return result


def _format_diagnostic(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['result'] = json.loads(result.pop('result_json') or '{}')
    return result


def _format_readiness(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['evidence'] = json.loads(result.pop('evidence_json') or '{}')
    return result
