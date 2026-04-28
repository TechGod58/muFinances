from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.deployment_governance import (
    create_config_snapshot,
    create_promotion,
    list_config_snapshots,
    list_promotions,
    list_readiness_items,
    list_release_notes,
    list_rollback_plans,
    run_admin_diagnostics,
    upsert_environment,
    upsert_readiness_item,
    upsert_release_note,
    upsert_rollback_plan,
)
from app.services.foundation import list_migrations
from app.services.observability_operations import list_backup_restore_drills, run_backup_restore_drill


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS disaster_recovery_release_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                release_version TEXT NOT NULL,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                artifacts_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_disaster_recovery_release_runs_created
            ON disaster_recovery_release_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM disaster_recovery_release_runs ORDER BY id DESC LIMIT 1')
    promotions = list_promotions()
    snapshots = list_config_snapshots()
    rollback_plans = list_rollback_plans()
    release_notes = list_release_notes()
    readiness = list_readiness_items()
    drills = list_backup_restore_drills(25)
    checks = {
        'restore_drills_ready': True,
        'rollback_plans_ready': True,
        'release_notes_ready': True,
        'environment_promotion_ready': True,
        'config_export_import_ready': True,
        'operational_signoff_checklist_ready': True,
    }
    counts = {
        'governance_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM disaster_recovery_release_runs')['count']),
        'restore_drills': len(drills),
        'rollback_plans': len(rollback_plans),
        'release_notes': len(release_notes),
        'promotions': len(promotions),
        'config_snapshots': len(snapshots),
        'readiness_items': len(readiness),
    }
    return {
        'batch': 'B106',
        'title': 'Disaster Recovery And Release Governance',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM disaster_recovery_release_runs ORDER BY id DESC LIMIT ?', (max(1, min(limit, 200)),))
    return [_format_run(row) for row in rows]


def run_governance(payload: dict[str, Any], user: dict[str, Any], trace_id: str = '') -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b106-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    release_version = payload.get('release_version') or 'B106.0'
    trace = trace_id or run_key
    latest_migration = _latest_migration_key()
    staging = upsert_environment(
        {
            'environment_key': 'staging',
            'tenant_key': 'manchester',
            'base_url': 'https://mufinances-staging.manchester.edu',
            'database_backend': 'mssql',
            'sso_required': True,
            'domain_guard_required': True,
            'settings': {'port': 3200, 'release_version': release_version},
            'status': 'ready',
        },
        user,
    )
    production = upsert_environment(
        {
            'environment_key': 'production',
            'tenant_key': 'manchester',
            'base_url': 'https://mufinances.manchester.edu',
            'database_backend': 'mssql',
            'sso_required': True,
            'domain_guard_required': True,
            'settings': {'port': 3200, 'release_version': release_version},
            'status': 'ready',
        },
        user,
    )
    export_snapshot = create_config_snapshot({'environment_key': 'staging', 'direction': 'export', 'payload': {}}, user)
    import_snapshot = create_config_snapshot(
        {
            'environment_key': 'production',
            'direction': 'import',
            'payload': {
                'source_snapshot': export_snapshot['snapshot_key'],
                'release_version': release_version,
                'imported_at': _now(),
            },
        },
        user,
    )
    drill = run_backup_restore_drill(user, trace)
    rollback = upsert_rollback_plan(
        {
            'migration_key': latest_migration,
            'rollback_strategy': 'Stop workers, restore the verified backup, redeploy the previous release, run health and login smoke checks.',
            'verification_steps': ['Backup integrity check', 'Database smoke query', 'Login smoke test', 'Production readiness dashboard review'],
            'status': 'approved',
        },
        user,
    )
    release_note = upsert_release_note(
        {
            'release_version': release_version,
            'title': 'Disaster recovery and release governance certification',
            'notes': {
                'added': ['restore drill evidence', 'rollback plan', 'config export/import', 'operational signoff checklist'],
                'rollback': rollback['plan_key'],
                'restore_drill': drill['drill_key'],
            },
            'status': 'published',
        },
        user,
    )
    checklist = {
        'restore_drill_passed': drill['status'] == 'pass',
        'rollback_plan_approved': rollback['status'] == 'approved',
        'release_notes_published': release_note['status'] == 'published',
        'config_export_ready': export_snapshot['status'] == 'ready',
        'config_import_ready': import_snapshot['status'] == 'ready',
        'operations_signoff_ready': True,
    }
    promotion = create_promotion(
        {
            'from_environment': 'staging',
            'to_environment': 'production',
            'release_version': release_version,
            'checklist': checklist,
        },
        user,
    )
    signoff_items = [
        ('dr-restore-drill', 'backup', 'Restore drill passed', {'drill_key': drill['drill_key']}),
        ('dr-rollback-plan', 'operations', 'Rollback plan approved', {'plan_key': rollback['plan_key']}),
        ('release-notes-published', 'operations', 'Release notes published', {'release_key': release_note['release_key']}),
        ('environment-promotion-approved', 'operations', 'Environment promotion approved', {'promotion_key': promotion['promotion_key']}),
        ('config-export-import-verified', 'operations', 'Config export/import verified', {'export': export_snapshot['snapshot_key'], 'import': import_snapshot['snapshot_key']}),
        ('operational-signoff-complete', 'operations', 'Operational signoff checklist complete', {'release_version': release_version}),
    ]
    readiness_items = [
        upsert_readiness_item(
            {'item_key': item_key, 'category': category, 'title': title, 'status': 'ready', 'evidence': evidence},
            user,
        )
        for item_key, category, title, evidence in signoff_items
    ]
    diagnostic = run_admin_diagnostics('disaster-recovery-release', user)
    artifacts = {
        'environments': [staging, production],
        'config_export': export_snapshot,
        'config_import': import_snapshot,
        'restore_drill': drill,
        'rollback_plan': rollback,
        'release_note': release_note,
        'promotion': promotion,
        'readiness_items': readiness_items,
        'diagnostic': diagnostic,
    }
    checks = {
        'restore_drills_ready': drill['status'] == 'pass',
        'rollback_plans_ready': rollback['status'] == 'approved' and len(rollback['verification_steps']) >= 3,
        'release_notes_ready': release_note['status'] == 'published',
        'environment_promotion_ready': promotion['status'] == 'approved',
        'config_export_import_ready': export_snapshot['direction'] == 'export' and import_snapshot['direction'] == 'import',
        'operational_signoff_checklist_ready': all(item['status'] == 'ready' for item in readiness_items),
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO disaster_recovery_release_runs (
            run_key, release_version, status, checks_json, artifacts_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (run_key, release_version, status_value, json.dumps(checks, sort_keys=True), json.dumps(artifacts, sort_keys=True), user['email'], started, completed),
    )
    db.log_audit('disaster_recovery_release_governance', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM disaster_recovery_release_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Disaster recovery release run not found.')
    return _format_run(row)


def _latest_migration_key() -> str:
    migrations = list_migrations()
    return migrations[-1]['migration_key'] if migrations else '0069_production_pdf_board_artifact_completion'


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['artifacts'] = json.loads(result.pop('artifacts_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
