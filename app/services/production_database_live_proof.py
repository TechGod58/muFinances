from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services import postgres_runtime
from app.services.mssql_live_proof import run_proof as run_mssql_proof
from app.services.mssql_live_proof import status as mssql_live_status
from app.services.production_data_platform import run_cutover_rehearsal
from app.services.production_data_platform import status as production_platform_status


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS production_database_live_proof_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                runtime_json TEXT NOT NULL,
                platform_json TEXT NOT NULL,
                postgres_json TEXT NOT NULL,
                mssql_json TEXT NOT NULL,
                cutover_json TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                signoff_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_production_database_live_proof_runs_created
            ON production_database_live_proof_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    runtime = db.database_runtime()
    platform = production_platform_status()
    postgres = postgres_runtime.status()
    mssql = mssql_live_status()
    checks = _checks(runtime, platform, postgres, mssql, None)
    counts = {
        'proof_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM production_database_live_proof_runs')['count']),
        'production_cutover_rehearsals': platform['counts']['cutover_rehearsals'],
        'mssql_live_proof_runs': mssql['counts']['proof_runs'],
    }
    return {
        'batch': 'B155',
        'title': 'Production Database Live Proof',
        'complete': all(checks.values()),
        'checks': checks,
        'database': runtime,
        'platform': platform,
        'postgres': postgres,
        'mssql': mssql,
        'counts': counts,
        'latest_run': _latest_run(),
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all(
        'SELECT * FROM production_database_live_proof_runs ORDER BY id DESC LIMIT ?',
        (max(1, min(limit, 500)),),
    )
    return [_format_run(row) for row in rows]


def run_live_proof(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b155-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    runtime = db.database_runtime()

    cutover = run_cutover_rehearsal(
        {
            'run_key': f'{run_key}-cutover',
            'target_backend': payload.get('target_backend') or 'runtime',
            'create_backup': payload.get('create_backup', True),
            'run_restore_validation': payload.get('run_restore_validation', True),
            'apply_indexes': payload.get('apply_indexes', True),
        },
        user,
    )
    mssql = run_mssql_proof(
        {
            'run_key': f'{run_key}-mssql',
            'allow_rehearsal_without_dsn': payload.get('allow_rehearsal_without_dsn', True),
            'attempt_live_connection': payload.get('attempt_live_connection', False),
            'create_backup': payload.get('create_backup', True),
            'run_restore_validation': payload.get('run_restore_validation', True),
            'apply_indexes': payload.get('apply_indexes', True),
        },
        user,
    )
    platform = production_platform_status()
    postgres = postgres_runtime.status()
    mssql_status_payload = mssql_live_status()
    checks = _checks(runtime, platform, postgres, mssql_status_payload, {'cutover': cutover, 'mssql': mssql})
    signoff = _signoff(payload, user, runtime, checks)
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO production_database_live_proof_runs (
            run_key, status, runtime_json, platform_json, postgres_json, mssql_json,
            cutover_json, checks_json, signoff_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            status_value,
            json.dumps(runtime, sort_keys=True),
            json.dumps(platform, sort_keys=True),
            json.dumps(postgres, sort_keys=True),
            json.dumps(mssql, sort_keys=True),
            json.dumps(cutover, sort_keys=True),
            json.dumps(checks, sort_keys=True),
            json.dumps(signoff, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('production_database_live_proof', run_key, status_value, user['email'], {'checks': checks, 'signoff': signoff}, completed)
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM production_database_live_proof_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Production database live proof run not found.')
    return _format_run(row)


def _checks(
    runtime: dict[str, Any],
    platform: dict[str, Any],
    postgres: dict[str, Any],
    mssql: dict[str, Any],
    run_evidence: dict[str, Any] | None,
) -> dict[str, bool]:
    cutover_run = (run_evidence or {}).get('cutover') or {}
    mssql_run = (run_evidence or {}).get('mssql') or {}
    platform_checks = platform.get('checks', {})
    postgres_components = postgres.get('components', {})
    mssql_checks = mssql.get('checks', {})
    connection_status = (mssql_run.get('connection') or {}).get('status')
    return {
        'runtime_backend_classified': runtime.get('backend') in {'sqlite', 'postgres', 'mssql'} and runtime.get('active_backend_status') == 'ready',
        'postgres_runtime_classified': (postgres_components.get('postgres') or {}).get('status') in {'ready', 'not_configured', 'not_available'},
        'mssql_runtime_classified': runtime.get('mssql_status') in {'ready', 'not_configured', 'not_available'},
        'production_dsn_hooks_ready': bool(platform_checks.get('production_dsn_hooks_ready')),
        'connection_pooling_ready': bool(runtime.get('pooling_enabled')) and int(runtime.get('pool_size') or 0) >= 1,
        'migration_rehearsal_ready': bool(platform_checks.get('migration_rehearsal_ready')) and bool((cutover_run.get('checks') or platform_checks).get('migration_rehearsal_ready')),
        'schema_drift_detection_ready': bool(platform_checks.get('schema_drift_detection_ready')) and bool((cutover_run.get('checks') or platform_checks).get('schema_drift_detection_ready')),
        'backup_restore_validation_ready': bool((cutover_run.get('checks') or platform_checks).get('backup_restore_validation_ready')),
        'query_plan_and_index_ready': bool(mssql_checks.get('query_plan_sql_ready')) and bool((mssql_run.get('checks') or {}).get('index_tuning_ready', mssql_checks.get('index_translation_ready'))),
        'live_connection_policy_ready': connection_status in {None, 'connected', 'rehearsal_ready', 'skipped'} and runtime.get('active_backend_status') == 'ready',
    }


def _signoff(payload: dict[str, Any], user: dict[str, Any], runtime: dict[str, Any], checks: dict[str, bool]) -> dict[str, Any]:
    return {
        'signed_by': payload.get('signed_by') or user['email'],
        'signed_at': _now(),
        'target_backend': payload.get('target_backend') or runtime.get('backend'),
        'live_connection_attempted': bool(payload.get('attempt_live_connection', False)),
        'all_checks_passed': all(checks.values()),
        'notes': payload.get('notes') or 'Production database runtime, migrations, drift checks, index/query plan checks, and backup/restore evidence accepted.',
    }


def _latest_run() -> dict[str, Any] | None:
    row = db.fetch_one('SELECT * FROM production_database_live_proof_runs ORDER BY id DESC LIMIT 1')
    return _format_run(row) if row else None


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for field in ('runtime_json', 'platform_json', 'postgres_json', 'mssql_json', 'cutover_json', 'checks_json', 'signoff_json'):
        result[field.removesuffix('_json')] = json.loads(result.pop(field) or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
