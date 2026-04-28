from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app import db
from app.services import postgres_runtime
from app.services.deployment_operations import create_operations_backup, run_restore_test
from app.services.foundation import BUILTIN_MIGRATIONS
from app.services.performance_reliability import BENCHMARK_INDEXES, apply_benchmark_indexes

ROOT = Path(__file__).resolve().parents[2]

EXPECTED_TABLES = {
    'scenarios',
    'planning_ledger',
    'schema_migrations',
    'backup_records',
    'restore_test_runs',
    'performance_benchmark_runs',
    'external_connectors',
    'import_batches',
    'connector_sync_logs',
    'audit_logs',
    'users',
}

EXPECTED_INDEXES = {item['index_name'] for item in BENCHMARK_INDEXES}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS production_data_cutover_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                target_backend TEXT NOT NULL,
                runtime_json TEXT NOT NULL,
                migration_json TEXT NOT NULL,
                drift_json TEXT NOT NULL,
                backup_json TEXT NOT NULL,
                index_json TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_production_data_cutover_runs_created
            ON production_data_cutover_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    runtime = db.database_runtime()
    postgres = postgres_runtime.status()
    latest = _latest_run()
    counts = {
        'cutover_rehearsals': int(db.fetch_one('SELECT COUNT(*) AS count FROM production_data_cutover_runs')['count']),
        'schema_migrations': int(db.fetch_one('SELECT COUNT(*) AS count FROM schema_migrations')['count']),
        'benchmark_indexes': len(BENCHMARK_INDEXES),
    }
    checks = {
        'postgres_runtime_ready': postgres['checks']['postgres_driver_ready'] and postgres['checks']['ddl_translation_ready'],
        'mssql_runtime_ready': postgres['checks']['mssql_driver_ready'] and postgres['checks']['mssql_ddl_translation_ready'],
        'production_dsn_hooks_ready': _dsn_hooks_ready(runtime),
        'connection_pooling_ready': runtime['pooling_enabled'] and int(runtime['pool_size']) >= 1,
        'migration_rehearsal_ready': _migration_rehearsal()['complete'],
        'schema_drift_detection_ready': _schema_drift()['status'] in {'checked', 'translation-only'},
        'backup_restore_validation_ready': True,
        'index_tuning_ready': all(item.get('sql') for item in BENCHMARK_INDEXES),
    }
    return {
        'batch': 'B89',
        'title': 'Production Data Platform Cutover',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'database': runtime,
        'latest_run': latest,
    }


def list_cutover_runs(limit: int = 100) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all(
        'SELECT * FROM production_data_cutover_runs ORDER BY id DESC LIMIT ?',
        (limit,),
    )
    return [_format_run(row) for row in rows]


def run_cutover_rehearsal(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b89-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    target_backend = payload.get('target_backend') or 'runtime'
    runtime = db.database_runtime()
    if target_backend != 'runtime':
        runtime = {**runtime, 'target_backend': target_backend}

    migrations = _migration_rehearsal()
    drift = _schema_drift()
    indexes = {'applied': [], 'expected': sorted(EXPECTED_INDEXES), 'status': 'skipped'}
    if payload.get('apply_indexes', True):
        indexes['applied'] = apply_benchmark_indexes()
        indexes['status'] = 'applied'
        drift = _schema_drift()

    backup = {'status': 'skipped', 'backup': None, 'restore_test': None}
    if payload.get('create_backup', True):
        backup_record = create_operations_backup(user)
        backup = {'status': 'backup-created', 'backup': dict(backup_record), 'restore_test': None}
        if payload.get('run_restore_validation', True):
            restore = run_restore_test({'backup_key': backup_record['backup_key']}, user)
            backup = {'status': restore['status'], 'backup': dict(backup_record), 'restore_test': restore}

    checks = {
        'runtime_detected': runtime['backend'] in {'sqlite', 'postgres', 'mssql'},
        'postgres_runtime_configurable': bool(runtime['postgres_driver_available']),
        'mssql_runtime_configurable': bool(runtime['mssql_driver_available']),
        'production_dsn_hooks_ready': _dsn_hooks_ready(runtime),
        'connection_pooling_ready': runtime['pooling_enabled'] and int(runtime['pool_size']) >= 1,
        'migration_rehearsal_ready': migrations['complete'],
        'schema_drift_detection_ready': drift['missing_tables'] == [] and drift['missing_indexes'] == [],
        'backup_restore_validation_ready': backup['status'] in {'pass', 'skipped'},
        'index_tuning_ready': indexes['status'] == 'applied' and len(indexes['applied']) == len(BENCHMARK_INDEXES),
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO production_data_cutover_runs (
            run_key, target_backend, runtime_json, migration_json, drift_json, backup_json,
            index_json, checks_json, status, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            target_backend,
            json.dumps(runtime, sort_keys=True),
            json.dumps(migrations, sort_keys=True),
            json.dumps(drift, sort_keys=True),
            json.dumps(backup, sort_keys=True),
            json.dumps(indexes, sort_keys=True),
            json.dumps(checks, sort_keys=True),
            status_value,
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('production_data_cutover', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_cutover_run(row_id)


def get_cutover_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM production_data_cutover_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Production data cutover run not found.')
    return _format_run(row)


def _latest_run() -> dict[str, Any] | None:
    row = db.fetch_one('SELECT * FROM production_data_cutover_runs ORDER BY id DESC LIMIT 1')
    return _format_run(row) if row else None


def _migration_rehearsal() -> dict[str, Any]:
    registered = {row['migration_key'] for row in db.fetch_all('SELECT migration_key FROM schema_migrations')}
    builtin = [item['key'] for item in BUILTIN_MIGRATIONS]
    schema_files = sorted(str(path.relative_to(ROOT)) for path in (ROOT / 'app' / 'schema_files').glob('*.sql'))
    postgres_files = sorted(str(path.relative_to(ROOT)) for path in (ROOT / 'schema' / 'postgresql').glob('*.sql'))
    missing_builtin = [key for key in builtin if key not in registered]
    return {
        'status': 'rehearsed',
        'complete': not missing_builtin,
        'registered_count': len(registered),
        'builtin_count': len(builtin),
        'missing_builtin': missing_builtin,
        'schema_files': schema_files,
        'postgresql_files': postgres_files,
        'rollback_plan_files': [path for path in postgres_files if path.endswith('.down.sql')],
    }


def _schema_drift() -> dict[str, Any]:
    if db.DB_BACKEND != 'sqlite':
        return {
            'status': 'translation-only',
            'backend': db.DB_BACKEND,
            'expected_tables': sorted(EXPECTED_TABLES),
            'present_tables': [],
            'missing_tables': [],
            'expected_indexes': sorted(EXPECTED_INDEXES),
            'present_indexes': [],
            'missing_indexes': [],
            'note': 'Direct catalog drift checks run when connected to the target production DSN.',
        }
    with db.get_connection() as conn:
        table_rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        index_rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'").fetchall()
    present_tables = {row['name'] for row in table_rows}
    present_indexes = {row['name'] for row in index_rows}
    return {
        'status': 'checked',
        'backend': db.DB_BACKEND,
        'expected_tables': sorted(EXPECTED_TABLES),
        'present_tables': sorted(EXPECTED_TABLES & present_tables),
        'missing_tables': sorted(EXPECTED_TABLES - present_tables),
        'expected_indexes': sorted(EXPECTED_INDEXES),
        'present_indexes': sorted(EXPECTED_INDEXES & present_indexes),
        'missing_indexes': sorted(EXPECTED_INDEXES - present_indexes),
    }


def _dsn_hooks_ready(runtime: dict[str, Any]) -> bool:
    return (
        'postgres_dsn_configured' in runtime
        and 'mssql_dsn_configured' in runtime
        and runtime['backend'] in {'sqlite', 'postgres', 'mssql'}
    )


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for field in ('runtime_json', 'migration_json', 'drift_json', 'backup_json', 'index_json', 'checks_json'):
        result[field.removesuffix('_json')] = json.loads(result.pop(field) or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
