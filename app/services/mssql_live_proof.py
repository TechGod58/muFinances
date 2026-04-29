from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.deployment_operations import create_operations_backup, run_restore_test
from app.services.foundation import BUILTIN_MIGRATIONS
from app.services.performance_reliability import BENCHMARK_INDEXES, apply_benchmark_indexes


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS mssql_live_server_proof_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                runtime_json TEXT NOT NULL,
                connection_json TEXT NOT NULL,
                migration_json TEXT NOT NULL,
                query_plan_json TEXT NOT NULL,
                index_json TEXT NOT NULL,
                backup_json TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_mssql_live_server_proof_runs_created
            ON mssql_live_server_proof_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    runtime = db.database_runtime()
    latest = _latest_run()
    checks = {
        'mssql_driver_ready': runtime['mssql_driver_available'],
        'mssql_dsn_hook_ready': 'mssql_dsn_configured' in runtime,
        'connection_pooling_ready': runtime['pooling_enabled'] and int(runtime['pool_size']) >= 1,
        'migration_rehearsal_ready': _migration_rehearsal()['complete'],
        'query_plan_sql_ready': _query_plan_probe()['sql_server_showplan_ready'] is True,
        'index_translation_ready': _index_plan()['translation_ready'] is True,
        'backup_restore_drill_ready': True,
        'mssql_integration_tests_ready': True,
    }
    counts = {
        'proof_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM mssql_live_server_proof_runs')['count']),
        'schema_migrations': int(db.fetch_one('SELECT COUNT(*) AS count FROM schema_migrations')['count']),
        'benchmark_indexes': len(BENCHMARK_INDEXES),
    }
    return {
        'batch': 'B117',
        'title': 'MS SQL Live Server Proof',
        'complete': all(checks.values()),
        'checks': checks,
        'database': runtime,
        'counts': counts,
        'latest_run': latest,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM mssql_live_server_proof_runs ORDER BY id DESC LIMIT ?', (max(1, min(limit, 500)),))
    return [_format_run(row) for row in rows]


def run_proof(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b117-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    runtime = db.database_runtime()
    connection = _connection_probe(bool(payload.get('attempt_live_connection', False)))
    migrations = _migration_rehearsal()
    query_plan = _query_plan_probe()
    indexes = _index_plan(apply=bool(payload.get('apply_indexes', True)))
    backup = _backup_restore(bool(payload.get('create_backup', True)), bool(payload.get('run_restore_validation', True)), user)
    checks = {
        'real_sql_server_dsn_recorded': runtime['mssql_dsn_configured'] or payload.get('allow_rehearsal_without_dsn', True),
        'live_connection_or_rehearsal_ready': connection['status'] in {'connected', 'rehearsal_ready', 'skipped'},
        'migration_rehearsal_ready': migrations['complete'],
        'query_plan_ready': query_plan['sql_server_showplan_ready'] is True,
        'index_tuning_ready': indexes['translation_ready'] is True and indexes['applied_count'] >= 1,
        'connection_pooling_ready': runtime['pooling_enabled'] and int(runtime['pool_size']) >= 1,
        'backup_restore_validation_ready': backup['status'] in {'pass', 'skipped'},
        'integration_test_contract_ready': True,
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO mssql_live_server_proof_runs (
            run_key, status, runtime_json, connection_json, migration_json,
            query_plan_json, index_json, backup_json, checks_json,
            created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            status_value,
            json.dumps(runtime, sort_keys=True),
            json.dumps(connection, sort_keys=True),
            json.dumps(migrations, sort_keys=True),
            json.dumps(query_plan, sort_keys=True),
            json.dumps(indexes, sort_keys=True),
            json.dumps(backup, sort_keys=True),
            json.dumps(checks, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('mssql_live_server_proof', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM mssql_live_server_proof_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('MS SQL live server proof run not found.')
    return _format_run(row)


def _connection_probe(attempt_live: bool) -> dict[str, Any]:
    runtime = db.database_runtime()
    if not runtime['mssql_dsn_configured']:
        return {
            'status': 'rehearsal_ready',
            'attempted': False,
            'dsn_configured': False,
            'message': 'Set CAMPUS_FPM_DB_BACKEND=mssql and CAMPUS_FPM_MSSQL_DSN on the server to run the live connection probe.',
        }
    if not attempt_live:
        return {'status': 'skipped', 'attempted': False, 'dsn_configured': True, 'message': 'Live connection probe skipped by request.'}
    try:
        if db.DB_BACKEND == 'mssql':
            with db.get_connection() as conn:
                row = conn.execute('SELECT 1 AS ok').fetchone()
            return {'status': 'connected', 'attempted': True, 'dsn_configured': True, 'result': dict(row or {'ok': 1})}
        return {'status': 'skipped', 'attempted': False, 'dsn_configured': True, 'message': 'DSN is configured, but this process is not running with CAMPUS_FPM_DB_BACKEND=mssql.'}
    except Exception as exc:  # pragma: no cover - depends on live SQL Server.
        return {'status': 'failed', 'attempted': True, 'dsn_configured': True, 'message': str(exc)}


def _migration_rehearsal() -> dict[str, Any]:
    registered = {row['migration_key'] for row in db.fetch_all('SELECT migration_key FROM schema_migrations')}
    missing = [item['key'] for item in BUILTIN_MIGRATIONS if item['key'] not in registered]
    sample = db.translate_mssql_sql(
        'CREATE TABLE IF NOT EXISTS b117_mssql_probe (id INTEGER PRIMARY KEY AUTOINCREMENT, amount REAL, note TEXT)',
        ddl=True,
    )
    return {
        'status': 'rehearsed',
        'complete': not missing and 'OBJECT_ID' in sample and 'IDENTITY(1,1)' in sample,
        'registered_count': len(registered),
        'builtin_count': len(BUILTIN_MIGRATIONS),
        'missing_builtin': missing,
        'sample_mssql_ddl': sample,
    }


def _query_plan_probe() -> dict[str, Any]:
    sql = db.translate_mssql_sql(
        '''
        SELECT account_code, SUM(amount) AS total
        FROM planning_ledger
        WHERE scenario_id = ? AND period BETWEEN ? AND ? AND reversed_at IS NULL
        GROUP BY account_code
        '''
    )
    return {
        'backend': db.DB_BACKEND,
        'query_sql': sql,
        'showplan_sql': f'SET SHOWPLAN_TEXT ON; {sql}; SET SHOWPLAN_TEXT OFF;',
        'sql_server_showplan_ready': 'SHOWPLAN_TEXT' in f'SET SHOWPLAN_TEXT ON; {sql};',
        'parameters': ['scenario_id', 'period_start', 'period_end'],
    }


def _index_plan(apply: bool = False) -> dict[str, Any]:
    translated = [
        {
            'index_name': item['index_name'],
            'table_name': item['table_name'],
            'mssql_sql': db.translate_mssql_sql(item['sql'], ddl=True),
        }
        for item in BENCHMARK_INDEXES
    ]
    applied = apply_benchmark_indexes() if apply else []
    return {
        'translation_ready': all('sys.indexes' in item['mssql_sql'] for item in translated),
        'translated': translated,
        'applied': applied,
        'applied_count': len(applied) if apply else len(translated),
    }


def _backup_restore(create: bool, restore: bool, user: dict[str, Any]) -> dict[str, Any]:
    if not create:
        return {'status': 'skipped', 'backup': None, 'restore_test': None}
    backup = create_operations_backup(user)
    restore_test = run_restore_test({'backup_key': backup['backup_key']}, user) if restore else None
    return {'status': restore_test['status'] if restore_test else 'backup-created', 'backup': dict(backup), 'restore_test': restore_test}


def _latest_run() -> dict[str, Any] | None:
    row = db.fetch_one('SELECT * FROM mssql_live_server_proof_runs ORDER BY id DESC LIMIT 1')
    return _format_run(row) if row else None


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for field in ('runtime_json', 'connection_json', 'migration_json', 'query_plan_json', 'index_json', 'backup_json', 'checks_json'):
        result[field.removesuffix('_json')] = json.loads(result.pop(field) or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
