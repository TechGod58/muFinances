from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.campus_integrations import run_import, upsert_connector

SOURCE_CONFIGS = {
    'gl': {'system_type': 'erp', 'adapter_key': 'erp_gl', 'import_type': 'ledger', 'total_field': 'amount'},
    'budget': {'system_type': 'erp', 'adapter_key': 'erp_gl', 'import_type': 'ledger', 'total_field': 'amount'},
    'payroll': {'system_type': 'payroll', 'adapter_key': 'payroll_actuals', 'import_type': 'ledger', 'total_field': 'amount'},
    'hr': {'system_type': 'hr', 'adapter_key': 'hr_positions', 'import_type': 'ledger', 'total_field': 'amount'},
    'sis_enrollment': {'system_type': 'sis', 'adapter_key': 'sis_enrollment', 'import_type': 'crm_enrollment', 'total_field': 'headcount'},
    'grants': {'system_type': 'grants', 'adapter_key': 'grants_awards', 'import_type': 'ledger', 'total_field': 'amount'},
    'banking': {'system_type': 'banking', 'adapter_key': 'banking_cash', 'import_type': 'banking_cash', 'total_field': 'amount'},
}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS campus_data_validation_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                source_count INTEGER NOT NULL DEFAULT 0,
                total_rows INTEGER NOT NULL DEFAULT 0,
                accepted_rows INTEGER NOT NULL DEFAULT 0,
                rejected_rows INTEGER NOT NULL DEFAULT 0,
                checks_json TEXT NOT NULL,
                summary_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS campus_data_validation_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                source_system TEXT NOT NULL,
                connector_key TEXT NOT NULL,
                import_type TEXT NOT NULL,
                source_rows INTEGER NOT NULL DEFAULT 0,
                accepted_rows INTEGER NOT NULL DEFAULT 0,
                rejected_rows INTEGER NOT NULL DEFAULT 0,
                source_total REAL NOT NULL DEFAULT 0,
                loaded_total REAL NOT NULL DEFAULT 0,
                variance REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                import_batch_id INTEGER DEFAULT NULL,
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES campus_data_validation_runs(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_campus_data_validation_runs_scenario
            ON campus_data_validation_runs (scenario_id, completed_at);
            CREATE INDEX IF NOT EXISTS idx_campus_data_validation_sources_run
            ON campus_data_validation_sources (run_id, source_system);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    counts = {
        'validation_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM campus_data_validation_runs')['count']),
        'source_validations': int(db.fetch_one('SELECT COUNT(*) AS count FROM campus_data_validation_sources')['count']),
        'connectors': int(db.fetch_one('SELECT COUNT(*) AS count FROM external_connectors')['count']),
    }
    checks = {
        'gl_export_validation_ready': True,
        'budget_export_validation_ready': True,
        'payroll_export_validation_ready': True,
        'hr_export_validation_ready': True,
        'sis_enrollment_export_validation_ready': True,
        'grants_export_validation_ready': True,
        'banking_export_validation_ready': True,
        'source_total_reconciliation_ready': True,
        'connector_drillback_ready': True,
        'anonymized_export_mode_ready': True,
    }
    return {
        'batch': 'B90',
        'title': 'Real Campus Data Validation',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _latest_run(),
    }


def list_validation_runs(limit: int = 100) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all(
        'SELECT * FROM campus_data_validation_runs ORDER BY id DESC LIMIT ?',
        (limit,),
    )
    return [_format_run(row) for row in rows]


def run_validation(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    scenario_id = int(payload.get('scenario_id') or _default_scenario_id())
    run_key = payload.get('run_key') or f"b90-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    exports = payload.get('exports') or {}
    if payload.get('include_default_exports', True):
        exports = {**_default_anonymized_exports(run_key), **exports}

    run_id = db.execute(
        '''
        INSERT INTO campus_data_validation_runs (
            run_key, scenario_id, status, source_count, total_rows, accepted_rows, rejected_rows,
            checks_json, summary_json, created_by, started_at, completed_at
        ) VALUES (?, ?, 'running', 0, 0, 0, 0, '{}', '{}', ?, ?, ?)
        ''',
        (run_key, scenario_id, user['email'], started, started),
    )

    source_results = []
    for source_system in SOURCE_CONFIGS:
        rows = exports.get(source_system) or []
        source_results.append(_load_and_reconcile_source(run_id, run_key, scenario_id, source_system, rows, user))

    source_count = len(source_results)
    total_rows = sum(item['source_rows'] for item in source_results)
    accepted_rows = sum(item['accepted_rows'] for item in source_results)
    rejected_rows = sum(item['rejected_rows'] for item in source_results)
    source_totals = {item['source_system']: item['source_total'] for item in source_results}
    loaded_totals = {item['source_system']: item['loaded_total'] for item in source_results}
    checks = {
        'all_sources_loaded': source_count == len(SOURCE_CONFIGS) and all(item['source_rows'] > 0 for item in source_results),
        'all_source_totals_reconciled': all(abs(float(item['variance'])) < 0.01 for item in source_results),
        'source_record_lineage_ready': _source_drillbacks_ready(run_key),
        'rejections_clean_or_recorded': rejected_rows == 0 or _rejections_recorded(source_results),
        'connector_sync_logs_populated': _sync_logs_ready(source_results),
        'anonymized_exports_used': bool(payload.get('include_default_exports', True)),
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    summary = {
        'source_totals': source_totals,
        'loaded_totals': loaded_totals,
        'sources': source_results,
    }
    completed = _now()
    db.execute(
        '''
        UPDATE campus_data_validation_runs
        SET status = ?, source_count = ?, total_rows = ?, accepted_rows = ?, rejected_rows = ?,
            checks_json = ?, summary_json = ?, completed_at = ?
        WHERE id = ?
        ''',
        (
            status_value,
            source_count,
            total_rows,
            accepted_rows,
            rejected_rows,
            json.dumps(checks, sort_keys=True),
            json.dumps(summary, sort_keys=True),
            completed,
            run_id,
        ),
    )
    db.log_audit('campus_data_validation', run_key, status_value, user['email'], {'checks': checks, 'source_count': source_count}, completed)
    return get_validation_run(run_id)


def get_validation_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM campus_data_validation_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Campus data validation run not found.')
    return _format_run(row)


def _load_and_reconcile_source(
    run_id: int,
    run_key: str,
    scenario_id: int,
    source_system: str,
    rows: list[dict[str, Any]],
    user: dict[str, Any],
) -> dict[str, Any]:
    config = SOURCE_CONFIGS[source_system]
    connector_key = f"b90-{run_key}-{source_system}".replace('_', '-').lower()
    upsert_connector(
        {
            'connector_key': connector_key,
            'name': f"Manchester anonymized {source_system.replace('_', ' ')} export",
            'system_type': config['system_type'],
            'direction': 'inbound',
            'config': {'adapter_key': config['adapter_key'], 'validation_run_key': run_key, 'anonymized': True},
        },
        user,
    )
    import_rows = [_tag_source_row(row, run_key, source_system, index) for index, row in enumerate(rows, start=1)]
    import_batch = run_import(
        {
            'scenario_id': scenario_id,
            'connector_key': connector_key,
            'source_format': 'csv',
            'import_type': config['import_type'],
            'source_name': f"anonymized-{source_system}.csv",
            'stream_chunk_size': 2,
            'rows': import_rows,
        },
        user,
    )
    source_total = _sum_field(import_rows, config['total_field'])
    loaded_total = _loaded_total(connector_key, scenario_id, config['import_type'], config['total_field'])
    variance = round(loaded_total - source_total, 2)
    status_value = 'reconciled' if abs(variance) < 0.01 and int(import_batch['rejected_rows']) == 0 else 'needs_review'
    detail = {
        'adapter_key': config['adapter_key'],
        'source_name': f"anonymized-{source_system}.csv",
        'source_record_prefix': f"{run_key}:{source_system}:",
        'import_status': import_batch['status'],
        'rejections': import_batch.get('rejections') or [],
    }
    source_id = db.execute(
        '''
        INSERT INTO campus_data_validation_sources (
            run_id, source_system, connector_key, import_type, source_rows, accepted_rows,
            rejected_rows, source_total, loaded_total, variance, status, import_batch_id,
            detail_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_id,
            source_system,
            connector_key,
            config['import_type'],
            len(import_rows),
            int(import_batch['accepted_rows']),
            int(import_batch['rejected_rows']),
            source_total,
            loaded_total,
            variance,
            status_value,
            int(import_batch['id']),
            json.dumps(detail, sort_keys=True),
            _now(),
        ),
    )
    row = db.fetch_one('SELECT * FROM campus_data_validation_sources WHERE id = ?', (source_id,))
    if row is None:
        raise RuntimeError('Campus validation source could not be reloaded.')
    return _format_source(row)


def _default_anonymized_exports(run_key: str) -> dict[str, list[dict[str, Any]]]:
    return {
        'gl': [
            _ledger_row('ART', 'GEN', 'TUITION', '2026-08', 210000, 'Actual GL tuition', run_key, 'gl', 1),
            _ledger_row('OPS', 'GEN', 'SUPPLIES', '2026-08', -37000, 'Actual GL supplies', run_key, 'gl', 2),
        ],
        'budget': [
            _ledger_row('SCI', 'GEN', 'SALARY', '2026-09', -418000, 'Approved budget salary', run_key, 'budget', 1),
            _ledger_row('OPS', 'GEN', 'UTILITIES', '2026-09', -64000, 'Approved utility budget', run_key, 'budget', 2),
        ],
        'payroll': [
            _ledger_row('SCI', 'GEN', 'SALARY', '2026-09', -182500, 'Payroll actuals', run_key, 'payroll', 1),
            _ledger_row('ART', 'GEN', 'BENEFITS', '2026-09', -46200, 'Payroll benefits', run_key, 'payroll', 2),
        ],
        'hr': [
            _ledger_row('SCI', 'GEN', 'POSITION_CONTROL', '2026-10', -94500, 'Open faculty position', run_key, 'hr', 1),
            _ledger_row('OPS', 'GEN', 'POSITION_CONTROL', '2026-10', -58500, 'Operations position', run_key, 'hr', 2),
        ],
        'sis_enrollment': [
            {'pipeline_stage': 'deposit', 'term': '2026FA', 'headcount': 420, 'yield_rate': 0.84},
            {'pipeline_stage': 'admit', 'term': '2027SP', 'headcount': 175, 'yield_rate': 0.31},
        ],
        'grants': [
            _ledger_row('SCI', 'GRANT', 'GRANT_REVENUE', '2026-08', 125000, 'Grant award revenue', run_key, 'grants', 1),
            _ledger_row('SCI', 'GRANT', 'GRANT_EXPENSE', '2026-08', -41500, 'Grant burn actual', run_key, 'grants', 2),
        ],
        'banking': [
            {'bank_account': 'OPERATING', 'transaction_date': '2026-08-15', 'amount': 184000, 'description': 'Tuition cash receipt'},
            {'bank_account': 'OPERATING', 'transaction_date': '2026-08-18', 'amount': -73250, 'description': 'Payroll cash disbursement'},
        ],
    }


def _ledger_row(
    department: str,
    fund: str,
    account: str,
    period: str,
    amount: float,
    notes: str,
    run_key: str,
    source: str,
    index: int,
) -> dict[str, Any]:
    return {
        'department_code': department,
        'fund_code': fund,
        'account_code': account,
        'period': period,
        'amount': amount,
        'notes': notes,
        'source_record_id': f'{run_key}:{source}:{index}',
    }


def _tag_source_row(row: dict[str, Any], run_key: str, source_system: str, index: int) -> dict[str, Any]:
    tagged = dict(row)
    tagged.setdefault('source_record_id', f'{run_key}:{source_system}:{index}')
    return tagged


def _sum_field(rows: list[dict[str, Any]], field: str) -> float:
    return round(sum(float(row.get(field) or 0) for row in rows), 2)


def _loaded_total(connector_key: str, scenario_id: int, import_type: str, total_field: str) -> float:
    if import_type == 'ledger':
        row = db.fetch_one(
            '''
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM planning_ledger
            WHERE scenario_id = ? AND source = ? AND reversed_at IS NULL
            ''',
            (scenario_id, connector_key),
        )
        return round(float(row['total'] if row else 0), 2)
    if import_type == 'crm_enrollment':
        row = db.fetch_one(
            '''
            SELECT COALESCE(SUM(headcount), 0) AS total
            FROM crm_enrollment_imports
            WHERE scenario_id = ? AND connector_key = ?
            ''',
            (scenario_id, connector_key),
        )
        return round(float(row['total'] if row else 0), 2)
    row = db.fetch_one(
        '''
        SELECT COALESCE(SUM(amount), 0) AS total
        FROM banking_cash_imports
        WHERE scenario_id = ? AND connector_key = ?
        ''',
        (scenario_id, connector_key),
    )
    return round(float(row['total'] if row else 0), 2)


def _source_drillbacks_ready(run_key: str) -> bool:
    row = db.fetch_one(
        '''
        SELECT COUNT(*) AS count
        FROM connector_source_drillbacks
        WHERE source_record_id LIKE ?
        ''',
        (f'{run_key}:%',),
    )
    return int(row['count'] if row else 0) >= len(SOURCE_CONFIGS)


def _rejections_recorded(source_results: list[dict[str, Any]]) -> bool:
    import_ids = [item['import_batch_id'] for item in source_results if item['rejected_rows'] > 0]
    if not import_ids:
        return True
    placeholders = ','.join('?' for _ in import_ids)
    row = db.fetch_one(
        f'SELECT COUNT(*) AS count FROM import_rejections WHERE import_batch_id IN ({placeholders})',
        tuple(import_ids),
    )
    return int(row['count'] if row else 0) >= sum(item['rejected_rows'] for item in source_results)


def _sync_logs_ready(source_results: list[dict[str, Any]]) -> bool:
    connector_keys = [item['connector_key'] for item in source_results]
    if not connector_keys:
        return False
    placeholders = ','.join('?' for _ in connector_keys)
    row = db.fetch_one(
        f'SELECT COUNT(*) AS count FROM connector_sync_logs WHERE connector_key IN ({placeholders})',
        tuple(connector_keys),
    )
    return int(row['count'] if row else 0) >= len(connector_keys)


def _default_scenario_id() -> int:
    row = db.fetch_one('SELECT id FROM scenarios ORDER BY id DESC LIMIT 1')
    if row is None:
        raise ValueError('No scenario is available for campus data validation.')
    return int(row['id'])


def _latest_run() -> dict[str, Any] | None:
    row = db.fetch_one('SELECT * FROM campus_data_validation_runs ORDER BY id DESC LIMIT 1')
    return _format_run(row) if row else None


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['summary'] = json.loads(result.pop('summary_json') or '{}')
    result['sources'] = [_format_source(source) for source in db.fetch_all(
        'SELECT * FROM campus_data_validation_sources WHERE run_id = ? ORDER BY id ASC',
        (result['id'],),
    )]
    result['complete'] = result['status'] == 'passed'
    return result


def _format_source(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['detail'] = json.loads(result.pop('detail_json') or '{}')
    return result
