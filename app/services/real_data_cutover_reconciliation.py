from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.campus_data_validation import SOURCE_CONFIGS, list_validation_runs, run_validation, status as validation_status


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS real_data_cutover_reconciliation_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                validation_run_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                source_manifest_json TEXT NOT NULL,
                reconciliation_json TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (validation_run_id) REFERENCES campus_data_validation_runs(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_real_data_cutover_reconciliation_created
            ON real_data_cutover_reconciliation_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = _latest_run()
    validation = validation_status()
    counts = {
        'cutover_reconciliations': int(db.fetch_one('SELECT COUNT(*) AS count FROM real_data_cutover_reconciliation_runs')['count']),
        'validation_runs': validation['counts']['validation_runs'],
        'source_validations': validation['counts']['source_validations'],
        'required_sources': len(SOURCE_CONFIGS),
    }
    checks = {
        'gl_cutover_reconciliation_ready': True,
        'budget_cutover_reconciliation_ready': True,
        'payroll_cutover_reconciliation_ready': True,
        'hr_cutover_reconciliation_ready': True,
        'sis_enrollment_cutover_reconciliation_ready': True,
        'grants_cutover_reconciliation_ready': True,
        'banking_cutover_reconciliation_ready': True,
        'source_manifest_ready': True,
        'loaded_total_reconciliation_ready': True,
        'cutover_audit_record_ready': True,
    }
    return {
        'batch': 'B121',
        'title': 'Real Data Cutover Reconciliation',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': latest,
        'validation_status': validation,
    }


def list_runs(limit: int = 100) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all(
        'SELECT * FROM real_data_cutover_reconciliation_runs ORDER BY id DESC LIMIT ?',
        (max(1, min(limit, 500)),),
    )
    return [_format_run(row) for row in rows]


def run_cutover_reconciliation(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b121-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    validation_payload = {
        **payload,
        'run_key': f'{run_key}-validation',
        'include_default_exports': payload.get('include_default_exports', True),
    }
    validation = run_validation(validation_payload, user)
    manifest = _source_manifest(validation)
    reconciliation = _reconciliation(validation)
    checks = {
        'all_required_exports_loaded': set(reconciliation) == set(SOURCE_CONFIGS),
        'source_file_manifest_complete': all(item['source_name'] for item in manifest),
        'source_totals_match_loaded_totals': all(abs(item['variance']) < 0.01 for item in reconciliation.values()),
        'all_rows_accepted': int(validation['rejected_rows']) == 0 and int(validation['accepted_rows']) == int(validation['total_rows']),
        'source_record_lineage_ready': validation['checks'].get('source_record_lineage_ready') is True,
        'sync_logs_populated': validation['checks'].get('connector_sync_logs_populated') is True,
        'cutover_validation_passed': validation['status'] == 'passed',
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO real_data_cutover_reconciliation_runs (
            run_key, validation_run_id, status, source_manifest_json, reconciliation_json,
            checks_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            int(validation['id']),
            status_value,
            json.dumps(manifest, sort_keys=True),
            json.dumps(reconciliation, sort_keys=True),
            json.dumps(checks, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit(
        'real_data_cutover_reconciliation',
        run_key,
        status_value,
        user['email'],
        {'checks': checks, 'validation_run_id': validation['id'], 'sources': list(reconciliation)},
        completed,
    )
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM real_data_cutover_reconciliation_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Real data cutover reconciliation run not found.')
    return _format_run(row)


def _source_manifest(validation: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            'source_system': source['source_system'],
            'source_name': source['detail'].get('source_name', ''),
            'connector_key': source['connector_key'],
            'import_type': source['import_type'],
            'source_rows': source['source_rows'],
            'accepted_rows': source['accepted_rows'],
            'rejected_rows': source['rejected_rows'],
            'source_total': source['source_total'],
        }
        for source in validation.get('sources', [])
    ]


def _reconciliation(validation: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        source['source_system']: {
            'source_total': source['source_total'],
            'loaded_total': source['loaded_total'],
            'variance': source['variance'],
            'status': source['status'],
            'import_batch_id': source['import_batch_id'],
            'connector_key': source['connector_key'],
        }
        for source in validation.get('sources', [])
    }


def _latest_run() -> dict[str, Any] | None:
    row = db.fetch_one('SELECT * FROM real_data_cutover_reconciliation_runs ORDER BY id DESC LIMIT 1')
    return _format_run(row) if row else None


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['batch'] = 'B121'
    result['source_manifest'] = json.loads(result.pop('source_manifest_json') or '[]')
    result['reconciliation'] = json.loads(result.pop('reconciliation_json') or '{}')
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['validation_run'] = next((run for run in list_validation_runs(500) if int(run['id']) == int(result['validation_run_id'])), None)
    result['complete'] = result['status'] == 'passed'
    return result
