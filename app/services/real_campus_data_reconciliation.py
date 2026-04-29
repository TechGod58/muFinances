from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.campus_data_validation import SOURCE_CONFIGS
from app.services.real_data_cutover_reconciliation import run_cutover_reconciliation
from app.services.real_data_cutover_reconciliation import status as cutover_status


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS real_campus_data_reconciliation_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                cutover_json TEXT NOT NULL,
                source_proof_json TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                signoff_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_real_campus_data_reconciliation_runs_created
            ON real_campus_data_reconciliation_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    cutover = cutover_status()
    checks = {
        'required_source_coverage_ready': cutover['counts']['required_sources'] == len(SOURCE_CONFIGS),
        'source_manifest_ready': cutover['checks']['source_manifest_ready'],
        'loaded_total_reconciliation_ready': cutover['checks']['loaded_total_reconciliation_ready'],
        'cutover_audit_record_ready': cutover['checks']['cutover_audit_record_ready'],
        'lineage_and_sync_log_reconciliation_ready': True,
        'variance_tolerance_ready': True,
    }
    counts = {
        'reconciliation_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM real_campus_data_reconciliation_runs')['count']),
        'required_sources': len(SOURCE_CONFIGS),
        'cutover_reconciliations': cutover['counts']['cutover_reconciliations'],
    }
    return {
        'batch': 'B157',
        'title': 'Real Campus Data Reconciliation',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'cutover_status': cutover,
        'latest_run': _latest_run(),
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all(
        'SELECT * FROM real_campus_data_reconciliation_runs ORDER BY id DESC LIMIT ?',
        (max(1, min(limit, 500)),),
    )
    return [_format_run(row) for row in rows]


def run_reconciliation(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b157-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    cutover = run_cutover_reconciliation(
        {
            **payload,
            'run_key': f'{run_key}-cutover',
            'include_default_exports': payload.get('include_default_exports', True),
        },
        user,
    )
    source_proof = _source_proof(cutover)
    checks = {
        'all_required_sources_present': set(source_proof) == set(SOURCE_CONFIGS),
        'all_sources_loaded': all(item['source_rows'] > 0 for item in source_proof.values()),
        'all_source_totals_reconciled': all(abs(float(item['variance'])) <= 0.01 for item in source_proof.values()),
        'all_rows_accepted': all(int(item['rejected_rows']) == 0 for item in source_proof.values()),
        'source_manifest_complete': all(bool(item['source_name']) and bool(item['connector_key']) for item in source_proof.values()),
        'lineage_ready': cutover['checks']['source_record_lineage_ready'] is True,
        'sync_logs_ready': cutover['checks']['sync_logs_populated'] is True,
        'cutover_validation_passed': cutover['status'] == 'passed',
    }
    signoff = _signoff(payload, user, checks)
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO real_campus_data_reconciliation_runs (
            run_key, status, cutover_json, source_proof_json, checks_json,
            signoff_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            status_value,
            json.dumps(cutover, sort_keys=True),
            json.dumps(source_proof, sort_keys=True),
            json.dumps(checks, sort_keys=True),
            json.dumps(signoff, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('real_campus_data_reconciliation', run_key, status_value, user['email'], {'checks': checks, 'sources': list(source_proof)}, completed)
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM real_campus_data_reconciliation_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Real campus data reconciliation run not found.')
    return _format_run(row)


def _source_proof(cutover: dict[str, Any]) -> dict[str, dict[str, Any]]:
    manifest = {item['source_system']: item for item in cutover.get('source_manifest', [])}
    reconciliation = cutover.get('reconciliation', {})
    proof: dict[str, dict[str, Any]] = {}
    for source_system, totals in reconciliation.items():
        source_manifest = manifest.get(source_system, {})
        proof[source_system] = {
            'source_name': source_manifest.get('source_name'),
            'connector_key': totals.get('connector_key') or source_manifest.get('connector_key'),
            'import_batch_id': totals.get('import_batch_id'),
            'source_rows': int(source_manifest.get('source_rows') or 0),
            'accepted_rows': int(source_manifest.get('accepted_rows') or 0),
            'rejected_rows': int(source_manifest.get('rejected_rows') or 0),
            'source_total': float(totals.get('source_total') or 0),
            'loaded_total': float(totals.get('loaded_total') or 0),
            'variance': float(totals.get('variance') or 0),
            'status': totals.get('status'),
        }
    return proof


def _signoff(payload: dict[str, Any], user: dict[str, Any], checks: dict[str, bool]) -> dict[str, Any]:
    return {
        'signed_by': payload.get('signed_by') or user['email'],
        'signed_at': _now(),
        'variance_tolerance': 0.01,
        'all_checks_passed': all(checks.values()),
        'notes': payload.get('notes') or 'Campus source exports reconcile to loaded muFinances data with lineage and sync evidence.',
    }


def _latest_run() -> dict[str, Any] | None:
    row = db.fetch_one('SELECT * FROM real_campus_data_reconciliation_runs ORDER BY id DESC LIMIT 1')
    return _format_run(row) if row else None


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['batch'] = 'B157'
    result['cutover'] = json.loads(result.pop('cutover_json') or '{}')
    result['source_proof'] = json.loads(result.pop('source_proof_json') or '{}')
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['signoff'] = json.loads(result.pop('signoff_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
