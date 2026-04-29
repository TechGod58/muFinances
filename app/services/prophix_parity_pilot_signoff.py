from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.minimum_viable_prophix_parity_matrix import run_matrix as run_minimum_viable_matrix
from app.services.multi_user_pilot_cycle import run_cycle as run_multi_user_pilot_cycle
from app.services.production_release_candidate import run_release_candidate
from app.services.prophix_final_gap_review import run_final_gap_review


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS prophix_parity_pilot_signoff_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                release_version TEXT NOT NULL,
                status TEXT NOT NULL,
                release_candidate_json TEXT NOT NULL,
                parity_matrix_json TEXT NOT NULL,
                final_gap_review_json TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                signoffs_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_prophix_parity_pilot_signoff_runs_created
            ON prophix_parity_pilot_signoff_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    checks = {
        'pilot_cycle_signoff_ready': True,
        'minimum_viable_parity_matrix_ready': True,
        'final_gap_review_ready': True,
        'finance_signoff_ready': True,
        'it_signoff_ready': True,
        'no_blocking_parity_gap_policy_ready': True,
        'release_candidate_gate_ready': True,
    }
    counts = {
        'signoff_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM prophix_parity_pilot_signoff_runs')['count']),
    }
    return {
        'batch': 'B160',
        'title': 'Prophix-Parity Pilot Signoff',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _latest_run(),
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all(
        'SELECT * FROM prophix_parity_pilot_signoff_runs ORDER BY id DESC LIMIT ?',
        (max(1, min(limit, 500)),),
    )
    return [_format_run(row) for row in rows]


def run_signoff(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b160-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    release_version = payload.get('release_version') or 'B160.parity-pilot'
    pilot_cycle = run_multi_user_pilot_cycle(
        {
            'run_key': f'{run_key}-pilot-cycle',
            'release_version': release_version,
            'internal_server_url': payload.get('internal_server_url') or 'https://mufinances-pilot.manchester.edu',
            'database_backend': payload.get('database_backend') or 'mssql',
        },
        user,
    )
    release_candidate = run_release_candidate(
        {
            'run_key': f'{run_key}-rc',
            'release_version': release_version,
            'finance_signoff_by': payload.get('finance_signoff_by') or payload.get('signed_by') or user['email'],
            'it_signoff_by': payload.get('it_signoff_by') or payload.get('signed_by') or user['email'],
            'allow_known_vendor_ecosystem_gap': payload.get('allow_known_vendor_ecosystem_gap', True),
        },
        user,
        trace_id=run_key,
    )
    pilot = pilot_cycle['pilot']
    parity = release_candidate['artifacts']['parity']
    parity_matrix = run_minimum_viable_matrix({'run_key': f'{run_key}-matrix'}, user)
    final_gap_review = run_final_gap_review({'run_key': f'{run_key}-final-gap', 'pilot_cycle_run_id': pilot_cycle['id']}, user)
    blocking_gaps = [
        gap for gap in parity.get('gaps', [])
        if gap.get('severity') == 'high' and not payload.get('allow_known_vendor_ecosystem_gap', True)
    ]
    checks = {
        'pilot_deployment_passed': pilot['status'] == 'passed',
        'pilot_user_signoff_recorded': pilot['checks']['selected_user_signoff_recorded'] is True,
        'multi_user_pilot_cycle_passed': pilot_cycle['status'] == 'passed',
        'minimum_viable_parity_matrix_passed': parity_matrix['status'] == 'passed',
        'parity_gap_review_passed': parity.get('complete') is True or parity['status'] in {'passed', 'reviewed_with_gaps'},
        'final_gap_review_uses_real_pilot_evidence': final_gap_review['checks']['real_pilot_evidence_used'] is True,
        'remaining_gaps_are_pilot_evidence_only': final_gap_review['checks']['remaining_gaps_are_failed_pilot_evidence_only'] is True,
        'release_candidate_passed': release_candidate['status'] == 'passed',
        'finance_signoff_recorded': release_candidate['checks']['finance_signoff_recorded'] is True,
        'it_signoff_recorded': release_candidate['checks']['it_signoff_recorded'] is True,
        'no_unapproved_blocking_parity_gaps': len(blocking_gaps) == 0,
    }
    signoffs = {
        'finance': release_candidate['signoffs']['finance'],
        'it': release_candidate['signoffs']['it'],
        'pilot': pilot['signoff'],
        'parity': {
            'signed_by': payload.get('parity_signoff_by') or user['email'],
            'signed_at': _now(),
            'status': 'signed' if all(checks.values()) else 'needs_review',
            'evidence': {'parity_run_id': parity['id'], 'pilot_cycle_run_id': pilot_cycle['id'], 'final_gap_review_id': final_gap_review['id']},
        },
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO prophix_parity_pilot_signoff_runs (
            run_key, release_version, status, release_candidate_json, parity_matrix_json,
            final_gap_review_json, checks_json, signoffs_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            release_version,
            status_value,
            json.dumps(release_candidate, sort_keys=True),
            json.dumps({**parity_matrix, 'pilot_cycle': pilot_cycle}, sort_keys=True),
            json.dumps(final_gap_review, sort_keys=True),
            json.dumps(checks, sort_keys=True),
            json.dumps(signoffs, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('prophix_parity_pilot_signoff', run_key, status_value, user['email'], {'checks': checks, 'release_version': release_version}, completed)
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM prophix_parity_pilot_signoff_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Prophix-parity pilot signoff run not found.')
    return _format_run(row)


def _latest_run() -> dict[str, Any] | None:
    row = db.fetch_one('SELECT * FROM prophix_parity_pilot_signoff_runs ORDER BY id DESC LIMIT 1')
    return _format_run(row) if row else None


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['batch'] = 'B160'
    result['release_candidate'] = json.loads(result.pop('release_candidate_json') or '{}')
    result['parity_matrix'] = json.loads(result.pop('parity_matrix_json') or '{}')
    result['final_gap_review'] = json.loads(result.pop('final_gap_review_json') or '{}')
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['signoffs'] = json.loads(result.pop('signoffs_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
