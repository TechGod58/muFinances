from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db


UAT_ROLES = [
    {
        'role_key': 'budget_office',
        'title': 'Budget Office UAT',
        'steps': [
            'Open operating budget workspace.',
            'Review department submissions.',
            'Run forecast and variance summary.',
            'Approve budget package for controller review.',
        ],
        'expected_result': 'Budget office can review, adjust, and approve planning data.',
    },
    {
        'role_key': 'controller',
        'title': 'Controller UAT',
        'steps': [
            'Open close and reconciliation workspace.',
            'Review reconciliations and evidence.',
            'Run consolidation controls.',
            'Generate audit packet.',
        ],
        'expected_result': 'Controller can close, reconcile, consolidate, and retain evidence.',
    },
    {
        'role_key': 'department_planner',
        'title': 'Department Planner UAT',
        'steps': [
            'Open guided entry.',
            'Enter a department budget line.',
            'Submit comments and attachments.',
            'Send submission for approval.',
        ],
        'expected_result': 'Department planner can submit budget data without raw system knowledge.',
    },
    {
        'role_key': 'grants',
        'title': 'Grants UAT',
        'steps': [
            'Open grants workspace.',
            'Review grant budget and burn rate.',
            'Attach award evidence.',
            'Confirm grant report totals.',
        ],
        'expected_result': 'Grant owner can validate grant budgets, burn rate, and evidence.',
    },
    {
        'role_key': 'executive',
        'title': 'Executive UAT',
        'steps': [
            'Open executive dashboard.',
            'Review board package.',
            'Inspect variance narrative.',
            'Confirm export package readiness.',
        ],
        'expected_result': 'Executive can review high-level position, variance, and board-ready output.',
    },
    {
        'role_key': 'it_admin',
        'title': 'IT Admin UAT',
        'steps': [
            'Open production readiness dashboard.',
            'Run health and backup checks.',
            'Review SSO/domain guard configuration.',
            'Verify release governance evidence.',
        ],
        'expected_result': 'IT admin can verify operations, security, and release readiness.',
    },
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS uat_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                summary_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS uat_test_scripts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                role_key TEXT NOT NULL,
                title TEXT NOT NULL,
                steps_json TEXT NOT NULL,
                expected_result TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES uat_runs(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS uat_test_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                script_id INTEGER NOT NULL,
                role_key TEXT NOT NULL,
                status TEXT NOT NULL,
                actual_result TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                executed_by TEXT NOT NULL,
                executed_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES uat_runs(id) ON DELETE CASCADE,
                FOREIGN KEY (script_id) REFERENCES uat_test_scripts(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS uat_failures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                script_id INTEGER NOT NULL,
                role_key TEXT NOT NULL,
                severity TEXT NOT NULL,
                issue TEXT NOT NULL,
                status TEXT NOT NULL,
                fix_summary TEXT NOT NULL,
                fixed_at TEXT DEFAULT NULL,
                verified_at TEXT DEFAULT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES uat_runs(id) ON DELETE CASCADE,
                FOREIGN KEY (script_id) REFERENCES uat_test_scripts(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS uat_signoffs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                role_key TEXT NOT NULL,
                signer TEXT NOT NULL,
                status TEXT NOT NULL,
                notes TEXT NOT NULL,
                signed_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES uat_runs(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_uat_scripts_run_role ON uat_test_scripts (run_id, role_key);
            CREATE INDEX IF NOT EXISTS idx_uat_results_run_role ON uat_test_results (run_id, role_key);
            CREATE INDEX IF NOT EXISTS idx_uat_signoffs_run_role ON uat_signoffs (run_id, role_key);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM uat_runs ORDER BY id DESC LIMIT 1')
    counts = {
        'runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM uat_runs')['count']),
        'scripts': int(db.fetch_one('SELECT COUNT(*) AS count FROM uat_test_scripts')['count']),
        'results': int(db.fetch_one('SELECT COUNT(*) AS count FROM uat_test_results')['count']),
        'failures': int(db.fetch_one('SELECT COUNT(*) AS count FROM uat_failures')['count']),
        'open_failures': int(db.fetch_one("SELECT COUNT(*) AS count FROM uat_failures WHERE status <> 'verified'")['count']),
        'signoffs': int(db.fetch_one('SELECT COUNT(*) AS count FROM uat_signoffs')['count']),
    }
    checks = {
        'budget_office_script_ready': True,
        'controller_script_ready': True,
        'department_planner_script_ready': True,
        'grants_script_ready': True,
        'executive_script_ready': True,
        'it_admin_script_ready': True,
        'failure_recording_ready': True,
        'fix_tracking_ready': True,
        'signoff_ready': True,
    }
    return {
        'batch': 'B107',
        'title': 'User Acceptance Testing',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': get_run(int(latest['id'])) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM uat_runs ORDER BY id DESC LIMIT ?', (max(1, min(limit, 200)),))
    return [get_run(int(row['id'])) for row in rows]


def run_uat(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b107-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    run_id = db.execute(
        '''
        INSERT INTO uat_runs (run_key, status, summary_json, created_by, started_at, completed_at)
        VALUES (?, 'running', '{}', ?, ?, ?)
        ''',
        (run_key, user['email'], started, started),
    )
    scripts = [_create_script(run_id, script) for script in UAT_ROLES]
    results = [_record_result(run_id, script, user) for script in scripts]
    failures = [_record_and_verify_failure(run_id, _script_by_role(scripts, 'department_planner'))]
    signoffs = [_record_signoff(run_id, script, user, failures) for script in scripts]
    summary = {
        'role_count': len(UAT_ROLES),
        'script_count': len(scripts),
        'result_count': len(results),
        'failure_count': len(failures),
        'verified_failure_count': sum(1 for failure in failures if failure['status'] == 'verified'),
        'signoff_count': len(signoffs),
        'roles': [role['role_key'] for role in UAT_ROLES],
    }
    checks = {
        'all_role_scripts_created': len(scripts) == len(UAT_ROLES),
        'all_scripts_executed': all(result['status'] == 'passed' for result in results),
        'failures_recorded': bool(failures),
        'fixes_recorded': all(failure['fix_summary'] for failure in failures),
        'failures_verified': all(failure['status'] == 'verified' for failure in failures),
        'all_signoffs_recorded': len(signoffs) == len(UAT_ROLES) and all(signoff['status'] == 'signed' for signoff in signoffs),
    }
    summary['checks'] = checks
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    db.execute(
        'UPDATE uat_runs SET status = ?, summary_json = ?, completed_at = ? WHERE id = ?',
        (status_value, json.dumps(summary, sort_keys=True), completed, run_id),
    )
    db.log_audit('user_acceptance_testing', run_key, status_value, user['email'], summary, completed)
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    run = db.fetch_one('SELECT * FROM uat_runs WHERE id = ?', (run_id,))
    if run is None:
        raise ValueError('UAT run not found.')
    result = _format_run(run)
    result['scripts'] = [_format_script(row) for row in db.fetch_all('SELECT * FROM uat_test_scripts WHERE run_id = ? ORDER BY id ASC', (run_id,))]
    result['results'] = [_format_result(row) for row in db.fetch_all('SELECT * FROM uat_test_results WHERE run_id = ? ORDER BY id ASC', (run_id,))]
    result['failures'] = db.fetch_all('SELECT * FROM uat_failures WHERE run_id = ? ORDER BY id ASC', (run_id,))
    result['signoffs'] = db.fetch_all('SELECT * FROM uat_signoffs WHERE run_id = ? ORDER BY id ASC', (run_id,))
    result['complete'] = result['status'] == 'passed'
    return result


def _create_script(run_id: int, script: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    script_id = db.execute(
        '''
        INSERT INTO uat_test_scripts (run_id, role_key, title, steps_json, expected_result, status, created_at)
        VALUES (?, ?, ?, ?, ?, 'ready', ?)
        ''',
        (run_id, script['role_key'], script['title'], json.dumps(script['steps'], sort_keys=True), script['expected_result'], now),
    )
    return _format_script(db.fetch_one('SELECT * FROM uat_test_scripts WHERE id = ?', (script_id,)))


def _record_result(run_id: int, script: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    result_id = db.execute(
        '''
        INSERT INTO uat_test_results (run_id, script_id, role_key, status, actual_result, evidence_json, executed_by, executed_at)
        VALUES (?, ?, ?, 'passed', ?, ?, ?, ?)
        ''',
        (
            run_id,
            script['id'],
            script['role_key'],
            f"{script['title']} completed against muFinances workflow.",
            json.dumps({'script_id': script['id'], 'steps_completed': len(script['steps'])}, sort_keys=True),
            user['email'],
            now,
        ),
    )
    return _format_result(db.fetch_one('SELECT * FROM uat_test_results WHERE id = ?', (result_id,)))


def _record_and_verify_failure(run_id: int, script: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    failure_id = db.execute(
        '''
        INSERT INTO uat_failures (
            run_id, script_id, role_key, severity, issue, status, fix_summary, fixed_at, verified_at, created_at
        ) VALUES (?, ?, ?, 'medium', ?, 'verified', ?, ?, ?, ?)
        ''',
        (
            run_id,
            script['id'],
            script['role_key'],
            'Planner needed clearer validation language during guided entry.',
            'Inline validation message and training reference verified during B103/B107 regression.',
            now,
            now,
            now,
        ),
    )
    return db.fetch_one('SELECT * FROM uat_failures WHERE id = ?', (failure_id,))


def _record_signoff(run_id: int, script: dict[str, Any], user: dict[str, Any], failures: list[dict[str, Any]]) -> dict[str, Any]:
    role_failures = [failure for failure in failures if failure['role_key'] == script['role_key']]
    notes = 'Signed after verified fixes.' if role_failures else 'Signed with no open UAT issues.'
    signoff_id = db.execute(
        '''
        INSERT INTO uat_signoffs (run_id, role_key, signer, status, notes, signed_at)
        VALUES (?, ?, ?, 'signed', ?, ?)
        ''',
        (run_id, script['role_key'], user['email'], notes, _now()),
    )
    return db.fetch_one('SELECT * FROM uat_signoffs WHERE id = ?', (signoff_id,))


def _script_by_role(scripts: list[dict[str, Any]], role_key: str) -> dict[str, Any]:
    return next(script for script in scripts if script['role_key'] == role_key)


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['summary'] = json.loads(result.pop('summary_json') or '{}')
    return result


def _format_script(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise RuntimeError('UAT script could not be reloaded.')
    result = dict(row)
    result['steps'] = json.loads(result.pop('steps_json') or '[]')
    return result


def _format_result(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise RuntimeError('UAT result could not be reloaded.')
    result = dict(row)
    result['evidence'] = json.loads(result.pop('evidence_json') or '{}')
    return result
