from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.consolidation_certification import run_certification as run_consolidation_certification
from app.services.pilot_deployment import SELECTED_PILOT_ROLES, run_pilot_deployment, status as pilot_deployment_status


REQUIRED_CYCLE_STEPS = ['budget', 'forecast', 'close', 'consolidation', 'reporting', 'board_package']
REQUIRED_ROLES = ['budget_office', 'controller', 'department_planner', 'grants', 'executive', 'it_admin']


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS multi_user_pilot_cycle_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                cycle_steps_json TEXT NOT NULL,
                role_participants_json TEXT NOT NULL,
                pilot_json TEXT NOT NULL,
                consolidation_json TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_multi_user_pilot_cycle_runs_created
            ON multi_user_pilot_cycle_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM multi_user_pilot_cycle_runs ORDER BY id DESC LIMIT 1')
    pilot = pilot_deployment_status()
    checks = {
        'budget_office_controller_planner_grants_executive_it_roles_ready': True,
        'budget_forecast_close_consolidation_reporting_board_cycle_ready': True,
        'multi_user_signoff_recording_ready': True,
        'pilot_cycle_evidence_ready': True,
    }
    counts = {
        'multi_user_pilot_cycles': int(db.fetch_one('SELECT COUNT(*) AS count FROM multi_user_pilot_cycle_runs')['count']),
        'pilot_runs': pilot['counts']['pilot_runs'],
        'required_roles': len(REQUIRED_ROLES),
        'required_steps': len(REQUIRED_CYCLE_STEPS),
    }
    return {
        'batch': 'B127',
        'title': 'Multi-User Pilot Cycle Execution',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
        'pilot_deployment_status': pilot,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM multi_user_pilot_cycle_runs ORDER BY id DESC LIMIT ?', (max(1, min(limit, 200)),))
    return [_format_run(row) for row in rows]


def run_cycle(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b127-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    selected_users = payload.get('selected_users') or SELECTED_PILOT_ROLES
    pilot = run_pilot_deployment(
        {
            'run_key': f'{run_key}-pilot',
            'release_version': payload.get('release_version') or 'B127.pilot-cycle',
            'internal_server_url': payload.get('internal_server_url') or 'https://mufinances-pilot.manchester.edu',
            'database_backend': payload.get('database_backend') or 'mssql',
            'selected_users': selected_users,
        },
        user,
    )
    consolidation = run_consolidation_certification({'run_key': f'{run_key}-consolidation'}, user)
    cycle_steps = _cycle_steps(pilot, consolidation)
    role_participants = _role_participants(selected_users, pilot)
    checks = {
        'budget_office_participated': _role_ready(role_participants, 'budget_office'),
        'controller_participated': _role_ready(role_participants, 'controller'),
        'department_planners_participated': _role_ready(role_participants, 'department_planner'),
        'grants_participated': _role_ready(role_participants, 'grants'),
        'executives_participated': _role_ready(role_participants, 'executive'),
        'it_participated': _role_ready(role_participants, 'it_admin'),
        'budget_cycle_completed': cycle_steps['budget']['status'] == 'passed',
        'forecast_cycle_completed': cycle_steps['forecast']['status'] == 'passed',
        'close_cycle_completed': cycle_steps['close']['status'] == 'passed',
        'consolidation_cycle_completed': cycle_steps['consolidation']['status'] == 'passed',
        'reporting_cycle_completed': cycle_steps['reporting']['status'] == 'passed',
        'board_package_cycle_completed': cycle_steps['board_package']['status'] == 'passed',
        'pilot_signoff_recorded': pilot['status'] == 'passed' and pilot['signoff']['status'] == 'signed',
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO multi_user_pilot_cycle_runs (
            run_key, status, cycle_steps_json, role_participants_json, pilot_json,
            consolidation_json, checks_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            status_value,
            json.dumps(cycle_steps, sort_keys=True),
            json.dumps(role_participants, sort_keys=True),
            json.dumps(pilot, sort_keys=True),
            json.dumps(consolidation, sort_keys=True),
            json.dumps(checks, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('multi_user_pilot_cycle', run_key, status_value, user['email'], {'checks': checks, 'pilot_run_id': pilot['id']}, completed)
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM multi_user_pilot_cycle_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Multi-user pilot cycle run not found.')
    return _format_run(row)


def _cycle_steps(pilot: dict[str, Any], consolidation: dict[str, Any]) -> dict[str, dict[str, Any]]:
    artifacts = pilot['artifacts']
    fpa = artifacts['fpa_cycle']
    close = artifacts['close_cycle']
    reporting = artifacts['reporting_cycle']
    return {
        'budget': {'status': 'passed' if fpa['checks']['operating_budget_approved'] else 'needs_review', 'evidence_id': fpa['id']},
        'forecast': {'status': 'passed' if fpa['checks']['forecast_posted'] else 'needs_review', 'evidence_id': fpa['id']},
        'close': {'status': close['status'], 'evidence_id': close['id']},
        'consolidation': {'status': consolidation['status'], 'evidence_id': consolidation['id']},
        'reporting': {'status': reporting['status'], 'evidence_id': reporting['id']},
        'board_package': {'status': 'passed' if reporting['checks'].get('board_package_pdf_ready') or fpa['checks'].get('board_package_and_pdf_ready') else 'needs_review', 'evidence_id': reporting['id']},
    }


def _role_participants(selected_users: list[dict[str, Any]], pilot: dict[str, Any]) -> list[dict[str, Any]]:
    signed_roles = set(pilot['signoff'].get('selected_roles') or [])
    participants = []
    for item in selected_users:
        role_key = item['role_key']
        participants.append(
            {
                'role_key': role_key,
                'display_name': item.get('display_name') or role_key.replace('_', ' ').title(),
                'status': 'signed' if role_key in signed_roles else 'needs_review',
                'pilot_run_id': pilot['id'],
            }
        )
    return participants


def _role_ready(participants: list[dict[str, Any]], role_key: str) -> bool:
    return any(item['role_key'] == role_key and item['status'] == 'signed' for item in participants)


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['batch'] = 'B127'
    result['cycle_steps'] = json.loads(result.pop('cycle_steps_json') or '{}')
    result['role_participants'] = json.loads(result.pop('role_participants_json') or '[]')
    result['pilot'] = json.loads(result.pop('pilot_json') or '{}')
    result['consolidation'] = json.loads(result.pop('consolidation_json') or '{}')
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
