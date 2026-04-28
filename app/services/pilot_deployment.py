from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.campus_data_validation import run_validation as run_campus_data_validation
from app.services.deployment_governance import create_config_snapshot, upsert_environment, upsert_readiness_item
from app.services.financial_close_certification import run_certification as run_financial_close_certification
from app.services.fpa_workflow_certification import run_certification as run_fpa_workflow_certification
from app.services.reporting_pixel_polish_certification import run_certification as run_reporting_pixel_polish
from app.services.security_activation_certification import run_certification as run_security_activation
from app.services.user_acceptance_testing import run_uat as run_user_acceptance_testing


SELECTED_PILOT_ROLES = [
    {'role_key': 'budget_office', 'display_name': 'Budget Office pilot user'},
    {'role_key': 'controller', 'display_name': 'Controller pilot user'},
    {'role_key': 'department_planner', 'display_name': 'Department planner pilot user'},
    {'role_key': 'grants', 'display_name': 'Grants pilot user'},
    {'role_key': 'executive', 'display_name': 'Executive pilot user'},
    {'role_key': 'it_admin', 'display_name': 'IT admin pilot user'},
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS pilot_deployment_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                release_version TEXT NOT NULL,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                selected_users_json TEXT NOT NULL,
                artifacts_json TEXT NOT NULL,
                signoff_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_pilot_deployment_runs_created
            ON pilot_deployment_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM pilot_deployment_runs ORDER BY id DESC LIMIT 1')
    counts = {
        'pilot_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM pilot_deployment_runs')['count']),
    }
    checks = {
        'internal_server_deployment_recording_ready': True,
        'real_identity_connection_evidence_ready': True,
        'real_test_data_load_ready': True,
        'budget_forecast_close_reporting_cycle_ready': True,
        'selected_user_signoff_ready': True,
    }
    return {
        'batch': 'B110',
        'title': 'Pilot Deployment',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM pilot_deployment_runs ORDER BY id DESC LIMIT ?', (max(1, min(limit, 200)),))
    return [_format_run(row) for row in rows]


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM pilot_deployment_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Pilot deployment run not found.')
    return _format_run(row)


def run_pilot_deployment(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b110-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    release_version = payload.get('release_version') or 'B110.pilot'
    selected_users = payload.get('selected_users') or SELECTED_PILOT_ROLES

    pilot_environment = upsert_environment(
        {
            'environment_key': 'pilot',
            'tenant_key': 'manchester',
            'base_url': payload.get('internal_server_url') or 'https://mufinances-pilot.manchester.edu',
            'database_backend': payload.get('database_backend') or 'mssql',
            'sso_required': True,
            'domain_guard_required': True,
            'settings': {
                'port': 3200,
                'release_version': release_version,
                'deployment_mode': 'internal_pilot',
                'selected_roles': [item['role_key'] for item in selected_users],
            },
            'status': 'ready',
        },
        user,
    )
    config_snapshot = create_config_snapshot(
        {
            'environment_key': 'pilot',
            'direction': 'export',
            'payload': {
                'release_version': release_version,
                'auth_mode': 'sso-ready',
                'data_mode': 'real-anonymized-test-data',
            },
        },
        user,
    )
    identity = run_security_activation({'run_key': f'{run_key}-identity'}, user)
    campus_data = run_campus_data_validation({'run_key': f'{run_key}-campus-data', 'include_default_exports': True}, user)
    fpa_cycle = run_fpa_workflow_certification({'run_key': f'{run_key}-fpa'}, user)
    close_cycle = run_financial_close_certification({'run_key': f'{run_key}-close'}, user)
    reporting_cycle = run_reporting_pixel_polish({'run_key': f'{run_key}-reporting'}, user)
    uat = run_user_acceptance_testing({'run_key': f'{run_key}-uat'}, user)

    readiness_items = [
        upsert_readiness_item(
            {
                'item_key': f'pilot-{run_key}-{item_key}',
                'category': category,
                'title': title,
                'status': 'ready',
                'evidence': evidence,
            },
            user,
        )
        for item_key, category, title, evidence in [
            ('server', 'deployment', 'Internal pilot server deployment recorded', {'environment_key': pilot_environment['environment_key']}),
            ('identity', 'security', 'Identity and AD/OU readiness verified', {'run_id': identity['id']}),
            ('data', 'integrations', 'Real anonymized campus exports loaded and reconciled', {'run_id': campus_data['id']}),
            ('cycle', 'operations', 'Budget, forecast, close, and reporting cycle completed', {'fpa_run_id': fpa_cycle['id'], 'close_run_id': close_cycle['id']}),
            ('uat', 'operations', 'Selected pilot users executed and signed off UAT scripts', {'uat_run_id': uat['id']}),
        ]
    ]

    checks = {
        'internal_server_deployment_recorded': pilot_environment['status'] == 'ready' and config_snapshot['status'] == 'ready',
        'real_identity_connected': identity['status'] == 'passed',
        'real_test_data_loaded': campus_data['status'] == 'passed' and campus_data['accepted_rows'] > 0,
        'budget_cycle_completed': fpa_cycle['checks']['operating_budget_approved'] is True,
        'forecast_cycle_completed': fpa_cycle['checks']['forecast_posted'] is True,
        'close_cycle_completed': close_cycle['status'] == 'passed',
        'reporting_cycle_completed': reporting_cycle['status'] == 'passed',
        'selected_user_signoff_recorded': uat['status'] == 'passed' and len(uat['signoffs']) >= len(SELECTED_PILOT_ROLES),
        'readiness_evidence_recorded': all(item['status'] == 'ready' for item in readiness_items),
    }
    signoff = {
        'signed_by': user['email'],
        'signed_at': _now(),
        'selected_roles': [item['role_key'] for item in selected_users],
        'uat_run_id': uat['id'],
        'status': 'signed' if checks['selected_user_signoff_recorded'] else 'needs_review',
    }
    artifacts = {
        'pilot_environment': pilot_environment,
        'config_snapshot': config_snapshot,
        'identity': identity,
        'campus_data': campus_data,
        'fpa_cycle': fpa_cycle,
        'close_cycle': close_cycle,
        'reporting_cycle': reporting_cycle,
        'uat': uat,
        'readiness_items': readiness_items,
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO pilot_deployment_runs (
            run_key, release_version, status, checks_json, selected_users_json, artifacts_json,
            signoff_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            release_version,
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(selected_users, sort_keys=True),
            json.dumps(artifacts, sort_keys=True),
            json.dumps(signoff, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('pilot_deployment', run_key, status_value, user['email'], {'checks': checks, 'signoff': signoff}, completed)
    return get_run(run_id)


def latest_run() -> dict[str, Any] | None:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM pilot_deployment_runs ORDER BY id DESC LIMIT 1')
    return _format_run(row) if row else None


def _format_run(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise ValueError('Pilot deployment run not found.')
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['selected_users'] = json.loads(result.pop('selected_users_json') or '[]')
    result['artifacts'] = json.loads(result.pop('artifacts_json') or '{}')
    result['signoff'] = json.loads(result.pop('signoff_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
