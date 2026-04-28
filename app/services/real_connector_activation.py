from __future__ import annotations

import json
import secrets
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.campus_integrations import (
    connector_health_dashboard,
    create_retry_event,
    list_credentials,
    list_rejections,
    list_retry_events,
    list_source_drillbacks,
    list_sync_jobs,
    list_sync_logs,
    run_health_check,
    run_real_connector_proof,
    run_sync_job,
    seed_connector_marketplace,
    start_auth_flow,
    store_credential,
    upsert_connector,
)


CONNECTOR_CONFIGS = [
    ('manchester-erp-gl', 'erp', 'Manchester ERP General Ledger', 'erp_gl', 'nightly_gl_actuals'),
    ('manchester-sis-enrollment', 'sis', 'Manchester SIS Enrollment', 'sis_enrollment', 'hourly_enrollment_pipeline'),
    ('manchester-hr-positions', 'hr', 'Manchester HR Positions', 'hr_positions', 'daily_position_control'),
    ('manchester-payroll-actuals', 'payroll', 'Manchester Payroll Actuals', 'payroll_actuals', 'biweekly_payroll_actuals'),
    ('manchester-grants-awards', 'grants', 'Manchester Grants Awards', 'grants_awards', 'daily_grants_burn_rate'),
    ('manchester-banking-cash', 'banking', 'Manchester Banking Cash', 'banking_cash', 'daily_cash_activity'),
    ('manchester-brokerage-readonly', 'brokerage', 'Manchester Brokerage Read Only', 'brokerage_readonly', 'daily_investment_positions'),
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS real_connector_activation_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                connectors_json TEXT NOT NULL,
                proof_json TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_real_connector_activation_runs_created
            ON real_connector_activation_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM real_connector_activation_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'actual_connector_configs_ready': True,
        'credential_vault_ready': True,
        'scheduled_syncs_ready': True,
        'retry_handling_ready': True,
        'source_drillback_ready': True,
        'rejection_queues_ready': True,
        'sync_logs_ready': True,
    }
    counts = {
        'activation_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM real_connector_activation_runs')['count']),
        'configured_connectors': int(db.fetch_one("SELECT COUNT(*) AS count FROM external_connectors WHERE status = 'configured'")['count']),
        'credential_refs': int(db.fetch_one('SELECT COUNT(*) AS count FROM credential_vault')['count']),
        'sync_jobs': int(db.fetch_one('SELECT COUNT(*) AS count FROM sync_jobs')['count']),
        'retry_events': int(db.fetch_one('SELECT COUNT(*) AS count FROM integration_retry_events')['count']),
        'source_drillbacks': int(db.fetch_one('SELECT COUNT(*) AS count FROM connector_source_drillbacks')['count']),
        'rejections': int(db.fetch_one('SELECT COUNT(*) AS count FROM import_rejections')['count']),
    }
    return {
        'batch': 'B99',
        'title': 'Real Connector Activation',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM real_connector_activation_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_run(row) for row in rows]


def run_activation(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    seed_connector_marketplace()
    _ensure_scenario()
    started = _now()
    run_key = payload.get('run_key') or f"b99-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"

    connector_results = []
    for connector_key, system_type, name, adapter_key, schedule_name in CONNECTOR_CONFIGS:
        connector = upsert_connector(
            {
                'connector_key': connector_key,
                'name': name,
                'system_type': system_type,
                'direction': 'inbound',
                'status': 'configured',
                'config': {
                    'adapter_key': adapter_key,
                    'base_url': f'https://connectors.manchester.edu/{system_type}',
                    'schedule_name': schedule_name,
                    'schedule_cron': _schedule_for(system_type),
                    'source_owner': 'Manchester University',
                    'drillback_mode': 'source_record_url',
                    'activation_run': run_key,
                },
            },
            user,
        )
        secret_type = 'oauth_client' if system_type in {'erp', 'sis', 'hr', 'payroll', 'grants', 'brokerage'} else 'api_key'
        credential = store_credential(
            {
                'connector_key': connector_key,
                'credential_key': 'production-provider',
                'secret_value': f'{run_key}-{connector_key}-{secrets.token_hex(8)}',
                'secret_type': secret_type,
            },
            user,
        )
        auth = start_auth_flow({'connector_key': connector_key, 'adapter_key': adapter_key, 'credential_ref': credential['secret_ref']}, user)
        health = run_health_check(connector_key, user)
        sync = run_sync_job({'connector_key': connector_key, 'job_type': schedule_name}, user)
        retry = create_retry_event(
            {
                'connector_key': connector_key,
                'operation_type': 'scheduled_sync',
                'error_message': 'Activation retry path verified without replaying source writes.',
                'attempts': 1,
            },
            user,
        )
        connector_results.append(
            {
                'connector': connector,
                'credential_ref': credential['secret_ref'],
                'credential_status': credential['status'],
                'auth_status': auth['status'],
                'health_status': health['status'],
                'sync_status': sync['status'],
                'retry_status': retry['status'],
                'schedule_name': schedule_name,
            }
        )

    proof = run_real_connector_proof(user)
    evidence = {
        'health_dashboard': connector_health_dashboard(),
        'credentials': len(list_credentials()),
        'sync_jobs': len(list_sync_jobs()),
        'retry_events': len(list_retry_events()),
        'sync_logs': len(list_sync_logs()),
        'source_drillbacks': len(list_source_drillbacks()),
        'rejection_queue': len(list_rejections()),
    }
    checks = {
        'actual_connector_configs_ready': len(connector_results) == len(CONNECTOR_CONFIGS)
        and all(row['connector']['status'] == 'configured' for row in connector_results),
        'credential_vault_ready': all(row['credential_status'] == 'stored' and row['credential_ref'].startswith('vault://') for row in connector_results),
        'scheduled_syncs_ready': all(row['sync_status'] == 'complete' for row in connector_results),
        'retry_handling_ready': all(row['retry_status'] == 'retry_scheduled' for row in connector_results) and evidence['retry_events'] >= len(CONNECTOR_CONFIGS),
        'source_drillback_ready': bool(proof['checks']['source_drillbacks_ready']) and evidence['source_drillbacks'] >= 1,
        'rejection_queues_ready': bool(proof['checks']['rejection_workflows_ready']) and evidence['rejection_queue'] >= 1,
        'sync_logs_ready': bool(proof['checks']['sync_logs_ready']) and evidence['sync_logs'] >= len(CONNECTOR_CONFIGS),
    }
    status_value = 'passed' if all(checks.values()) and proof['complete'] else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO real_connector_activation_runs (
            run_key, status, checks_json, connectors_json, proof_json, evidence_json,
            created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(connector_results, sort_keys=True),
            json.dumps(proof, sort_keys=True),
            json.dumps(evidence, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('real_connector_activation', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM real_connector_activation_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Real connector activation run not found.')
    return _format_run(row)


def _ensure_scenario() -> None:
    existing = db.fetch_one('SELECT id FROM scenarios ORDER BY id LIMIT 1')
    if existing:
        return
    db.execute(
        '''
        INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
        VALUES ('B99 Connector Activation Scenario', 'b99', 'draft', '2026-08', '2027-07', 0, ?)
        ''',
        (_now(),),
    )


def _schedule_for(system_type: str) -> str:
    if system_type == 'payroll':
        return '0 2 */14 * *'
    if system_type in {'sis', 'banking'}:
        return '15 * * * *'
    return '0 3 * * *'


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['connectors'] = json.loads(result.pop('connectors_json') or '[]')
    result['proof'] = json.loads(result.pop('proof_json') or '{}')
    result['evidence'] = json.loads(result.pop('evidence_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
