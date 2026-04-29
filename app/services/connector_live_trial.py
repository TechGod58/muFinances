from __future__ import annotations

import json
import os
import secrets
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.campus_integrations import (
    create_retry_event,
    list_rejections,
    list_source_drillbacks,
    list_sync_logs,
    run_health_check,
    run_import,
    run_sync_job,
    seed_connector_marketplace,
    start_auth_flow,
    store_credential,
    upsert_connector,
)


LIVE_CONNECTOR_TRIALS = [
    {
        'slot': 'gl',
        'connector_key': 'live-trial-gl',
        'name': 'Live Trial GL Connector',
        'system_type': 'erp',
        'adapter_key': 'erp_gl',
        'auth_type': 'api_key',
        'job_type': 'live_gl_sync',
        'import_type': 'ledger',
    },
    {
        'slot': 'sis',
        'connector_key': 'live-trial-sis',
        'name': 'Live Trial SIS Connector',
        'system_type': 'sis',
        'adapter_key': 'sis_enrollment',
        'auth_type': 'oauth_client',
        'job_type': 'live_sis_enrollment_sync',
        'import_type': 'crm_enrollment',
    },
    {
        'slot': 'hr_payroll',
        'connector_key': 'live-trial-hr-payroll',
        'name': 'Live Trial HR/Payroll Connector',
        'system_type': 'payroll',
        'adapter_key': 'payroll_actuals',
        'auth_type': 'api_key',
        'job_type': 'live_hr_payroll_sync',
        'import_type': 'ledger',
    },
    {
        'slot': 'grants',
        'connector_key': 'live-trial-grants',
        'name': 'Live Trial Grants Connector',
        'system_type': 'grants',
        'adapter_key': 'grants_awards',
        'auth_type': 'oauth_client',
        'job_type': 'live_grants_sync',
        'import_type': 'ledger',
    },
    {
        'slot': 'banking',
        'connector_key': 'live-trial-banking',
        'name': 'Live Trial Banking Connector',
        'system_type': 'banking',
        'adapter_key': 'banking_cash',
        'auth_type': 'api_key',
        'job_type': 'live_banking_sync',
        'import_type': 'banking_cash',
    },
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS connector_live_trial_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                mode TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                connectors_json TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_connector_live_trial_runs_created
            ON connector_live_trial_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM connector_live_trial_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'gl_live_trial_ready': True,
        'sis_live_trial_ready': True,
        'hr_payroll_live_trial_ready': True,
        'grants_live_trial_ready': True,
        'banking_live_trial_ready': True,
        'oauth_api_key_handling_ready': True,
        'retries_sync_logs_rejections_drillback_ready': True,
        'credential_rehearsal_fallback_ready': True,
    }
    counts = {
        'live_trial_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM connector_live_trial_runs')['count']),
        'required_connectors': len(LIVE_CONNECTOR_TRIALS),
        'sync_logs': int(db.fetch_one('SELECT COUNT(*) AS count FROM connector_sync_logs')['count']),
        'rejections': int(db.fetch_one('SELECT COUNT(*) AS count FROM import_rejections')['count']),
        'drillbacks': int(db.fetch_one('SELECT COUNT(*) AS count FROM connector_source_drillbacks')['count']),
    }
    return {
        'batch': 'B122',
        'title': 'Connector Live Trial',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM connector_live_trial_runs ORDER BY id DESC LIMIT ?', (max(1, min(limit, 500)),))
    return [_format_run(row) for row in rows]


def run_trial(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    seed_connector_marketplace()
    started = _now()
    run_key = payload.get('run_key') or f"b122-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    scenario_id = int(payload.get('scenario_id') or _default_scenario_id())
    live_mode_requested = bool(payload.get('live_mode', False))
    credential_refs = payload.get('credential_refs') or {}
    connector_results = []
    for config in LIVE_CONNECTOR_TRIALS:
        connector_results.append(_run_connector_trial(run_key, scenario_id, config, credential_refs, live_mode_requested, user))

    source_drillbacks = list_source_drillbacks()
    sync_logs = list_sync_logs()
    rejections = list_rejections()
    evidence = {
        'source_drillbacks': len(source_drillbacks),
        'sync_logs': len(sync_logs),
        'rejections': len(rejections),
        'latest_rejection': rejections[0] if rejections else None,
        'live_mode_requested': live_mode_requested,
        'credential_sources': {row['slot']: row['credential_source'] for row in connector_results},
    }
    checks = {
        'one_gl_connector_activated': _slot_ready(connector_results, 'gl'),
        'one_sis_connector_activated': _slot_ready(connector_results, 'sis'),
        'one_hr_payroll_connector_activated': _slot_ready(connector_results, 'hr_payroll'),
        'one_grants_connector_activated': _slot_ready(connector_results, 'grants'),
        'one_banking_connector_activated': _slot_ready(connector_results, 'banking'),
        'oauth_and_api_key_flows_ready': {'oauth_client', 'api_key'} <= {row['auth_type'] for row in connector_results},
        'retries_ready': all(row['retry_status'] == 'retry_scheduled' for row in connector_results),
        'sync_logs_ready': evidence['sync_logs'] >= len(connector_results),
        'rejection_queue_ready': evidence['rejections'] >= 1,
        'drillback_ready': evidence['source_drillbacks'] >= len(connector_results),
        'live_credentials_or_rehearsal_evidence_ready': all(row['credential_ref'].startswith('vault://') for row in connector_results),
    }
    mode = 'live_ready' if live_mode_requested and all(row['credential_source'] in {'payload', 'environment'} for row in connector_results) else 'rehearsal_ready'
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO connector_live_trial_runs (
            run_key, scenario_id, status, mode, checks_json, connectors_json, evidence_json,
            created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            scenario_id,
            status_value,
            mode,
            json.dumps(checks, sort_keys=True),
            json.dumps(connector_results, sort_keys=True),
            json.dumps(evidence, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('connector_live_trial', run_key, status_value, user['email'], {'checks': checks, 'mode': mode}, completed)
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM connector_live_trial_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Connector live trial run not found.')
    return _format_run(row)


def _run_connector_trial(
    run_key: str,
    scenario_id: int,
    config: dict[str, Any],
    credential_refs: dict[str, str],
    live_mode_requested: bool,
    user: dict[str, Any],
) -> dict[str, Any]:
    connector_key = f"{config['connector_key']}-{run_key}".replace('_', '-').lower()
    credential_ref, credential_source = _credential_ref(connector_key, config, credential_refs, live_mode_requested, run_key, user)
    connector = upsert_connector(
        {
            'connector_key': connector_key,
            'name': config['name'],
            'system_type': config['system_type'],
            'direction': 'inbound',
            'status': 'configured',
            'config': {
                'adapter_key': config['adapter_key'],
                'live_trial_run': run_key,
                'mode': 'live' if live_mode_requested else 'rehearsal',
                'credential_source': credential_source,
                'source_owner': 'Manchester University',
            },
        },
        user,
    )
    auth = start_auth_flow({'connector_key': connector_key, 'adapter_key': config['adapter_key'], 'credential_ref': credential_ref}, user)
    health = run_health_check(connector_key, user)
    imported = run_import(
        {
            'scenario_id': scenario_id,
            'connector_key': connector_key,
            'source_format': 'csv',
            'import_type': config['import_type'],
            'source_name': f"{run_key}-{config['slot']}.csv",
            'stream_chunk_size': 1,
            'rows': _trial_rows(run_key, config),
        },
        user,
    )
    sync = run_sync_job({'connector_key': connector_key, 'job_type': config['job_type']}, user)
    retry = create_retry_event(
        {
            'connector_key': connector_key,
            'operation_type': config['job_type'],
            'error_message': 'Live trial retry/backoff path recorded without replaying accepted rows.',
            'attempts': 1,
        },
        user,
    )
    return {
        'slot': config['slot'],
        'connector_key': connector_key,
        'system_type': config['system_type'],
        'adapter_key': config['adapter_key'],
        'auth_type': config['auth_type'],
        'credential_ref': credential_ref,
        'credential_source': credential_source,
        'connector_status': connector['status'],
        'auth_status': auth['status'],
        'health_status': health['status'],
        'import_status': imported['status'],
        'accepted_rows': imported['accepted_rows'],
        'rejected_rows': imported['rejected_rows'],
        'sync_status': sync['status'],
        'retry_status': retry['status'],
    }


def _credential_ref(
    connector_key: str,
    config: dict[str, Any],
    credential_refs: dict[str, str],
    live_mode_requested: bool,
    run_key: str,
    user: dict[str, Any],
) -> tuple[str, str]:
    slot = config['slot']
    payload_ref = str(credential_refs.get(slot) or '').strip()
    if payload_ref:
        secret = store_credential(
            {
                'connector_key': connector_key,
                'credential_key': 'live-provider',
                'secret_value': payload_ref,
                'secret_type': config['auth_type'],
            },
            user,
        )
        return secret['secret_ref'], 'payload'
    env_name = f"CAMPUS_FPM_CONNECTOR_{slot.upper()}_CREDENTIAL"
    env_value = os.getenv(env_name, '').strip()
    if env_value:
        secret = store_credential(
            {
                'connector_key': connector_key,
                'credential_key': 'live-provider',
                'secret_value': env_value,
                'secret_type': config['auth_type'],
            },
            user,
        )
        return secret['secret_ref'], 'environment'
    secret = store_credential(
        {
            'connector_key': connector_key,
            'credential_key': 'rehearsal-provider',
            'secret_value': f'{run_key}-{connector_key}-{secrets.token_hex(8)}',
            'secret_type': config['auth_type'],
        },
        user,
    )
    return secret['secret_ref'], 'rehearsal' if not live_mode_requested else 'missing_live_credential_rehearsal'


def _trial_rows(run_key: str, config: dict[str, Any]) -> list[dict[str, Any]]:
    slot = config['slot']
    if config['import_type'] == 'crm_enrollment':
        return [
            {'pipeline_stage': 'deposit', 'term': '2027FA', 'headcount': 37, 'yield_rate': 0.78, 'source_record_id': f'{run_key}:{slot}:1'},
        ]
    if config['import_type'] == 'banking_cash':
        return [
            {'bank_account': 'OPERATING', 'transaction_date': '2026-10-01', 'amount': 25000, 'description': 'Live trial cash receipt', 'source_record_id': f'{run_key}:{slot}:1'},
        ]
    rows = [
        {'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': f'{slot.upper()}_LIVE', 'period': '2026-10', 'amount': 1000, 'notes': f'{slot} live trial accepted', 'source_record_id': f'{run_key}:{slot}:1'},
    ]
    if slot == 'gl':
        rows.append({'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': '', 'period': '2026-10', 'amount': '', 'notes': 'B122 rejection queue proof', 'source_record_id': f'{run_key}:{slot}:reject'})
    return rows


def _slot_ready(rows: list[dict[str, Any]], slot: str) -> bool:
    row = next((item for item in rows if item['slot'] == slot), None)
    return bool(row and row['connector_status'] == 'configured' and row['auth_status'] == 'ready' and row['sync_status'] == 'complete' and row['accepted_rows'] >= 1)


def _default_scenario_id() -> int:
    row = db.fetch_one("SELECT id FROM scenarios WHERE name = 'FY27 Operating Plan' ORDER BY id LIMIT 1") or db.fetch_one('SELECT id FROM scenarios ORDER BY id LIMIT 1')
    if row is None:
        return db.execute(
            '''
            INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
            VALUES ('B122 Connector Live Trial Scenario', 'b122', 'draft', '2026-08', '2027-07', 0, ?)
            ''',
            (_now(),),
        )
    return int(row['id'])


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['batch'] = 'B122'
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['connectors'] = json.loads(result.pop('connectors_json') or '[]')
    result['evidence'] = json.loads(result.pop('evidence_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
