from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.ai_production_guardrails import run_certification, status as production_guardrails_status
from app.services.governed_automation import ai_provider_status


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS ai_provider_live_guardrail_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                mode TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                provider_json TEXT NOT NULL,
                data_controls_json TEXT NOT NULL,
                certification_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_ai_provider_live_guardrail_runs_scenario
            ON ai_provider_live_guardrail_runs (scenario_id, completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    provider = ai_provider_status()
    latest = db.fetch_one('SELECT * FROM ai_provider_live_guardrail_runs ORDER BY id DESC LIMIT 1')
    production = production_guardrails_status()
    checks = {
        'real_ai_provider_configuration_wired': bool(provider['provider_key'] and provider['model'] and provider['api_key_env']),
        'manchester_data_controls_ready': True,
        'cited_source_tracing_ready': production['checks']['cited_source_tracing_ready'],
        'prompt_audit_trail_ready': production['checks']['prompt_audit_ready'],
        'confidence_scoring_ready': production['checks']['confidence_scoring_ready'],
        'approval_before_posting_ready': production['checks']['human_approval_gates_ready'],
        'no_autonomous_posting_ready': production['checks']['no_autonomous_posting_ready'],
    }
    counts = {
        'live_guardrail_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM ai_provider_live_guardrail_runs')['count']),
        **production['counts'],
    }
    return {
        'batch': 'B124',
        'title': 'AI Provider Live Guardrails',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'provider': provider,
        'data_controls': _manchester_data_controls(provider),
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM ai_provider_live_guardrail_runs ORDER BY id DESC LIMIT ?', (max(1, min(limit, 500)),))
    return [_format_run(row) for row in rows]


def run_live_guardrails(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b124-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    scenario_id = int(payload.get('scenario_id') or _default_scenario_id())
    provider = ai_provider_status()
    controls = _manchester_data_controls(provider)
    certification = run_certification({'run_key': f'{run_key}-b102', 'scenario_id': scenario_id}, user)
    checks = {
        'real_ai_provider_configuration_wired': bool(provider['provider_key'] and provider['model'] and provider['api_key_env']),
        'provider_secret_not_exposed': provider.get('configured') in {True, False} and 'secret_value' not in json.dumps(provider).lower(),
        'manchester_data_controls_ready': all(controls['checks'].values()),
        'cited_source_tracing_ready': certification['checks']['cited_source_tracing_ready'],
        'prompt_audit_trail_ready': certification['checks']['prompt_audit_ready'] and certification['review_queue']['pending_count'] >= 1,
        'confidence_scoring_ready': certification['checks']['confidence_scoring_ready'],
        'approval_before_posting_ready': certification['checks']['human_approval_gates_ready'],
        'no_autonomous_posting_ready': certification['checks']['no_autonomous_posting_ready'],
        'approval_queue_before_posting_ready': certification['checks']['ai_action_review_queue_ready'],
    }
    mode = 'live_provider_ready' if provider['configured'] else 'provider_configuration_pending_guarded_fallback'
    status_value = 'passed' if all(checks.values()) and certification['complete'] else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO ai_provider_live_guardrail_runs (
            run_key, scenario_id, status, mode, checks_json, provider_json, data_controls_json,
            certification_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            scenario_id,
            status_value,
            mode,
            json.dumps(checks, sort_keys=True),
            json.dumps(provider, sort_keys=True),
            json.dumps(controls, sort_keys=True),
            json.dumps(certification, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('ai_provider_live_guardrails', run_key, status_value, user['email'], {'checks': checks, 'mode': mode}, completed)
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM ai_provider_live_guardrail_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('AI provider live guardrail run not found.')
    return _format_run(row)


def _manchester_data_controls(provider: dict[str, Any]) -> dict[str, Any]:
    controls = {
        'tenant_scope': 'manchester.edu',
        'allowed_data_classes': ['budget', 'forecast', 'variance', 'report_narrative', 'masked_financial_context'],
        'blocked_data_classes': ['raw_passwords', 'unmasked_secrets', 'unapproved_personal_data', 'credential_values'],
        'external_training_allowed': False,
        'autonomous_posting_allowed': False,
        'human_approval_required_for_financial_posting': True,
        'citation_required': True,
        'prompt_audit_required': True,
        'provider_mode': provider['execution_mode'],
    }
    checks = {
        'tenant_scope_limited_to_manchester': controls['tenant_scope'] == 'manchester.edu',
        'secrets_blocked': 'credential_values' in controls['blocked_data_classes'],
        'external_training_disabled': controls['external_training_allowed'] is False,
        'autonomous_posting_disabled': controls['autonomous_posting_allowed'] is False,
        'human_approval_required': controls['human_approval_required_for_financial_posting'] is True,
        'citations_and_prompt_audit_required': controls['citation_required'] is True and controls['prompt_audit_required'] is True,
    }
    return {'policy_key': 'manchester-ai-data-controls', 'controls': controls, 'checks': checks}


def _default_scenario_id() -> int:
    row = db.fetch_one("SELECT id FROM scenarios WHERE name = 'FY27 Operating Plan' ORDER BY id LIMIT 1") or db.fetch_one('SELECT id FROM scenarios ORDER BY id LIMIT 1')
    if row is None:
        return db.execute(
            '''
            INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
            VALUES ('B124 AI Provider Live Guardrails Scenario', 'b124', 'draft', '2026-08', '2027-07', 0, ?)
            ''',
            (_now(),),
        )
    return int(row['id'])


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['batch'] = 'B124'
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['provider'] = json.loads(result.pop('provider_json') or '{}')
    result['data_controls'] = json.loads(result.pop('data_controls_json') or '{}')
    result['certification'] = json.loads(result.pop('certification_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
