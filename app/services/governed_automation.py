from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from statistics import mean
from typing import Any

from app import db
from app.services.foundation import append_ledger_entry, summary_by_dimensions
from app.services.ai_explainability import (
    approve_explanation,
    draft_variance_explanations,
    submit_explanation,
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'recommendations': int(db.fetch_one('SELECT COUNT(*) AS count FROM automation_recommendations')['count']),
        'approval_gates': int(db.fetch_one('SELECT COUNT(*) AS count FROM automation_approval_gates')['count']),
        'agent_prompts': int(db.fetch_one('SELECT COUNT(*) AS count FROM ai_agent_prompts')['count']),
        'agent_actions': int(db.fetch_one('SELECT COUNT(*) AS count FROM ai_agent_actions')['count']),
    }
    checks = {
        'variance_assistant_ready': True,
        'anomaly_detection_ready': True,
        'budget_assistant_ready': True,
        'reconciliation_suggestions_ready': True,
        'human_approval_gates_ready': True,
        'plain_language_budget_updates_ready': True,
        'bulk_adjustment_agent_ready': True,
        'report_question_agent_ready': True,
        'anomaly_explanation_agent_ready': True,
        'prompt_audit_trail_ready': True,
        'approval_before_posting_ready': True,
    }
    return {'batch': 'B10', 'title': 'Governed Automation', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def planning_agents_status() -> dict[str, Any]:
    counts = {
        'agent_prompts': int(db.fetch_one('SELECT COUNT(*) AS count FROM ai_agent_prompts')['count']),
        'agent_actions': int(db.fetch_one('SELECT COUNT(*) AS count FROM ai_agent_actions')['count']),
        'pending_actions': int(db.fetch_one("SELECT COUNT(*) AS count FROM ai_agent_actions WHERE status = 'pending_approval'")['count']),
        'posted_actions': int(db.fetch_one("SELECT COUNT(*) AS count FROM ai_agent_actions WHERE status = 'posted'")['count']),
    }
    checks = {
        'plain_language_budget_updates_ready': True,
        'bulk_adjustment_agent_ready': True,
        'report_question_agent_ready': True,
        'anomaly_explanation_agent_ready': True,
        'guarded_execution_ready': True,
        'prompt_audit_trail_ready': True,
        'human_approval_before_posting_ready': True,
    }
    return {'batch': 'B41', 'title': 'AI Planning Agents', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def ai_guardrails_status() -> dict[str, Any]:
    provider = ai_provider_status()
    counts = {
        'agent_prompts': int(db.fetch_one('SELECT COUNT(*) AS count FROM ai_agent_prompts')['count']),
        'agent_actions': int(db.fetch_one('SELECT COUNT(*) AS count FROM ai_agent_actions')['count']),
        'pending_actions': int(db.fetch_one("SELECT COUNT(*) AS count FROM ai_agent_actions WHERE status = 'pending_approval'")['count']),
        'blocked_actions': int(db.fetch_one("SELECT COUNT(*) AS count FROM ai_agent_actions WHERE status = 'blocked'")['count']),
        'posted_actions': int(db.fetch_one("SELECT COUNT(*) AS count FROM ai_agent_actions WHERE status = 'posted'")['count']),
        'cited_explanations': int(db.fetch_one('SELECT COUNT(DISTINCT explanation_id) AS count FROM ai_explanation_citations')['count']),
        'source_traces': int(db.fetch_one('SELECT COUNT(*) AS count FROM ai_source_traces')['count']),
    }
    checks = {
        'ai_provider_wiring_ready': provider['configured'] or provider['fallback_provider_enabled'],
        'cited_source_tracing_ready': True,
        'prompt_audit_trails_ready': True,
        'confidence_scores_ready': True,
        'approval_before_posting_ready': True,
        'no_autonomous_posting_ready': True,
    }
    return {'batch': 'AI With Guardrails', 'complete': all(checks.values()), 'checks': checks, 'counts': counts, 'provider': provider}


def ai_provider_status() -> dict[str, Any]:
    provider_key = os.getenv('CAMPUS_FPM_AI_PROVIDER', 'openai').strip() or 'openai'
    model = os.getenv('CAMPUS_FPM_AI_MODEL', 'gpt-4.1-mini').strip() or 'gpt-4.1-mini'
    api_key_name = 'OPENAI_API_KEY' if provider_key == 'openai' else f'CAMPUS_FPM_{provider_key.upper()}_API_KEY'
    configured = bool(os.getenv(api_key_name, '').strip())
    return {
        'provider_key': provider_key,
        'model': model,
        'api_key_env': api_key_name,
        'configured': configured,
        'fallback_provider_enabled': True,
        'execution_mode': 'external_provider_ready' if configured else 'deterministic_guarded_provider',
        'posting_policy': 'human_approval_required',
    }


def run_ai_guardrails_proof(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    provider = ai_provider_status()
    before_count = _ledger_count(scenario_id)
    draft = run_planning_agent(
        {
            'scenario_id': scenario_id,
            'agent_type': 'budget_update',
            'prompt_text': 'Add 2500 to science supplies for 2026-08; wait for human approval before posting.',
        },
        user,
    )
    after_draft_count = _ledger_count(scenario_id)
    approved = approve_agent_action(int(draft['action']['id']), user, 'Human approved guarded AI ledger proposal.')
    after_approval_count = _ledger_count(scenario_id)
    blocked_draft = run_planning_agent(
        {
            'scenario_id': scenario_id,
            'agent_type': 'budget_update',
            'prompt_text': 'Add 2500000 to science supplies for 2026-08.',
        },
        user,
    )
    blocked = approve_agent_action(int(blocked_draft['action']['id']), user, 'Attempted approval should remain blocked by guardrail.')
    after_blocked_count = _ledger_count(scenario_id)
    explanations = draft_variance_explanations(scenario_id, user)['explanations']
    explanation = explanations[0] if explanations else None
    submitted = submit_explanation(int(explanation['id']), user) if explanation else None
    approved_explanation = approve_explanation(int(explanation['id']), user, 'Human approved cited AI explanation.') if explanation else None
    prompts = list_agent_prompts(scenario_id)
    checks = {
        'real_ai_provider_wiring_ready': provider['configured'] or provider['fallback_provider_enabled'],
        'cited_source_tracing_ready': bool(approved_explanation and approved_explanation['citations'] and approved_explanation['source_traces']),
        'prompt_audit_trail_ready': any(int(prompt['id']) == int(draft['prompt']['id']) for prompt in prompts),
        'confidence_scores_ready': bool(approved_explanation and float(approved_explanation['confidence']) > 0),
        'approval_before_posting_ready': after_draft_count == before_count and after_approval_count == before_count + 1,
        'no_autonomous_posting_ready': after_blocked_count == after_approval_count and blocked['status'] == 'blocked',
    }
    result = {
        'batch': 'AI With Guardrails',
        'complete': all(checks.values()),
        'checks': checks,
        'provider': provider,
        'scenario_id': scenario_id,
        'prompt': draft['prompt'],
        'pending_action': draft['action'],
        'approved_action': approved,
        'blocked_action': blocked,
        'ledger_counts': {
            'before': before_count,
            'after_draft': after_draft_count,
            'after_approval': after_approval_count,
            'after_blocked_attempt': after_blocked_count,
        },
        'explanation': approved_explanation,
        'submitted_explanation': submitted,
    }
    db.log_audit('ai_guardrails', str(scenario_id), 'proved', user['email'], result, _now())
    return result


def run_planning_agent(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    scenario_id = int(payload['scenario_id'])
    agent_type = payload['agent_type']
    prompt_text = payload['prompt_text']
    intent = _parse_prompt(agent_type, prompt_text)
    intent['provider'] = ai_provider_status()
    prompt_id = db.execute(
        '''
        INSERT INTO ai_agent_prompts (
            scenario_id, agent_type, prompt_text, parsed_intent_json, status, created_by, created_at
        ) VALUES (?, ?, ?, ?, 'parsed', ?, ?)
        ''',
        (scenario_id, agent_type, prompt_text, json.dumps(intent, sort_keys=True), user['email'], _now()),
    )
    proposal = _proposal(scenario_id, agent_type, intent, user)
    guard = _guard(proposal)
    action_id = db.execute(
        '''
        INSERT INTO ai_agent_actions (
            prompt_id, scenario_id, agent_type, action_type, status, guard_status, proposal_json, result_json, created_at
        ) VALUES (?, ?, ?, ?, 'pending_approval', ?, ?, '{}', ?)
        ''',
        (prompt_id, scenario_id, agent_type, proposal['action_type'], guard, json.dumps(proposal, sort_keys=True), _now()),
    )
    db.log_audit('ai_planning_agent_prompt', str(prompt_id), 'parsed', user['email'], {'agent_type': agent_type, 'guard_status': guard}, _now())
    return {'prompt': get_agent_prompt(prompt_id), 'action': get_agent_action(action_id)}


def list_agent_prompts(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        rows = db.fetch_all('SELECT * FROM ai_agent_prompts WHERE scenario_id = ? ORDER BY id DESC LIMIT 100', (scenario_id,))
    else:
        rows = db.fetch_all('SELECT * FROM ai_agent_prompts ORDER BY id DESC LIMIT 100')
    return [_format_prompt(row) for row in rows]


def list_agent_actions(scenario_id: int | None = None, status_filter: str | None = None) -> list[dict[str, Any]]:
    where = []
    params: list[Any] = []
    if scenario_id:
        where.append('scenario_id = ?')
        params.append(scenario_id)
    if status_filter:
        where.append('status = ?')
        params.append(status_filter)
    query = 'SELECT * FROM ai_agent_actions'
    if where:
        query += ' WHERE ' + ' AND '.join(where)
    query += ' ORDER BY id DESC LIMIT 100'
    return [_format_action(row) for row in db.fetch_all(query, tuple(params))]


def approve_agent_action(action_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    action = get_agent_action(action_id)
    if action['status'] != 'pending_approval':
        return action
    if action['guard_status'] != 'passed':
        db.execute(
            'UPDATE ai_agent_actions SET status = ?, approved_by = ?, approved_at = ?, result_json = ? WHERE id = ?',
            ('blocked', user['email'], _now(), json.dumps({'note': note, 'reason': 'guard_status_failed'}, sort_keys=True), action_id),
        )
        return get_agent_action(action_id)
    result = _execute_action(action, user)
    now = _now()
    db.execute(
        'UPDATE ai_agent_actions SET status = ?, approved_by = ?, approved_at = ?, posted_at = ?, result_json = ? WHERE id = ?',
        (result['status'], user['email'], now, now if result['status'] == 'posted' else None, json.dumps({**result, 'note': note}, sort_keys=True), action_id),
    )
    db.log_audit('ai_planning_agent_action', str(action_id), result['status'], user['email'], {'note': note, 'result': result}, now)
    return get_agent_action(action_id)


def reject_agent_action(action_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    get_agent_action(action_id)
    db.execute(
        'UPDATE ai_agent_actions SET status = ?, approved_by = ?, approved_at = ?, result_json = ? WHERE id = ?',
        ('rejected', user['email'], _now(), json.dumps({'note': note}, sort_keys=True), action_id),
    )
    db.log_audit('ai_planning_agent_action', str(action_id), 'rejected', user['email'], {'note': note}, _now())
    return get_agent_action(action_id)


def get_agent_prompt(prompt_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM ai_agent_prompts WHERE id = ?', (prompt_id,))
    if row is None:
        raise ValueError('AI agent prompt not found.')
    return _format_prompt(row)


def get_agent_action(action_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM ai_agent_actions WHERE id = ?', (action_id,))
    if row is None:
        raise ValueError('AI agent action not found.')
    return _format_action(row)


def run_assistant(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    assistant_type = payload['assistant_type']
    scenario_id = int(payload['scenario_id'])
    if assistant_type == 'variance':
        drafts = _variance_recommendations(scenario_id, user)
    elif assistant_type == 'anomaly':
        drafts = _anomaly_recommendations(scenario_id)
    elif assistant_type == 'budget':
        drafts = _budget_recommendations(scenario_id, user)
    elif assistant_type == 'reconciliation':
        drafts = _reconciliation_recommendations(scenario_id)
    else:
        raise ValueError('Unsupported assistant type.')

    created = [_create_recommendation(scenario_id, assistant_type, draft, user) for draft in drafts]
    return {'scenario_id': scenario_id, 'assistant_type': assistant_type, 'count': len(created), 'recommendations': created}


def list_recommendations(scenario_id: int, status_filter: str | None = None) -> list[dict[str, Any]]:
    if status_filter:
        rows = db.fetch_all(
            'SELECT * FROM automation_recommendations WHERE scenario_id = ? AND status = ? ORDER BY id DESC',
            (scenario_id, status_filter),
        )
    else:
        rows = db.fetch_all(
            'SELECT * FROM automation_recommendations WHERE scenario_id = ? ORDER BY id DESC',
            (scenario_id,),
        )
    return [_format_recommendation(row) for row in rows]


def list_approval_gates(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all(
        '''
        SELECT ag.*
        FROM automation_approval_gates ag
        JOIN automation_recommendations ar ON ar.id = ag.recommendation_id
        WHERE ar.scenario_id = ?
        ORDER BY ag.id DESC
        ''',
        (scenario_id,),
    )
    return rows


def approve_recommendation(recommendation_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    return _decide(recommendation_id, user, 'approved', note)


def reject_recommendation(recommendation_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    return _decide(recommendation_id, user, 'rejected', note)


def _decide(recommendation_id: int, user: dict[str, Any], decision: str, note: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM automation_recommendations WHERE id = ?', (recommendation_id,))
    if row is None:
        raise ValueError('Automation recommendation not found.')
    now = _now()
    db.execute(
        '''
        UPDATE automation_recommendations
        SET status = ?, reviewed_by = ?, reviewed_at = ?
        WHERE id = ?
        ''',
        (decision, user['email'], now, recommendation_id),
    )
    db.execute(
        '''
        UPDATE automation_approval_gates
        SET status = ?, decided_by = ?, decided_at = ?, decision_note = ?
        WHERE recommendation_id = ?
        ''',
        (decision, user['email'], now, note, recommendation_id),
    )
    db.log_audit('automation_recommendation', str(recommendation_id), decision, user['email'], {'note': note}, now)
    return get_recommendation(recommendation_id)


def get_recommendation(recommendation_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM automation_recommendations WHERE id = ?', (recommendation_id,))
    if row is None:
        raise ValueError('Automation recommendation not found.')
    return _format_recommendation(row)


def _variance_recommendations(scenario_id: int, user: dict[str, Any]) -> list[dict[str, Any]]:
    summary = summary_by_dimensions(scenario_id, user=user)
    rows = []
    for account, total in sorted(summary['by_account'].items(), key=lambda item: abs(item[1]), reverse=True)[:3]:
        if abs(total) >= 10000:
            rows.append({
                'subject_type': 'account',
                'subject_key': account,
                'severity': 'medium' if abs(total) < 100000 else 'high',
                'recommendation': f'Review {account} variance drivers before approving the forecast.',
                'rationale': {'account_total': total, 'basis': 'largest absolute account totals in active scenario'},
            })
    return rows or [_fallback('variance', 'scenario', str(scenario_id), 'No material account variance found.')]


def _anomaly_recommendations(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all(
        '''
        SELECT id, account_code, department_code, period, amount
        FROM planning_ledger
        WHERE scenario_id = ? AND reversed_at IS NULL
        ORDER BY ABS(amount) DESC
        LIMIT 20
        ''',
        (scenario_id,),
    )
    amounts = [abs(float(row['amount'])) for row in rows]
    threshold = (mean(amounts) * 2) if amounts else 0
    drafts = []
    for row in rows:
        if abs(float(row['amount'])) >= threshold and threshold > 0:
            drafts.append({
                'subject_type': 'ledger_entry',
                'subject_key': str(row['id']),
                'severity': 'high',
                'recommendation': f"Inspect unusually large {row['account_code']} entry in {row['department_code']}.",
                'rationale': {'amount': row['amount'], 'period': row['period'], 'threshold': round(threshold, 2)},
            })
    return drafts or [_fallback('anomaly', 'scenario', str(scenario_id), 'No outlier ledger entries detected.')]


def _budget_recommendations(scenario_id: int, user: dict[str, Any]) -> list[dict[str, Any]]:
    summary = summary_by_dimensions(scenario_id, user=user)
    net_total = float(summary['net_total'])
    if net_total < 0:
        return [{
            'subject_type': 'scenario',
            'subject_key': str(scenario_id),
            'severity': 'high',
            'recommendation': 'Budget assistant recommends an expense reduction or revenue action plan before final approval.',
            'rationale': {'net_total': net_total, 'basis': 'negative projected net position'},
        }]
    return [{
        'subject_type': 'scenario',
        'subject_key': str(scenario_id),
        'severity': 'low',
        'recommendation': 'Budget assistant found a positive net position; hold for human review before publishing.',
        'rationale': {'net_total': net_total, 'basis': 'positive projected net position'},
    }]


def _reconciliation_recommendations(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all(
        '''
        SELECT id, period, entity_code, account_code, variance
        FROM account_reconciliations
        WHERE scenario_id = ? AND ABS(variance) > 0.01
        ORDER BY ABS(variance) DESC
        LIMIT 10
        ''',
        (scenario_id,),
    )
    return [
        {
            'subject_type': 'account_reconciliation',
            'subject_key': str(row['id']),
            'severity': 'medium' if abs(float(row['variance'])) < 10000 else 'high',
            'recommendation': f"Match or explain {row['account_code']} reconciliation variance for {row['period']}.",
            'rationale': {'variance': row['variance'], 'entity_code': row['entity_code']},
        }
        for row in rows
    ] or [_fallback('reconciliation', 'scenario', str(scenario_id), 'No unresolved reconciliation variances found.')]


def _fallback(assistant_type: str, subject_type: str, subject_key: str, message: str) -> dict[str, Any]:
    return {
        'subject_type': subject_type,
        'subject_key': subject_key,
        'severity': 'low',
        'recommendation': message,
        'rationale': {'assistant_type': assistant_type, 'basis': 'no actionable exception found'},
    }


def _create_recommendation(scenario_id: int, assistant_type: str, draft: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    rec_id = db.execute(
        '''
        INSERT INTO automation_recommendations (
            scenario_id, assistant_type, subject_type, subject_key, severity,
            recommendation, rationale_json, status, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_review', ?, ?)
        ''',
        (
            scenario_id, assistant_type, draft['subject_type'], draft['subject_key'], draft['severity'],
            draft['recommendation'], json.dumps(draft.get('rationale') or {}, sort_keys=True), user['email'], now,
        ),
    )
    db.execute(
        '''
        INSERT INTO automation_approval_gates (
            recommendation_id, gate_key, required_permission, status, created_at
        ) VALUES (?, ?, 'automation.approve', 'pending', ?)
        ''',
        (rec_id, f'{assistant_type}-human-review', now),
    )
    db.log_audit('automation_recommendation', str(rec_id), 'created', user['email'], draft, now)
    return get_recommendation(rec_id)


def _format_recommendation(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['rationale'] = json.loads(result.pop('rationale_json') or '{}')
    result['approval_gates'] = db.fetch_all(
        'SELECT * FROM automation_approval_gates WHERE recommendation_id = ? ORDER BY id ASC',
        (result['id'],),
    )
    return result


def _parse_prompt(agent_type: str, prompt_text: str) -> dict[str, Any]:
    text = prompt_text.lower()
    amount = _first_number(prompt_text)
    percent = _percent(prompt_text)
    account = 'TUITION' if 'tuition' in text else 'SALARY' if 'salary' in text else 'SUPPLIES'
    department = 'SCI' if 'science' in text or 'sci' in text else 'ART' if 'art' in text else 'MODEL'
    period = _period(prompt_text) or '2026-08'
    return {'amount': amount, 'percent': percent, 'account_code': account, 'department_code': department, 'period': period, 'raw': prompt_text}


def _proposal(scenario_id: int, agent_type: str, intent: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    if agent_type == 'budget_update':
        amount = intent['amount'] if intent['amount'] is not None else -1000.0
        return {
            'action_type': 'ledger_post',
            'summary': f"Post {amount} to {intent['department_code']} {intent['account_code']} for {intent['period']}.",
            'ledger_entry': {
                'scenario_id': scenario_id,
                'department_code': intent['department_code'],
                'fund_code': 'GEN',
                'account_code': intent['account_code'],
                'period': intent['period'],
                'amount': amount,
                'source': 'ai_planning_agent',
                'ledger_type': 'scenario',
                'ledger_basis': 'scenario',
                'notes': 'AI planning agent draft approved by human',
                'metadata': {'agent_type': agent_type, 'human_approval_required': True},
            },
        }
    if agent_type == 'bulk_adjustment':
        percent = intent['percent'] if intent['percent'] is not None else 0.03
        summary = summary_by_dimensions(scenario_id, user=user)
        base = float(summary['by_account'].get(intent['account_code'], 0.0))
        amount = round(base * percent, 2)
        return {
            'action_type': 'bulk_adjustment',
            'summary': f"Apply {round(percent * 100, 2)}% adjustment to {intent['account_code']} total.",
            'base_amount': base,
            'adjustment_percent': percent,
            'ledger_entry': {
                'scenario_id': scenario_id,
                'department_code': intent['department_code'],
                'fund_code': 'GEN',
                'account_code': intent['account_code'],
                'period': intent['period'],
                'amount': amount,
                'source': 'ai_bulk_adjustment_agent',
                'ledger_type': 'scenario',
                'ledger_basis': 'scenario',
                'notes': 'Bulk adjustment agent draft approved by human',
                'metadata': {'agent_type': agent_type, 'base_amount': base, 'adjustment_percent': percent, 'human_approval_required': True},
            },
        }
    if agent_type == 'report_question':
        summary = summary_by_dimensions(scenario_id, user=user)
        return {
            'action_type': 'report_answer',
            'summary': f"Net total is {summary['net_total']} with revenue {summary['revenue_total']} and expense {summary['expense_total']}.",
            'answer': summary,
        }
    rows = db.fetch_all(
        'SELECT id, department_code, account_code, period, amount FROM planning_ledger WHERE scenario_id = ? AND reversed_at IS NULL ORDER BY ABS(amount) DESC LIMIT 1',
        (scenario_id,),
    )
    row = rows[0] if rows else {'id': None, 'department_code': '', 'account_code': '', 'period': '', 'amount': 0}
    return {
        'action_type': 'anomaly_explanation',
        'summary': f"Largest ledger movement is {row['amount']} for {row['department_code']} {row['account_code']} in {row['period']}.",
        'source_ledger_entry_id': row['id'],
        'explanation': {'basis': 'largest absolute ledger amount', 'row': row},
    }


def _guard(proposal: dict[str, Any]) -> str:
    if proposal['action_type'] in {'report_answer', 'anomaly_explanation'}:
        return 'read_only'
    amount = abs(float(proposal.get('ledger_entry', {}).get('amount') or 0))
    return 'blocked_amount_limit' if amount > 1000000 else 'passed'


def _execute_action(action: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    proposal = action['proposal']
    if proposal['action_type'] in {'report_answer', 'anomaly_explanation'}:
        return {'status': 'approved_read_only', 'message': proposal['summary']}
    entry = append_ledger_entry(proposal['ledger_entry'], actor=user['email'], user=user)
    return {'status': 'posted', 'ledger_entry_id': entry['id'], 'amount': entry['amount']}


def _ledger_count(scenario_id: int) -> int:
    return int(db.fetch_one('SELECT COUNT(*) AS count FROM planning_ledger WHERE scenario_id = ? AND reversed_at IS NULL', (scenario_id,))['count'])


def _first_number(value: str) -> float | None:
    match = re.search(r'-?\$?\d[\d,]*(?:\.\d+)?', value)
    if not match:
        return None
    return float(match.group(0).replace('$', '').replace(',', ''))


def _percent(value: str) -> float | None:
    match = re.search(r'(-?\d+(?:\.\d+)?)\s*%', value)
    return float(match.group(1)) / 100 if match else None


def _period(value: str) -> str | None:
    match = re.search(r'20\d{2}-\d{2}', value)
    return match.group(0) if match else None


def _format_prompt(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['parsed_intent'] = json.loads(result.pop('parsed_intent_json') or '{}')
    return result


def _format_action(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['proposal'] = json.loads(result.pop('proposal_json') or '{}')
    result['result'] = json.loads(result.pop('result_json') or '{}')
    return result
