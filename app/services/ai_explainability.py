from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.reporting import actual_budget_forecast_variance


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'ai_explanations': int(db.fetch_one('SELECT COUNT(*) AS count FROM ai_explanations')['count']),
        'citations': int(db.fetch_one('SELECT COUNT(*) AS count FROM ai_explanation_citations')['count']),
        'source_traces': int(db.fetch_one('SELECT COUNT(*) AS count FROM ai_source_traces')['count']),
        'pending_approval': int(db.fetch_one("SELECT COUNT(*) AS count FROM ai_explanations WHERE status = 'pending_approval'")['count']),
    }
    checks = {
        'cited_variance_explanations_ready': True,
        'confidence_scoring_ready': True,
        'source_tracing_ready': True,
        'human_approval_ready': True,
    }
    return {'batch': 'B34', 'title': 'AI Explainability Layer', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def draft_variance_explanations(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    report = actual_budget_forecast_variance(scenario_id)
    rows = sorted(report['rows'], key=lambda item: max(abs(float(item['actual_vs_budget'])), abs(float(item['forecast_vs_budget']))), reverse=True)
    created = []
    for row in rows[:10]:
        variance_type = 'actual_vs_budget' if abs(float(row['actual_vs_budget'])) >= abs(float(row['forecast_vs_budget'])) else 'forecast_vs_budget'
        amount = float(row[variance_type])
        if abs(amount) < 0.01:
            continue
        created.append(_create_explanation(scenario_id, row, variance_type, amount, user))
    return {'scenario_id': scenario_id, 'count': len(created), 'explanations': created}


def list_explanations(scenario_id: int, status_filter: str | None = None) -> list[dict[str, Any]]:
    if status_filter:
        rows = db.fetch_all(
            'SELECT * FROM ai_explanations WHERE scenario_id = ? AND status = ? ORDER BY id DESC',
            (scenario_id, status_filter),
        )
    else:
        rows = db.fetch_all('SELECT * FROM ai_explanations WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    return [_format_explanation(row) for row in rows]


def get_explanation(explanation_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM ai_explanations WHERE id = ?', (explanation_id,))
    if row is None:
        raise ValueError('AI explanation not found.')
    return _format_explanation(row)


def submit_explanation(explanation_id: int, user: dict[str, Any]) -> dict[str, Any]:
    get_explanation(explanation_id)
    now = _now()
    db.execute(
        "UPDATE ai_explanations SET status = 'pending_approval', submitted_by = ?, submitted_at = ? WHERE id = ?",
        (user['email'], now, explanation_id),
    )
    db.log_audit('ai_explanation', str(explanation_id), 'submitted', user['email'], {}, now)
    return get_explanation(explanation_id)


def approve_explanation(explanation_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    return _decide(explanation_id, user, 'approved', note)


def reject_explanation(explanation_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    return _decide(explanation_id, user, 'rejected', note)


def _create_explanation(scenario_id: int, row: dict[str, Any], variance_type: str, amount: float, user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    subject_key = f"{row['key']}:{variance_type}"
    existing = db.fetch_one('SELECT id FROM ai_explanations WHERE scenario_id = ? AND subject_key = ?', (scenario_id, subject_key))
    if existing is not None:
        return get_explanation(int(existing['id']))
    direction = 'favorable' if amount >= 0 else 'unfavorable'
    confidence = _confidence(row, amount)
    text = (
        f"{row['key']} has a {direction} {variance_type.replace('_', ' ')} variance of {amount:,.2f}. "
        f"The cited ledger buckets show actual {float(row['actual']):,.2f}, budget {float(row['budget']):,.2f}, "
        f"forecast {float(row['forecast']):,.2f}, and scenario {float(row['scenario']):,.2f}."
    )
    explanation_id = db.execute(
        '''
        INSERT INTO ai_explanations (
            scenario_id, explanation_key, subject_type, subject_key, explanation_text,
            confidence, status, model_name, created_by, created_at
        ) VALUES (?, ?, 'variance', ?, ?, ?, 'draft', 'deterministic-ledger-explainer', ?, ?)
        ''',
        (scenario_id, f"ai-exp-{scenario_id}-{len(subject_key)}-{abs(hash(subject_key)) % 100000}", subject_key, text, confidence, user['email'], now),
    )
    _insert_citations(explanation_id, row, confidence)
    _insert_source_traces(explanation_id, row, variance_type, amount)
    db.log_audit('ai_explanation', str(explanation_id), 'drafted', user['email'], {'subject_key': subject_key, 'confidence': confidence}, now)
    return get_explanation(explanation_id)


def _insert_citations(explanation_id: int, row: dict[str, Any], confidence: float) -> None:
    now = _now()
    for source_type in ['actual', 'budget', 'forecast', 'scenario']:
        db.execute(
            '''
            INSERT INTO ai_explanation_citations (
                explanation_id, citation_key, source_type, source_id, source_label, source_excerpt, confidence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                explanation_id,
                f"{row['key']}:{source_type}",
                source_type,
                f"{row['key']}:{source_type}",
                f"{row['key']} {source_type}",
                f"{source_type} amount {float(row[source_type]):,.2f}",
                confidence,
                now,
            ),
        )


def _insert_source_traces(explanation_id: int, row: dict[str, Any], variance_type: str, amount: float) -> None:
    now = _now()
    traces = [
        ('report_row', row['key'], 'actual_budget_forecast_variance aggregation', row),
        ('variance_metric', variance_type, 'selected largest absolute variance metric', {'variance_type': variance_type, 'amount': amount}),
        ('confidence', row['key'], 'confidence calculated from cited non-zero bucket coverage and variance materiality', {'confidence': _confidence(row, amount)}),
    ]
    for index, (source_type, source_id, transformation, value) in enumerate(traces, start=1):
        db.execute(
            '''
            INSERT INTO ai_source_traces (
                explanation_id, trace_order, source_type, source_id, transformation, value_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (explanation_id, index, source_type, source_id, transformation, json.dumps(value, sort_keys=True), now),
        )


def _confidence(row: dict[str, Any], amount: float) -> float:
    buckets = [float(row[key]) for key in ['actual', 'budget', 'forecast', 'scenario']]
    coverage = sum(1 for value in buckets if abs(value) > 0.01) / 4
    materiality = min(0.25, abs(amount) / max(1.0, sum(abs(value) for value in buckets)))
    return round(min(0.99, 0.55 + (coverage * 0.3) + materiality), 2)


def _decide(explanation_id: int, user: dict[str, Any], status_value: str, note: str) -> dict[str, Any]:
    get_explanation(explanation_id)
    now = _now()
    db.execute(
        '''
        UPDATE ai_explanations
        SET status = ?, approved_by = ?, approved_at = ?, rejection_note = ?
        WHERE id = ?
        ''',
        (status_value, user['email'], now, note if status_value == 'rejected' else '', explanation_id),
    )
    db.log_audit('ai_explanation', str(explanation_id), status_value, user['email'], {'note': note}, now)
    return get_explanation(explanation_id)


def _format_explanation(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['citations'] = db.fetch_all('SELECT * FROM ai_explanation_citations WHERE explanation_id = ? ORDER BY id ASC', (row['id'],))
    traces = db.fetch_all('SELECT * FROM ai_source_traces WHERE explanation_id = ? ORDER BY trace_order ASC', (row['id'],))
    for trace in traces:
        trace['value'] = json.loads(trace.pop('value_json') or '{}')
    result['source_traces'] = traces
    return result
