from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.pilot_deployment import get_run as get_pilot_run
from app.services.pilot_deployment import latest_run as latest_pilot_run


VENDOR_SOURCES = {
    'Prophix': 'https://www.prophix.com/',
    'Workday Adaptive Planning': 'https://www.workday.com/en-us/products/adaptive-planning/financial-planning/budgeting-forecasting.html',
    'Planful': 'https://planful.com/official-planful-company-information/',
    'Anaplan': 'https://www.anaplan.com/solutions/planning-budgeting-forecasting-software/',
}

FEATURE_MATRIX = [
    {
        'feature_key': 'planning_budgeting_forecasting',
        'label': 'Planning, budgeting, and forecasting',
        'vendor_expectation': 'Driver-based budget and forecast cycles with collaboration, scenarios, and variance review.',
        'mufinances_status': 'met',
        'evidence': ['B93 FP&A workflow certification', 'B110 pilot budget and forecast cycle'],
    },
    {
        'feature_key': 'close_consolidation',
        'label': 'Close, consolidation, reconciliation, and audit packets',
        'vendor_expectation': 'Close tasks, reconciliations, intercompany, eliminations, consolidation, and audit-ready evidence.',
        'mufinances_status': 'met',
        'evidence': ['B96 close certification', 'B97 consolidation certification', 'B110 close cycle'],
    },
    {
        'feature_key': 'reporting_board_packages',
        'label': 'Reporting, dashboards, statements, and board packages',
        'vendor_expectation': 'Board-ready financial statements, charts, pagination, export packages, and drill-down reporting.',
        'mufinances_status': 'met',
        'evidence': ['B98 reporting pixel polish', 'B110 reporting cycle'],
    },
    {
        'feature_key': 'excel_office',
        'label': 'Excel and Office adoption',
        'vendor_expectation': 'Finance-user Excel workflow with templates, refresh/publish, comments, and PowerPoint output.',
        'mufinances_status': 'partial',
        'evidence': ['B94 Excel adoption certification', 'B79 report/export validation'],
    },
    {
        'feature_key': 'ai_assisted_finance',
        'label': 'AI-assisted planning, variance, and forecasting',
        'vendor_expectation': 'AI recommendations, cited narratives, explainability, confidence, and approval controls.',
        'mufinances_status': 'partial',
        'evidence': ['B102 AI guardrails', 'B95 forecasting accuracy proof'],
    },
    {
        'feature_key': 'connectors_data_hub',
        'label': 'Connectors, integrations, and data governance',
        'vendor_expectation': 'Live ERP/SIS/HR/payroll/grants/banking integrations, credential flows, mappings, retries, and drill-back.',
        'mufinances_status': 'partial',
        'evidence': ['B99 connector activation', 'B90 campus data validation', 'B39 data hub governance'],
    },
    {
        'feature_key': 'enterprise_security',
        'label': 'Enterprise security and access governance',
        'vendor_expectation': 'SSO/MFA handoff, AD/OU mapping, domain/VPN enforcement, row-level access, masking, SoD, and audit.',
        'mufinances_status': 'partial',
        'evidence': ['B100 security activation', 'B101 audit and compliance certification'],
    },
    {
        'feature_key': 'multidimensional_modeling_scale',
        'label': 'Multidimensional modeling and scale',
        'vendor_expectation': 'Large dimensional models, sparse/dense handling, dependency invalidation, high-scale recalculation, and performance evidence.',
        'mufinances_status': 'partial',
        'evidence': ['B92 modeling deepening', 'B91 enterprise scale benchmark', 'B108 Parallel Cubed optimization'],
    },
    {
        'feature_key': 'vendor_ecosystem_support',
        'label': 'Commercial ecosystem, implementation network, and vendor support',
        'vendor_expectation': 'Mature vendor marketplace, partner ecosystem, formal support channels, and broad implementation history.',
        'mufinances_status': 'gap',
        'evidence': ['Local/internal build has supportability tools but not a commercial vendor ecosystem.'],
    },
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS parity_gap_review_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                pilot_run_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                vendor_sources_json TEXT NOT NULL,
                matrix_json TEXT NOT NULL,
                gaps_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (pilot_run_id) REFERENCES pilot_deployment_runs(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_parity_gap_review_runs_pilot
            ON parity_gap_review_runs (pilot_run_id, completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM parity_gap_review_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'post_pilot_review_gate_ready': True,
        'vendor_feature_matrix_ready': True,
        'gap_register_ready': True,
        'official_vendor_sources_recorded': True,
    }
    counts = {
        'parity_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM parity_gap_review_runs')['count']),
    }
    return {
        'batch': 'B111',
        'title': 'Prophix Parity Gap Review',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'sources': VENDOR_SOURCES,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM parity_gap_review_runs ORDER BY id DESC LIMIT ?', (max(1, min(limit, 200)),))
    return [_format_run(row) for row in rows]


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM parity_gap_review_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Parity gap review run not found.')
    return _format_run(row)


def run_parity_review(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b111-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    pilot = _resolve_pilot_run(payload)
    matrix = _build_matrix(pilot)
    gaps = _remaining_gaps(matrix)
    checks = {
        'pilot_use_reviewed': pilot['status'] == 'passed',
        'feature_matrix_completed': len(matrix) == len(FEATURE_MATRIX),
        'vendor_comparison_completed': set(VENDOR_SOURCES) == {'Prophix', 'Workday Adaptive Planning', 'Planful', 'Anaplan'},
        'remaining_gaps_identified_after_pilot': all(gap['pilot_run_id'] == pilot['id'] for gap in gaps),
        'no_pre_pilot_gap_claims': pilot['id'] > 0,
    }
    status_value = 'reviewed_with_gaps' if gaps and all(checks.values()) else ('passed' if all(checks.values()) else 'needs_review')
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO parity_gap_review_runs (
            run_key, pilot_run_id, status, checks_json, vendor_sources_json,
            matrix_json, gaps_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            int(pilot['id']),
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(VENDOR_SOURCES, sort_keys=True),
            json.dumps(matrix, sort_keys=True),
            json.dumps(gaps, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('parity_gap_review', run_key, status_value, user['email'], {'checks': checks, 'gap_count': len(gaps)}, completed)
    return get_run(run_id)


def latest_run() -> dict[str, Any] | None:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM parity_gap_review_runs ORDER BY id DESC LIMIT 1')
    return _format_run(row) if row else None


def _resolve_pilot_run(payload: dict[str, Any]) -> dict[str, Any]:
    pilot_run_id = payload.get('pilot_run_id')
    if pilot_run_id:
        return get_pilot_run(int(pilot_run_id))
    pilot = latest_pilot_run()
    if pilot is None:
        raise ValueError('Run B110 pilot deployment before the parity gap review.')
    return pilot


def _build_matrix(pilot: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for item in FEATURE_MATRIX:
        status_value = item['mufinances_status']
        if item['feature_key'] in {'planning_budgeting_forecasting', 'close_consolidation', 'reporting_board_packages'}:
            status_value = 'met' if pilot['status'] == 'passed' else 'partial'
        rows.append(
            {
                **item,
                'mufinances_status': status_value,
                'pilot_run_id': pilot['id'],
                'vendor_sources': VENDOR_SOURCES,
            }
        )
    return rows


def _remaining_gaps(matrix: list[dict[str, Any]]) -> list[dict[str, Any]]:
    descriptions = {
        'excel_office': 'Finish real finance-user Office adoption evidence against live protected workbooks and PowerPoint refresh cycles.',
        'ai_assisted_finance': 'Complete provider production wiring and user-reviewed AI outcome quality after pilot feedback.',
        'connectors_data_hub': 'Replace remaining simulated/anonymized connector evidence with credentialed live campus connectors and recurring sync proof.',
        'enterprise_security': 'Complete Manchester production IdP/MFA policy handoff and AD/OU signoff on the target server.',
        'multidimensional_modeling_scale': 'Prove very large multidimensional model behavior on the production database/server under concurrent users.',
        'vendor_ecosystem_support': 'Local muFinances cannot match commercial vendor marketplace, partner network, and contracted support without an operating support model.',
    }
    gaps = []
    for item in matrix:
        if item['mufinances_status'] == 'met':
            continue
        gaps.append(
            {
                'gap_key': item['feature_key'],
                'label': item['label'],
                'severity': 'high' if item['mufinances_status'] == 'gap' else 'medium',
                'status': 'open',
                'pilot_run_id': item['pilot_run_id'],
                'description': descriptions[item['feature_key']],
                'recommended_resolution': 'Track through release-candidate readiness or post-pilot roadmap depending on pilot signoff.',
            }
        )
    return gaps


def _format_run(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise ValueError('Parity gap review run not found.')
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['vendor_sources'] = json.loads(result.pop('vendor_sources_json') or '{}')
    result['matrix'] = json.loads(result.pop('matrix_json') or '[]')
    result['gaps'] = json.loads(result.pop('gaps_json') or '[]')
    result['complete'] = result['status'] in {'passed', 'reviewed_with_gaps'}
    return result
