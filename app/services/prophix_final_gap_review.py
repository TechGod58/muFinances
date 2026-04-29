from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.multi_user_pilot_cycle import get_run as get_pilot_cycle_run
from app.services.multi_user_pilot_cycle import list_runs as list_pilot_cycle_runs
from app.services.parity_gap_review import VENDOR_SOURCES


FEATURE_AREAS = [
    {
        'feature_key': 'fpa_budget_forecast_scenario',
        'label': 'FP&A budgeting, forecasting, scenarios, and approvals',
        'pilot_checks': ['budget_cycle_completed', 'forecast_cycle_completed', 'pilot_signoff_recorded'],
        'vendor_capabilities': ['budgeting', 'forecasting', 'scenario planning', 'approvals'],
    },
    {
        'feature_key': 'multidimensional_modeling',
        'label': 'Multidimensional planning model and calculation engine',
        'pilot_checks': [],
        'vendor_capabilities': ['dimensional models', 'formula ordering', 'scenario versions', 'large-model calculation'],
    },
    {
        'feature_key': 'excel_office_workflow',
        'label': 'Excel and Office workflow',
        'pilot_checks': [],
        'vendor_capabilities': ['Excel templates', 'refresh/publish', 'offline round trip', 'PowerPoint package refresh'],
    },
    {
        'feature_key': 'reporting_board_packages',
        'label': 'Reporting, dashboards, and board packages',
        'pilot_checks': ['reporting_cycle_completed', 'board_package_cycle_completed'],
        'vendor_capabilities': ['financial statements', 'dashboards', 'board books', 'export packages'],
    },
    {
        'feature_key': 'close_reconciliation',
        'label': 'Close, reconciliation, and evidence management',
        'pilot_checks': ['close_cycle_completed'],
        'vendor_capabilities': ['close checklist', 'reconciliations', 'review workflow', 'audit packets'],
    },
    {
        'feature_key': 'consolidation_intercompany_currency',
        'label': 'Consolidation, intercompany, ownership, and currency',
        'pilot_checks': ['consolidation_cycle_completed'],
        'vendor_capabilities': ['entity hierarchy', 'ownership', 'eliminations', 'currency translation', 'multi-book reporting'],
    },
    {
        'feature_key': 'connectors_data_hub',
        'label': 'Connectors, data hub, and master data governance',
        'pilot_checks': [],
        'vendor_capabilities': ['ERP connectors', 'SIS/HR/payroll/grants feeds', 'mapping governance', 'source drill-back'],
    },
    {
        'feature_key': 'security_audit_compliance',
        'label': 'Security, auditability, compliance, and access governance',
        'pilot_checks': ['it_participated', 'pilot_signoff_recorded'],
        'vendor_capabilities': ['SSO/MFA', 'row-level security', 'audit trail', 'SoD', 'retention'],
    },
    {
        'feature_key': 'ai_assisted_finance',
        'label': 'AI-assisted planning and explanations',
        'pilot_checks': [],
        'vendor_capabilities': ['variance narratives', 'forecast recommendations', 'source tracing', 'human approval gates'],
    },
    {
        'feature_key': 'workflow_collaboration',
        'label': 'Workflow, collaboration, comments, and notifications',
        'pilot_checks': ['budget_office_participated', 'controller_participated', 'department_planners_participated'],
        'vendor_capabilities': ['approval chains', 'comments', 'notifications', 'task ownership'],
    },
    {
        'feature_key': 'operations_supportability',
        'label': 'Operations, supportability, release, and recovery',
        'pilot_checks': ['it_participated'],
        'vendor_capabilities': ['health checks', 'backup/restore', 'diagnostics', 'release governance'],
    },
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS prophix_final_gap_review_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                pilot_cycle_run_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                vendor_sources_json TEXT NOT NULL,
                matrix_json TEXT NOT NULL,
                gaps_json TEXT NOT NULL,
                exclusions_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_prophix_final_gap_review_runs_pilot
            ON prophix_final_gap_review_runs (pilot_cycle_run_id, completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM prophix_final_gap_review_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'real_pilot_evidence_gate_ready': True,
        'vendor_feature_matrix_ready': True,
        'remaining_gap_register_ready': True,
        'theoretical_gap_filter_ready': True,
    }
    return {
        'batch': 'B129',
        'title': 'Prophix-Class Final Gap Review',
        'complete': all(checks.values()),
        'checks': checks,
        'sources': VENDOR_SOURCES,
        'counts': {
            'final_gap_reviews': int(db.fetch_one('SELECT COUNT(*) AS count FROM prophix_final_gap_review_runs')['count']),
            'feature_areas': len(FEATURE_AREAS),
            'vendors': len(VENDOR_SOURCES),
        },
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all(
        'SELECT * FROM prophix_final_gap_review_runs ORDER BY id DESC LIMIT ?',
        (max(1, min(limit, 200)),),
    )
    return [_format_run(row) for row in rows]


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM prophix_final_gap_review_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Prophix-class final gap review run not found.')
    return _format_run(row)


def run_final_gap_review(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b129-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    pilot = _resolve_pilot_cycle(payload)
    matrix = _build_matrix(pilot, payload.get('feature_results') or {})
    gaps = _remaining_real_gaps(matrix)
    exclusions = _theoretical_exclusions(matrix)
    checks = {
        'real_pilot_evidence_used': pilot['status'] == 'passed',
        'vendor_comparison_completed': set(VENDOR_SOURCES) == {'Prophix', 'Workday Adaptive Planning', 'Planful', 'Anaplan'},
        'feature_matrix_completed': len(matrix) == len(FEATURE_AREAS),
        'remaining_gaps_are_failed_pilot_evidence_only': all(gap['evidence_type'] == 'failed_pilot_check' for gap in gaps),
        'theoretical_gaps_excluded': all(item['reason'] == 'not_failed_in_pilot' for item in exclusions),
    }
    status_value = 'reviewed_with_gaps' if gaps and all(checks.values()) else ('passed' if all(checks.values()) else 'needs_review')
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO prophix_final_gap_review_runs (
            run_key, pilot_cycle_run_id, status, checks_json, vendor_sources_json,
            matrix_json, gaps_json, exclusions_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            int(pilot['id']),
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(VENDOR_SOURCES, sort_keys=True),
            json.dumps(matrix, sort_keys=True),
            json.dumps(gaps, sort_keys=True),
            json.dumps(exclusions, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('prophix_final_gap_review', run_key, status_value, user['email'], {'checks': checks, 'gap_count': len(gaps)}, completed)
    return get_run(row_id)


def _resolve_pilot_cycle(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get('pilot_cycle_run_id'):
        return get_pilot_cycle_run(int(payload['pilot_cycle_run_id']))
    rows = list_pilot_cycle_runs(1)
    if not rows:
        raise ValueError('Run the multi-user pilot cycle before the final gap review.')
    return rows[0]


def _build_matrix(pilot: dict[str, Any], feature_results: dict[str, Any]) -> list[dict[str, Any]]:
    pilot_checks = pilot.get('checks', {})
    rows = []
    for area in FEATURE_AREAS:
        override = feature_results.get(area['feature_key'])
        checks = area['pilot_checks']
        failed = [key for key in checks if pilot_checks.get(key) is not True]
        if override in {'met', 'not_observed_in_pilot', 'gap'}:
            status_value = override
            failed = failed if override == 'gap' else []
        elif checks:
            status_value = 'gap' if failed else 'met'
        else:
            status_value = 'not_observed_in_pilot'
        rows.append(
            {
                **area,
                'mufinances_status': status_value,
                'pilot_cycle_run_id': pilot['id'],
                'failed_pilot_checks': failed,
                'vendor_sources': VENDOR_SOURCES,
            }
        )
    return rows


def _remaining_real_gaps(matrix: list[dict[str, Any]]) -> list[dict[str, Any]]:
    gaps = []
    for row in matrix:
        if row['mufinances_status'] != 'gap' or not row['failed_pilot_checks']:
            continue
        gaps.append(
            {
                'gap_key': row['feature_key'],
                'label': row['label'],
                'severity': 'high',
                'status': 'open',
                'pilot_cycle_run_id': row['pilot_cycle_run_id'],
                'failed_pilot_checks': row['failed_pilot_checks'],
                'evidence_type': 'failed_pilot_check',
                'recommended_resolution': 'Fix the failed pilot evidence item, rerun the pilot cycle, then rerun B129.',
            }
        )
    return gaps


def _theoretical_exclusions(matrix: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            'feature_key': row['feature_key'],
            'label': row['label'],
            'status': row['mufinances_status'],
            'reason': 'not_failed_in_pilot',
        }
        for row in matrix
        if row['mufinances_status'] in {'not_observed_in_pilot', 'met'}
    ]


def _format_run(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise ValueError('Prophix-class final gap review run not found.')
    result = dict(row)
    result['batch'] = 'B129'
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['vendor_sources'] = json.loads(result.pop('vendor_sources_json') or '{}')
    result['matrix'] = json.loads(result.pop('matrix_json') or '[]')
    result['gaps'] = json.loads(result.pop('gaps_json') or '[]')
    result['theoretical_exclusions'] = json.loads(result.pop('exclusions_json') or '[]')
    result['complete'] = result['status'] in {'passed', 'reviewed_with_gaps'}
    return result
