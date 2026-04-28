from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.foundation import append_ledger_entry
from app.services.ledger_depth import publish_scenario
from app.services.operating_budget import add_budget_line, approve_submission, create_submission, submit_submission
from app.services.reporting import (
    approve_narrative_report,
    approve_variance_explanation,
    assemble_board_package,
    assemble_narrative_report,
    create_export_artifact,
    draft_variance_narratives,
    generate_required_variance_explanations,
    list_variance_explanations,
    submit_variance_explanation,
    upsert_variance_threshold,
)
from app.services.scenario_engine import clone_scenario, compare_scenarios, ingest_actuals, run_forecast


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS fpa_workflow_certification_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                branch_scenario_id INTEGER DEFAULT NULL,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                artifacts_json TEXT NOT NULL,
                finance_signoff_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_fpa_workflow_certification_runs_scenario
            ON fpa_workflow_certification_runs (scenario_id, completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM fpa_workflow_certification_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'operating_budget_certification_ready': True,
        'forecast_certification_ready': True,
        'scenario_compare_certification_ready': True,
        'variance_workflow_certification_ready': True,
        'ai_narrative_human_approval_ready': True,
        'approval_publish_ready': True,
        'board_package_finance_signoff_ready': True,
    }
    counts = {
        'certification_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM fpa_workflow_certification_runs')['count']),
        'budget_submissions': int(db.fetch_one('SELECT COUNT(*) AS count FROM budget_submissions')['count']),
        'forecast_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM forecast_runs')['count']),
        'board_packages': int(db.fetch_one('SELECT COUNT(*) AS count FROM board_packages')['count']),
    }
    return {
        'batch': 'B93',
        'title': 'FP&A Workflow Certification',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM fpa_workflow_certification_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_run(row) for row in rows]


def run_certification(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b93-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    scenario_id = int(payload.get('scenario_id') or _create_certification_scenario(run_key))

    submission = create_submission(
        {
            'scenario_id': scenario_id,
            'department_code': 'SCI',
            'owner': user['email'],
            'notes': 'B93 FP&A workflow certification submission.',
        },
        user,
    )
    budget_line = add_budget_line(
        int(submission['id']),
        {
            'fund_code': 'GEN',
            'account_code': 'TUITION',
            'period': '2026-08',
            'amount': 125000.0,
            'line_type': 'revenue',
            'recurrence': 'recurring',
            'notes': 'Certification tuition budget line.',
        },
        user,
    )
    submitted = submit_submission(int(submission['id']), user)
    approved = approve_submission(int(submission['id']), user, 'Finance certification approval.')

    actuals = ingest_actuals(
        {
            'scenario_id': scenario_id,
            'source_version': f'{run_key}-actuals',
            'rows': [
                {
                    'department_code': 'SCI',
                    'fund_code': 'GEN',
                    'account_code': 'TUITION',
                    'period': '2026-08',
                    'amount': 118250.0,
                    'notes': 'Certification actual tuition row.',
                }
            ],
        },
        user,
    )
    forecast = run_forecast(
        {
            'scenario_id': scenario_id,
            'method_key': 'straight_line',
            'account_code': 'TUITION',
            'department_code': 'SCI',
            'period_start': '2026-09',
            'period_end': '2026-10',
            'confidence': 0.9,
        },
        user,
    )
    branch = clone_scenario(
        scenario_id,
        {'name': f'B93 Scenario Compare {run_key}', 'version': 'b93-branch'},
        user,
    )
    branch_entry = append_ledger_entry(
        {
            'scenario_id': int(branch['id']),
            'department_code': 'SCI',
            'fund_code': 'GEN',
            'account_code': 'TUITION',
            'period': '2026-08',
            'amount': 5000.0,
            'source': 'b93_scenario_branch',
            'ledger_type': 'scenario',
            'ledger_basis': 'scenario',
            'notes': 'Scenario compare adjustment.',
            'source_record_id': run_key,
        },
        actor=user['email'],
        user=user,
    )
    scenario_compare = compare_scenarios(scenario_id, int(branch['id']))

    threshold = upsert_variance_threshold(
        {
            'scenario_id': scenario_id,
            'threshold_key': f'b93-{run_key}',
            'amount_threshold': 1.0,
            'percent_threshold': None,
            'require_explanation': True,
        },
        user,
    )
    required = generate_required_variance_explanations(scenario_id, user)
    drafted = draft_variance_narratives(scenario_id, user)
    approved_explanations = []
    for explanation in list_variance_explanations(scenario_id):
        submitted_explanation = submit_variance_explanation(int(explanation['id']), user)
        approved_explanations.append(approve_variance_explanation(int(submitted_explanation['id']), user, 'Approved for board package.'))

    board_package = assemble_board_package(
        {
            'scenario_id': scenario_id,
            'package_name': f'B93 Finance Signoff Package {run_key}',
            'period_start': '2026-08',
            'period_end': '2026-10',
        },
        user,
    )
    narrative = assemble_narrative_report(
        {
            'scenario_id': scenario_id,
            'package_id': board_package['id'],
            'title': f'B93 Board Narrative {run_key}',
        },
        user,
    )
    approved_narrative = approve_narrative_report(int(narrative['id']), user, 'Finance narrative approval.')
    pdf_artifact = create_export_artifact(
        {
            'scenario_id': scenario_id,
            'artifact_type': 'pdf',
            'file_name': f'b93-board-package-{run_key}.pdf',
            'package_id': board_package['id'],
            'retention_until': None,
        },
        user,
    )
    published = publish_scenario(scenario_id, user)
    finance_signoff = {
        'signed_by': user['email'],
        'signed_at': _now(),
        'signoff_type': 'finance_certification',
        'scenario_status': published['status'],
        'board_package_id': board_package['id'],
        'narrative_id': approved_narrative['id'],
        'export_artifact_id': pdf_artifact['id'],
    }
    checks = {
        'operating_budget_approved': approved['status'] == 'approved' and budget_line['ledger_entry']['id'] is not None,
        'forecast_posted': forecast['status'] == 'posted' and len(forecast['created_lines']) >= 1,
        'scenario_compare_has_variance': any(float(row['variance']) != 0 for row in scenario_compare['rows']),
        'variance_workflow_approved': bool(approved_explanations) and all(row['status'] == 'approved' for row in approved_explanations),
        'ai_narrative_approved': approved_narrative['status'] == 'approved' and drafted['count'] >= 1,
        'scenario_published_locked': published['status'] == 'published' and bool(published['locked']),
        'board_package_and_pdf_ready': board_package['status'] == 'assembled' and pdf_artifact['status'] == 'ready',
        'finance_signoff_recorded': finance_signoff['signed_by'] == user['email'],
    }
    artifacts = {
        'submission': approved,
        'actuals': actuals,
        'forecast': forecast,
        'scenario_branch': branch,
        'branch_entry': branch_entry,
        'scenario_compare': scenario_compare,
        'threshold': threshold,
        'required_variances': required,
        'approved_explanations': approved_explanations,
        'board_package': board_package,
        'narrative': approved_narrative,
        'pdf_artifact': pdf_artifact,
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO fpa_workflow_certification_runs (
            run_key, scenario_id, branch_scenario_id, status, checks_json, artifacts_json,
            finance_signoff_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            scenario_id,
            int(branch['id']),
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(artifacts, sort_keys=True),
            json.dumps(finance_signoff, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('fpa_workflow_certification', run_key, status_value, user['email'], {'checks': checks, 'finance_signoff': finance_signoff}, completed)
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM fpa_workflow_certification_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('FP&A workflow certification run not found.')
    return _format_run(row)


def _create_certification_scenario(run_key: str) -> int:
    now = _now()
    return db.execute(
        '''
        INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
        VALUES (?, ?, 'draft', '2026-08', '2027-07', 0, ?)
        ''',
        (f'B93 FP&A Certification {run_key}', 'b93', now),
    )


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['artifacts'] = json.loads(result.pop('artifacts_json') or '{}')
    result['finance_signoff'] = json.loads(result.pop('finance_signoff_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
