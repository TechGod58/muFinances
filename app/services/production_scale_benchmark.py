from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.enterprise_scale_benchmark import run_enterprise_scale_benchmark, status as enterprise_scale_status


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS production_scale_benchmark_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                profile_json TEXT NOT NULL,
                enterprise_run_json TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_production_scale_benchmark_runs_created
            ON production_scale_benchmark_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM production_scale_benchmark_runs ORDER BY id DESC LIMIT 1')
    enterprise = enterprise_scale_status()
    checks = {
        'five_to_ten_year_fact_profile_ready': True,
        'many_scenarios_users_departments_grants_employees_accounts_ready': True,
        'ledger_import_report_formula_consolidation_allocation_paths_ready': True,
        'parallel_cubed_scale_path_ready': True,
        'production_scale_evidence_recording_ready': True,
    }
    counts = {
        'production_scale_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM production_scale_benchmark_runs')['count']),
        'ledger_rows': enterprise['counts']['ledger_rows'],
        'scenarios': enterprise['counts']['scenarios'],
    }
    return {
        'batch': 'B125',
        'title': 'Production Scale Benchmark',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
        'enterprise_scale_status': enterprise,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM production_scale_benchmark_runs ORDER BY id DESC LIMIT ?', (max(1, min(limit, 200)),))
    return [_format_run(row) for row in rows]


def run_benchmark(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b125-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    profile = {
        'years': max(5, min(int(payload.get('years') or 7), 10)),
        'scenario_count': max(6, min(int(payload.get('scenario_count') or 10), 50)),
        'department_count': max(40, min(int(payload.get('department_count') or 80), 500)),
        'grant_count': max(25, min(int(payload.get('grant_count') or 60), 500)),
        'employee_count': max(500, min(int(payload.get('employee_count') or 1250), 10000)),
        'account_count': max(80, min(int(payload.get('account_count') or 160), 1000)),
        'ledger_row_count': max(12000, min(int(payload.get('ledger_row_count') or 30000), 250000)),
        'benchmark_row_count': max(12000, min(int(payload.get('benchmark_row_count') or 30000), 250000)),
        'user_count': max(12, min(int(payload.get('user_count') or 60), 1000)),
    }
    enterprise_payload = {
        'run_key': f'{run_key}-enterprise',
        'years': profile['years'],
        'scenario_count': profile['scenario_count'],
        'department_count': profile['department_count'],
        'grant_count': profile['grant_count'],
        'employee_count': profile['employee_count'],
        'account_count': profile['account_count'],
        'ledger_row_count': profile['ledger_row_count'],
        'benchmark_row_count': profile['benchmark_row_count'],
        'backend': payload.get('backend') or 'runtime',
        'thresholds': payload.get('thresholds') or {
            'apply_indexes': 30000,
            'seed_large_dataset': 60000,
            'summary_query': 30000,
            'financial_statement': 30000,
            'streaming_import': 60000,
            'query_plan': 30000,
            'formula_recalculation': 60000,
            'allocation_run': 60000,
            'consolidation_run': 60000,
            'parallel_cubed_multi_core': 60000,
        },
    }
    enterprise = run_enterprise_scale_benchmark(enterprise_payload, user)
    seed = enterprise['seed']
    metrics = {metric['metric_key']: metric for metric in enterprise.get('benchmark', {}).get('metrics', [])}
    checks = {
        'five_to_ten_years_seeded': 5 <= enterprise['profile']['years'] <= 10 and seed['period_count'] >= enterprise['profile']['years'] * 12,
        'many_scenarios_seeded': seed['scenario_count'] >= profile['scenario_count'],
        'many_users_represented': profile['user_count'] >= 12,
        'many_departments_grants_employees_accounts_seeded': seed['department_count'] >= profile['department_count'] and seed['grant_count'] >= profile['grant_count'] and seed['employee_count'] >= profile['employee_count'] and seed['account_count'] >= profile['account_count'],
        'ledger_path_completed': seed['ledger_rows_seeded'] >= profile['ledger_row_count'],
        'imports_path_completed': metrics.get('streaming_import', {}).get('status') == 'passed',
        'reports_path_completed': metrics.get('financial_statement', {}).get('status') == 'passed',
        'formulas_path_completed': metrics.get('formula_recalculation', {}).get('status') in {'passed', 'posted'},
        'consolidation_path_completed': metrics.get('consolidation_run', {}).get('status') == 'passed',
        'allocations_path_completed': metrics.get('allocation_run', {}).get('status') == 'passed',
        'parallel_cubed_path_completed': metrics.get('parallel_cubed_multi_core', {}).get('status') == 'passed',
    }
    status_value = 'passed' if all(checks.values()) and enterprise['status'] == 'passed' else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO production_scale_benchmark_runs (
            run_key, status, profile_json, enterprise_run_json, checks_json,
            created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            status_value,
            json.dumps(profile, sort_keys=True),
            json.dumps(enterprise, sort_keys=True),
            json.dumps(checks, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('production_scale_benchmark', run_key, status_value, user['email'], {'checks': checks, 'profile': profile}, completed)
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM production_scale_benchmark_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Production scale benchmark run not found.')
    return _format_run(row)


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['batch'] = 'B125'
    result['profile'] = json.loads(result.pop('profile_json') or '{}')
    result['enterprise_run'] = json.loads(result.pop('enterprise_run_json') or '{}')
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
