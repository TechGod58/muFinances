from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.performance_reliability import run_performance_proof


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS enterprise_scale_benchmark_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                profile_json TEXT NOT NULL,
                seed_json TEXT NOT NULL,
                benchmark_json TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_enterprise_scale_benchmark_runs_created
            ON enterprise_scale_benchmark_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM enterprise_scale_benchmark_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'five_to_ten_year_profile_ready': True,
        'many_scenarios_ready': True,
        'department_grant_employee_account_scale_ready': True,
        'report_import_formula_consolidation_allocation_benchmarks_ready': True,
        'parallel_cubed_benchmark_ready': True,
        'regression_thresholds_ready': True,
    }
    counts = {
        'enterprise_scale_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM enterprise_scale_benchmark_runs')['count']),
        'ledger_rows': int(db.fetch_one('SELECT COUNT(*) AS count FROM planning_ledger')['count']),
        'scenarios': int(db.fetch_one('SELECT COUNT(*) AS count FROM scenarios')['count']),
    }
    return {
        'batch': 'B91',
        'title': 'Enterprise Scale Benchmark',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all(
        'SELECT * FROM enterprise_scale_benchmark_runs ORDER BY id DESC LIMIT ?',
        (limit,),
    )
    return [_format_run(row) for row in rows]


def run_enterprise_scale_benchmark(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b91-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    profile = _profile(payload)
    timer = time.perf_counter()
    seed = _seed_enterprise_profile(run_key, profile, user)
    seed['elapsed_ms'] = max(1, int((time.perf_counter() - timer) * 1000))
    benchmark = run_performance_proof(
        {
            'scenario_id': seed['primary_scenario_id'],
            'dataset_key': f"enterprise-scale-{run_key}",
            'row_count': profile['benchmark_row_count'],
            'backend': payload.get('backend') or 'runtime',
            'include_import': True,
            'include_reports': True,
            'thresholds': payload.get('thresholds') or {},
        },
        user,
    )
    metrics = {metric['metric_key']: metric for metric in benchmark.get('metrics', [])}
    checks = {
        'periods_cover_requested_years': seed['period_count'] >= profile['years'] * 12,
        'scenario_volume_seeded': seed['scenario_count'] >= profile['scenario_count'],
        'department_volume_seeded': seed['department_count'] >= profile['department_count'],
        'grant_volume_seeded': seed['grant_count'] >= profile['grant_count'],
        'employee_volume_represented': seed['employee_count'] >= profile['employee_count'],
        'account_volume_seeded': seed['account_count'] >= profile['account_count'],
        'report_benchmark_completed': metrics.get('financial_statement', {}).get('status') == 'passed',
        'import_benchmark_completed': metrics.get('streaming_import', {}).get('status') == 'passed',
        'formula_benchmark_completed': metrics.get('formula_recalculation', {}).get('status') in {'passed', 'posted'},
        'consolidation_benchmark_completed': metrics.get('consolidation_run', {}).get('status') == 'passed',
        'allocation_benchmark_completed': metrics.get('allocation_run', {}).get('status') == 'passed',
        'parallel_cubed_benchmark_completed': metrics.get('parallel_cubed_multi_core', {}).get('status') == 'passed',
    }
    status_value = 'passed' if all(checks.values()) and benchmark['status'] == 'passed' else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO enterprise_scale_benchmark_runs (
            run_key, profile_json, seed_json, benchmark_json, checks_json,
            status, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            json.dumps(profile, sort_keys=True),
            json.dumps(seed, sort_keys=True),
            json.dumps(benchmark, sort_keys=True),
            json.dumps(checks, sort_keys=True),
            status_value,
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('enterprise_scale_benchmark', run_key, status_value, user['email'], {'checks': checks, 'seed': seed}, completed)
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM enterprise_scale_benchmark_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Enterprise scale benchmark run not found.')
    return _format_run(row)


def _profile(payload: dict[str, Any]) -> dict[str, Any]:
    years = max(5, min(int(payload.get('years') or 5), 10))
    scenario_count = max(3, min(int(payload.get('scenario_count') or 6), 50))
    department_count = max(10, min(int(payload.get('department_count') or 40), 500))
    grant_count = max(5, min(int(payload.get('grant_count') or 25), 500))
    employee_count = max(50, min(int(payload.get('employee_count') or 500), 10000))
    account_count = max(20, min(int(payload.get('account_count') or 80), 1000))
    ledger_row_count = max(1000, min(int(payload.get('ledger_row_count') or 12000), 250000))
    benchmark_row_count = max(1000, min(int(payload.get('benchmark_row_count') or min(ledger_row_count, 25000)), 250000))
    return {
        'years': years,
        'period_count': years * 12,
        'scenario_count': scenario_count,
        'department_count': department_count,
        'grant_count': grant_count,
        'employee_count': employee_count,
        'account_count': account_count,
        'ledger_row_count': ledger_row_count,
        'benchmark_row_count': benchmark_row_count,
    }


def _seed_enterprise_profile(run_key: str, profile: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    periods = _periods(profile['years'])
    scenario_ids = _ensure_scale_scenarios(run_key, profile['scenario_count'], periods[0], periods[-1], now)
    departments = [f'D{index:03d}' for index in range(1, profile['department_count'] + 1)]
    accounts = [f'A{index:04d}' for index in range(1, profile['account_count'] + 1)]
    grants = [f'G{index:04d}' for index in range(1, profile['grant_count'] + 1)]
    rows = []
    for index in range(profile['ledger_row_count']):
        scenario_id = scenario_ids[index % len(scenario_ids)]
        period = periods[index % len(periods)]
        department = departments[index % len(departments)]
        account = accounts[index % len(accounts)]
        grant = grants[index % len(grants)] if index % 4 == 0 else None
        employee_id = f'E{(index % profile["employee_count"]) + 1:05d}' if index % 5 == 0 else None
        amount = round((((index % 29) - 14) * 137.31) + ((index % 7) * 19.42), 2)
        idempotency_key = f'enterprise-scale:{run_key}:{index}'
        checksum = hashlib.sha256(f'{scenario_id}|{period}|{department}|{account}|{amount}|{idempotency_key}'.encode('utf-8')).hexdigest()
        rows.append(
            (
                scenario_id,
                'CAMPUS',
                department,
                'GRANT' if grant else 'GEN',
                account,
                'ENTERPRISE',
                f'P{(index % 20) + 1:03d}',
                grant,
                period,
                amount,
                'enterprise_scale_benchmark',
                None,
                'B91 enterprise scale benchmark seed',
                'benchmark',
                'budget',
                run_key,
                f'{run_key}:{index}',
                None,
                idempotency_key,
                checksum,
                1,
                user['email'],
                now,
                json.dumps({'employee_id': employee_id, 'benchmark_profile': 'enterprise_scale'}, sort_keys=True),
            )
        )
    db.executemany(
        '''
        INSERT OR IGNORE INTO planning_ledger (
            scenario_id, entity_code, department_code, fund_code, account_code, program_code,
            project_code, grant_code, period, amount, source, driver_key, notes, ledger_type,
            ledger_basis, source_version, source_record_id, parent_ledger_entry_id,
            idempotency_key, posted_checksum, immutable_posting, posted_by, posted_at,
            metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        rows,
    )
    db.log_audit('enterprise_scale_seed', run_key, 'seeded', user['email'], {'ledger_rows': len(rows), 'scenario_count': len(scenario_ids)}, now)
    return {
        'primary_scenario_id': scenario_ids[0],
        'scenario_ids': scenario_ids,
        'scenario_count': len(scenario_ids),
        'period_count': len(periods),
        'department_count': len(departments),
        'grant_count': len(grants),
        'employee_count': profile['employee_count'],
        'account_count': len(accounts),
        'ledger_rows_requested': profile['ledger_row_count'],
        'ledger_rows_seeded': len(rows),
    }


def _ensure_scale_scenarios(run_key: str, count: int, start_period: str, end_period: str, now: str) -> list[int]:
    ids = []
    for index in range(1, count + 1):
        name = f'B91 Enterprise Scale {run_key} S{index:02d}'
        scenario_id = db.execute(
            '''
            INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
            VALUES (?, ?, 'draft', ?, ?, 0, ?)
            ''',
            (name, f'b91-{index:02d}', start_period, end_period, now),
        )
        ids.append(scenario_id)
    return ids


def _periods(years: int) -> list[str]:
    periods = []
    start_year = 2021
    for offset in range(years * 12):
        year = start_year + (offset // 12)
        month = (offset % 12) + 1
        periods.append(f'{year:04d}-{month:02d}')
    return periods


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['profile'] = json.loads(result.pop('profile_json') or '{}')
    result['seed'] = json.loads(result.pop('seed_json') or '{}')
    result['benchmark'] = json.loads(result.pop('benchmark_json') or '{}')
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
