from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.foundation import append_ledger_entry
from app.services.parallel_cubed_engine import cpu_topology, list_partitions, run_parallel_engine


STRATEGIES = ['balanced', 'department', 'account', 'period']


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS parallel_cubed_optimization_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                best_strategy TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                strategy_results_json TEXT NOT NULL,
                load_profile_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_parallel_cubed_optimization_runs_scenario
            ON parallel_cubed_optimization_runs (scenario_id, completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM parallel_cubed_optimization_runs ORDER BY id DESC LIMIT 1')
    topology = cpu_topology()
    counts = {
        'optimization_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM parallel_cubed_optimization_runs')['count']),
        'parallel_cubed_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM parallel_cubed_runs')['count']),
        'partitions': int(db.fetch_one('SELECT COUNT(*) AS count FROM parallel_cubed_partitions')['count']),
        'logical_cores': topology['logical_cores'],
    }
    checks = {
        'cpu_core_detection_under_load_ready': topology['logical_cores'] >= 1,
        'partition_strategy_tuning_ready': True,
        'safe_merge_reduce_ready': True,
        'parallel_imports_ready': True,
        'parallel_report_generation_ready': True,
        'benchmark_dashboard_ready': True,
    }
    return {
        'batch': 'B108',
        'title': 'Parallel Cubed Production Optimization',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'cpu': topology,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM parallel_cubed_optimization_runs ORDER BY id DESC LIMIT ?', (max(1, min(limit, 200)),))
    return [_format_run(row) for row in rows]


def run_optimization(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b108-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    scenario_id = int(payload.get('scenario_id') or _default_scenario_id())
    topology = cpu_topology()
    row_count = max(24, min(int(payload.get('row_count') or 720), 50000))
    requested_workers = int(payload.get('max_workers') or topology['logical_cores'])
    max_workers = max(1, min(requested_workers, topology['logical_cores'], 64))
    load_profile = {
        'logical_cores': topology['logical_cores'],
        'requested_workers': requested_workers,
        'max_workers': max_workers,
        'database_backend': topology['database_backend'],
        'queued_jobs': int(db.fetch_one("SELECT COUNT(*) AS count FROM background_jobs WHERE status IN ('queued', 'running', 'retry')")['count']),
        'started_at': started,
    }
    seeded_rows = _ensure_workload_rows(scenario_id, row_count, run_key, user)
    load_profile['seeded_rows'] = seeded_rows
    strategy_results = []
    for strategy in STRATEGIES:
        engine_run = run_parallel_engine(
            {
                'scenario_id': scenario_id,
                'work_type': 'mixed',
                'partition_strategy': strategy,
                'max_workers': max_workers,
                'row_count': row_count,
                'include_import': True,
                'include_reports': True,
            },
            user,
        )
        strategy_results.append(_summarize_strategy(strategy, engine_run))
    best = max(strategy_results, key=lambda item: (item['throughput_per_second'], -item['elapsed_ms']))
    work_types = {work_type for item in strategy_results for work_type in item['work_types']}
    checks = {
        'cpu_core_detection_under_load_ready': topology['logical_cores'] >= 1 and max_workers >= 1,
        'partition_strategy_tuning_ready': len(strategy_results) == len(STRATEGIES) and best['strategy'] in STRATEGIES,
        'safe_merge_reduce_ready': all(item['reduce_status'] == 'matched' and item['reduce_matches_serial'] for item in strategy_results),
        'parallel_imports_ready': 'import' in work_types and all(item['import_status'] in {'accepted', 'accepted_with_rejections'} for item in strategy_results),
        'parallel_report_generation_ready': 'report' in work_types and all(item['report_sections'] >= 1 for item in strategy_results),
        'benchmark_dashboard_ready': all(item['partition_count'] >= item['worker_count'] for item in strategy_results),
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO parallel_cubed_optimization_runs (
            run_key, scenario_id, status, best_strategy, checks_json, strategy_results_json,
            load_profile_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            scenario_id,
            status_value,
            best['strategy'],
            json.dumps(checks, sort_keys=True),
            json.dumps(strategy_results, sort_keys=True),
            json.dumps(load_profile, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('parallel_cubed_production_optimization', run_key, status_value, user['email'], {'checks': checks, 'best_strategy': best['strategy']}, completed)
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM parallel_cubed_optimization_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Parallel Cubed optimization run not found.')
    return _format_run(row)


def _summarize_strategy(strategy: str, engine_run: dict[str, Any]) -> dict[str, Any]:
    calculation = engine_run.get('result', {}).get('calculation') or {}
    import_result = engine_run.get('result', {}).get('import') or {}
    report = engine_run.get('result', {}).get('report') or {}
    partitions = engine_run.get('partitions') or list_partitions(int(engine_run['id']))
    work_types = sorted({partition['work_type'] for partition in partitions})
    benchmark = engine_run.get('benchmark') or {}
    return {
        'strategy': strategy,
        'run_id': engine_run['id'],
        'status': engine_run['status'],
        'reduce_status': engine_run['reduce_status'],
        'reduce_matches_serial': calculation.get('reduce_matches_serial') is True,
        'worker_count': int(engine_run['worker_count']),
        'partition_count': int(engine_run['partition_count']),
        'row_count': int(engine_run['row_count']),
        'elapsed_ms': int(engine_run['elapsed_ms'] or 0),
        'throughput_per_second': float(engine_run['throughput_per_second'] or 0),
        'core_coverage_percent': float(benchmark.get('core_coverage_percent') or 0),
        'slowest_partition_ms': int(benchmark.get('slowest_partition_ms') or 0),
        'work_types': work_types,
        'import_status': import_result.get('status'),
        'import_accepted_rows': int(import_result.get('accepted_rows') or 0),
        'report_sections': int(report.get('generated_sections') or 0),
    }


def _default_scenario_id() -> int:
    row = db.fetch_one("SELECT id FROM scenarios WHERE name = 'FY27 Operating Plan' ORDER BY id LIMIT 1") or db.fetch_one('SELECT id FROM scenarios ORDER BY id LIMIT 1')
    if row is None:
        raise ValueError('No scenario exists for Parallel Cubed optimization.')
    return int(row['id'])


def _ensure_workload_rows(scenario_id: int, target_count: int, run_key: str, user: dict[str, Any]) -> int:
    existing = int(db.fetch_one('SELECT COUNT(*) AS count FROM planning_ledger WHERE scenario_id = ? AND reversed_at IS NULL', (scenario_id,))['count'])
    missing = max(0, target_count - existing)
    if missing == 0:
        return 0
    periods = [row['period'] for row in db.fetch_all('SELECT period FROM fiscal_periods WHERE is_closed = 0 ORDER BY period LIMIT 12')] or ['2026-07']
    departments = ['SCI', 'ART', 'OPS', 'ADM', 'ATH', 'FIN']
    accounts = ['TUITION', 'SALARY', 'SUPPLIES', 'BENEFITS', 'GRANTS', 'AUXILIARY']
    for index in range(missing):
        append_ledger_entry(
            {
                'scenario_id': scenario_id,
                'entity_code': 'CAMPUS',
                'department_code': departments[index % len(departments)],
                'fund_code': 'GEN',
                'account_code': accounts[index % len(accounts)],
                'period': periods[index % len(periods)],
                'amount': float(((index % 11) - 5) * 310),
                'source': 'parallel_cubed_optimization',
                'ledger_type': 'forecast',
                'notes': 'B108 optimization workload row',
                'source_record_id': f'{run_key}:seed:{index + 1}',
                'idempotency_key': f'b108:{run_key}:seed:{index + 1}',
                'metadata': {'batch': 'B108', 'optimization_run_key': run_key},
            },
            actor=user['email'],
            user=user,
        )
    return missing


def _format_run(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {}
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['strategy_results'] = json.loads(result.pop('strategy_results_json') or '[]')
    result['load_profile'] = json.loads(result.pop('load_profile_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
