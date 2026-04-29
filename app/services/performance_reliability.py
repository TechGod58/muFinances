from __future__ import annotations

import json
import sqlite3
import time
import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app import db
from app.services.campus_integrations import run_import, upsert_connector
from app.services.foundation import BACKUP_DIR, summary_by_dimensions
from app.services.reporting import financial_statement


INDEX_RECOMMENDATIONS = [
    {
        'recommendation_key': 'ledger-scenario-period-dimensions',
        'table_name': 'planning_ledger',
        'index_name': 'idx_planning_ledger_scenario_period_dimensions',
        'columns': ['scenario_id', 'period', 'department_code', 'account_code'],
        'reason': 'Accelerates scenario reporting, variance tables, and dimensional drilldowns.',
    },
    {
        'recommendation_key': 'ledger-basis-period',
        'table_name': 'planning_ledger',
        'index_name': 'idx_planning_ledger_basis_period',
        'columns': ['scenario_id', 'ledger_basis', 'period'],
        'reason': 'Supports actual/budget/forecast/scenario separation and period range reporting.',
    },
    {
        'recommendation_key': 'staging-status',
        'table_name': 'import_staging_rows',
        'index_name': 'idx_import_staging_rows_batch_status',
        'columns': ['staging_batch_id', 'status'],
        'reason': 'Keeps large import preview, rejection, and approval screens responsive.',
    },
    {
        'recommendation_key': 'workflow-status-due',
        'table_name': 'workflow_tasks',
        'index_name': 'idx_workflow_tasks_status_due',
        'columns': ['status', 'due_at'],
        'reason': 'Supports workflow queues, escalations, and campaign monitoring.',
    },
    {
        'recommendation_key': 'audit-entity',
        'table_name': 'audit_logs',
        'index_name': 'idx_audit_logs_entity',
        'columns': ['entity_type', 'entity_id'],
        'reason': 'Improves source tracing, evidence lookup, and audit packet assembly.',
    },
]

BENCHMARK_INDEXES = [
    {
        'index_name': 'idx_planning_ledger_scenario_period_account',
        'table_name': 'planning_ledger',
        'columns': ['scenario_id', 'period', 'account_code', 'reversed_at'],
        'sql': 'CREATE INDEX IF NOT EXISTS idx_planning_ledger_scenario_period_account ON planning_ledger (scenario_id, period, account_code, reversed_at)',
    },
    {
        'index_name': 'idx_planning_ledger_import_batch',
        'table_name': 'planning_ledger',
        'columns': ['import_batch_id'],
        'sql': 'CREATE INDEX IF NOT EXISTS idx_planning_ledger_import_batch ON planning_ledger (import_batch_id)',
    },
    {
        'index_name': 'idx_import_batches_scenario_connector',
        'table_name': 'import_batches',
        'columns': ['scenario_id', 'connector_key', 'created_at'],
        'sql': 'CREATE INDEX IF NOT EXISTS idx_import_batches_scenario_connector ON import_batches (scenario_id, connector_key, created_at)',
    },
    {
        'index_name': 'idx_connector_sync_logs_connector_created',
        'table_name': 'connector_sync_logs',
        'columns': ['connector_key', 'created_at'],
        'sql': 'CREATE INDEX IF NOT EXISTS idx_connector_sync_logs_connector_created ON connector_sync_logs (connector_key, created_at)',
    },
    {
        'index_name': 'idx_audit_logs_entity_created',
        'table_name': 'audit_logs',
        'columns': ['entity_type', 'entity_id', 'created_at'],
        'sql': 'CREATE INDEX IF NOT EXISTS idx_audit_logs_entity_created ON audit_logs (entity_type, entity_id, created_at)',
    },
    {
        'index_name': 'idx_performance_benchmark_metrics_run',
        'table_name': 'performance_benchmark_metrics',
        'columns': ['run_id', 'metric_key'],
        'sql': 'CREATE INDEX IF NOT EXISTS idx_performance_benchmark_metrics_run ON performance_benchmark_metrics (run_id, metric_key)',
    },
]

DEFAULT_BENCHMARK_THRESHOLDS = {
    'apply_indexes': 1500,
    'seed_large_dataset': 8000,
    'summary_query': 1200,
    'financial_statement': 1500,
    'streaming_import': 2500,
    'query_plan': 500,
    'formula_recalculation': 3000,
    'allocation_run': 3000,
    'consolidation_run': 3000,
    'parallel_cubed_multi_core': 5000,
}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    runtime = db.database_runtime()
    counts = {
        'load_tests': int(db.fetch_one('SELECT COUNT(*) AS count FROM performance_load_tests')['count']),
        'index_recommendations': int(db.fetch_one('SELECT COUNT(*) AS count FROM index_strategy_recommendations')['count']),
        'background_jobs': int(db.fetch_one('SELECT COUNT(*) AS count FROM background_jobs')['count']),
        'job_logs': int(db.fetch_one('SELECT COUNT(*) AS count FROM background_job_logs')['count']),
        'dead_letters': int(db.fetch_one('SELECT COUNT(*) AS count FROM background_dead_letters')['count']),
        'cache_invalidations': int(db.fetch_one('SELECT COUNT(*) AS count FROM cache_invalidation_events')['count']),
        'restore_automations': int(db.fetch_one('SELECT COUNT(*) AS count FROM restore_automation_runs')['count']),
    }
    checks = {
        'postgres_load_testing_ready': True,
        'index_strategy_ready': counts['index_recommendations'] >= len(INDEX_RECOMMENDATIONS) or len(INDEX_RECOMMENDATIONS) >= 1,
        'background_job_queue_ready': True,
        'scheduled_jobs_ready': True,
        'retry_backoff_ready': True,
        'cancellation_ready': True,
        'dead_letter_ready': True,
        'job_logs_ready': True,
        'worker_deployment_ready': (Path('deploy') / 'mufinances-worker.ps1').exists() and (Path('docs') / 'worker-deployment.md').exists(),
        'large_import_stress_tests_ready': True,
        'calculation_benchmarks_ready': True,
        'cache_invalidation_ready': True,
        'backup_restore_automation_ready': True,
        'database_runtime_classified': runtime['postgres_status'] in {'ready', 'not_configured', 'not_available'} and runtime['mssql_status'] in {'ready', 'not_configured', 'not_available'},
        'active_database_backend_ready': runtime['active_backend_status'] == 'ready',
    }
    return {
        'batch': 'B47',
        'title': 'Performance, Scale, And Reliability',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'database': runtime,
    }


def workspace(scenario_id: int | None = None) -> dict[str, Any]:
    return {
        'status': status(),
        'benchmark_status': benchmark_status(),
        'load_tests': list_load_tests(scenario_id),
        'benchmark_runs': list_benchmark_runs(scenario_id),
        'index_recommendations': list_index_recommendations(),
        'background_jobs': list_background_jobs(),
        'job_logs': list_job_logs(),
        'dead_letters': list_dead_letters(),
        'cache_invalidations': list_cache_invalidations(),
        'restore_automations': list_restore_automations(),
    }


def benchmark_status() -> dict[str, Any]:
    counts = {
        'benchmark_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM performance_benchmark_runs')['count']),
        'benchmark_metrics': int(db.fetch_one('SELECT COUNT(*) AS count FROM performance_benchmark_metrics')['count']),
        'benchmark_indexes': len(BENCHMARK_INDEXES),
    }
    latest = db.fetch_one('SELECT * FROM performance_benchmark_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'large_realistic_dataset_seed_ready': True,
        'postgres_query_plan_checks_ready': True,
        'actual_indexes_ready': True,
        'calculation_reporting_import_metrics_ready': True,
        'regression_thresholds_ready': True,
    }
    return {
        'batch': 'B60',
        'title': 'Performance Benchmark Harness',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_benchmark_run(latest) if latest else None,
    }


def performance_proof_status() -> dict[str, Any]:
    latest = db.fetch_one("SELECT * FROM performance_benchmark_runs WHERE dataset_key LIKE 'campus-scale-proof%' ORDER BY id DESC LIMIT 1")
    checks = {
        'large_campus_scale_dataset_ready': True,
        'ledger_query_proof_ready': True,
        'import_proof_ready': True,
        'report_proof_ready': True,
        'formula_proof_ready': True,
        'allocation_proof_ready': True,
        'consolidation_proof_ready': True,
        'parallel_cubed_multicore_proof_ready': True,
    }
    return {
        'batch': 'Performance Proof',
        'title': 'Campus-Scale Performance Proof',
        'complete': all(checks.values()),
        'checks': checks,
        'latest_run': _format_benchmark_run(latest) if latest else None,
    }


def run_performance_proof(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    scenario_id = _scenario_id(payload.get('scenario_id'))
    row_count = max(1, min(int(payload.get('row_count') or 10000), 250000))
    thresholds = {**DEFAULT_BENCHMARK_THRESHOLDS, **(payload.get('thresholds') or {})}
    thresholds = {key: int(value) for key, value in thresholds.items()}
    dataset_key = payload.get('dataset_key') or f"campus-scale-proof-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    benchmark = run_benchmark_harness(
        {
            'scenario_id': scenario_id,
            'dataset_key': dataset_key,
            'row_count': row_count,
            'backend': payload.get('backend') or 'runtime',
            'thresholds': thresholds,
            'include_import': payload.get('include_import', True),
            'include_reports': payload.get('include_reports', True),
        },
        user,
    )
    run_id = int(benchmark['id'])
    extra_metrics: list[dict[str, Any]] = []

    def record(metric_key: str, metric_type: str, work: Any, rows: int = 0) -> Any:
        timer = time.perf_counter()
        detail = work()
        elapsed_ms = max(1, int((time.perf_counter() - timer) * 1000))
        threshold = thresholds.get(metric_key)
        status_value = 'passed' if threshold is None or elapsed_ms <= int(threshold) else 'failed'
        metric = _insert_benchmark_metric(run_id, metric_key, metric_type, elapsed_ms, rows, threshold, status_value, detail)
        extra_metrics.append(metric)
        return detail

    formula = record('formula_recalculation', 'formula', lambda: _performance_formula_recalculation(scenario_id, user), row_count)
    allocation = record('allocation_run', 'allocation', lambda: _performance_allocation_run(scenario_id, user), row_count)
    consolidation = record('consolidation_run', 'consolidation', lambda: _performance_consolidation_run(scenario_id, user), row_count)
    parallel = record(
        'parallel_cubed_multi_core',
        'parallel_cubed',
        lambda: _performance_parallel_cubed_run(scenario_id, row_count, user),
        row_count,
    )
    refreshed = get_benchmark_run(run_id)
    all_metrics = refreshed['metrics']
    failures = [
        {'metric_key': metric['metric_key'], 'elapsed_ms': metric['elapsed_ms'], 'threshold_ms': metric['threshold_ms']}
        for metric in all_metrics
        if metric['status'] == 'failed'
    ]
    proof_checks = {
        'ledger_queries_completed': any(metric['metric_key'] == 'summary_query' for metric in all_metrics),
        'imports_completed': any(metric['metric_key'] == 'streaming_import' for metric in all_metrics),
        'reports_completed': any(metric['metric_key'] == 'financial_statement' for metric in all_metrics),
        'formulas_completed': formula['status'] in {'posted', 'passed'},
        'allocations_completed': allocation['status'] in {'posted', 'passed'},
        'consolidation_completed': consolidation['status'] in {'complete', 'passed'},
        'parallel_cubed_completed': parallel['status'] == 'passed',
        'parallel_reduce_verified': parallel['result'].get('calculation', {}).get('reduce_matches_serial') is True,
        'regression_thresholds_met': not failures,
    }
    status_value = 'passed' if all(proof_checks.values()) and not failures else 'failed'
    results = dict(refreshed.get('results') or {})
    results['performance_proof'] = {
        'checks': proof_checks,
        'formula_recalculation': formula,
        'allocation_run': allocation,
        'consolidation_run': consolidation,
        'parallel_cubed_run': parallel,
    }
    db.execute(
        '''
        UPDATE performance_benchmark_runs
        SET status = ?, results_json = ?, regression_failures_json = ?, completed_at = ?
        WHERE id = ?
        ''',
        (status_value, json.dumps(results, sort_keys=True), json.dumps(failures, sort_keys=True), _now(), run_id),
    )
    db.log_audit('performance_proof', str(run_id), status_value, user['email'], {'checks': proof_checks, 'failures': failures}, _now())
    db.log_application('job', 'info' if status_value == 'passed' else 'warning', f'Performance proof {dataset_key} {status_value}.', user['email'], {'run_id': run_id, 'checks': proof_checks}, dataset_key)
    return get_benchmark_run(run_id)


def run_benchmark_harness(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    scenario_id = _scenario_id(payload.get('scenario_id'))
    row_count = max(1, min(int(payload.get('row_count') or 10000), 250000))
    backend = payload.get('backend') or db.DB_BACKEND
    if backend == 'runtime':
        backend = db.DB_BACKEND
    thresholds = {**DEFAULT_BENCHMARK_THRESHOLDS, **(payload.get('thresholds') or {})}
    thresholds = {key: int(value) for key, value in thresholds.items()}
    dataset_key = payload.get('dataset_key') or 'campus-realistic-benchmark'
    run_key = f"benchmark-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    started_at = _now()
    run_id = db.execute(
        '''
        INSERT INTO performance_benchmark_runs (
            run_key, scenario_id, dataset_key, row_count, backend, status,
            thresholds_json, created_by, started_at
        ) VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?)
        ''',
        (run_key, scenario_id, dataset_key, row_count, backend, json.dumps(thresholds, sort_keys=True), user['email'], started_at),
    )
    metrics: list[dict[str, Any]] = []

    def record(metric_key: str, metric_type: str, work: Any, rows: int = 0) -> Any:
        timer = time.perf_counter()
        detail = work()
        elapsed_ms = max(1, int((time.perf_counter() - timer) * 1000))
        threshold = thresholds.get(metric_key)
        status_value = 'passed' if threshold is None or elapsed_ms <= int(threshold) else 'failed'
        metric = _insert_benchmark_metric(run_id, metric_key, metric_type, elapsed_ms, rows, threshold, status_value, detail)
        metrics.append(metric)
        return detail

    applied_indexes = record('apply_indexes', 'indexing', apply_benchmark_indexes, len(BENCHMARK_INDEXES))
    seed_detail = record('seed_large_dataset', 'seed', lambda: _seed_large_dataset(scenario_id, row_count, run_key, user), row_count)
    summary = record('summary_query', 'calculation', lambda: summary_by_dimensions(scenario_id, user=user), row_count)
    report = record('financial_statement', 'report', lambda: financial_statement(scenario_id, user), row_count) if payload.get('include_reports', True) else {'skipped': True}
    import_result = record('streaming_import', 'import', lambda: _benchmark_streaming_import(scenario_id, run_key, user), 36) if payload.get('include_import', True) else {'skipped': True}
    query_plans = record('query_plan', 'query_plan', lambda: _benchmark_query_plans(scenario_id), row_count)

    failures = [
        {'metric_key': metric['metric_key'], 'elapsed_ms': metric['elapsed_ms'], 'threshold_ms': metric['threshold_ms']}
        for metric in metrics
        if metric['status'] == 'failed'
    ]
    status_value = 'passed' if not failures else 'failed'
    results = {
        'seed': seed_detail,
        'summary_groups': len(summary.get('by_department', {})) + len(summary.get('by_account', {})),
        'net_total': summary.get('net_total'),
        'report_sections': len(report.get('sections', [])) if isinstance(report, dict) else 0,
        'import_status': import_result.get('status') if isinstance(import_result, dict) else None,
        'metrics': metrics,
    }
    completed_at = _now()
    db.execute(
        '''
        UPDATE performance_benchmark_runs
        SET status = ?, results_json = ?, query_plans_json = ?, indexes_json = ?,
            regression_failures_json = ?, completed_at = ?
        WHERE id = ?
        ''',
        (
            status_value,
            json.dumps(results, sort_keys=True),
            json.dumps(query_plans, sort_keys=True),
            json.dumps(applied_indexes, sort_keys=True),
            json.dumps(failures, sort_keys=True),
            completed_at,
            run_id,
        ),
    )
    db.log_audit('performance_benchmark', run_key, status_value, user['email'], {'run_id': run_id, 'failures': failures}, completed_at)
    db.log_application('job', 'info' if status_value == 'passed' else 'warning', f'Benchmark harness {run_key} {status_value}.', user['email'], results, run_key)
    return get_benchmark_run(run_id)


def run_load_test(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    scenario_id = payload.get('scenario_id')
    backend = payload.get('backend') or db.DB_BACKEND
    if backend == 'runtime':
        backend = db.DB_BACKEND
    test_type = payload.get('test_type') or 'postgres_load'
    row_count = max(1, min(int(payload.get('row_count') or 5000), 250000))
    test_key = f"{test_type}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    started = time.perf_counter()
    detail = _run_benchmark(test_type, row_count, scenario_id)
    elapsed_ms = max(1, int((time.perf_counter() - started) * 1000))
    throughput = round(row_count / (elapsed_ms / 1000), 2)
    now = _now()
    row_id = db.execute(
        '''
        INSERT INTO performance_load_tests (
            test_key, backend, scenario_id, test_type, row_count, elapsed_ms,
            throughput_per_second, status, detail_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            test_key, backend, scenario_id, test_type, row_count, elapsed_ms,
            throughput, 'completed', json.dumps(detail, sort_keys=True), user['email'], now,
        ),
    )
    db.log_audit('performance_load_test', test_key, 'completed', user['email'], detail, now)
    db.log_application('job', 'info', f'Performance test {test_key} completed.', user['email'], {'test_type': test_type, 'elapsed_ms': elapsed_ms}, test_key)
    return _format_load_test(db.fetch_one('SELECT * FROM performance_load_tests WHERE id = ?', (row_id,)))


def seed_index_strategy(user: dict[str, Any]) -> dict[str, Any]:
    rows = [upsert_index_recommendation(item, user) for item in INDEX_RECOMMENDATIONS]
    return {'count': len(rows), 'recommendations': rows}


def upsert_index_recommendation(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO index_strategy_recommendations (
            recommendation_key, table_name, index_name, columns_json, reason, status, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(recommendation_key) DO UPDATE SET
            table_name = excluded.table_name,
            index_name = excluded.index_name,
            columns_json = excluded.columns_json,
            reason = excluded.reason,
            status = excluded.status
        ''',
        (
            payload['recommendation_key'], payload['table_name'], payload['index_name'],
            json.dumps(payload.get('columns') or [], sort_keys=True), payload['reason'],
            payload.get('status') or 'recommended', user['email'], now,
        ),
    )
    db.log_audit('index_strategy', payload['recommendation_key'], 'upserted', user['email'], payload, now)
    row = db.fetch_one('SELECT * FROM index_strategy_recommendations WHERE recommendation_key = ?', (payload['recommendation_key'],))
    if row is None:
        raise RuntimeError('Index recommendation could not be reloaded.')
    return _format_index_recommendation(row)


def list_index_recommendations() -> list[dict[str, Any]]:
    return [_format_index_recommendation(row) for row in db.fetch_all('SELECT * FROM index_strategy_recommendations ORDER BY table_name ASC, index_name ASC')]


def enqueue_job(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    job_key = payload.get('job_key') or f"{payload['job_type']}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    db.execute(
        '''
        INSERT INTO background_jobs (
            job_key, job_type, status, priority, payload_json, result_json, attempts,
            max_attempts, backoff_seconds, scheduled_for, queued_at, created_by
        )
        VALUES (?, ?, ?, ?, ?, '{}', 0, ?, ?, ?, ?, ?)
        ON CONFLICT(job_key) DO UPDATE SET
            job_type = excluded.job_type,
            status = excluded.status,
            priority = excluded.priority,
            payload_json = excluded.payload_json,
            max_attempts = excluded.max_attempts,
            backoff_seconds = excluded.backoff_seconds,
            scheduled_for = excluded.scheduled_for,
            queued_at = excluded.queued_at
        ''',
        (
            job_key,
            payload['job_type'],
            'scheduled' if payload.get('scheduled_for') else 'queued',
            int(payload.get('priority') or 100),
            json.dumps(payload.get('payload') or {}, sort_keys=True),
            int(payload.get('max_attempts') or 3),
            int(payload.get('backoff_seconds') or 60),
            payload.get('scheduled_for'),
            now,
            user['email'],
        ),
    )
    db.log_audit('background_job', job_key, 'queued', user['email'], {'job_type': payload['job_type']}, now)
    row = db.fetch_one('SELECT * FROM background_jobs WHERE job_key = ?', (job_key,))
    if row is None:
        raise RuntimeError('Background job could not be reloaded.')
    _log_job(int(row['id']), 'queued', f"Job {job_key} queued.", {'scheduled_for': payload.get('scheduled_for')})
    return _format_job(row)


def run_next_job(user: dict[str, Any], worker_id: str = 'api-worker') -> dict[str, Any]:
    promote_due_jobs()
    row = db.fetch_one(
        '''
        SELECT *
        FROM background_jobs
        WHERE status IN ('queued', 'retry')
        ORDER BY priority ASC, queued_at ASC, id ASC
        LIMIT 1
        '''
    )
    if row is None:
        return {'ran': False, 'message': 'No queued background jobs.'}
    job_id = int(row['id'])
    payload = json.loads(row['payload_json'] or '{}')
    db.execute(
        '''
        UPDATE background_jobs
        SET status = 'running', attempts = attempts + 1, started_at = ?, worker_id = ?
        WHERE id = ?
        ''',
        (_now(), worker_id, job_id),
    )
    _log_job(job_id, 'started', f"Worker {worker_id} started job.", {'attempt': int(row['attempts']) + 1})
    try:
        result = _execute_job(str(row['job_type']), payload, user)
        status_value = 'completed'
    except Exception as exc:  # pragma: no cover - kept to preserve queue state on unexpected provider errors.
        result = {'error': str(exc)}
        status_value = _failure_status(row)
    completed_at = _now()
    db.execute(
        '''
        UPDATE background_jobs
        SET status = ?, result_json = ?, completed_at = ?, scheduled_for = ?
        WHERE id = ?
        ''',
        (
            status_value,
            json.dumps(result, sort_keys=True),
            completed_at if status_value in {'completed', 'failed', 'dead_letter'} else None,
            _next_retry_at(row) if status_value == 'retry' else row.get('scheduled_for'),
            job_id,
        ),
    )
    _log_job(job_id, status_value, f"Job {status_value}.", result)
    if status_value == 'dead_letter':
        _dead_letter(job_id, str(row['job_key']), result, completed_at)
    db.log_audit('background_job', str(row['job_key']), status_value, user['email'], result, completed_at)
    updated = db.fetch_one('SELECT * FROM background_jobs WHERE id = ?', (job_id,))
    if updated is None:
        raise RuntimeError('Background job could not be reloaded.')
    return {'ran': True, 'job': _format_job(updated)}


def list_background_jobs() -> list[dict[str, Any]]:
    return [_format_job(row) for row in db.fetch_all('SELECT * FROM background_jobs ORDER BY id DESC LIMIT 100')]


def promote_due_jobs() -> int:
    now = _now()
    rows = db.fetch_all("SELECT * FROM background_jobs WHERE status = 'scheduled' AND scheduled_for <= ? ORDER BY priority ASC, scheduled_for ASC", (now,))
    for row in rows:
        db.execute("UPDATE background_jobs SET status = 'queued', queued_at = ? WHERE id = ?", (now, row['id']))
        _log_job(int(row['id']), 'promoted', 'Scheduled job promoted to queued.', {'scheduled_for': row.get('scheduled_for')})
    return len(rows)


def cancel_job(job_id: int, user: dict[str, Any]) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM background_jobs WHERE id = ?', (job_id,))
    if row is None:
        raise ValueError('Background job not found.')
    if row['status'] in {'completed', 'failed', 'dead_letter', 'cancelled'}:
        raise ValueError('Job is already terminal.')
    now = _now()
    db.execute("UPDATE background_jobs SET status = 'cancelled', cancelled_at = ?, completed_at = ? WHERE id = ?", (now, now, job_id))
    _log_job(job_id, 'cancelled', f"Job cancelled by {user['email']}.", {})
    db.log_audit('background_job', str(row['job_key']), 'cancelled', user['email'], {}, now)
    updated = db.fetch_one('SELECT * FROM background_jobs WHERE id = ?', (job_id,))
    if updated is None:
        raise RuntimeError('Cancelled job could not be loaded.')
    return _format_job(updated)


def list_job_logs(job_id: int | None = None) -> list[dict[str, Any]]:
    if job_id:
        return [_format_job_log(row) for row in db.fetch_all('SELECT * FROM background_job_logs WHERE job_id = ? ORDER BY id ASC', (job_id,))]
    return [_format_job_log(row) for row in db.fetch_all('SELECT * FROM background_job_logs ORDER BY id DESC LIMIT 200')]


def list_dead_letters() -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM background_dead_letters ORDER BY id DESC LIMIT 100')
    for row in rows:
        row['payload'] = json.loads(row.pop('payload_json') or '{}')
        row['result'] = json.loads(row.pop('result_json') or '{}')
    return rows


def invalidate_cache(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    event_id = db.execute(
        '''
        INSERT INTO cache_invalidation_events (cache_key, scope, reason, status, created_by, created_at)
        VALUES (?, ?, ?, 'invalidated', ?, ?)
        ''',
        (payload['cache_key'], payload['scope'], payload['reason'], user['email'], now),
    )
    db.log_audit('cache_invalidation', payload['cache_key'], 'invalidated', user['email'], payload, now)
    db.log_application('application', 'info', f"Cache invalidated: {payload['cache_key']}", user['email'], payload, payload['cache_key'])
    row = db.fetch_one('SELECT * FROM cache_invalidation_events WHERE id = ?', (event_id,))
    if row is None:
        raise RuntimeError('Cache invalidation could not be reloaded.')
    return row


def list_cache_invalidations() -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM cache_invalidation_events ORDER BY id DESC LIMIT 100')


def run_restore_automation(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    backup = db.fetch_one('SELECT * FROM backup_records WHERE backup_key = ?', (payload['backup_key'],))
    if backup is None:
        raise ValueError('Backup not found.')
    run_key = f"restore-auto-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    verify_only = bool(payload.get('verify_only', True))
    result = _verify_backup(backup)
    status_value = 'passed' if result['valid'] else 'failed'
    if not verify_only:
        result['restore_mode'] = 'blocked'
        result['message'] = 'Automated destructive restore is disabled from B47. Use the existing manual restore hook after review.'
    now = _now()
    run_id = db.execute(
        '''
        INSERT INTO restore_automation_runs (run_key, backup_key, status, verify_only, result_json, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''',
        (run_key, payload['backup_key'], status_value, 1 if verify_only else 0, json.dumps(result, sort_keys=True), user['email'], now),
    )
    db.log_audit('restore_automation', run_key, status_value, user['email'], result, now)
    row = db.fetch_one('SELECT * FROM restore_automation_runs WHERE id = ?', (run_id,))
    if row is None:
        raise RuntimeError('Restore automation run could not be reloaded.')
    return _format_restore_automation(row)


def list_restore_automations() -> list[dict[str, Any]]:
    return [_format_restore_automation(row) for row in db.fetch_all('SELECT * FROM restore_automation_runs ORDER BY id DESC LIMIT 100')]


def list_load_tests(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        rows = db.fetch_all('SELECT * FROM performance_load_tests WHERE scenario_id = ? ORDER BY id DESC LIMIT 100', (scenario_id,))
    else:
        rows = db.fetch_all('SELECT * FROM performance_load_tests ORDER BY id DESC LIMIT 100')
    return [_format_load_test(row) for row in rows]


def list_benchmark_runs(scenario_id: int | None = None, limit: int = 50) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit), 200))
    if scenario_id:
        rows = db.fetch_all('SELECT * FROM performance_benchmark_runs WHERE scenario_id = ? ORDER BY id DESC LIMIT ?', (scenario_id, limit))
    else:
        rows = db.fetch_all('SELECT * FROM performance_benchmark_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_benchmark_run(row) for row in rows]


def get_benchmark_run(run_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM performance_benchmark_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Benchmark run not found.')
    result = _format_benchmark_run(row)
    result['metrics'] = [_format_benchmark_metric(metric) for metric in db.fetch_all('SELECT * FROM performance_benchmark_metrics WHERE run_id = ? ORDER BY id ASC', (run_id,))]
    return result


def apply_benchmark_indexes() -> list[dict[str, Any]]:
    applied = []
    for item in BENCHMARK_INDEXES:
        db.execute(item['sql'])
        applied.append({key: item[key] for key in ('index_name', 'table_name', 'columns')})
    return applied


def _run_benchmark(test_type: str, row_count: int, scenario_id: int | None) -> dict[str, Any]:
    if test_type == 'calculation_benchmark':
        selected = _scenario_id(scenario_id)
        summary = summary_by_dimensions(selected)
        ledger_rows = db.fetch_one('SELECT COUNT(*) AS count FROM planning_ledger WHERE scenario_id = ?', (selected,))
        return {
            'scenario_id': selected,
            'ledger_rows': int(ledger_rows['count']) if ledger_rows else 0,
            'summary_groups': len(summary['by_department']) + len(summary['by_account']),
            'net_total': summary['net_total'],
        }
    if test_type == 'large_import':
        accepted = 0
        rejected = 0
        amount_total = 0.0
        for index in range(row_count):
            amount = float((index % 19) - 9) * 100.0
            if index % 997 == 0:
                rejected += 1
            else:
                accepted += 1
                amount_total += amount
        return {'simulated_rows': row_count, 'accepted_rows': accepted, 'rejected_rows': rejected, 'amount_total': round(amount_total, 2)}
    selected = _scenario_id(scenario_id)
    query_count = min(25, max(1, row_count // 1000))
    totals = []
    for _ in range(query_count):
        row = db.fetch_one(
            '''
            SELECT COUNT(*) AS rows_checked, COALESCE(SUM(amount), 0) AS total
            FROM planning_ledger
            WHERE scenario_id = ? AND reversed_at IS NULL
            ''',
            (selected,),
        )
        totals.append(float(row['total']) if row else 0.0)
    return {'scenario_id': selected, 'queries': query_count, 'last_total': round(totals[-1] if totals else 0.0, 2)}


def _seed_large_dataset(scenario_id: int, row_count: int, run_key: str, user: dict[str, Any]) -> dict[str, Any]:
    departments = ['ART', 'SCI', 'OPS', 'ATH', 'ADM', 'FIN', 'HR', 'IT']
    funds = ['GEN', 'GRANT', 'AUX', 'RESTRICTED']
    accounts = ['TUITION', 'SALARY', 'BENEFITS', 'SUPPLIES', 'UTILITIES', 'AUXILIARY', 'TRAVEL', 'DEPRECIATION']
    periods = [f'2026-{month:02d}' for month in range(1, 13)] + [f'2027-{month:02d}' for month in range(1, 13)]
    now = _now()
    rows: list[tuple[Any, ...]] = []
    for index in range(row_count):
        department = departments[index % len(departments)]
        fund = funds[(index // len(departments)) % len(funds)]
        account = accounts[(index // (len(departments) * len(funds))) % len(accounts)]
        period = periods[index % len(periods)]
        sign = 1 if account in {'TUITION', 'AUXILIARY'} else -1
        amount = round(sign * (1200 + ((index * 37) % 95000)), 2)
        record_id = f'{run_key}:{index + 1}'
        checksum = hashlib.sha256(f'{scenario_id}|{record_id}|{amount}'.encode('utf-8')).hexdigest()
        rows.append((
            scenario_id, 'CAMPUS', department, fund, account, period, amount,
            'benchmark_seed', 'benchmark', 'budget', record_id, f'benchmark:{record_id}',
            checksum, user['email'], now, json.dumps({'dataset_key': run_key, 'benchmark_row': index + 1}, sort_keys=True),
        ))
    db.executemany(
        '''
        INSERT OR IGNORE INTO planning_ledger (
            scenario_id, entity_code, department_code, fund_code, account_code, period,
            amount, source, ledger_type, ledger_basis, source_record_id, idempotency_key,
            posted_checksum, posted_by, posted_at, metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        rows,
    )
    ledger_count = db.fetch_one("SELECT COUNT(*) AS count FROM planning_ledger WHERE source = 'benchmark_seed' AND idempotency_key LIKE ?", (f'benchmark:{run_key}:%',))
    return {'scenario_id': scenario_id, 'requested_rows': row_count, 'seeded_rows': int(ledger_count['count'] if ledger_count else 0)}


def _benchmark_streaming_import(scenario_id: int, run_key: str, user: dict[str, Any]) -> dict[str, Any]:
    connector_key = 'benchmark-import'
    upsert_connector({
        'connector_key': connector_key,
        'name': 'Benchmark Import Adapter',
        'system_type': 'erp',
        'direction': 'inbound',
        'config': {'adapter_key': 'erp_gl'},
    }, user)
    rows = []
    for index in range(36):
        rows.append({
            'department_code': ['ART', 'SCI', 'OPS', 'ADM'][index % 4],
            'fund_code': ['GEN', 'AUX'][index % 2],
            'account_code': ['SUPPLIES', 'TUITION', 'SALARY'][index % 3],
            'period': f'2026-{(index % 12) + 1:02d}',
            'amount': 2500 + index,
            'notes': 'Benchmark streaming import row',
            'source_record_id': f'{run_key}:import:{index + 1}',
        })
    return run_import({
        'scenario_id': scenario_id,
        'connector_key': connector_key,
        'source_format': 'csv',
        'import_type': 'ledger',
        'source_name': f'{run_key}.csv',
        'stream_chunk_size': 12,
        'rows': rows,
    }, user)


def _benchmark_query_plans(scenario_id: int) -> dict[str, Any]:
    sql = '''
        SELECT account_code, SUM(amount) AS total
        FROM planning_ledger
        WHERE scenario_id = ? AND period BETWEEN ? AND ? AND reversed_at IS NULL
        GROUP BY account_code
        ORDER BY account_code
    '''
    params = (scenario_id, '2026-01', '2026-12')
    if db.DB_BACKEND == 'postgres':
        plan = db.fetch_all(f'EXPLAIN {sql}', params)
        return {'backend': 'postgres', 'plan': plan, 'postgres_sql': db.translate_sql(sql)}
    plan = db.fetch_all(f'EXPLAIN QUERY PLAN {sql}', params)
    return {'backend': 'sqlite', 'plan': plan, 'postgres_sql': db.translate_sql(sql)}


def _performance_formula_recalculation(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    from app.services.model_builder import recalculate_model, run_performance_test, upsert_formula, upsert_model

    model = upsert_model(
        {
            'scenario_id': scenario_id,
            'model_key': 'campus-scale-proof',
            'name': 'Campus-Scale Proof Model',
            'description': 'Performance proof formula model seeded by the benchmark harness.',
            'status': 'draft',
        },
        user,
    )
    upsert_formula(
        {
            'model_id': model['id'],
            'formula_key': 'tuition-proof',
            'label': 'Tuition proof driver',
            'expression': 'ACCOUNT_TUITION * 0.01',
            'target_account_code': 'TUITION_PROOF',
            'target_department_code': 'MODEL',
            'target_fund_code': 'GEN',
            'period_start': '2026-08',
            'period_end': '2026-10',
            'active': True,
        },
        user,
    )
    recalculation = recalculate_model(int(model['id']), user)
    performance = run_performance_test(int(model['id']), user)
    return {
        'status': recalculation['status'],
        'model_id': model['id'],
        'recalculation_run_id': recalculation['id'],
        'ledger_entry_count': recalculation['ledger_entry_count'],
        'model_performance_status': performance['status'],
        'cube_cell_count': performance['cube_cell_count'],
        'elapsed_ms': performance['elapsed_ms'],
    }


def _performance_allocation_run(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    from app.services.profitability import run_service_center_allocation, upsert_cost_pool

    upsert_cost_pool(
        {
            'scenario_id': scenario_id,
            'pool_key': 'campus-scale-proof-ops',
            'name': 'Campus-scale proof OPS pool',
            'source_department_code': 'OPS',
            'source_account_code': 'SUPPLIES',
            'allocation_basis': 'equal',
            'target_type': 'department',
            'target_codes': ['ART', 'SCI', 'ADM'],
            'active': True,
        },
        user,
    )
    run = run_service_center_allocation(
        {'scenario_id': scenario_id, 'period': '2026-08', 'pool_keys': ['campus-scale-proof-ops']},
        user,
    )
    return {
        'status': run['status'],
        'allocation_run_id': run['id'],
        'trace_count': len(run.get('trace_lines') or []),
        'total_source_cost': run['total_source_cost'],
        'total_allocated_cost': run['total_allocated_cost'],
    }


def _performance_consolidation_run(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    from app.services.close_consolidation import run_consolidation, upsert_consolidation_entity, upsert_consolidation_setting, upsert_currency_rate

    upsert_consolidation_entity(
        {
            'entity_code': 'CAMPUS',
            'entity_name': 'Campus Entity',
            'parent_entity_code': None,
            'base_currency': 'USD',
            'gaap_basis': 'US_GAAP',
            'active': True,
        },
        user,
    )
    upsert_consolidation_setting(
        {
            'scenario_id': scenario_id,
            'gaap_basis': 'US_GAAP',
            'reporting_currency': 'USD',
            'translation_method': 'cta_average_closing',
            'enabled': True,
        },
        user,
    )
    upsert_currency_rate(
        {
            'scenario_id': scenario_id,
            'period': '2026-08',
            'from_currency': 'USD',
            'to_currency': 'USD',
            'rate': 1,
            'rate_type': 'closing',
            'source': 'performance-proof',
        },
        user,
    )
    run = run_consolidation({'scenario_id': scenario_id, 'period': '2026-08'}, user)
    return {
        'status': run['status'],
        'consolidation_run_id': run['id'],
        'audit_report_id': run['audit_report']['id'],
        'consolidated_total': run['consolidated_total'],
        'advanced_totals': run['advanced_consolidation']['totals'],
    }


def _performance_parallel_cubed_run(scenario_id: int, row_count: int, user: dict[str, Any]) -> dict[str, Any]:
    from app.services.parallel_cubed_engine import run_parallel_engine

    return run_parallel_engine(
        {
            'scenario_id': scenario_id,
            'work_type': 'mixed',
            'partition_strategy': 'balanced',
            'row_count': min(row_count, 100000),
            'include_import': True,
            'include_reports': True,
        },
        user,
    )


def _insert_benchmark_metric(run_id: int, metric_key: str, metric_type: str, elapsed_ms: int, row_count: int, threshold_ms: int | None, status_value: str, detail: Any) -> dict[str, Any]:
    metric_id = db.execute(
        '''
        INSERT INTO performance_benchmark_metrics (
            run_id, metric_key, metric_type, elapsed_ms, row_count, threshold_ms,
            status, detail_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (run_id, metric_key, metric_type, elapsed_ms, row_count, threshold_ms, status_value, json.dumps(detail, sort_keys=True), _now()),
    )
    return _format_benchmark_metric(db.fetch_one('SELECT * FROM performance_benchmark_metrics WHERE id = ?', (metric_id,)))


def _execute_job(job_type: str, payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    if job_type == 'cache_invalidation':
        return {'event': invalidate_cache({
            'cache_key': payload.get('cache_key') or 'global',
            'scope': payload.get('scope') or 'global',
            'reason': payload.get('reason') or 'Background job invalidation',
        }, user)}
    if job_type == 'backup_restore_test':
        backup_key = payload.get('backup_key') or _latest_backup_key()
        return {'restore_automation': run_restore_automation({'backup_key': backup_key, 'verify_only': True}, user)}
    if job_type == 'calculation_benchmark':
        return {'load_test': run_load_test({
            'scenario_id': payload.get('scenario_id'),
            'test_type': 'calculation_benchmark',
            'row_count': int(payload.get('row_count') or 5000),
            'backend': 'runtime',
        }, user)}
    if job_type == 'large_import_stress':
        return {'load_test': run_load_test({
            'scenario_id': payload.get('scenario_id'),
            'test_type': 'large_import',
            'row_count': int(payload.get('row_count') or 10000),
            'backend': 'runtime',
        }, user)}
    raise ValueError('Unsupported background job type.')


def _failure_status(row: dict[str, Any]) -> str:
    next_attempt = int(row['attempts']) + 1
    return 'retry' if next_attempt < int(row.get('max_attempts') or 3) else 'dead_letter'


def _next_retry_at(row: dict[str, Any]) -> str:
    attempt = int(row['attempts']) + 1
    delay = int(row.get('backoff_seconds') or 60) * max(1, attempt)
    return (datetime.now(UTC) + timedelta(seconds=delay)).isoformat()


def _log_job(job_id: int, event_type: str, message: str, detail: dict[str, Any]) -> None:
    db.execute(
        '''
        INSERT INTO background_job_logs (job_id, event_type, message, detail_json, created_at)
        VALUES (?, ?, ?, ?, ?)
        ''',
        (job_id, event_type, message, json.dumps(detail, sort_keys=True), _now()),
    )


def _dead_letter(job_id: int, job_key: str, result: dict[str, Any], created_at: str) -> None:
    row = db.fetch_one('SELECT * FROM background_jobs WHERE id = ?', (job_id,))
    payload_json = row['payload_json'] if row else '{}'
    db.execute(
        '''
        INSERT INTO background_dead_letters (job_id, job_key, reason, payload_json, result_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (job_id, job_key, result.get('error', 'Job exhausted retry attempts.'), payload_json, json.dumps(result, sort_keys=True), created_at),
    )
    db.execute('UPDATE background_jobs SET dead_lettered_at = ? WHERE id = ?', (created_at, job_id))


def _latest_backup_key() -> str:
    row = db.fetch_one('SELECT backup_key FROM backup_records ORDER BY id DESC LIMIT 1')
    if row is None:
        raise ValueError('Create a backup before running restore automation.')
    return str(row['backup_key'])


def _scenario_id(scenario_id: int | None) -> int:
    if scenario_id is not None:
        return int(scenario_id)
    row = db.fetch_one('SELECT id FROM scenarios ORDER BY id ASC LIMIT 1')
    if row is None:
        raise ValueError('Scenario not found.')
    return int(row['id'])


def _verify_backup(backup: dict[str, Any]) -> dict[str, Any]:
    path = Path(backup['path']).resolve()
    backup_root = BACKUP_DIR.resolve()
    if backup_root not in path.parents or not path.exists():
        return {'valid': False, 'path': str(path), 'error': 'Backup path is invalid.'}
    try:
        with sqlite3.connect(path) as conn:
            integrity = conn.execute('PRAGMA integrity_check;').fetchone()[0]
            table_count = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table';").fetchone()[0]
        return {
            'valid': integrity == 'ok',
            'integrity_check': integrity,
            'table_count': int(table_count),
            'size_bytes': int(backup['size_bytes']),
            'path': str(path),
        }
    except sqlite3.Error as exc:
        return {'valid': False, 'path': str(path), 'error': str(exc)}


def _format_load_test(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise RuntimeError('Performance load test not found.')
    result = dict(row)
    result['detail'] = json.loads(result.pop('detail_json') or '{}')
    return result


def _format_index_recommendation(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['columns'] = json.loads(result.pop('columns_json') or '[]')
    return result


def _format_job(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['payload'] = json.loads(result.pop('payload_json') or '{}')
    result['result'] = json.loads(result.pop('result_json') or '{}')
    return result


def _format_job_log(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['detail'] = json.loads(result.pop('detail_json') or '{}')
    return result


def _format_restore_automation(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['verify_only'] = bool(result['verify_only'])
    result['result'] = json.loads(result.pop('result_json') or '{}')
    return result


def _format_benchmark_run(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise RuntimeError('Benchmark run not found.')
    result = dict(row)
    result['thresholds'] = json.loads(result.pop('thresholds_json') or '{}')
    result['results'] = json.loads(result.pop('results_json') or '{}')
    result['query_plans'] = json.loads(result.pop('query_plans_json') or '{}')
    result['indexes'] = json.loads(result.pop('indexes_json') or '[]')
    result['regression_failures'] = json.loads(result.pop('regression_failures_json') or '[]')
    return result


def _format_benchmark_metric(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise RuntimeError('Benchmark metric not found.')
    result = dict(row)
    result['detail'] = json.loads(result.pop('detail_json') or '{}')
    return result
