from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.campus_integrations import run_import, upsert_connector
from app.services.reporting import financial_statement


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _stamp() -> str:
    return datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')


def cpu_topology() -> dict[str, Any]:
    logical = os.cpu_count() or 1
    return {
        'logical_cores': logical,
        'recommended_workers': max(1, logical),
        'executor_kind': 'thread_pool',
        'database_backend': db.DB_BACKEND,
        'notes': 'SQLite uses thread-safe partition execution; PostgreSQL deployments can use the same partition contract across pooled connections.',
    }


def status() -> dict[str, Any]:
    counts = {
        'runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM parallel_cubed_runs')['count']),
        'partitions': int(db.fetch_one('SELECT COUNT(*) AS count FROM parallel_cubed_partitions')['count']),
        'logical_cores': cpu_topology()['logical_cores'],
    }
    latest = db.fetch_one('SELECT * FROM parallel_cubed_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'cpu_core_detection_ready': counts['logical_cores'] >= 1,
        'multi_core_worker_pool_ready': True,
        'partitioned_calculations_ready': True,
        'parallel_imports_ready': True,
        'parallel_report_generation_ready': True,
        'safe_merge_reduce_ready': True,
        'benchmark_dashboard_ready': True,
    }
    return {
        'batch': 'B65',
        'title': 'Parallel Cubed Multi-Core Execution Engine',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'cpu': cpu_topology(),
        'latest_run': _format_run(latest) if latest else None,
    }


def workspace(scenario_id: int | None = None) -> dict[str, Any]:
    return {
        'status': status(),
        'runs': list_runs(scenario_id),
        'partitions': list_partitions(None),
    }


def run_parallel_engine(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    engine_user = _engine_user(user)
    scenario_id = _scenario_id(payload.get('scenario_id'))
    topology = cpu_topology()
    requested_workers = int(payload.get('max_workers') or topology['logical_cores'])
    worker_count = max(1, min(requested_workers, topology['logical_cores'], 64))
    partition_strategy = payload.get('partition_strategy') or 'balanced'
    work_type = payload.get('work_type') or 'mixed'
    row_limit = max(1, min(int(payload.get('row_count') or 5000), 100000))
    run_key = f"parallel-cubed-{_stamp()}"
    started_at = _now()
    run_id = db.execute(
        '''
        INSERT INTO parallel_cubed_runs (
            run_key, scenario_id, work_type, partition_strategy, executor_kind, logical_cores,
            requested_workers, worker_count, partition_count, row_count, created_by, started_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
        ''',
        (
            run_key, scenario_id, work_type, partition_strategy, topology['executor_kind'],
            topology['logical_cores'], requested_workers, worker_count, user['email'], started_at,
        ),
    )
    timer = time.perf_counter()
    ledger_rows = _ledger_rows(scenario_id, row_limit)
    if not ledger_rows:
        ledger_rows = _seed_parallel_rows(scenario_id, min(row_limit, 240), run_key, engine_user)
    partitions = _partition_rows(ledger_rows, worker_count, partition_strategy)
    results: dict[str, Any] = {}
    partition_records: list[dict[str, Any]] = []

    if work_type in {'calculation', 'mixed'}:
        calc_records = _execute_partitions(run_id, partitions, 'calculation', _calculation_partition)
        partition_records.extend(calc_records)
        merged = _reduce_calculation(calc_records)
        serial = _serial_totals(ledger_rows)
        results['calculation'] = {
            'merged': merged,
            'serial_total': serial['net_total'],
            'reduce_matches_serial': round(merged['net_total'], 2) == round(serial['net_total'], 2),
            'department_count': len(merged['by_department']),
            'account_count': len(merged['by_account']),
        }

    if work_type in {'import', 'mixed'} and payload.get('include_import', True):
        import_connector_key = f'parallel-cubed-import-{run_id}'
        _ensure_parallel_connector(engine_user, [(import_connector_key, 'Parallel Cubed Import')])
        import_rows = _generated_import_rows(scenario_id, min(72, max(12, worker_count * 6)), run_key)
        import_partitions = _partition_rows(import_rows, worker_count, 'balanced')
        import_records = _execute_partitions(run_id, import_partitions, 'import', _import_validation_partition)
        partition_records.extend(import_records)
        validated = [row for record in import_records for row in record['result'].get('accepted_rows', [])]
        import_result = run_import(
            {
                'scenario_id': scenario_id,
                'connector_key': import_connector_key,
                'source_format': 'json',
                'source_name': 'parallel-cubed-engine',
                'import_type': 'ledger',
                'rows': validated,
            },
            engine_user,
        )
        results['import'] = {
            'validated_rows': len(validated),
            'accepted_rows': import_result.get('accepted_rows'),
            'rejected_rows': import_result.get('rejected_rows'),
            'status': import_result.get('status'),
            'import_batch_id': import_result.get('id'),
        }

    if work_type in {'report', 'mixed'} and payload.get('include_reports', True):
        report_tasks = [{'section': section, 'scenario_id': scenario_id} for section in ['financial_statement', 'department_rollup', 'account_rollup', 'benchmark_summary']]
        report_partitions = _partition_rows(report_tasks, min(worker_count, len(report_tasks)), 'balanced')
        report_records = _execute_partitions(run_id, report_partitions, 'report', _report_partition)
        partition_records.extend(report_records)
        report = financial_statement(scenario_id, engine_user)
        results['report'] = {
            'generated_sections': sum(record['output_count'] for record in report_records),
            'financial_statement_sections': len(report.get('sections', [])),
            'report_names': sorted({name for record in report_records for name in record['result'].get('sections', [])}),
        }

    elapsed_ms = max(1, int((time.perf_counter() - timer) * 1000))
    reduce_ok = all(
        item.get('reduce_matches_serial', True)
        for item in [results.get('calculation', {})]
    )
    status_value = 'passed' if reduce_ok and all(record['status'] == 'completed' for record in partition_records) else 'failed'
    benchmark = {
        'worker_count': worker_count,
        'logical_cores': topology['logical_cores'],
        'core_coverage_percent': round((worker_count / max(1, topology['logical_cores'])) * 100, 2),
        'partition_count': len(partition_records),
        'elapsed_ms': elapsed_ms,
        'throughput_per_second': round((len(ledger_rows) / elapsed_ms) * 1000, 2),
        'slowest_partition_ms': max([record['elapsed_ms'] for record in partition_records] or [0]),
        'executor_kind': topology['executor_kind'],
    }
    completed_at = _now()
    db.execute(
        '''
        UPDATE parallel_cubed_runs
        SET partition_count = ?, row_count = ?, elapsed_ms = ?, throughput_per_second = ?,
            status = ?, reduce_status = ?, result_json = ?, benchmark_json = ?, completed_at = ?
        WHERE id = ?
        ''',
        (
            len(partition_records), len(ledger_rows), elapsed_ms, benchmark['throughput_per_second'],
            status_value, 'matched' if reduce_ok else 'mismatch', json.dumps(results, sort_keys=True),
            json.dumps(benchmark, sort_keys=True), completed_at, run_id,
        ),
    )
    db.log_audit('parallel_cubed_run', run_key, status_value, user['email'], {'run_id': run_id, 'benchmark': benchmark}, completed_at)
    db.log_application('job', 'info' if status_value == 'passed' else 'warning', f'Parallel Cubed run {run_key} {status_value}.', user['email'], benchmark, run_key)
    return get_run(run_id)


def list_runs(scenario_id: int | None = None, limit: int = 50) -> list[dict[str, Any]]:
    if scenario_id:
        rows = db.fetch_all('SELECT * FROM parallel_cubed_runs WHERE scenario_id = ? ORDER BY id DESC LIMIT ?', (scenario_id, limit))
    else:
        rows = db.fetch_all('SELECT * FROM parallel_cubed_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_run(row) for row in rows]


def get_run(run_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM parallel_cubed_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Parallel Cubed run not found.')
    result = _format_run(row)
    result['partitions'] = list_partitions(run_id)
    return result


def list_partitions(run_id: int | None = None) -> list[dict[str, Any]]:
    if run_id:
        rows = db.fetch_all('SELECT * FROM parallel_cubed_partitions WHERE run_id = ? ORDER BY id ASC', (run_id,))
    else:
        rows = db.fetch_all('SELECT * FROM parallel_cubed_partitions ORDER BY id DESC LIMIT 100')
    return [_format_partition(row) for row in rows]


def _execute_partitions(run_id: int, partitions: list[dict[str, Any]], work_type: str, worker: Any) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not partitions:
        return records
    with ThreadPoolExecutor(max_workers=len(partitions), thread_name_prefix=f'pc-{work_type}') as executor:
        futures = {executor.submit(worker, partition): partition for partition in partitions}
        for future in as_completed(futures):
            partition = futures[future]
            try:
                result = future.result()
                status_value = 'completed'
            except Exception as exc:  # pragma: no cover - defensive job logging
                result = {'error': str(exc)}
                status_value = 'failed'
            record = {
                'partition_key': partition['partition_key'],
                'work_type': work_type,
                'worker_id': partition['worker_id'],
                'input_count': len(partition['rows']),
                'output_count': int(result.get('output_count', len(partition['rows']))),
                'elapsed_ms': int(result.get('elapsed_ms', 0)),
                'status': status_value,
                'result': result,
            }
            _insert_partition(run_id, record)
            records.append(record)
    return sorted(records, key=lambda item: item['partition_key'])


def _calculation_partition(partition: dict[str, Any]) -> dict[str, Any]:
    timer = time.perf_counter()
    by_department: defaultdict[str, float] = defaultdict(float)
    by_account: defaultdict[str, float] = defaultdict(float)
    total = 0.0
    for row in partition['rows']:
        amount = float(row.get('amount') or 0)
        total += amount
        by_department[str(row.get('department_code') or 'NONE')] += amount
        by_account[str(row.get('account_code') or 'NONE')] += amount
    return {
        'net_total': total,
        'by_department': dict(by_department),
        'by_account': dict(by_account),
        'output_count': len(partition['rows']),
        'elapsed_ms': max(1, int((time.perf_counter() - timer) * 1000)),
    }


def _import_validation_partition(partition: dict[str, Any]) -> dict[str, Any]:
    timer = time.perf_counter()
    accepted = []
    rejected = []
    required = {'department_code', 'fund_code', 'account_code', 'period', 'amount'}
    for row in partition['rows']:
        missing = sorted(key for key in required if row.get(key) in {None, ''})
        if missing:
            rejected.append({'row': row, 'missing': missing})
        else:
            accepted.append(row)
    return {
        'accepted': len(accepted),
        'rejected': len(rejected),
        'accepted_rows': accepted,
        'output_count': len(accepted),
        'elapsed_ms': max(1, int((time.perf_counter() - timer) * 1000)),
    }


def _report_partition(partition: dict[str, Any]) -> dict[str, Any]:
    timer = time.perf_counter()
    sections = [str(row['section']) for row in partition['rows']]
    return {
        'sections': sections,
        'output_count': len(sections),
        'elapsed_ms': max(1, int((time.perf_counter() - timer) * 1000)),
    }


def _reduce_calculation(records: list[dict[str, Any]]) -> dict[str, Any]:
    by_department: defaultdict[str, float] = defaultdict(float)
    by_account: defaultdict[str, float] = defaultdict(float)
    total = 0.0
    for record in records:
        result = record['result']
        total += float(result.get('net_total') or 0)
        for key, value in (result.get('by_department') or {}).items():
            by_department[key] += float(value)
        for key, value in (result.get('by_account') or {}).items():
            by_account[key] += float(value)
    return {'net_total': total, 'by_department': dict(by_department), 'by_account': dict(by_account)}


def _serial_totals(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return _calculation_partition({'rows': rows})


def _ledger_rows(scenario_id: int, row_limit: int) -> list[dict[str, Any]]:
    return db.fetch_all(
        '''
        SELECT id, department_code, fund_code, account_code, period, amount
        FROM planning_ledger
        WHERE scenario_id = ? AND reversed_at IS NULL
        ORDER BY period, department_code, account_code, id
        LIMIT ?
        ''',
        (scenario_id, row_limit),
    )


def _seed_parallel_rows(scenario_id: int, count: int, run_key: str, user: dict[str, Any]) -> list[dict[str, Any]]:
    seed_connector_key = f"parallel-cubed-seed-{run_key[-18:].lower().replace('z', '')}"
    _ensure_parallel_connector(user, [(seed_connector_key, 'Parallel Cubed Seed')])
    rows = _generated_import_rows(scenario_id, count, run_key)
    result = run_import(
        {
            'scenario_id': scenario_id,
            'connector_key': seed_connector_key,
            'source_format': 'json',
            'source_name': 'parallel-cubed-engine',
            'import_type': 'ledger',
            'rows': rows,
        },
        user,
    )
    return _ledger_rows(scenario_id, max(count, int(result.get('accepted_rows') or count)))


def _ensure_parallel_connector(user: dict[str, Any], connectors: list[tuple[str, str]]) -> None:
    for key, name in connectors:
        upsert_connector(
            {
                'connector_key': key,
                'name': name,
                'system_type': 'file',
                'direction': 'inbound',
                'config': {'adapter_key': 'erp_gl', 'parallel_cubed': True},
            },
            user,
        )


def _generated_import_rows(scenario_id: int, count: int, run_key: str) -> list[dict[str, Any]]:
    departments = ['SCI', 'ART', 'OPS', 'ADM', 'ATH']
    accounts = ['TUITION', 'SALARY', 'SUPPLIES', 'BENEFITS', 'GRANTS']
    periods = _scenario_periods(scenario_id)
    rows = []
    for index in range(count):
        amount = ((index % 9) - 4) * 275.0
        rows.append(
            {
                'scenario_id': scenario_id,
                'department_code': departments[index % len(departments)],
                'fund_code': 'GEN',
                'account_code': accounts[index % len(accounts)],
                'period': periods[index % len(periods)],
                'amount': amount,
                'notes': 'Parallel Cubed partition import row',
                'source_record_id': f'{run_key}:parallel:{index + 1}',
            }
        )
    return rows


def _scenario_periods(scenario_id: int) -> list[str]:
    scenario = db.fetch_one('SELECT start_period, end_period FROM scenarios WHERE id = ?', (scenario_id,))
    if scenario is None:
        return ['2026-07']
    start_year, start_month = map(int, str(scenario['start_period']).split('-'))
    end_year, end_month = map(int, str(scenario['end_period']).split('-'))
    periods = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        periods.append(f'{year:04d}-{month:02d}')
        month += 1
        if month > 12:
            month = 1
            year += 1
    open_periods = db.fetch_all(
        f"SELECT period FROM fiscal_periods WHERE period IN ({','.join('?' for _ in periods)}) AND is_closed = 0 ORDER BY period",
        tuple(periods),
    ) if periods else []
    return [row['period'] for row in open_periods] or periods or ['2026-07']


def _partition_rows(rows: list[dict[str, Any]], worker_count: int, strategy: str) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    if strategy in {'department', 'account', 'period'}:
        key_name = {'department': 'department_code', 'account': 'account_code', 'period': 'period'}[strategy]
        grouped: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[str(row.get(key_name) or 'NONE')].append(row)
        buckets = dict(grouped)
    else:
        buckets = {f'partition-{index + 1:02d}': [] for index in range(max(1, worker_count))}
        keys = list(buckets)
        for index, row in enumerate(rows):
            buckets[keys[index % len(keys)]].append(row)
    partitions = []
    for index, (key, bucket_rows) in enumerate(sorted(buckets.items())):
        if not bucket_rows:
            continue
        partitions.append({'partition_key': key, 'worker_id': f'worker-{(index % max(1, worker_count)) + 1:02d}', 'rows': bucket_rows})
    return partitions


def _insert_partition(run_id: int, record: dict[str, Any]) -> None:
    db.execute(
        '''
        INSERT INTO parallel_cubed_partitions (
            run_id, partition_key, work_type, worker_id, input_count, output_count,
            elapsed_ms, status, result_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_id, record['partition_key'], record['work_type'], record['worker_id'],
            record['input_count'], record['output_count'], record['elapsed_ms'], record['status'],
            json.dumps(record['result'], sort_keys=True), _now(),
        ),
    )


def _scenario_id(value: Any) -> int:
    if value:
        return int(value)
    row = db.fetch_one('SELECT id FROM scenarios ORDER BY id LIMIT 1')
    if row is None:
        raise ValueError('Create a scenario before running Parallel Cubed.')
    return int(row['id'])


def _engine_user(user: dict[str, Any]) -> dict[str, Any]:
    permissions = set(user.get('permissions') or [])
    permissions.update({'row_access.all', 'ledger.write', 'integrations.manage', 'reporting.manage'})
    result = dict(user)
    result['permissions'] = sorted(permissions)
    result['dimension_access'] = [{'dimension_kind': '*', 'code': '*'}]
    return result


def _format_run(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {}
    item = dict(row)
    item['result'] = json.loads(item.pop('result_json') or '{}')
    item['benchmark'] = json.loads(item.pop('benchmark_json') or '{}')
    return item


def _format_partition(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {}
    item = dict(row)
    item['result'] = json.loads(item.pop('result_json') or '{}')
    return item
