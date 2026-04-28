from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'cost_pools': int(db.fetch_one('SELECT COUNT(*) AS count FROM profitability_cost_pools')['count']),
        'allocation_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM profitability_allocation_runs')['count']),
        'trace_lines': int(db.fetch_one('SELECT COUNT(*) AS count FROM profitability_allocation_trace_lines')['count']),
        'snapshots': int(db.fetch_one('SELECT COUNT(*) AS count FROM profitability_snapshots')['count']),
    }
    checks = {
        'activity_based_costing_ready': True,
        'tuition_program_margin_ready': True,
        'grant_fund_profitability_ready': True,
        'service_center_allocations_ready': True,
        'allocation_trace_reports_ready': True,
        'before_after_allocation_comparison_ready': True,
    }
    return {'batch': 'B44', 'title': 'Profitability And Allocation Management', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def workspace(scenario_id: int) -> dict[str, Any]:
    return {
        'scenario_id': scenario_id,
        'status': status(),
        'cost_pools': list_cost_pools(scenario_id),
        'allocation_runs': list_allocation_runs(scenario_id),
        'trace_lines': list_trace_lines(scenario_id),
        'program_margins': program_margin_report(scenario_id)['rows'],
        'fund_profitability': fund_profitability_report(scenario_id)['rows'],
        'grant_profitability': grant_profitability_report(scenario_id)['rows'],
        'before_after': before_after_allocation_comparison(scenario_id)['rows'],
        'snapshots': list_snapshots(scenario_id),
    }


def upsert_cost_pool(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO profitability_cost_pools (
            scenario_id, pool_key, name, source_department_code, source_account_code,
            allocation_basis, target_type, target_codes_json, active, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, pool_key) DO UPDATE SET
            name = excluded.name,
            source_department_code = excluded.source_department_code,
            source_account_code = excluded.source_account_code,
            allocation_basis = excluded.allocation_basis,
            target_type = excluded.target_type,
            target_codes_json = excluded.target_codes_json,
            active = excluded.active,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        ''',
        (
            payload['scenario_id'], payload['pool_key'], payload['name'], payload['source_department_code'],
            payload['source_account_code'], payload.get('allocation_basis') or 'revenue', payload.get('target_type') or 'department',
            json.dumps(payload.get('target_codes') or [], sort_keys=True), 1 if payload.get('active', True) else 0, user['email'], now,
        ),
    )
    row = _one('SELECT * FROM profitability_cost_pools WHERE scenario_id = ? AND pool_key = ?', (payload['scenario_id'], payload['pool_key']))
    db.log_audit('profitability_cost_pool', str(row['id']), 'upserted', user['email'], payload, now)
    return _format_cost_pool(row)


def list_cost_pools(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM profitability_cost_pools WHERE scenario_id = ? ORDER BY pool_key', (scenario_id,))
    return [_format_cost_pool(row) for row in rows]


def run_service_center_allocation(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    pools = [row for row in list_cost_pools(payload['scenario_id']) if row['active']]
    requested = set(payload.get('pool_keys') or [])
    if requested:
        pools = [row for row in pools if row['pool_key'] in requested]
    if not pools:
        raise ValueError('No active cost pools found.')
    now = _now()
    run_key = f"profit-{payload['period']}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    run_id = db.execute(
        '''
        INSERT INTO profitability_allocation_runs (
            scenario_id, period, run_key, status, total_source_cost, total_allocated_cost, created_by, created_at
        ) VALUES (?, ?, ?, 'running', 0, 0, ?, ?)
        ''',
        (payload['scenario_id'], payload['period'], run_key, user['email'], now),
    )
    source_total = 0.0
    allocated_total = 0.0
    created = []
    for pool in pools:
        amount = abs(_ledger_total(payload['scenario_id'], payload['period'], pool['source_account_code'], pool['source_department_code']))
        source_total = round(source_total + amount, 2)
        targets = pool['target_codes'] or _target_codes(payload['scenario_id'], payload['period'], pool['target_type'], pool['source_department_code'])
        weights = _target_weights(payload['scenario_id'], payload['period'], pool, targets)
        total_weight = sum(weights.values()) or float(len(targets) or 1)
        for target in targets:
            basis = weights.get(target, 1.0)
            percent = basis / total_weight if total_weight else 0.0
            allocated = round(amount * percent, 2)
            allocated_total = round(allocated_total + allocated, 2)
            line_id = db.execute(
                '''
                INSERT INTO profitability_allocation_trace_lines (
                    allocation_run_id, scenario_id, period, pool_key, source_department_code,
                    source_account_code, target_type, target_code, basis_value, allocation_percent,
                    allocated_amount, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    run_id, payload['scenario_id'], payload['period'], pool['pool_key'], pool['source_department_code'],
                    pool['source_account_code'], pool['target_type'], target, basis, round(percent, 6), allocated, now,
                ),
            )
            created.append(_one('SELECT * FROM profitability_allocation_trace_lines WHERE id = ?', (line_id,)))
    db.execute(
        '''
        UPDATE profitability_allocation_runs
        SET status = 'posted', total_source_cost = ?, total_allocated_cost = ?
        WHERE id = ?
        ''',
        (round(source_total, 2), round(allocated_total, 2), run_id),
    )
    db.log_audit('profitability_allocation_run', str(run_id), 'posted', user['email'], {'period': payload['period'], 'trace_count': len(created)}, now)
    result = get_allocation_run(run_id)
    result['trace_lines'] = created
    return result


def list_allocation_runs(scenario_id: int) -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM profitability_allocation_runs WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))


def get_allocation_run(run_id: int) -> dict[str, Any]:
    return _one('SELECT * FROM profitability_allocation_runs WHERE id = ?', (run_id,))


def list_trace_lines(scenario_id: int, run_id: int | None = None) -> list[dict[str, Any]]:
    if run_id:
        return db.fetch_all('SELECT * FROM profitability_allocation_trace_lines WHERE scenario_id = ? AND allocation_run_id = ? ORDER BY id', (scenario_id, run_id))
    return db.fetch_all('SELECT * FROM profitability_allocation_trace_lines WHERE scenario_id = ? ORDER BY id DESC LIMIT 200', (scenario_id,))


def program_margin_report(scenario_id: int, period_start: str | None = None, period_end: str | None = None) -> dict[str, Any]:
    rows = _ledger_grouped(scenario_id, 'program_code', period_start, period_end, include_null=False)
    allocations = _allocated_by_target(scenario_id, 'program')
    result = []
    for key, bucket in rows.items():
        allocated = allocations.get(key, 0.0)
        net_after = round(bucket['revenue'] + bucket['expense'] - allocated, 2)
        result.append({**bucket, 'program_code': key, 'allocated_cost': allocated, 'net_after_allocation': net_after, 'margin_percent': _margin(net_after, bucket['revenue'])})
    return {'scenario_id': scenario_id, 'period_start': period_start, 'period_end': period_end, 'rows': sorted(result, key=lambda item: item['program_code'])}


def fund_profitability_report(scenario_id: int, period_start: str | None = None, period_end: str | None = None) -> dict[str, Any]:
    rows = _ledger_grouped(scenario_id, 'fund_code', period_start, period_end, include_null=True)
    allocations = _allocated_by_target(scenario_id, 'fund')
    result = []
    for key, bucket in rows.items():
        allocated = allocations.get(key, 0.0)
        net_after = round(bucket['revenue'] + bucket['expense'] - allocated, 2)
        result.append({**bucket, 'fund_code': key, 'allocated_cost': allocated, 'net_after_allocation': net_after, 'margin_percent': _margin(net_after, bucket['revenue'])})
    return {'scenario_id': scenario_id, 'period_start': period_start, 'period_end': period_end, 'rows': sorted(result, key=lambda item: item['fund_code'])}


def grant_profitability_report(scenario_id: int) -> dict[str, Any]:
    grants = db.fetch_all('SELECT * FROM grant_budgets WHERE scenario_id = ? ORDER BY grant_code', (scenario_id,))
    ledger = _ledger_grouped(scenario_id, 'grant_code', None, None, include_null=False)
    allocations = _allocated_by_target(scenario_id, 'grant')
    rows = []
    for grant in grants:
        key = grant['grant_code']
        bucket = ledger.get(key, {'revenue': float(grant['total_award']), 'expense': -float(grant['spent_to_date'])})
        allocated = allocations.get(key, 0.0)
        remaining = round(float(grant['total_award']) - float(grant['spent_to_date']) - allocated, 2)
        rows.append({'grant_code': key, 'department_code': grant['department_code'], 'sponsor': grant['sponsor'], 'award': float(grant['total_award']), 'spent': float(grant['spent_to_date']), 'allocated_cost': allocated, 'remaining_after_allocation': remaining, 'burn_rate_after_allocation': round((float(grant['spent_to_date']) + allocated) / max(1.0, float(grant['total_award'])), 4)})
    return {'scenario_id': scenario_id, 'rows': rows}


def before_after_allocation_comparison(scenario_id: int) -> dict[str, Any]:
    base = _ledger_grouped(scenario_id, 'department_code', None, None, include_null=True)
    allocations = _allocated_by_target(scenario_id, 'department')
    rows = []
    for department, bucket in sorted(base.items()):
        allocated = allocations.get(department, 0.0)
        before = round(bucket['revenue'] + bucket['expense'], 2)
        after = round(before - allocated, 2)
        rows.append({'department_code': department, 'before_allocation': before, 'allocated_cost': allocated, 'after_allocation': after, 'change': round(after - before, 2)})
    return {'scenario_id': scenario_id, 'rows': rows}


def create_snapshot(scenario_id: int, period_start: str, period_end: str, snapshot_type: str, user: dict[str, Any]) -> dict[str, Any]:
    contents = {
        'program_margin': program_margin_report(scenario_id, period_start, period_end),
        'fund_profitability': fund_profitability_report(scenario_id, period_start, period_end),
        'grant_profitability': grant_profitability_report(scenario_id),
        'before_after': before_after_allocation_comparison(scenario_id),
    }
    key = f"{snapshot_type}-{period_start}-{period_end}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    snapshot_id = db.execute(
        '''
        INSERT INTO profitability_snapshots (
            scenario_id, period_start, period_end, snapshot_key, snapshot_type, contents_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (scenario_id, period_start, period_end, key, snapshot_type, json.dumps(contents, sort_keys=True), user['email'], _now()),
    )
    return _format_snapshot(_one('SELECT * FROM profitability_snapshots WHERE id = ?', (snapshot_id,)))


def list_snapshots(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM profitability_snapshots WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    return [_format_snapshot(row) for row in rows]


def _target_codes(scenario_id: int, period: str, target_type: str, source_department: str) -> list[str]:
    column = {'department': 'department_code', 'program': 'program_code', 'fund': 'fund_code', 'grant': 'grant_code'}[target_type]
    null_filter = f'AND {column} IS NOT NULL' if target_type in {'program', 'grant'} else ''
    rows = db.fetch_all(
        f'''
        SELECT DISTINCT {column} AS code
        FROM planning_ledger
        WHERE scenario_id = ? AND period = ? AND reversed_at IS NULL
          AND {column} IS NOT NULL AND {column} != '' AND department_code != ?
          {null_filter}
        ORDER BY {column}
        ''',
        (scenario_id, period, source_department),
    )
    return [str(row['code']) for row in rows]


def _target_weights(scenario_id: int, period: str, pool: dict[str, Any], targets: list[str]) -> dict[str, float]:
    if pool['allocation_basis'] == 'equal':
        return {target: 1.0 for target in targets}
    if pool['allocation_basis'] == 'headcount':
        return _headcount_weights(scenario_id, targets)
    column = {'department': 'department_code', 'program': 'program_code', 'fund': 'fund_code', 'grant': 'grant_code'}[pool['target_type']]
    operator = '>=' if pool['allocation_basis'] == 'revenue' else '<'
    weights = {}
    for target in targets:
        row = db.fetch_one(
            f'''
            SELECT COALESCE(SUM(ABS(amount)), 0) AS amount
            FROM planning_ledger
            WHERE scenario_id = ? AND period = ? AND {column} = ? AND amount {operator} 0 AND reversed_at IS NULL
            ''',
            (scenario_id, period, target),
        )
        weights[target] = float(row['amount'] if row else 0) or 1.0
    return weights


def _headcount_weights(scenario_id: int, targets: list[str]) -> dict[str, float]:
    rows = db.fetch_all('SELECT program_code, COALESCE(SUM(headcount), 0) AS headcount FROM enrollment_forecast_inputs WHERE scenario_id = ? GROUP BY program_code', (scenario_id,))
    values = {row['program_code']: float(row['headcount']) for row in rows}
    return {target: values.get(target, 1.0) or 1.0 for target in targets}


def _ledger_total(scenario_id: int, period: str, account_code: str, department_code: str) -> float:
    row = db.fetch_one(
        '''
        SELECT COALESCE(SUM(amount), 0) AS amount
        FROM planning_ledger
        WHERE scenario_id = ? AND period = ? AND account_code = ? AND department_code = ? AND reversed_at IS NULL
        ''',
        (scenario_id, period, account_code, department_code),
    )
    return round(float(row['amount'] if row else 0), 2)


def _ledger_grouped(scenario_id: int, dimension: str, period_start: str | None, period_end: str | None, include_null: bool) -> dict[str, dict[str, float]]:
    where = ['scenario_id = ?', 'reversed_at IS NULL']
    params: list[Any] = [scenario_id]
    if period_start:
        where.append('period >= ?')
        params.append(period_start)
    if period_end:
        where.append('period <= ?')
        params.append(period_end)
    if not include_null:
        where.append(f"{dimension} IS NOT NULL AND {dimension} != ''")
    rows = db.fetch_all(
        f'''
        SELECT COALESCE({dimension}, 'UNASSIGNED') AS key, amount
        FROM planning_ledger
        WHERE {' AND '.join(where)}
        ''',
        tuple(params),
    )
    grouped: dict[str, dict[str, float]] = {}
    for row in rows:
        key = str(row['key'] or 'UNASSIGNED')
        bucket = grouped.setdefault(key, {'revenue': 0.0, 'expense': 0.0})
        amount = float(row['amount'])
        if amount >= 0:
            bucket['revenue'] = round(bucket['revenue'] + amount, 2)
        else:
            bucket['expense'] = round(bucket['expense'] + amount, 2)
    return grouped


def _allocated_by_target(scenario_id: int, target_type: str) -> dict[str, float]:
    rows = db.fetch_all(
        '''
        SELECT target_code, COALESCE(SUM(allocated_amount), 0) AS amount
        FROM profitability_allocation_trace_lines
        WHERE scenario_id = ? AND target_type = ?
        GROUP BY target_code
        ''',
        (scenario_id, target_type),
    )
    return {str(row['target_code']): round(float(row['amount']), 2) for row in rows}


def _margin(net: float, revenue: float) -> float:
    if revenue == 0:
        return 0.0
    return round(net / abs(revenue), 4)


def _format_cost_pool(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['target_codes'] = json.loads(result.pop('target_codes_json') or '[]')
    result['active'] = bool(result['active'])
    return result


def _format_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['contents'] = json.loads(result.pop('contents_json') or '{}')
    return result


def _one(query: str, params: tuple[Any, ...]) -> dict[str, Any]:
    row = db.fetch_one(query, params)
    if row is None:
        raise ValueError('Record not found.')
    return row
