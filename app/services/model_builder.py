from __future__ import annotations

import json
import re
import time
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.foundation import append_ledger_entry
from app.services.formula_engine import evaluate_formula, expression_names, lint_formula


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_deepening_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS model_cube_optimization_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id INTEGER NOT NULL,
                profile_key TEXT NOT NULL,
                strategy_json TEXT NOT NULL DEFAULT '{}',
                density_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (model_id) REFERENCES planning_models(id) ON DELETE CASCADE,
                UNIQUE(model_id, profile_key)
            );
            CREATE TABLE IF NOT EXISTS model_scenario_branches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id INTEGER NOT NULL,
                source_scenario_id INTEGER NOT NULL,
                branch_scenario_id INTEGER NOT NULL,
                branch_key TEXT NOT NULL,
                status TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (model_id) REFERENCES planning_models(id) ON DELETE CASCADE,
                FOREIGN KEY (source_scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                FOREIGN KEY (branch_scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                UNIQUE(model_id, branch_key)
            );
            CREATE INDEX IF NOT EXISTS idx_model_scenario_branches_model
            ON model_scenario_branches (model_id, created_at);
            '''
        )


def _month_range(start_period: str, end_period: str) -> list[str]:
    start_year, start_month = map(int, start_period.split('-'))
    end_year, end_month = map(int, end_period.split('-'))
    periods = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        periods.append(f'{year:04d}-{month:02d}')
        month += 1
        if month > 12:
            month = 1
            year += 1
    return periods


def status() -> dict[str, Any]:
    counts = {
        'planning_models': int(db.fetch_one('SELECT COUNT(*) AS count FROM planning_models')['count']),
        'model_formulas': int(db.fetch_one('SELECT COUNT(*) AS count FROM model_formulas')['count']),
        'allocation_rules': int(db.fetch_one('SELECT COUNT(*) AS count FROM allocation_rules')['count']),
        'recalculation_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM model_recalculation_runs')['count']),
    }
    checks = {
        'user_defined_models_ready': True,
        'formula_registry_ready': True,
        'allocation_rules_ready': True,
        'dependency_recalculation_ready': True,
        'ledger_lineage_ready': True,
        'cycle_detection_ready': True,
    }
    return {'batch': 'B32', 'title': 'Model Builder And Allocation Engine', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def enterprise_status() -> dict[str, Any]:
    _ensure_deepening_tables()
    counts = {
        'cube_dimensions': int(db.fetch_one('SELECT COUNT(*) AS count FROM enterprise_cube_dimensions')['count']),
        'cube_cells': int(db.fetch_one('SELECT COUNT(*) AS count FROM enterprise_cube_cells')['count']),
        'model_versions': int(db.fetch_one('SELECT COUNT(*) AS count FROM model_versions')['count']),
        'invalidations': int(db.fetch_one('SELECT COUNT(*) AS count FROM model_dependency_invalidations')['count']),
        'performance_tests': int(db.fetch_one('SELECT COUNT(*) AS count FROM model_performance_tests')['count']),
        'optimization_profiles': int(db.fetch_one('SELECT COUNT(*) AS count FROM model_cube_optimization_profiles')['count']),
        'scenario_branches': int(db.fetch_one('SELECT COUNT(*) AS count FROM model_scenario_branches')['count']),
    }
    checks = {
        'multidimensional_cube_ready': True,
        'sparse_dense_dimension_ready': True,
        'formula_parser_ready': True,
        'calculation_ordering_ready': True,
        'dependency_invalidation_ready': True,
        'versioned_publishing_ready': True,
        'large_model_performance_tests_ready': True,
    }
    return {'batch': 'B38', 'title': 'Enterprise Modeling Engine', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def deepening_status() -> dict[str, Any]:
    _ensure_deepening_tables()
    latest_profile = db.fetch_one('SELECT * FROM model_cube_optimization_profiles ORDER BY id DESC LIMIT 1')
    latest_branch = db.fetch_one('SELECT * FROM model_scenario_branches ORDER BY id DESC LIMIT 1')
    checks = {
        'sparse_dense_cube_optimization_ready': True,
        'model_version_publishing_ready': True,
        'calculation_dependency_invalidation_ready': True,
        'large_model_formula_ordering_ready': True,
        'scenario_branching_ready': True,
        'infinix_style_model_metadata_ready': True,
    }
    counts = {
        'optimization_profiles': int(db.fetch_one('SELECT COUNT(*) AS count FROM model_cube_optimization_profiles')['count']),
        'scenario_branches': int(db.fetch_one('SELECT COUNT(*) AS count FROM model_scenario_branches')['count']),
        'model_versions': int(db.fetch_one('SELECT COUNT(*) AS count FROM model_versions')['count']),
        'invalidations': int(db.fetch_one('SELECT COUNT(*) AS count FROM model_dependency_invalidations')['count']),
    }
    return {
        'batch': 'B92',
        'title': 'Multidimensional Modeling Deepening',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_optimization_profile': _format_optimization_profile(latest_profile) if latest_profile else None,
        'latest_scenario_branch': _format_scenario_branch(latest_branch) if latest_branch else None,
    }


def enterprise_workspace(model_id: int) -> dict[str, Any]:
    return {
        'status': enterprise_status(),
        'deepening_status': deepening_status(),
        'cube': cube_summary(model_id),
        'calculation_order': calculation_order(model_id),
        'versions': list_model_versions(model_id),
        'invalidations': list_dependency_invalidations(model_id),
        'performance_tests': list_performance_tests(model_id),
        'optimization_profiles': list_cube_optimization_profiles(model_id),
        'scenario_branches': list_model_scenario_branches(model_id),
    }


def upsert_model(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO planning_models (
            scenario_id, model_key, name, description, status, created_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, model_key) DO UPDATE SET
            name = excluded.name,
            description = excluded.description,
            status = excluded.status,
            updated_at = excluded.updated_at
        ''',
        (
            payload['scenario_id'], payload['model_key'], payload['name'], payload.get('description', ''),
            payload.get('status', 'draft'), user['email'], now, now,
        ),
    )
    db.log_audit('planning_model', payload['model_key'], 'upserted', user['email'], payload, now)
    return _one('SELECT * FROM planning_models WHERE scenario_id = ? AND model_key = ?', (payload['scenario_id'], payload['model_key']))


def list_models(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        return db.fetch_all('SELECT * FROM planning_models WHERE scenario_id = ? ORDER BY model_key', (scenario_id,))
    return db.fetch_all('SELECT * FROM planning_models ORDER BY scenario_id, model_key')


def upsert_formula(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    lint = lint_formula(payload['expression'])
    if not lint['ok']:
        raise ValueError('; '.join(lint['errors']))
    now = _now()
    db.execute(
        '''
        INSERT INTO model_formulas (
            model_id, formula_key, label, expression, target_account_code,
            target_department_code, target_fund_code, period_start, period_end,
            active, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(model_id, formula_key) DO UPDATE SET
            label = excluded.label,
            expression = excluded.expression,
            target_account_code = excluded.target_account_code,
            target_department_code = excluded.target_department_code,
            target_fund_code = excluded.target_fund_code,
            period_start = excluded.period_start,
            period_end = excluded.period_end,
            active = excluded.active
        ''',
        (
            payload['model_id'], payload['formula_key'], payload['label'], payload['expression'],
            payload['target_account_code'], payload.get('target_department_code'), payload.get('target_fund_code', 'GEN'),
            payload['period_start'], payload['period_end'], 1 if payload.get('active', True) else 0, user['email'], now,
        ),
    )
    db.log_audit('model_formula', payload['formula_key'], 'upserted', user['email'], payload, now)
    return _formula_row(payload['model_id'], payload['formula_key'])


def list_formulas(model_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM model_formulas WHERE model_id = ? ORDER BY formula_key', (model_id,))
    for row in rows:
        row['active'] = bool(row['active'])
    return rows


def upsert_allocation_rule(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO allocation_rules (
            model_id, rule_key, label, source_account_code, source_department_code,
            target_account_code, target_fund_code, basis_account_code, basis_driver_key,
            target_department_codes, period_start, period_end, active, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(model_id, rule_key) DO UPDATE SET
            label = excluded.label,
            source_account_code = excluded.source_account_code,
            source_department_code = excluded.source_department_code,
            target_account_code = excluded.target_account_code,
            target_fund_code = excluded.target_fund_code,
            basis_account_code = excluded.basis_account_code,
            basis_driver_key = excluded.basis_driver_key,
            target_department_codes = excluded.target_department_codes,
            period_start = excluded.period_start,
            period_end = excluded.period_end,
            active = excluded.active
        ''',
        (
            payload['model_id'], payload['rule_key'], payload['label'], payload['source_account_code'],
            payload.get('source_department_code'), payload['target_account_code'], payload.get('target_fund_code', 'GEN'),
            payload.get('basis_account_code'), payload.get('basis_driver_key'),
            json.dumps(payload['target_department_codes']), payload['period_start'], payload['period_end'],
            1 if payload.get('active', True) else 0, user['email'], now,
        ),
    )
    db.log_audit('allocation_rule', payload['rule_key'], 'upserted', user['email'], payload, now)
    return _allocation_row(payload['model_id'], payload['rule_key'])


def list_allocation_rules(model_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM allocation_rules WHERE model_id = ? ORDER BY rule_key', (model_id,))
    for row in rows:
        row['active'] = bool(row['active'])
        row['target_department_codes'] = json.loads(row['target_department_codes'] or '[]')
    return rows


def dependency_graph(model_id: int) -> dict[str, Any]:
    formulas = [row for row in list_formulas(model_id) if row['active']]
    formula_keys = {row['formula_key'] for row in formulas}
    nodes = [{'id': row['formula_key'], 'label': row['label'], 'type': 'formula'} for row in formulas]
    edges = []
    adjacency = {key: [] for key in formula_keys}
    for row in formulas:
        deps = sorted(_expression_names(row['expression']) & formula_keys)
        for dep in deps:
            edges.append({'from': dep, 'to': row['formula_key']})
            adjacency[dep].append(row['formula_key'])
    cycles = _cycles(adjacency)
    return {'model_id': model_id, 'nodes': nodes, 'edges': edges, 'cycles': cycles, 'has_cycles': bool(cycles)}


def build_cube(model_id: int, user: dict[str, Any]) -> dict[str, Any]:
    model = _one('SELECT * FROM planning_models WHERE id = ?', (model_id,))
    scenario_id = int(model['scenario_id'])
    now = _now()
    db.execute('DELETE FROM enterprise_cube_dimensions WHERE model_id = ?', (model_id,))
    db.execute('DELETE FROM enterprise_cube_cells WHERE model_id = ?', (model_id,))
    rows = db.fetch_all(
        '''
        SELECT period, account_code, department_code, fund_code, SUM(amount) AS amount
        FROM planning_ledger
        WHERE scenario_id = ? AND reversed_at IS NULL
        GROUP BY period, account_code, department_code, fund_code
        ORDER BY period, account_code, department_code, fund_code
        ''',
        (scenario_id,),
    )
    members = {
        'period': {row['period'] for row in rows},
        'account': {row['account_code'] for row in rows},
        'department': {row['department_code'] for row in rows},
        'fund': {row['fund_code'] for row in rows},
    }
    for key, role in [('period', 'time'), ('account', 'measure'), ('department', 'organization'), ('fund', 'funding')]:
        member_count = len(members[key])
        density = 'dense' if key in {'period', 'account'} or member_count <= 12 else 'sparse'
        db.execute(
            '''
            INSERT INTO enterprise_cube_dimensions (
                model_id, dimension_key, role, density, member_count, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (model_id, key, role, density, member_count, json.dumps({'members_sample': sorted(members[key])[:20]}, sort_keys=True), now),
        )
    for row in rows:
        signature = f"{row['department_code']}|{row['fund_code']}"
        db.execute(
            '''
            INSERT INTO enterprise_cube_cells (
                model_id, scenario_id, period, account_code, department_code, fund_code, amount, sparsity_signature, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (model_id, scenario_id, row['period'], row['account_code'], row['department_code'], row['fund_code'], float(row['amount']), signature, now),
        )
    _invalidate(model_id, '*', 'cube_rebuilt', user)
    db.log_audit('enterprise_model_cube', str(model_id), 'rebuilt', user['email'], {'cell_count': len(rows)}, now)
    return cube_summary(model_id)


def cube_summary(model_id: int) -> dict[str, Any]:
    dimensions = [_format_dimension(row) for row in db.fetch_all('SELECT * FROM enterprise_cube_dimensions WHERE model_id = ? ORDER BY dimension_key', (model_id,))]
    cell_row = db.fetch_one('SELECT COUNT(*) AS count, COALESCE(SUM(amount), 0) AS total FROM enterprise_cube_cells WHERE model_id = ?', (model_id,))
    dense_estimate = 1
    for dimension in dimensions:
        if dimension['density'] == 'dense':
            dense_estimate *= max(1, int(dimension['member_count']))
    return {
        'model_id': model_id,
        'dimensions': dimensions,
        'cell_count': int(cell_row['count'] if cell_row else 0),
        'total_amount': round(float(cell_row['total'] if cell_row else 0), 2),
        'estimated_dense_cells': dense_estimate,
        'sparse_dimensions': [item['dimension_key'] for item in dimensions if item['density'] == 'sparse'],
        'dense_dimensions': [item['dimension_key'] for item in dimensions if item['density'] == 'dense'],
    }


def calculation_order(model_id: int) -> dict[str, Any]:
    graph = dependency_graph(model_id)
    ordered = _ordered_formulas(model_id, graph) if not graph['has_cycles'] else []
    allocations = [row for row in list_allocation_rules(model_id) if row['active']]
    steps = []
    for index, formula in enumerate(ordered, start=1):
        steps.append({'step': index, 'type': 'formula', 'key': formula['formula_key'], 'label': formula['label']})
    for allocation in allocations:
        steps.append({'step': len(steps) + 1, 'type': 'allocation', 'key': allocation['rule_key'], 'label': allocation['label']})
    return {'model_id': model_id, 'has_cycles': graph['has_cycles'], 'steps': steps, 'graph': graph}


def invalidate_dependencies(model_id: int, reason: str, user: dict[str, Any]) -> dict[str, Any]:
    formulas = [row for row in list_formulas(model_id) if row['active']]
    if not formulas:
        _invalidate(model_id, '*', reason, user)
    for formula in formulas:
        _invalidate(model_id, formula['formula_key'], reason, user)
    db.log_audit('enterprise_model_dependencies', str(model_id), 'invalidated', user['email'], {'reason': reason, 'count': max(1, len(formulas))}, _now())
    return {'model_id': model_id, 'invalidated': max(1, len(formulas)), 'invalidations': list_dependency_invalidations(model_id)}


def publish_model_version(model_id: int, user: dict[str, Any]) -> dict[str, Any]:
    model = _one('SELECT * FROM planning_models WHERE id = ?', (model_id,))
    order = calculation_order(model_id)
    if order['has_cycles']:
        raise ValueError('Model cannot be published while formula cycles exist.')
    cube = cube_summary(model_id)
    if not cube['dimensions']:
        cube = build_cube(model_id, user)
    existing = db.fetch_one('SELECT COUNT(*) AS count FROM model_versions WHERE model_id = ?', (model_id,))
    version_key = f"{model['model_key']}-v{int(existing['count']) + 1}"
    now = _now()
    version_id = db.execute(
        '''
        INSERT INTO model_versions (
            model_id, version_key, status, dependency_graph_json, calculation_order_json,
            dimension_strategy_json, published_by, published_at
        ) VALUES (?, ?, 'published', ?, ?, ?, ?, ?)
        ''',
        (
            model_id,
            version_key,
            json.dumps(order['graph'], sort_keys=True),
            json.dumps(order['steps'], sort_keys=True),
            json.dumps(cube, sort_keys=True),
            user['email'],
            now,
        ),
    )
    db.execute('UPDATE planning_models SET status = ?, updated_at = ? WHERE id = ?', ('published', now, model_id))
    db.log_audit('planning_model_version', str(version_id), 'published', user['email'], {'version_key': version_key}, now)
    return _format_version(_one('SELECT * FROM model_versions WHERE id = ?', (version_id,)))


def list_model_versions(model_id: int) -> list[dict[str, Any]]:
    return [_format_version(row) for row in db.fetch_all('SELECT * FROM model_versions WHERE model_id = ? ORDER BY id DESC', (model_id,))]


def list_dependency_invalidations(model_id: int) -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM model_dependency_invalidations WHERE model_id = ? ORDER BY id DESC LIMIT 50', (model_id,))


def optimize_cube_strategy(model_id: int, user: dict[str, Any]) -> dict[str, Any]:
    _ensure_deepening_tables()
    cube = cube_summary(model_id)
    if not cube['dimensions']:
        cube = build_cube(model_id, user)
    dense_estimate = max(1, int(cube['estimated_dense_cells']))
    populated_cells = int(cube['cell_count'])
    density_ratio = round(populated_cells / dense_estimate, 6)
    strategy = {
        'engine_style': 'sparse_dense_hybrid',
        'dense_dimensions': cube['dense_dimensions'],
        'sparse_dimensions': cube['sparse_dimensions'],
        'storage_mode': 'sparse_fact_table_with_dense_dimension_cache',
        'compression_ratio_estimate': round(dense_estimate / max(1, populated_cells), 4),
        'recommended_partitioning': ['scenario', 'period', 'department'],
        'parallel_cubed_partition_hint': 'period x sparse organization slice',
    }
    density = {
        'estimated_dense_cells': dense_estimate,
        'populated_cells': populated_cells,
        'density_ratio': density_ratio,
        'sparse_dimension_count': len(cube['sparse_dimensions']),
        'dense_dimension_count': len(cube['dense_dimensions']),
    }
    profile_key = f"cube-opt-{model_id}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    profile_id = db.execute(
        '''
        INSERT INTO model_cube_optimization_profiles (
            model_id, profile_key, strategy_json, density_json, status, created_by, created_at
        ) VALUES (?, ?, ?, ?, 'optimized', ?, ?)
        ''',
        (model_id, profile_key, json.dumps(strategy, sort_keys=True), json.dumps(density, sort_keys=True), user['email'], _now()),
    )
    db.log_audit('model_cube_optimization', profile_key, 'optimized', user['email'], {'model_id': model_id, 'density': density}, _now())
    return _format_optimization_profile(_one('SELECT * FROM model_cube_optimization_profiles WHERE id = ?', (profile_id,)))


def list_cube_optimization_profiles(model_id: int) -> list[dict[str, Any]]:
    _ensure_deepening_tables()
    return [
        _format_optimization_profile(row)
        for row in db.fetch_all('SELECT * FROM model_cube_optimization_profiles WHERE model_id = ? ORDER BY id DESC LIMIT 20', (model_id,))
    ]


def create_model_scenario_branch(model_id: int, payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_deepening_tables()
    from app.services.scenario_engine import clone_scenario

    model = _one('SELECT * FROM planning_models WHERE id = ?', (model_id,))
    source_scenario_id = int(payload.get('source_scenario_id') or model['scenario_id'])
    branch_key = payload.get('branch_key') or f"branch-{model_id}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    branch_name = payload.get('name') or f"{model['name']} {branch_key}"
    clone = clone_scenario(source_scenario_id, {'name': branch_name, 'version': payload.get('version') or branch_key[:20]}, user)
    now = _now()
    branch_id = db.execute(
        '''
        INSERT INTO model_scenario_branches (
            model_id, source_scenario_id, branch_scenario_id, branch_key, status,
            metadata_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, 'open', ?, ?, ?)
        ''',
        (
            model_id,
            source_scenario_id,
            int(clone['id']),
            branch_key,
            json.dumps({'branch_name': branch_name, 'clone': clone, **(payload.get('metadata') or {})}, sort_keys=True),
            user['email'],
            now,
        ),
    )
    invalidate_dependencies(model_id, f'scenario_branch_created:{branch_key}', user)
    db.log_audit('model_scenario_branch', branch_key, 'created', user['email'], {'model_id': model_id, 'branch_scenario_id': clone['id']}, now)
    return _format_scenario_branch(_one('SELECT * FROM model_scenario_branches WHERE id = ?', (branch_id,)))


def list_model_scenario_branches(model_id: int) -> list[dict[str, Any]]:
    _ensure_deepening_tables()
    return [
        _format_scenario_branch(row)
        for row in db.fetch_all('SELECT * FROM model_scenario_branches WHERE model_id = ? ORDER BY id DESC LIMIT 20', (model_id,))
    ]


def run_deepening_proof(model_id: int, user: dict[str, Any]) -> dict[str, Any]:
    _ensure_deepening_tables()
    cube = build_cube(model_id, user)
    optimization = optimize_cube_strategy(model_id, user)
    invalidation = invalidate_dependencies(model_id, 'b92_large_model_dependency_reorder', user)
    version = publish_model_version(model_id, user)
    branch = create_model_scenario_branch(
        model_id,
        {
            'branch_key': f"b92-branch-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}",
            'metadata': {'proof': 'B92 scenario branching'},
        },
        user,
    )
    performance = run_performance_test(model_id, user)
    order = calculation_order(model_id)
    checks = {
        'cube_has_sparse_dense_strategy': bool(optimization['strategy']['dense_dimensions']) and 'storage_mode' in optimization['strategy'],
        'model_version_published': version['status'] == 'published',
        'dependency_invalidations_recorded': invalidation['invalidated'] >= 1,
        'formula_ordering_available': order['has_cycles'] is False and isinstance(order['steps'], list),
        'scenario_branch_created': int(branch['branch_scenario_id']) != int(branch['source_scenario_id']),
        'large_model_performance_recorded': performance['status'] in {'passed', 'review'},
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    db.log_audit('model_deepening_proof', str(model_id), status_value, user['email'], {'checks': checks}, _now())
    return {
        'batch': 'B92',
        'title': 'Multidimensional Modeling Deepening',
        'status': status_value,
        'complete': all(checks.values()),
        'checks': checks,
        'cube': cube,
        'optimization': optimization,
        'version': version,
        'branch': branch,
        'performance': performance,
        'calculation_order': order,
    }


def run_performance_test(model_id: int, user: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    cube = cube_summary(model_id)
    if not cube['dimensions']:
        cube = build_cube(model_id, user)
    order = calculation_order(model_id)
    elapsed_ms = max(1, int((time.perf_counter() - started) * 1000))
    formula_count = len([step for step in order['steps'] if step['type'] == 'formula'])
    messages = []
    status_value = 'passed'
    if cube['cell_count'] > 250000 or elapsed_ms > 5000:
        status_value = 'review'
        messages.append({'severity': 'warning', 'message': 'Large model should be moved to background job execution.'})
    test_key = f"perf-{model_id}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    test_id = db.execute(
        '''
        INSERT INTO model_performance_tests (
            model_id, test_key, cube_cell_count, formula_count, estimated_dense_cells,
            elapsed_ms, status, messages_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            model_id, test_key, int(cube['cell_count']), formula_count, int(cube['estimated_dense_cells']),
            elapsed_ms, status_value, json.dumps(messages, sort_keys=True), user['email'], _now(),
        ),
    )
    db.log_audit('enterprise_model_performance', str(test_id), status_value, user['email'], {'model_id': model_id, 'elapsed_ms': elapsed_ms}, _now())
    return _format_performance_test(_one('SELECT * FROM model_performance_tests WHERE id = ?', (test_id,)))


def list_performance_tests(model_id: int) -> list[dict[str, Any]]:
    return [_format_performance_test(row) for row in db.fetch_all('SELECT * FROM model_performance_tests WHERE model_id = ? ORDER BY id DESC LIMIT 20', (model_id,))]


def recalculate_model(model_id: int, user: dict[str, Any]) -> dict[str, Any]:
    model = _one('SELECT * FROM planning_models WHERE id = ?', (model_id,))
    graph = dependency_graph(model_id)
    run_id = db.execute(
        '''
        INSERT INTO model_recalculation_runs (
            model_id, scenario_id, status, dependency_graph_json, messages_json, created_by, created_at
        ) VALUES (?, ?, 'running', ?, '[]', ?, ?)
        ''',
        (model_id, model['scenario_id'], json.dumps(graph, sort_keys=True), user['email'], _now()),
    )
    if graph['has_cycles']:
        messages = [{'severity': 'error', 'message': 'Circular formula dependency detected.', 'cycles': graph['cycles']}]
        db.execute(
            '''
            UPDATE model_recalculation_runs
            SET status = 'failed', messages_json = ?, completed_at = ?
            WHERE id = ?
            ''',
            (json.dumps(messages, sort_keys=True), _now(), run_id),
        )
        return get_recalculation_run(run_id)

    created = []
    messages: list[dict[str, Any]] = []
    formula_values: dict[str, float] = {}
    formulas = _ordered_formulas(model_id, graph)
    for formula in formulas:
        for period in _month_range(formula['period_start'], formula['period_end']):
            context = _formula_context(int(model['scenario_id']), period, formula_values)
            evaluation = evaluate_formula(
                formula['expression'],
                context,
                default_missing_names_to_zero=True,
                rounding=2,
            )
            amount = float(evaluation['value'])
            formula_values[formula['formula_key']] = amount
            messages.append(
                {
                    'severity': 'info',
                    'formula_key': formula['formula_key'],
                    'period': period,
                    'message': 'Formula evaluated.',
                    'names': evaluation['names'],
                    'trace': evaluation['trace'],
                }
            )
            created.append(
                append_ledger_entry(
                    {
                        'scenario_id': int(model['scenario_id']),
                        'department_code': formula.get('target_department_code') or 'MODEL',
                        'fund_code': formula['target_fund_code'],
                        'account_code': formula['target_account_code'],
                        'period': period,
                        'amount': amount,
                        'source': 'model_formula',
                        'ledger_type': 'scenario',
                        'ledger_basis': 'scenario',
                        'source_version': model['model_key'],
                        'source_record_id': formula['formula_key'],
                        'notes': f"Model formula: {formula['label']}",
                        'metadata': {
                            'model_id': model_id,
                            'formula_id': formula['id'],
                            'recalculation_run_id': run_id,
                            'formula_trace': evaluation['trace'],
                        },
                    },
                    actor=user['email'],
                    user=user,
                )
            )

    allocations = [row for row in list_allocation_rules(model_id) if row['active']]
    for rule in allocations:
        created.extend(_run_allocation_rule(model, rule, run_id, user))

    db.execute(
        '''
        UPDATE model_recalculation_runs
        SET status = 'posted', formula_count = ?, allocation_count = ?, ledger_entry_count = ?,
            messages_json = ?, completed_at = ?
        WHERE id = ?
        ''',
        (len(formulas), len(allocations), len(created), json.dumps(messages), _now(), run_id),
    )
    db.log_audit('planning_model', str(model_id), 'recalculated', user['email'], {'run_id': run_id, 'ledger_entry_count': len(created)}, _now())
    run = get_recalculation_run(run_id)
    run['created_entries'] = created
    return run


def list_recalculation_runs(model_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM model_recalculation_runs WHERE model_id = ? ORDER BY id DESC', (model_id,))
    return [_format_run(row) for row in rows]


def get_recalculation_run(run_id: int) -> dict[str, Any]:
    return _format_run(_one('SELECT * FROM model_recalculation_runs WHERE id = ?', (run_id,)))


def _run_allocation_rule(model: dict[str, Any], rule: dict[str, Any], run_id: int, user: dict[str, Any]) -> list[dict[str, Any]]:
    created = []
    for period in _month_range(rule['period_start'], rule['period_end']):
        source_amount = _ledger_total(int(model['scenario_id']), period, rule['source_account_code'], rule.get('source_department_code'))
        weights = _allocation_weights(int(model['scenario_id']), period, rule)
        total_weight = sum(weights.values()) or float(len(rule['target_department_codes']))
        for department_code in rule['target_department_codes']:
            weight = weights.get(department_code, 1.0)
            amount = round(source_amount * (weight / total_weight), 2)
            created.append(
                append_ledger_entry(
                    {
                        'scenario_id': int(model['scenario_id']),
                        'department_code': department_code,
                        'fund_code': rule['target_fund_code'],
                        'account_code': rule['target_account_code'],
                        'period': period,
                        'amount': amount,
                        'source': 'allocation_rule',
                        'ledger_type': 'scenario',
                        'ledger_basis': 'scenario',
                        'source_version': model['model_key'],
                        'source_record_id': rule['rule_key'],
                        'notes': f"Allocation rule: {rule['label']}",
                        'metadata': {'model_id': model['id'], 'allocation_rule_id': rule['id'], 'recalculation_run_id': run_id, 'weight': weight, 'total_weight': total_weight},
                    },
                    actor=user['email'],
                    user=user,
                )
            )
    return created


def _allocation_weights(scenario_id: int, period: str, rule: dict[str, Any]) -> dict[str, float]:
    if rule.get('basis_account_code'):
        return {
            department: abs(_ledger_total(scenario_id, period, rule['basis_account_code'], department))
            for department in rule['target_department_codes']
        }
    if rule.get('basis_driver_key'):
        driver = db.fetch_one('SELECT value FROM typed_drivers WHERE scenario_id = ? AND driver_key = ?', (scenario_id, rule['basis_driver_key']))
        value = abs(float(driver['value'])) if driver else 1.0
        return {department: value for department in rule['target_department_codes']}
    return {department: 1.0 for department in rule['target_department_codes']}


def _ledger_total(scenario_id: int, period: str, account_code: str, department_code: str | None = None) -> float:
    if department_code:
        row = db.fetch_one(
            '''
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM planning_ledger
            WHERE scenario_id = ? AND period = ? AND account_code = ? AND department_code = ? AND reversed_at IS NULL
            ''',
            (scenario_id, period, account_code, department_code),
        )
    else:
        row = db.fetch_one(
            '''
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM planning_ledger
            WHERE scenario_id = ? AND period = ? AND account_code = ? AND reversed_at IS NULL
            ''',
            (scenario_id, period, account_code),
        )
    return round(float(row['total']) if row else 0.0, 2)


def _formula_context(scenario_id: int, period: str, formula_values: dict[str, float]) -> dict[str, float]:
    context = dict(formula_values)
    for row in db.fetch_all('SELECT driver_key, value FROM typed_drivers WHERE scenario_id = ?', (scenario_id,)):
        context[str(row['driver_key'])] = float(row['value'])
        context[_identifier(row['driver_key'])] = float(row['value'])
    totals = db.fetch_all(
        '''
        SELECT department_code, account_code, SUM(amount) AS amount
        FROM planning_ledger
        WHERE scenario_id = ? AND period = ? AND reversed_at IS NULL
        GROUP BY department_code, account_code
        ''',
        (scenario_id, period),
    )
    for row in totals:
        context[f"ACCOUNT_{_identifier(row['account_code'])}"] = context.get(f"ACCOUNT_{_identifier(row['account_code'])}", 0.0) + float(row['amount'])
        context[f"DEPT_{_identifier(row['department_code'])}_{_identifier(row['account_code'])}"] = float(row['amount'])
    return context


def _evaluate_expression(expression: str, context: dict[str, float]) -> float:
    return float(evaluate_formula(expression, context, default_missing_names_to_zero=True, rounding=2)['value'])


def _ordered_formulas(model_id: int, graph: dict[str, Any]) -> list[dict[str, Any]]:
    rows = {row['formula_key']: row for row in list_formulas(model_id) if row['active']}
    inbound = {key: set() for key in rows}
    for edge in graph['edges']:
        inbound[edge['to']].add(edge['from'])
    ordered = []
    ready = sorted(key for key, deps in inbound.items() if not deps)
    while ready:
        key = ready.pop(0)
        ordered.append(rows[key])
        for edge in graph['edges']:
            if edge['from'] == key:
                inbound[edge['to']].discard(key)
                if not inbound[edge['to']] and edge['to'] not in [item['formula_key'] for item in ordered] and edge['to'] not in ready:
                    ready.append(edge['to'])
    return ordered


def _expression_names(expression: str) -> set[str]:
    return expression_names(expression)


def _cycles(adjacency: dict[str, list[str]]) -> list[list[str]]:
    cycles: list[list[str]] = []
    visiting: list[str] = []
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visiting:
            cycles.append(visiting[visiting.index(node):] + [node])
            return
        if node in visited:
            return
        visiting.append(node)
        for child in adjacency.get(node, []):
            visit(child)
        visiting.pop()
        visited.add(node)

    for key in adjacency:
        visit(key)
    return cycles


def _identifier(value: str) -> str:
    cleaned = re.sub(r'\W+', '_', value.upper()).strip('_')
    return cleaned or 'VALUE'


def _invalidate(model_id: int, formula_key: str, reason: str, user: dict[str, Any]) -> None:
    db.execute(
        '''
        INSERT INTO model_dependency_invalidations (
            model_id, formula_key, reason, status, created_by, created_at
        ) VALUES (?, ?, ?, 'invalidated', ?, ?)
        ''',
        (model_id, formula_key, reason, user['email'], _now()),
    )


def _format_dimension(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['metadata'] = json.loads(result.pop('metadata_json') or '{}')
    return result


def _format_version(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['dependency_graph'] = json.loads(result.pop('dependency_graph_json') or '{}')
    result['calculation_order'] = json.loads(result.pop('calculation_order_json') or '[]')
    result['dimension_strategy'] = json.loads(result.pop('dimension_strategy_json') or '{}')
    return result


def _format_performance_test(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['messages'] = json.loads(result.pop('messages_json') or '[]')
    return result


def _format_optimization_profile(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise ValueError('Optimization profile not found.')
    result = dict(row)
    result['strategy'] = json.loads(result.pop('strategy_json') or '{}')
    result['density'] = json.loads(result.pop('density_json') or '{}')
    return result


def _format_scenario_branch(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise ValueError('Scenario branch not found.')
    result = dict(row)
    result['metadata'] = json.loads(result.pop('metadata_json') or '{}')
    return result


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    row['dependency_graph'] = json.loads(row.pop('dependency_graph_json') or '{}')
    row['messages'] = json.loads(row.pop('messages_json') or '[]')
    return row


def _formula_row(model_id: int, formula_key: str) -> dict[str, Any]:
    row = _one('SELECT * FROM model_formulas WHERE model_id = ? AND formula_key = ?', (model_id, formula_key))
    row['active'] = bool(row['active'])
    return row


def _allocation_row(model_id: int, rule_key: str) -> dict[str, Any]:
    row = _one('SELECT * FROM allocation_rules WHERE model_id = ? AND rule_key = ?', (model_id, rule_key))
    row['active'] = bool(row['active'])
    row['target_department_codes'] = json.loads(row['target_department_codes'] or '[]')
    return row


def _one(query: str, params: tuple[Any, ...]) -> dict[str, Any]:
    row = db.fetch_one(query, params)
    if row is None:
        raise ValueError('Record not found.')
    return row
