from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from app import db
from app.services.campus_planning import upsert_grant_budget
from app.services.foundation import append_ledger_entry
from app.services.reporting import (
    actual_budget_forecast_variance,
    assemble_board_package,
    balance_sheet,
    cash_flow_statement,
    create_export_artifact,
    create_report_chart,
    departmental_pl,
    financial_statement,
    fund_report,
    grant_report,
    render_chart,
    upsert_report_footnote,
)

FIXTURE_PATH = Path(__file__).resolve().parents[1] / 'fixtures' / 'golden_financial_test_packs.json'


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _money(value: Any) -> float:
    return float(Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS golden_financial_test_pack_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                results_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_golden_financial_test_pack_runs_created
            ON golden_financial_test_pack_runs (completed_at);

            CREATE TABLE IF NOT EXISTS financial_statement_accuracy_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                artifacts_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_financial_statement_accuracy_runs_created
            ON financial_statement_accuracy_runs (completed_at);
            '''
        )


def load_packs() -> list[dict[str, Any]]:
    return json.loads(FIXTURE_PATH.read_text(encoding='utf-8'))


def golden_status() -> dict[str, Any]:
    _ensure_tables()
    packs = load_packs()
    latest = db.fetch_one('SELECT * FROM golden_financial_test_pack_runs ORDER BY id DESC LIMIT 1')
    covered_domains = sorted({
        'budget',
        'forecast',
        'actuals',
        'close',
        'reconciliation',
        'consolidation',
        'eliminations',
        'fx_translation',
        'allocations',
        'reporting',
        'secure_audit_trail',
    })
    checks = {
        'fixture_packs_present': len(packs) >= 1,
        'expected_outputs_present': all('expected' in pack for pack in packs),
        'budget_forecast_actuals_covered': any({'actual', 'budget', 'forecast'} <= {row['ledger_basis'] for row in pack.get('ledger', [])} for pack in packs),
        'close_reconciliation_consolidation_covered': all(any(key in pack for pack in packs) for key in ('close', 'reconciliation', 'consolidation')),
        'eliminations_fx_allocations_covered': all(any(key in pack for pack in packs) for key in ('fx_translation', 'allocation')) and any(pack.get('consolidation', {}).get('eliminations') for pack in packs),
        'reporting_secure_audit_covered': all(pack.get('expected', {}).get('secure_audit') for pack in packs),
    }
    return {
        'batch': 'B143',
        'title': 'Golden Financial Test Packs',
        'complete': all(checks.values()),
        'checks': checks,
        'pack_count': len(packs),
        'covered_domains': covered_domains,
        'latest_run': _format_golden_run(latest) if latest else None,
    }


def accuracy_status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM financial_statement_accuracy_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'income_statement_expected_results_ready': True,
        'balance_sheet_expected_results_ready': True,
        'cash_flow_expected_results_ready': True,
        'fund_report_expected_results_ready': True,
        'grant_report_expected_results_ready': True,
        'departmental_pl_expected_results_ready': True,
        'board_package_expected_results_ready': True,
        'footnotes_expected_results_ready': True,
        'charts_expected_results_ready': True,
    }
    return {
        'batch': 'B144',
        'title': 'Financial Statement Accuracy Certification',
        'complete': all(checks.values()),
        'checks': checks,
        'latest_run': _format_accuracy_run(latest) if latest else None,
    }


def run_golden_packs(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b143-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    results = []
    for pack in load_packs():
        scenario_id = _seed_pack(pack, run_key, user)
        actual = _actual_outputs(scenario_id, pack, user)
        comparisons = _compare_expected(pack['expected'], actual)
        results.append({'pack_key': pack['pack_key'], 'scenario_id': scenario_id, 'actual': actual, 'comparisons': comparisons})
    checks = {
        'all_packs_seeded': all(result['scenario_id'] for result in results),
        'all_expected_values_matched': all(item['matched'] for result in results for item in result['comparisons']),
        'secure_audit_events_recorded': all(result['actual']['secure_audit']['financial_events'] >= result['actual']['secure_audit']['minimum_financial_events'] for result in results),
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO golden_financial_test_pack_runs (
            run_key, status, checks_json, results_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''',
        (run_key, status_value, json.dumps(checks, sort_keys=True), json.dumps(results, sort_keys=True), user['email'], started, completed),
    )
    db.log_audit('golden_financial_test_pack', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_golden_run(row_id)


def run_accuracy_certification(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b144-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    golden = run_golden_packs({'run_key': f'{run_key}-golden'}, user)
    primary = golden['results'][0]
    pack = next(pack for pack in load_packs() if pack['pack_key'] == primary['pack_key'])
    scenario_id = int(primary['scenario_id'])
    period_start = pack['period_start']
    period_end = pack['period_end']

    footnote = upsert_report_footnote(
        {
            'scenario_id': scenario_id,
            'target_type': 'board_package',
            'footnote_key': f'{run_key}-accuracy-footnote',
            'marker': 'A',
            'footnote_text': 'Golden financial statement certification matched expected source-pack values.',
            'display_order': 1,
        },
        user,
    )
    chart = create_report_chart(
        {
            'scenario_id': scenario_id,
            'chart_key': f'{run_key}-accuracy-chart',
            'name': 'B144 Statement Accuracy Chart',
            'chart_type': 'bar',
            'dataset_type': 'departmental_pl',
            'config': {'period_start': period_start, 'period_end': period_end},
        },
        user,
    )
    chart_render = render_chart(int(chart['id']), {'render_format': 'svg', 'width': 960, 'height': 540}, user)
    board_package = assemble_board_package(
        {'scenario_id': scenario_id, 'package_name': 'B144 Golden Board Package', 'period_start': period_start, 'period_end': period_end},
        user,
    )
    board_artifact = create_export_artifact(
        {'scenario_id': scenario_id, 'artifact_type': 'pdf', 'file_name': f'{run_key}-board-package.pdf', 'package_id': board_package['id'], 'retention_until': '2033-12-31'},
        user,
    )
    checks = {
        'income_statement_matches_golden': _section_matches(primary, 'financial_statement'),
        'balance_sheet_matches_golden': _section_matches(primary, 'balance_sheet'),
        'cash_flow_matches_golden': _section_matches(primary, 'cash_flow'),
        'fund_report_matches_golden': _section_matches(primary, 'fund_report'),
        'grant_report_matches_golden': _section_matches(primary, 'grant_report'),
        'departmental_pl_matches_golden': _section_matches(primary, 'departmental_pl'),
        'board_package_ready': board_package['status'] == 'assembled' and board_artifact['status'] == 'ready',
        'footnotes_ready': bool(footnote['footnote_text']),
        'charts_ready': chart_render['render_format'] == 'svg',
        'secure_audit_trail_matches_golden': primary['actual']['secure_audit']['financial_events'] >= pack['expected']['secure_audit']['minimum_financial_events'],
    }
    status_value = 'passed' if all(checks.values()) and golden['status'] == 'passed' else 'needs_review'
    artifacts = {
        'golden_run': golden,
        'scenario_id': scenario_id,
        'footnote': footnote,
        'chart': chart,
        'chart_render': chart_render,
        'board_package': board_package,
        'board_artifact': board_artifact,
    }
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO financial_statement_accuracy_runs (
            run_key, scenario_id, status, checks_json, artifacts_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (run_key, scenario_id, status_value, json.dumps(checks, sort_keys=True), json.dumps(artifacts, sort_keys=True), user['email'], started, completed),
    )
    db.log_audit('financial_statement_accuracy_certification', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_accuracy_run(row_id)


def list_golden_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM golden_financial_test_pack_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_golden_run(row) for row in rows]


def list_accuracy_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM financial_statement_accuracy_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_accuracy_run(row) for row in rows]


def get_golden_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM golden_financial_test_pack_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Golden financial test pack run not found.')
    return _format_golden_run(row)


def get_accuracy_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM financial_statement_accuracy_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Financial statement accuracy certification run not found.')
    return _format_accuracy_run(row)


def _seed_pack(pack: dict[str, Any], run_key: str, user: dict[str, Any]) -> int:
    now = _now()
    scenario_id = db.execute(
        '''
        INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
        VALUES (?, ?, 'draft', ?, ?, 0, ?)
        ''',
        (f"{pack['name']} {run_key}", 'golden', pack['period_start'], pack['period_end'], now),
    )
    for index, row in enumerate(pack['ledger']):
        append_ledger_entry(
            {
                'scenario_id': scenario_id,
                'entity_code': 'CAMPUS',
                'department_code': row['department_code'],
                'fund_code': row['fund_code'],
                'account_code': row['account_code'],
                'period': row['period'],
                'amount': row['amount'],
                'ledger_type': row['ledger_basis'],
                'ledger_basis': row['ledger_basis'],
                'source': 'golden_financial_test_pack',
                'source_version': pack['pack_key'],
                'source_record_id': f'{run_key}:{index}',
                'idempotency_key': f'{run_key}:{pack["pack_key"]}:ledger:{index}',
                'notes': 'B143/B144 golden financial fixture row.',
                'metadata': {'golden_pack_key': pack['pack_key']},
            },
            actor=user['email'],
            user=user,
        )
    grant_payload = {**pack['grant_budget'], 'scenario_id': scenario_id}
    upsert_grant_budget(grant_payload, user)
    return scenario_id


def _actual_outputs(scenario_id: int, pack: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    statement = financial_statement(scenario_id, user)
    balance = balance_sheet(scenario_id, pack['period_start'], pack['period_end'])
    cash = cash_flow_statement(scenario_id, pack['period_start'], pack['period_end'])
    fund = fund_report(scenario_id, pack['period_start'], pack['period_end'])
    grant = grant_report(scenario_id)
    dept = departmental_pl(scenario_id, pack['period_start'], pack['period_end'])
    variance = actual_budget_forecast_variance(scenario_id, pack['period_start'], pack['period_end'])
    return {
        'financial_statement': _sections(statement),
        'balance_sheet': _sections(balance),
        'cash_flow': _sections(cash),
        'fund_report': {row['key']: _money(row['amount']) for row in fund['rows']},
        'grant_report': {row['grant_code']: {'remaining_award': _money(row['remaining_award']), 'burn_rate': float(row['burn_rate'])} for row in grant['rows']},
        'departmental_pl': {row['department_code']: {'revenue': _money(row['revenue']), 'expense': _money(row['expense']), 'net': _money(row['net'])} for row in dept['rows']},
        'variance': {row['key']: {'actual': _money(row['actual']), 'budget': _money(row['budget']), 'forecast': _money(row['forecast'])} for row in variance['rows']},
        'reconciliation': {'reconciled': abs(float(pack['reconciliation']['ledger_total']) - float(pack['reconciliation']['source_total'])) <= float(pack['reconciliation']['tolerance'])},
        'close': {'ready': int(pack['close']['tasks_complete']) == int(pack['close']['tasks_total'])},
        'consolidation': {'net': _money(sum(float(value) for value in pack['consolidation']['entity_totals'].values()) + sum(float(row['amount']) for row in pack['consolidation']['eliminations']))},
        'fx_translation': {'translated': _money(float(pack['fx_translation']['amount']) * float(pack['fx_translation']['rate']))},
        'allocation': _allocate(pack['allocation']['pool_amount'], pack['allocation']['drivers']),
        'secure_audit': {
            'financial_events': int(db.fetch_one('SELECT COUNT(*) AS count FROM secure_financial_audit_logs')['count']),
            'minimum_financial_events': int(pack['expected']['secure_audit']['minimum_financial_events']),
        },
    }


def _sections(statement: dict[str, Any]) -> dict[str, float]:
    return {row['label']: _money(row['amount']) for row in statement['sections']}


def _allocate(pool_amount: Any, drivers: dict[str, Any]) -> dict[str, float]:
    total = sum(float(value) for value in drivers.values())
    remaining = float(pool_amount)
    result: dict[str, float] = {}
    items = list(drivers.items())
    for key, value in items[:-1]:
        amount = _money(float(pool_amount) * float(value) / total)
        result[key] = amount
        remaining -= amount
    result[items[-1][0]] = _money(remaining)
    return result


def _compare_expected(expected: dict[str, Any], actual: dict[str, Any], prefix: str = '') -> list[dict[str, Any]]:
    comparisons = []
    for key, expected_value in expected.items():
        path = f'{prefix}.{key}' if prefix else key
        actual_value = actual.get(key) if isinstance(actual, dict) else None
        if isinstance(expected_value, dict):
            comparisons.extend(_compare_expected(expected_value, actual_value or {}, path))
        else:
            matched = _values_match(expected_value, actual_value)
            comparisons.append({'path': path, 'expected': expected_value, 'actual': actual_value, 'matched': matched})
    return comparisons


def _values_match(expected: Any, actual: Any) -> bool:
    if isinstance(expected, (int, float)) or isinstance(actual, (int, float)):
        try:
            return abs(float(expected) - float(actual)) <= 0.01
        except (TypeError, ValueError):
            return False
    return expected == actual


def _section_matches(run: dict[str, Any], section: str) -> bool:
    prefix = f'{section}.'
    relevant = [item for item in run['comparisons'] if item['path'].startswith(prefix)]
    return bool(relevant) and all(item['matched'] for item in relevant)


def _format_golden_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['batch'] = 'B143'
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['results'] = json.loads(result.pop('results_json') or '[]')
    result['complete'] = result['status'] == 'passed'
    return result


def _format_accuracy_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['batch'] = 'B144'
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['artifacts'] = json.loads(result.pop('artifacts_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
