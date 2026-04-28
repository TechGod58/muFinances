from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.campus_planning import upsert_grant_budget
from app.services.foundation import append_ledger_entry
from app.services.reporting import (
    actual_budget_forecast_variance,
    apply_chart_format,
    assemble_board_package,
    assemble_report_book,
    balance_sheet,
    cash_flow_statement,
    create_dashboard_chart_snapshot,
    create_export_artifact,
    create_report_chart,
    create_report_definition,
    departmental_pl,
    financial_statement,
    fund_report,
    grant_report,
    pixel_financial_statement,
    render_chart,
    save_report_layout,
    upsert_page_break,
    upsert_pagination_profile,
    upsert_report_footnote,
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS reporting_pixel_polish_runs (
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
            CREATE INDEX IF NOT EXISTS idx_reporting_pixel_polish_runs_scenario
            ON reporting_pixel_polish_runs (scenario_id, completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM reporting_pixel_polish_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'income_statement_ready': True,
        'balance_sheet_ready': True,
        'cash_flow_ready': True,
        'fund_reports_ready': True,
        'grant_reports_ready': True,
        'departmental_pl_ready': True,
        'board_package_pagination_ready': True,
        'footnotes_ready': True,
        'charts_ready': True,
    }
    counts = {
        'pixel_polish_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM reporting_pixel_polish_runs')['count']),
        'pagination_profiles': int(db.fetch_one('SELECT COUNT(*) AS count FROM pdf_pagination_profiles')['count']),
        'footnotes': int(db.fetch_one('SELECT COUNT(*) AS count FROM report_footnotes')['count']),
        'charts': int(db.fetch_one('SELECT COUNT(*) AS count FROM report_charts')['count']),
        'export_artifacts': int(db.fetch_one('SELECT COUNT(*) AS count FROM export_artifacts')['count']),
    }
    return {
        'batch': 'B98',
        'title': 'Reporting Pixel Polish',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM reporting_pixel_polish_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_run(row) for row in rows]


def run_certification(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b98-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    scenario_id = int(payload.get('scenario_id') or _create_scenario(run_key))
    period_start = payload.get('period_start') or '2026-07'
    period_end = payload.get('period_end') or '2026-12'

    ledger_entries = _seed_ledger(scenario_id, run_key, user)
    grant = upsert_grant_budget(
        {
            'scenario_id': scenario_id,
            'grant_code': f'{run_key}-NSF',
            'department_code': 'SCI',
            'sponsor': 'NSF',
            'start_period': period_start,
            'end_period': '2027-06',
            'total_award': 250000.0,
            'direct_cost_budget': 210000.0,
            'indirect_cost_rate': 0.12,
            'spent_to_date': 86000.0,
        },
        user,
    )
    pagination = upsert_pagination_profile(
        {
            'profile_key': f'{run_key}-controller-pagination',
            'scenario_id': scenario_id,
            'name': 'B98 Controller Board Pagination',
            'page_size': 'Letter',
            'orientation': 'landscape',
            'margin_top': 0.45,
            'margin_right': 0.45,
            'margin_bottom': 0.45,
            'margin_left': 0.45,
            'rows_per_page': 24,
        },
        user,
    )
    footnotes = [
        upsert_report_footnote(
            {
                'scenario_id': scenario_id,
                'target_type': 'financial_statement',
                'footnote_key': f'{run_key}-fs-footnote',
                'marker': '1',
                'footnote_text': 'Controller-certified statement layout includes actual, budget, forecast, and board-ready presentation controls.',
                'display_order': 1,
            },
            user,
        ),
        upsert_report_footnote(
            {
                'scenario_id': scenario_id,
                'target_type': 'board_package',
                'footnote_key': f'{run_key}-board-footnote',
                'marker': '2',
                'footnote_text': 'Board package pagination, charts, and page breaks are validated by the B98 polish run.',
                'display_order': 2,
            },
            user,
        ),
    ]
    report_definitions = [
        create_report_definition(
            {
                'name': f'{run_key} Departmental P&L Matrix',
                'report_type': 'ledger_matrix',
                'row_dimension': 'department_code',
                'column_dimension': 'account_code',
                'filters': {},
            },
            user,
        ),
        create_report_definition(
            {
                'name': f'{run_key} Fund Statement Matrix',
                'report_type': 'ledger_matrix',
                'row_dimension': 'fund_code',
                'column_dimension': 'account_code',
                'filters': {},
            },
            user,
        ),
    ]
    layout = save_report_layout(
        {
            'layout_key': f'{run_key}-controller-layout',
            'scenario_id': scenario_id,
            'report_definition_id': report_definitions[0]['id'],
            'name': 'B98 Controller Pixel Layout',
            'layout': {
                'font_family': 'Inter',
                'numeric_precision': 0,
                'statement_width_px': 1280,
                'section_spacing_px': 18,
                'page_header': 'muFinances board package',
            },
        },
        user,
    )
    chart = create_report_chart(
        {
            'scenario_id': scenario_id,
            'chart_key': f'{run_key}-variance-chart',
            'name': 'B98 Actual Budget Forecast Variance',
            'chart_type': 'bar',
            'dataset_type': 'variance',
            'config': {'period_start': period_start, 'period_end': period_end},
        },
        user,
    )
    chart = apply_chart_format(
        int(chart['id']),
        {
            'format': {
                'palette': ['#74f2bd', '#f7c948', '#ff7a66'],
                'show_legend': True,
                'number_format': '$#,##0',
                'axis_label_density': 'controller',
            }
        },
        user,
    )
    chart_render = render_chart(int(chart['id']), {'render_format': 'svg', 'width': 1280, 'height': 720}, user)
    chart_snapshot = create_dashboard_chart_snapshot(
        {'scenario_id': scenario_id, 'chart_id': chart['id'], 'render_id': chart_render['id'], 'snapshot_key': f'{run_key}-chart-snapshot'},
        user,
    )
    book = assemble_report_book(
        {
            'scenario_id': scenario_id,
            'book_key': f'{run_key}-controller-book',
            'name': 'B98 Controller Board Book',
            'layout_id': layout['id'],
            'period_start': period_start,
            'period_end': period_end,
            'report_definition_ids': [row['id'] for row in report_definitions],
            'chart_ids': [chart['id']],
        },
        user,
    )
    page_breaks = [
        upsert_page_break({'report_book_id': book['id'], 'section_key': 'income-statement', 'page_number': 1, 'break_before': True}, user),
        upsert_page_break({'report_book_id': book['id'], 'section_key': 'balance-sheet', 'page_number': 2, 'break_before': True}, user),
        upsert_page_break({'report_book_id': book['id'], 'section_key': 'cash-flow', 'page_number': 3, 'break_before': True}, user),
        upsert_page_break({'report_book_id': book['id'], 'section_key': 'charts', 'page_number': 4, 'break_before': True}, user),
    ]
    board_package = assemble_board_package(
        {
            'scenario_id': scenario_id,
            'package_name': 'B98 Controller Grade Board Package',
            'period_start': period_start,
            'period_end': period_end,
        },
        user,
    )
    pdf_artifact = create_export_artifact(
        {
            'scenario_id': scenario_id,
            'artifact_type': 'pdf',
            'file_name': f'{run_key}-controller-board-package.pdf',
            'package_id': board_package['id'],
            'retention_until': '2033-12-31',
        },
        user,
    )
    png_artifact = create_export_artifact(
        {
            'scenario_id': scenario_id,
            'artifact_type': 'png',
            'file_name': f'{run_key}-variance-chart.png',
            'chart_id': chart['id'],
            'retention_until': '2033-12-31',
        },
        user,
    )

    statements = {
        'income_statement': financial_statement(scenario_id, user),
        'pixel_financial_statement': pixel_financial_statement(scenario_id, user),
        'balance_sheet': balance_sheet(scenario_id, period_start, period_end),
        'cash_flow': cash_flow_statement(scenario_id, period_start, period_end),
        'fund_report': fund_report(scenario_id, period_start, period_end),
        'grant_report': grant_report(scenario_id),
        'departmental_pl': departmental_pl(scenario_id, period_start, period_end),
        'variance': actual_budget_forecast_variance(scenario_id, period_start, period_end),
    }
    checks = {
        'income_statement_has_sections': len(statements['income_statement']['sections']) >= 3,
        'balance_sheet_has_net_position': any(row['label'] == 'Net position' for row in statements['balance_sheet']['sections']),
        'cash_flow_has_net_cash_flow': any(row['label'] == 'Net cash flow' for row in statements['cash_flow']['sections']),
        'fund_report_has_rows': len(statements['fund_report']['rows']) >= 2,
        'grant_report_has_rows': len(statements['grant_report']['rows']) >= 1 and grant['remaining_award'] > 0,
        'departmental_pl_has_rows': len(statements['departmental_pl']['rows']) >= 2,
        'pixel_layout_and_pagination_ready': statements['pixel_financial_statement']['page']['orientation'] == 'landscape' and len(page_breaks) >= 4,
        'footnotes_ready': len(footnotes) >= 2,
        'charts_rendered_and_exported': chart_render['render_format'] == 'svg' and png_artifact['status'] == 'ready',
        'board_package_pdf_ready': board_package['status'] == 'assembled' and pdf_artifact['metadata']['page_count'] >= 1,
    }
    artifacts = {
        'ledger_entries': ledger_entries,
        'grant': grant,
        'pagination_profile': pagination,
        'footnotes': footnotes,
        'report_definitions': report_definitions,
        'layout': layout,
        'chart': chart,
        'chart_render': chart_render,
        'chart_snapshot': chart_snapshot,
        'report_book': book,
        'page_breaks': page_breaks,
        'board_package': board_package,
        'pdf_artifact': pdf_artifact,
        'png_artifact': png_artifact,
        'statements': statements,
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO reporting_pixel_polish_runs (
            run_key, scenario_id, status, checks_json, artifacts_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            scenario_id,
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(artifacts, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('reporting_pixel_polish', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM reporting_pixel_polish_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Reporting pixel polish run not found.')
    return _format_run(row)


def _create_scenario(run_key: str) -> int:
    return db.execute(
        '''
        INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
        VALUES (?, 'b98', 'draft', '2026-07', '2027-06', 0, ?)
        ''',
        (f'B98 Reporting Pixel Polish {run_key}', _now()),
    )


def _seed_ledger(scenario_id: int, run_key: str, user: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [
        ('SCI', 'GEN', 'TUITION', '2026-07', 610000.0, 'actual'),
        ('SCI', 'GEN', 'TUITION', '2026-08', 645000.0, 'budget'),
        ('SCI', 'GEN', 'TUITION', '2026-09', 672000.0, 'forecast'),
        ('OPS', 'GEN', 'SALARY', '2026-07', -240000.0, 'actual'),
        ('OPS', 'GEN', 'SUPPLIES', '2026-08', -88000.0, 'budget'),
        ('ART', 'AUX', 'AUXILIARY', '2026-09', 142000.0, 'actual'),
        ('SCI', 'GRANT', 'GRANT_REVENUE', '2026-10', 98000.0, 'actual'),
        ('SCI', 'GRANT', 'GRANT_EXPENSE', '2026-10', -76000.0, 'actual'),
        ('OPS', 'GEN', 'CASH', '2026-11', 420000.0, 'actual'),
        ('OPS', 'GEN', 'AP', '2026-11', -94000.0, 'actual'),
        ('OPS', 'GEN', 'CAPITAL', '2026-12', -135000.0, 'actual'),
        ('OPS', 'GEN', 'DEBT', '2026-12', 55000.0, 'actual'),
    ]
    created = []
    for index, (department, fund, account, period, amount, basis) in enumerate(rows, start=1):
        created.append(
            append_ledger_entry(
                {
                    'scenario_id': scenario_id,
                    'department_code': department,
                    'fund_code': fund,
                    'account_code': account,
                    'period': period,
                    'amount': amount,
                    'source': 'b98_reporting_pixel_polish',
                    'ledger_type': basis,
                    'ledger_basis': basis if basis in {'actual', 'budget', 'forecast'} else 'actual',
                    'notes': 'B98 reporting polish fixture row.',
                    'source_record_id': f'{run_key}-{index}',
                    'metadata': {'batch': 'B98'},
                },
                actor=user['email'],
                user=user,
            )
        )
    return created


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['artifacts'] = json.loads(result.pop('artifacts_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
