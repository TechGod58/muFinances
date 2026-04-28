from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_pressure_scenarios.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def create_scenario(headers: dict[str, str], name: str) -> int:
    response = client.post(
        '/api/scenarios',
        headers=headers,
        json={'name': name, 'version': 'pressure', 'start_period': '2026-07', 'end_period': '2026-12'},
    )
    assert response.status_code == 200
    return int(response.json()['id'])


def assert_money(actual: float, expected: float) -> None:
    assert round(float(actual), 2) == round(float(expected), 2)


def test_pressure_01_external_export_import_reconciles_to_summary_and_reports() -> None:
    headers = admin_headers()
    sid = create_scenario(headers, 'Pressure 01 Export Import')
    connector_key = 'pressure-01-export'

    connector = client.post(
        '/api/integrations/connectors',
        headers=headers,
        json={'connector_key': connector_key, 'name': 'Pressure export file', 'system_type': 'file', 'direction': 'inbound', 'config': {'source': 'prophix-or-other-export'}},
    )
    assert connector.status_code == 200

    imported = client.post(
        '/api/integrations/imports',
        headers=headers,
        json={
            'scenario_id': sid,
            'connector_key': connector_key,
            'source_format': 'csv',
            'import_type': 'ledger',
            'rows': [
                {'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': 'TUITION', 'period': '2026-08', 'amount': 100000, 'notes': 'Export revenue'},
                {'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': 'SUPPLIES', 'period': '2026-08', 'amount': -25000, 'notes': 'Export expense'},
                {'department_code': 'OPS', 'fund_code': 'GEN', 'account_code': 'UTILITIES', 'period': '2026-08', 'amount': -10000, 'notes': 'Export expense'},
            ],
        },
    )
    assert imported.status_code == 200
    assert imported.json()['accepted_rows'] == 3
    assert imported.json()['rejected_rows'] == 0

    summary = client.get(f'/api/reports/summary?scenario_id={sid}', headers=headers).json()
    assert_money(summary['revenue_total'], 100000)
    assert_money(summary['expense_total'], -35000)
    assert_money(summary['net_total'], 65000)
    assert_money(summary['by_department']['SCI'], 75000)
    assert_money(summary['by_department']['OPS'], -10000)

    period_report = client.get(
        f'/api/reporting/period-range?scenario_id={sid}&period_start=2026-08&period_end=2026-08&dimension=account_code',
        headers=headers,
    )
    assert period_report.status_code == 200
    account_totals = {row['key']: row['amount'] for row in period_report.json()['rows']}
    assert_money(account_totals['TUITION'], 100000)
    assert_money(account_totals['SUPPLIES'], -25000)
    assert_money(account_totals['UTILITIES'], -10000)


def test_pressure_02_operating_budget_approval_and_transfer_keep_totals_aligned() -> None:
    headers = admin_headers()
    sid = create_scenario(headers, 'Pressure 02 Operating Budget')

    submission = client.post(
        '/api/operating-budget/submissions',
        headers=headers,
        json={'scenario_id': sid, 'department_code': 'SCI', 'owner': 'Budget Office', 'notes': 'Pressure submission'},
    )
    assert submission.status_code == 200
    submission_id = submission.json()['id']
    for amount, recurrence in [(-10000, 'recurring'), (-2000, 'one_time')]:
        line = client.post(
            f'/api/operating-budget/submissions/{submission_id}/lines',
            headers=headers,
            json={'fund_code': 'GEN', 'account_code': 'SUPPLIES', 'period': '2026-08', 'amount': amount, 'line_type': 'expense', 'recurrence': recurrence, 'notes': recurrence},
        )
        assert line.status_code == 200

    submitted = client.post(f'/api/operating-budget/submissions/{submission_id}/submit', headers=headers)
    assert submitted.status_code == 200
    approved = client.post(f'/api/operating-budget/submissions/{submission_id}/approve', headers=headers, json={'note': 'Pressure approved'})
    assert approved.status_code == 200
    assert approved.json()['line_count'] == 2
    assert_money(approved.json()['recurring_total'], -10000)
    assert_money(approved.json()['one_time_total'], -2000)

    transfer = client.post(
        '/api/operating-budget/transfers',
        headers=headers,
        json={'scenario_id': sid, 'from_department_code': 'SCI', 'to_department_code': 'OPS', 'fund_code': 'GEN', 'account_code': 'SUPPLIES', 'period': '2026-08', 'amount': 300, 'reason': 'Move budget capacity'},
    )
    assert transfer.status_code == 200
    transfer_approved = client.post(f"/api/operating-budget/transfers/{transfer.json()['id']}/approve", headers=headers)
    assert transfer_approved.status_code == 200
    assert transfer_approved.json()['from_ledger_entry_id'] is not None
    assert transfer_approved.json()['to_ledger_entry_id'] is not None

    summary = client.get(f'/api/reports/summary?scenario_id={sid}', headers=headers).json()
    assert_money(summary['net_total'], -12000)
    assert_money(summary['by_department']['SCI'], -11700)
    assert_money(summary['by_department']['OPS'], -300)


def test_pressure_03_enrollment_forecast_actuals_and_variance_align() -> None:
    headers = admin_headers()
    sid = create_scenario(headers, 'Pressure 03 Enrollment Forecast')

    assert client.post('/api/enrollment/terms', headers=headers, json={'scenario_id': sid, 'term_code': '2026FA-P3', 'term_name': 'Fall Pressure', 'period': '2026-08', 'census_date': '2026-09-15'}).status_code == 200
    assert client.post('/api/enrollment/tuition-rates', headers=headers, json={'scenario_id': sid, 'program_code': 'BIO-P3', 'residency': 'resident', 'rate_per_credit': 600, 'default_credit_load': 12, 'effective_term': '2026FA-P3'}).status_code == 200
    assert client.post('/api/enrollment/forecast-inputs', headers=headers, json={'scenario_id': sid, 'term_code': '2026FA-P3', 'program_code': 'BIO-P3', 'residency': 'resident', 'headcount': 50, 'fte': 45, 'retention_rate': 0.8, 'yield_rate': 0.75, 'discount_rate': 0.1}).status_code == 200

    run = client.post('/api/enrollment/tuition-forecast-runs', headers=headers, json={'scenario_id': sid, 'term_code': '2026FA-P3'})
    assert run.status_code == 200
    assert_money(run.json()['gross_revenue'], 324000)
    assert_money(run.json()['discount_amount'], 32400)
    assert_money(run.json()['net_revenue'], 291600)

    actuals = client.post(
        '/api/scenario-engine/actuals',
        headers=headers,
        json={
            'scenario_id': sid,
            'source_version': 'pressure-actuals',
            'rows': [{'scenario_id': sid, 'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': 'TUITION', 'period': '2026-08', 'amount': 300000, 'notes': 'Actual tuition', 'source': 'actuals'}],
        },
    )
    assert actuals.status_code == 200
    assert actuals.json()['count'] == 1

    variance_run = client.post(f'/api/scenario-engine/forecast-actual-variances/run?scenario_id={sid}', headers=headers)
    assert variance_run.status_code == 200
    assert variance_run.json()['count'] == 1
    variance = variance_run.json()['variances'][0]
    assert_money(variance['forecast_amount'], 291600)
    assert_money(variance['actual_amount'], 300000)
    assert_money(variance['variance_amount'], 8400)

    abf = client.get(f'/api/reporting/actual-budget-forecast-variance?scenario_id={sid}', headers=headers).json()
    row = next(item for item in abf['rows'] if item['key'] == 'SCI:TUITION')
    assert_money(row['actual'], 300000)
    assert_money(row['forecast'], 291600)


def test_pressure_04_workforce_grants_capital_and_board_package_align() -> None:
    headers = admin_headers()
    sid = create_scenario(headers, 'Pressure 04 Campus Planning')

    position = client.post(
        '/api/campus-planning/positions',
        headers=headers,
        json={'scenario_id': sid, 'position_code': 'P3-SCI-001', 'title': 'Lab Manager', 'department_code': 'SCI', 'employee_type': 'staff', 'fte': 1, 'annual_salary': 80000, 'benefit_rate': 0.25, 'vacancy_rate': 0.1},
    )
    assert position.status_code == 200
    assert_money(position.json()['salary_cost'], 72000)
    assert_money(position.json()['benefit_cost'], 18000)
    assert_money(position.json()['total_compensation'], 90000)

    grant = client.post(
        '/api/campus-planning/grants',
        headers=headers,
        json={'scenario_id': sid, 'grant_code': 'GR-P3', 'department_code': 'SCI', 'sponsor': 'NSF', 'start_period': '2026-07', 'end_period': '2026-12', 'total_award': 150000, 'direct_cost_budget': 100000, 'indirect_cost_rate': 0.1, 'spent_to_date': 45000},
    )
    assert grant.status_code == 200
    assert_money(grant.json()['indirect_cost_budget'], 10000)
    assert_money(grant.json()['remaining_award'], 105000)
    assert_money(grant.json()['burn_rate'], 0.3)

    capital = client.post(
        '/api/campus-planning/capital-requests',
        headers=headers,
        json={'scenario_id': sid, 'request_code': 'CAP-P3', 'department_code': 'OPS', 'project_name': 'Boiler controls', 'asset_category': 'Facilities', 'acquisition_period': '2026-08', 'capital_cost': 60000, 'useful_life_years': 10, 'funding_source': 'GEN'},
    )
    assert capital.status_code == 200
    approved = client.post(f"/api/campus-planning/capital-requests/{capital.json()['id']}/approve", headers=headers)
    assert approved.status_code == 200
    assert approved.json()['ledger_entry_id'] is not None
    assert_money(approved.json()['annual_depreciation'], 6000)

    summary = client.get(f'/api/reports/summary?scenario_id={sid}', headers=headers).json()
    assert_money(summary['net_total'], -60000)
    board = client.post('/api/reporting/board-packages', headers=headers, json={'scenario_id': sid, 'package_name': 'Pressure board package', 'period_start': '2026-08', 'period_end': '2026-08'})
    assert board.status_code == 200
    contents = board.json()['contents']
    assert_money(next(row['amount'] for row in contents['fund_report']['rows'] if row['key'] == 'GEN'), -60000)
    assert contents['grant_report']['rows'][0]['grant_code'] == 'GR-P3'


def test_pressure_05_close_consolidation_audit_and_import_lineage_align() -> None:
    headers = admin_headers()
    sid = create_scenario(headers, 'Pressure 05 Close Consolidation')
    connector_key = 'pressure-05-close'
    assert client.post('/api/integrations/connectors', headers=headers, json={'connector_key': connector_key, 'name': 'Close import', 'system_type': 'file', 'direction': 'inbound', 'config': {}}).status_code == 200
    imported = client.post(
        '/api/integrations/imports',
        headers=headers,
        json={'scenario_id': sid, 'connector_key': connector_key, 'source_format': 'csv', 'import_type': 'ledger', 'rows': [
            {'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': 'TUITION', 'period': '2026-08', 'amount': 1000},
            {'department_code': 'OPS', 'fund_code': 'GEN', 'account_code': 'SUPPLIES', 'period': '2026-08', 'amount': -400},
        ]},
    )
    assert imported.status_code == 200
    assert imported.json()['accepted_rows'] == 2

    checklist = client.post('/api/close/checklists', headers=headers, json={'scenario_id': sid, 'period': '2026-08', 'checklist_key': 'p5-ledger', 'title': 'Pressure ledger review', 'owner': 'Controller', 'due_date': '2026-09-05'})
    assert checklist.status_code == 200
    assert client.post(f"/api/close/checklists/{checklist.json()['id']}/complete", headers=headers, json={'evidence': {'source': 'pressure'}}).status_code == 200

    reconciliation = client.post('/api/close/reconciliations', headers=headers, json={'scenario_id': sid, 'period': '2026-08', 'entity_code': 'CAMPUS', 'account_code': 'TUITION', 'source_balance': 1000, 'owner': 'Controller', 'notes': 'Pressure tie-out'})
    assert reconciliation.status_code == 200
    assert reconciliation.json()['status'] in {'reconciled', 'prepared'}

    elimination = client.post('/api/close/eliminations', headers=headers, json={'scenario_id': sid, 'period': '2026-08', 'entity_code': 'CAMPUS', 'account_code': 'SUPPLIES', 'amount': -100, 'reason': 'Pressure elimination'})
    assert elimination.status_code == 200
    assert elimination.json()['ledger_entry_id'] is not None

    run = client.post('/api/close/consolidation-runs', headers=headers, json={'scenario_id': sid, 'period': '2026-08'})
    assert run.status_code == 200
    payload = run.json()
    assert_money(payload['total_before_eliminations'], 600)
    assert_money(payload['total_eliminations'], -100)
    assert_money(payload['consolidated_total'], 500)
    assert_money(payload['audit_packet']['contents']['totals']['consolidated'], 500)

    audit = client.get('/api/production-ops/admin-audit-report?limit=1000', headers=headers)
    assert audit.status_code == 200
    assert audit.json()['totals']['audit_logs'] >= 8
