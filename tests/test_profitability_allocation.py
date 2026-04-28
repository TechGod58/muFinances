from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_profitability_allocation.db'
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


def scenario_id(headers: dict[str, str]) -> int:
    scenarios = client.get('/api/scenarios', headers=headers).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def post_ledger(headers: dict[str, str], sid: int, **row: object) -> None:
    payload = {
        'scenario_id': sid,
        'entity_code': 'CAMPUS',
        'department_code': row.get('department_code', 'SCI'),
        'fund_code': row.get('fund_code', 'GEN'),
        'account_code': row.get('account_code', 'TUITION'),
        'period': row.get('period', '2026-08'),
        'amount': row.get('amount', 0),
        'source': 'profitability_test',
        'ledger_type': 'actual',
        'ledger_basis': 'actual',
        'program_code': row.get('program_code'),
        'grant_code': row.get('grant_code'),
    }
    response = client.post('/api/foundation/ledger', headers=headers, json=payload)
    assert response.status_code == 200


def test_profitability_allocations_program_fund_grant_and_trace_reports() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    period = '2026-08'

    post_ledger(headers, sid, department_code='B44OPS', account_code='SUPPLIES', amount=-1000, period=period)
    post_ledger(headers, sid, department_code='B44SCI', account_code='TUITION', amount=3000, program_code='BIO-BS', period=period)
    post_ledger(headers, sid, department_code='B44SCI', account_code='SALARY', amount=-900, program_code='BIO-BS', period=period)
    post_ledger(headers, sid, department_code='B44ART', account_code='TUITION', amount=1000, program_code='ART-BA', period=period)
    post_ledger(headers, sid, department_code='B44ART', account_code='SALARY', amount=-600, program_code='ART-BA', period=period)
    post_ledger(headers, sid, department_code='RES', fund_code='GRANT', account_code='GRANT_REV', amount=5000, grant_code='NSF-B44', period=period)
    post_ledger(headers, sid, department_code='RES', fund_code='GRANT', account_code='GRANT_EXP', amount=-1200, grant_code='NSF-B44', period=period)

    grant = client.post(
        '/api/campus-planning/grants',
        headers=headers,
        json={'scenario_id': sid, 'grant_code': 'NSF-B44', 'department_code': 'SCI', 'sponsor': 'NSF', 'start_period': period, 'end_period': '2026-12', 'total_award': 5000, 'direct_cost_budget': 3000, 'indirect_cost_rate': 0.1, 'spent_to_date': 1200},
    )
    assert grant.status_code == 200

    pool = client.post(
        '/api/profitability/cost-pools',
        headers=headers,
        json={
            'scenario_id': sid,
            'pool_key': 'ops-service',
            'name': 'Operations service center',
            'source_department_code': 'B44OPS',
            'source_account_code': 'SUPPLIES',
            'allocation_basis': 'revenue',
            'target_type': 'department',
            'target_codes': ['B44SCI', 'B44ART'],
        },
    )
    assert pool.status_code == 200
    assert pool.json()['target_codes'] == ['B44SCI', 'B44ART']

    run = client.post('/api/profitability/allocation-runs', headers=headers, json={'scenario_id': sid, 'period': period, 'pool_keys': ['ops-service']})
    assert run.status_code == 200
    assert run.json()['total_source_cost'] == 1000
    assert run.json()['total_allocated_cost'] == 1000
    assert len(run.json()['trace_lines']) == 2

    trace = client.get(f'/api/profitability/trace-lines?scenario_id={sid}&run_id={run.json()["id"]}', headers=headers)
    assert trace.status_code == 200
    assert {row['target_code'] for row in trace.json()['trace_lines']} == {'B44SCI', 'B44ART'}

    program = client.get(f'/api/profitability/program-margin?scenario_id={sid}', headers=headers)
    assert program.status_code == 200
    assert {row['program_code'] for row in program.json()['rows']} >= {'BIO-BS', 'ART-BA'}

    fund = client.get(f'/api/profitability/fund-profitability?scenario_id={sid}', headers=headers)
    assert fund.status_code == 200
    assert any(row['fund_code'] == 'GRANT' and row['net_after_allocation'] == 3800 for row in fund.json()['rows'])

    grant_report = client.get(f'/api/profitability/grant-profitability?scenario_id={sid}', headers=headers)
    assert grant_report.status_code == 200
    grant_row = next(row for row in grant_report.json()['rows'] if row['grant_code'] == 'NSF-B44')
    assert grant_row['remaining_after_allocation'] == 3800

    before_after = client.get(f'/api/profitability/before-after?scenario_id={sid}', headers=headers)
    assert before_after.status_code == 200
    sci = next(row for row in before_after.json()['rows'] if row['department_code'] == 'B44SCI')
    assert sci['allocated_cost'] == 750
    assert sci['after_allocation'] == 1350

    snapshot = client.post(f'/api/profitability/snapshots?scenario_id={sid}&period_start={period}&period_end={period}&snapshot_type=b44', headers=headers)
    assert snapshot.status_code == 200
    assert snapshot.json()['contents']['before_after']['rows']


def test_profitability_status_reports_b44_complete() -> None:
    response = client.get('/api/profitability/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B44'
    assert payload['complete'] is True
    assert payload['checks']['before_after_allocation_comparison_ready'] is True
