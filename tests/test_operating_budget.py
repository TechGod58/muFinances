from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_operating_budget.db'
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


def scenario_id() -> int:
    scenarios = client.get('/api/scenarios', headers=admin_headers()).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def test_operating_budget_submission_line_and_approval_flow() -> None:
    headers = admin_headers()
    sid = scenario_id()
    submission = client.post(
        '/api/operating-budget/submissions',
        headers=headers,
        json={'scenario_id': sid, 'department_code': 'SCI', 'owner': 'Budget Office', 'notes': 'SCI operating budget'},
    )
    assert submission.status_code == 200
    submission_id = submission.json()['id']

    recurring = client.post(
        f'/api/operating-budget/submissions/{submission_id}/lines',
        headers=headers,
        json={
            'fund_code': 'GEN',
            'account_code': 'SUPPLIES',
            'period': '2026-08',
            'amount': -12000,
            'line_type': 'expense',
            'recurrence': 'recurring',
            'notes': 'Recurring lab supplies',
        },
    )
    assert recurring.status_code == 200
    assert recurring.json()['ledger_entry']['source'] == 'operating_budget'

    one_time = client.post(
        f'/api/operating-budget/submissions/{submission_id}/lines',
        headers=headers,
        json={
            'fund_code': 'GEN',
            'account_code': 'SUPPLIES',
            'period': '2026-09',
            'amount': -3000,
            'line_type': 'expense',
            'recurrence': 'one_time',
            'notes': 'One-time equipment supplies',
        },
    )
    assert one_time.status_code == 200

    submitted = client.post(f'/api/operating-budget/submissions/{submission_id}/submit', headers=headers)
    assert submitted.status_code == 200
    assert submitted.json()['status'] == 'submitted'

    approved = client.post(
        f'/api/operating-budget/submissions/{submission_id}/approve',
        headers=headers,
        json={'note': 'Approved for B03'},
    )
    assert approved.status_code == 200
    assert approved.json()['status'] == 'approved'
    assert approved.json()['line_count'] == 2
    assert approved.json()['recurring_total'] == -12000
    assert approved.json()['one_time_total'] == -3000


def test_operating_budget_assumptions_and_transfer_approval() -> None:
    headers = admin_headers()
    sid = scenario_id()
    assumption = client.post(
        '/api/operating-budget/assumptions',
        headers=headers,
        json={
            'scenario_id': sid,
            'department_code': 'SCI',
            'assumption_key': 'supplies_growth',
            'label': 'Supplies growth',
            'value': 0.035,
            'unit': 'ratio',
            'notes': 'B03 assumption',
        },
    )
    assert assumption.status_code == 200
    assert assumption.json()['value'] == 0.035

    transfer = client.post(
        '/api/operating-budget/transfers',
        headers=headers,
        json={
            'scenario_id': sid,
            'from_department_code': 'SCI',
            'to_department_code': 'OPS',
            'fund_code': 'GEN',
            'account_code': 'SUPPLIES',
            'period': '2026-08',
            'amount': 1500,
            'reason': 'Move supplies budget to operations',
        },
    )
    assert transfer.status_code == 200
    transfer_id = transfer.json()['id']
    assert transfer.json()['status'] == 'requested'

    approved = client.post(f'/api/operating-budget/transfers/{transfer_id}/approve', headers=headers)
    assert approved.status_code == 200
    payload = approved.json()
    assert payload['status'] == 'approved'
    assert payload['from_ledger_entry_id'] is not None
    assert payload['to_ledger_entry_id'] is not None


def test_operating_budget_status_reports_b03_complete() -> None:
    response = client.get('/api/operating-budget/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B03'
    assert payload['complete'] is True
    assert payload['checks']['approvals_ready'] is True
