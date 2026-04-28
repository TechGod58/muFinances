from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_campus_planning.db'
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


def test_position_control_faculty_load_and_grant_burn_rate() -> None:
    headers = admin_headers()
    sid = scenario_id()
    position = client.post(
        '/api/campus-planning/positions',
        headers=headers,
        json={
            'scenario_id': sid,
            'position_code': 'SCI-FAC-001',
            'title': 'Assistant Professor',
            'department_code': 'SCI',
            'employee_type': 'faculty',
            'fte': 1,
            'annual_salary': 90000,
            'benefit_rate': 0.28,
            'vacancy_rate': 0.1,
        },
    )
    assert position.status_code == 200
    assert position.json()['salary_cost'] == 81000
    assert position.json()['benefit_cost'] == 22680
    assert position.json()['total_compensation'] == 103680

    load = client.post(
        '/api/campus-planning/faculty-loads',
        headers=headers,
        json={
            'scenario_id': sid,
            'department_code': 'SCI',
            'term_code': '2026FA',
            'course_code': 'BIO101',
            'sections': 4,
            'credit_hours': 12,
            'faculty_fte': 1.2,
            'adjunct_cost': 6000,
        },
    )
    assert load.status_code == 200
    assert load.json()['sections'] == 4

    grant = client.post(
        '/api/campus-planning/grants',
        headers=headers,
        json={
            'scenario_id': sid,
            'grant_code': 'NSF-001',
            'department_code': 'SCI',
            'sponsor': 'NSF',
            'start_period': '2026-07',
            'end_period': '2026-12',
            'total_award': 200000,
            'direct_cost_budget': 150000,
            'indirect_cost_rate': 0.1,
            'spent_to_date': 50000,
        },
    )
    assert grant.status_code == 200
    assert grant.json()['indirect_cost_budget'] == 15000
    assert grant.json()['burn_rate'] == 0.25
    assert grant.json()['remaining_award'] == 150000


def test_capital_request_approval_posts_to_ledger_with_depreciation() -> None:
    headers = admin_headers()
    sid = scenario_id()
    capital = client.post(
        '/api/campus-planning/capital-requests',
        headers=headers,
        json={
            'scenario_id': sid,
            'request_code': 'CAP-SCI-001',
            'department_code': 'SCI',
            'project_name': 'Lab Equipment Refresh',
            'asset_category': 'equipment',
            'acquisition_period': '2026-09',
            'capital_cost': 50000,
            'useful_life_years': 5,
            'funding_source': 'GEN',
        },
    )
    assert capital.status_code == 200
    payload = capital.json()
    assert payload['annual_depreciation'] == 10000
    assert payload['status'] == 'requested'

    approved = client.post(f"/api/campus-planning/capital-requests/{payload['id']}/approve", headers=headers)
    assert approved.status_code == 200
    assert approved.json()['status'] == 'approved'
    assert approved.json()['ledger_entry_id'] is not None

    ledger = client.get(f'/api/foundation/ledger?scenario_id={sid}', headers=headers)
    assert any(item['source'] == 'capital_request' and item['amount'] == -50000 for item in ledger.json()['entries'])


def test_campus_planning_status_reports_b05_complete() -> None:
    response = client.get('/api/campus-planning/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B05'
    assert payload['complete'] is True
    assert payload['checks']['capital_depreciation_ready'] is True
