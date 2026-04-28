from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_enrollment.db'
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


def test_enrollment_tuition_forecast_posts_net_revenue_to_ledger() -> None:
    headers = admin_headers()
    sid = scenario_id()
    term = client.post(
        '/api/enrollment/terms',
        headers=headers,
        json={
            'scenario_id': sid,
            'term_code': '2026FA',
            'term_name': 'Fall 2026',
            'period': '2026-08',
            'census_date': '2026-09-15',
        },
    )
    assert term.status_code == 200

    rate = client.post(
        '/api/enrollment/tuition-rates',
        headers=headers,
        json={
            'scenario_id': sid,
            'program_code': 'BIO-BS',
            'residency': 'resident',
            'rate_per_credit': 500,
            'default_credit_load': 12,
            'effective_term': '2026FA',
        },
    )
    assert rate.status_code == 200

    forecast_input = client.post(
        '/api/enrollment/forecast-inputs',
        headers=headers,
        json={
            'scenario_id': sid,
            'term_code': '2026FA',
            'program_code': 'BIO-BS',
            'residency': 'resident',
            'headcount': 100,
            'fte': 90,
            'retention_rate': 0.9,
            'yield_rate': 0.8,
            'discount_rate': 0.2,
        },
    )
    assert forecast_input.status_code == 200

    run = client.post(
        '/api/enrollment/tuition-forecast-runs',
        headers=headers,
        json={'scenario_id': sid, 'term_code': '2026FA'},
    )
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'posted'
    assert payload['gross_revenue'] == 540000
    assert payload['discount_amount'] == 108000
    assert payload['net_revenue'] == 432000
    assert payload['lines'][0]['ledger_entry']['source'] == 'tuition_forecast'

    ledger = client.get(f'/api/foundation/ledger?scenario_id={sid}', headers=headers)
    assert ledger.status_code == 200
    assert any(item['source'] == 'tuition_forecast' and item['amount'] == 432000 for item in ledger.json()['entries'])


def test_enrollment_status_reports_b04_complete() -> None:
    response = client.get('/api/enrollment/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B04'
    assert payload['complete'] is True
    assert payload['checks']['tuition_forecast_ready'] is True
