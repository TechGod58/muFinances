from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_b145_b147_consolidation_coa_seed.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app
from app.services.seed_demo_enforcement import production_blockers, seed_allowed, status as seed_demo_status

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200, response.text
    return {'Authorization': f"Bearer {response.json()['token']}"}


def test_b145_consolidation_golden_cases_prove_consolidation_outputs() -> None:
    headers = admin_headers()

    status = client.get('/api/close/consolidation-golden-cases/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B145'
    assert status.json()['complete'] is True

    run = client.post('/api/close/consolidation-golden-cases/run', headers=headers, json={'run_key': 'b145-golden-pytest'})
    assert run.status_code == 200, run.text
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert all(payload['checks'].values())
    assert payload['actual']['effective_ownership_percent'] == payload['expected']['effective_ownership_percent']
    assert 'currency_translation_adjustment' in payload['actual']['journal_types']
    assert 'multi_book_bridge' in payload['actual']['supplemental_schedule_types']
    assert payload['audit_report_id'] >= 1

    rows = client.get('/api/close/consolidation-golden-cases/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1


def test_b146_chart_of_accounts_governance_and_sign_validation() -> None:
    headers = admin_headers()

    status = client.get('/api/data-hub/chart-of-accounts/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B146'
    assert status.json()['checks']['debit_credit_sign_conventions_ready'] is True

    accounts = client.get('/api/data-hub/chart-of-accounts/accounts', headers=headers)
    assert accounts.status_code == 200
    by_code = {row['account_code']: row for row in accounts.json()['accounts']}
    assert by_code['TUITION']['normal_balance'] == 'credit'
    assert by_code['SALARY']['sign_multiplier'] == -1
    assert by_code['CASH']['statement'] == 'balance_sheet'

    mappings = client.get('/api/data-hub/chart-of-accounts/statement-mappings', headers=headers)
    assert mappings.status_code == 200
    assert any(row['account_code'] == 'TUITION' and row['statement'] == 'income_statement' for row in mappings.json()['statement_mappings'])

    validation = client.post(
        '/api/data-hub/chart-of-accounts/validate',
        headers=headers,
        json={'run_key': 'b146-coa-pytest', 'account_codes': ['TUITION', 'SALARY', 'BENEFITS', 'UTILITIES', 'SUPPLIES']},
    )
    assert validation.status_code == 200
    payload = validation.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['exceptions'] == []
    assert payload['checks']['all_ledger_accounts_have_governance'] is True


def test_b147_seed_demo_enforcement_blocks_production_demo_leaks(monkeypatch) -> None:
    headers = admin_headers()

    runtime_status = client.get('/api/production-ops/seed-demo-enforcement/status', headers=headers)
    assert runtime_status.status_code == 200
    assert runtime_status.json()['batch'] == 'B147'
    assert runtime_status.json()['complete'] is True

    production_env = {
        'CAMPUS_FPM_ENV': 'production',
        'MUFINANCES_SEED_MODE': 'demo',
        'MUFINANCES_ALLOW_DEMO_SEED': 'true',
        'MUFINANCES_ALLOW_SAMPLE_LOGINS': 'true',
        'MUFINANCES_ALLOW_MOCK_CONNECTORS': 'true',
    }
    blockers = production_blockers(production_env)
    assert any('MUFINANCES_SEED_MODE' in blocker for blocker in blockers)
    assert any('MUFINANCES_ALLOW_DEMO_SEED' in blocker for blocker in blockers)
    assert seed_allowed(production_env) is False

    safe_production = {'CAMPUS_FPM_ENV': 'production', 'MUFINANCES_SEED_MODE': 'none'}
    safe_status = seed_demo_status(safe_production)
    assert safe_status['complete'] is True
    assert safe_status['seed_allowed'] is False
    assert safe_status['blockers'] == []
