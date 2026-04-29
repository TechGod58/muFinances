from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_financial_service_contracts.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.contracts.financial import CONTRACT_REGISTRY, ForecastRunContract, LedgerPostContract
from app.main import app
from app.services.foundation import append_ledger_entry

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def scenario_id(headers: dict[str, str]) -> int:
    response = client.get('/api/scenarios', headers=headers)
    assert response.status_code == 200
    return int(response.json()[0]['id'])


def test_financial_contract_registry_covers_major_workflow_boundaries() -> None:
    assert {
        'ledger.post',
        'budget.submission',
        'budget.line',
        'forecast.run',
        'close.reconciliation',
        'consolidation.run',
        'report.definition',
        'integration.import_batch',
        'audit.financial_event',
        'security.workflow',
    } <= set(CONTRACT_REGISTRY)


def test_financial_contract_status_endpoint_reports_all_contracts() -> None:
    headers = admin_headers()
    response = client.get('/api/contracts/financial/status', headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B114'
    assert payload['complete'] is True
    assert all(payload['checks'].values())


def test_ledger_post_contract_rejects_zero_amount_before_write() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    before = client.get(f'/api/foundation/ledger?scenario_id={sid}', headers=headers).json()['count']

    with pytest.raises(ValidationError):
        append_ledger_entry(
            {
                'scenario_id': sid,
                'department_code': 'SCI',
                'fund_code': 'GEN',
                'account_code': 'SUPPLIES',
                'period': '2026-08',
                'amount': 0,
                'source': 'contract-test',
                'ledger_type': 'actual',
                'ledger_basis': 'actual',
            },
            actor='contract@test.local',
        )

    after = client.get(f'/api/foundation/ledger?scenario_id={sid}', headers=headers).json()['count']
    assert after == before


def test_contracts_enforce_period_order_and_extra_fields() -> None:
    with pytest.raises(ValidationError):
        ForecastRunContract.model_validate(
            {
                'scenario_id': 1,
                'method_key': 'straight_line',
                'account_code': 'TUITION',
                'period_start': '2026-12',
                'period_end': '2026-01',
            }
        )

    with pytest.raises(ValidationError):
        LedgerPostContract.model_validate(
            {
                'scenario_id': 1,
                'department_code': 'SCI',
                'fund_code': 'GEN',
                'account_code': 'SUPPLIES',
                'period': '2026-08',
                'amount': 1,
                'not_a_contract_field': True,
            }
        )
