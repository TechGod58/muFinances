from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_advanced_consolidation_engine.db'
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


def test_advanced_consolidation_currency_ownership_gaap_and_journals() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    period = '2026-12'

    entity = client.post(
        '/api/close/consolidation-entities',
        headers=headers,
        json={
            'entity_code': 'INTL',
            'entity_name': 'International Campus',
            'parent_entity_code': 'CAMPUS',
            'base_currency': 'EUR',
            'gaap_basis': 'IFRS',
        },
    )
    assert entity.status_code == 200

    ownership = client.post(
        '/api/close/entity-ownerships',
        headers=headers,
        json={'scenario_id': sid, 'parent_entity_code': 'CAMPUS', 'child_entity_code': 'INTL', 'ownership_percent': 80, 'effective_period': period},
    )
    assert ownership.status_code == 200

    setting = client.post(
        '/api/close/consolidation-settings',
        headers=headers,
        json={'scenario_id': sid, 'gaap_basis': 'US_GAAP', 'reporting_currency': 'USD', 'translation_method': 'closing_rate', 'enabled': True},
    )
    assert setting.status_code == 200

    rate = client.post(
        '/api/close/currency-rates',
        headers=headers,
        json={'scenario_id': sid, 'period': period, 'from_currency': 'EUR', 'to_currency': 'USD', 'rate': 1.2, 'rate_type': 'closing', 'source': 'treasury'},
    )
    assert rate.status_code == 200

    mapping = client.post(
        '/api/close/gaap-book-mappings',
        headers=headers,
        json={
            'scenario_id': sid,
            'source_gaap_basis': 'IFRS',
            'target_gaap_basis': 'US_GAAP',
            'source_account_code': 'TUITION',
            'target_account_code': 'TUITION_US',
            'adjustment_percent': 105,
        },
    )
    assert mapping.status_code == 200
    assert mapping.json()['active'] is True

    ledger = client.post(
        '/api/foundation/ledger',
        headers=headers,
        json={
            'scenario_id': sid,
            'entity_code': 'INTL',
            'department_code': 'SCI',
            'fund_code': 'GEN',
            'account_code': 'TUITION',
            'period': period,
            'amount': 1000,
            'source': 'actuals_import',
            'ledger_type': 'actual',
            'ledger_basis': 'actual',
        },
    )
    assert ledger.status_code == 200

    run = client.post('/api/close/consolidation-runs', headers=headers, json={'scenario_id': sid, 'period': period})
    assert run.status_code == 200
    advanced = run.json()['advanced_consolidation']
    assert advanced['reporting_currency'] == 'USD'
    assert advanced['gaap_basis'] == 'US_GAAP'
    assert advanced['totals']['translated_amount'] == 1200
    assert advanced['totals']['owned_amount'] == 960
    assert advanced['totals']['non_controlling_interest'] == 240
    assert advanced['totals']['gaap_adjustment'] == 48
    assert advanced['lines'][0]['account_code'] == 'TUITION_US'

    journals = client.get(f"/api/close/consolidation-journals?scenario_id={sid}&run_id={run.json()['id']}", headers=headers)
    assert journals.status_code == 200
    journal_types = {row['journal_type'] for row in journals.json()['journals']}
    assert {'consolidated_book', 'non_controlling_interest', 'gaap_adjustment'} <= journal_types

    report = run.json()['audit_report']['contents']
    assert report['controls']['currency_translation'] == 'closing_rate'
    assert report['controls']['multi_gaap'] == 'US_GAAP'
    assert report['controls']['journals_generated'] is True


def test_advanced_consolidation_status_reports_b28_complete() -> None:
    response = client.get('/api/close/advanced-consolidation/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] in {'B28', 'B43'}
    assert payload['complete'] is True
    assert payload['checks']['currency_translation_ready'] is True
    assert payload['checks']['ownership_logic_ready'] is True
    assert payload['checks']['multi_gaap_books_ready'] is True
    assert payload['checks']['consolidation_journals_ready'] is True


def test_statutory_reporting_complex_ownership_cta_rules_and_schedules() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    period = '2027-01'

    assert client.post('/api/close/consolidation-entities', headers=headers, json={'entity_code': 'HOLDCO', 'entity_name': 'Holding Entity', 'parent_entity_code': 'CAMPUS', 'base_currency': 'USD', 'gaap_basis': 'US_GAAP'}).status_code == 200
    assert client.post('/api/close/consolidation-entities', headers=headers, json={'entity_code': 'STATINTL', 'entity_name': 'Statutory International', 'parent_entity_code': 'HOLDCO', 'base_currency': 'EUR', 'gaap_basis': 'IFRS'}).status_code == 200
    assert client.post('/api/close/entity-ownerships', headers=headers, json={'scenario_id': sid, 'parent_entity_code': 'CAMPUS', 'child_entity_code': 'HOLDCO', 'ownership_percent': 90, 'effective_period': period}).status_code == 200
    assert client.post('/api/close/entity-ownerships', headers=headers, json={'scenario_id': sid, 'parent_entity_code': 'HOLDCO', 'child_entity_code': 'STATINTL', 'ownership_percent': 80, 'effective_period': period}).status_code == 200
    assert client.post('/api/close/consolidation-settings', headers=headers, json={'scenario_id': sid, 'gaap_basis': 'US_GAAP', 'reporting_currency': 'USD', 'translation_method': 'cta_average_closing', 'enabled': True}).status_code == 200
    assert client.post('/api/close/currency-rates', headers=headers, json={'scenario_id': sid, 'period': period, 'from_currency': 'EUR', 'to_currency': 'USD', 'rate': 1.1, 'rate_type': 'average', 'source': 'treasury'}).status_code == 200
    assert client.post('/api/close/currency-rates', headers=headers, json={'scenario_id': sid, 'period': period, 'from_currency': 'EUR', 'to_currency': 'USD', 'rate': 1.25, 'rate_type': 'closing', 'source': 'treasury'}).status_code == 200
    assert client.post('/api/close/gaap-book-mappings', headers=headers, json={'scenario_id': sid, 'source_gaap_basis': 'IFRS', 'target_gaap_basis': 'US_GAAP', 'source_account_code': 'TUITION', 'target_account_code': 'TUITION_US', 'adjustment_percent': 100}).status_code == 200
    rule = client.post('/api/close/consolidation-rules', headers=headers, json={'scenario_id': sid, 'rule_key': 'stat-pack-required', 'rule_type': 'statutory_schedule', 'source_filter': {'entity_code': 'STATINTL'}, 'action': {'schedule_type': 'minority_interest'}, 'priority': 10, 'active': True})
    assert rule.status_code == 200

    ledger = client.post('/api/foundation/ledger', headers=headers, json={'scenario_id': sid, 'entity_code': 'STATINTL', 'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': 'TUITION', 'period': period, 'amount': 1000, 'source': 'actuals_import', 'ledger_type': 'actual', 'ledger_basis': 'actual'})
    assert ledger.status_code == 200

    run = client.post('/api/close/consolidation-runs', headers=headers, json={'scenario_id': sid, 'period': period})
    assert run.status_code == 200
    advanced = run.json()['advanced_consolidation']
    assert advanced['totals']['translated_amount'] == 1250
    assert advanced['totals']['owned_amount'] == 900
    assert advanced['totals']['non_controlling_interest'] == 350
    assert advanced['totals']['cta_amount'] == 150

    chains = client.get(f'/api/close/ownership-chain-calculations?scenario_id={sid}&run_id={run.json()["id"]}', headers=headers)
    assert chains.status_code == 200
    assert chains.json()['ownership_chains'][0]['effective_ownership_percent'] == 72

    cta = client.get(f'/api/close/currency-translation-adjustments?scenario_id={sid}&run_id={run.json()["id"]}', headers=headers)
    assert cta.status_code == 200
    assert cta.json()['currency_translation_adjustments'][0]['cta_amount'] == 150

    packs = client.get(f'/api/close/statutory-packs?scenario_id={sid}', headers=headers)
    assert packs.status_code == 200
    assert packs.json()['statutory_packs']
    schedules = client.get(f'/api/close/supplemental-schedules?scenario_id={sid}&run_id={run.json()["id"]}', headers=headers)
    assert schedules.status_code == 200
    assert {item['schedule_type'] for item in schedules.json()['supplemental_schedules']} >= {'minority_interest', 'currency_translation_adjustment', 'ownership_chain', 'multi_book_bridge'}


def test_financial_correctness_depth_runs_live_control_proof() -> None:
    headers = admin_headers()

    proof = client.post('/api/close/financial-correctness-depth/run', headers=headers)

    assert proof.status_code == 200
    payload = proof.json()
    assert payload['complete'] is True
    assert payload['checks']['real_currency_rates_ready'] is True
    assert payload['checks']['ownership_chains_ready'] is True
    assert payload['checks']['intercompany_matching_ready'] is True
    assert payload['checks']['eliminations_ready'] is True
    assert payload['checks']['multi_gaap_books_ready'] is True
    assert payload['checks']['audit_reports_ready'] is True
    assert payload['checks']['locked_period_enforcement_ready'] is True
    assert payload['intercompany_match']['status'] == 'matched'
    assert payload['elimination']['review_status'] == 'approved'
    assert payload['consolidation_run']['advanced_totals']['translated_amount'] > 0
    assert payload['locked_period_enforcement']['blocked'] is True
