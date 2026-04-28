from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_tax_compliance.db'
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


def active_scenario_id(headers: dict[str, str]) -> int:
    response = client.get('/api/bootstrap', headers=headers)
    assert response.status_code == 200
    return int(response.json()['activeScenario']['id'])


def test_b63_tax_classification_form990_source_checks_alerts_and_review() -> None:
    headers = admin_headers()
    scenario_id = active_scenario_id(headers)

    status = client.get('/api/compliance/tax/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B63'
    assert status.json()['complete'] is True
    assert status.json()['counts']['rule_sources'] >= 4

    ledger = client.post(
        '/api/foundation/ledger',
        headers=headers,
        json={
            'scenario_id': scenario_id,
            'entity_code': 'CAMPUS',
            'department_code': 'AUX',
            'fund_code': 'AUX',
            'account_code': 'RENTAL',
            'period': '2026-08',
            'amount': 5000,
            'source': 'tax-test',
            'ledger_type': 'actual',
            'ledger_basis': 'actual',
        },
    )
    assert ledger.status_code == 200

    classification = client.post(
        '/api/compliance/tax/classifications',
        headers=headers,
        json={
            'classification_key': 'aux-rental-taxable',
            'scenario_id': scenario_id,
            'ledger_entry_id': ledger.json()['id'],
            'activity_name': 'Campus auxiliary rental',
            'tax_status': 'taxable',
            'activity_tag': 'unrelated_business',
            'income_type': 'unrelated_business',
            'ubit_code': 'rental-review',
            'regularly_carried_on': True,
            'substantially_related': False,
            'amount': 5000,
            'expense_offset': 1200,
            'form990_part': 'VIII',
            'form990_line': '11',
            'form990_column': 'C',
            'review_status': 'needs_review',
            'notes': 'Test classification.',
        },
    )
    assert classification.status_code == 200
    assert classification.json()['net_ubti'] == 3800
    assert classification.json()['review_status'] == 'needs_review'

    form990 = client.post(
        '/api/compliance/tax/form990',
        headers=headers,
        json={
            'support_key': 'form990-viii-11c',
            'scenario_id': scenario_id,
            'period': '2026-08',
            'form_part': 'VIII',
            'line_number': '11',
            'column_code': 'C',
            'description': 'Unrelated business revenue',
            'amount': 5000,
            'basis': {'classification_key': 'aux-rental-taxable'},
            'review_status': 'needs_review',
        },
    )
    assert form990.status_code == 200
    assert form990.json()['basis']['classification_key'] == 'aux-rental-taxable'

    review = client.post(
        f"/api/compliance/tax/classifications/{classification.json()['id']}/review",
        headers=headers,
        json={'decision': 'approve', 'note': 'Reviewed by controller.', 'evidence': {'source': 'test'}},
    )
    assert review.status_code == 200
    assert review.json()['status'] == 'approved'
    assert review.json()['classification']['review_status'] == 'approved'

    changed = client.post(
        '/api/compliance/tax/update-checks',
        headers=headers,
        json={
            'source_key': 'irs-publication-598',
            'observed_version': '2026-04-27-test',
            'detail': {'release_note': 'simulated source update'},
        },
    )
    assert changed.status_code == 200
    assert changed.json()['detected_change'] is True
    assert changed.json()['alert']['status'] == 'open'

    alerts = client.get('/api/compliance/tax/alerts?status=open', headers=headers)
    assert alerts.status_code == 200
    assert alerts.json()['count'] >= 1

    summary = client.get(f'/api/compliance/tax/summary?scenario_id={scenario_id}', headers=headers)
    assert summary.status_code == 200
    assert any(row['tax_status'] == 'taxable' and row['net_ubti'] == 3800 for row in summary.json()['by_status'])

    workspace = client.get(f'/api/compliance/tax/workspace?scenario_id={scenario_id}', headers=headers)
    assert workspace.status_code == 200
    assert workspace.json()['classifications']
    assert workspace.json()['form990_support_fields']
    assert workspace.json()['tax_alerts']


def test_b63_migration_and_ui_surface_exist() -> None:
    headers = admin_headers()
    migrations = client.get('/api/foundation/migrations', headers=headers)
    assert migrations.status_code == 200
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0063_tax_classification_compliance_watch' in keys

    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')
    assert 'id="classifyTaxActivityButton"' in index
    assert 'id="taxClassificationTable"' in index
    assert 'id="taxRuleSourceTable"' in index
    assert '/api/compliance/tax/workspace' in app_js
    assert '/api/compliance/tax/update-checks' in app_js
