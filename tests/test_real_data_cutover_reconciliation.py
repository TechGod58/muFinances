from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_real_data_cutover_reconciliation.db'
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


def test_real_data_cutover_reconciliation_loads_all_anonymized_sources_and_balances_totals() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/integrations/real-data-cutover-reconciliation/status', headers=headers)
    assert status.status_code == 200
    status_payload = status.json()
    assert status_payload['batch'] == 'B121'
    assert status_payload['complete'] is True
    assert status_payload['checks']['loaded_total_reconciliation_ready'] is True
    assert status_payload['counts']['required_sources'] == 7

    run = client.post(
        '/api/integrations/real-data-cutover-reconciliation/run',
        headers=headers,
        json={'scenario_id': sid, 'run_key': 'b121-cutover-regression', 'include_default_exports': True},
    )
    assert run.status_code == 200, run.text
    payload = run.json()
    assert payload['batch'] == 'B121'
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['validation_run']['status'] == 'passed'
    assert payload['checks']['all_required_exports_loaded'] is True
    assert payload['checks']['source_file_manifest_complete'] is True
    assert payload['checks']['source_totals_match_loaded_totals'] is True
    assert payload['checks']['all_rows_accepted'] is True
    assert payload['checks']['source_record_lineage_ready'] is True
    assert payload['checks']['sync_logs_populated'] is True

    assert {item['source_system'] for item in payload['source_manifest']} == {
        'gl',
        'budget',
        'payroll',
        'hr',
        'sis_enrollment',
        'grants',
        'banking',
    }
    assert set(payload['reconciliation']) == {
        'gl',
        'budget',
        'payroll',
        'hr',
        'sis_enrollment',
        'grants',
        'banking',
    }
    assert all(abs(item['variance']) < 0.01 for item in payload['reconciliation'].values())
    assert payload['reconciliation']['sis_enrollment']['source_total'] == 595
    assert payload['reconciliation']['sis_enrollment']['loaded_total'] == 595
    assert payload['reconciliation']['banking']['source_total'] == 110750
    assert payload['reconciliation']['banking']['loaded_total'] == 110750

    rows = client.get('/api/integrations/real-data-cutover-reconciliation/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
    assert rows.json()['cutover_reconciliations'][0]['run_key'] == 'b121-cutover-regression'
