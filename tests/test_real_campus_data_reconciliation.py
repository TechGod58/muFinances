from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_real_campus_data_reconciliation.db'
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


def test_b157_real_campus_data_reconciliation_certifies_source_totals_lineage_and_sync_logs() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/integrations/real-campus-data-reconciliation/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B157'
    assert payload['complete'] is True
    assert payload['checks']['required_source_coverage_ready'] is True
    assert payload['counts']['required_sources'] == 7

    run = client.post(
        '/api/integrations/real-campus-data-reconciliation/run',
        headers=headers,
        json={
            'scenario_id': sid,
            'run_key': 'b157-regression',
            'include_default_exports': True,
            'signed_by': 'integration.admin@manchester.edu',
        },
    )
    assert run.status_code == 200, run.text
    proof = run.json()
    assert proof['batch'] == 'B157'
    assert proof['status'] == 'passed'
    assert proof['complete'] is True
    assert proof['checks']['all_required_sources_present'] is True
    assert proof['checks']['all_source_totals_reconciled'] is True
    assert proof['checks']['all_rows_accepted'] is True
    assert proof['checks']['lineage_ready'] is True
    assert proof['checks']['sync_logs_ready'] is True
    assert set(proof['source_proof']) == {'gl', 'budget', 'payroll', 'hr', 'sis_enrollment', 'grants', 'banking'}
    assert all(abs(item['variance']) <= 0.01 for item in proof['source_proof'].values())
    assert proof['source_proof']['sis_enrollment']['source_total'] == 595
    assert proof['source_proof']['banking']['loaded_total'] == 110750
    assert proof['signoff']['signed_by'] == 'integration.admin@manchester.edu'
    assert proof['signoff']['all_checks_passed'] is True

    rows = client.get('/api/integrations/real-campus-data-reconciliation/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
