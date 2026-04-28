from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_real_campus_data_validation.db'
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


def test_anonymized_manchester_exports_load_and_reconcile_to_source_totals() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/integrations/campus-data-validation/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B90'
    assert payload['complete'] is True
    assert payload['checks']['source_total_reconciliation_ready'] is True

    run = client.post(
        '/api/integrations/campus-data-validation/run',
        headers=headers,
        json={'scenario_id': sid, 'include_default_exports': True},
    )
    assert run.status_code == 200
    result = run.json()
    assert result['status'] == 'passed'
    assert result['complete'] is True
    assert result['source_count'] == 7
    assert result['accepted_rows'] == 14
    assert result['rejected_rows'] == 0
    assert result['checks']['all_sources_loaded'] is True
    assert result['checks']['all_source_totals_reconciled'] is True
    assert result['checks']['source_record_lineage_ready'] is True
    assert result['checks']['connector_sync_logs_populated'] is True

    sources = {item['source_system']: item for item in result['sources']}
    assert set(sources) == {'gl', 'budget', 'payroll', 'hr', 'sis_enrollment', 'grants', 'banking'}
    assert all(abs(item['variance']) < 0.01 for item in sources.values())
    assert sources['sis_enrollment']['source_total'] == 595
    assert sources['sis_enrollment']['loaded_total'] == 595
    assert sources['banking']['source_total'] == 110750
    assert sources['banking']['loaded_total'] == 110750

    rows = client.get('/api/integrations/campus-data-validation/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
