from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_production_data_platform_cutover.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)
os.environ['CAMPUS_FPM_DB_POOL_SIZE'] = '4'

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def test_production_data_platform_status_and_cutover_rehearsal() -> None:
    headers = admin_headers()

    status = client.get('/api/production-data-platform/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B89'
    assert payload['complete'] is True
    assert payload['checks']['postgres_runtime_ready'] is True
    assert payload['checks']['mssql_runtime_ready'] is True
    assert payload['checks']['production_dsn_hooks_ready'] is True
    assert payload['checks']['connection_pooling_ready'] is True

    rehearsal = client.post(
        '/api/production-data-platform/rehearsals/run',
        headers=headers,
        json={'target_backend': 'runtime', 'create_backup': True, 'run_restore_validation': True, 'apply_indexes': True},
    )
    assert rehearsal.status_code == 200
    run = rehearsal.json()
    assert run['status'] == 'passed'
    assert run['complete'] is True
    assert run['checks']['runtime_detected'] is True
    assert run['checks']['postgres_runtime_configurable'] is True
    assert run['checks']['mssql_runtime_configurable'] is True
    assert run['checks']['migration_rehearsal_ready'] is True
    assert run['checks']['schema_drift_detection_ready'] is True
    assert run['checks']['backup_restore_validation_ready'] is True
    assert run['checks']['index_tuning_ready'] is True
    assert run['backup']['restore_test']['status'] == 'pass'
    assert len(run['index']['applied']) >= 1

    rows = client.get('/api/production-data-platform/rehearsals', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
