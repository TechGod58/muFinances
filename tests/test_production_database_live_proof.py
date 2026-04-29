from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_production_database_live_proof.db'
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


def test_b155_production_database_live_proof_records_runtime_cutover_mssql_and_restore_evidence() -> None:
    headers = admin_headers()

    status = client.get('/api/production-database-live-proof/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B155'
    assert payload['complete'] is True
    assert payload['checks']['runtime_backend_classified'] is True
    assert payload['checks']['production_dsn_hooks_ready'] is True
    assert payload['checks']['query_plan_and_index_ready'] is True

    run = client.post(
        '/api/production-database-live-proof/run',
        headers=headers,
        json={
            'run_key': 'b155-regression',
            'target_backend': 'runtime',
            'allow_rehearsal_without_dsn': True,
            'attempt_live_connection': False,
            'create_backup': True,
            'run_restore_validation': True,
            'apply_indexes': True,
            'signed_by': 'it.database@manchester.edu',
        },
    )
    assert run.status_code == 200, run.text
    proof = run.json()
    assert proof['status'] == 'passed'
    assert proof['complete'] is True
    assert proof['checks']['migration_rehearsal_ready'] is True
    assert proof['checks']['schema_drift_detection_ready'] is True
    assert proof['checks']['backup_restore_validation_ready'] is True
    assert proof['checks']['live_connection_policy_ready'] is True
    assert proof['cutover']['status'] == 'passed'
    assert proof['mssql']['connection']['status'] in {'rehearsal_ready', 'skipped'}
    assert proof['mssql']['backup']['status'] == 'pass'
    assert proof['signoff']['signed_by'] == 'it.database@manchester.edu'
    assert proof['signoff']['all_checks_passed'] is True

    runs = client.get('/api/production-database-live-proof/runs', headers=headers)
    assert runs.status_code == 200
    assert runs.json()['count'] >= 1
