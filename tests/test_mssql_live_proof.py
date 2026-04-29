from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_mssql_live_proof.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)
os.environ['CAMPUS_FPM_DB_POOL_SIZE'] = '4'

from fastapi.testclient import TestClient

from app import db
from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def test_mssql_live_server_proof_rehearses_runtime_migrations_indexes_and_restore() -> None:
    headers = admin_headers()
    status = client.get('/api/mssql-live-proof/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B117'
    assert payload['complete'] is True
    assert payload['checks']['mssql_driver_ready'] is True
    assert payload['checks']['mssql_dsn_hook_ready'] is True
    assert payload['checks']['query_plan_sql_ready'] is True

    run = client.post(
        '/api/mssql-live-proof/run',
        headers=headers,
        json={'allow_rehearsal_without_dsn': True, 'attempt_live_connection': False, 'create_backup': True, 'run_restore_validation': True},
    )
    assert run.status_code == 200
    proof = run.json()
    assert proof['status'] == 'passed'
    assert proof['complete'] is True
    assert proof['connection']['status'] in {'rehearsal_ready', 'skipped'}
    assert 'SHOWPLAN_TEXT' in proof['query_plan']['showplan_sql']
    assert proof['index']['translation_ready'] is True
    assert proof['backup']['status'] == 'pass'
    assert proof['checks']['backup_restore_validation_ready'] is True

    translated = db.translate_mssql_sql(
        'CREATE TABLE IF NOT EXISTS b117_check (id INTEGER PRIMARY KEY AUTOINCREMENT, amount REAL, note TEXT)',
        ddl=True,
    )
    assert "OBJECT_ID(N'b117_check'" in translated
    assert 'IDENTITY(1,1)' in translated

    runs = client.get('/api/mssql-live-proof/runs', headers=headers)
    assert runs.status_code == 200
    assert runs.json()['count'] >= 1
