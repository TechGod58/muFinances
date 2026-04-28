from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_migration_framework.db'
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


def test_migration_framework_status_dry_run_and_rollback_plan() -> None:
    headers = admin_headers()

    status = client.get('/api/migrations/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B53'
    assert payload['complete'] is True
    assert payload['checks']['migration_locks_ready'] is True
    assert any(item['migration_key'] == '0054_real_migration_framework' for item in payload['migrations'])

    dry_run = client.post('/api/migrations/dry-run?target_key=0054_real_migration_framework', headers=headers)
    assert dry_run.status_code == 200
    assert dry_run.json()['dry_run'] is True
    assert dry_run.json()['results'][0]['status'] in {'skipped', 'validated'}

    runs = client.get('/api/migrations/runs', headers=headers)
    assert runs.status_code == 200
    assert runs.json()['count'] >= 1
    assert runs.json()['runs'][0]['migration_key'] == '0054_real_migration_framework'

    rollback = client.get('/api/migrations/rollback-plan/0054_real_migration_framework', headers=headers)
    assert rollback.status_code == 200
    assert rollback.json()['available'] is True
    assert any('DROP TABLE IF EXISTS migration_runs' in step for step in rollback.json()['steps'])


def test_migration_framework_files_and_docs_exist() -> None:
    assert (PROJECT_ROOT / 'app' / 'schema_files' / '0054_real_migration_framework.sql').exists()
    assert (PROJECT_ROOT / 'app' / 'schema_files' / 'rollback' / '0054_real_migration_framework.sql').exists()
    assert (PROJECT_ROOT / 'docs' / 'migration-framework.md').exists()
