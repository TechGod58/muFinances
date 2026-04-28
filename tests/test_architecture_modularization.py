from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_architecture_modularization.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_b52_router_contract_and_migration_marker() -> None:
    health = client.get('/api/health')
    assert health.status_code == 200
    assert health.json()['status'] == 'ok'

    login = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert login.status_code == 200
    headers = {'Authorization': f"Bearer {login.json()['token']}"}

    me = client.get('/api/auth/me', headers=headers)
    assert me.status_code == 200
    assert me.json()['email'] == 'admin@mufinances.local'

    migrations = client.get('/api/foundation/migrations', headers=headers)
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0053_architecture_modularization' in keys


def test_b52_architecture_files_exist_and_are_wired() -> None:
    main = (PROJECT_ROOT / 'app' / 'main.py').read_text(encoding='utf-8')
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    assert 'from app.routers import auth, health' in main
    assert 'app.include_router(health.router)' in main
    assert 'app.include_router(auth.router)' in main
    assert 'static/modules/registry.js' in index
    assert (PROJECT_ROOT / 'app' / 'schema_files' / '0053_architecture_modularization.sql').exists()
    assert (PROJECT_ROOT / 'docs' / 'architecture-service-boundaries.md').exists()
