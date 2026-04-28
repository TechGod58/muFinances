from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_deployment_operations.db'
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


def test_operations_checks_backup_restore_test_and_runbook() -> None:
    headers = admin_headers()
    check = client.post(
        '/api/operations/checks',
        headers=headers,
        json={'check_key': 'database', 'category': 'health'},
    )
    assert check.status_code == 200
    assert check.json()['status'] == 'pass'

    backup = client.post('/api/operations/backups', headers=headers)
    assert backup.status_code == 200
    assert backup.json()['backup_key'].startswith('backup-')

    restore_test = client.post(
        '/api/operations/restore-tests',
        headers=headers,
        json={'backup_key': backup.json()['backup_key']},
    )
    assert restore_test.status_code == 200
    assert restore_test.json()['status'] == 'pass'
    assert restore_test.json()['validation']['integrity_check'] == 'ok'

    runbook = client.post(
        '/api/operations/runbooks',
        headers=headers,
        json={
            'runbook_key': 'deployment',
            'title': 'Deployment Runbook',
            'category': 'deployment',
            'path': 'docs/runbooks/deployment.md',
            'status': 'ready',
        },
    )
    assert runbook.status_code == 200
    assert runbook.json()['status'] == 'ready'


def test_operations_status_reports_b12_complete() -> None:
    response = client.get('/api/operations/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B12'
    assert payload['complete'] is True
    assert payload['checks']['windows_service_ready'] is True
    assert payload['checks']['docker_packaging_ready'] is True
