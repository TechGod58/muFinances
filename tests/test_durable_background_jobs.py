from __future__ import annotations

import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_durable_background_jobs.db'
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


def test_scheduled_jobs_logs_cancel_and_dead_letters() -> None:
    headers = admin_headers()
    due = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    scheduled = client.post(
        '/api/performance/jobs',
        headers=headers,
        json={'job_type': 'cache_invalidation', 'priority': 5, 'scheduled_for': due, 'payload': {'cache_key': 'b55', 'scope': 'global', 'reason': 'scheduled smoke'}},
    )
    assert scheduled.status_code == 200
    assert scheduled.json()['status'] == 'scheduled'

    promoted = client.post('/api/performance/jobs/promote-due', headers=headers)
    assert promoted.status_code == 200
    assert promoted.json()['promoted'] >= 1
    ran = client.post('/api/performance/jobs/run-next', headers=headers)
    assert ran.status_code == 200
    assert ran.json()['job']['status'] == 'completed'

    logs = client.get(f"/api/performance/job-logs?job_id={ran.json()['job']['id']}", headers=headers)
    assert logs.status_code == 200
    assert {item['event_type'] for item in logs.json()['logs']} >= {'queued', 'started', 'completed'}

    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    cancellable = client.post(
        '/api/performance/jobs',
        headers=headers,
        json={'job_type': 'cache_invalidation', 'priority': 20, 'scheduled_for': future, 'payload': {'cache_key': 'cancel-me'}},
    )
    cancelled = client.post(f"/api/performance/jobs/{cancellable.json()['id']}/cancel", headers=headers)
    assert cancelled.status_code == 200
    assert cancelled.json()['status'] == 'cancelled'

    failing = client.post(
        '/api/performance/jobs',
        headers=headers,
        json={'job_type': 'backup_restore_test', 'priority': 1, 'max_attempts': 1, 'backoff_seconds': 1, 'payload': {'backup_key': 'missing-b55-backup'}},
    )
    assert failing.status_code == 200
    failed_run = client.post('/api/performance/jobs/run-next', headers=headers)
    assert failed_run.status_code == 200
    assert failed_run.json()['job']['status'] == 'dead_letter'
    dead = client.get('/api/performance/dead-letters', headers=headers)
    assert dead.status_code == 200
    assert dead.json()['count'] >= 1
    assert dead.json()['dead_letters'][0]['job_key'] == failing.json()['job_key']


def test_b55_status_migration_and_worker_assets() -> None:
    headers = admin_headers()
    status = client.get('/api/performance/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['checks']['scheduled_jobs_ready'] is True
    assert payload['checks']['dead_letter_ready'] is True
    assert payload['checks']['worker_deployment_ready'] is True

    migrations = client.get('/api/foundation/migrations', headers=headers)
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0056_durable_background_jobs_scheduler' in keys
    assert (PROJECT_ROOT / 'app' / 'worker.py').exists()
    assert (PROJECT_ROOT / 'deploy' / 'mufinances-worker.ps1').exists()
    assert (PROJECT_ROOT / 'docs' / 'worker-deployment.md').exists()
