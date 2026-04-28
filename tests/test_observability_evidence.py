from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_observability_evidence.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}", 'X-Trace-Id': 'test-observability-evidence'}


def test_observability_evidence_populates_dashboard_health_alerts_jobs_and_drills() -> None:
    headers = admin_headers()

    response = client.post('/api/observability/evidence/run', headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload['complete'] is True
    assert payload['trace_id'] == 'test-observability-evidence'
    assert payload['checks']['metrics_populated'] is True
    assert payload['checks']['health_probes_populated'] is True
    assert payload['checks']['alerts_populated'] is True
    assert payload['checks']['backup_drill_records_populated'] is True
    assert payload['checks']['job_diagnostics_populated'] is True
    assert payload['checks']['operational_dashboard_evidence_populated'] is True
    assert payload['health']['status'] == 'pass'
    assert payload['backup_restore_drill']['status'] == 'pass'
    assert payload['job']['ran'] is True
    assert payload['alert']['status'] == 'open'
    assert payload['diagnostic']['status'] == 'pass'
    assert payload['workspace']['metrics']
    assert payload['workspace']['job_logs']

    status = client.get('/api/observability/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['counts']['job_logs'] > 0
    assert status.json()['counts']['admin_diagnostics'] > 0
