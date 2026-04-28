from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_real_connector_activation.db'
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


def test_real_connector_activation_certifies_config_sync_drillback_and_rejections() -> None:
    headers = admin_headers()

    status = client.get('/api/integrations/real-connector-activation/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B99'
    assert status.json()['complete'] is True

    run = client.post('/api/integrations/real-connector-activation/run', headers=headers, json={})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert len(payload['connectors']) == 7
    assert payload['checks']['actual_connector_configs_ready'] is True
    assert payload['checks']['credential_vault_ready'] is True
    assert payload['checks']['scheduled_syncs_ready'] is True
    assert payload['checks']['retry_handling_ready'] is True
    assert payload['checks']['source_drillback_ready'] is True
    assert payload['checks']['rejection_queues_ready'] is True
    assert payload['checks']['sync_logs_ready'] is True
    assert payload['proof']['complete'] is True
    assert payload['evidence']['credentials'] >= 7
    assert payload['evidence']['rejection_queue'] >= 1

    rows = client.get('/api/integrations/real-connector-activation/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
