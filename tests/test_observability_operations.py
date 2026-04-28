from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_observability_operations.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}", 'X-Trace-Id': 'test-trace-b61'}


def test_b61_health_probes_metrics_alerts_drills_and_trace_headers() -> None:
    headers = admin_headers()

    ready = client.get('/api/health/ready')
    assert ready.status_code == 200
    assert ready.json()['status'] == 'ready'
    assert ready.headers['x-trace-id']

    status = client.get('/api/observability/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B61'
    assert status.json()['complete'] is True

    probes = client.post('/api/observability/health-probes/run', headers=headers)
    assert probes.status_code == 200
    assert probes.json()['status'] == 'pass'
    assert probes.json()['trace_id'] == 'test-trace-b61'
    assert len(probes.json()['probes']) >= 5

    drill = client.post('/api/observability/backup-restore-drills/run', headers=headers)
    assert drill.status_code == 200
    assert drill.json()['status'] == 'pass'
    assert drill.json()['validation']['integrity_check'] == 'ok'
    assert drill.json()['trace_id'] == 'test-trace-b61'

    metrics = client.get('/api/observability/metrics', headers=headers)
    assert metrics.status_code == 200
    metric_keys = {row['metric_key'] for row in metrics.json()['metrics']}
    assert 'health_probe.failures' in metric_keys
    assert 'backup_restore_drill.size_bytes' in metric_keys

    workspace = client.get('/api/observability/workspace', headers=headers)
    assert workspace.status_code == 200
    assert workspace.json()['health_probes']
    assert workspace.json()['backup_restore_drills']

    logs = client.get('/api/production-ops/application-logs', headers=headers)
    assert logs.status_code == 200


def test_b61_migration_and_ui_surface_exist() -> None:
    headers = admin_headers()
    migrations = client.get('/api/foundation/migrations', headers=headers)
    assert migrations.status_code == 200
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0061_observability_operations' in keys
    assert '0062_documentation_freeze_operator_readiness' in keys
    assert '0069_production_pdf_board_artifact_completion' in keys

    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')
    assert 'id="runHealthProbeButton"' in index
    assert 'id="observabilityMetricTable"' in index
    assert '/api/observability/health-probes/run' in app_js
    assert '/api/observability/backup-restore-drills/run' in app_js
