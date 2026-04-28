from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_production_operations.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)
os.environ['CAMPUS_FPM_DB_POOL_SIZE'] = '3'

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def test_production_operations_status_logs_guides_and_audit_report() -> None:
    headers = admin_headers()

    status = client.get('/api/production-ops/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B24'
    assert payload['complete'] is True
    assert payload['checks']['postgresql_option_ready'] is True
    assert payload['checks']['connection_pooling_ready'] is True
    assert payload['checks']['tls_deployment_notes_ready'] is True
    assert payload['checks']['encrypted_secrets_handling_ready'] is True
    assert payload['database']['pool_size'] >= 1

    log = client.post(
        '/api/production-ops/application-logs',
        headers=headers,
        json={'log_type': 'admin', 'severity': 'info', 'message': 'Production hardening smoke log', 'detail': {'batch': 'B24'}},
    )
    assert log.status_code == 200
    assert log.json()['message'] == 'Production hardening smoke log'
    assert log.json()['detail']['batch'] == 'B24'

    logs = client.get('/api/production-ops/application-logs', headers=headers)
    assert logs.status_code == 200
    assert logs.json()['count'] >= 1

    guides = client.get('/api/production-ops/guides', headers=headers)
    assert guides.status_code == 200
    assert guides.json()['count'] == 8
    assert all(item['exists'] for item in guides.json()['guides'])
    assert guides.json()['readiness']['batch'] == 'B62'
    assert guides.json()['readiness']['complete'] is True

    readiness = client.get('/api/production-ops/documentation-readiness', headers=headers)
    assert readiness.status_code == 200
    assert readiness.json()['checks']['controller_guide_ready'] is True
    assert readiness.json()['checks']['release_checklist_ready'] is True

    report = client.get('/api/production-ops/admin-audit-report', headers=headers)
    assert report.status_code == 200
    assert report.json()['totals']['audit_logs'] >= 1
    assert report.json()['by_actor']


def test_production_migration_is_registered() -> None:
    response = client.get('/api/foundation/migrations', headers=admin_headers())
    assert response.status_code == 200
    keys = {row['migration_key'] for row in response.json()['migrations']}
    assert '0025_production_operations_hardening' in keys
    assert '0062_documentation_freeze_operator_readiness' in keys
