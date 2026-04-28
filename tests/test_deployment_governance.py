from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_deployment_governance.db'
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


def test_deployment_governance_release_control_flow() -> None:
    headers = admin_headers()

    env = client.post(
        '/api/deployment-governance/environments',
        headers=headers,
        json={
            'environment_key': 'staging',
            'tenant_key': 'manchester',
            'base_url': 'https://mufinances.manchester.edu',
            'database_backend': 'postgres',
            'sso_required': True,
            'domain_guard_required': True,
            'settings': {'port': 3200},
            'status': 'ready',
        },
    )
    assert env.status_code == 200
    assert env.json()['sso_required'] is True

    snapshot = client.post(
        '/api/deployment-governance/config-snapshots',
        headers=headers,
        json={'environment_key': 'staging', 'direction': 'export', 'payload': {}},
    )
    assert snapshot.status_code == 200
    assert snapshot.json()['payload']['environment']['environment_key'] == 'staging'

    promotion = client.post(
        '/api/deployment-governance/promotions',
        headers=headers,
        json={
            'from_environment': 'staging',
            'to_environment': 'production',
            'release_version': 'B50.0',
            'checklist': {'tests_passed': True, 'backup_verified': True, 'readiness_complete': True},
        },
    )
    assert promotion.status_code == 200
    assert promotion.json()['status'] == 'approved'

    rollback = client.post(
        '/api/deployment-governance/rollback-plans',
        headers=headers,
        json={
            'migration_key': '0051_deployment_governance_release_controls',
            'rollback_strategy': 'Restore backup and redeploy previous build.',
            'verification_steps': ['Integrity check', 'Login smoke', 'Status smoke'],
            'status': 'approved',
        },
    )
    assert rollback.status_code == 200
    assert rollback.json()['verification_steps'][0] == 'Integrity check'

    notes = client.post(
        '/api/deployment-governance/release-notes',
        headers=headers,
        json={'release_version': 'B50.0', 'title': 'Release governance', 'notes': {'added': ['promotion controls']}, 'status': 'published'},
    )
    assert notes.status_code == 200
    assert notes.json()['published_by'] == 'admin@mufinances.local'

    diagnostic = client.post('/api/deployment-governance/diagnostics/run?scope=release', headers=headers)
    assert diagnostic.status_code == 200
    assert diagnostic.json()['status'] == 'pass'
    assert diagnostic.json()['result']['latest_migration'] == '0069_production_pdf_board_artifact_completion'
    assert diagnostic.json()['result']['health_probes']['status'] == 'pass'

    readiness = client.post(
        '/api/deployment-governance/readiness',
        headers=headers,
        json={'item_key': 'release-governance-ready', 'category': 'operations', 'title': 'Release governance reviewed', 'status': 'ready', 'evidence': {'diagnostic': diagnostic.json()['diagnostic_key']}},
    )
    assert readiness.status_code == 200
    assert readiness.json()['status'] == 'ready'

    workspace = client.get('/api/deployment-governance/workspace', headers=headers)
    assert workspace.status_code == 200
    payload = workspace.json()
    assert payload['status']['batch'] == 'B50'
    assert payload['environments']
    assert payload['promotions']
    assert payload['config_snapshots']
    assert payload['rollback_plans']
    assert payload['release_notes']
    assert payload['diagnostics']
    assert len(payload['readiness_items']) >= 6


def test_deployment_governance_status_and_migration_are_registered() -> None:
    headers = admin_headers()

    status = client.get('/api/deployment-governance/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B50'
    assert payload['complete'] is True
    assert payload['checks']['environment_promotion_ready'] is True

    migrations = client.get('/api/foundation/migrations', headers=headers)
    assert migrations.status_code == 200
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0051_deployment_governance_release_controls' in keys


def test_deployment_governance_ui_surface_exists() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')

    assert 'id="deployment-governance"' in index
    assert 'id="environmentSettingsTable"' in index
    assert 'id="environmentPromotionTable"' in index
    assert 'id="readinessChecklistTable"' in index
    assert 'id="runAdminDiagnosticsButton"' in index
    assert '/api/deployment-governance/workspace' in app_js
    assert 'renderDeploymentGovernance' in app_js
