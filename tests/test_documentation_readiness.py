from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_documentation_readiness.db'
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


def test_b62_operator_guides_are_complete_and_registered() -> None:
    headers = admin_headers()

    readiness = client.get('/api/production-ops/documentation-readiness', headers=headers)
    assert readiness.status_code == 200
    payload = readiness.json()
    assert payload['batch'] == 'B62'
    assert payload['complete'] is True
    assert payload['count'] == 8
    assert all(guide['ready'] for guide in payload['guides'])
    assert {guide['key'] for guide in payload['guides']} >= {
        'admin-guide',
        'planner-guide',
        'controller-guide',
        'integration-guide',
        'data-dictionary',
        'close-process-guide',
        'release-checklist',
    }

    runbooks = client.get('/api/operations/runbooks', headers=headers)
    assert runbooks.status_code == 200
    keys = {row['runbook_key'] for row in runbooks.json()['runbooks']}
    assert 'controller-guide' in keys
    assert 'release-checklist' in keys

    migrations = client.get('/api/foundation/migrations', headers=headers)
    assert migrations.status_code == 200
    migration_keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0062_documentation_freeze_operator_readiness' in migration_keys


def test_b62_guide_files_have_operator_sections() -> None:
    required = {
        'admin-guide.md': ['## Daily Checks', '## Security Operations', '## Incident Response'],
        'planner-guide.md': ['## Planning Workflow', '## Data Entry', '## Planner Closeout'],
        'controller-guide.md': ['## Controller Workspace', '## Consolidation', '## Signoff'],
        'integration-guide.md': ['## Mapping And Validation', '## Sync Logs', '## Recovery'],
        'data-dictionary.md': ['## Security And Audit', '## Operations', '## Retention Notes'],
        'close-process-guide.md': ['## Reconciliation Workflow', '## Consolidation Workflow', '## Audit Packet'],
        'release-checklist.md': ['## Pre-Release Freeze', '## Smoke Tests', '## Release Signoff'],
    }
    for filename, sections in required.items():
        text = (PROJECT_ROOT / 'docs' / 'guides' / filename).read_text(encoding='utf-8')
        for section in sections:
            assert section in text
