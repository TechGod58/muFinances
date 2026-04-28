from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_documentation_lock.db'
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


def test_documentation_lock_status_and_run_cover_all_required_guides() -> None:
    headers = admin_headers()

    status = client.get('/api/documentation-lock/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B109'
    assert payload['complete'] is True
    assert payload['count'] == 9
    assert all(payload['checks'].values())

    guide_keys = {guide['guide_key'] for guide in payload['guides']}
    assert guide_keys == {
        'admin_guide',
        'planner_guide',
        'controller_guide',
        'integration_guide',
        'data_dictionary',
        'close_guide',
        'deployment_guide',
        'recovery_guide',
        'security_guide',
    }
    assert all(guide['checksum'] and guide['ready'] for guide in payload['guides'])

    run = client.post('/api/documentation-lock/run', headers=headers, json={'lock_key': 'b109-regression'})
    assert run.status_code == 200
    run_payload = run.json()
    assert run_payload['status'] == 'locked'
    assert run_payload['complete'] is True
    assert len(run_payload['items']) == 9
    assert all(item['status'] == 'locked' for item in run_payload['items'])
    assert all(not item['missing_sections'] for item in run_payload['items'])

    detail = client.get(f"/api/documentation-lock/runs/{run_payload['id']}", headers=headers)
    assert detail.status_code == 200
    assert detail.json()['lock_key'] == 'b109-regression'

    rows = client.get('/api/documentation-lock/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1


def test_documentation_lock_required_guide_files_exist() -> None:
    required = [
        'admin-guide.md',
        'planner-guide.md',
        'controller-guide.md',
        'integration-guide.md',
        'data-dictionary.md',
        'close-process-guide.md',
        'deployment-guide.md',
        'recovery-guide.md',
        'security-guide.md',
    ]
    for name in required:
        path = PROJECT_ROOT / 'docs' / 'guides' / name
        assert path.exists()
        assert path.read_text(encoding='utf-8').strip()
