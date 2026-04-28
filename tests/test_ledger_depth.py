from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_ledger_depth.db'
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


def scenario_id() -> int:
    scenarios = client.get('/api/scenarios', headers=admin_headers()).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def test_journal_adjustment_posts_actuals_with_source_lineage() -> None:
    headers = admin_headers()
    sid = scenario_id()

    journal = client.post(
        '/api/ledger-depth/journals',
        headers=headers,
        json={
            'scenario_id': sid,
            'period': '2026-08',
            'department_code': 'SCI',
            'fund_code': 'GEN',
            'account_code': 'SUPPLIES',
            'amount': -500,
            'ledger_basis': 'actual',
            'reason': 'Post actual supplies adjustment',
        },
    )
    assert journal.status_code == 200
    assert journal.json()['status'] == 'draft'

    submitted = client.post(f"/api/ledger-depth/journals/{journal.json()['id']}/submit", headers=headers)
    assert submitted.status_code == 200
    assert submitted.json()['status'] == 'pending_approval'

    approved = client.post(f"/api/ledger-depth/journals/{journal.json()['id']}/approve", headers=headers)
    assert approved.status_code == 200
    payload = approved.json()
    assert payload['status'] == 'posted'
    assert payload['ledger_entry']['ledger_basis'] == 'actual'
    assert payload['ledger_entry']['source_version'] == f"journal-{journal.json()['id']}"
    assert payload['ledger_entry']['source_record_id'] == str(journal.json()['id'])

    summary = client.get(f'/api/ledger-depth/basis-summary?scenario_id={sid}', headers=headers)
    assert summary.status_code == 200
    assert any(item['ledger_basis'] == 'actual' for item in summary.json()['basis'])


def test_scenario_publication_lock_and_approved_merge() -> None:
    headers = admin_headers()
    sid = scenario_id()
    clone = client.post(
        f'/api/scenario-engine/scenarios/{sid}/clone',
        headers=headers,
        json={'name': 'FY27 Approved Merge Source', 'version': 'approved-source'},
    )
    assert clone.status_code == 200
    source_id = clone.json()['id']

    approved = client.post(f'/api/scenarios/{source_id}/approve', headers=headers)
    assert approved.status_code == 200
    assert approved.json()['status'] == 'approved'
    assert approved.json()['locked'] is False

    merge_target = client.post(
        '/api/scenarios',
        headers=headers,
        json={'name': 'FY27 Merge Target', 'version': 'merge-target', 'start_period': '2026-07', 'end_period': '2026-12'},
    )
    assert merge_target.status_code == 200
    merge = client.post(
        f"/api/scenarios/{merge_target.json()['id']}/merge-approved",
        headers=headers,
        json={'source_scenario_id': source_id, 'note': 'B13 approved merge test'},
    )
    assert merge.status_code == 200
    assert merge.json()['created_count'] > 0
    assert merge.json()['created_entries'][0]['ledger_basis'] == 'scenario'
    assert merge.json()['created_entries'][0]['parent_ledger_entry_id'] is not None

    published = client.post(f'/api/scenarios/{merge_target.json()["id"]}/publish', headers=headers)
    assert published.status_code == 200
    assert published.json()['status'] == 'published'
    assert published.json()['locked'] is True

    blocked = client.post(
        f'/api/scenarios/{merge_target.json()["id"]}/line-items',
        headers=headers,
        json={'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': 'SUPPLIES', 'period': '2026-08', 'amount': -1},
    )
    assert blocked.status_code == 409


def test_ledger_depth_status_reports_b13_complete() -> None:
    response = client.get('/api/ledger-depth/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B13'
    assert payload['complete'] is True
    assert payload['checks']['approved_change_merge_ready'] is True
