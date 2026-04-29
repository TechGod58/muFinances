from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_final_ui_polish_and_gap_review.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}", 'X-Trace-Id': 'test-b128-b129'}


def scenario_id(headers: dict[str, str]) -> int:
    response = client.get('/api/bootstrap', headers=headers)
    assert response.status_code == 200
    return int(response.json()['activeScenario']['id'])


def test_b128_pilot_defect_fix_and_final_ui_polish_records_evidence() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/ui/pilot-defect-polish/status', headers=headers)
    assert status.status_code == 200
    status_payload = status.json()
    assert status_payload['batch'] == 'B128'
    assert status_payload['complete'] is True
    assert status_payload['checks']['workspace_state_finished'] is True
    assert status_payload['checks']['dock_undock_finished'] is True
    assert status_payload['checks']['chat_popout_finished'] is True
    assert status_payload['checks']['command_gap_and_ready_status_finished'] is True

    run = client.post(
        '/api/ui/pilot-defect-polish/run',
        headers=headers,
        json={'run_key': 'b128-regression', 'scenario_id': sid},
    )
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['pilot_defect_queue_reviewed'] is True
    assert payload['checks']['ux_finance_polish_passed'] is True
    assert len(payload['defects']) >= 6
    assert all(defect['status'] == 'fixed' for defect in payload['defects'])
    assert payload['artifacts']['ui_asset_evidence']['workspace_script_loaded'] is True
    assert payload['artifacts']['ui_asset_evidence']['dock_script_loaded'] is True
    assert 'chat pop-out window' in payload['artifacts']['playwright_coverage']

    rows = client.get('/api/ui/pilot-defect-polish/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1


def test_b129_prophix_class_final_gap_review_uses_only_real_pilot_failures() -> None:
    headers = admin_headers()

    pilot = client.post(
        '/api/multi-user-pilot-cycle/run',
        headers=headers,
        json={'run_key': 'b129-pilot-cycle', 'release_version': 'B129.pilot'},
    )
    assert pilot.status_code == 200
    assert pilot.json()['status'] == 'passed'

    review = client.post(
        '/api/prophix-final-gap-review/run',
        headers=headers,
        json={'run_key': 'b129-regression', 'pilot_cycle_run_id': pilot.json()['id']},
    )
    assert review.status_code == 200
    payload = review.json()
    assert payload['batch'] == 'B129'
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['gaps'] == []
    assert payload['checks']['real_pilot_evidence_used'] is True
    assert payload['checks']['remaining_gaps_are_failed_pilot_evidence_only'] is True
    assert payload['checks']['theoretical_gaps_excluded'] is True
    assert {'Prophix', 'Workday Adaptive Planning', 'Planful', 'Anaplan'} == set(payload['vendor_sources'])
    assert len(payload['matrix']) >= 10
    assert any(row['mufinances_status'] == 'met' for row in payload['matrix'])
    assert any(row['mufinances_status'] == 'not_observed_in_pilot' for row in payload['matrix'])
    assert all(item['reason'] == 'not_failed_in_pilot' for item in payload['theoretical_exclusions'])

    status = client.get('/api/prophix-final-gap-review/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['latest_run']['run_key'] == 'b129-regression'
