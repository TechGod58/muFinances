from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_prophix_parity_pilot_signoff.db'
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


def test_b160_prophix_parity_pilot_signoff_records_pilot_parity_release_and_signoffs() -> None:
    headers = admin_headers()

    status = client.get('/api/prophix-parity-pilot-signoff/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B160'
    assert payload['complete'] is True

    run = client.post(
        '/api/prophix-parity-pilot-signoff/run',
        headers=headers,
        json={
            'run_key': 'b160-regression',
            'release_version': 'B160.test',
            'finance_signoff_by': 'finance.owner@manchester.edu',
            'it_signoff_by': 'it.owner@manchester.edu',
            'parity_signoff_by': 'program.owner@manchester.edu',
        },
    )
    assert run.status_code == 200, run.text
    proof = run.json()
    assert proof['batch'] == 'B160'
    assert proof['status'] == 'passed'
    assert proof['complete'] is True
    assert proof['checks']['pilot_deployment_passed'] is True
    assert proof['checks']['pilot_user_signoff_recorded'] is True
    assert proof['checks']['multi_user_pilot_cycle_passed'] is True
    assert proof['checks']['minimum_viable_parity_matrix_passed'] is True
    assert proof['checks']['final_gap_review_uses_real_pilot_evidence'] is True
    assert proof['checks']['release_candidate_passed'] is True
    assert proof['checks']['finance_signoff_recorded'] is True
    assert proof['checks']['it_signoff_recorded'] is True
    assert proof['signoffs']['finance']['signed_by'] == 'finance.owner@manchester.edu'
    assert proof['signoffs']['it']['signed_by'] == 'it.owner@manchester.edu'
    assert proof['signoffs']['parity']['signed_by'] == 'program.owner@manchester.edu'
    assert proof['release_candidate']['release_version'] == 'B160.test'
    assert proof['final_gap_review']['checks']['remaining_gaps_are_failed_pilot_evidence_only'] is True

    rows = client.get('/api/prophix-parity-pilot-signoff/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
