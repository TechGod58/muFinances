from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_narrative_reporting.db'
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


def test_narrative_reporting_variance_workflow() -> None:
    headers = admin_headers()
    sid = scenario_id()

    threshold = client.post(
        '/api/reporting/variance-thresholds',
        headers=headers,
        json={'scenario_id': sid, 'threshold_key': 'test-materiality', 'amount_threshold': 1, 'percent_threshold': None, 'require_explanation': True},
    )
    assert threshold.status_code == 200
    assert threshold.json()['amount_threshold'] == 1

    generated = client.post(f'/api/reporting/variance-explanations/generate?scenario_id={sid}', headers=headers, json={})
    assert generated.status_code == 200
    assert generated.json()['explanations']
    explanation = generated.json()['explanations'][0]

    drafted = client.post(f'/api/reporting/variance-explanations/draft?scenario_id={sid}', headers=headers, json={})
    assert drafted.status_code == 200
    assert drafted.json()['count'] >= 1

    commented = client.post(
        '/api/reporting/variance-explanations',
        headers=headers,
        json={
            'scenario_id': sid,
            'variance_key': explanation['variance_key'],
            'explanation_text': 'Variance is due to timing between actual posting and budget phasing.',
        },
    )
    assert commented.status_code == 200
    assert commented.json()['status'] == 'draft'

    submitted = client.post(f"/api/reporting/variance-explanations/{commented.json()['id']}/submit", headers=headers, json={})
    assert submitted.status_code == 200
    assert submitted.json()['status'] == 'pending_approval'

    approved = client.post(
        f"/api/reporting/variance-explanations/{commented.json()['id']}/approve",
        headers=headers,
        json={'note': 'Approved for board package.'},
    )
    assert approved.status_code == 200
    assert approved.json()['status'] == 'approved'

    package = client.post(
        '/api/reporting/board-packages',
        headers=headers,
        json={'scenario_id': sid, 'package_name': 'B18 Board Package', 'period_start': '2026-07', 'period_end': '2026-12'},
    )
    assert package.status_code == 200

    narrative = client.post(
        '/api/reporting/narratives',
        headers=headers,
        json={'scenario_id': sid, 'title': 'B18 Board Narrative', 'package_id': package.json()['id']},
    )
    assert narrative.status_code == 200
    assert narrative.json()['status'] == 'pending_approval'
    assert narrative.json()['narrative']['variance_commentary']
    assert narrative.json()['narrative']['human_approval_required'] is True

    narrative_approved = client.post(
        f"/api/reporting/narratives/{narrative.json()['id']}/approve",
        headers=headers,
        json={'note': 'Approved.'},
    )
    assert narrative_approved.status_code == 200
    assert narrative_approved.json()['status'] == 'approved'


def test_reporting_status_reports_b18_complete() -> None:
    response = client.get('/api/reporting/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B18'
    assert payload['complete'] is True
    assert payload['checks']['commentary_workflow_ready'] is True
    assert payload['checks']['ai_drafted_narratives_human_approval_ready'] is True
