from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_ai_explainability.db'
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


def seeded_scenario_id(headers: dict[str, str]) -> int:
    scenarios = client.get('/api/scenarios', headers=headers).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def test_ai_explainability_status_reports_b34_complete() -> None:
    response = client.get('/api/ai-explainability/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B34'
    assert payload['complete'] is True
    assert payload['checks']['source_tracing_ready'] is True


def test_cited_variance_explanation_confidence_trace_and_approval() -> None:
    headers = admin_headers()
    scenario_id = seeded_scenario_id(headers)
    draft = client.post(f'/api/ai-explainability/explanations/draft?scenario_id={scenario_id}', headers=headers, json={})
    assert draft.status_code == 200
    assert draft.json()['count'] >= 1
    explanation = draft.json()['explanations'][0]
    assert explanation['subject_type'] == 'variance'
    assert explanation['confidence'] >= 0.55
    assert len(explanation['citations']) == 4
    assert len(explanation['source_traces']) == 3
    assert explanation['source_traces'][0]['transformation'] == 'actual_budget_forecast_variance aggregation'

    submitted = client.post(f"/api/ai-explainability/explanations/{explanation['id']}/submit", headers=headers, json={})
    assert submitted.status_code == 200
    assert submitted.json()['status'] == 'pending_approval'

    approved = client.post(
        f"/api/ai-explainability/explanations/{explanation['id']}/approve",
        headers=headers,
        json={'note': 'Citations reviewed.'},
    )
    assert approved.status_code == 200
    assert approved.json()['status'] == 'approved'
    assert approved.json()['approved_by'] == 'admin@mufinances.local'

    listing = client.get(f'/api/ai-explainability/explanations?scenario_id={scenario_id}', headers=headers)
    assert listing.status_code == 200
    assert listing.json()['count'] >= 1
    assert listing.json()['explanations'][0]['citations']
