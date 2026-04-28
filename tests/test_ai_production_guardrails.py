from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_ai_production_guardrails.db'
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


def scenario_id(headers: dict[str, str]) -> int:
    response = client.get('/api/scenarios', headers=headers)
    assert response.status_code == 200
    return int(response.json()[0]['id'])


def test_ai_production_guardrails_prove_provider_prompt_citations_and_approval_queue() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/automation/ai-production-guardrails/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B102'
    assert status.json()['complete'] is True

    run = client.post('/api/automation/ai-production-guardrails/run', headers=headers, json={'scenario_id': sid})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['real_ai_provider_configuration_ready'] is True
    assert payload['checks']['cited_source_tracing_ready'] is True
    assert payload['checks']['confidence_scoring_ready'] is True
    assert payload['checks']['prompt_audit_ready'] is True
    assert payload['checks']['human_approval_gates_ready'] is True
    assert payload['checks']['no_autonomous_posting_ready'] is True
    assert payload['checks']['ai_action_review_queue_ready'] is True
    assert payload['proof']['complete'] is True
    assert payload['proof']['explanation']['confidence'] > 0
    assert len(payload['proof']['explanation']['citations']) >= 1
    assert payload['review_queue']['pending_count'] >= 1
    assert payload['review_queue']['blocked_count'] >= 1

    rows = client.get('/api/automation/ai-production-guardrails/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
