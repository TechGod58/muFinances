from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_ai_provider_live_guardrails.db'
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


def test_ai_provider_live_guardrails_record_provider_controls_citations_and_approval_policy() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/automation/ai-provider-live-guardrails/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B124'
    assert status.json()['complete'] is True
    assert status.json()['data_controls']['checks']['tenant_scope_limited_to_manchester'] is True

    run = client.post(
        '/api/automation/ai-provider-live-guardrails/run',
        headers=headers,
        json={'scenario_id': sid, 'run_key': 'b124-ai-provider-live'},
    )
    assert run.status_code == 200, run.text
    payload = run.json()
    assert payload['batch'] == 'B124'
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['mode'] in {'live_provider_ready', 'provider_configuration_pending_guarded_fallback'}
    assert payload['checks']['real_ai_provider_configuration_wired'] is True
    assert payload['checks']['provider_secret_not_exposed'] is True
    assert payload['checks']['manchester_data_controls_ready'] is True
    assert payload['checks']['cited_source_tracing_ready'] is True
    assert payload['checks']['prompt_audit_trail_ready'] is True
    assert payload['checks']['confidence_scoring_ready'] is True
    assert payload['checks']['approval_before_posting_ready'] is True
    assert payload['checks']['no_autonomous_posting_ready'] is True
    assert payload['data_controls']['controls']['tenant_scope'] == 'manchester.edu'
    assert payload['data_controls']['controls']['external_training_allowed'] is False
    assert payload['data_controls']['controls']['autonomous_posting_allowed'] is False
    assert payload['certification']['proof']['explanation']['citations']
    assert payload['certification']['review_queue']['pending_count'] >= 1

    rows = client.get('/api/automation/ai-provider-live-guardrails/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
