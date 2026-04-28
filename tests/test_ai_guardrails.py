from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_ai_guardrails.db'
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
    scenarios = client.get('/api/scenarios', headers=headers).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def test_ai_guardrails_provider_citations_prompt_audit_and_no_autonomous_posting() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    response = client.post(f'/api/automation/ai-guardrails/run?scenario_id={sid}', headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload['complete'] is True
    assert payload['checks']['real_ai_provider_wiring_ready'] is True
    assert payload['checks']['cited_source_tracing_ready'] is True
    assert payload['checks']['prompt_audit_trail_ready'] is True
    assert payload['checks']['confidence_scores_ready'] is True
    assert payload['checks']['approval_before_posting_ready'] is True
    assert payload['checks']['no_autonomous_posting_ready'] is True
    assert payload['pending_action']['status'] == 'pending_approval'
    assert payload['approved_action']['status'] == 'posted'
    assert payload['blocked_action']['status'] == 'blocked'
    assert payload['ledger_counts']['after_draft'] == payload['ledger_counts']['before']
    assert payload['ledger_counts']['after_approval'] == payload['ledger_counts']['before'] + 1
    assert payload['ledger_counts']['after_blocked_attempt'] == payload['ledger_counts']['after_approval']
    assert payload['explanation']['status'] == 'approved'
    assert payload['explanation']['citations']
    assert payload['explanation']['source_traces']
    assert payload['explanation']['confidence'] > 0


def test_ai_guardrails_status_surface() -> None:
    headers = admin_headers()
    status = client.get('/api/automation/ai-guardrails/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'AI With Guardrails'
    assert status.json()['checks']['no_autonomous_posting_ready'] is True
