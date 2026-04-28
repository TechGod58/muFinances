from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_governed_automation.db'
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


def test_governed_assistants_create_reviewable_recommendations() -> None:
    headers = admin_headers()
    sid = scenario_id()

    reconciliation = client.post(
        '/api/close/reconciliations',
        headers=headers,
        json={
            'scenario_id': sid,
            'period': '2026-08',
            'entity_code': 'CAMPUS',
            'account_code': 'TUITION',
            'source_balance': 1,
            'owner': 'Controller',
            'notes': 'Create variance for automation suggestion',
        },
    )
    assert reconciliation.status_code == 200

    run = client.post(
        '/api/automation/run',
        headers=headers,
        json={'scenario_id': sid, 'assistant_type': 'reconciliation'},
    )
    assert run.status_code == 200
    payload = run.json()
    assert payload['count'] >= 1
    recommendation = payload['recommendations'][0]
    assert recommendation['status'] == 'pending_review'
    assert recommendation['approval_gates'][0]['status'] == 'pending'

    approve = client.post(
        f"/api/automation/recommendations/{recommendation['id']}/approve",
        headers=headers,
        json={'note': 'Reviewed by controller'},
    )
    assert approve.status_code == 200
    assert approve.json()['status'] == 'approved'
    assert approve.json()['approval_gates'][0]['status'] == 'approved'


def test_variance_anomaly_budget_assistants_and_status() -> None:
    headers = admin_headers()
    sid = scenario_id()

    for assistant_type in ['variance', 'anomaly', 'budget']:
        response = client.post(
            '/api/automation/run',
            headers=headers,
            json={'scenario_id': sid, 'assistant_type': assistant_type},
        )
        assert response.status_code == 200
        assert response.json()['count'] >= 1

    status = client.get('/api/automation/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B10'
    assert payload['complete'] is True
    assert payload['checks']['human_approval_gates_ready'] is True


def test_ai_planning_agents_prompt_audit_guard_and_approval_before_posting() -> None:
    headers = admin_headers()
    sid = scenario_id()

    before = client.get(f'/api/foundation/ledger?scenario_id={sid}', headers=headers)
    assert before.status_code == 200
    before_count = len(before.json()['entries'])

    draft = client.post(
        '/api/automation/planning-agents/run',
        headers=headers,
        json={'scenario_id': sid, 'agent_type': 'budget_update', 'prompt_text': 'Add 2500 to science supplies for 2026-08'},
    )
    assert draft.status_code == 200
    assert draft.json()['prompt']['parsed_intent']['amount'] == 2500
    action = draft.json()['action']
    assert action['status'] == 'pending_approval'
    assert action['guard_status'] == 'passed'
    assert action['proposal']['action_type'] == 'ledger_post'

    pending = client.get(f'/api/foundation/ledger?scenario_id={sid}', headers=headers)
    assert len(pending.json()['entries']) == before_count

    approved = client.post(
        f"/api/automation/planning-agents/actions/{action['id']}/approve",
        headers=headers,
        json={'note': 'Human reviewed prompt and proposed ledger line.'},
    )
    assert approved.status_code == 200
    assert approved.json()['status'] == 'posted'
    assert approved.json()['result']['ledger_entry_id']

    after = client.get(f'/api/foundation/ledger?scenario_id={sid}', headers=headers)
    assert len(after.json()['entries']) == before_count + 1

    prompts = client.get(f'/api/automation/planning-agents/prompts?scenario_id={sid}', headers=headers)
    assert prompts.status_code == 200
    assert prompts.json()['prompts'][0]['prompt_text'].startswith('Add 2500')


def test_ai_planning_agents_report_question_anomaly_and_status() -> None:
    headers = admin_headers()
    sid = scenario_id()

    for agent_type, prompt in [
        ('bulk_adjustment', 'Increase supplies by 3% for 2026-08'),
        ('report_question', 'What is the current net position?'),
        ('anomaly_explanation', 'Explain the largest unusual ledger movement'),
    ]:
        response = client.post(
            '/api/automation/planning-agents/run',
            headers=headers,
            json={'scenario_id': sid, 'agent_type': agent_type, 'prompt_text': prompt},
        )
        assert response.status_code == 200
        assert response.json()['action']['status'] == 'pending_approval'

    status = client.get('/api/automation/planning-agents/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B41'
    assert payload['complete'] is True
    assert payload['checks']['prompt_audit_trail_ready'] is True
    assert payload['checks']['human_approval_before_posting_ready'] is True


def test_ai_planning_agents_ui_contract() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')

    assert 'id="runBudgetUpdateAgentButton"' in index
    assert 'id="runBulkAdjustmentAgentButton"' in index
    assert 'id="runReportQuestionAgentButton"' in index
    assert 'id="runAnomalyExplanationAgentButton"' in index
    assert 'id="agentPromptTable"' in index
    assert 'id="agentActionTable"' in index
    assert 'handlePlanningAgentRun' in app_js
    assert 'handleAgentActionApprove' in app_js
