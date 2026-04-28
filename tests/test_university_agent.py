from __future__ import annotations

import hmac
import hashlib
import json
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_university_agent.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)
AGENT_KEY = 'university-agent-dev'
AGENT_SECRET = 'local-agent-signing-placeholder'


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def scenario_id(headers: dict[str, str]) -> int:
    scenarios = client.get('/api/scenarios', headers=headers).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def signed_headers(body: bytes, secret: str = AGENT_SECRET) -> dict[str, str]:
    timestamp = str(int(time.time()))
    signature = hmac.new(secret.encode('utf-8'), timestamp.encode('utf-8') + b'.' + body, hashlib.sha256).hexdigest()
    return {
        'X-Agent-Key': AGENT_KEY,
        'X-Agent-Timestamp': timestamp,
        'X-Agent-Signature': signature,
        'Content-Type': 'application/json',
    }


def post_signed(payload: dict[str, object], secret: str = AGENT_SECRET):
    body = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return client.post('/api/university-agent/requests', content=body, headers=signed_headers(body, secret))


def test_university_agent_status_registry_and_signed_summary_request() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/university-agent/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B67'
    assert payload['complete'] is True
    assert payload['checks']['signed_agent_requests_ready'] is True

    tools = client.get('/api/university-agent/tools', headers=headers)
    assert tools.status_code == 200
    tool_keys = {tool['tool_key'] for tool in tools.json()['tools']}
    assert {'finance.summary', 'planning.agent.run', 'ledger.post.request', 'webhook.callback.register'} <= tool_keys

    signed = post_signed({'request_key': 'b67-summary', 'tool_key': 'finance.summary', 'scenario_id': sid})
    assert signed.status_code == 200
    result = signed.json()
    assert result['signature_status'] == 'verified'
    assert result['policy_status'] == 'allowed'
    assert result['approval_status'] == 'not_required'
    assert result['result']['summary']['scenario_id'] == sid


def test_university_agent_ledger_post_requires_human_approval_before_posting_and_callback() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    before = client.get(f'/api/foundation/ledger?scenario_id={sid}', headers=headers)
    before_count = len(before.json()['entries'])

    request = post_signed(
        {
            'request_key': 'b67-ledger-request',
            'tool_key': 'ledger.post.request',
            'scenario_id': sid,
            'callback_url': 'https://agent.manchester.edu/hooks/mufinances',
            'ledger_entry': {
                'department_code': 'SCI',
                'fund_code': 'GEN',
                'account_code': 'SUPPLIES',
                'period': '2026-08',
                'amount': 3210,
                'notes': 'University Agent draft',
            },
        }
    )
    assert request.status_code == 200
    payload = request.json()
    assert payload['status'] == 'pending_approval'
    assert payload['approval_status'] == 'pending_approval'
    action_id = payload['result']['action']['id']

    pending = client.get(f'/api/foundation/ledger?scenario_id={sid}', headers=headers)
    assert len(pending.json()['entries']) == before_count

    callbacks = client.get('/api/university-agent/callbacks', headers=headers)
    assert callbacks.status_code == 200
    assert callbacks.json()['callbacks'][0]['callback_url'].startswith('https://agent.manchester.edu')

    approved = client.post(
        f'/api/automation/planning-agents/actions/{action_id}/approve',
        headers=headers,
        json={'note': 'Approved University Agent request.'},
    )
    assert approved.status_code == 200
    assert approved.json()['status'] == 'posted'

    after = client.get(f'/api/foundation/ledger?scenario_id={sid}', headers=headers)
    assert len(after.json()['entries']) == before_count + 1

    audit = client.get('/api/university-agent/audit-logs', headers=headers)
    assert audit.status_code == 200
    assert audit.json()['audit_logs'][0]['event_type'] == 'request_completed'


def test_university_agent_rejects_bad_signature_and_policy_amount_limit() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    bad = post_signed({'request_key': 'b67-bad-signature', 'tool_key': 'finance.summary', 'scenario_id': sid}, secret='wrong-secret')
    assert bad.status_code == 200
    assert bad.json()['signature_status'] == 'failed'
    assert bad.json()['status'] == 'rejected'

    too_large = post_signed(
        {
            'request_key': 'b67-too-large',
            'tool_key': 'ledger.post.request',
            'scenario_id': sid,
            'ledger_entry': {'amount': 9999999, 'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': 'SUPPLIES', 'period': '2026-08'},
        }
    )
    assert too_large.status_code == 200
    assert too_large.json()['policy_status'] == 'amount_exceeds_policy'
    assert too_large.json()['status'] == 'rejected'


def test_university_agent_migration_and_ui_surface() -> None:
    headers = admin_headers()
    migrations = client.get('/api/foundation/migrations', headers=headers)
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0067_university_agent_integration_layer' in keys

    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')
    assert 'id="refreshUniversityAgentButton"' in index
    assert 'id="universityAgentToolTable"' in index
    assert 'id="universityAgentRequestTable"' in index
    assert '/api/university-agent/workspace' in app_js
    assert 'handleUniversityAgentRefresh' in app_js
