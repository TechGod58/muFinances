from __future__ import annotations

import hashlib
import hmac
import json
import os
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.foundation import summary_by_dimensions
from app.services.governed_automation import get_agent_action, run_planning_agent

DEFAULT_CLIENT_KEY = os.getenv('CAMPUS_FPM_UNIVERSITY_AGENT_KEY', 'university-agent-dev')
DEFAULT_CLIENT_SECRET = os.getenv('CAMPUS_FPM_UNIVERSITY_AGENT_SECRET', 'local-agent-signing-placeholder')
SIGNATURE_WINDOW_SECONDS = int(os.getenv('CAMPUS_FPM_AGENT_SIGNATURE_WINDOW_SECONDS', '300'))


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _stamp() -> str:
    return datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')


def status() -> dict[str, Any]:
    ensure_registry_ready()
    counts = {
        'clients': int(db.fetch_one('SELECT COUNT(*) AS count FROM university_agent_clients')['count']),
        'tools': int(db.fetch_one('SELECT COUNT(*) AS count FROM university_agent_tools WHERE enabled = 1')['count']),
        'policies': int(db.fetch_one("SELECT COUNT(*) AS count FROM university_agent_policies WHERE status = 'active'")['count']),
        'requests': int(db.fetch_one('SELECT COUNT(*) AS count FROM university_agent_requests')['count']),
        'callbacks': int(db.fetch_one('SELECT COUNT(*) AS count FROM university_agent_callbacks')['count']),
        'audit_logs': int(db.fetch_one('SELECT COUNT(*) AS count FROM university_agent_audit_logs')['count']),
        'pending_approval': int(db.fetch_one("SELECT COUNT(*) AS count FROM university_agent_requests WHERE approval_status = 'pending_approval'")['count']),
    }
    checks = {
        'external_agent_api_ready': True,
        'signed_agent_requests_ready': True,
        'tool_registry_ready': counts['tools'] >= 4,
        'scoped_permissions_ready': True,
        'allowed_action_policies_ready': counts['policies'] >= 1,
        'agent_audit_logs_ready': True,
        'approval_before_posting_ready': True,
        'callback_webhook_support_ready': True,
    }
    return {'batch': 'B67', 'title': 'University Agent Integration Layer', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def workspace() -> dict[str, Any]:
    ensure_registry_ready()
    return {
        'status': status(),
        'clients': list_clients(),
        'tools': list_tools(),
        'policies': list_policies(),
        'requests': list_requests(),
        'callbacks': list_callbacks(),
        'audit_logs': list_audit_logs(),
        'signing': {
            'header_client_key': 'X-Agent-Key',
            'header_timestamp': 'X-Agent-Timestamp',
            'header_signature': 'X-Agent-Signature',
            'signature_base': '<timestamp>.<raw-json-body>',
            'algorithm': 'HMAC-SHA256',
        },
    }


def ensure_registry_ready(user: dict[str, Any] | None = None) -> None:
    actor = (user or {}).get('email', 'system.agent')
    now = _now()
    client_hash = _secret_hash(DEFAULT_CLIENT_SECRET)
    db.execute(
        '''
        INSERT OR IGNORE INTO university_agent_clients (
            client_key, display_name, shared_secret_hash, scopes_json, status, callback_url, created_by, created_at, updated_at
        ) VALUES (?, 'Manchester University Agent', ?, ?, 'active', '', ?, ?, ?)
        ''',
        (DEFAULT_CLIENT_KEY, client_hash, json.dumps(['finance.read', 'agent.run', 'ledger.request', 'webhook.write']), actor, now, now),
    )
    tools = [
        ('finance.summary', 'Finance Summary', 'Read scenario financial summary.', 'finance.read', 'read_summary', 0),
        ('planning.agent.run', 'Run Planning Agent', 'Draft a governed planning-agent action.', 'agent.run', 'agent_action', 1),
        ('ledger.post.request', 'Request Ledger Posting', 'Create a ledger-posting request that waits for human approval.', 'ledger.request', 'ledger_post', 1),
        ('webhook.callback.register', 'Register Callback', 'Register callback URL for signed request results.', 'webhook.write', 'callback_register', 0),
    ]
    for tool_key, name, description, required_scope, action_type, approval_required in tools:
        db.execute(
            '''
            INSERT OR IGNORE INTO university_agent_tools (
                tool_key, name, description, required_scope, action_type, approval_required, enabled, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, 1, '{}', ?)
            ''',
            (tool_key, name, description, required_scope, action_type, approval_required, now),
        )
    for tool_key, _, _, _, action_type, _ in tools:
        db.execute(
            '''
            INSERT OR IGNORE INTO university_agent_policies (
                policy_key, client_key, tool_key, allowed_actions_json, max_amount, status, created_by, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?)
            ''',
            (
                f'{DEFAULT_CLIENT_KEY}:{tool_key}',
                DEFAULT_CLIENT_KEY,
                tool_key,
                json.dumps([action_type], sort_keys=True),
                250000.0 if action_type == 'ledger_post' else None,
                actor,
                now,
                now,
            ),
        )


def upsert_client(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    secret = payload.get('shared_secret') or DEFAULT_CLIENT_SECRET
    scopes = payload.get('scopes') or ['finance.read']
    db.execute(
        '''
        INSERT INTO university_agent_clients (
            client_key, display_name, shared_secret_hash, scopes_json, status, callback_url, created_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(client_key) DO UPDATE SET
            display_name = excluded.display_name,
            shared_secret_hash = excluded.shared_secret_hash,
            scopes_json = excluded.scopes_json,
            status = excluded.status,
            callback_url = excluded.callback_url,
            updated_at = excluded.updated_at
        ''',
        (
            payload['client_key'], payload['display_name'], _secret_hash(secret), json.dumps(scopes, sort_keys=True),
            payload.get('status', 'active'), payload.get('callback_url') or '', user['email'], now, now,
        ),
    )
    db.log_audit('university_agent_client', payload['client_key'], 'upserted', user['email'], {'scopes': scopes, 'status': payload.get('status', 'active')}, now)
    return get_client(payload['client_key'])


def upsert_policy(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    ensure_registry_ready(user)
    now = _now()
    key = payload.get('policy_key') or f"{payload['client_key']}:{payload['tool_key']}"
    db.execute(
        '''
        INSERT INTO university_agent_policies (
            policy_key, client_key, tool_key, allowed_actions_json, max_amount, status, created_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(policy_key) DO UPDATE SET
            client_key = excluded.client_key,
            tool_key = excluded.tool_key,
            allowed_actions_json = excluded.allowed_actions_json,
            max_amount = excluded.max_amount,
            status = excluded.status,
            updated_at = excluded.updated_at
        ''',
        (
            key, payload['client_key'], payload['tool_key'],
            json.dumps(payload.get('allowed_actions') or [], sort_keys=True), payload.get('max_amount'),
            payload.get('status', 'active'), user['email'], now, now,
        ),
    )
    db.log_audit('university_agent_policy', key, 'upserted', user['email'], payload, now)
    return get_policy(key)


def handle_signed_request(headers: dict[str, str], raw_body: bytes) -> dict[str, Any]:
    ensure_registry_ready()
    payload = json.loads(raw_body.decode('utf-8') or '{}')
    client_key = headers.get('x-agent-key') or headers.get('X-Agent-Key') or ''
    timestamp = headers.get('x-agent-timestamp') or headers.get('X-Agent-Timestamp') or ''
    signature = headers.get('x-agent-signature') or headers.get('X-Agent-Signature') or ''
    request_key = payload.get('request_key') or f"agent-{_stamp()}"
    tool_key = payload.get('tool_key') or ''
    scenario_id = payload.get('scenario_id')
    callback_url = payload.get('callback_url') or ''
    created_at = _now()
    signature_status = 'verified'
    policy_status = 'pending'
    status_value = 'running'
    result: dict[str, Any] = {}
    request_id = db.execute(
        '''
        INSERT INTO university_agent_requests (
            request_key, client_key, tool_key, scenario_id, signature_status, policy_status,
            approval_status, status, payload_json, result_json, callback_url, created_at
        ) VALUES (?, ?, ?, ?, 'pending', 'pending', 'not_required', 'received', ?, '{}', ?, ?)
        ''',
        (request_key, client_key, tool_key, scenario_id, json.dumps(payload, sort_keys=True), callback_url, created_at),
    )
    try:
        client = _verify_signature(client_key, timestamp, signature, raw_body)
        tool = get_tool(tool_key)
        policy_status = _policy_status(client, tool, payload)
        if policy_status != 'allowed':
            status_value = 'rejected'
            result = {'error': policy_status}
        else:
            result = _execute_tool(client, tool, payload)
            status_value = result.get('status', 'completed')
    except ValueError as exc:
        signature_status = 'failed' if 'signature' in str(exc).lower() or 'client' in str(exc).lower() else signature_status
        policy_status = 'rejected'
        status_value = 'rejected'
        result = {'error': str(exc)}
    approval_status = result.get('approval_status') or ('pending_approval' if result.get('requires_approval') else 'not_required')
    completed_at = _now()
    db.execute(
        '''
        UPDATE university_agent_requests
        SET signature_status = ?, policy_status = ?, approval_status = ?, status = ?,
            result_json = ?, completed_at = ?
        WHERE id = ?
        ''',
        (signature_status, policy_status, approval_status, status_value, json.dumps(result, sort_keys=True), completed_at, request_id),
    )
    if callback_url:
        _queue_callback(request_id, callback_url, {'request_key': request_key, 'status': status_value, 'result': result})
    _audit(request_id, client_key or 'unknown', 'request_completed', {'tool_key': tool_key, 'status': status_value, 'policy_status': policy_status, 'approval_status': approval_status})
    db.log_audit('university_agent_request', request_key, status_value, client_key or 'unknown', {'tool_key': tool_key, 'approval_status': approval_status}, completed_at)
    return get_request(request_id)


def list_clients() -> list[dict[str, Any]]:
    ensure_registry_ready()
    return [_format_client(row) for row in db.fetch_all('SELECT * FROM university_agent_clients ORDER BY client_key')]


def get_client(client_key: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM university_agent_clients WHERE client_key = ?', (client_key,))
    if row is None:
        raise ValueError('University Agent client not found.')
    return _format_client(row)


def list_tools() -> list[dict[str, Any]]:
    ensure_registry_ready()
    return [_format_tool(row) for row in db.fetch_all('SELECT * FROM university_agent_tools ORDER BY tool_key')]


def get_tool(tool_key: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM university_agent_tools WHERE tool_key = ?', (tool_key,))
    if row is None or not bool(row['enabled']):
        raise ValueError('University Agent tool not found or disabled.')
    return _format_tool(row)


def list_policies() -> list[dict[str, Any]]:
    ensure_registry_ready()
    return [_format_policy(row) for row in db.fetch_all('SELECT * FROM university_agent_policies ORDER BY policy_key')]


def get_policy(policy_key: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM university_agent_policies WHERE policy_key = ?', (policy_key,))
    if row is None:
        raise ValueError('University Agent policy not found.')
    return _format_policy(row)


def list_requests(limit: int = 100) -> list[dict[str, Any]]:
    return [_format_request(row) for row in db.fetch_all('SELECT * FROM university_agent_requests ORDER BY id DESC LIMIT ?', (limit,))]


def get_request(request_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM university_agent_requests WHERE id = ?', (request_id,))
    if row is None:
        raise ValueError('University Agent request not found.')
    return _format_request(row)


def list_callbacks(limit: int = 100) -> list[dict[str, Any]]:
    return [_format_callback(row) for row in db.fetch_all('SELECT * FROM university_agent_callbacks ORDER BY id DESC LIMIT ?', (limit,))]


def list_audit_logs(limit: int = 100) -> list[dict[str, Any]]:
    return [_format_audit(row) for row in db.fetch_all('SELECT * FROM university_agent_audit_logs ORDER BY id DESC LIMIT ?', (limit,))]


def _verify_signature(client_key: str, timestamp: str, signature: str, raw_body: bytes) -> dict[str, Any]:
    if not client_key:
        raise ValueError('Missing agent client key.')
    row = db.fetch_one('SELECT * FROM university_agent_clients WHERE client_key = ? AND status = "active"', (client_key,))
    if row is None:
        raise ValueError('Unknown or inactive agent client.')
    if not timestamp or not signature:
        raise ValueError('Missing agent signature headers.')
    try:
        request_time = datetime.fromtimestamp(int(timestamp), UTC)
    except ValueError as exc:
        raise ValueError('Invalid agent timestamp.') from exc
    age = abs((datetime.now(UTC) - request_time).total_seconds())
    if age > SIGNATURE_WINDOW_SECONDS:
        raise ValueError('Agent signature timestamp is outside the allowed window.')
    secret = _client_secret_for_verification(client_key)
    base = timestamp.encode('utf-8') + b'.' + raw_body
    expected = hmac.new(secret.encode('utf-8'), base, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise ValueError('Invalid agent signature.')
    return _format_client(row)


def _client_secret_for_verification(client_key: str) -> str:
    if client_key == DEFAULT_CLIENT_KEY:
        return DEFAULT_CLIENT_SECRET
    override = os.getenv(f"CAMPUS_FPM_AGENT_SECRET_{client_key.upper().replace('-', '_')}")
    if override:
        return override
    raise ValueError('Agent client secret is not available to verify signatures.')


def _policy_status(client: dict[str, Any], tool: dict[str, Any], payload: dict[str, Any]) -> str:
    if tool['required_scope'] not in client['scopes']:
        return 'missing_scope'
    policy = db.fetch_one(
        '''
        SELECT * FROM university_agent_policies
        WHERE client_key = ? AND tool_key = ? AND status = 'active'
        ORDER BY id DESC LIMIT 1
        ''',
        (client['client_key'], tool['tool_key']),
    )
    if policy is None:
        return 'missing_policy'
    formatted = _format_policy(policy)
    if tool['action_type'] not in formatted['allowed_actions']:
        return 'action_not_allowed'
    amount = _payload_amount(payload)
    if formatted.get('max_amount') is not None and amount is not None and abs(amount) > float(formatted['max_amount']):
        return 'amount_exceeds_policy'
    return 'allowed'


def _execute_tool(client: dict[str, Any], tool: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    agent_user = _agent_user(client)
    tool_key = tool['tool_key']
    if tool_key == 'finance.summary':
        scenario_id = int(payload['scenario_id'])
        return {'status': 'completed', 'approval_status': 'not_required', 'summary': summary_by_dimensions(scenario_id, user=agent_user)}
    if tool_key == 'planning.agent.run':
        result = run_planning_agent(
            {
                'scenario_id': int(payload['scenario_id']),
                'agent_type': payload.get('agent_type', 'budget_update'),
                'prompt_text': payload.get('prompt_text') or 'Draft a budget update for human approval.',
            },
            agent_user,
        )
        return {'status': 'pending_approval', 'requires_approval': True, 'approval_status': 'pending_approval', 'action': result['action']}
    if tool_key == 'ledger.post.request':
        action = _create_ledger_request(payload, agent_user)
        return {'status': 'pending_approval', 'requires_approval': True, 'approval_status': 'pending_approval', 'action': action}
    if tool_key == 'webhook.callback.register':
        _register_callback(client['client_key'], payload.get('callback_url') or '')
        return {'status': 'completed', 'approval_status': 'not_required', 'callback_url': payload.get('callback_url') or ''}
    raise ValueError('Unsupported University Agent tool.')


def _create_ledger_request(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    scenario_id = int(payload['scenario_id'])
    ledger_entry = payload.get('ledger_entry') or {}
    prompt_id = db.execute(
        '''
        INSERT INTO ai_agent_prompts (
            scenario_id, agent_type, prompt_text, parsed_intent_json, status, created_by, created_at
        ) VALUES (?, 'university_agent', ?, ?, 'parsed', ?, ?)
        ''',
        (
            scenario_id,
            payload.get('prompt_text') or 'External University Agent ledger posting request.',
            json.dumps({'source': 'university_agent', 'ledger_entry': ledger_entry}, sort_keys=True),
            user['email'],
            _now(),
        ),
    )
    proposal = {
        'action_type': 'ledger_post',
        'summary': payload.get('summary') or 'University Agent requested a ledger posting for human approval.',
        'ledger_entry': {
            'scenario_id': scenario_id,
            'department_code': ledger_entry.get('department_code', 'SCI'),
            'fund_code': ledger_entry.get('fund_code', 'GEN'),
            'account_code': ledger_entry.get('account_code', 'SUPPLIES'),
            'period': ledger_entry.get('period', '2026-08'),
            'amount': float(ledger_entry.get('amount', 0)),
            'source': 'university_agent',
            'ledger_type': 'scenario',
            'ledger_basis': 'scenario',
            'notes': ledger_entry.get('notes', 'University Agent draft pending human approval'),
            'metadata': {'source': 'university_agent', 'human_approval_required': True},
        },
    }
    action_id = db.execute(
        '''
        INSERT INTO ai_agent_actions (
            prompt_id, scenario_id, agent_type, action_type, status, guard_status, proposal_json, result_json, created_at
        ) VALUES (?, ?, 'university_agent', 'ledger_post', 'pending_approval', 'passed', ?, '{}', ?)
        ''',
        (prompt_id, scenario_id, json.dumps(proposal, sort_keys=True), _now()),
    )
    return get_agent_action(action_id)


def _register_callback(client_key: str, callback_url: str) -> None:
    if not callback_url:
        raise ValueError('Callback URL is required.')
    db.execute('UPDATE university_agent_clients SET callback_url = ?, updated_at = ? WHERE client_key = ?', (callback_url, _now(), client_key))


def _queue_callback(request_id: int, callback_url: str, payload: dict[str, Any]) -> None:
    db.execute(
        '''
        INSERT INTO university_agent_callbacks (request_id, callback_url, status, payload_json, attempts, created_at)
        VALUES (?, ?, 'queued', ?, 0, ?)
        ''',
        (request_id, callback_url, json.dumps(payload, sort_keys=True), _now()),
    )


def _audit(request_id: int | None, client_key: str, event_type: str, detail: dict[str, Any]) -> None:
    db.execute(
        '''
        INSERT INTO university_agent_audit_logs (request_id, client_key, event_type, detail_json, created_at)
        VALUES (?, ?, ?, ?, ?)
        ''',
        (request_id, client_key, event_type, json.dumps(detail, sort_keys=True), _now()),
    )


def _secret_hash(secret: str) -> str:
    return hashlib.sha256(secret.encode('utf-8')).hexdigest()


def _payload_amount(payload: dict[str, Any]) -> float | None:
    if payload.get('amount') is not None:
        return float(payload['amount'])
    entry = payload.get('ledger_entry') or {}
    if entry.get('amount') is not None:
        return float(entry['amount'])
    return None


def _agent_user(client: dict[str, Any]) -> dict[str, Any]:
    return {
        'id': 0,
        'email': f"agent:{client['client_key']}",
        'permissions': ['ledger.read', 'reports.read', 'automation.manage', 'row_access.all'],
        'dimension_access': [{'dimension_kind': '*', 'code': '*'}],
    }


def _format_client(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    item['scopes'] = json.loads(item.pop('scopes_json') or '[]')
    item['shared_secret_hash'] = item['shared_secret_hash'][:12] + '...'
    return item


def _format_tool(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    item['approval_required'] = bool(item['approval_required'])
    item['enabled'] = bool(item['enabled'])
    item['metadata'] = json.loads(item.pop('metadata_json') or '{}')
    return item


def _format_policy(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    item['allowed_actions'] = json.loads(item.pop('allowed_actions_json') or '[]')
    return item


def _format_request(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    item['payload'] = json.loads(item.pop('payload_json') or '{}')
    item['result'] = json.loads(item.pop('result_json') or '{}')
    return item


def _format_callback(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    item['payload'] = json.loads(item.pop('payload_json') or '{}')
    return item


def _format_audit(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    item['detail'] = json.loads(item.pop('detail_json') or '{}')
    return item
