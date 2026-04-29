from __future__ import annotations

import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_financial_transaction_expansion.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app import db
from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def scenario_id(headers: dict[str, str]) -> int:
    scenarios = client.get('/api/scenarios', headers=headers)
    assert scenarios.status_code == 200
    return int(next(item for item in scenarios.json() if item['name'] == 'FY27 Operating Plan')['id'])


def post_ledger(headers: dict[str, str], sid: int, **overrides: object) -> dict[str, object]:
    payload = {
        'scenario_id': sid,
        'entity_code': 'CAMPUS',
        'department_code': overrides.get('department_code', 'SCI'),
        'fund_code': overrides.get('fund_code', 'GEN'),
        'account_code': overrides.get('account_code', 'B115_TXN'),
        'period': overrides.get('period', '2026-11'),
        'amount': overrides.get('amount', -100),
        'source': overrides.get('source', 'b115_test'),
        'ledger_type': overrides.get('ledger_type', 'budget'),
        'ledger_basis': overrides.get('ledger_basis', 'budget'),
        'notes': overrides.get('notes', 'B115 transaction expansion'),
    }
    if overrides.get('idempotency_key'):
        payload['idempotency_key'] = overrides['idempotency_key']
    response = client.post('/api/foundation/ledger', headers=headers, json=payload)
    assert response.status_code == 200, response.text
    return response.json()


def audit_count(entity_type: str, entity_id: str, action: str) -> int:
    row = db.fetch_one(
        'SELECT COUNT(*) AS count FROM audit_logs WHERE entity_type = ? AND entity_id = ? AND action = ?',
        (entity_type, entity_id, action),
    )
    return int(row['count'])


def test_ledger_post_reverse_and_concurrent_idempotency_are_transactional() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    key = 'b115-ledger-concurrent-idempotency'
    payload = {
        'scenario_id': sid,
        'entity_code': 'CAMPUS',
        'department_code': 'SCI',
        'fund_code': 'GEN',
        'account_code': 'B115_CONCURRENT',
        'period': '2026-11',
        'amount': -115,
        'source': 'b115_test',
        'ledger_type': 'budget',
        'ledger_basis': 'budget',
        'idempotency_key': key,
    }

    def post_once() -> dict[str, object]:
        response = client.post('/api/foundation/ledger', headers=headers, json=payload)
        assert response.status_code == 200, response.text
        return response.json()

    with ThreadPoolExecutor(max_workers=6) as pool:
        entries = list(pool.map(lambda _: post_once(), range(6)))

    entry_ids = {entry['id'] for entry in entries}
    assert len(entry_ids) == 1
    entry_id = int(next(iter(entry_ids)))
    assert audit_count('planning_ledger', str(entry_id), 'posted') == 1

    reversed_response = client.post(
        f'/api/foundation/ledger/{entry_id}/reverse',
        headers=headers,
        json={'reason': 'B115 reversal control'},
    )
    assert reversed_response.status_code == 200
    assert reversed_response.json()['reversed_at'] is not None

    second_reverse = client.post(
        f'/api/foundation/ledger/{entry_id}/reverse',
        headers=headers,
        json={'reason': 'Duplicate reversal should fail'},
    )
    assert second_reverse.status_code == 409
    assert audit_count('planning_ledger', str(entry_id), 'reversed') == 1


def test_budget_submission_approval_and_transfer_approval_do_not_duplicate_financial_posts() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    submission = client.post(
        '/api/operating-budget/submissions',
        headers=headers,
        json={'scenario_id': sid, 'department_code': 'B115BDG', 'owner': 'Budget Office', 'notes': 'B115 budget workflow'},
    )
    assert submission.status_code == 200, submission.text
    submission_id = int(submission.json()['id'])

    line = client.post(
        f'/api/operating-budget/submissions/{submission_id}/lines',
        headers=headers,
        json={'fund_code': 'GEN', 'account_code': 'SUPPLIES', 'period': '2026-11', 'amount': -4500, 'line_type': 'expense', 'recurrence': 'one_time'},
    )
    assert line.status_code == 200, line.text

    submitted = client.post(f'/api/operating-budget/submissions/{submission_id}/submit', headers=headers)
    assert submitted.status_code == 200
    approved = client.post(f'/api/operating-budget/submissions/{submission_id}/approve', headers=headers, json={'note': 'B115 approval'})
    assert approved.status_code == 200
    assert approved.json()['status'] == 'approved'

    blocked = client.post(
        f'/api/operating-budget/submissions/{submission_id}/lines',
        headers=headers,
        json={'fund_code': 'GEN', 'account_code': 'SUPPLIES', 'period': '2026-11', 'amount': -1, 'line_type': 'adjustment', 'recurrence': 'one_time'},
    )
    assert blocked.status_code == 409

    transfer = client.post(
        '/api/operating-budget/transfers',
        headers=headers,
        json={
            'scenario_id': sid,
            'from_department_code': 'B115BDG',
            'to_department_code': 'OPS',
            'fund_code': 'GEN',
            'account_code': 'SUPPLIES',
            'period': '2026-11',
            'amount': 300,
            'reason': 'B115 transfer approval test',
        },
    )
    assert transfer.status_code == 200, transfer.text
    transfer_id = int(transfer.json()['id'])

    first_approval = client.post(f'/api/operating-budget/transfers/{transfer_id}/approve', headers=headers)
    second_approval = client.post(f'/api/operating-budget/transfers/{transfer_id}/approve', headers=headers)
    assert first_approval.status_code == 200
    assert second_approval.status_code == 200
    assert first_approval.json()['from_ledger_entry_id'] == second_approval.json()['from_ledger_entry_id']
    assert first_approval.json()['to_ledger_entry_id'] == second_approval.json()['to_ledger_entry_id']


def test_journal_import_and_allocation_mutations_leave_audited_posted_records() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    period = '2026-11'

    journal = client.post(
        '/api/ledger-depth/journals',
        headers=headers,
        json={
            'scenario_id': sid,
            'period': period,
            'department_code': 'SCI',
            'fund_code': 'GEN',
            'account_code': 'B115_JOURNAL',
            'amount': -875,
            'ledger_basis': 'budget',
            'reason': 'B115 journal transaction test',
        },
    )
    assert journal.status_code == 200, journal.text
    journal_id = int(journal.json()['id'])
    approved = client.post(f'/api/ledger-depth/journals/{journal_id}/approve', headers=headers)
    assert approved.status_code == 200
    assert approved.json()['status'] == 'posted'
    assert approved.json()['ledger_entry_id'] is not None
    duplicate_approval = client.post(f'/api/ledger-depth/journals/{journal_id}/approve', headers=headers)
    assert duplicate_approval.status_code == 409

    connector = client.post(
        '/api/integrations/connectors',
        headers=headers,
        json={'connector_key': 'b115-erp', 'name': 'B115 ERP', 'system_type': 'erp', 'direction': 'inbound', 'config': {'mode': 'test'}},
    )
    assert connector.status_code == 200
    import_payload = {
        'scenario_id': sid,
        'connector_key': 'b115-erp',
        'source_format': 'csv',
        'import_type': 'ledger',
        'rows': [
            {'source_record_id': 'row-1', 'department_code': 'B115OPS', 'fund_code': 'GEN', 'account_code': 'B115_ALLOC', 'period': period, 'amount': -1000},
            {'source_record_id': 'row-bad', 'department_code': 'B115OPS', 'fund_code': 'GEN', 'account_code': '', 'period': period, 'amount': 'bad'},
        ],
    }
    first_import = client.post('/api/integrations/imports', headers=headers, json=import_payload)
    second_import = client.post('/api/integrations/imports', headers=headers, json=import_payload)
    assert first_import.status_code == 200, first_import.text
    assert second_import.status_code == 200, second_import.text
    assert first_import.json()['accepted_rows'] == 1
    assert first_import.json()['rejected_rows'] == 1
    assert second_import.json()['accepted_rows'] == 1
    imported_rows = db.fetch_all(
        'SELECT id FROM planning_ledger WHERE scenario_id = ? AND idempotency_key = ?',
        (sid, 'import:b115-erp:row-1'),
    )
    assert len(imported_rows) == 1

    post_ledger(headers, sid, department_code='B115SCI', account_code='TUITION', amount=4000, period=period, ledger_type='actual', ledger_basis='actual')
    pool = client.post(
        '/api/profitability/cost-pools',
        headers=headers,
        json={
            'scenario_id': sid,
            'pool_key': 'b115-service',
            'name': 'B115 service center',
            'source_department_code': 'B115OPS',
            'source_account_code': 'B115_ALLOC',
            'allocation_basis': 'equal',
            'target_type': 'department',
            'target_codes': ['B115SCI', 'OPS'],
        },
    )
    assert pool.status_code == 200, pool.text
    allocation = client.post('/api/profitability/allocation-runs', headers=headers, json={'scenario_id': sid, 'period': period, 'pool_keys': ['b115-service']})
    assert allocation.status_code == 200, allocation.text
    assert allocation.json()['status'] == 'posted'
    assert allocation.json()['total_source_cost'] == 1000
    assert len(allocation.json()['trace_lines']) == 2


def test_close_reconciliation_consolidation_and_period_lock_enforcement_are_audited() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    period = '2026-12'
    ledger = post_ledger(headers, sid, department_code='SCI', account_code='B115_CLOSE', amount=2500, period=period, ledger_type='actual', ledger_basis='actual')

    reconciliation = client.post(
        '/api/close/reconciliations',
        headers=headers,
        json={
            'scenario_id': sid,
            'period': period,
            'entity_code': 'CAMPUS',
            'account_code': 'B115_CLOSE',
            'source_balance': 2400,
            'owner': 'Controller',
            'notes': 'B115 reconciliation variance',
        },
    )
    assert reconciliation.status_code == 200, reconciliation.text
    assert reconciliation.json()['status'] == 'exception'
    assert reconciliation.json()['variance'] == 100

    consolidation = client.post('/api/close/consolidation-runs', headers=headers, json={'scenario_id': sid, 'period': period})
    assert consolidation.status_code == 200, consolidation.text
    assert consolidation.json()['status'] == 'complete'
    assert consolidation.json()['audit_packet']['status'] == 'sealed'

    locked = client.post(f'/api/close/calendar/{period}/lock?scenario_id={sid}', headers=headers, json={'lock_state': 'locked'})
    assert locked.status_code == 200
    assert locked.json()['lock_state'] == 'locked'

    blocked_consolidation = client.post('/api/close/consolidation-runs', headers=headers, json={'scenario_id': sid, 'period': period})
    assert blocked_consolidation.status_code == 409
    assert f'Period {period} is locked' in blocked_consolidation.json()['detail']
    assert audit_count('planning_ledger', str(ledger['id']), 'posted') == 1
