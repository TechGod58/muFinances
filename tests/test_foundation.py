from __future__ import annotations

import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_foundation.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def auth_headers() -> dict[str, str]:
    response = client.post(
        '/api/auth/login',
        json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'},
    )
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def _seeded_scenario_id() -> int:
    scenarios = client.get('/api/scenarios', headers=auth_headers()).json()
    seeded = next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')
    return int(seeded['id'])


def test_foundation_migrations_are_registered() -> None:
    response = client.get('/api/foundation/migrations', headers=auth_headers())
    assert response.status_code == 200
    payload = response.json()
    keys = [item['migration_key'] for item in payload['migrations']]
    assert '0001_foundation_planning_ledger' in keys
    assert '0002_parallel_cubed_finance_genome' in keys


def test_foundation_dimension_hierarchy_upsert() -> None:
    response = client.post(
        '/api/foundation/dimensions',
        headers=auth_headers(),
        json={
            'dimension_kind': 'program',
            'code': 'BIO-BS',
            'name': 'Biology BS',
            'parent_code': 'SCI',
            'metadata': {'degree': 'BS'},
        },
    )
    assert response.status_code == 200
    assert response.json()['parent_code'] == 'SCI'

    hierarchy = client.get('/api/foundation/dimensions/hierarchy', headers=auth_headers())
    assert hierarchy.status_code == 200
    programs = hierarchy.json()['program']
    assert any(item['code'] == 'BIO-BS' and item['metadata']['degree'] == 'BS' for item in programs)


def test_foundation_ledger_post_and_list() -> None:
    scenario_id = _seeded_scenario_id()
    response = client.post(
        '/api/foundation/ledger',
        headers=auth_headers(),
        json={
            'scenario_id': scenario_id,
            'department_code': 'SCI',
            'fund_code': 'GEN',
            'account_code': 'SUPPLIES',
            'period': '2026-08',
            'amount': -2500,
            'notes': 'B01 service-layer posting',
            'source': 'manual',
            'ledger_type': 'planning',
            'program_code': 'BIO-BS',
            'metadata': {'batch': 'B01'},
        },
    )
    assert response.status_code == 200
    entry = response.json()
    assert entry['posted_by'] == 'admin@mufinances.local'
    assert entry['metadata']['batch'] == 'B01'

    ledger = client.get(f'/api/foundation/ledger?scenario_id={scenario_id}', headers=auth_headers())
    assert ledger.status_code == 200
    assert any(item['id'] == entry['id'] for item in ledger.json()['entries'])


def test_ledger_idempotency_and_immutable_audit_controls() -> None:
    scenario_id = _seeded_scenario_id()
    headers = auth_headers()
    payload = {
        'scenario_id': scenario_id,
        'department_code': 'SCI',
        'fund_code': 'GEN',
        'account_code': 'B57_IDEMPOTENT',
        'period': '2026-08',
        'amount': -157,
        'notes': 'B57 idempotent posting',
        'idempotency_key': 'b57-idempotent-ledger-post',
    }

    first = client.post('/api/foundation/ledger', headers=headers, json=payload)
    second = client.post('/api/foundation/ledger', headers=headers, json={**payload, 'amount': -999})

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()['id'] == first.json()['id']
    assert second.json()['amount'] == -157
    assert first.json()['posted_checksum']
    assert first.json()['immutable_posting'] == 1

    audit = client.get('/api/compliance/audit/verify', headers=headers)
    assert audit.status_code == 200
    assert audit.json()['valid'] is True


def test_concurrent_idempotent_ledger_posts_create_one_row() -> None:
    scenario_id = _seeded_scenario_id()
    headers = auth_headers()
    payload = {
        'scenario_id': scenario_id,
        'department_code': 'OPS',
        'fund_code': 'GEN',
        'account_code': 'B57_CONCURRENT',
        'period': '2026-08',
        'amount': -321,
        'notes': 'B57 concurrent idempotent posting',
        'idempotency_key': 'b57-concurrent-ledger-post',
    }

    def post_once() -> dict[str, object]:
        response = client.post('/api/foundation/ledger', headers=headers, json=payload)
        assert response.status_code == 200
        return response.json()

    with ThreadPoolExecutor(max_workers=5) as pool:
        entries = list(pool.map(lambda _: post_once(), range(5)))

    ids = {entry['id'] for entry in entries}
    assert len(ids) == 1


def test_foundation_ledger_reverse_preserves_history() -> None:
    scenario_id = _seeded_scenario_id()
    created = client.post(
        '/api/foundation/ledger',
        headers=auth_headers(),
        json={
            'scenario_id': scenario_id,
            'department_code': 'SCI',
            'fund_code': 'GEN',
            'account_code': 'SUPPLIES',
            'period': '2026-09',
            'amount': -800,
            'notes': 'Temporary line to reverse',
        },
    )
    assert created.status_code == 200
    entry_id = created.json()['id']

    reversed_response = client.post(
        f'/api/foundation/ledger/{entry_id}/reverse',
        headers=auth_headers(),
        json={'reason': 'Replace with corrected line'},
    )
    assert reversed_response.status_code == 200
    assert reversed_response.json()['reversed_at'] is not None

    active = client.get(f'/api/foundation/ledger?scenario_id={scenario_id}', headers=auth_headers())
    assert all(item['id'] != entry_id for item in active.json()['entries'])

    full_history = client.get(f'/api/foundation/ledger?scenario_id={scenario_id}&include_reversed=true', headers=auth_headers())
    assert any(item['id'] == entry_id and item['reversed_at'] for item in full_history.json()['entries'])


def test_foundation_fiscal_period_close_blocks_posting() -> None:
    scenario_id = _seeded_scenario_id()
    period = client.post(
        '/api/foundation/fiscal-periods',
        headers=auth_headers(),
        json={
            'fiscal_year': 'FY27',
            'period': '2026-10',
            'period_index': 4,
            'is_closed': False,
        },
    )
    assert period.status_code == 200

    closed = client.post('/api/foundation/fiscal-periods/2026-10/close', headers=auth_headers())
    assert closed.status_code == 200
    assert closed.json()['is_closed'] is True

    blocked = client.post(
        '/api/foundation/ledger',
        headers=auth_headers(),
        json={
            'scenario_id': scenario_id,
            'department_code': 'SCI',
            'fund_code': 'GEN',
            'account_code': 'SUPPLIES',
            'period': '2026-10',
            'amount': -1200,
            'notes': 'Should be blocked by period close',
        },
    )
    assert blocked.status_code == 409
    assert blocked.json()['detail'] == 'Fiscal period is closed.'

    reopened = client.post('/api/foundation/fiscal-periods/2026-10/reopen', headers=auth_headers())
    assert reopened.status_code == 200
    assert reopened.json()['is_closed'] is False


def test_foundation_backup_create_records_file() -> None:
    response = client.post('/api/foundation/backups', headers=auth_headers(), json={'note': 'B01 test backup'})
    assert response.status_code == 200
    payload = response.json()
    assert payload['backup_key'].startswith('backup-')
    assert payload['size_bytes'] > 0
    assert Path(payload['path']).exists()

    listing = client.get('/api/foundation/backups', headers=auth_headers())
    assert listing.status_code == 200
    assert any(item['backup_key'] == payload['backup_key'] for item in listing.json()['backups'])


def test_foundation_status_reports_b01_complete() -> None:
    response = client.get('/api/foundation/status', headers=auth_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B01'
    assert payload['complete'] is True
    assert payload['checks']['ledger_ready'] is True
    assert payload['checks']['periods_ready'] is True
    assert payload['checks']['ledger_transactions_ready'] is True
    assert payload['checks']['idempotency_keys_ready'] is True
    assert payload['checks']['immutable_posting_ready'] is True
    assert payload['checks']['restore_safeguards_ready'] is True
    assert payload['checks']['audit_chain_enforced'] is True
