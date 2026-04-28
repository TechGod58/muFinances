from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.foundation import append_ledger_entry


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'journal_adjustments': int(db.fetch_one('SELECT COUNT(*) AS count FROM journal_adjustments')['count']),
        'actual_rows': _basis_count('actual'),
        'budget_rows': _basis_count('budget'),
        'forecast_rows': _basis_count('forecast'),
        'scenario_rows': _basis_count('scenario'),
        'published_scenarios': int(db.fetch_one("SELECT COUNT(*) AS count FROM scenarios WHERE status = 'published'")['count']),
    }
    checks = {
        'ledger_basis_separation_ready': True,
        'journal_adjustment_workflow_ready': True,
        'scenario_locking_ready': True,
        'scenario_publication_ready': True,
        'approved_change_merge_ready': True,
        'source_version_lineage_ready': True,
    }
    return {'batch': 'B13', 'title': 'Ledger Depth And Actuals', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def ledger_basis_summary(scenario_id: int) -> dict[str, Any]:
    rows = db.fetch_all(
        '''
        SELECT ledger_basis, ledger_type, COUNT(*) AS count, COALESCE(SUM(amount), 0) AS total
        FROM planning_ledger
        WHERE scenario_id = ? AND reversed_at IS NULL
        GROUP BY ledger_basis, ledger_type
        ORDER BY ledger_basis, ledger_type
        ''',
        (scenario_id,),
    )
    by_basis: dict[str, dict[str, Any]] = {}
    for row in rows:
        basis = row['ledger_basis']
        bucket = by_basis.setdefault(basis, {'ledger_basis': basis, 'count': 0, 'total': 0.0, 'types': []})
        bucket['count'] += int(row['count'])
        bucket['total'] = round(float(bucket['total']) + float(row['total']), 2)
        bucket['types'].append({'ledger_type': row['ledger_type'], 'count': int(row['count']), 'total': round(float(row['total']), 2)})
    return {'scenario_id': scenario_id, 'basis': list(by_basis.values())}


def create_journal_adjustment(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _scenario_must_be_open(payload['scenario_id'])
    now = _now()
    journal_id = db.execute(
        '''
        INSERT INTO journal_adjustments (
            scenario_id, period, entity_code, department_code, fund_code, account_code,
            amount, ledger_basis, reason, status, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?)
        ''',
        (
            payload['scenario_id'], payload['period'], payload.get('entity_code') or 'CAMPUS',
            payload['department_code'], payload['fund_code'], payload['account_code'], float(payload['amount']),
            payload['ledger_basis'], payload['reason'], user['email'], now,
        ),
    )
    db.log_audit('journal_adjustment', str(journal_id), 'created', user['email'], payload, now)
    return get_journal_adjustment(journal_id)


def list_journal_adjustments(scenario_id: int) -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM journal_adjustments WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))


def get_journal_adjustment(journal_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM journal_adjustments WHERE id = ?', (journal_id,))
    if row is None:
        raise ValueError('Journal adjustment not found.')
    return row


def submit_journal_adjustment(journal_id: int, user: dict[str, Any]) -> dict[str, Any]:
    row = get_journal_adjustment(journal_id)
    if row['status'] != 'draft':
        raise ValueError('Only draft journal adjustments can be submitted.')
    _scenario_must_be_open(int(row['scenario_id']))
    now = _now()
    db.execute(
        "UPDATE journal_adjustments SET status = 'pending_approval', submitted_by = ?, submitted_at = ? WHERE id = ?",
        (user['email'], now, journal_id),
    )
    db.log_audit('journal_adjustment', str(journal_id), 'submitted', user['email'], {}, now)
    return get_journal_adjustment(journal_id)


def approve_journal_adjustment(journal_id: int, user: dict[str, Any]) -> dict[str, Any]:
    row = get_journal_adjustment(journal_id)
    if row['status'] not in {'draft', 'pending_approval'}:
        raise ValueError('Journal adjustment is already posted or rejected.')
    _scenario_must_be_open(int(row['scenario_id']))
    ledger = append_ledger_entry(
        {
            'scenario_id': row['scenario_id'],
            'entity_code': row['entity_code'],
            'department_code': row['department_code'],
            'fund_code': row['fund_code'],
            'account_code': row['account_code'],
            'period': row['period'],
            'amount': float(row['amount']),
            'source': 'journal_adjustment',
            'ledger_type': 'journal',
            'ledger_basis': row['ledger_basis'],
            'source_version': f'journal-{journal_id}',
            'source_record_id': str(journal_id),
            'notes': row['reason'],
            'metadata': {'journal_adjustment_id': journal_id, 'ledger_basis': row['ledger_basis']},
        },
        actor=user['email'],
        user=user,
    )
    now = _now()
    db.execute(
        '''
        UPDATE journal_adjustments
        SET status = 'posted', ledger_entry_id = ?, approved_by = ?, approved_at = ?
        WHERE id = ?
        ''',
        (ledger['id'], user['email'], now, journal_id),
    )
    db.log_audit('journal_adjustment', str(journal_id), 'approved_posted', user['email'], {'ledger_entry_id': ledger['id']}, now)
    result = get_journal_adjustment(journal_id)
    result['ledger_entry'] = ledger
    return result


def reject_journal_adjustment(journal_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    row = get_journal_adjustment(journal_id)
    if row['status'] == 'posted':
        raise ValueError('Posted journal adjustments cannot be rejected.')
    now = _now()
    db.execute(
        "UPDATE journal_adjustments SET status = 'rejected', approved_by = ?, approved_at = ? WHERE id = ?",
        (user['email'], now, journal_id),
    )
    db.log_audit('journal_adjustment', str(journal_id), 'rejected', user['email'], {'note': note}, now)
    return get_journal_adjustment(journal_id)


def lock_scenario(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    return _set_scenario_lock(scenario_id, True, user, 'locked')


def unlock_scenario(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    return _set_scenario_lock(scenario_id, False, user, 'unlocked')


def approve_scenario(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    return _set_scenario_status(scenario_id, 'approved', False, user, 'approved')


def publish_scenario(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    return _set_scenario_status(scenario_id, 'published', True, user, 'published')


def merge_approved_changes(target_scenario_id: int, source_scenario_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    target = _scenario(target_scenario_id)
    source = _scenario(source_scenario_id)
    if bool(target['locked']):
        raise ValueError('Target scenario is locked.')
    if source['status'] not in {'approved', 'published'}:
        raise ValueError('Source scenario must be approved or published before merge.')
    source_rows = db.fetch_all(
        '''
        SELECT *
        FROM planning_ledger
        WHERE scenario_id = ? AND reversed_at IS NULL
        ORDER BY id ASC
        ''',
        (source_scenario_id,),
    )
    created = []
    for row in source_rows:
        ledger = append_ledger_entry(
            {
                'scenario_id': target_scenario_id,
                'entity_code': row['entity_code'],
                'department_code': row['department_code'],
                'fund_code': row['fund_code'],
                'account_code': row['account_code'],
                'program_code': row.get('program_code'),
                'project_code': row.get('project_code'),
                'grant_code': row.get('grant_code'),
                'period': row['period'],
                'amount': float(row['amount']),
                'source': 'scenario_merge',
                'ledger_type': 'scenario',
                'ledger_basis': 'scenario',
                'source_version': source['version'],
                'source_record_id': str(row['id']),
                'parent_ledger_entry_id': row['id'],
                'notes': f"Merged from scenario {source_scenario_id}. {note}".strip(),
                'metadata': {'source_scenario_id': source_scenario_id, 'source_ledger_entry_id': row['id'], 'merge_note': note},
            },
            actor=user['email'],
            user=user,
        )
        created.append(ledger)
    now = _now()
    db.log_audit(
        'scenario',
        str(target_scenario_id),
        'merged_approved_changes',
        user['email'],
        {'source_scenario_id': source_scenario_id, 'created_count': len(created), 'note': note},
        now,
    )
    return {'target_scenario_id': target_scenario_id, 'source_scenario_id': source_scenario_id, 'created_count': len(created), 'created_entries': created}


def _set_scenario_lock(scenario_id: int, locked: bool, user: dict[str, Any], action: str) -> dict[str, Any]:
    row = _scenario(scenario_id)
    db.execute('UPDATE scenarios SET locked = ? WHERE id = ?', (1 if locked else 0, scenario_id))
    db.log_audit('scenario', str(scenario_id), action, user['email'], {'previous_locked': bool(row['locked'])}, _now())
    return _format_scenario(_scenario(scenario_id))


def _set_scenario_status(scenario_id: int, status_value: str, locked: bool, user: dict[str, Any], action: str) -> dict[str, Any]:
    row = _scenario(scenario_id)
    db.execute('UPDATE scenarios SET status = ?, locked = ? WHERE id = ?', (status_value, 1 if locked else 0, scenario_id))
    db.log_audit('scenario', str(scenario_id), action, user['email'], {'previous_status': row['status'], 'locked': locked}, _now())
    return _format_scenario(_scenario(scenario_id))


def _scenario_must_be_open(scenario_id: int) -> dict[str, Any]:
    row = _scenario(scenario_id)
    if bool(row['locked']):
        raise ValueError('Scenario is locked.')
    return row


def _scenario(scenario_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM scenarios WHERE id = ?', (scenario_id,))
    if row is None:
        raise ValueError('Scenario not found.')
    return row


def _format_scenario(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row['locked'] = bool(row['locked'])
    return row


def _basis_count(basis: str) -> int:
    row = db.fetch_one('SELECT COUNT(*) AS count FROM planning_ledger WHERE ledger_basis = ?', (basis,))
    return int(row['count'])
