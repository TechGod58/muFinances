from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app import db
from app.contracts.financial import BudgetSubmissionContract, OperatingBudgetLineContract
from app.services.foundation import append_ledger_entry
from app.services.security import allowed_codes


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _check_department(user: dict[str, Any], department_code: str) -> None:
    codes = allowed_codes(user, 'department')
    if codes is not None and department_code not in codes:
        raise PermissionError('Department access denied.')


def status() -> dict[str, Any]:
    counts = {
        'submissions': int(db.fetch_one('SELECT COUNT(*) AS count FROM budget_submissions')['count']),
        'assumptions': int(db.fetch_one('SELECT COUNT(*) AS count FROM budget_assumptions')['count']),
        'lines': int(db.fetch_one('SELECT COUNT(*) AS count FROM operating_budget_lines')['count']),
        'transfers': int(db.fetch_one('SELECT COUNT(*) AS count FROM budget_transfers')['count']),
    }
    checks = {
        'submissions_ready': True,
        'assumptions_ready': True,
        'line_posting_ready': True,
        'transfers_ready': True,
        'approvals_ready': True,
    }
    return {'batch': 'B03', 'title': 'Operating Budget Workspace', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def create_submission(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    payload = BudgetSubmissionContract.model_validate(payload).model_dump()
    _check_department(user, payload['department_code'])
    now = _now()
    submission_id = db.execute(
        '''
        INSERT INTO budget_submissions (
            scenario_id, department_code, status, owner, notes, created_at, updated_at
        ) VALUES (?, ?, 'draft', ?, ?, ?, ?)
        ON CONFLICT(scenario_id, department_code) DO UPDATE SET
            owner = excluded.owner,
            notes = excluded.notes,
            updated_at = excluded.updated_at
        ''',
        (payload['scenario_id'], payload['department_code'], payload['owner'], payload.get('notes') or '', now, now),
    )
    row = db.fetch_one(
        'SELECT id FROM budget_submissions WHERE scenario_id = ? AND department_code = ?',
        (payload['scenario_id'], payload['department_code']),
    )
    actual_id = int(row['id'] if row else submission_id)
    db.log_audit('budget_submission', str(actual_id), 'upserted', user['email'], payload, now)
    return get_submission(actual_id, user)


def list_submissions(scenario_id: int, user: dict[str, Any]) -> list[dict[str, Any]]:
    codes = allowed_codes(user, 'department')
    if codes is not None and not codes:
        return []
    params: list[Any] = [scenario_id]
    where = 'scenario_id = ?'
    if codes is not None:
        placeholders = ','.join('?' for _ in codes)
        where += f' AND department_code IN ({placeholders})'
        params.extend(sorted(codes))
    rows = db.fetch_all(
        f'SELECT * FROM budget_submissions WHERE {where} ORDER BY department_code',
        tuple(params),
    )
    return [with_submission_counts(row) for row in rows]


def get_submission(submission_id: int, user: dict[str, Any]) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM budget_submissions WHERE id = ?', (submission_id,))
    if row is None:
        raise ValueError('Budget submission not found.')
    _check_department(user, row['department_code'])
    return with_submission_counts(row)


def submit_submission(submission_id: int, user: dict[str, Any]) -> dict[str, Any]:
    row = get_submission(submission_id, user)
    now = _now()
    db.execute(
        "UPDATE budget_submissions SET status = 'submitted', submitted_at = ?, updated_at = ? WHERE id = ?",
        (now, now, submission_id),
    )
    db.log_audit('budget_submission', str(submission_id), 'submitted', user['email'], {}, now)
    return get_submission(submission_id, user)


def approve_submission(submission_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    row = get_submission(submission_id, user)
    now = _now()
    db.execute(
        '''
        UPDATE budget_submissions
        SET status = 'approved', approved_at = ?, approved_by = ?, updated_at = ?
        WHERE id = ?
        ''',
        (now, user['email'], now, submission_id),
    )
    db.log_audit('budget_submission', str(submission_id), 'approved', user['email'], {'note': note, 'department_code': row['department_code']}, now)
    return get_submission(submission_id, user)


def reject_submission(submission_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    get_submission(submission_id, user)
    now = _now()
    db.execute(
        "UPDATE budget_submissions SET status = 'rejected', updated_at = ? WHERE id = ?",
        (now, submission_id),
    )
    db.log_audit('budget_submission', str(submission_id), 'rejected', user['email'], {'note': note}, now)
    return get_submission(submission_id, user)


def add_budget_line(submission_id: int, payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    payload = OperatingBudgetLineContract.model_validate(payload).model_dump()
    submission = get_submission(submission_id, user)
    if submission['status'] == 'approved':
        raise ValueError('Approved submissions cannot be edited.')
    ledger = append_ledger_entry(
        {
            'scenario_id': submission['scenario_id'],
            'department_code': submission['department_code'],
            'fund_code': payload['fund_code'],
            'account_code': payload['account_code'],
            'period': payload['period'],
            'amount': payload['amount'],
            'notes': payload.get('notes') or '',
            'source': 'operating_budget',
            'ledger_type': 'budget',
            'metadata': {
                'submission_id': submission_id,
                'line_type': payload['line_type'],
                'recurrence': payload['recurrence'],
            },
        },
        actor=user['email'],
        user=user,
    )
    line_id = db.execute(
        '''
        INSERT INTO operating_budget_lines (submission_id, ledger_entry_id, line_type, recurrence, created_at)
        VALUES (?, ?, ?, ?, ?)
        ''',
        (submission_id, ledger['id'], payload['line_type'], payload['recurrence'], _now()),
    )
    db.log_audit('operating_budget_line', str(line_id), 'created', user['email'], payload, _now())
    return {'id': line_id, 'submission_id': submission_id, 'ledger_entry': ledger, 'line_type': payload['line_type'], 'recurrence': payload['recurrence']}


def list_assumptions(scenario_id: int, user: dict[str, Any]) -> list[dict[str, Any]]:
    codes = allowed_codes(user, 'department')
    params: list[Any] = [scenario_id]
    where = 'scenario_id = ?'
    if codes is not None:
        if not codes:
            where += ' AND department_code IS NULL'
        else:
            placeholders = ','.join('?' for _ in codes)
            where += f' AND (department_code IS NULL OR department_code IN ({placeholders}))'
            params.extend(sorted(codes))
    return db.fetch_all(f'SELECT * FROM budget_assumptions WHERE {where} ORDER BY department_code, assumption_key', tuple(params))


def create_assumption(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    if payload.get('department_code'):
        _check_department(user, payload['department_code'])
    now = _now()
    db.execute(
        '''
        INSERT INTO budget_assumptions (
            scenario_id, department_code, assumption_key, label, value, unit, notes, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, department_code, assumption_key) DO UPDATE SET
            label = excluded.label,
            value = excluded.value,
            unit = excluded.unit,
            notes = excluded.notes
        ''',
        (
            payload['scenario_id'],
            payload.get('department_code'),
            payload['assumption_key'],
            payload['label'],
            payload['value'],
            payload.get('unit') or 'ratio',
            payload.get('notes') or '',
            user['email'],
            now,
        ),
    )
    db.log_audit('budget_assumption', payload['assumption_key'], 'upserted', user['email'], payload, now)
    rows = list_assumptions(payload['scenario_id'], user)
    return next(row for row in rows if row['assumption_key'] == payload['assumption_key'] and row['department_code'] == payload.get('department_code'))


def request_transfer(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _check_department(user, payload['from_department_code'])
    now = _now()
    transfer_id = db.execute(
        '''
        INSERT INTO budget_transfers (
            scenario_id, from_department_code, to_department_code, fund_code, account_code,
            period, amount, status, reason, requested_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'requested', ?, ?, ?)
        ''',
        (
            payload['scenario_id'],
            payload['from_department_code'],
            payload['to_department_code'],
            payload['fund_code'],
            payload['account_code'],
            payload['period'],
            payload['amount'],
            payload['reason'],
            user['email'],
            now,
        ),
    )
    db.log_audit('budget_transfer', str(transfer_id), 'requested', user['email'], payload, now)
    return get_transfer(transfer_id)


def approve_transfer(transfer_id: int, user: dict[str, Any]) -> dict[str, Any]:
    transfer = get_transfer(transfer_id)
    if transfer['status'] == 'approved':
        return transfer
    now = _now()
    amount = float(transfer['amount'])
    from_entry = append_ledger_entry(
        {
            'scenario_id': transfer['scenario_id'],
            'department_code': transfer['from_department_code'],
            'fund_code': transfer['fund_code'],
            'account_code': transfer['account_code'],
            'period': transfer['period'],
            'amount': amount,
            'notes': f"Transfer out: {transfer['reason']}",
            'source': 'budget_transfer',
            'ledger_type': 'budget',
            'metadata': {'transfer_id': transfer_id, 'side': 'from'},
        },
        actor=user['email'],
    )
    to_entry = append_ledger_entry(
        {
            'scenario_id': transfer['scenario_id'],
            'department_code': transfer['to_department_code'],
            'fund_code': transfer['fund_code'],
            'account_code': transfer['account_code'],
            'period': transfer['period'],
            'amount': -amount,
            'notes': f"Transfer in: {transfer['reason']}",
            'source': 'budget_transfer',
            'ledger_type': 'budget',
            'metadata': {'transfer_id': transfer_id, 'side': 'to'},
        },
        actor=user['email'],
    )
    db.execute(
        '''
        UPDATE budget_transfers
        SET status = 'approved', approved_by = ?, approved_at = ?,
            from_ledger_entry_id = ?, to_ledger_entry_id = ?
        WHERE id = ?
        ''',
        (user['email'], now, from_entry['id'], to_entry['id'], transfer_id),
    )
    db.log_audit('budget_transfer', str(transfer_id), 'approved', user['email'], {}, now)
    return get_transfer(transfer_id)


def list_transfers(scenario_id: int, user: dict[str, Any]) -> list[dict[str, Any]]:
    codes = allowed_codes(user, 'department')
    if codes is None:
        return db.fetch_all('SELECT * FROM budget_transfers WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    if not codes:
        return []
    placeholders = ','.join('?' for _ in codes)
    return db.fetch_all(
        f'''
        SELECT *
        FROM budget_transfers
        WHERE scenario_id = ? AND (from_department_code IN ({placeholders}) OR to_department_code IN ({placeholders}))
        ORDER BY id DESC
        ''',
        (scenario_id, *sorted(codes), *sorted(codes)),
    )


def get_transfer(transfer_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM budget_transfers WHERE id = ?', (transfer_id,))
    if row is None:
        raise ValueError('Budget transfer not found.')
    return row


def with_submission_counts(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    counts = db.fetch_one(
        '''
        SELECT
            COUNT(*) AS line_count,
            SUM(CASE WHEN obl.recurrence = 'recurring' THEN pl.amount ELSE 0 END) AS recurring_total,
            SUM(CASE WHEN obl.recurrence = 'one_time' THEN pl.amount ELSE 0 END) AS one_time_total
        FROM operating_budget_lines obl
        JOIN planning_ledger pl ON pl.id = obl.ledger_entry_id
        WHERE obl.submission_id = ? AND pl.reversed_at IS NULL
        ''',
        (row['id'],),
    )
    row['line_count'] = int(counts['line_count'] or 0)
    row['recurring_total'] = round(float(counts['recurring_total'] or 0.0), 2)
    row['one_time_total'] = round(float(counts['one_time_total'] or 0.0), 2)
    return row
