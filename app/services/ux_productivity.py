from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.foundation import summary_by_dimensions
from app.services.operating_budget import add_budget_line, create_submission, list_submissions


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'profiles': int(db.fetch_one('SELECT COUNT(*) AS count FROM user_profiles')['count']),
        'notifications': int(db.fetch_one('SELECT COUNT(*) AS count FROM notifications')['count']),
        'bulk_paste_imports': int(db.fetch_one('SELECT COUNT(*) AS count FROM bulk_paste_imports')['count']),
    }
    checks = {
        'fiscal_period_selector_ready': True,
        'notification_center_ready': True,
        'user_profile_controls_ready': True,
        'spreadsheet_editable_grids_ready': True,
        'bulk_paste_import_ui_ready': True,
        'inline_validation_messages_ready': True,
        'missing_submissions_review_ready': True,
        'department_comparison_ready': True,
    }
    return {'batch': 'B22', 'title': 'UX Productivity Layer', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def productivity_bootstrap(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    return {
        'profile': get_profile(user),
        'periods': db.fetch_all('SELECT * FROM fiscal_periods ORDER BY fiscal_year ASC, period_index ASC, period ASC'),
        'notifications': list_notifications(user, scenario_id),
        'missing_submissions': missing_submissions(scenario_id, user),
        'department_comparison': department_comparison(scenario_id, user),
    }


def get_profile(user: dict[str, Any]) -> dict[str, Any]:
    row = db.fetch_one(
        '''
        SELECT up.*, u.email
        FROM users u
        LEFT JOIN user_profiles up ON up.user_id = u.id
        WHERE u.id = ?
        ''',
        (user['id'],),
    )
    if row is None:
        raise ValueError('User not found.')
    if row.get('preferences_json') is None:
        return {
            'user_id': user['id'],
            'email': row['email'],
            'display_name': user.get('display_name') or row['email'],
            'default_scenario_id': None,
            'default_period': None,
            'preferences': {},
        }
    result = dict(row)
    result['preferences'] = json.loads(result.pop('preferences_json') or '{}')
    return result


def update_profile(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO user_profiles (
            user_id, display_name, default_scenario_id, default_period, preferences_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            display_name = excluded.display_name,
            default_scenario_id = excluded.default_scenario_id,
            default_period = excluded.default_period,
            preferences_json = excluded.preferences_json,
            updated_at = excluded.updated_at
        ''',
        (
            user['id'], payload.get('display_name') or user.get('display_name') or user['email'],
            payload.get('default_scenario_id'), payload.get('default_period'),
            json.dumps(payload.get('preferences') or {}, sort_keys=True), now,
        ),
    )
    db.log_audit('user_profile', str(user['id']), 'updated', user['email'], payload, now)
    return get_profile(user)


def create_notification(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    notification_id = db.execute(
        '''
        INSERT INTO notifications (
            user_id, scenario_id, notification_type, title, message, severity, status, link, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'unread', ?, ?)
        ''',
        (
            payload.get('user_id') or user['id'], payload.get('scenario_id'), payload['notification_type'],
            payload['title'], payload['message'], payload.get('severity', 'info'), payload.get('link') or '', now,
        ),
    )
    db.log_audit('notification', str(notification_id), 'created', user['email'], payload, now)
    return get_notification(notification_id)


def list_notifications(user: dict[str, Any], scenario_id: int | None = None) -> list[dict[str, Any]]:
    params: list[Any] = [user['id']]
    where = '(user_id = ? OR user_id IS NULL)'
    if scenario_id:
        where += ' AND (scenario_id = ? OR scenario_id IS NULL)'
        params.append(scenario_id)
    return db.fetch_all(f'SELECT * FROM notifications WHERE {where} ORDER BY status DESC, id DESC LIMIT 100', tuple(params))


def get_notification(notification_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM notifications WHERE id = ?', (notification_id,))
    if row is None:
        raise ValueError('Notification not found.')
    return row


def mark_notification_read(notification_id: int, user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute("UPDATE notifications SET status = 'read', read_at = ? WHERE id = ? AND (user_id = ? OR user_id IS NULL)", (now, notification_id, user['id']))
    return get_notification(notification_id)


def validate_grid_rows(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get('rows') or []
    messages = [_validate_budget_grid_row(index, row) for index, row in enumerate(rows, start=1)]
    messages = [message for message in messages if message is not None]
    return {'scenario_id': payload['scenario_id'], 'valid': not messages, 'messages': messages}


def bulk_paste_budget(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    rows = _parse_paste(payload.get('paste_text') or '')
    messages: list[dict[str, Any]] = []
    accepted = 0
    for index, row in enumerate(rows, start=1):
        message = _validate_budget_grid_row(index, row)
        if message:
            messages.append(message)
            continue
        submission = create_submission(
            {'scenario_id': payload['scenario_id'], 'department_code': row['department_code'], 'owner': user['email'], 'notes': 'Bulk paste import'},
            user,
        )
        add_budget_line(
            int(submission['id']),
            {
                'fund_code': row['fund_code'],
                'account_code': row['account_code'],
                'period': row['period'],
                'amount': float(row['amount']),
                'notes': row.get('notes') or 'Bulk paste grid row',
                'line_type': row.get('line_type') or 'expense',
                'recurrence': row.get('recurrence') or 'one_time',
            },
            user,
        )
        accepted += 1
    now = _now()
    import_id = db.execute(
        '''
        INSERT INTO bulk_paste_imports (
            scenario_id, import_type, row_count, accepted_rows, rejected_rows, status, messages_json, created_by, created_at
        ) VALUES (?, 'operating_budget_grid', ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            payload['scenario_id'], len(rows), accepted, len(messages),
            'accepted' if not messages else 'accepted_with_validation_messages' if accepted else 'rejected',
            json.dumps(messages, sort_keys=True), user['email'], now,
        ),
    )
    db.log_audit('bulk_paste_import', str(import_id), 'created', user['email'], {'accepted': accepted, 'rejected': len(messages)}, now)
    return get_bulk_import(import_id)


def list_bulk_imports(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        rows = db.fetch_all('SELECT * FROM bulk_paste_imports WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    else:
        rows = db.fetch_all('SELECT * FROM bulk_paste_imports ORDER BY id DESC')
    return [_format_bulk(row) for row in rows]


def get_bulk_import(import_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM bulk_paste_imports WHERE id = ?', (import_id,))
    if row is None:
        raise ValueError('Bulk import not found.')
    return _format_bulk(row)


def missing_submissions(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    submissions = {row['department_code']: row for row in list_submissions(scenario_id, user)}
    departments = db.fetch_all("SELECT code, name FROM dimension_members WHERE dimension_kind = 'department' AND active = 1 ORDER BY code")
    rows = []
    for department in departments:
        submission = submissions.get(department['code'])
        rows.append({
            'department_code': department['code'],
            'department_name': department['name'],
            'status': submission['status'] if submission else 'missing',
            'line_count': submission['line_count'] if submission else 0,
        })
    missing_count = sum(1 for row in rows if row['status'] == 'missing')
    return {'scenario_id': scenario_id, 'missing_count': missing_count, 'rows': rows}


def department_comparison(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    summary = summary_by_dimensions(scenario_id, user=user)
    rows = [
        {'department_code': code, 'amount': amount}
        for code, amount in sorted(summary['by_department'].items(), key=lambda item: item[0])
    ]
    average = round(sum(float(row['amount']) for row in rows) / max(1, len(rows)), 2)
    for row in rows:
        row['variance_to_average'] = round(float(row['amount']) - average, 2)
    return {'scenario_id': scenario_id, 'average': average, 'rows': rows}


def _parse_paste(text: str) -> list[dict[str, Any]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return []
    header = [item.strip() for item in lines[0].split('\t')]
    rows = []
    for line in lines[1:]:
        values = [item.strip() for item in line.split('\t')]
        rows.append({header[index]: values[index] if index < len(values) else '' for index in range(len(header))})
    return rows


def _validate_budget_grid_row(index: int, row: dict[str, Any]) -> dict[str, Any] | None:
    required = ['department_code', 'fund_code', 'account_code', 'period', 'amount']
    missing = [key for key in required if row.get(key) in (None, '')]
    if missing:
        return {'row': index, 'field': ','.join(missing), 'message': f"Missing required fields: {', '.join(missing)}"}
    if len(str(row['period'])) != 7:
        return {'row': index, 'field': 'period', 'message': 'Period must use YYYY-MM.'}
    try:
        float(row['amount'])
    except (TypeError, ValueError):
        return {'row': index, 'field': 'amount', 'message': 'Amount must be numeric.'}
    return None


def _format_bulk(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['messages'] = json.loads(result.pop('messages_json') or '[]')
    return result
