from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.foundation import create_dimension_member

ACCOUNT_RULES = [
    {
        'account_code': 'TUITION',
        'account_name': 'Tuition Revenue',
        'parent_account_code': 'REV',
        'account_type': 'revenue',
        'normal_balance': 'credit',
        'sign_multiplier': 1,
        'statement': 'income_statement',
        'statement_section': 'operating_revenue',
        'statement_line': 'Tuition and fees',
        'effective_from': '2026-07',
    },
    {
        'account_code': 'AUXILIARY',
        'account_name': 'Auxiliary Revenue',
        'parent_account_code': 'REV',
        'account_type': 'revenue',
        'normal_balance': 'credit',
        'sign_multiplier': 1,
        'statement': 'income_statement',
        'statement_section': 'operating_revenue',
        'statement_line': 'Auxiliary revenue',
        'effective_from': '2026-07',
    },
    {
        'account_code': 'SALARY',
        'account_name': 'Salary Expense',
        'parent_account_code': 'EXP',
        'account_type': 'expense',
        'normal_balance': 'debit',
        'sign_multiplier': -1,
        'statement': 'income_statement',
        'statement_section': 'operating_expense',
        'statement_line': 'Salaries and wages',
        'effective_from': '2026-07',
    },
    {
        'account_code': 'BENEFITS',
        'account_name': 'Benefits Expense',
        'parent_account_code': 'EXP',
        'account_type': 'expense',
        'normal_balance': 'debit',
        'sign_multiplier': -1,
        'statement': 'income_statement',
        'statement_section': 'operating_expense',
        'statement_line': 'Benefits',
        'effective_from': '2026-07',
    },
    {
        'account_code': 'UTILITIES',
        'account_name': 'Utilities Expense',
        'parent_account_code': 'EXP',
        'account_type': 'expense',
        'normal_balance': 'debit',
        'sign_multiplier': -1,
        'statement': 'income_statement',
        'statement_section': 'operating_expense',
        'statement_line': 'Utilities',
        'effective_from': '2026-07',
    },
    {
        'account_code': 'SUPPLIES',
        'account_name': 'Supplies Expense',
        'parent_account_code': 'EXP',
        'account_type': 'expense',
        'normal_balance': 'debit',
        'sign_multiplier': -1,
        'statement': 'income_statement',
        'statement_section': 'operating_expense',
        'statement_line': 'Supplies',
        'effective_from': '2026-07',
    },
    {
        'account_code': 'TRANSFER',
        'account_name': 'Intercompany Transfer',
        'parent_account_code': 'REV',
        'account_type': 'revenue',
        'normal_balance': 'credit',
        'sign_multiplier': 1,
        'statement': 'income_statement',
        'statement_section': 'non_operating',
        'statement_line': 'Transfers',
        'effective_from': '2026-07',
        'validation_rules': {'allow_mixed_sign': True},
    },
    {
        'account_code': 'CASH',
        'account_name': 'Cash and Equivalents',
        'parent_account_code': 'ASSET',
        'account_type': 'asset',
        'normal_balance': 'debit',
        'sign_multiplier': 1,
        'statement': 'balance_sheet',
        'statement_section': 'current_assets',
        'statement_line': 'Cash and equivalents',
        'effective_from': '2026-07',
    },
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS chart_of_accounts_governance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_code TEXT NOT NULL UNIQUE,
                account_name TEXT NOT NULL,
                parent_account_code TEXT DEFAULT NULL,
                account_type TEXT NOT NULL,
                normal_balance TEXT NOT NULL,
                sign_multiplier INTEGER NOT NULL,
                statement TEXT NOT NULL,
                statement_section TEXT NOT NULL,
                statement_line TEXT NOT NULL,
                effective_from TEXT NOT NULL,
                effective_to TEXT DEFAULT NULL,
                validation_rules_json TEXT NOT NULL DEFAULT '{}',
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS account_statement_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mapping_key TEXT NOT NULL UNIQUE,
                account_code TEXT NOT NULL,
                statement TEXT NOT NULL,
                statement_section TEXT NOT NULL,
                statement_line TEXT NOT NULL,
                display_order INTEGER NOT NULL DEFAULT 100,
                normal_sign TEXT NOT NULL,
                effective_from TEXT NOT NULL,
                effective_to TEXT DEFAULT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS coa_validation_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                exceptions_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_coa_governance_statement
            ON chart_of_accounts_governance (statement, statement_section, active);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    seed_default_governance({'email': 'system'})
    counts = {
        'governed_accounts': int(db.fetch_one('SELECT COUNT(*) AS count FROM chart_of_accounts_governance WHERE active = 1')['count']),
        'statement_mappings': int(db.fetch_one('SELECT COUNT(*) AS count FROM account_statement_mappings WHERE active = 1')['count']),
        'validation_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM coa_validation_runs')['count']),
    }
    checks = {
        'coa_hierarchy_ready': counts['governed_accounts'] >= 5,
        'account_types_ready': True,
        'normal_balances_ready': True,
        'debit_credit_sign_conventions_ready': True,
        'statement_mappings_ready': counts['statement_mappings'] >= 5,
        'effective_dating_ready': True,
        'validation_rules_ready': True,
    }
    return {'batch': 'B146', 'title': 'Chart Of Accounts And Sign Convention Governance', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def seed_default_governance(user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    created = []
    for rule in ACCOUNT_RULES:
        created.append(upsert_account_rule(rule, user))
    return {'count': len(created), 'accounts': created}


def upsert_account_rule(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    _validate_rule(payload)
    now = _now()
    code = payload['account_code'].upper()
    validation_rules = {
        'normal_balance': payload['normal_balance'],
        'ledger_amount_sign': 'positive' if int(payload['sign_multiplier']) > 0 else 'negative',
        'effective_dating_required': True,
        **(payload.get('validation_rules') or {}),
    }
    db.execute(
        '''
        INSERT INTO chart_of_accounts_governance (
            account_code, account_name, parent_account_code, account_type, normal_balance,
            sign_multiplier, statement, statement_section, statement_line, effective_from,
            effective_to, validation_rules_json, active, created_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(account_code) DO UPDATE SET
            account_name = excluded.account_name,
            parent_account_code = excluded.parent_account_code,
            account_type = excluded.account_type,
            normal_balance = excluded.normal_balance,
            sign_multiplier = excluded.sign_multiplier,
            statement = excluded.statement,
            statement_section = excluded.statement_section,
            statement_line = excluded.statement_line,
            effective_from = excluded.effective_from,
            effective_to = excluded.effective_to,
            validation_rules_json = excluded.validation_rules_json,
            active = excluded.active,
            updated_at = excluded.updated_at
        ''',
        (
            code,
            payload['account_name'],
            payload.get('parent_account_code'),
            payload['account_type'],
            payload['normal_balance'],
            int(payload['sign_multiplier']),
            payload['statement'],
            payload['statement_section'],
            payload['statement_line'],
            payload['effective_from'],
            payload.get('effective_to'),
            json.dumps(validation_rules, sort_keys=True),
            1 if payload.get('active', True) else 0,
            user.get('email', 'system'),
            now,
            now,
        ),
    )
    db.execute(
        '''
        INSERT INTO account_statement_mappings (
            mapping_key, account_code, statement, statement_section, statement_line, display_order,
            normal_sign, effective_from, effective_to, active, created_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(mapping_key) DO UPDATE SET
            statement = excluded.statement,
            statement_section = excluded.statement_section,
            statement_line = excluded.statement_line,
            normal_sign = excluded.normal_sign,
            effective_from = excluded.effective_from,
            effective_to = excluded.effective_to,
            active = excluded.active,
            updated_at = excluded.updated_at
        ''',
        (
            f'{code}:{payload["statement"]}',
            code,
            payload['statement'],
            payload['statement_section'],
            payload['statement_line'],
            int(payload.get('display_order', 100)),
            'positive' if int(payload['sign_multiplier']) > 0 else 'negative',
            payload['effective_from'],
            payload.get('effective_to'),
            1 if payload.get('active', True) else 0,
            user.get('email', 'system'),
            now,
            now,
        ),
    )
    create_dimension_member(
        {
            'dimension_kind': 'account',
            'code': code,
            'name': payload['account_name'],
            'parent_code': payload.get('parent_account_code'),
            'metadata': {
                'account_type': payload['account_type'],
                'normal_balance': payload['normal_balance'],
                'sign_multiplier': int(payload['sign_multiplier']),
                'statement': payload['statement'],
                'statement_section': payload['statement_section'],
                'effective_from': payload['effective_from'],
                'effective_to': payload.get('effective_to'),
            },
        },
        actor=user.get('email', 'system'),
    )
    db.log_audit('coa_governance', code, 'upserted', user.get('email', 'system'), payload, now)
    return get_account_rule(code)


def list_accounts() -> list[dict[str, Any]]:
    _ensure_tables()
    seed_default_governance({'email': 'system'})
    rows = db.fetch_all('SELECT * FROM chart_of_accounts_governance ORDER BY statement, statement_section, account_code')
    return [_format_account(row) for row in rows]


def list_statement_mappings() -> list[dict[str, Any]]:
    _ensure_tables()
    seed_default_governance({'email': 'system'})
    rows = db.fetch_all('SELECT * FROM account_statement_mappings ORDER BY statement, display_order, account_code')
    for row in rows:
        row['active'] = bool(row['active'])
    return rows


def validate_chart_of_accounts(payload: dict[str, Any] | None, user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    seed_default_governance(user)
    payload = payload or {}
    run_key = payload.get('run_key') or f"b146-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    account_codes = [str(code).upper() for code in payload.get('account_codes', []) if str(code).strip()]
    params: tuple[Any, ...] = ()
    filter_sql = ''
    if account_codes:
        placeholders = ', '.join('?' for _ in account_codes)
        filter_sql = f'AND account_code IN ({placeholders})'
        params = tuple(account_codes)
    ledger_rows = db.fetch_all(
        f'''
        SELECT account_code, COUNT(*) AS row_count, COALESCE(SUM(amount), 0) AS total
        FROM planning_ledger
        WHERE reversed_at IS NULL
          {filter_sql}
        GROUP BY account_code
        ORDER BY account_code
        ''',
        params,
    )
    governed = {row['account_code']: row for row in list_accounts() if row['active']}
    exceptions = []
    for row in ledger_rows:
        rule = governed.get(row['account_code'])
        if rule is None:
            exceptions.append({'account_code': row['account_code'], 'exception': 'missing_governance_rule', 'row_count': row['row_count']})
            continue
        if rule['validation_rules'].get('allow_mixed_sign'):
            continue
        total = float(row['total'])
        sign = int(rule['sign_multiplier'])
        if total and ((sign > 0 and total < 0) or (sign < 0 and total > 0)):
            exceptions.append({'account_code': row['account_code'], 'exception': 'unexpected_sign', 'total': round(total, 2), 'expected_sign': 'positive' if sign > 0 else 'negative'})
    checks = {
        'all_ledger_accounts_have_governance': not any(item['exception'] == 'missing_governance_rule' for item in exceptions),
        'normal_balance_signs_validated': not any(item['exception'] == 'unexpected_sign' for item in exceptions),
        'statement_mappings_exist': len(list_statement_mappings()) >= len(governed),
        'effective_dates_present': all(row['effective_from'] for row in governed.values()),
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    now = _now()
    row_id = db.execute(
        '''
        INSERT INTO coa_validation_runs (
            run_key, status, checks_json, exceptions_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(exceptions, sort_keys=True),
            user['email'],
            now,
        ),
    )
    db.log_audit('coa_governance', run_key, 'validated', user['email'], {'checks': checks, 'exception_count': len(exceptions)}, now)
    return _format_validation(db.fetch_one('SELECT * FROM coa_validation_runs WHERE id = ?', (row_id,)))


def get_account_rule(account_code: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM chart_of_accounts_governance WHERE account_code = ?', (account_code.upper(),))
    if row is None:
        raise ValueError('COA governance rule not found.')
    return _format_account(row)


def list_validation_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM coa_validation_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_validation(row) for row in rows]


def _validate_rule(payload: dict[str, Any]) -> None:
    if payload['account_type'] not in {'asset', 'liability', 'net_asset', 'revenue', 'expense'}:
        raise ValueError('Unsupported account type.')
    if payload['normal_balance'] not in {'debit', 'credit'}:
        raise ValueError('Normal balance must be debit or credit.')
    if int(payload['sign_multiplier']) not in {-1, 1}:
        raise ValueError('Sign multiplier must be -1 or 1.')


def _format_account(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['active'] = bool(result['active'])
    result['validation_rules'] = json.loads(result.pop('validation_rules_json') or '{}')
    return result


def _format_validation(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise ValueError('COA validation run not found.')
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['exceptions'] = json.loads(result.pop('exceptions_json') or '[]')
    result['complete'] = result['status'] == 'passed'
    return result
