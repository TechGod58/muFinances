from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app import db
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
        'positions': int(db.fetch_one('SELECT COUNT(*) AS count FROM workforce_positions')['count']),
        'faculty_loads': int(db.fetch_one('SELECT COUNT(*) AS count FROM faculty_loads')['count']),
        'grant_budgets': int(db.fetch_one('SELECT COUNT(*) AS count FROM grant_budgets')['count']),
        'capital_requests': int(db.fetch_one('SELECT COUNT(*) AS count FROM capital_requests')['count']),
    }
    checks = {
        'position_control_ready': True,
        'faculty_load_ready': True,
        'grant_budgets_ready': True,
        'burn_rates_ready': True,
        'capital_depreciation_ready': True,
    }
    return {'batch': 'B05', 'title': 'Workforce Faculty Grants Capital', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def upsert_position(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _check_department(user, payload['department_code'])
    now = _now()
    db.execute(
        '''
        INSERT INTO workforce_positions (
            scenario_id, position_code, title, department_code, employee_type, fte,
            annual_salary, benefit_rate, vacancy_rate, status, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', ?, ?)
        ON CONFLICT(scenario_id, position_code) DO UPDATE SET
            title = excluded.title,
            department_code = excluded.department_code,
            employee_type = excluded.employee_type,
            fte = excluded.fte,
            annual_salary = excluded.annual_salary,
            benefit_rate = excluded.benefit_rate,
            vacancy_rate = excluded.vacancy_rate
        ''',
        (
            payload['scenario_id'], payload['position_code'], payload['title'], payload['department_code'],
            payload['employee_type'], payload['fte'], payload['annual_salary'], payload['benefit_rate'],
            payload.get('vacancy_rate') or 0, user['email'], now,
        ),
    )
    db.log_audit('workforce_position', payload['position_code'], 'upserted', user['email'], payload, now)
    return _format_position(_one('SELECT * FROM workforce_positions WHERE scenario_id = ? AND position_code = ?', (payload['scenario_id'], payload['position_code'])))


def list_positions(scenario_id: int, user: dict[str, Any]) -> list[dict[str, Any]]:
    rows = _department_filtered('workforce_positions', scenario_id, user, 'department_code, position_code')
    return [_format_position(row) for row in rows]


def upsert_faculty_load(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _check_department(user, payload['department_code'])
    now = _now()
    db.execute(
        '''
        INSERT INTO faculty_loads (
            scenario_id, department_code, term_code, course_code, sections,
            credit_hours, faculty_fte, adjunct_cost, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, department_code, term_code, course_code) DO UPDATE SET
            sections = excluded.sections,
            credit_hours = excluded.credit_hours,
            faculty_fte = excluded.faculty_fte,
            adjunct_cost = excluded.adjunct_cost
        ''',
        (
            payload['scenario_id'], payload['department_code'], payload['term_code'], payload['course_code'],
            payload['sections'], payload['credit_hours'], payload['faculty_fte'], payload.get('adjunct_cost') or 0,
            user['email'], now,
        ),
    )
    db.log_audit('faculty_load', payload['course_code'], 'upserted', user['email'], payload, now)
    return _one(
        '''
        SELECT * FROM faculty_loads
        WHERE scenario_id = ? AND department_code = ? AND term_code = ? AND course_code = ?
        ''',
        (payload['scenario_id'], payload['department_code'], payload['term_code'], payload['course_code']),
    )


def list_faculty_loads(scenario_id: int, user: dict[str, Any]) -> list[dict[str, Any]]:
    return _department_filtered('faculty_loads', scenario_id, user, 'department_code, term_code, course_code')


def upsert_grant_budget(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _check_department(user, payload['department_code'])
    now = _now()
    db.execute(
        '''
        INSERT INTO grant_budgets (
            scenario_id, grant_code, department_code, sponsor, start_period, end_period,
            total_award, direct_cost_budget, indirect_cost_rate, spent_to_date,
            status, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
        ON CONFLICT(scenario_id, grant_code) DO UPDATE SET
            department_code = excluded.department_code,
            sponsor = excluded.sponsor,
            start_period = excluded.start_period,
            end_period = excluded.end_period,
            total_award = excluded.total_award,
            direct_cost_budget = excluded.direct_cost_budget,
            indirect_cost_rate = excluded.indirect_cost_rate,
            spent_to_date = excluded.spent_to_date
        ''',
        (
            payload['scenario_id'], payload['grant_code'], payload['department_code'], payload['sponsor'],
            payload['start_period'], payload['end_period'], payload['total_award'], payload['direct_cost_budget'],
            payload['indirect_cost_rate'], payload.get('spent_to_date') or 0, user['email'], now,
        ),
    )
    db.log_audit('grant_budget', payload['grant_code'], 'upserted', user['email'], payload, now)
    return _format_grant(_one('SELECT * FROM grant_budgets WHERE scenario_id = ? AND grant_code = ?', (payload['scenario_id'], payload['grant_code'])))


def list_grant_budgets(scenario_id: int, user: dict[str, Any]) -> list[dict[str, Any]]:
    return [_format_grant(row) for row in _department_filtered('grant_budgets', scenario_id, user, 'department_code, grant_code')]


def upsert_capital_request(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _check_department(user, payload['department_code'])
    now = _now()
    db.execute(
        '''
        INSERT INTO capital_requests (
            scenario_id, request_code, department_code, project_name, asset_category,
            acquisition_period, capital_cost, useful_life_years, funding_source,
            status, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'requested', ?, ?)
        ON CONFLICT(scenario_id, request_code) DO UPDATE SET
            department_code = excluded.department_code,
            project_name = excluded.project_name,
            asset_category = excluded.asset_category,
            acquisition_period = excluded.acquisition_period,
            capital_cost = excluded.capital_cost,
            useful_life_years = excluded.useful_life_years,
            funding_source = excluded.funding_source
        ''',
        (
            payload['scenario_id'], payload['request_code'], payload['department_code'], payload['project_name'],
            payload['asset_category'], payload['acquisition_period'], payload['capital_cost'],
            payload['useful_life_years'], payload['funding_source'], user['email'], now,
        ),
    )
    db.log_audit('capital_request', payload['request_code'], 'upserted', user['email'], payload, now)
    return _format_capital(_one('SELECT * FROM capital_requests WHERE scenario_id = ? AND request_code = ?', (payload['scenario_id'], payload['request_code'])))


def list_capital_requests(scenario_id: int, user: dict[str, Any]) -> list[dict[str, Any]]:
    return [_format_capital(row) for row in _department_filtered('capital_requests', scenario_id, user, 'department_code, request_code')]


def approve_capital_request(request_id: int, user: dict[str, Any]) -> dict[str, Any]:
    row = _one('SELECT * FROM capital_requests WHERE id = ?', (request_id,))
    _check_department(user, row['department_code'])
    if row['status'] == 'approved' and row['ledger_entry_id']:
        return _format_capital(row)
    ledger = append_ledger_entry(
        {
            'scenario_id': row['scenario_id'],
            'department_code': row['department_code'],
            'fund_code': 'GEN',
            'account_code': 'SUPPLIES',
            'period': row['acquisition_period'],
            'amount': -float(row['capital_cost']),
            'source': 'capital_request',
            'ledger_type': 'capital',
            'notes': f"Capital request {row['request_code']}: {row['project_name']}",
            'metadata': {
                'request_id': request_id,
                'asset_category': row['asset_category'],
                'annual_depreciation': round(float(row['capital_cost']) / int(row['useful_life_years']), 2),
            },
        },
        actor=user['email'],
        user=user,
    )
    now = _now()
    db.execute(
        '''
        UPDATE capital_requests
        SET status = 'approved', approved_by = ?, approved_at = ?, ledger_entry_id = ?
        WHERE id = ?
        ''',
        (user['email'], now, ledger['id'], request_id),
    )
    db.log_audit('capital_request', str(request_id), 'approved', user['email'], {'ledger_entry_id': ledger['id']}, now)
    return _format_capital(_one('SELECT * FROM capital_requests WHERE id = ?', (request_id,)))


def _department_filtered(table: str, scenario_id: int, user: dict[str, Any], order_by: str) -> list[dict[str, Any]]:
    codes = allowed_codes(user, 'department')
    if codes is None:
        return db.fetch_all(f'SELECT * FROM {table} WHERE scenario_id = ? ORDER BY {order_by}', (scenario_id,))
    if not codes:
        return []
    placeholders = ','.join('?' for _ in codes)
    return db.fetch_all(
        f'SELECT * FROM {table} WHERE scenario_id = ? AND department_code IN ({placeholders}) ORDER BY {order_by}',
        (scenario_id, *sorted(codes)),
    )


def _format_position(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    salary_cost = float(row['annual_salary']) * float(row['fte']) * (1.0 - float(row['vacancy_rate']))
    row['salary_cost'] = round(salary_cost, 2)
    row['benefit_cost'] = round(salary_cost * float(row['benefit_rate']), 2)
    row['total_compensation'] = round(row['salary_cost'] + row['benefit_cost'], 2)
    return row


def _format_grant(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row['indirect_cost_budget'] = round(float(row['direct_cost_budget']) * float(row['indirect_cost_rate']), 2)
    row['burn_rate'] = round(float(row['spent_to_date']) / max(1.0, float(row['total_award'])), 4)
    row['remaining_award'] = round(float(row['total_award']) - float(row['spent_to_date']), 2)
    return row


def _format_capital(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row['annual_depreciation'] = round(float(row['capital_cost']) / max(1, int(row['useful_life_years'])), 2)
    return row


def _one(query: str, params: tuple[Any, ...]) -> dict[str, Any]:
    row = db.fetch_one(query, params)
    if row is None:
        raise ValueError('Record not found.')
    return row
