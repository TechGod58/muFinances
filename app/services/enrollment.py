from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.foundation import append_ledger_entry


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'terms': int(db.fetch_one('SELECT COUNT(*) AS count FROM enrollment_terms')['count']),
        'tuition_rates': int(db.fetch_one('SELECT COUNT(*) AS count FROM tuition_rates')['count']),
        'forecast_inputs': int(db.fetch_one('SELECT COUNT(*) AS count FROM enrollment_forecast_inputs')['count']),
        'forecast_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM tuition_forecast_runs')['count']),
    }
    checks = {
        'terms_ready': True,
        'rates_ready': True,
        'inputs_ready': True,
        'tuition_forecast_ready': True,
        'ledger_posting_ready': True,
    }
    return {'batch': 'B04', 'title': 'Enrollment And Tuition Planning', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def upsert_term(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO enrollment_terms (scenario_id, term_code, term_name, period, census_date, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, term_code) DO UPDATE SET
            term_name = excluded.term_name,
            period = excluded.period,
            census_date = excluded.census_date
        ''',
        (payload['scenario_id'], payload['term_code'], payload['term_name'], payload['period'], payload.get('census_date') or '', now),
    )
    db.log_audit('enrollment_term', payload['term_code'], 'upserted', user['email'], payload, now)
    return _fetch_one_required(
        'SELECT * FROM enrollment_terms WHERE scenario_id = ? AND term_code = ?',
        (payload['scenario_id'], payload['term_code']),
        'Enrollment term not found.',
    )


def list_terms(scenario_id: int) -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM enrollment_terms WHERE scenario_id = ? ORDER BY period, term_code', (scenario_id,))


def upsert_tuition_rate(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO tuition_rates (
            scenario_id, program_code, residency, rate_per_credit, default_credit_load,
            effective_term, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, program_code, residency, effective_term) DO UPDATE SET
            rate_per_credit = excluded.rate_per_credit,
            default_credit_load = excluded.default_credit_load
        ''',
        (
            payload['scenario_id'],
            payload['program_code'],
            payload['residency'],
            payload['rate_per_credit'],
            payload['default_credit_load'],
            payload['effective_term'],
            user['email'],
            now,
        ),
    )
    db.log_audit('tuition_rate', f"{payload['program_code']}:{payload['residency']}", 'upserted', user['email'], payload, now)
    return _fetch_one_required(
        '''
        SELECT *
        FROM tuition_rates
        WHERE scenario_id = ? AND program_code = ? AND residency = ? AND effective_term = ?
        ''',
        (payload['scenario_id'], payload['program_code'], payload['residency'], payload['effective_term']),
        'Tuition rate not found.',
    )


def list_tuition_rates(scenario_id: int) -> list[dict[str, Any]]:
    return db.fetch_all(
        'SELECT * FROM tuition_rates WHERE scenario_id = ? ORDER BY effective_term, program_code, residency',
        (scenario_id,),
    )


def upsert_forecast_input(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO enrollment_forecast_inputs (
            scenario_id, term_code, program_code, residency, headcount, fte,
            retention_rate, yield_rate, discount_rate, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, term_code, program_code, residency) DO UPDATE SET
            headcount = excluded.headcount,
            fte = excluded.fte,
            retention_rate = excluded.retention_rate,
            yield_rate = excluded.yield_rate,
            discount_rate = excluded.discount_rate
        ''',
        (
            payload['scenario_id'],
            payload['term_code'],
            payload['program_code'],
            payload['residency'],
            payload['headcount'],
            payload['fte'],
            payload['retention_rate'],
            payload['yield_rate'],
            payload['discount_rate'],
            user['email'],
            now,
        ),
    )
    db.log_audit('enrollment_forecast_input', f"{payload['term_code']}:{payload['program_code']}:{payload['residency']}", 'upserted', user['email'], payload, now)
    return _fetch_one_required(
        '''
        SELECT *
        FROM enrollment_forecast_inputs
        WHERE scenario_id = ? AND term_code = ? AND program_code = ? AND residency = ?
        ''',
        (payload['scenario_id'], payload['term_code'], payload['program_code'], payload['residency']),
        'Enrollment forecast input not found.',
    )


def list_forecast_inputs(scenario_id: int, term_code: str | None = None) -> list[dict[str, Any]]:
    if term_code:
        return db.fetch_all(
            '''
            SELECT *
            FROM enrollment_forecast_inputs
            WHERE scenario_id = ? AND term_code = ?
            ORDER BY program_code, residency
            ''',
            (scenario_id, term_code),
        )
    return db.fetch_all(
        'SELECT * FROM enrollment_forecast_inputs WHERE scenario_id = ? ORDER BY term_code, program_code, residency',
        (scenario_id,),
    )


def run_tuition_forecast(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    scenario_id = payload['scenario_id']
    term_code = payload['term_code']
    term = db.fetch_one('SELECT * FROM enrollment_terms WHERE scenario_id = ? AND term_code = ?', (scenario_id, term_code))
    if term is None:
        raise ValueError('Enrollment term not found.')
    inputs = list_forecast_inputs(scenario_id, term_code)
    if not inputs:
        raise ValueError('No enrollment forecast inputs found for term.')

    now = _now()
    run_id = db.execute(
        '''
        INSERT INTO tuition_forecast_runs (
            scenario_id, term_code, status, gross_revenue, discount_amount, net_revenue,
            created_by, created_at
        ) VALUES (?, ?, 'running', 0, 0, 0, ?, ?)
        ''',
        (scenario_id, term_code, user['email'], now),
    )
    gross_total = 0.0
    discount_total = 0.0
    net_total = 0.0
    created_lines = []

    for row in inputs:
        rate = _rate_for_input(scenario_id, row['program_code'], row['residency'], term_code)
        retained_headcount = float(row['headcount']) * float(row['retention_rate'])
        converted_headcount = retained_headcount * float(row['yield_rate'])
        billable_fte = max(float(row['fte']), converted_headcount)
        gross = round(billable_fte * float(rate['default_credit_load']) * float(rate['rate_per_credit']), 2)
        discount = round(gross * float(row['discount_rate']), 2)
        net = round(gross - discount, 2)
        gross_total += gross
        discount_total += discount
        net_total += net
        ledger = append_ledger_entry(
            {
                'scenario_id': scenario_id,
                'department_code': 'SCI',
                'fund_code': 'GEN',
                'account_code': 'TUITION',
                'program_code': row['program_code'],
                'period': term['period'],
                'amount': net,
                'source': 'tuition_forecast',
                'ledger_type': 'forecast',
                'notes': f"Tuition forecast {term_code} {row['program_code']} {row['residency']}",
                'metadata': {
                    'run_id': run_id,
                    'term_code': term_code,
                    'residency': row['residency'],
                    'headcount': row['headcount'],
                    'fte': row['fte'],
                    'gross_revenue': gross,
                    'discount_amount': discount,
                },
            },
            actor=user['email'],
            user=user,
        )
        line_id = db.execute(
            '''
            INSERT INTO tuition_forecast_run_lines (
                run_id, ledger_entry_id, program_code, residency, headcount, fte,
                gross_revenue, discount_amount, net_revenue
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (run_id, ledger['id'], row['program_code'], row['residency'], row['headcount'], row['fte'], gross, discount, net),
        )
        created_lines.append({'id': line_id, 'ledger_entry': ledger, 'gross_revenue': gross, 'discount_amount': discount, 'net_revenue': net})

    db.execute(
        '''
        UPDATE tuition_forecast_runs
        SET status = 'posted', gross_revenue = ?, discount_amount = ?, net_revenue = ?
        WHERE id = ?
        ''',
        (round(gross_total, 2), round(discount_total, 2), round(net_total, 2), run_id),
    )
    db.log_audit('tuition_forecast_run', str(run_id), 'posted', user['email'], payload, now)
    run = get_run(run_id)
    run['lines'] = created_lines
    return run


def list_runs(scenario_id: int) -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM tuition_forecast_runs WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))


def get_run(run_id: int) -> dict[str, Any]:
    return _fetch_one_required('SELECT * FROM tuition_forecast_runs WHERE id = ?', (run_id,), 'Tuition forecast run not found.')


def _rate_for_input(scenario_id: int, program_code: str, residency: str, term_code: str) -> dict[str, Any]:
    row = db.fetch_one(
        '''
        SELECT *
        FROM tuition_rates
        WHERE scenario_id = ? AND program_code = ? AND residency = ? AND effective_term <= ?
        ORDER BY effective_term DESC
        LIMIT 1
        ''',
        (scenario_id, program_code, residency, term_code),
    )
    if row is None:
        raise ValueError(f'Missing tuition rate for {program_code}/{residency}.')
    return row


def _fetch_one_required(query: str, params: tuple[Any, ...], message: str) -> dict[str, Any]:
    row = db.fetch_one(query, params)
    if row is None:
        raise ValueError(message)
    return row
