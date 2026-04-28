from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.formula_engine import evaluate_formula


@dataclass(slots=True)
class Driver:
    key: str
    label: str
    expression: str | None
    value: float | None
    unit: str


def safe_eval(expr: str, context: dict[str, float]) -> float:
    return float(evaluate_formula(expr, context)['value'])


def resolve_drivers(scenario_id: int) -> dict[str, float]:
    rows = db.fetch_all(
        '''
        SELECT driver_key, label, expression, value, unit
        FROM drivers
        WHERE scenario_id = ?
        ORDER BY id ASC
        ''',
        (scenario_id,),
    )
    drivers = [Driver(key=row['driver_key'], label=row['label'], expression=row['expression'], value=row['value'], unit=row['unit']) for row in rows]
    resolved: dict[str, float] = {}
    pending = drivers[:]

    max_passes = len(pending) + 2
    for _ in range(max_passes):
        next_pending: list[Driver] = []
        progressed = False
        for driver in pending:
            if driver.value is not None:
                resolved[driver.key] = float(driver.value)
                progressed = True
                continue
            if not driver.expression:
                raise ValueError(f'Driver {driver.key} is missing both a literal value and an expression.')
            try:
                resolved[driver.key] = safe_eval(driver.expression, resolved)
                progressed = True
            except NameError:
                next_pending.append(driver)
        if not next_pending:
            return resolved
        if not progressed:
            missing = ', '.join(item.key for item in next_pending)
            raise ValueError(f'Circular or unresolved driver dependencies: {missing}')
        pending = next_pending

    raise ValueError('Could not resolve drivers within the allowed passes.')


def _month_range(start_period: str, end_period: str) -> list[str]:
    start_year, start_month = map(int, start_period.split('-'))
    end_year, end_month = map(int, end_period.split('-'))

    months: list[str] = []
    year = start_year
    month = start_month
    while (year, month) <= (end_year, end_month):
        months.append(f'{year:04d}-{month:02d}')
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


def run_forecast(scenario_id: int, actor: str = 'planner.bot') -> dict[str, Any]:
    scenario = db.fetch_one('SELECT * FROM scenarios WHERE id = ?', (scenario_id,))
    if scenario is None:
        raise ValueError('Scenario not found.')

    resolved = resolve_drivers(scenario_id)
    periods = _month_range(scenario['start_period'], scenario['end_period'])
    existing = db.fetch_all(
        '''
        SELECT period, account_code, department_code, fund_code, amount
        FROM planning_ledger
        WHERE scenario_id = ? AND source = 'manual' AND reversed_at IS NULL
        ORDER BY period, account_code
        ''',
        (scenario_id,),
    )

    if not existing:
        raise ValueError('Scenario has no base line items to forecast from.')

    base_period = min(row['period'] for row in existing)
    base_rows = [row for row in existing if row['period'] == base_period]

    growth_map = {
        'TUITION': 1.0 + resolved.get('student_growth', 0.0),
        'SALARY': 1.0 + resolved.get('salary_step_increase', 0.0),
        'BENEFITS': 1.0 + resolved.get('salary_step_increase', 0.0),
        'UTILITIES': 1.0 + resolved.get('utilities_inflation', 0.0),
        'SUPPLIES': 1.0 + resolved.get('utilities_inflation', 0.0),
        'AUXILIARY': 1.0 + resolved.get('auxiliary_growth', 0.0),
    }

    with db.get_connection() as conn:
        now = datetime.now(UTC).isoformat()
        conn.execute(
            '''
            UPDATE planning_ledger
            SET reversed_at = ?
            WHERE scenario_id = ? AND source = 'forecast' AND reversed_at IS NULL
            ''',
            (now, scenario_id),
        )

        created_rows: list[dict[str, Any]] = []
        for period in periods:
            if period == base_period:
                continue
            for row in base_rows:
                factor = growth_map.get(row['account_code'], 1.0)
                amount = round(float(row['amount']) * factor, 2)
                cur = conn.execute(
                    '''
                    INSERT INTO planning_ledger (
                        scenario_id, entity_code, department_code, fund_code, account_code,
                        period, amount, notes, source, driver_key, ledger_type,
                        posted_by, posted_at, metadata_json
                    ) VALUES (?, 'CAMPUS', ?, ?, ?, ?, ?, ?, 'forecast', ?, 'forecast', ?, ?, '{}')
                    ''',
                    (
                        scenario_id,
                        row['department_code'],
                        row['fund_code'],
                        row['account_code'],
                        period,
                        amount,
                        f'Forecast generated from {base_period}',
                        row['account_code'].lower(),
                        actor,
                        now,
                    ),
                )
                created_rows.append(
                    {
                        'id': int(cur.lastrowid),
                        'scenario_id': scenario_id,
                        'department_code': row['department_code'],
                        'fund_code': row['fund_code'],
                        'account_code': row['account_code'],
                        'period': period,
                        'amount': amount,
                        'notes': f'Forecast generated from {base_period}',
                        'source': 'forecast',
                        'driver_key': row['account_code'].lower(),
                    }
                )

    db.log_audit(
        entity_type='scenario',
        entity_id=str(scenario_id),
        action='forecast_run',
        actor=actor,
        detail={'drivers': resolved, 'count': len(created_rows)},
        created_at=datetime.now(UTC).isoformat(),
    )
    return {'scenario_id': scenario_id, 'resolved_drivers': resolved, 'created_line_items': created_rows}
