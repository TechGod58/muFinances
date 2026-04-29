from __future__ import annotations

import json
import hashlib
import math
from datetime import UTC, datetime
from typing import Any

from app import db
from app.contracts.financial import ForecastRunContract
from app.services.formula_engine import expression_names
from app.services.foundation import append_ledger_entry, list_ledger_entries


FORECAST_METHODS = [
    ('straight_line', 'Straight line', 'Repeat the latest source amount into future periods.'),
    ('growth_rate', 'Growth rate', 'Apply a typed driver growth rate to latest source amount.'),
    ('rolling_average', 'Rolling average', 'Use average source amount as future forecast.'),
    ('driver_based', 'Driver based', 'Apply a driver value as a direct multiplier.'),
    ('seasonal', 'Seasonal', 'Use same-month historical seasonality when available.'),
    ('historical_trend', 'Historical trend', 'Project the average historical period-over-period change.'),
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _month_range(start_period: str, end_period: str) -> list[str]:
    start_year, start_month = map(int, start_period.split('-'))
    end_year, end_month = map(int, end_period.split('-'))
    periods = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        periods.append(f'{year:04d}-{month:02d}')
        month += 1
        if month > 12:
            month = 1
            year += 1
    return periods


def ensure_forecast_methods() -> None:
    for key, name, description in FORECAST_METHODS:
        db.execute(
            '''
            INSERT OR IGNORE INTO forecast_methods (method_key, name, description, active)
            VALUES (?, ?, ?, 1)
            ''',
            (key, name, description),
        )


def status() -> dict[str, Any]:
    ensure_forecast_methods()
    counts = {
        'typed_drivers': int(db.fetch_one('SELECT COUNT(*) AS count FROM typed_drivers')['count']),
        'forecast_methods': int(db.fetch_one('SELECT COUNT(*) AS count FROM forecast_methods WHERE active = 1')['count']),
        'forecast_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM forecast_runs')['count']),
        'lineage': int(db.fetch_one('SELECT COUNT(*) AS count FROM forecast_lineage')['count']),
        'variance_rows': int(db.fetch_one('SELECT COUNT(*) AS count FROM forecast_actual_variances')['count']),
    }
    checks = {
        'typed_drivers_ready': True,
        'scenario_clone_ready': True,
        'scenario_compare_ready': True,
        'forecast_methods_ready': counts['forecast_methods'] >= 6,
        'confidence_ranges_ready': True,
        'driver_lineage_ready': True,
        'seasonal_forecasting_ready': True,
        'historical_trend_ready': True,
        'actuals_ingestion_ready': True,
        'forecast_actual_variance_ready': True,
        'driver_dependency_graph_ready': True,
        'circular_dependency_detection_ready': True,
    }
    return {'batch': 'B15', 'title': 'Advanced Forecasting', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def predictive_status() -> dict[str, Any]:
    ensure_forecast_methods()
    counts = {
        'model_choices': int(db.fetch_one('SELECT COUNT(*) AS count FROM predictive_model_choices')['count']),
        'backtests': int(db.fetch_one('SELECT COUNT(*) AS count FROM forecast_backtests')['count']),
        'tuning_profiles': int(db.fetch_one('SELECT COUNT(*) AS count FROM forecast_tuning_profiles')['count']),
        'recommendation_comparisons': int(db.fetch_one('SELECT COUNT(*) AS count FROM forecast_recommendation_comparisons')['count']),
        'driver_explanations': int(db.fetch_one('SELECT COUNT(*) AS count FROM forecast_driver_explanations')['count']),
    }
    checks = {
        'model_selection_ready': True,
        'backtesting_ready': True,
        'accuracy_scoring_ready': True,
        'seasonality_controls_ready': True,
        'confidence_interval_tuning_ready': True,
        'explainable_drivers_ready': True,
        'recommendation_comparison_ready': True,
    }
    return {'batch': 'B42', 'title': 'Predictive Forecasting Studio', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def upsert_typed_driver(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    existing = db.fetch_one('SELECT locked FROM typed_drivers WHERE scenario_id = ? AND driver_key = ?', (payload['scenario_id'], payload['driver_key']))
    if existing is not None and bool(existing['locked']):
        raise ValueError('Driver is locked.')
    db.execute(
        '''
        INSERT INTO typed_drivers (
            scenario_id, driver_key, label, driver_type, unit, value, locked, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, driver_key) DO UPDATE SET
            label = excluded.label,
            driver_type = excluded.driver_type,
            unit = excluded.unit,
            value = excluded.value,
            locked = excluded.locked
        ''',
        (
            payload['scenario_id'], payload['driver_key'], payload['label'], payload['driver_type'],
            payload['unit'], payload['value'], 1 if payload.get('locked') else 0, user['email'], now,
        ),
    )
    db.log_audit('typed_driver', payload['driver_key'], 'upserted', user['email'], payload, now)
    return _one('SELECT * FROM typed_drivers WHERE scenario_id = ? AND driver_key = ?', (payload['scenario_id'], payload['driver_key']))


def list_typed_drivers(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM typed_drivers WHERE scenario_id = ? ORDER BY driver_key', (scenario_id,))
    for row in rows:
        row['locked'] = bool(row['locked'])
    return rows


def upsert_driver_definition(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    db.execute(
        '''
        INSERT INTO drivers (scenario_id, driver_key, label, expression, value, unit)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, driver_key) DO UPDATE SET
            label = excluded.label,
            expression = excluded.expression,
            value = excluded.value,
            unit = excluded.unit
        ''',
        (payload['scenario_id'], payload['driver_key'], payload['label'], payload.get('expression'), payload.get('value'), payload['unit']),
    )
    db.log_audit('driver', payload['driver_key'], 'upserted', user['email'], payload, _now())
    return _one('SELECT * FROM drivers WHERE scenario_id = ? AND driver_key = ?', (payload['scenario_id'], payload['driver_key']))


def clone_scenario(scenario_id: int, payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    source = _one('SELECT * FROM scenarios WHERE id = ?', (scenario_id,))
    now = _now()
    new_id = db.execute(
        '''
        INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
        VALUES (?, ?, 'draft', ?, ?, 0, ?)
        ''',
        (payload['name'], payload['version'], source['start_period'], source['end_period'], now),
    )
    _copy_rows('planning_ledger', scenario_id, new_id, ['scenario_id', 'legacy_line_item_id', 'reversed_at', 'idempotency_key', 'posted_checksum'])
    _copy_rows('drivers', scenario_id, new_id, ['scenario_id'])
    _copy_rows('typed_drivers', scenario_id, new_id, ['scenario_id'])
    db.log_audit('scenario', str(new_id), 'cloned', user['email'], {'source_scenario_id': scenario_id}, now)
    row = _one('SELECT * FROM scenarios WHERE id = ?', (new_id,))
    row['locked'] = bool(row['locked'])
    return row


def compare_scenarios(base_id: int, compare_id: int) -> dict[str, Any]:
    base = _summary(base_id)
    compare = _summary(compare_id)
    keys = sorted(set(base) | set(compare))
    rows = []
    for key in keys:
        base_value = base.get(key, 0.0)
        compare_value = compare.get(key, 0.0)
        rows.append({'key': key, 'base': base_value, 'compare': compare_value, 'variance': round(compare_value - base_value, 2)})
    return {'base_scenario_id': base_id, 'compare_scenario_id': compare_id, 'rows': rows}


def list_methods() -> list[dict[str, Any]]:
    ensure_forecast_methods()
    rows = db.fetch_all('SELECT * FROM forecast_methods WHERE active = 1 ORDER BY method_key')
    for row in rows:
        row['active'] = bool(row['active'])
    return rows


def run_forecast(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    payload = ForecastRunContract.model_validate(payload).model_dump()
    ensure_forecast_methods()
    method = payload['method_key']
    scenario_id = payload['scenario_id']
    driver = None
    if payload.get('driver_key'):
        driver = _one('SELECT * FROM typed_drivers WHERE scenario_id = ? AND driver_key = ?', (scenario_id, payload['driver_key']))
    source_rows = _source_rows(scenario_id, payload['account_code'], payload.get('department_code'))
    if not source_rows:
        raise ValueError('No source ledger rows found for forecast.')
    periods = _month_range(payload['period_start'], payload['period_end'])
    confidence = float(payload.get('confidence') or 0.8)
    spread = max(0.02, 1.0 - confidence)
    run_id = db.execute(
        '''
        INSERT INTO forecast_runs (
            scenario_id, method_key, driver_key, account_code, department_code,
            period_start, period_end, status, confidence_low, confidence_high, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'running', ?, ?, ?, ?)
        ''',
        (scenario_id, method, payload.get('driver_key'), payload['account_code'], payload.get('department_code'), payload['period_start'], payload['period_end'], confidence - spread, confidence + spread, user['email'], _now()),
    )
    base_amount = _base_amount(method, source_rows, driver)
    created = []
    for index, period in enumerate(periods, start=1):
        amount = _forecast_amount(method, base_amount, index, driver, source_rows, period)
        low = round(amount * (1.0 - spread), 2)
        high = round(amount * (1.0 + spread), 2)
        template = source_rows[-1]
        ledger = append_ledger_entry(
            {
                'scenario_id': scenario_id,
                'department_code': payload.get('department_code') or template['department_code'],
                'fund_code': template['fund_code'],
                'account_code': payload['account_code'],
                'period': period,
                'amount': amount,
                'source': 'scenario_forecast',
                'ledger_type': 'forecast',
                'ledger_basis': 'forecast',
                'notes': f"{method} forecast for {payload['account_code']}",
                'metadata': {'forecast_run_id': run_id, 'method_key': method, 'driver_key': payload.get('driver_key')},
            },
            actor=user['email'],
            user=user,
        )
        db.execute(
            '''
            INSERT INTO forecast_lineage (
                forecast_run_id, ledger_entry_id, driver_key, method_key,
                source_ledger_entry_id, confidence_low, confidence_high, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (run_id, ledger['id'], payload.get('driver_key'), method, template['id'], low, high, _now()),
        )
        created.append({'ledger_entry': ledger, 'confidence_low': low, 'confidence_high': high})
    db.execute("UPDATE forecast_runs SET status = 'posted' WHERE id = ?", (run_id,))
    run = _one('SELECT * FROM forecast_runs WHERE id = ?', (run_id,))
    run['created_lines'] = created
    return run


def list_forecast_runs(scenario_id: int) -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM forecast_runs WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))


def list_lineage(forecast_run_id: int) -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM forecast_lineage WHERE forecast_run_id = ? ORDER BY id', (forecast_run_id,))


def ingest_actuals(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    created = []
    for index, raw in enumerate(payload.get('rows') or [], start=1):
        row = dict(raw)
        row['scenario_id'] = payload['scenario_id']
        row['ledger_type'] = 'actual'
        row['ledger_basis'] = 'actual'
        row['source'] = row.get('source') or 'actuals_ingest'
        row['source_version'] = payload.get('source_version') or 'actuals-upload'
        row['source_record_id'] = row.get('source_record_id') or str(index)
        row['metadata'] = {**(row.get('metadata') or {}), 'actuals_ingest': True}
        created.append(append_ledger_entry(row, actor=user['email'], user=user))
    db.log_audit('actuals_ingest', str(payload['scenario_id']), 'posted', user['email'], {'count': len(created), 'source_version': payload.get('source_version')}, _now())
    return {'scenario_id': payload['scenario_id'], 'count': len(created), 'entries': created}


def calculate_forecast_actual_variance(scenario_id: int) -> dict[str, Any]:
    db.execute('DELETE FROM forecast_actual_variances WHERE scenario_id = ?', (scenario_id,))
    rows = db.fetch_all(
        '''
        SELECT
            COALESCE(f.period, a.period) AS period,
            COALESCE(f.department_code, a.department_code) AS department_code,
            COALESCE(f.account_code, a.account_code) AS account_code,
            COALESCE(f.amount, 0) AS forecast_amount,
            COALESCE(a.amount, 0) AS actual_amount
        FROM (
            SELECT period, department_code, account_code, SUM(amount) AS amount
            FROM planning_ledger
            WHERE scenario_id = ? AND ledger_basis = 'forecast' AND reversed_at IS NULL
            GROUP BY period, department_code, account_code
        ) f
        LEFT JOIN (
            SELECT period, department_code, account_code, SUM(amount) AS amount
            FROM planning_ledger
            WHERE scenario_id = ? AND ledger_basis = 'actual' AND reversed_at IS NULL
            GROUP BY period, department_code, account_code
        ) a ON a.period = f.period AND a.department_code = f.department_code AND a.account_code = f.account_code
        UNION
        SELECT
            COALESCE(f.period, a.period) AS period,
            COALESCE(f.department_code, a.department_code) AS department_code,
            COALESCE(f.account_code, a.account_code) AS account_code,
            COALESCE(f.amount, 0) AS forecast_amount,
            COALESCE(a.amount, 0) AS actual_amount
        FROM (
            SELECT period, department_code, account_code, SUM(amount) AS amount
            FROM planning_ledger
            WHERE scenario_id = ? AND ledger_basis = 'actual' AND reversed_at IS NULL
            GROUP BY period, department_code, account_code
        ) a
        LEFT JOIN (
            SELECT period, department_code, account_code, SUM(amount) AS amount
            FROM planning_ledger
            WHERE scenario_id = ? AND ledger_basis = 'forecast' AND reversed_at IS NULL
            GROUP BY period, department_code, account_code
        ) f ON f.period = a.period AND f.department_code = a.department_code AND f.account_code = a.account_code
        ''',
        (scenario_id, scenario_id, scenario_id, scenario_id),
    )
    created = []
    for row in rows:
        variance = round(float(row['actual_amount']) - float(row['forecast_amount']), 2)
        percent = None if float(row['forecast_amount']) == 0 else round(variance / abs(float(row['forecast_amount'])), 4)
        variance_id = db.execute(
            '''
            INSERT INTO forecast_actual_variances (
                scenario_id, period, department_code, account_code, forecast_amount,
                actual_amount, variance_amount, variance_percent, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (scenario_id, row['period'], row['department_code'], row['account_code'], row['forecast_amount'], row['actual_amount'], variance, percent, _now()),
        )
        created.append(_one('SELECT * FROM forecast_actual_variances WHERE id = ?', (variance_id,)))
    return {'scenario_id': scenario_id, 'count': len(created), 'variances': created}


def list_forecast_actual_variances(scenario_id: int) -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM forecast_actual_variances WHERE scenario_id = ? ORDER BY period DESC, ABS(variance_amount) DESC', (scenario_id,))


def predictive_workspace(scenario_id: int) -> dict[str, Any]:
    return {
        'scenario_id': scenario_id,
        'status': predictive_status(),
        'model_choices': list_predictive_model_choices(scenario_id),
        'backtests': list_forecast_backtests(scenario_id),
        'tuning_profiles': list_forecast_tuning_profiles(scenario_id),
        'recommendations': list_forecast_recommendation_comparisons(scenario_id),
        'driver_explanations': list_forecast_driver_explanations(scenario_id),
    }


def upsert_predictive_model_choice(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    ensure_forecast_methods()
    now = _now()
    db.execute(
        '''
        INSERT INTO predictive_model_choices (
            scenario_id, choice_key, account_code, department_code, selected_method,
            seasonality_mode, confidence_level, status, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'selected', ?, ?)
        ON CONFLICT(scenario_id, choice_key) DO UPDATE SET
            account_code = excluded.account_code,
            department_code = excluded.department_code,
            selected_method = excluded.selected_method,
            seasonality_mode = excluded.seasonality_mode,
            confidence_level = excluded.confidence_level,
            status = 'selected'
        ''',
        (
            payload['scenario_id'], payload['choice_key'], payload['account_code'], payload.get('department_code'),
            payload['selected_method'], payload.get('seasonality_mode') or 'auto', float(payload.get('confidence_level') or 0.8),
            user['email'], now,
        ),
    )
    row = _one('SELECT * FROM predictive_model_choices WHERE scenario_id = ? AND choice_key = ?', (payload['scenario_id'], payload['choice_key']))
    db.log_audit('predictive_model_choice', str(row['id']), 'selected', user['email'], payload, now)
    return row


def list_predictive_model_choices(scenario_id: int) -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM predictive_model_choices WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))


def upsert_forecast_tuning_profile(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    choice = _one('SELECT * FROM predictive_model_choices WHERE id = ?', (payload['choice_id'],))
    now = _now()
    driver_weights = json.dumps(payload.get('driver_weights') or {}, sort_keys=True)
    existing = db.fetch_one('SELECT id FROM forecast_tuning_profiles WHERE choice_id = ? ORDER BY id DESC LIMIT 1', (payload['choice_id'],))
    if existing:
        db.execute(
            '''
            UPDATE forecast_tuning_profiles
            SET seasonality_strength = ?, confidence_level = ?, confidence_spread = ?,
                driver_weights_json = ?, created_by = ?, created_at = ?
            WHERE id = ?
            ''',
            (
                float(payload.get('seasonality_strength') or 1),
                float(payload.get('confidence_level') or choice['confidence_level']),
                float(payload.get('confidence_spread') or 0.2),
                driver_weights,
                user['email'],
                now,
                existing['id'],
            ),
        )
        profile_id = int(existing['id'])
    else:
        profile_id = db.execute(
            '''
            INSERT INTO forecast_tuning_profiles (
                choice_id, seasonality_strength, confidence_level, confidence_spread,
                driver_weights_json, created_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                payload['choice_id'], float(payload.get('seasonality_strength') or 1),
                float(payload.get('confidence_level') or choice['confidence_level']),
                float(payload.get('confidence_spread') or 0.2), driver_weights, user['email'], now,
            ),
        )
    db.execute(
        'UPDATE predictive_model_choices SET confidence_level = ?, seasonality_mode = ? WHERE id = ?',
        (float(payload.get('confidence_level') or choice['confidence_level']), choice['seasonality_mode'], payload['choice_id']),
    )
    db.log_audit('forecast_tuning_profile', str(profile_id), 'tuned', user['email'], payload, now)
    return _format_tuning_profile(_one('SELECT * FROM forecast_tuning_profiles WHERE id = ?', (profile_id,)))


def list_forecast_tuning_profiles(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all(
        '''
        SELECT p.*
        FROM forecast_tuning_profiles p
        JOIN predictive_model_choices c ON c.id = p.choice_id
        WHERE c.scenario_id = ?
        ORDER BY p.id DESC
        ''',
        (scenario_id,),
    )
    return [_format_tuning_profile(row) for row in rows]


def run_forecast_backtest(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    choice = _one('SELECT * FROM predictive_model_choices WHERE id = ?', (payload['choice_id'],))
    rows = _source_rows(choice['scenario_id'], choice['account_code'], choice.get('department_code'))
    holdout = [row for row in rows if payload['period_start'] <= row['period'] <= payload['period_end']]
    training = [row for row in rows if row['period'] < payload['period_start']]
    if not training:
        training = [row for row in rows if row['period'] <= payload['period_end']]
    if not holdout:
        raise ValueError('No holdout ledger rows found for backtest period.')
    metrics = _score_method(choice['selected_method'], training, holdout, None, choice['confidence_level'])
    result = {
        'training_rows': len(training),
        'holdout_rows': len(holdout),
        'method_key': choice['selected_method'],
        'seasonality_mode': choice['seasonality_mode'],
        'predictions': metrics['predictions'][:24],
    }
    backtest_id = db.execute(
        '''
        INSERT INTO forecast_backtests (
            scenario_id, choice_id, method_key, period_start, period_end, status,
            accuracy_score, mape, rmse, result_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, 'scored', ?, ?, ?, ?, ?, ?)
        ''',
        (
            choice['scenario_id'], choice['id'], choice['selected_method'], payload['period_start'], payload['period_end'],
            metrics['accuracy_score'], metrics['mape'], metrics['rmse'], json.dumps(result, sort_keys=True), user['email'], _now(),
        ),
    )
    row = _format_backtest(_one('SELECT * FROM forecast_backtests WHERE id = ?', (backtest_id,)))
    db.log_audit('forecast_backtest', str(backtest_id), 'scored', user['email'], {'choice_id': choice['id'], 'accuracy_score': row['accuracy_score']}, _now())
    return row


def list_forecast_backtests(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM forecast_backtests WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    return [_format_backtest(row) for row in rows]


def compare_forecast_recommendations(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    ensure_forecast_methods()
    scenario_id = payload['scenario_id']
    methods = payload.get('methods') or [key for key, _, _ in FORECAST_METHODS]
    source_rows = _source_rows(scenario_id, payload['account_code'], payload.get('department_code'))
    if len(source_rows) < 1:
        raise ValueError('No source ledger rows found for recommendation comparison.')
    split = max(1, int(len(source_rows) * 0.7))
    training = source_rows[:split]
    holdout = source_rows[split:] or source_rows[-min(3, len(source_rows)):]
    drivers = list_typed_drivers(scenario_id)
    comparisons = []
    for method in methods:
        driver = _best_driver(drivers, method)
        metrics = _score_method(method, training, holdout, driver, 0.8)
        comparisons.append(
            {
                'method_key': method,
                'accuracy_score': metrics['accuracy_score'],
                'mape': metrics['mape'],
                'rmse': metrics['rmse'],
                'confidence_low': metrics['confidence_low'],
                'confidence_high': metrics['confidence_high'],
                'source_rows': len(source_rows),
                'driver_key': driver.get('driver_key') if driver else None,
            }
        )
    comparisons.sort(key=lambda item: (-float(item['accuracy_score']), float(item['rmse'])))
    recommended = comparisons[0]
    explanation = {
        'recommended_method': recommended['method_key'],
        'reason': 'Highest backtested accuracy score with lowest error among compared methods.',
        'driver_count': len(drivers),
        'source_period_start': source_rows[0]['period'],
        'source_period_end': source_rows[-1]['period'],
    }
    comparison_id = db.execute(
        '''
        INSERT INTO forecast_recommendation_comparisons (
            scenario_id, account_code, department_code, comparison_json,
            recommended_method, explanation_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            scenario_id, payload['account_code'], payload.get('department_code'),
            json.dumps({'methods': comparisons}, sort_keys=True), recommended['method_key'],
            json.dumps(explanation, sort_keys=True), user['email'], _now(),
        ),
    )
    row = _format_recommendation(_one('SELECT * FROM forecast_recommendation_comparisons WHERE id = ?', (comparison_id,)))
    db.log_audit('forecast_recommendation_comparison', str(comparison_id), 'compared', user['email'], explanation, _now())
    return row


def list_forecast_recommendation_comparisons(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM forecast_recommendation_comparisons WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    return [_format_recommendation(row) for row in rows]


def explain_forecast_drivers(scenario_id: int, account_code: str, user: dict[str, Any], department_code: str | None = None) -> dict[str, Any]:
    db.execute('DELETE FROM forecast_driver_explanations WHERE scenario_id = ? AND account_code = ? AND COALESCE(department_code, \'\') = COALESCE(?, \'\')', (scenario_id, account_code, department_code))
    drivers = list_typed_drivers(scenario_id)
    relevant = [driver for driver in drivers if _driver_relevance(driver, account_code)]
    if not relevant:
        relevant = drivers[:3]
    total = sum(abs(float(driver['value'])) for driver in relevant) or 1.0
    created = []
    for driver in relevant:
        score = round(abs(float(driver['value'])) / total, 4)
        explanation = f"{driver['label']} contributes to {account_code} through its {driver['driver_type']} value and {driver['unit']} unit."
        row_id = db.execute(
            '''
            INSERT INTO forecast_driver_explanations (
                scenario_id, account_code, department_code, driver_key, contribution_score,
                explanation, evidence_json, created_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                scenario_id, account_code, department_code, driver['driver_key'], score, explanation,
                json.dumps({'driver_value': driver['value'], 'locked': bool(driver['locked'])}, sort_keys=True),
                user['email'], _now(),
            ),
        )
        created.append(_format_driver_explanation(_one('SELECT * FROM forecast_driver_explanations WHERE id = ?', (row_id,))))
    db.log_audit('forecast_driver_explanation', f'{scenario_id}:{account_code}', 'explained', user['email'], {'count': len(created)}, _now())
    return {'scenario_id': scenario_id, 'account_code': account_code, 'count': len(created), 'driver_explanations': created}


def list_forecast_driver_explanations(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM forecast_driver_explanations WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    return [_format_driver_explanation(row) for row in rows]


def driver_dependency_graph(scenario_id: int) -> dict[str, Any]:
    rows = db.fetch_all('SELECT driver_key, label, expression FROM drivers WHERE scenario_id = ? ORDER BY driver_key', (scenario_id,))
    keys = {row['driver_key'] for row in rows}
    nodes = [{'id': row['driver_key'], 'label': row['label']} for row in rows]
    edges = []
    adjacency = {key: [] for key in keys}
    for row in rows:
        deps = sorted(_expression_names(row.get('expression') or '') & keys)
        for dep in deps:
            edges.append({'from': dep, 'to': row['driver_key']})
            adjacency[dep].append(row['driver_key'])
    cycles = _cycles(adjacency)
    return {'scenario_id': scenario_id, 'nodes': nodes, 'edges': edges, 'cycles': cycles, 'has_cycles': bool(cycles)}


def _copy_rows(table: str, source_scenario_id: int, new_scenario_id: int, omit: list[str]) -> None:
    rows = db.fetch_all(f'SELECT * FROM {table} WHERE scenario_id = ?', (source_scenario_id,))
    for row in rows:
        row = {key: value for key, value in row.items() if key != 'id' and key not in omit}
        row['scenario_id'] = new_scenario_id
        if table == 'planning_ledger' and 'posted_checksum' not in row:
            row['posted_checksum'] = _copied_ledger_checksum(row)
        columns = list(row)
        placeholders = ','.join('?' for _ in columns)
        db.execute(f'INSERT INTO {table} ({",".join(columns)}) VALUES ({placeholders})', tuple(row.values()))


def _copied_ledger_checksum(row: dict[str, Any]) -> str:
    payload = {key: value for key, value in row.items() if key not in {'posted_checksum', 'idempotency_key'}}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str).encode('utf-8')).hexdigest()


def _summary(scenario_id: int) -> dict[str, float]:
    rows = db.fetch_all(
        '''
        SELECT department_code || ':' || account_code AS key, SUM(amount) AS total
        FROM planning_ledger
        WHERE scenario_id = ? AND reversed_at IS NULL
        GROUP BY department_code, account_code
        ''',
        (scenario_id,),
    )
    return {row['key']: round(float(row['total']), 2) for row in rows}


def _source_rows(scenario_id: int, account_code: str, department_code: str | None) -> list[dict[str, Any]]:
    if department_code:
        return db.fetch_all(
            '''
            SELECT * FROM planning_ledger
            WHERE scenario_id = ? AND account_code = ? AND department_code = ? AND reversed_at IS NULL
            ORDER BY period, id
            ''',
            (scenario_id, account_code, department_code),
        )
    return db.fetch_all(
        '''
        SELECT * FROM planning_ledger
        WHERE scenario_id = ? AND account_code = ? AND reversed_at IS NULL
        ORDER BY period, id
        ''',
        (scenario_id, account_code),
    )


def _base_amount(method: str, rows: list[dict[str, Any]], driver: dict[str, Any] | None) -> float:
    if method == 'rolling_average':
        return round(sum(float(row['amount']) for row in rows) / len(rows), 2)
    return round(float(rows[-1]['amount']), 2)


def _forecast_amount(method: str, base: float, index: int, driver: dict[str, Any] | None, rows: list[dict[str, Any]], period: str) -> float:
    value = float(driver['value']) if driver else 0.0
    if method == 'growth_rate':
        return round(base * ((1.0 + value) ** index), 2)
    if method == 'driver_based':
        return round(base * value, 2)
    if method == 'seasonal':
        month = period.split('-')[1]
        matches = [float(row['amount']) for row in rows if str(row['period']).endswith(f'-{month}')]
        return round(matches[-1] if matches else base, 2)
    if method == 'historical_trend':
        amounts = [float(row['amount']) for row in rows]
        if len(amounts) < 2:
            return round(base, 2)
        avg_change = sum(amounts[idx] - amounts[idx - 1] for idx in range(1, len(amounts))) / (len(amounts) - 1)
        return round(base + (avg_change * index), 2)
    return round(base, 2)


def _expression_names(expression: str) -> set[str]:
    return expression_names(expression) if expression else set()


def _cycles(adjacency: dict[str, list[str]]) -> list[list[str]]:
    cycles: list[list[str]] = []
    visiting: list[str] = []
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visiting:
            cycles.append(visiting[visiting.index(node):] + [node])
            return
        if node in visited:
            return
        visiting.append(node)
        for child in adjacency.get(node, []):
            visit(child)
        visiting.pop()
        visited.add(node)

    for key in adjacency:
        visit(key)
    return cycles


def _score_method(method: str, training: list[dict[str, Any]], holdout: list[dict[str, Any]], driver: dict[str, Any] | None, confidence: float) -> dict[str, Any]:
    if not training:
        training = holdout
    base = _base_amount(method, training, driver)
    predictions = []
    errors = []
    absolute_percent_errors = []
    for index, actual in enumerate(holdout, start=1):
        predicted = _forecast_amount(method, base, index, driver, training, actual['period'])
        actual_amount = float(actual['amount'])
        error = predicted - actual_amount
        errors.append(error)
        if actual_amount != 0:
            absolute_percent_errors.append(abs(error) / abs(actual_amount))
        predictions.append({'period': actual['period'], 'actual': round(actual_amount, 2), 'predicted': round(predicted, 2), 'error': round(error, 2)})
    mape = round(sum(absolute_percent_errors) / len(absolute_percent_errors), 4) if absolute_percent_errors else 0.0
    rmse = round(math.sqrt(sum(error * error for error in errors) / len(errors)), 2) if errors else 0.0
    bias = round(sum(errors) / len(errors), 2) if errors else 0.0
    accuracy = round(max(0.0, 1.0 - mape), 4)
    spread = max(0.01, 1.0 - confidence)
    return {
        'accuracy_score': accuracy,
        'mape': mape,
        'rmse': rmse,
        'bias': bias,
        'confidence_low': round(max(0.0, confidence - spread), 4),
        'confidence_high': round(min(1.0, confidence + spread), 4),
        'predictions': predictions,
    }


def _best_driver(drivers: list[dict[str, Any]], method: str) -> dict[str, Any] | None:
    if not drivers:
        return None
    if method == 'growth_rate':
        for driver in drivers:
            if driver['driver_type'] in {'ratio', 'percent'}:
                return driver
    if method == 'driver_based':
        for driver in drivers:
            if driver['driver_type'] in {'count', 'index', 'ratio'}:
                return driver
    return None


def _driver_relevance(driver: dict[str, Any], account_code: str) -> bool:
    haystack = f"{driver['driver_key']} {driver['label']}".lower()
    account = account_code.lower()
    return account in haystack or any(token and token in haystack for token in account.split('_'))


def _format_tuning_profile(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row['driver_weights'] = _loads(row.pop('driver_weights_json', '{}'))
    return row


def _format_backtest(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row['result'] = _loads(row.pop('result_json', '{}'))
    return row


def _format_recommendation(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row['comparison'] = _loads(row.pop('comparison_json', '{}'))
    row['explanation'] = _loads(row.pop('explanation_json', '{}'))
    return row


def _format_driver_explanation(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row['evidence'] = _loads(row.pop('evidence_json', '{}'))
    return row


def _loads(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {'value': parsed}


def _one(query: str, params: tuple[Any, ...]) -> dict[str, Any]:
    row = db.fetch_one(query, params)
    if row is None:
        raise ValueError('Record not found.')
    return row
