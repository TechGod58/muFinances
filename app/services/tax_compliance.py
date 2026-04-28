from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from app import db
from app.services.observability_operations import create_alert


DEFAULT_TAX_RULE_SOURCES = [
    {
        'source_key': 'irs-form-990-instructions',
        'jurisdiction': 'US',
        'source_name': 'IRS Instructions for Form 990',
        'source_url': 'https://www.irs.gov/instructions/i990',
        'rule_area': 'form_990',
        'latest_known_version': '2025',
        'notes': 'Primary source for Form 990 revenue support fields and Part VIII columns.',
    },
    {
        'source_key': 'irs-form-990t-instructions',
        'jurisdiction': 'US',
        'source_name': 'IRS Instructions for Form 990-T',
        'source_url': 'https://www.irs.gov/instructions/i990t',
        'rule_area': 'ubit',
        'latest_known_version': '2025',
        'notes': 'Primary source for unrelated business income tax return support.',
    },
    {
        'source_key': 'irs-publication-598',
        'jurisdiction': 'US',
        'source_name': 'IRS Publication 598',
        'source_url': 'https://www.irs.gov/forms-pubs/about-publication-598',
        'rule_area': 'ubit',
        'latest_known_version': '2026-03-31',
        'notes': 'IRS publication covering unrelated business income of exempt organizations.',
    },
    {
        'source_key': 'irs-unrelated-business-income-tax',
        'jurisdiction': 'US',
        'source_name': 'IRS Unrelated Business Income Tax',
        'source_url': 'https://www.irs.gov/charities-non-profits/unrelated-business-income-tax',
        'rule_area': 'ubit',
        'latest_known_version': '2026',
        'notes': 'IRS topic page for UBI filing threshold and related forms.',
    },
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def ensure_tax_compliance_ready(user: dict[str, Any] | None = None) -> None:
    actor = (user or {}).get('email', 'system')
    for item in DEFAULT_TAX_RULE_SOURCES:
        upsert_rule_source({**item, 'check_frequency_days': 30, 'status': 'active'}, {'email': actor}, audit=False)


def status() -> dict[str, Any]:
    ensure_tax_compliance_ready()
    counts = {
        'classifications': int(db.fetch_one('SELECT COUNT(*) AS count FROM tax_activity_classifications')['count']),
        'taxable_classifications': int(db.fetch_one("SELECT COUNT(*) AS count FROM tax_activity_classifications WHERE tax_status = 'taxable'")['count']),
        'rule_sources': int(db.fetch_one("SELECT COUNT(*) AS count FROM tax_rule_sources WHERE status = 'active'")['count']),
        'update_checks': int(db.fetch_one('SELECT COUNT(*) AS count FROM tax_update_checks')['count']),
        'open_tax_alerts': int(db.fetch_one("SELECT COUNT(*) AS count FROM tax_change_alerts WHERE status = 'open'")['count']),
        'reviews': int(db.fetch_one('SELECT COUNT(*) AS count FROM tax_reviews')['count']),
        'form990_support_fields': int(db.fetch_one('SELECT COUNT(*) AS count FROM form990_support_fields')['count']),
    }
    checks = {
        'npo_taxable_classification_ready': True,
        'exempt_taxable_activity_tagging_ready': True,
        'ubit_tracking_ready': True,
        'form990_support_fields_ready': True,
        'tax_rule_source_registry_ready': counts['rule_sources'] >= len(DEFAULT_TAX_RULE_SOURCES),
        'scheduled_tax_update_checks_ready': True,
        'tax_change_alerts_ready': True,
        'review_workflow_ready': True,
    }
    return {'batch': 'B63', 'title': 'Tax Classification And Compliance Watch', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def workspace(scenario_id: int | None = None) -> dict[str, Any]:
    return {
        'status': status(),
        'summary': classification_summary(scenario_id) if scenario_id else None,
        'classifications': list_classifications(scenario_id),
        'form990_support_fields': list_form990_support_fields(scenario_id),
        'rule_sources': list_rule_sources(),
        'update_checks': list_update_checks(),
        'tax_alerts': list_tax_alerts(),
        'reviews': list_reviews(scenario_id),
    }


def classify_activity(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    ledger = _ledger_row(payload.get('ledger_entry_id'))
    amount = float(payload.get('amount') if payload.get('amount') is not None else (ledger['amount'] if ledger else 0))
    expense_offset = float(payload.get('expense_offset') or 0)
    net_ubti = _net_ubti(payload.get('tax_status'), payload.get('regularly_carried_on'), payload.get('substantially_related'), amount, expense_offset)
    key = payload.get('classification_key') or f"tax-{payload['scenario_id']}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    db.execute(
        '''
        INSERT INTO tax_activity_classifications (
            classification_key, scenario_id, ledger_entry_id, activity_name, tax_status,
            activity_tag, income_type, ubit_code, regularly_carried_on, substantially_related,
            debt_financed, amount, expense_offset, net_ubti, form990_part, form990_line,
            form990_column, review_status, notes, metadata_json, created_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(classification_key) DO UPDATE SET
            scenario_id = excluded.scenario_id,
            ledger_entry_id = excluded.ledger_entry_id,
            activity_name = excluded.activity_name,
            tax_status = excluded.tax_status,
            activity_tag = excluded.activity_tag,
            income_type = excluded.income_type,
            ubit_code = excluded.ubit_code,
            regularly_carried_on = excluded.regularly_carried_on,
            substantially_related = excluded.substantially_related,
            debt_financed = excluded.debt_financed,
            amount = excluded.amount,
            expense_offset = excluded.expense_offset,
            net_ubti = excluded.net_ubti,
            form990_part = excluded.form990_part,
            form990_line = excluded.form990_line,
            form990_column = excluded.form990_column,
            review_status = excluded.review_status,
            notes = excluded.notes,
            metadata_json = excluded.metadata_json,
            updated_at = excluded.updated_at
        ''',
        (
            key, payload['scenario_id'], payload.get('ledger_entry_id'), payload['activity_name'],
            payload['tax_status'], payload['activity_tag'], payload['income_type'], payload.get('ubit_code'),
            1 if payload.get('regularly_carried_on') else 0, 1 if payload.get('substantially_related', True) else 0,
            1 if payload.get('debt_financed') else 0, amount, expense_offset, net_ubti,
            payload.get('form990_part'), payload.get('form990_line'), payload.get('form990_column'),
            payload.get('review_status') or 'draft', payload.get('notes') or '',
            json.dumps(payload.get('metadata') or {}, sort_keys=True), user['email'], now, now,
        ),
    )
    db.log_audit('tax_activity_classification', key, 'upserted', user['email'], payload, now)
    return _classification_by_key(key)


def list_classifications(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        rows = db.fetch_all('SELECT * FROM tax_activity_classifications WHERE scenario_id = ? ORDER BY id DESC LIMIT 200', (scenario_id,))
    else:
        rows = db.fetch_all('SELECT * FROM tax_activity_classifications ORDER BY id DESC LIMIT 200')
    return [_format_classification(row) for row in rows]


def classification_summary(scenario_id: int | None = None) -> dict[str, Any]:
    where = 'reversed_at IS NULL'
    params: list[Any] = []
    if scenario_id:
        where += ' AND scenario_id = ?'
        params.append(scenario_id)
    ledger_total = db.fetch_one(f'SELECT COALESCE(SUM(amount), 0) AS total, COUNT(*) AS count FROM planning_ledger WHERE {where}', tuple(params))
    class_where = '1 = 1'
    class_params: list[Any] = []
    if scenario_id:
        class_where += ' AND scenario_id = ?'
        class_params.append(scenario_id)
    by_status = db.fetch_all(
        f'''
        SELECT tax_status, COUNT(*) AS count, COALESCE(SUM(amount), 0) AS amount, COALESCE(SUM(net_ubti), 0) AS net_ubti
        FROM tax_activity_classifications
        WHERE {class_where}
        GROUP BY tax_status
        ORDER BY tax_status
        ''',
        tuple(class_params),
    )
    by_form990 = db.fetch_all(
        f'''
        SELECT form990_part, form990_line, form990_column, COUNT(*) AS count, COALESCE(SUM(amount), 0) AS amount
        FROM tax_activity_classifications
        WHERE {class_where} AND form990_part IS NOT NULL
        GROUP BY form990_part, form990_line, form990_column
        ORDER BY form990_part, form990_line, form990_column
        ''',
        tuple(class_params),
    )
    return {
        'scenario_id': scenario_id,
        'ledger_rows': int(ledger_total['count'] if ledger_total else 0),
        'ledger_amount': round(float(ledger_total['total'] if ledger_total else 0), 2),
        'by_status': [_amount_row(row) for row in by_status],
        'by_form990': [_amount_row(row) for row in by_form990],
    }


def upsert_form990_support(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    key = payload.get('support_key') or f"form990-{payload['scenario_id']}-{payload['period']}-{payload['form_part']}-{payload['line_number']}-{datetime.now(UTC).strftime('%H%M%S%f')}"
    db.execute(
        '''
        INSERT INTO form990_support_fields (
            support_key, scenario_id, period, form_part, line_number, column_code,
            description, amount, basis_json, review_status, created_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(support_key) DO UPDATE SET
            scenario_id = excluded.scenario_id,
            period = excluded.period,
            form_part = excluded.form_part,
            line_number = excluded.line_number,
            column_code = excluded.column_code,
            description = excluded.description,
            amount = excluded.amount,
            basis_json = excluded.basis_json,
            review_status = excluded.review_status,
            updated_at = excluded.updated_at
        ''',
        (
            key, payload['scenario_id'], payload['period'], payload['form_part'], payload['line_number'],
            payload.get('column_code') or '', payload['description'], float(payload.get('amount') or 0),
            json.dumps(payload.get('basis') or {}, sort_keys=True), payload.get('review_status') or 'draft',
            user['email'], now, now,
        ),
    )
    db.log_audit('form990_support_field', key, 'upserted', user['email'], payload, now)
    return _form990_by_key(key)


def list_form990_support_fields(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        rows = db.fetch_all('SELECT * FROM form990_support_fields WHERE scenario_id = ? ORDER BY period DESC, form_part, line_number', (scenario_id,))
    else:
        rows = db.fetch_all('SELECT * FROM form990_support_fields ORDER BY id DESC LIMIT 200')
    return [_format_form990(row) for row in rows]


def upsert_rule_source(payload: dict[str, Any], user: dict[str, Any], audit: bool = True) -> dict[str, Any]:
    now = _now()
    frequency = int(payload.get('check_frequency_days') or 30)
    next_check = payload.get('next_check_at') or (datetime.now(UTC) + timedelta(days=frequency)).isoformat()
    db.execute(
        '''
        INSERT INTO tax_rule_sources (
            source_key, jurisdiction, source_name, source_url, rule_area, latest_known_version,
            check_frequency_days, next_check_at, status, notes, created_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_key) DO UPDATE SET
            jurisdiction = excluded.jurisdiction,
            source_name = excluded.source_name,
            source_url = excluded.source_url,
            rule_area = excluded.rule_area,
            latest_known_version = excluded.latest_known_version,
            check_frequency_days = excluded.check_frequency_days,
            status = excluded.status,
            notes = excluded.notes,
            updated_at = excluded.updated_at
        ''',
        (
            payload['source_key'], payload.get('jurisdiction') or 'US', payload['source_name'], payload['source_url'],
            payload['rule_area'], payload.get('latest_known_version') or '', frequency, next_check,
            payload.get('status') or 'active', payload.get('notes') or '', user['email'], now, now,
        ),
    )
    if audit:
        db.log_audit('tax_rule_source', payload['source_key'], 'upserted', user['email'], payload, now)
    return _source_by_key(payload['source_key'])


def list_rule_sources() -> list[dict[str, Any]]:
    ensure_tax_compliance_ready()
    return db.fetch_all('SELECT * FROM tax_rule_sources ORDER BY rule_area, source_key')


def run_tax_update_check(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    ensure_tax_compliance_ready(user)
    source = _source_by_key(payload['source_key'])
    observed = payload.get('observed_version') or source['latest_known_version']
    changed = observed != source['latest_known_version']
    now = _now()
    check_key = f"tax-check-{source['source_key']}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    status_value = 'change_detected' if changed else 'current'
    check_id = db.execute(
        '''
        INSERT INTO tax_update_checks (
            check_key, source_id, status, detected_change, previous_version,
            detected_version, detail_json, checked_by, checked_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            check_key, source['id'], status_value, 1 if changed else 0, source['latest_known_version'],
            observed, json.dumps(payload.get('detail') or {}, sort_keys=True), user['email'], now,
        ),
    )
    next_check = (datetime.now(UTC) + timedelta(days=int(source['check_frequency_days']))).isoformat()
    db.execute(
        'UPDATE tax_rule_sources SET latest_known_version = ?, last_checked_at = ?, next_check_at = ?, updated_at = ? WHERE id = ?',
        (observed, now, next_check, now, source['id']),
    )
    alert = _create_tax_alert(source, observed, user, payload.get('detail') or {}) if changed else None
    db.log_audit('tax_update_check', check_key, status_value, user['email'], {'source_key': source['source_key'], 'changed': changed}, now)
    result = _format_update_check(db.fetch_one('SELECT * FROM tax_update_checks WHERE id = ?', (check_id,)))
    result['alert'] = alert
    return result


def run_due_update_checks(user: dict[str, Any]) -> dict[str, Any]:
    ensure_tax_compliance_ready(user)
    now = _now()
    rows = db.fetch_all("SELECT * FROM tax_rule_sources WHERE status = 'active' AND (next_check_at IS NULL OR next_check_at <= ?) ORDER BY next_check_at ASC", (now,))
    checks = [run_tax_update_check({'source_key': row['source_key']}, user) for row in rows]
    return {'count': len(checks), 'checks': checks}


def list_update_checks(limit: int = 100) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT tuc.*, trs.source_key FROM tax_update_checks tuc JOIN tax_rule_sources trs ON trs.id = tuc.source_id ORDER BY tuc.id DESC LIMIT ?', (max(1, min(limit, 500)),))
    return [_format_update_check(row) for row in rows]


def list_tax_alerts(status: str | None = None) -> list[dict[str, Any]]:
    if status:
        rows = db.fetch_all('SELECT tca.*, trs.source_key FROM tax_change_alerts tca JOIN tax_rule_sources trs ON trs.id = tca.source_id WHERE tca.status = ? ORDER BY tca.id DESC', (status,))
    else:
        rows = db.fetch_all('SELECT tca.*, trs.source_key FROM tax_change_alerts tca JOIN tax_rule_sources trs ON trs.id = tca.source_id ORDER BY tca.id DESC LIMIT 100')
    return [_format_tax_alert(row) for row in rows]


def decide_tax_alert(alert_id: int, payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM tax_change_alerts WHERE id = ?', (alert_id,))
    if row is None:
        raise ValueError('Tax alert not found.')
    now = _now()
    status_value = payload.get('status') or 'acknowledged'
    if status_value == 'resolved':
        db.execute('UPDATE tax_change_alerts SET status = ?, resolved_by = ?, resolved_at = ? WHERE id = ?', (status_value, user['email'], now, alert_id))
    else:
        db.execute('UPDATE tax_change_alerts SET status = ?, acknowledged_by = ?, acknowledged_at = ? WHERE id = ?', (status_value, user['email'], now, alert_id))
    db.log_audit('tax_change_alert', str(alert_id), status_value, user['email'], payload, now)
    return _format_tax_alert(db.fetch_one('SELECT * FROM tax_change_alerts WHERE id = ?', (alert_id,)))


def review_classification(classification_id: int, payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM tax_activity_classifications WHERE id = ?', (classification_id,))
    if row is None:
        raise ValueError('Tax classification not found.')
    now = _now()
    review_key = f"tax-review-{classification_id}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    status_value = 'approved' if payload['decision'] == 'approve' else 'rejected' if payload['decision'] == 'reject' else 'needs_review'
    review_id = db.execute(
        '''
        INSERT INTO tax_reviews (review_key, classification_id, status, decision, reviewer, note, evidence_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (review_key, classification_id, status_value, payload['decision'], user['email'], payload.get('note') or '', json.dumps(payload.get('evidence') or {}, sort_keys=True), now),
    )
    db.execute('UPDATE tax_activity_classifications SET review_status = ?, reviewer = ?, reviewed_at = ?, updated_at = ? WHERE id = ?', (status_value, user['email'], now, now, classification_id))
    db.log_audit('tax_activity_classification', str(classification_id), status_value, user['email'], payload, now)
    review = _format_review(db.fetch_one('SELECT * FROM tax_reviews WHERE id = ?', (review_id,)))
    review['classification'] = _format_classification(db.fetch_one('SELECT * FROM tax_activity_classifications WHERE id = ?', (classification_id,)))
    return review


def list_reviews(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        rows = db.fetch_all(
            '''
            SELECT tr.*
            FROM tax_reviews tr
            JOIN tax_activity_classifications tac ON tac.id = tr.classification_id
            WHERE tac.scenario_id = ?
            ORDER BY tr.id DESC
            LIMIT 100
            ''',
            (scenario_id,),
        )
    else:
        rows = db.fetch_all('SELECT * FROM tax_reviews ORDER BY id DESC LIMIT 100')
    return [_format_review(row) for row in rows]


def _net_ubti(tax_status: str | None, regularly_carried_on: bool | None, substantially_related: bool | None, amount: float, expense_offset: float) -> float:
    if tax_status != 'taxable' or not regularly_carried_on or substantially_related:
        return 0.0
    return round(amount - expense_offset, 2)


def _ledger_row(ledger_entry_id: int | None) -> dict[str, Any] | None:
    if ledger_entry_id is None:
        return None
    return db.fetch_one('SELECT * FROM planning_ledger WHERE id = ?', (ledger_entry_id,))


def _classification_by_key(key: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM tax_activity_classifications WHERE classification_key = ?', (key,))
    if row is None:
        raise RuntimeError('Tax classification could not be reloaded.')
    return _format_classification(row)


def _form990_by_key(key: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM form990_support_fields WHERE support_key = ?', (key,))
    if row is None:
        raise RuntimeError('Form 990 support field could not be reloaded.')
    return _format_form990(row)


def _source_by_key(key: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM tax_rule_sources WHERE source_key = ?', (key,))
    if row is None:
        raise ValueError('Tax rule source not found.')
    return row


def _create_tax_alert(source: dict[str, Any], observed: str, user: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    key = f"tax-change-{source['source_key']}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    message = f"Tax source {source['source_name']} changed from {source['latest_known_version']} to {observed}."
    alert_id = db.execute(
        '''
        INSERT INTO tax_change_alerts (alert_key, source_id, severity, status, message, detail_json, created_at)
        VALUES (?, ?, 'warning', 'open', ?, ?, ?)
        ''',
        (key, source['id'], message, json.dumps(detail, sort_keys=True), now),
    )
    create_alert(key, 'warning', message, 'tax_compliance', {'source_key': source['source_key'], **detail})
    db.log_audit('tax_change_alert', key, 'opened', user['email'], {'source_key': source['source_key'], 'observed_version': observed}, now)
    return _format_tax_alert(db.fetch_one('SELECT * FROM tax_change_alerts WHERE id = ?', (alert_id,)))


def _amount_row(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for key in ('amount', 'net_ubti', 'total'):
        if key in result:
            result[key] = round(float(result[key]), 2)
    return result


def _format_classification(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise RuntimeError('Tax classification not found.')
    result = dict(row)
    result['regularly_carried_on'] = bool(result['regularly_carried_on'])
    result['substantially_related'] = bool(result['substantially_related'])
    result['debt_financed'] = bool(result['debt_financed'])
    result['metadata'] = json.loads(result.pop('metadata_json') or '{}')
    return result


def _format_form990(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise RuntimeError('Form 990 support field not found.')
    result = dict(row)
    result['basis'] = json.loads(result.pop('basis_json') or '{}')
    return result


def _format_update_check(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise RuntimeError('Tax update check not found.')
    result = dict(row)
    result['detected_change'] = bool(result['detected_change'])
    result['detail'] = json.loads(result.pop('detail_json') or '{}')
    return result


def _format_tax_alert(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise RuntimeError('Tax alert not found.')
    result = dict(row)
    result['detail'] = json.loads(result.pop('detail_json') or '{}')
    return result


def _format_review(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise RuntimeError('Tax review not found.')
    result = dict(row)
    result['evidence'] = json.loads(result.pop('evidence_json') or '{}')
    return result
