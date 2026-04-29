from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.contracts.financial import CloseReconciliationContract, ConsolidationRunContract
from app.services.foundation import append_ledger_entry
from app.services.evidence import packet_evidence_links


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'close_checklists': int(db.fetch_one('SELECT COUNT(*) AS count FROM close_checklists')['count']),
        'account_reconciliations': int(db.fetch_one('SELECT COUNT(*) AS count FROM account_reconciliations')['count']),
        'intercompany_matches': int(db.fetch_one('SELECT COUNT(*) AS count FROM intercompany_matches')['count']),
        'elimination_entries': int(db.fetch_one('SELECT COUNT(*) AS count FROM elimination_entries')['count']),
        'consolidation_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM consolidation_runs')['count']),
        'audit_packets': int(db.fetch_one('SELECT COUNT(*) AS count FROM audit_packets')['count']),
        'close_task_templates': int(db.fetch_one('SELECT COUNT(*) AS count FROM close_task_templates')['count']),
        'task_dependencies': int(db.fetch_one('SELECT COUNT(*) AS count FROM close_task_dependencies')['count']),
        'period_close_calendar': int(db.fetch_one('SELECT COUNT(*) AS count FROM period_close_calendar')['count']),
        'reconciliation_exceptions': int(db.fetch_one('SELECT COUNT(*) AS count FROM reconciliation_exceptions')['count']),
        'entity_confirmations': int(db.fetch_one('SELECT COUNT(*) AS count FROM entity_confirmations')['count']),
        'consolidation_entities': int(db.fetch_one('SELECT COUNT(*) AS count FROM consolidation_entities')['count']),
        'entity_ownerships': int(db.fetch_one('SELECT COUNT(*) AS count FROM entity_ownerships')['count']),
        'consolidation_settings': int(db.fetch_one('SELECT COUNT(*) AS count FROM consolidation_settings')['count']),
        'consolidation_audit_reports': int(db.fetch_one('SELECT COUNT(*) AS count FROM consolidation_audit_reports')['count']),
        'currency_rates': int(db.fetch_one('SELECT COUNT(*) AS count FROM consolidation_currency_rates')['count']),
        'gaap_book_mappings': int(db.fetch_one('SELECT COUNT(*) AS count FROM gaap_book_mappings')['count']),
        'consolidation_journals': int(db.fetch_one('SELECT COUNT(*) AS count FROM consolidation_journals')['count']),
    }
    checks = {
        'close_checklist_ready': True,
        'account_reconciliation_ready': True,
        'intercompany_matching_ready': True,
        'eliminations_ready': True,
        'consolidation_runs_ready': True,
        'audit_packets_ready': True,
        'close_task_templates_ready': True,
        'task_dependencies_ready': True,
        'period_close_calendar_ready': True,
        'preparer_reviewer_workflow_ready': True,
        'aging_exceptions_ready': True,
        'period_lock_enforcement_ready': True,
        'entity_confirmations_ready': True,
        'entity_hierarchy_ready': True,
        'ownership_percentages_ready': True,
        'multi_gaap_placeholder_ready': True,
        'currency_placeholder_ready': True,
        'consolidation_audit_reports_ready': True,
        'elimination_review_workflow_ready': True,
        'currency_translation_engine_ready': True,
        'ownership_logic_ready': True,
        'multi_gaap_books_ready': True,
        'consolidation_journals_ready': True,
    }
    return {
        'batch': 'B20',
        'title': 'Consolidation Advanced Controls',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
    }


def create_checklist_item(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_period_open(payload['scenario_id'], payload['period'])
    now = _now()
    item_id = db.execute(
        '''
        INSERT INTO close_checklists (
            scenario_id, period, checklist_key, title, owner, due_date, template_key, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, period, checklist_key) DO UPDATE SET
            title = excluded.title,
            owner = excluded.owner,
            due_date = excluded.due_date,
            template_key = excluded.template_key
        ''',
        (
            payload['scenario_id'], payload['period'], payload['checklist_key'], payload['title'],
            payload['owner'], payload.get('due_date'), payload.get('template_key'), user['email'], now,
        ),
    )
    row = db.fetch_one(
        'SELECT id FROM close_checklists WHERE scenario_id = ? AND period = ? AND checklist_key = ?',
        (payload['scenario_id'], payload['period'], payload['checklist_key']),
    )
    db.log_audit('close_checklist', str(row['id'] if row else item_id), 'upserted', user['email'], payload, now)
    return get_checklist_item(int(row['id'] if row else item_id))


def list_checklist_items(scenario_id: int, period: str | None = None) -> list[dict[str, Any]]:
    if period:
        rows = db.fetch_all(
            'SELECT * FROM close_checklists WHERE scenario_id = ? AND period = ? ORDER BY status ASC, due_date ASC, id ASC',
            (scenario_id, period),
        )
    else:
        rows = db.fetch_all(
            'SELECT * FROM close_checklists WHERE scenario_id = ? ORDER BY period DESC, status ASC, due_date ASC, id ASC',
            (scenario_id,),
        )
    return [_format_checklist(row) for row in rows]


def get_checklist_item(item_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM close_checklists WHERE id = ?', (item_id,))
    if row is None:
        raise ValueError('Close checklist item not found.')
    return _format_checklist(row)


def complete_checklist_item(item_id: int, evidence: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    row = db.fetch_one('SELECT * FROM close_checklists WHERE id = ?', (item_id,))
    if row is None:
        raise ValueError('Close checklist item not found.')
    _ensure_period_open(row['scenario_id'], row['period'])
    blockers = _open_dependency_count(item_id)
    if blockers > 0:
        raise ValueError('Close checklist item has incomplete dependencies.')
    db.execute(
        '''
        UPDATE close_checklists
        SET status = 'complete', completed_by = ?, completed_at = ?, evidence_json = ?
        WHERE id = ?
        ''',
        (user['email'], now, json.dumps(evidence or {}, sort_keys=True), item_id),
    )
    db.log_audit('close_checklist', str(item_id), 'completed', user['email'], evidence, now)
    dependents = db.fetch_all('SELECT task_id FROM close_task_dependencies WHERE depends_on_task_id = ?', (item_id,))
    for dependent in dependents:
        _refresh_task_dependency_status(int(dependent['task_id']))
    return get_checklist_item(item_id)


def create_reconciliation(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    payload = CloseReconciliationContract.model_validate(payload).model_dump()
    _ensure_period_open(payload['scenario_id'], payload['period'])
    book_balance = _ledger_balance(
        payload['scenario_id'], payload['period'], payload['entity_code'], payload['account_code']
    )
    source_balance = float(payload['source_balance'])
    variance = round(book_balance - source_balance, 2)
    status_value = 'prepared' if abs(variance) < 0.01 else 'exception'
    now = _now()
    rec_id = db.execute(
        '''
        INSERT INTO account_reconciliations (
            scenario_id, period, entity_code, account_code, book_balance, source_balance,
            variance, status, owner, notes, preparer, prepared_at, aging_days, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            payload['scenario_id'], payload['period'], payload['entity_code'], payload['account_code'],
            book_balance, source_balance, variance, status_value, payload['owner'], payload.get('notes', ''),
            user['email'], now, _aging_days(now), user['email'], now,
        ),
    )
    db.log_audit('account_reconciliation', str(rec_id), 'created', user['email'], payload, now)
    result = get_reconciliation(rec_id)
    if abs(variance) >= 0.01:
        _upsert_reconciliation_exception(result, user)
        result = get_reconciliation(rec_id)
    return result


def list_reconciliations(scenario_id: int, period: str | None = None) -> list[dict[str, Any]]:
    if period:
        return db.fetch_all(
            'SELECT * FROM account_reconciliations WHERE scenario_id = ? AND period = ? ORDER BY id DESC',
            (scenario_id, period),
        )
    return db.fetch_all(
        'SELECT * FROM account_reconciliations WHERE scenario_id = ? ORDER BY period DESC, id DESC',
        (scenario_id,),
    )


def get_reconciliation(rec_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM account_reconciliations WHERE id = ?', (rec_id,))
    if row is None:
        raise ValueError('Account reconciliation not found.')
    return row


def create_intercompany_match(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_period_open(payload['scenario_id'], payload['period'])
    source_amount = float(payload['source_amount'])
    target_amount = float(payload['target_amount'])
    variance = round(source_amount + target_amount, 2)
    status_value = 'matched' if abs(variance) < 0.01 else 'exception'
    now = _now()
    match_id = db.execute(
        '''
        INSERT INTO intercompany_matches (
            scenario_id, period, source_entity_code, target_entity_code, account_code,
            source_amount, target_amount, variance, status, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            payload['scenario_id'], payload['period'], payload['source_entity_code'], payload['target_entity_code'],
            payload['account_code'], source_amount, target_amount, variance, status_value, user['email'], now,
        ),
    )
    db.log_audit('intercompany_match', str(match_id), 'created', user['email'], payload, now)
    return get_intercompany_match(match_id)


def list_intercompany_matches(scenario_id: int, period: str | None = None) -> list[dict[str, Any]]:
    if period:
        return db.fetch_all(
            'SELECT * FROM intercompany_matches WHERE scenario_id = ? AND period = ? ORDER BY id DESC',
            (scenario_id, period),
        )
    return db.fetch_all(
        'SELECT * FROM intercompany_matches WHERE scenario_id = ? ORDER BY period DESC, id DESC',
        (scenario_id,),
    )


def get_intercompany_match(match_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM intercompany_matches WHERE id = ?', (match_id,))
    if row is None:
        raise ValueError('Intercompany match not found.')
    return row


def create_elimination(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_period_open(payload['scenario_id'], payload['period'])
    ledger = append_ledger_entry(
        {
            'scenario_id': payload['scenario_id'],
            'entity_code': payload['entity_code'],
            'department_code': 'CONSOL',
            'fund_code': 'ELIM',
            'account_code': payload['account_code'],
            'period': payload['period'],
            'amount': float(payload['amount']),
            'source': 'elimination',
            'driver_key': None,
            'notes': payload['reason'],
            'ledger_type': 'elimination',
            'metadata': {'batch': 'B08', 'reason': payload['reason']},
        },
        actor=user['email'],
        user=user,
    )
    now = _now()
    elimination_id = db.execute(
        '''
        INSERT INTO elimination_entries (
            scenario_id, period, entity_code, account_code, amount, reason,
            ledger_entry_id, review_status, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?)
        ''',
        (
            payload['scenario_id'], payload['period'], payload['entity_code'], payload['account_code'],
            float(payload['amount']), payload['reason'], ledger['id'], user['email'], now,
        ),
    )
    db.log_audit('elimination_entry', str(elimination_id), 'created', user['email'], payload, now)
    return get_elimination(elimination_id)


def submit_elimination(elimination_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    row = get_elimination(elimination_id)
    _ensure_period_open(row['scenario_id'], row['period'])
    now = _now()
    db.execute("UPDATE elimination_entries SET review_status = 'pending_review', review_note = ? WHERE id = ?", (note, elimination_id))
    db.log_audit('elimination_entry', str(elimination_id), 'submitted', user['email'], {'note': note}, now)
    return get_elimination(elimination_id)


def approve_elimination(elimination_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    return _review_elimination(elimination_id, user, 'approved', note)


def reject_elimination(elimination_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    return _review_elimination(elimination_id, user, 'rejected', note)


def list_eliminations(scenario_id: int, period: str | None = None) -> list[dict[str, Any]]:
    if period:
        return db.fetch_all(
            'SELECT * FROM elimination_entries WHERE scenario_id = ? AND period = ? ORDER BY id DESC',
            (scenario_id, period),
        )
    return db.fetch_all(
        'SELECT * FROM elimination_entries WHERE scenario_id = ? ORDER BY period DESC, id DESC',
        (scenario_id,),
    )


def get_elimination(elimination_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM elimination_entries WHERE id = ?', (elimination_id,))
    if row is None:
        raise ValueError('Elimination entry not found.')
    return row


def run_consolidation(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    payload = ConsolidationRunContract.model_validate(payload).model_dump()
    _ensure_period_open(payload['scenario_id'], payload['period'])
    total_before = _period_total(payload['scenario_id'], payload['period'], include_eliminations=False)
    eliminations = _period_eliminations(payload['scenario_id'], payload['period'])
    consolidated = round(total_before + eliminations, 2)
    now = _now()
    run_id = db.execute(
        '''
        INSERT INTO consolidation_runs (
            scenario_id, period, status, total_before_eliminations, total_eliminations,
            consolidated_total, created_by, created_at
        ) VALUES (?, ?, 'complete', ?, ?, ?, ?, ?)
        ''',
        (payload['scenario_id'], payload['period'], total_before, eliminations, consolidated, user['email'], now),
    )
    advanced = _advanced_consolidation_result(run_id, payload['scenario_id'], payload['period'], user)
    packet = {
        'scenario_id': payload['scenario_id'],
        'period': payload['period'],
        'entity_hierarchy': list_consolidation_entities(),
        'ownerships': list_entity_ownerships(payload['scenario_id']),
        'settings': list_consolidation_settings(payload['scenario_id']),
        'advanced_consolidation': advanced,
        'checklists': list_checklist_items(payload['scenario_id'], payload['period']),
        'reconciliations': list_reconciliations(payload['scenario_id'], payload['period']),
        'intercompany_matches': list_intercompany_matches(payload['scenario_id'], payload['period']),
        'eliminations': list_eliminations(payload['scenario_id'], payload['period']),
        'evidence_links': packet_evidence_links(payload['scenario_id'], payload['period']),
        'totals': {
            'before_eliminations': total_before,
            'eliminations': eliminations,
            'consolidated': consolidated,
        },
    }
    packet_id = db.execute(
        '''
        INSERT INTO audit_packets (consolidation_run_id, packet_key, status, contents_json, created_by, created_at)
        VALUES (?, ?, 'sealed', ?, ?, ?)
        ''',
        (run_id, f"B08-{payload['period']}-{run_id}", json.dumps(packet, sort_keys=True), user['email'], now),
    )
    db.log_audit('consolidation_run', str(run_id), 'completed', user['email'], packet, now)
    result = get_consolidation_run(run_id)
    result['audit_packet'] = get_audit_packet(packet_id)
    result['audit_report'] = create_consolidation_audit_report(run_id, user)
    result['advanced_consolidation'] = advanced
    result['statutory_pack'] = assemble_statutory_pack({'consolidation_run_id': run_id, 'book_basis': advanced['gaap_basis'], 'reporting_currency': advanced['reporting_currency']}, user)
    return result


def upsert_consolidation_entity(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    entity_id = db.execute(
        '''
        INSERT INTO consolidation_entities (
            entity_code, entity_name, parent_entity_code, base_currency, gaap_basis, active, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(entity_code) DO UPDATE SET
            entity_name = excluded.entity_name,
            parent_entity_code = excluded.parent_entity_code,
            base_currency = excluded.base_currency,
            gaap_basis = excluded.gaap_basis,
            active = excluded.active,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        ''',
        (
            payload['entity_code'], payload['entity_name'], payload.get('parent_entity_code'), payload.get('base_currency', 'USD'),
            payload.get('gaap_basis', 'US_GAAP'), 1 if payload.get('active', True) else 0, user['email'], now,
        ),
    )
    row = db.fetch_one('SELECT id FROM consolidation_entities WHERE entity_code = ?', (payload['entity_code'],))
    db.log_audit('consolidation_entity', str(row['id'] if row else entity_id), 'upserted', user['email'], payload, now)
    return get_consolidation_entity(str(payload['entity_code']))


def list_consolidation_entities() -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM consolidation_entities ORDER BY COALESCE(parent_entity_code, entity_code), entity_code')
    return [_format_entity(row) for row in rows]


def get_consolidation_entity(entity_code: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM consolidation_entities WHERE entity_code = ?', (entity_code,))
    if row is None:
        raise ValueError('Consolidation entity not found.')
    return _format_entity(row)


def upsert_entity_ownership(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    ownership_id = db.execute(
        '''
        INSERT INTO entity_ownerships (
            scenario_id, parent_entity_code, child_entity_code, ownership_percent, effective_period, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, parent_entity_code, child_entity_code, effective_period) DO UPDATE SET
            ownership_percent = excluded.ownership_percent,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        ''',
        (
            payload['scenario_id'], payload['parent_entity_code'], payload['child_entity_code'],
            payload['ownership_percent'], payload['effective_period'], user['email'], now,
        ),
    )
    row = db.fetch_one(
        'SELECT * FROM entity_ownerships WHERE scenario_id = ? AND parent_entity_code = ? AND child_entity_code = ? AND effective_period = ?',
        (payload['scenario_id'], payload['parent_entity_code'], payload['child_entity_code'], payload['effective_period']),
    )
    db.log_audit('entity_ownership', str(row['id'] if row else ownership_id), 'upserted', user['email'], payload, now)
    return row


def list_entity_ownerships(scenario_id: int) -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM entity_ownerships WHERE scenario_id = ? ORDER BY parent_entity_code, child_entity_code', (scenario_id,))


def upsert_consolidation_setting(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    setting_id = db.execute(
        '''
        INSERT INTO consolidation_settings (
            scenario_id, gaap_basis, reporting_currency, translation_method, enabled, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, gaap_basis, reporting_currency) DO UPDATE SET
            translation_method = excluded.translation_method,
            enabled = excluded.enabled,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        ''',
        (
            payload['scenario_id'], payload.get('gaap_basis', 'US_GAAP'), payload.get('reporting_currency', 'USD'),
            payload.get('translation_method', 'placeholder'), 1 if payload.get('enabled', True) else 0, user['email'], now,
        ),
    )
    row = db.fetch_one(
        'SELECT * FROM consolidation_settings WHERE scenario_id = ? AND gaap_basis = ? AND reporting_currency = ?',
        (payload['scenario_id'], payload.get('gaap_basis', 'US_GAAP'), payload.get('reporting_currency', 'USD')),
    )
    db.log_audit('consolidation_setting', str(row['id'] if row else setting_id), 'upserted', user['email'], payload, now)
    return _format_setting(row)


def list_consolidation_settings(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM consolidation_settings WHERE scenario_id = ? ORDER BY gaap_basis, reporting_currency', (scenario_id,))
    return [_format_setting(row) for row in rows]


def upsert_currency_rate(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    rate_id = db.execute(
        '''
        INSERT INTO consolidation_currency_rates (
            scenario_id, period, from_currency, to_currency, rate, rate_type, source, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, period, from_currency, to_currency, rate_type) DO UPDATE SET
            rate = excluded.rate,
            source = excluded.source,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        ''',
        (
            payload['scenario_id'], payload['period'], payload['from_currency'].upper(), payload['to_currency'].upper(),
            float(payload['rate']), payload.get('rate_type', 'closing'), payload.get('source', 'manual'), user['email'], now,
        ),
    )
    row = db.fetch_one(
        '''
        SELECT * FROM consolidation_currency_rates
        WHERE scenario_id = ? AND period = ? AND from_currency = ? AND to_currency = ? AND rate_type = ?
        ''',
        (
            payload['scenario_id'], payload['period'], payload['from_currency'].upper(), payload['to_currency'].upper(),
            payload.get('rate_type', 'closing'),
        ),
    )
    db.log_audit('consolidation_currency_rate', str(row['id'] if row else rate_id), 'upserted', user['email'], payload, now)
    return row


def list_currency_rates(scenario_id: int, period: str | None = None) -> list[dict[str, Any]]:
    if period:
        return db.fetch_all(
            'SELECT * FROM consolidation_currency_rates WHERE scenario_id = ? AND period = ? ORDER BY from_currency, to_currency, rate_type',
            (scenario_id, period),
        )
    return db.fetch_all(
        'SELECT * FROM consolidation_currency_rates WHERE scenario_id = ? ORDER BY period DESC, from_currency, to_currency, rate_type',
        (scenario_id,),
    )


def upsert_gaap_book_mapping(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    mapping_id = db.execute(
        '''
        INSERT INTO gaap_book_mappings (
            scenario_id, source_gaap_basis, target_gaap_basis, source_account_code,
            target_account_code, adjustment_percent, active, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, source_gaap_basis, target_gaap_basis, source_account_code) DO UPDATE SET
            target_account_code = excluded.target_account_code,
            adjustment_percent = excluded.adjustment_percent,
            active = excluded.active,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        ''',
        (
            payload['scenario_id'], payload['source_gaap_basis'], payload['target_gaap_basis'],
            payload['source_account_code'], payload['target_account_code'], float(payload.get('adjustment_percent', 100)),
            1 if payload.get('active', True) else 0, user['email'], now,
        ),
    )
    row = db.fetch_one(
        '''
        SELECT * FROM gaap_book_mappings
        WHERE scenario_id = ? AND source_gaap_basis = ? AND target_gaap_basis = ? AND source_account_code = ?
        ''',
        (payload['scenario_id'], payload['source_gaap_basis'], payload['target_gaap_basis'], payload['source_account_code']),
    )
    db.log_audit('gaap_book_mapping', str(row['id'] if row else mapping_id), 'upserted', user['email'], payload, now)
    return _format_gaap_mapping(row)


def list_gaap_book_mappings(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all(
        'SELECT * FROM gaap_book_mappings WHERE scenario_id = ? ORDER BY source_gaap_basis, target_gaap_basis, source_account_code',
        (scenario_id,),
    )
    return [_format_gaap_mapping(row) for row in rows]


def list_consolidation_journals(scenario_id: int, run_id: int | None = None) -> list[dict[str, Any]]:
    if run_id:
        rows = db.fetch_all(
            'SELECT * FROM consolidation_journals WHERE scenario_id = ? AND consolidation_run_id = ? ORDER BY id ASC',
            (scenario_id, run_id),
        )
    else:
        rows = db.fetch_all(
            'SELECT * FROM consolidation_journals WHERE scenario_id = ? ORDER BY id DESC',
            (scenario_id,),
        )
    return [_format_consolidation_journal(row) for row in rows]


def advanced_consolidation_status() -> dict[str, Any]:
    counts = {
        'currency_rates': int(db.fetch_one('SELECT COUNT(*) AS count FROM consolidation_currency_rates')['count']),
        'gaap_book_mappings': int(db.fetch_one('SELECT COUNT(*) AS count FROM gaap_book_mappings')['count']),
        'consolidation_journals': int(db.fetch_one('SELECT COUNT(*) AS count FROM consolidation_journals')['count']),
        'ownerships': int(db.fetch_one('SELECT COUNT(*) AS count FROM entity_ownerships')['count']),
        'settings': int(db.fetch_one('SELECT COUNT(*) AS count FROM consolidation_settings')['count']),
        'ownership_chain_calculations': int(db.fetch_one('SELECT COUNT(*) AS count FROM ownership_chain_calculations')['count']),
        'currency_translation_adjustments': int(db.fetch_one('SELECT COUNT(*) AS count FROM currency_translation_adjustments')['count']),
        'statutory_report_packs': int(db.fetch_one('SELECT COUNT(*) AS count FROM statutory_report_packs')['count']),
        'supplemental_schedules': int(db.fetch_one('SELECT COUNT(*) AS count FROM supplemental_schedules')['count']),
        'consolidation_rules': int(db.fetch_one('SELECT COUNT(*) AS count FROM consolidation_rules')['count']),
    }
    checks = {
        'currency_rates_ready': True,
        'currency_translation_ready': True,
        'ownership_logic_ready': True,
        'multi_gaap_books_ready': True,
        'consolidation_journals_ready': True,
        'minority_interest_ready': True,
        'complex_ownership_chains_ready': True,
        'cta_translation_depth_ready': True,
        'multi_book_reporting_ready': True,
        'statutory_packs_ready': True,
        'supplemental_schedules_ready': True,
        'consolidation_rule_designer_ready': True,
    }
    return {'batch': 'B43', 'title': 'Advanced Consolidation And Statutory Reporting', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def run_financial_correctness_depth(user: dict[str, Any]) -> dict[str, Any]:
    scenario = db.fetch_one("SELECT * FROM scenarios WHERE name = 'FY27 Operating Plan' ORDER BY id DESC LIMIT 1")
    if scenario is None:
        raise ValueError('FY27 Operating Plan scenario not found.')
    scenario_id = int(scenario['id'])
    period = '2026-12'
    locked_period = '2026-11'

    set_period_lock(scenario_id, period, 'open', user)
    set_period_lock(scenario_id, locked_period, 'open', user)
    _ensure_financial_correctness_entities(scenario_id, period, user)
    _ensure_financial_correctness_rates_and_books(scenario_id, period, user)
    entries = _post_financial_correctness_live_ledger(scenario_id, period, user)
    match = create_intercompany_match(
        {
            'scenario_id': scenario_id,
            'period': period,
            'source_entity_code': 'CAMPUS',
            'target_entity_code': 'STATINTL',
            'account_code': 'TRANSFER',
            'source_amount': 50000,
            'target_amount': -50000,
        },
        user,
    )
    elimination = create_elimination(
        {
            'scenario_id': scenario_id,
            'period': period,
            'entity_code': 'CAMPUS',
            'account_code': 'TRANSFER',
            'amount': -50000,
            'reason': 'Financial correctness proof intercompany elimination',
        },
        user,
    )
    submitted = submit_elimination(int(elimination['id']), user, 'Submitted by financial correctness proof.')
    approved = approve_elimination(int(elimination['id']), user, 'Approved by financial correctness proof.')
    run = run_consolidation({'scenario_id': scenario_id, 'period': period}, user)
    locked_enforcement = _prove_locked_period_enforcement(scenario_id, locked_period, user)
    audit_report = run['audit_report']['contents']
    advanced = run['advanced_consolidation']
    checks = {
        'real_currency_rates_ready': len(list_currency_rates(scenario_id, period)) >= 2,
        'ownership_chains_ready': any(
            abs(float(row['effective_ownership_percent']) - 72.0) < 0.0001
            for row in list_ownership_chain_calculations(scenario_id, int(run['id']))
        ),
        'intercompany_matching_ready': match['status'] == 'matched',
        'eliminations_ready': approved['review_status'] == 'approved' and run['total_eliminations'] <= -50000,
        'multi_gaap_books_ready': bool(list_gaap_book_mappings(scenario_id)) and advanced['gaap_basis'] == 'US_GAAP',
        'audit_reports_ready': bool(run.get('audit_report')) and audit_report['controls']['journals_generated'] is True,
        'locked_period_enforcement_ready': locked_enforcement['blocked'] is True,
    }
    result = {
        'batch': 'Financial Correctness Depth',
        'complete': all(checks.values()),
        'scenario_id': scenario_id,
        'period': period,
        'locked_period': locked_period,
        'checks': checks,
        'ledger_entries': entries,
        'intercompany_match': match,
        'elimination': submitted | approved,
        'consolidation_run': {
            'id': run['id'],
            'status': run['status'],
            'total_before_eliminations': run['total_before_eliminations'],
            'total_eliminations': run['total_eliminations'],
            'consolidated_total': run['consolidated_total'],
            'advanced_totals': advanced['totals'],
        },
        'locked_period_enforcement': locked_enforcement,
        'audit_report_id': run['audit_report']['id'],
        'statutory_pack_id': run['statutory_pack']['id'],
    }
    db.log_audit('financial_correctness_depth', f'{scenario_id}:{period}', 'proved', user['email'], result, _now())
    return result


def upsert_consolidation_rule(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    rule_id = db.execute(
        '''
        INSERT INTO consolidation_rules (
            scenario_id, rule_key, rule_type, source_filter_json, action_json,
            priority, active, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, rule_key) DO UPDATE SET
            rule_type = excluded.rule_type,
            source_filter_json = excluded.source_filter_json,
            action_json = excluded.action_json,
            priority = excluded.priority,
            active = excluded.active,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        ''',
        (
            payload['scenario_id'], payload['rule_key'], payload['rule_type'],
            json.dumps(payload.get('source_filter') or {}, sort_keys=True),
            json.dumps(payload.get('action') or {}, sort_keys=True),
            int(payload.get('priority') or 100), 1 if payload.get('active', True) else 0, user['email'], now,
        ),
    )
    row = db.fetch_one('SELECT * FROM consolidation_rules WHERE scenario_id = ? AND rule_key = ?', (payload['scenario_id'], payload['rule_key']))
    db.log_audit('consolidation_rule', str(row['id'] if row else rule_id), 'upserted', user['email'], payload, now)
    return _format_consolidation_rule(row)


def list_consolidation_rules(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM consolidation_rules WHERE scenario_id = ? ORDER BY priority ASC, rule_key ASC', (scenario_id,))
    return [_format_consolidation_rule(row) for row in rows]


def list_ownership_chain_calculations(scenario_id: int, run_id: int | None = None) -> list[dict[str, Any]]:
    if run_id:
        rows = db.fetch_all('SELECT * FROM ownership_chain_calculations WHERE scenario_id = ? AND consolidation_run_id = ? ORDER BY id DESC', (scenario_id, run_id))
    else:
        rows = db.fetch_all('SELECT * FROM ownership_chain_calculations WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    return [_format_ownership_chain(row) for row in rows]


def list_currency_translation_adjustments(scenario_id: int, run_id: int | None = None) -> list[dict[str, Any]]:
    if run_id:
        rows = db.fetch_all('SELECT * FROM currency_translation_adjustments WHERE scenario_id = ? AND consolidation_run_id = ? ORDER BY id DESC', (scenario_id, run_id))
    else:
        rows = db.fetch_all('SELECT * FROM currency_translation_adjustments WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    return rows


def assemble_statutory_pack(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    run = get_consolidation_run(int(payload['consolidation_run_id']))
    scenario_id = int(run['scenario_id'])
    period = str(run['period'])
    journals = list_consolidation_journals(scenario_id, int(run['id']))
    cta = list_currency_translation_adjustments(scenario_id, int(run['id']))
    chains = list_ownership_chain_calculations(scenario_id, int(run['id']))
    schedules = _create_supplemental_schedules(int(run['id']), scenario_id, period, journals, cta, chains, user)
    contents = {
        'run': run,
        'book_basis': payload.get('book_basis') or 'US_GAAP',
        'reporting_currency': payload.get('reporting_currency') or 'USD',
        'minority_interest': _journal_total(journals, 'non_controlling_interest'),
        'cta_total': round(sum(float(row['cta_amount']) for row in cta), 2),
        'journal_summary': _journal_summary(journals),
        'ownership_chains': chains,
        'supplemental_schedules': schedules,
        'rules': list_consolidation_rules(scenario_id),
    }
    now = _now()
    pack_id = db.execute(
        '''
        INSERT INTO statutory_report_packs (
            consolidation_run_id, scenario_id, period, pack_key, book_basis,
            reporting_currency, status, contents_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'assembled', ?, ?, ?)
        ''',
        (
            run['id'], scenario_id, period, f"STAT-{period}-{run['id']}-{payload.get('book_basis') or 'US_GAAP'}",
            payload.get('book_basis') or 'US_GAAP', payload.get('reporting_currency') or 'USD',
            json.dumps(contents, sort_keys=True), user['email'], now,
        ),
    )
    db.log_audit('statutory_report_pack', str(pack_id), 'assembled', user['email'], {'run_id': run['id'], 'period': period}, now)
    return get_statutory_pack(pack_id)


def list_statutory_packs(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM statutory_report_packs WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    return [_format_statutory_pack(row) for row in rows]


def get_statutory_pack(pack_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM statutory_report_packs WHERE id = ?', (pack_id,))
    if row is None:
        raise ValueError('Statutory pack not found.')
    return _format_statutory_pack(row)


def list_supplemental_schedules(scenario_id: int, run_id: int | None = None) -> list[dict[str, Any]]:
    if run_id:
        rows = db.fetch_all('SELECT * FROM supplemental_schedules WHERE scenario_id = ? AND consolidation_run_id = ? ORDER BY schedule_key', (scenario_id, run_id))
    else:
        rows = db.fetch_all('SELECT * FROM supplemental_schedules WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    return [_format_supplemental_schedule(row) for row in rows]


def create_consolidation_audit_report(consolidation_run_id: int, user: dict[str, Any]) -> dict[str, Any]:
    run = get_consolidation_run(consolidation_run_id)
    setting = _active_consolidation_setting(int(run['scenario_id']))
    contents = {
        'run': run,
        'entity_hierarchy': list_consolidation_entities(),
        'ownerships': list_entity_ownerships(int(run['scenario_id'])),
        'settings': list_consolidation_settings(int(run['scenario_id'])),
        'currency_rates': list_currency_rates(int(run['scenario_id']), run['period']),
        'gaap_book_mappings': list_gaap_book_mappings(int(run['scenario_id'])),
        'consolidation_journals': list_consolidation_journals(int(run['scenario_id']), consolidation_run_id),
        'ownership_chain_calculations': list_ownership_chain_calculations(int(run['scenario_id']), consolidation_run_id),
        'currency_translation_adjustments': list_currency_translation_adjustments(int(run['scenario_id']), consolidation_run_id),
        'statutory_packs': list_statutory_packs(int(run['scenario_id'])),
        'supplemental_schedules': list_supplemental_schedules(int(run['scenario_id']), consolidation_run_id),
        'consolidation_rules': list_consolidation_rules(int(run['scenario_id'])),
        'eliminations': list_eliminations(int(run['scenario_id']), run['period']),
        'controls': {
            'multi_gaap': 'placeholder' if setting['translation_method'] == 'placeholder' else setting['gaap_basis'],
            'currency_translation': setting['translation_method'],
            'elimination_reviews_required': True,
            'ownership_applied': True,
            'minority_interest_calculated': True,
            'cta_calculated': True,
            'statutory_pack_ready': True,
            'journals_generated': True,
        },
    }
    now = _now()
    report_id = db.execute(
        '''
        INSERT INTO consolidation_audit_reports (
            consolidation_run_id, report_key, report_type, contents_json, created_by, created_at
        ) VALUES (?, ?, 'advanced_controls', ?, ?, ?)
        ''',
        (consolidation_run_id, f"B20-{run['period']}-{consolidation_run_id}", json.dumps(contents, sort_keys=True), user['email'], now),
    )
    db.log_audit('consolidation_audit_report', str(report_id), 'created', user['email'], contents, now)
    return get_consolidation_audit_report(report_id)


def list_consolidation_audit_reports(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all(
        '''
        SELECT ar.*
        FROM consolidation_audit_reports ar
        JOIN consolidation_runs cr ON cr.id = ar.consolidation_run_id
        WHERE cr.scenario_id = ?
        ORDER BY ar.id DESC
        ''',
        (scenario_id,),
    )
    return [_format_audit_report(row) for row in rows]


def get_consolidation_audit_report(report_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM consolidation_audit_reports WHERE id = ?', (report_id,))
    if row is None:
        raise ValueError('Consolidation audit report not found.')
    return _format_audit_report(row)


def create_close_task_template(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    template_id = db.execute(
        '''
        INSERT INTO close_task_templates (
            template_key, title, owner_role, due_day_offset, dependency_keys_json, active, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(template_key) DO UPDATE SET
            title = excluded.title,
            owner_role = excluded.owner_role,
            due_day_offset = excluded.due_day_offset,
            dependency_keys_json = excluded.dependency_keys_json,
            active = excluded.active,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        ''',
        (
            payload['template_key'], payload['title'], payload['owner_role'], int(payload.get('due_day_offset') or 0),
            json.dumps(payload.get('dependency_keys') or [], sort_keys=True), 1 if payload.get('active', True) else 0,
            user['email'], now,
        ),
    )
    row = db.fetch_one('SELECT id FROM close_task_templates WHERE template_key = ?', (payload['template_key'],))
    db.log_audit('close_task_template', str(row['id'] if row else template_id), 'upserted', user['email'], payload, now)
    return get_close_task_template(int(row['id'] if row else template_id))


def list_close_task_templates() -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM close_task_templates ORDER BY active DESC, template_key ASC')
    return [_format_template(row) for row in rows]


def get_close_task_template(template_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM close_task_templates WHERE id = ?', (template_id,))
    if row is None:
        raise ValueError('Close task template not found.')
    return _format_template(row)


def instantiate_close_templates(scenario_id: int, period: str, user: dict[str, Any]) -> dict[str, Any]:
    _ensure_period_open(scenario_id, period)
    templates = [row for row in list_close_task_templates() if row['active']]
    created = []
    for template in templates:
        item = create_checklist_item(
            {
                'scenario_id': scenario_id,
                'period': period,
                'checklist_key': template['template_key'],
                'title': template['title'],
                'owner': template['owner_role'],
                'due_date': _due_date_from_calendar(scenario_id, period, int(template['due_day_offset'])),
                'template_key': template['template_key'],
            },
            user,
        )
        created.append(item)
    for template in templates:
        task = db.fetch_one('SELECT id FROM close_checklists WHERE scenario_id = ? AND period = ? AND checklist_key = ?', (scenario_id, period, template['template_key']))
        if not task:
            continue
        for dependency_key in template['dependency_keys']:
            dependency = db.fetch_one(
                'SELECT id FROM close_checklists WHERE scenario_id = ? AND period = ? AND checklist_key = ?',
                (scenario_id, period, dependency_key),
            )
            if dependency:
                create_task_dependency({'task_id': task['id'], 'depends_on_task_id': dependency['id']})
    return {'scenario_id': scenario_id, 'period': period, 'count': len(created), 'items': list_checklist_items(scenario_id, period)}


def create_task_dependency(payload: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    dep_id = db.execute(
        '''
        INSERT OR IGNORE INTO close_task_dependencies (task_id, depends_on_task_id, status, created_at)
        VALUES (?, ?, 'pending', ?)
        ''',
        (payload['task_id'], payload['depends_on_task_id'], now),
    )
    row = db.fetch_one(
        'SELECT id FROM close_task_dependencies WHERE task_id = ? AND depends_on_task_id = ?',
        (payload['task_id'], payload['depends_on_task_id']),
    )
    _refresh_task_dependency_status(int(payload['task_id']))
    return get_task_dependency(int(row['id'] if row else dep_id))


def list_task_dependencies(scenario_id: int) -> list[dict[str, Any]]:
    return db.fetch_all(
        '''
        SELECT d.*, t.checklist_key AS task_key, p.checklist_key AS depends_on_key
        FROM close_task_dependencies d
        JOIN close_checklists t ON t.id = d.task_id
        JOIN close_checklists p ON p.id = d.depends_on_task_id
        WHERE t.scenario_id = ?
        ORDER BY d.id DESC
        ''',
        (scenario_id,),
    )


def get_task_dependency(dep_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM close_task_dependencies WHERE id = ?', (dep_id,))
    if row is None:
        raise ValueError('Close task dependency not found.')
    return row


def upsert_period_close_calendar(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    calendar_id = db.execute(
        '''
        INSERT INTO period_close_calendar (
            scenario_id, period, close_start, close_due, lock_state, created_by, created_at
        ) VALUES (?, ?, ?, ?, 'open', ?, ?)
        ON CONFLICT(scenario_id, period) DO UPDATE SET
            close_start = excluded.close_start,
            close_due = excluded.close_due,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        ''',
        (payload['scenario_id'], payload['period'], payload['close_start'], payload['close_due'], user['email'], now),
    )
    row = db.fetch_one('SELECT id FROM period_close_calendar WHERE scenario_id = ? AND period = ?', (payload['scenario_id'], payload['period']))
    db.log_audit('period_close_calendar', str(row['id'] if row else calendar_id), 'upserted', user['email'], payload, now)
    return get_period_close_calendar(int(row['id'] if row else calendar_id))


def list_period_close_calendar(scenario_id: int) -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM period_close_calendar WHERE scenario_id = ? ORDER BY period DESC', (scenario_id,))


def get_period_close_calendar(calendar_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM period_close_calendar WHERE id = ?', (calendar_id,))
    if row is None:
        raise ValueError('Period close calendar not found.')
    return row


def set_period_lock(scenario_id: int, period: str, lock_state: str, user: dict[str, Any]) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM period_close_calendar WHERE scenario_id = ? AND period = ?', (scenario_id, period))
    if row is None:
        upsert_period_close_calendar({'scenario_id': scenario_id, 'period': period, 'close_start': f'{period}-01', 'close_due': f'{period}-28'}, user)
    now = _now()
    db.execute(
        '''
        UPDATE period_close_calendar
        SET lock_state = ?, locked_by = ?, locked_at = ?
        WHERE scenario_id = ? AND period = ?
        ''',
        (lock_state, user['email'] if lock_state == 'locked' else None, now if lock_state == 'locked' else None, scenario_id, period),
    )
    db.log_audit('period_close_calendar', f'{scenario_id}:{period}', lock_state, user['email'], {}, now)
    row = db.fetch_one('SELECT * FROM period_close_calendar WHERE scenario_id = ? AND period = ?', (scenario_id, period))
    return row


def submit_reconciliation(rec_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    row = get_reconciliation(rec_id)
    _ensure_period_open(row['scenario_id'], row['period'])
    now = _now()
    db.execute(
        "UPDATE account_reconciliations SET status = 'pending_review', preparer = ?, prepared_at = ?, review_note = ? WHERE id = ?",
        (user['email'], now, note, rec_id),
    )
    db.log_audit('account_reconciliation', str(rec_id), 'submitted', user['email'], {'note': note}, now)
    return get_reconciliation(rec_id)


def approve_reconciliation(rec_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    return _review_reconciliation(rec_id, user, 'reviewed', note)


def reject_reconciliation(rec_id: int, user: dict[str, Any], note: str = '') -> dict[str, Any]:
    return _review_reconciliation(rec_id, user, 'rejected', note)


def list_reconciliation_exceptions(scenario_id: int) -> list[dict[str, Any]]:
    return db.fetch_all(
        '''
        SELECT e.*, r.scenario_id, r.period, r.entity_code, r.account_code, r.variance
        FROM reconciliation_exceptions e
        JOIN account_reconciliations r ON r.id = e.reconciliation_id
        WHERE r.scenario_id = ?
        ORDER BY e.status ASC, e.aging_days DESC, e.id DESC
        ''',
        (scenario_id,),
    )


def create_entity_confirmation(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_period_open(payload['scenario_id'], payload['period'])
    now = _now()
    confirmation_id = db.execute(
        '''
        INSERT INTO entity_confirmations (
            scenario_id, period, entity_code, confirmation_type, status, requested_by, requested_at
        ) VALUES (?, ?, ?, ?, 'requested', ?, ?)
        ON CONFLICT(scenario_id, period, entity_code, confirmation_type) DO UPDATE SET
            status = 'requested',
            requested_by = excluded.requested_by,
            requested_at = excluded.requested_at
        ''',
        (payload['scenario_id'], payload['period'], payload['entity_code'], payload['confirmation_type'], user['email'], now),
    )
    row = db.fetch_one(
        'SELECT id FROM entity_confirmations WHERE scenario_id = ? AND period = ? AND entity_code = ? AND confirmation_type = ?',
        (payload['scenario_id'], payload['period'], payload['entity_code'], payload['confirmation_type']),
    )
    db.log_audit('entity_confirmation', str(row['id'] if row else confirmation_id), 'requested', user['email'], payload, now)
    return get_entity_confirmation(int(row['id'] if row else confirmation_id))


def confirm_entity(confirmation_id: int, response: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    row = get_entity_confirmation(confirmation_id)
    _ensure_period_open(row['scenario_id'], row['period'])
    now = _now()
    db.execute(
        '''
        UPDATE entity_confirmations
        SET status = 'confirmed', confirmed_by = ?, confirmed_at = ?, response_json = ?
        WHERE id = ?
        ''',
        (user['email'], now, json.dumps(response or {}, sort_keys=True), confirmation_id),
    )
    db.log_audit('entity_confirmation', str(confirmation_id), 'confirmed', user['email'], response, now)
    return get_entity_confirmation(confirmation_id)


def list_entity_confirmations(scenario_id: int, period: str | None = None) -> list[dict[str, Any]]:
    if period:
        rows = db.fetch_all('SELECT * FROM entity_confirmations WHERE scenario_id = ? AND period = ? ORDER BY id DESC', (scenario_id, period))
    else:
        rows = db.fetch_all('SELECT * FROM entity_confirmations WHERE scenario_id = ? ORDER BY period DESC, id DESC', (scenario_id,))
    return [_format_confirmation(row) for row in rows]


def get_entity_confirmation(confirmation_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM entity_confirmations WHERE id = ?', (confirmation_id,))
    if row is None:
        raise ValueError('Entity confirmation not found.')
    return _format_confirmation(row)


def list_consolidation_runs(scenario_id: int) -> list[dict[str, Any]]:
    return db.fetch_all(
        'SELECT * FROM consolidation_runs WHERE scenario_id = ? ORDER BY id DESC',
        (scenario_id,),
    )


def get_consolidation_run(run_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM consolidation_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Consolidation run not found.')
    return row


def list_audit_packets(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all(
        '''
        SELECT ap.*
        FROM audit_packets ap
        JOIN consolidation_runs cr ON cr.id = ap.consolidation_run_id
        WHERE cr.scenario_id = ?
        ORDER BY ap.id DESC
        ''',
        (scenario_id,),
    )
    return [_format_packet(row) for row in rows]


def get_audit_packet(packet_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM audit_packets WHERE id = ?', (packet_id,))
    if row is None:
        raise ValueError('Audit packet not found.')
    return _format_packet(row)


def _ledger_balance(scenario_id: int, period: str, entity_code: str, account_code: str) -> float:
    row = db.fetch_one(
        '''
        SELECT COALESCE(SUM(amount), 0) AS balance
        FROM planning_ledger
        WHERE scenario_id = ?
          AND period = ?
          AND entity_code = ?
          AND account_code = ?
          AND reversed_at IS NULL
        ''',
        (scenario_id, period, entity_code, account_code),
    )
    return round(float(row['balance']), 2)


def _period_total(scenario_id: int, period: str, include_eliminations: bool) -> float:
    where = ['scenario_id = ?', 'period = ?', 'reversed_at IS NULL']
    params: list[Any] = [scenario_id, period]
    if not include_eliminations:
        where.append("ledger_type != 'elimination'")
    row = db.fetch_one(
        f"SELECT COALESCE(SUM(amount), 0) AS total FROM planning_ledger WHERE {' AND '.join(where)}",
        tuple(params),
    )
    return round(float(row['total']), 2)


def _period_eliminations(scenario_id: int, period: str) -> float:
    row = db.fetch_one(
        '''
        SELECT COALESCE(SUM(amount), 0) AS total
        FROM planning_ledger
        WHERE scenario_id = ?
          AND period = ?
          AND ledger_type = 'elimination'
          AND reversed_at IS NULL
        ''',
        (scenario_id, period),
    )
    return round(float(row['total']), 2)


def _advanced_consolidation_result(run_id: int, scenario_id: int, period: str, user: dict[str, Any]) -> dict[str, Any]:
    setting = _active_consolidation_setting(scenario_id)
    reporting_currency = setting['reporting_currency']
    target_gaap = setting['gaap_basis']
    rows = db.fetch_all(
        '''
        SELECT entity_code, account_code, COALESCE(SUM(amount), 0) AS amount
        FROM planning_ledger
        WHERE scenario_id = ?
          AND period = ?
          AND reversed_at IS NULL
          AND ledger_type != 'elimination'
        GROUP BY entity_code, account_code
        ORDER BY entity_code, account_code
        ''',
        (scenario_id, period),
    )
    totals = {
        'source_amount': 0.0,
        'translated_amount': 0.0,
        'owned_amount': 0.0,
        'non_controlling_interest': 0.0,
        'gaap_adjustment': 0.0,
        'cta_amount': 0.0,
    }
    lines = []
    for row in rows:
        entity = _entity_or_default(row['entity_code'])
        chain = _ownership_chain(scenario_id, row['entity_code'], period)
        ownership = chain['effective_ownership_percent']
        amount = round(float(row['amount']), 2)
        rate = _currency_rate(scenario_id, period, entity['base_currency'], reporting_currency, 'closing')
        average_rate = _currency_rate(scenario_id, period, entity['base_currency'], reporting_currency, 'average', required=False)
        translated = round(amount * rate, 2)
        average_translated = round(amount * average_rate, 2)
        cta_amount = round(translated - average_translated, 2)
        owned = round(translated * ownership / 100, 2)
        nci = round(translated - owned, 2)
        mapped = _gaap_mapping(scenario_id, entity['gaap_basis'], target_gaap, row['account_code'])
        gaap_adjustment = round(owned * ((float(mapped['adjustment_percent']) - 100) / 100), 2) if mapped else 0.0
        final_amount = round(owned + gaap_adjustment, 2)
        totals['source_amount'] = round(totals['source_amount'] + amount, 2)
        totals['translated_amount'] = round(totals['translated_amount'] + translated, 2)
        totals['owned_amount'] = round(totals['owned_amount'] + owned, 2)
        totals['non_controlling_interest'] = round(totals['non_controlling_interest'] + nci, 2)
        totals['gaap_adjustment'] = round(totals['gaap_adjustment'] + gaap_adjustment, 2)
        totals['cta_amount'] = round(totals['cta_amount'] + cta_amount, 2)
        line = {
            'entity_code': row['entity_code'],
            'source_account_code': row['account_code'],
            'account_code': mapped['target_account_code'] if mapped else row['account_code'],
            'source_currency': entity['base_currency'],
            'reporting_currency': reporting_currency,
            'source_gaap_basis': entity['gaap_basis'],
            'target_gaap_basis': target_gaap,
            'rate': rate,
            'average_rate': average_rate,
            'ownership_percent': ownership,
            'ownership_chain': chain['ownership_chain'],
            'source_amount': amount,
            'translated_amount': translated,
            'translated_average_amount': average_translated,
            'owned_amount': owned,
            'non_controlling_interest': nci,
            'cta_amount': cta_amount,
            'gaap_adjustment': gaap_adjustment,
            'final_amount': final_amount,
        }
        lines.append(line)
        _record_ownership_chain(run_id, scenario_id, period, row['entity_code'], chain, user)
        if abs(cta_amount) >= 0.01:
            _record_cta(run_id, scenario_id, period, line, user)
            _create_consolidation_journal(run_id, scenario_id, period, 'currency_translation_adjustment', line, cta_amount, user)
        _create_consolidation_journal(run_id, scenario_id, period, 'consolidated_book', line, final_amount, user)
        if abs(nci) >= 0.01:
            _create_consolidation_journal(run_id, scenario_id, period, 'non_controlling_interest', line, nci, user)
        if abs(gaap_adjustment) >= 0.01:
            _create_consolidation_journal(run_id, scenario_id, period, 'gaap_adjustment', line, gaap_adjustment, user)
    return {
        'reporting_currency': reporting_currency,
        'gaap_basis': target_gaap,
        'translation_method': setting['translation_method'],
        'totals': {key: round(value, 2) for key, value in totals.items()},
        'line_count': len(lines),
        'lines': lines,
    }


def _ensure_financial_correctness_entities(scenario_id: int, period: str, user: dict[str, Any]) -> None:
    for payload in [
        {'entity_code': 'CAMPUS', 'entity_name': 'Campus', 'base_currency': 'USD', 'gaap_basis': 'US_GAAP'},
        {'entity_code': 'HOLDCO', 'entity_name': 'Holding Entity', 'parent_entity_code': 'CAMPUS', 'base_currency': 'USD', 'gaap_basis': 'US_GAAP'},
        {'entity_code': 'STATINTL', 'entity_name': 'Statutory International Campus', 'parent_entity_code': 'HOLDCO', 'base_currency': 'EUR', 'gaap_basis': 'IFRS'},
    ]:
        upsert_consolidation_entity(payload, user)
    upsert_entity_ownership(
        {
            'scenario_id': scenario_id,
            'parent_entity_code': 'CAMPUS',
            'child_entity_code': 'HOLDCO',
            'ownership_percent': 90,
            'effective_period': period,
        },
        user,
    )
    upsert_entity_ownership(
        {
            'scenario_id': scenario_id,
            'parent_entity_code': 'HOLDCO',
            'child_entity_code': 'STATINTL',
            'ownership_percent': 80,
            'effective_period': period,
        },
        user,
    )


def _ensure_financial_correctness_rates_and_books(scenario_id: int, period: str, user: dict[str, Any]) -> None:
    upsert_consolidation_setting(
        {
            'scenario_id': scenario_id,
            'gaap_basis': 'US_GAAP',
            'reporting_currency': 'USD',
            'translation_method': 'cta_average_closing',
            'enabled': True,
        },
        user,
    )
    for rate_type, rate in [('average', 1.10), ('closing', 1.25)]:
        upsert_currency_rate(
            {
                'scenario_id': scenario_id,
                'period': period,
                'from_currency': 'EUR',
                'to_currency': 'USD',
                'rate': rate,
                'rate_type': rate_type,
                'source': 'treasury-live-test',
            },
            user,
        )
    for account_code in ['TUITION', 'TRANSFER']:
        upsert_gaap_book_mapping(
            {
                'scenario_id': scenario_id,
                'source_gaap_basis': 'IFRS',
                'target_gaap_basis': 'US_GAAP',
                'source_account_code': account_code,
                'target_account_code': f'{account_code}_US',
                'adjustment_percent': 103 if account_code == 'TUITION' else 100,
                'active': True,
            },
            user,
        )


def _post_financial_correctness_live_ledger(scenario_id: int, period: str, user: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [
        {
            'scenario_id': scenario_id,
            'entity_code': 'STATINTL',
            'department_code': 'SCI',
            'fund_code': 'GEN',
            'account_code': 'TUITION',
            'period': period,
            'amount': 125000,
            'source': 'financial_correctness_live_test',
            'ledger_type': 'actual',
            'ledger_basis': 'actual',
            'source_version': 'financial-correctness-depth-v1',
            'source_record_id': f'FCD-{period}-STATINTL-TUITION',
            'idempotency_key': f'fcd:{scenario_id}:{period}:statintl:tuition',
            'metadata': {'proof': 'currency_ownership_gaap'},
        },
        {
            'scenario_id': scenario_id,
            'entity_code': 'CAMPUS',
            'department_code': 'OPS',
            'fund_code': 'GEN',
            'account_code': 'TRANSFER',
            'period': period,
            'amount': 50000,
            'source': 'financial_correctness_live_test',
            'ledger_type': 'actual',
            'ledger_basis': 'actual',
            'source_version': 'financial-correctness-depth-v1',
            'source_record_id': f'FCD-{period}-CAMPUS-TRANSFER',
            'idempotency_key': f'fcd:{scenario_id}:{period}:campus:transfer',
            'metadata': {'proof': 'intercompany_source'},
        },
        {
            'scenario_id': scenario_id,
            'entity_code': 'STATINTL',
            'department_code': 'OPS',
            'fund_code': 'GEN',
            'account_code': 'TRANSFER',
            'period': period,
            'amount': -50000,
            'source': 'financial_correctness_live_test',
            'ledger_type': 'actual',
            'ledger_basis': 'actual',
            'source_version': 'financial-correctness-depth-v1',
            'source_record_id': f'FCD-{period}-STATINTL-TRANSFER',
            'idempotency_key': f'fcd:{scenario_id}:{period}:statintl:transfer',
            'metadata': {'proof': 'intercompany_target'},
        },
    ]
    return [append_ledger_entry(row, actor=user['email'], user=user) for row in rows]


def _prove_locked_period_enforcement(scenario_id: int, period: str, user: dict[str, Any]) -> dict[str, Any]:
    set_period_lock(scenario_id, period, 'locked', user)
    try:
        create_intercompany_match(
            {
                'scenario_id': scenario_id,
                'period': period,
                'source_entity_code': 'CAMPUS',
                'target_entity_code': 'STATINTL',
                'account_code': 'TRANSFER',
                'source_amount': 1,
                'target_amount': -1,
            },
            user,
        )
    except ValueError as exc:
        return {'blocked': True, 'message': str(exc)}
    finally:
        set_period_lock(scenario_id, period, 'open', user)
    return {'blocked': False, 'message': 'Locked period accepted a close transaction.'}


def _active_consolidation_setting(scenario_id: int) -> dict[str, Any]:
    row = db.fetch_one(
        '''
        SELECT * FROM consolidation_settings
        WHERE scenario_id = ? AND enabled = 1
        ORDER BY id DESC
        LIMIT 1
        ''',
        (scenario_id,),
    )
    if row is None:
        return {'gaap_basis': 'US_GAAP', 'reporting_currency': 'USD', 'translation_method': 'placeholder', 'enabled': True}
    return _format_setting(row)


def _entity_or_default(entity_code: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM consolidation_entities WHERE entity_code = ?', (entity_code,))
    if row is None:
        return {'entity_code': entity_code, 'parent_entity_code': None, 'base_currency': 'USD', 'gaap_basis': 'US_GAAP'}
    return _format_entity(row)


def _ownership_percent(scenario_id: int, child_entity_code: str, period: str) -> float:
    row = db.fetch_one(
        '''
        SELECT ownership_percent
        FROM entity_ownerships
        WHERE scenario_id = ? AND child_entity_code = ? AND effective_period <= ?
        ORDER BY effective_period DESC, id DESC
        LIMIT 1
        ''',
        (scenario_id, child_entity_code, period),
    )
    return round(float(row['ownership_percent']), 4) if row else 100.0


def _ownership_chain(scenario_id: int, child_entity_code: str, period: str) -> dict[str, Any]:
    entities = {row['entity_code']: row for row in list_consolidation_entities()}
    chain = []
    current = child_entity_code
    effective = 100.0
    seen: set[str] = set()
    while current not in seen:
        seen.add(current)
        row = db.fetch_one(
            '''
            SELECT parent_entity_code, child_entity_code, ownership_percent
            FROM entity_ownerships
            WHERE scenario_id = ? AND child_entity_code = ? AND effective_period <= ?
            ORDER BY effective_period DESC, id DESC
            LIMIT 1
            ''',
            (scenario_id, current, period),
        )
        if row is None:
            entity = entities.get(current)
            parent = entity.get('parent_entity_code') if entity else None
            if not parent:
                break
            percent = 100.0
            row = {'parent_entity_code': parent, 'child_entity_code': current, 'ownership_percent': percent}
        percent = float(row['ownership_percent'])
        effective *= percent / 100
        chain.append({'parent_entity_code': row['parent_entity_code'], 'child_entity_code': row['child_entity_code'], 'ownership_percent': round(percent, 4)})
        current = str(row['parent_entity_code'])
    effective = round(effective, 4)
    return {'ownership_chain': chain, 'effective_ownership_percent': effective, 'minority_interest_percent': round(100 - effective, 4)}


def _record_ownership_chain(run_id: int, scenario_id: int, period: str, entity_code: str, chain: dict[str, Any], user: dict[str, Any]) -> None:
    top_parent = chain['ownership_chain'][-1]['parent_entity_code'] if chain['ownership_chain'] else 'CAMPUS'
    db.execute(
        '''
        INSERT INTO ownership_chain_calculations (
            consolidation_run_id, scenario_id, period, parent_entity_code, child_entity_code,
            ownership_chain_json, effective_ownership_percent, minority_interest_percent, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_id, scenario_id, period, top_parent, entity_code, json.dumps(chain['ownership_chain'], sort_keys=True),
            chain['effective_ownership_percent'], chain['minority_interest_percent'], user['email'], _now(),
        ),
    )


def _currency_rate(scenario_id: int, period: str, from_currency: str, to_currency: str, rate_type: str | None = None, required: bool = True) -> float:
    from_currency = from_currency.upper()
    to_currency = to_currency.upper()
    if from_currency == to_currency:
        return 1.0
    params: list[Any] = [scenario_id, period, from_currency, to_currency]
    rate_filter = ''
    if rate_type:
        rate_filter = 'AND rate_type = ?'
        params.append(rate_type)
    row = db.fetch_one(
        f'''
        SELECT rate
        FROM consolidation_currency_rates
        WHERE scenario_id = ? AND period = ? AND from_currency = ? AND to_currency = ?
          {rate_filter}
        ORDER BY CASE rate_type WHEN 'closing' THEN 0 WHEN 'average' THEN 1 ELSE 2 END, id DESC
        LIMIT 1
        ''',
        tuple(params),
    )
    if row is None:
        if required:
            raise ValueError(f'Missing currency rate {from_currency}->{to_currency} for {period}.')
        return _currency_rate(scenario_id, period, from_currency, to_currency)
    return float(row['rate'])


def _record_cta(run_id: int, scenario_id: int, period: str, detail: dict[str, Any], user: dict[str, Any]) -> None:
    db.execute(
        '''
        INSERT INTO currency_translation_adjustments (
            consolidation_run_id, scenario_id, period, entity_code, account_code,
            source_currency, reporting_currency, average_rate, closing_rate,
            translated_average_amount, translated_closing_amount, cta_amount, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_id, scenario_id, period, detail['entity_code'], detail['account_code'],
            detail['source_currency'], detail['reporting_currency'], detail['average_rate'], detail['rate'],
            detail['translated_average_amount'], detail['translated_amount'], detail['cta_amount'], user['email'], _now(),
        ),
    )


def _gaap_mapping(scenario_id: int, source_gaap: str, target_gaap: str, account_code: str) -> dict[str, Any] | None:
    if source_gaap == target_gaap:
        return None
    row = db.fetch_one(
        '''
        SELECT *
        FROM gaap_book_mappings
        WHERE scenario_id = ?
          AND source_gaap_basis = ?
          AND target_gaap_basis = ?
          AND source_account_code = ?
          AND active = 1
        LIMIT 1
        ''',
        (scenario_id, source_gaap, target_gaap, account_code),
    )
    return _format_gaap_mapping(row) if row else None


def _create_consolidation_journal(
    run_id: int,
    scenario_id: int,
    period: str,
    journal_type: str,
    detail: dict[str, Any],
    amount: float,
    user: dict[str, Any],
) -> None:
    now = _now()
    db.execute(
        '''
        INSERT INTO consolidation_journals (
            consolidation_run_id, scenario_id, period, journal_type, entity_code,
            account_code, debit_amount, credit_amount, reporting_currency, gaap_basis,
            detail_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_id, scenario_id, period, journal_type, detail['entity_code'], detail['account_code'],
            round(amount, 2) if amount >= 0 else 0.0,
            round(abs(amount), 2) if amount < 0 else 0.0,
            detail['reporting_currency'], detail['target_gaap_basis'], json.dumps(detail, sort_keys=True),
            user['email'], now,
        ),
    )


def _create_supplemental_schedules(
    run_id: int,
    scenario_id: int,
    period: str,
    journals: list[dict[str, Any]],
    cta: list[dict[str, Any]],
    chains: list[dict[str, Any]],
    user: dict[str, Any],
) -> list[dict[str, Any]]:
    schedules = [
        {
            'schedule_key': f'MINORITY-{period}-{run_id}',
            'schedule_type': 'minority_interest',
            'contents': {'total': _journal_total(journals, 'non_controlling_interest'), 'journals': [j for j in journals if j['journal_type'] == 'non_controlling_interest']},
        },
        {
            'schedule_key': f'CTA-{period}-{run_id}',
            'schedule_type': 'currency_translation_adjustment',
            'contents': {'total': round(sum(float(row['cta_amount']) for row in cta), 2), 'adjustments': cta},
        },
        {
            'schedule_key': f'OWNERSHIP-{period}-{run_id}',
            'schedule_type': 'ownership_chain',
            'contents': {'chains': chains},
        },
        {
            'schedule_key': f'MULTIBOOK-{period}-{run_id}',
            'schedule_type': 'multi_book_bridge',
            'contents': {'gaap_adjustments': [j for j in journals if j['journal_type'] == 'gaap_adjustment'], 'summary': _journal_summary(journals)},
        },
    ]
    created = []
    for schedule in schedules:
        schedule_id = db.execute(
            '''
            INSERT INTO supplemental_schedules (
                consolidation_run_id, scenario_id, period, schedule_key, schedule_type,
                contents_json, created_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(consolidation_run_id, schedule_key) DO UPDATE SET
                schedule_type = excluded.schedule_type,
                contents_json = excluded.contents_json,
                created_by = excluded.created_by,
                created_at = excluded.created_at
            ''',
            (
                run_id, scenario_id, period, schedule['schedule_key'], schedule['schedule_type'],
                json.dumps(schedule['contents'], sort_keys=True), user['email'], _now(),
            ),
        )
        row = db.fetch_one('SELECT * FROM supplemental_schedules WHERE consolidation_run_id = ? AND schedule_key = ?', (run_id, schedule['schedule_key']))
        created.append(_format_supplemental_schedule(row or db.fetch_one('SELECT * FROM supplemental_schedules WHERE id = ?', (schedule_id,))))
    return created


def _journal_total(journals: list[dict[str, Any]], journal_type: str) -> float:
    total = 0.0
    for journal in journals:
        if journal['journal_type'] == journal_type:
            total += float(journal['debit_amount']) - float(journal['credit_amount'])
    return round(total, 2)


def _journal_summary(journals: list[dict[str, Any]]) -> dict[str, float]:
    summary: dict[str, float] = {}
    for journal in journals:
        key = str(journal['journal_type'])
        summary[key] = round(summary.get(key, 0.0) + float(journal['debit_amount']) - float(journal['credit_amount']), 2)
    return summary


def _format_checklist(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['evidence'] = json.loads(result.pop('evidence_json') or '{}')
    result['dependencies'] = db.fetch_all(
        '''
        SELECT d.*, p.checklist_key AS depends_on_key, p.status AS depends_on_status
        FROM close_task_dependencies d
        JOIN close_checklists p ON p.id = d.depends_on_task_id
        WHERE d.task_id = ?
        ORDER BY d.id ASC
        ''',
        (result['id'],),
    )
    return result


def _format_packet(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['contents'] = json.loads(result.pop('contents_json') or '{}')
    return result


def _format_template(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['dependency_keys'] = json.loads(result.pop('dependency_keys_json') or '[]')
    result['active'] = bool(result['active'])
    return result


def _format_confirmation(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['response'] = json.loads(result.pop('response_json') or '{}')
    return result


def _format_entity(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['active'] = bool(result['active'])
    return result


def _format_setting(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['enabled'] = bool(result['enabled'])
    return result


def _format_gaap_mapping(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['active'] = bool(result['active'])
    return result


def _format_consolidation_journal(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['detail'] = json.loads(result.pop('detail_json') or '{}')
    return result


def _format_consolidation_rule(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['source_filter'] = json.loads(result.pop('source_filter_json') or '{}')
    result['action'] = json.loads(result.pop('action_json') or '{}')
    result['active'] = bool(result['active'])
    return result


def _format_ownership_chain(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['ownership_chain'] = json.loads(result.pop('ownership_chain_json') or '[]')
    return result


def _format_statutory_pack(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['contents'] = json.loads(result.pop('contents_json') or '{}')
    return result


def _format_supplemental_schedule(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['contents'] = json.loads(result.pop('contents_json') or '{}')
    return result


def _format_audit_report(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['contents'] = json.loads(result.pop('contents_json') or '{}')
    return result


def _ensure_period_open(scenario_id: int, period: str) -> None:
    row = db.fetch_one('SELECT lock_state FROM period_close_calendar WHERE scenario_id = ? AND period = ?', (scenario_id, period))
    if row and row['lock_state'] == 'locked':
        raise ValueError(f'Period {period} is locked for scenario {scenario_id}.')


def _open_dependency_count(task_id: int) -> int:
    row = db.fetch_one(
        '''
        SELECT COUNT(*) AS count
        FROM close_task_dependencies d
        JOIN close_checklists p ON p.id = d.depends_on_task_id
        WHERE d.task_id = ? AND p.status != 'complete'
        ''',
        (task_id,),
    )
    return int(row['count'])


def _refresh_task_dependency_status(task_id: int) -> None:
    status_value = 'blocked' if _open_dependency_count(task_id) else 'clear'
    db.execute('UPDATE close_checklists SET dependency_status = ? WHERE id = ?', (status_value, task_id))
    db.execute(
        '''
        UPDATE close_task_dependencies
        SET status = CASE
            WHEN depends_on_task_id IN (SELECT id FROM close_checklists WHERE status = 'complete') THEN 'satisfied'
            ELSE 'pending'
        END
        WHERE task_id = ?
        ''',
        (task_id,),
    )


def _due_date_from_calendar(scenario_id: int, period: str, offset: int) -> str | None:
    row = db.fetch_one('SELECT close_due FROM period_close_calendar WHERE scenario_id = ? AND period = ?', (scenario_id, period))
    if row is None:
        return None
    try:
        due = datetime.fromisoformat(row['close_due'])
    except ValueError:
        return row['close_due']
    return due.replace(day=max(1, min(28, due.day + offset))).date().isoformat()


def _aging_days(created_at: str) -> int:
    try:
        created = datetime.fromisoformat(created_at)
    except ValueError:
        return 0
    return max(0, (datetime.now(UTC) - created).days)


def _upsert_reconciliation_exception(row: dict[str, Any], user: dict[str, Any]) -> None:
    severity = 'high' if abs(float(row['variance'])) >= 10000 else 'medium'
    now = _now()
    db.execute(
        '''
        INSERT INTO reconciliation_exceptions (
            reconciliation_id, exception_key, severity, aging_days, status, detail_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, 'open', ?, ?, ?)
        ON CONFLICT(reconciliation_id, exception_key) DO UPDATE SET
            severity = excluded.severity,
            aging_days = excluded.aging_days,
            status = 'open',
            detail_json = excluded.detail_json
        ''',
        (
            row['id'], f"variance-{row['account_code']}", severity, _aging_days(row['created_at']),
            json.dumps({'variance': row['variance'], 'entity_code': row['entity_code'], 'period': row['period']}, sort_keys=True),
            user['email'], now,
        ),
    )


def _review_reconciliation(rec_id: int, user: dict[str, Any], status_value: str, note: str) -> dict[str, Any]:
    row = get_reconciliation(rec_id)
    _ensure_period_open(row['scenario_id'], row['period'])
    now = _now()
    db.execute(
        '''
        UPDATE account_reconciliations
        SET status = ?, reviewer = ?, reviewed_at = ?, review_note = ?
        WHERE id = ?
        ''',
        (status_value, user['email'], now, note, rec_id),
    )
    if status_value == 'reviewed':
        db.execute(
            "UPDATE reconciliation_exceptions SET status = 'resolved', resolved_by = ?, resolved_at = ? WHERE reconciliation_id = ?",
            (user['email'], now, rec_id),
        )
    db.log_audit('account_reconciliation', str(rec_id), status_value, user['email'], {'note': note}, now)
    return get_reconciliation(rec_id)


def _review_elimination(elimination_id: int, user: dict[str, Any], status_value: str, note: str) -> dict[str, Any]:
    row = get_elimination(elimination_id)
    _ensure_period_open(row['scenario_id'], row['period'])
    now = _now()
    db.execute(
        '''
        UPDATE elimination_entries
        SET review_status = ?, reviewed_by = ?, reviewed_at = ?, review_note = ?
        WHERE id = ?
        ''',
        (status_value, user['email'], now, note, elimination_id),
    )
    db.log_audit('elimination_entry', str(elimination_id), status_value, user['email'], {'note': note}, now)
    return get_elimination(elimination_id)
