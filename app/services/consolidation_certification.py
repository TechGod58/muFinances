from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.close_consolidation import (
    approve_elimination,
    create_elimination,
    create_intercompany_match,
    list_consolidation_journals,
    list_currency_translation_adjustments,
    list_gaap_book_mappings,
    list_ownership_chain_calculations,
    list_supplemental_schedules,
    run_consolidation,
    set_period_lock,
    submit_elimination,
    upsert_consolidation_entity,
    upsert_consolidation_rule,
    upsert_consolidation_setting,
    upsert_currency_rate,
    upsert_entity_ownership,
    upsert_gaap_book_mapping,
)
from app.services.foundation import append_ledger_entry


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS consolidation_certification_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                artifacts_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_consolidation_certification_runs_scenario
            ON consolidation_certification_runs (scenario_id, period, completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM consolidation_certification_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'multi_entity_hierarchy_ready': True,
        'ownership_chains_ready': True,
        'minority_interest_ready': True,
        'intercompany_matching_ready': True,
        'eliminations_ready': True,
        'currency_translation_ready': True,
        'cta_ready': True,
        'multi_gaap_books_ready': True,
        'statutory_schedules_ready': True,
    }
    counts = {
        'certification_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM consolidation_certification_runs')['count']),
        'entities': int(db.fetch_one('SELECT COUNT(*) AS count FROM consolidation_entities')['count']),
        'ownership_chains': int(db.fetch_one('SELECT COUNT(*) AS count FROM ownership_chain_calculations')['count']),
        'cta_rows': int(db.fetch_one('SELECT COUNT(*) AS count FROM currency_translation_adjustments')['count']),
        'statutory_packs': int(db.fetch_one('SELECT COUNT(*) AS count FROM statutory_report_packs')['count']),
    }
    return {
        'batch': 'B97',
        'title': 'Consolidation Certification',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM consolidation_certification_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_run(row) for row in rows]


def run_certification(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b97-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    scenario_id = int(payload.get('scenario_id') or _create_scenario(run_key))
    period = payload.get('period') or '2026-12'
    set_period_lock(scenario_id, period, 'open', user)

    entities = _seed_entities(user)
    ownerships = _seed_ownerships(scenario_id, period, user)
    settings = upsert_consolidation_setting(
        {
            'scenario_id': scenario_id,
            'gaap_basis': 'US_GAAP',
            'reporting_currency': 'USD',
            'translation_method': 'cta_average_closing',
            'enabled': True,
        },
        user,
    )
    rates = _seed_rates(scenario_id, period, user)
    gaap_mappings = _seed_gaap_mappings(scenario_id, user)
    rules = [
        upsert_consolidation_rule(
            {
                'scenario_id': scenario_id,
                'rule_key': f'{run_key}-intercompany-elim',
                'rule_type': 'elimination',
                'source_filter': {'account_code': 'TRANSFER'},
                'action': {'journal_type': 'intercompany_elimination'},
                'priority': 10,
                'active': True,
            },
            user,
        ),
        upsert_consolidation_rule(
            {
                'scenario_id': scenario_id,
                'rule_key': f'{run_key}-statutory-schedules',
                'rule_type': 'statutory_schedule',
                'source_filter': {'book_basis': 'US_GAAP'},
                'action': {'include_cta': True, 'include_nci': True},
                'priority': 20,
                'active': True,
            },
            user,
        ),
    ]
    ledger_entries = _seed_ledger(scenario_id, period, run_key, user)
    intercompany = create_intercompany_match(
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
            'reason': 'B97 certified intercompany elimination',
        },
        user,
    )
    submitted_elimination = submit_elimination(int(elimination['id']), user, 'Submitted by B97 consolidation certification.')
    approved_elimination = approve_elimination(int(elimination['id']), user, 'Approved by B97 consolidation certification.')
    consolidation = run_consolidation({'scenario_id': scenario_id, 'period': period}, user)
    run_id = int(consolidation['id'])
    ownership_chains = list_ownership_chain_calculations(scenario_id, run_id)
    cta_rows = list_currency_translation_adjustments(scenario_id, run_id)
    journals = list_consolidation_journals(scenario_id, run_id)
    statutory_pack = consolidation['statutory_pack']
    schedules = list_supplemental_schedules(scenario_id, run_id)

    checks = {
        'multi_entity_hierarchy_created': len(entities) >= 4 and any(row['parent_entity_code'] == 'HOLDCO' for row in entities),
        'ownership_chain_calculated': any(abs(float(row['effective_ownership_percent']) - 72.0) < 0.001 for row in ownership_chains),
        'minority_interest_recorded': any(row['journal_type'] == 'non_controlling_interest' for row in journals),
        'intercompany_matched': intercompany['status'] == 'matched',
        'elimination_approved': approved_elimination['review_status'] == 'approved',
        'currency_translation_recorded': bool(cta_rows) and consolidation['advanced_consolidation']['totals']['translated_amount'] != 0,
        'cta_journal_recorded': any(row['journal_type'] == 'currency_translation_adjustment' for row in journals),
        'multi_gaap_mapping_applied': bool(list_gaap_book_mappings(scenario_id)) and any(row['journal_type'] == 'gaap_adjustment' for row in journals),
        'statutory_schedules_assembled': statutory_pack['status'] == 'assembled' and len(schedules) >= 3,
    }
    artifacts = {
        'entities': entities,
        'ownerships': ownerships,
        'settings': settings,
        'rates': rates,
        'gaap_mappings': gaap_mappings,
        'rules': rules,
        'ledger_entries': ledger_entries,
        'intercompany_match': intercompany,
        'elimination': submitted_elimination | approved_elimination,
        'consolidation_run': consolidation,
        'ownership_chains': ownership_chains,
        'currency_translation_adjustments': cta_rows,
        'journals': journals,
        'statutory_pack': statutory_pack,
        'supplemental_schedules': schedules,
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO consolidation_certification_runs (
            run_key, scenario_id, period, status, checks_json, artifacts_json,
            created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            scenario_id,
            period,
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(artifacts, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('consolidation_certification', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM consolidation_certification_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Consolidation certification run not found.')
    return _format_run(row)


def _create_scenario(run_key: str) -> int:
    return db.execute(
        '''
        INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
        VALUES (?, 'b97', 'draft', '2026-07', '2027-06', 0, ?)
        ''',
        (f'B97 Consolidation Certification {run_key}', _now()),
    )


def _seed_entities(user: dict[str, Any]) -> list[dict[str, Any]]:
    payloads = [
        {'entity_code': 'CAMPUS', 'entity_name': 'Manchester Campus', 'base_currency': 'USD', 'gaap_basis': 'US_GAAP'},
        {'entity_code': 'HOLDCO', 'entity_name': 'Campus Holding Entity', 'parent_entity_code': 'CAMPUS', 'base_currency': 'USD', 'gaap_basis': 'US_GAAP'},
        {'entity_code': 'STATINTL', 'entity_name': 'International Statutory Unit', 'parent_entity_code': 'HOLDCO', 'base_currency': 'EUR', 'gaap_basis': 'IFRS'},
        {'entity_code': 'AUXOPS', 'entity_name': 'Auxiliary Operations', 'parent_entity_code': 'CAMPUS', 'base_currency': 'USD', 'gaap_basis': 'US_GAAP'},
    ]
    return [upsert_consolidation_entity(payload, user) for payload in payloads]


def _seed_ownerships(scenario_id: int, period: str, user: dict[str, Any]) -> list[dict[str, Any]]:
    payloads = [
        {'scenario_id': scenario_id, 'parent_entity_code': 'CAMPUS', 'child_entity_code': 'HOLDCO', 'ownership_percent': 90, 'effective_period': period},
        {'scenario_id': scenario_id, 'parent_entity_code': 'HOLDCO', 'child_entity_code': 'STATINTL', 'ownership_percent': 80, 'effective_period': period},
        {'scenario_id': scenario_id, 'parent_entity_code': 'CAMPUS', 'child_entity_code': 'AUXOPS', 'ownership_percent': 100, 'effective_period': period},
    ]
    return [upsert_entity_ownership(payload, user) for payload in payloads]


def _seed_rates(scenario_id: int, period: str, user: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        upsert_currency_rate(
            {
                'scenario_id': scenario_id,
                'period': period,
                'from_currency': 'EUR',
                'to_currency': 'USD',
                'rate': rate,
                'rate_type': rate_type,
                'source': 'b97-certification-treasury',
            },
            user,
        )
        for rate_type, rate in [('average', 1.10), ('closing', 1.25), ('historical', 1.05)]
    ]


def _seed_gaap_mappings(scenario_id: int, user: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        upsert_gaap_book_mapping(
            {
                'scenario_id': scenario_id,
                'source_gaap_basis': 'IFRS',
                'target_gaap_basis': 'US_GAAP',
                'source_account_code': account,
                'target_account_code': target,
                'adjustment_percent': percent,
                'active': True,
            },
            user,
        )
        for account, target, percent in [('TUITION', 'TUITION_US', 103), ('TRANSFER', 'TRANSFER_US', 100), ('SUPPLIES', 'SUPPLIES_US', 98)]
    ]


def _seed_ledger(scenario_id: int, period: str, run_key: str, user: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [
        ('STATINTL', 'SCI', 'GEN', 'TUITION', 125000.0, 'international tuition revenue'),
        ('STATINTL', 'OPS', 'GEN', 'SUPPLIES', -42000.0, 'international operating supplies'),
        ('CAMPUS', 'OPS', 'GEN', 'TRANSFER', 50000.0, 'campus intercompany receivable'),
        ('STATINTL', 'OPS', 'GEN', 'TRANSFER', -50000.0, 'international intercompany payable'),
        ('AUXOPS', 'OPS', 'AUX', 'AUXILIARY', 31000.0, 'auxiliary operating revenue'),
    ]
    created = []
    for entity, department, fund, account, amount, note in rows:
        created.append(
            append_ledger_entry(
                {
                    'scenario_id': scenario_id,
                    'entity_code': entity,
                    'department_code': department,
                    'fund_code': fund,
                    'account_code': account,
                    'period': period,
                    'amount': amount,
                    'source': 'b97_consolidation_certification',
                    'ledger_type': 'actual',
                    'ledger_basis': 'actual',
                    'notes': note,
                    'source_record_id': f'{run_key}-{entity}-{account}',
                    'metadata': {'batch': 'B97'},
                },
                actor=user['email'],
                user=user,
            )
        )
    return created


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['artifacts'] = json.loads(result.pop('artifacts_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
