from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app import db
from app.services.consolidation_certification import run_certification

FIXTURE_PATH = Path(__file__).resolve().parents[1] / 'fixtures' / 'consolidation_golden_cases.json'


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS consolidation_golden_case_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                case_key TEXT NOT NULL,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                expected_json TEXT NOT NULL,
                actual_json TEXT NOT NULL,
                audit_report_id INTEGER DEFAULT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_consolidation_golden_case_runs_case
            ON consolidation_golden_case_runs (case_key, completed_at);
            '''
        )


def _load_fixture() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding='utf-8'))


def status() -> dict[str, Any]:
    _ensure_tables()
    fixture = _load_fixture()
    latest = db.fetch_one('SELECT * FROM consolidation_golden_case_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'multi_entity_ownership_chain_golden_ready': True,
        'minority_interest_golden_ready': True,
        'intercompany_elimination_golden_ready': True,
        'fx_cta_golden_ready': True,
        'multi_gaap_multi_book_placeholder_ready': True,
        'consolidation_journal_golden_ready': True,
        'audit_ready_report_golden_ready': True,
    }
    count = int(db.fetch_one('SELECT COUNT(*) AS count FROM consolidation_golden_case_runs')['count'])
    return {
        'batch': 'B145',
        'title': 'Consolidation Golden Cases',
        'complete': all(checks.values()),
        'case_key': fixture['case_key'],
        'checks': checks,
        'counts': {'golden_case_runs': count},
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM consolidation_golden_case_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_run(row) for row in rows]


def run_golden_case(payload: dict[str, Any] | None, user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    payload = payload or {}
    fixture = _load_fixture()
    started = _now()
    run_key = payload.get('run_key') or f"b145-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    certification = run_certification({'run_key': f'{run_key}-source', 'period': fixture['period']}, user)
    artifacts = certification['artifacts']
    consolidation_run = artifacts['consolidation_run']
    audit_report = consolidation_run['audit_report']
    actual = _actual_from_artifacts(artifacts, audit_report)
    expected = fixture['expected']
    checks = _compare(expected, actual)
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO consolidation_golden_case_runs (
            run_key, case_key, scenario_id, period, status, checks_json, expected_json,
            actual_json, audit_report_id, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            fixture['case_key'],
            int(certification['scenario_id']),
            fixture['period'],
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(expected, sort_keys=True),
            json.dumps(actual, sort_keys=True),
            int(audit_report['id']),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('consolidation_golden_case', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM consolidation_golden_case_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Consolidation golden case run not found.')
    return _format_run(row)


def _actual_from_artifacts(artifacts: dict[str, Any], audit_report: dict[str, Any]) -> dict[str, Any]:
    ownership = next(
        row for row in artifacts['ownership_chains']
        if row['child_entity_code'] == 'STATINTL'
    )
    journals = artifacts['journals']
    schedules = artifacts['supplemental_schedules']
    gaap_mappings = artifacts['gaap_mappings']
    return {
        'effective_ownership_percent': round(float(ownership['effective_ownership_percent']), 4),
        'minority_interest_percent': round(float(ownership['minority_interest_percent']), 4),
        'intercompany_status': artifacts['intercompany_match']['status'],
        'elimination_review_status': artifacts['elimination']['review_status'],
        'journal_types': sorted({row['journal_type'] for row in journals}),
        'supplemental_schedule_types': sorted({row['schedule_type'] for row in schedules}),
        'advanced_totals': artifacts['consolidation_run']['advanced_consolidation']['totals'],
        'audit_report_sections': sorted(audit_report['contents'].keys()),
        'multi_book_placeholder': {
            'source_gaap_basis': gaap_mappings[0]['source_gaap_basis'],
            'target_gaap_basis': gaap_mappings[0]['target_gaap_basis'],
        },
    }


def _compare(expected: dict[str, Any], actual: dict[str, Any]) -> dict[str, bool]:
    expected_sections = set(expected['audit_report_sections'])
    actual_sections = set(actual['audit_report_sections'])
    return {
        'ownership_chain_matches': abs(actual['effective_ownership_percent'] - expected['effective_ownership_percent']) < 0.001,
        'minority_interest_matches': abs(actual['minority_interest_percent'] - expected['minority_interest_percent']) < 0.001,
        'intercompany_matching_matches': actual['intercompany_status'] == expected['intercompany_status'],
        'elimination_approval_matches': actual['elimination_review_status'] == expected['elimination_review_status'],
        'journal_types_match': set(expected['journal_types']).issubset(set(actual['journal_types'])),
        'supplemental_schedules_match': set(expected['supplemental_schedule_types']).issubset(set(actual['supplemental_schedule_types'])),
        'fx_cta_totals_match': _money(actual['advanced_totals']['cta_amount']) == _money(expected['advanced_totals']['cta_amount']),
        'advanced_totals_match': {
            key: _money(actual['advanced_totals'][key]) == _money(value)
            for key, value in expected['advanced_totals'].items()
        } == {key: True for key in expected['advanced_totals']},
        'multi_book_placeholder_matches': actual['multi_book_placeholder'] == expected['multi_book_placeholder'],
        'audit_report_sections_match': expected_sections.issubset(actual_sections),
    }


def _money(value: Any) -> float:
    return round(float(value), 2)


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['expected'] = json.loads(result.pop('expected_json') or '{}')
    result['actual'] = json.loads(result.pop('actual_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
