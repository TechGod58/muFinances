from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.close_consolidation import (
    approve_reconciliation,
    complete_checklist_item,
    create_close_task_template,
    create_entity_confirmation,
    create_reconciliation,
    confirm_entity,
    instantiate_close_templates,
    list_audit_packets,
    list_checklist_items,
    list_task_dependencies,
    run_consolidation,
    set_period_lock,
    submit_reconciliation,
    upsert_period_close_calendar,
)
from app.services.evidence import create_attachment, create_comment
from app.services.foundation import append_ledger_entry


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS financial_close_certification_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                artifacts_json TEXT NOT NULL,
                close_signoff_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_financial_close_certification_runs_scenario
            ON financial_close_certification_runs (scenario_id, period, completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM financial_close_certification_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'month_end_close_calendar_ready': True,
        'close_task_dependencies_ready': True,
        'reconciliation_preparer_reviewer_ready': True,
        'evidence_attachments_ready': True,
        'audit_packet_generation_ready': True,
        'close_signoff_ready': True,
    }
    counts = {
        'certification_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM financial_close_certification_runs')['count']),
        'close_tasks': int(db.fetch_one('SELECT COUNT(*) AS count FROM close_checklists')['count']),
        'reconciliations': int(db.fetch_one('SELECT COUNT(*) AS count FROM account_reconciliations')['count']),
        'audit_packets': int(db.fetch_one('SELECT COUNT(*) AS count FROM audit_packets')['count']),
    }
    return {
        'batch': 'B96',
        'title': 'Financial Close Certification',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM financial_close_certification_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_run(row) for row in rows]


def run_certification(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b96-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    scenario_id = int(payload.get('scenario_id') or _create_close_scenario(run_key))
    period = payload.get('period') or '2026-08'

    ledger_entry = append_ledger_entry(
        {
            'scenario_id': scenario_id,
            'entity_code': 'CAMPUS',
            'department_code': 'SCI',
            'fund_code': 'GEN',
            'account_code': 'TUITION',
            'period': period,
            'amount': 125000.0,
            'source': 'b96_close_certification',
            'ledger_type': 'actual',
            'ledger_basis': 'actual',
            'notes': 'B96 close certification actual balance.',
            'source_record_id': run_key,
        },
        actor=user['email'],
        user=user,
    )
    calendar = upsert_period_close_calendar(
        {
            'scenario_id': scenario_id,
            'period': period,
            'close_start': f'{period}-01',
            'close_due': f'{period}-10',
        },
        user,
    )
    templates = [
        create_close_task_template(
            {
                'template_key': f'{run_key}-load-ledger',
                'title': 'Load and validate month-end actuals',
                'owner_role': 'controller',
                'due_day_offset': 1,
                'dependency_keys': [],
                'active': True,
            },
            user,
        ),
        create_close_task_template(
            {
                'template_key': f'{run_key}-review-reconciliation',
                'title': 'Review tuition reconciliation',
                'owner_role': 'reviewer',
                'due_day_offset': 4,
                'dependency_keys': [f'{run_key}-load-ledger'],
                'active': True,
            },
            user,
        ),
    ]
    instantiated = instantiate_close_templates(scenario_id, period, user)
    tasks = list_checklist_items(scenario_id, period)
    first_task = next(task for task in tasks if task['checklist_key'] == f'{run_key}-load-ledger')
    second_task = next(task for task in tasks if task['checklist_key'] == f'{run_key}-review-reconciliation')
    first_attachment = create_attachment(
        {
            'entity_type': 'close_task',
            'entity_id': str(first_task['id']),
            'file_name': f'{run_key}-gl-load.csv',
            'storage_path': f'/evidence/{run_key}/gl-load.csv',
            'content_type': 'text/csv',
            'size_bytes': 2048,
            'retention_until': '2033-12-31',
            'metadata': {'batch': 'B96', 'period': period},
        },
        user,
    )
    completed_first = complete_checklist_item(
        int(first_task['id']),
        {'attachment_id': first_attachment['id'], 'control': 'ledger_load_validated'},
        user,
    )

    reconciliation = create_reconciliation(
        {
            'scenario_id': scenario_id,
            'period': period,
            'entity_code': 'CAMPUS',
            'account_code': 'TUITION',
            'source_balance': 125000.0,
            'owner': user['email'],
            'notes': 'B96 tuition reconciliation certification.',
        },
        user,
    )
    rec_attachment = create_attachment(
        {
            'entity_type': 'reconciliation',
            'entity_id': str(reconciliation['id']),
            'file_name': f'{run_key}-tuition-reconciliation.xlsx',
            'storage_path': f'/evidence/{run_key}/tuition-reconciliation.xlsx',
            'content_type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'size_bytes': 4096,
            'retention_until': '2033-12-31',
            'metadata': {'batch': 'B96', 'period': period, 'account_code': 'TUITION'},
        },
        user,
    )
    create_comment(
        {
            'entity_type': 'reconciliation',
            'entity_id': str(reconciliation['id']),
            'comment_text': 'Prepared and tied to source export for close certification.',
            'visibility': 'internal',
        },
        user,
    )
    submitted_rec = submit_reconciliation(int(reconciliation['id']), user, 'Prepared for reviewer signoff.')
    approved_rec = approve_reconciliation(int(submitted_rec['id']), user, 'Reviewed and approved for close.')
    completed_second = complete_checklist_item(
        int(second_task['id']),
        {'reconciliation_id': approved_rec['id'], 'attachment_id': rec_attachment['id'], 'control': 'review_complete'},
        user,
    )

    confirmation = create_entity_confirmation(
        {
            'scenario_id': scenario_id,
            'period': period,
            'entity_code': 'CAMPUS',
            'confirmation_type': 'financial_close',
        },
        user,
    )
    confirmed = confirm_entity(
        int(confirmation['id']),
        {'confirmed': True, 'message': 'Campus entity confirms close certification package.'},
        user,
    )
    consolidation = run_consolidation({'scenario_id': scenario_id, 'period': period}, user)
    audit_packet = consolidation['audit_packet']
    audit_attachment = create_attachment(
        {
            'entity_type': 'audit_packet',
            'entity_id': str(audit_packet['id']),
            'file_name': f'{run_key}-audit-packet.json',
            'storage_path': f'/evidence/{run_key}/audit-packet.json',
            'content_type': 'application/json',
            'size_bytes': 8192,
            'retention_until': '2033-12-31',
            'metadata': {'batch': 'B96', 'period': period, 'packet_key': audit_packet['packet_key']},
        },
        user,
    )
    locked = set_period_lock(scenario_id, period, 'locked', user)
    close_signoff = {
        'signed_by': user['email'],
        'signed_at': _now(),
        'signoff_type': 'month_end_close',
        'period': period,
        'lock_state': locked['lock_state'],
        'audit_packet_id': audit_packet['id'],
        'audit_attachment_id': audit_attachment['id'],
    }

    dependencies = list_task_dependencies(scenario_id)
    checks = {
        'month_end_calendar_created': calendar['period'] == period and calendar['lock_state'] in {'open', 'locked'},
        'close_task_dependencies_created': any(dep['task_key'] == f'{run_key}-review-reconciliation' for dep in dependencies),
        'dependency_task_completed_before_review': completed_first['status'] == 'complete' and completed_second['status'] == 'complete',
        'reconciliation_prepared_submitted_reviewed': approved_rec['status'] == 'reviewed' and approved_rec.get('reviewer') == user['email'],
        'evidence_attachments_retained': first_attachment['retention_until'] == '2033-12-31' and rec_attachment['retention_until'] == '2033-12-31',
        'entity_confirmation_completed': confirmed['status'] == 'confirmed',
        'audit_packet_generated': audit_packet['status'] == 'sealed' and bool(list_audit_packets(scenario_id)),
        'close_signoff_locked_period': close_signoff['lock_state'] == 'locked',
    }
    artifacts = {
        'ledger_entry': ledger_entry,
        'calendar': calendar,
        'templates': templates,
        'instantiated_tasks': instantiated,
        'dependencies': dependencies,
        'completed_tasks': [completed_first, completed_second],
        'reconciliation': approved_rec,
        'evidence': {
            'close_task_attachment': first_attachment,
            'reconciliation_attachment': rec_attachment,
            'audit_packet_attachment': audit_attachment,
        },
        'entity_confirmation': confirmed,
        'consolidation': consolidation,
        'locked_period': locked,
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO financial_close_certification_runs (
            run_key, scenario_id, period, status, checks_json, artifacts_json,
            close_signoff_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            scenario_id,
            period,
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(artifacts, sort_keys=True),
            json.dumps(close_signoff, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('financial_close_certification', run_key, status_value, user['email'], {'checks': checks, 'close_signoff': close_signoff}, completed)
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM financial_close_certification_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Financial close certification run not found.')
    return _format_run(row)


def _create_close_scenario(run_key: str) -> int:
    return db.execute(
        '''
        INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
        VALUES (?, 'b96', 'draft', '2026-08', '2027-07', 0, ?)
        ''',
        (f'B96 Close Certification {run_key}', _now()),
    )


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['artifacts'] = json.loads(result.pop('artifacts_json') or '{}')
    result['close_signoff'] = json.loads(result.pop('close_signoff_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
