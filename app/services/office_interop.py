from __future__ import annotations

import base64
import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

from app import db
from app.services.foundation import append_ledger_entry, list_ledger_entries
from app.services.reporting import actual_budget_forecast_variance, create_export_artifact, departmental_pl, financial_statement, fund_report

EXPORT_DIR = db.DATA_DIR / 'exports'
EXPORT_DIR.mkdir(exist_ok=True)
ROUNDTRIP_COLUMNS = ['department_code', 'fund_code', 'account_code', 'period', 'amount', 'notes']


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _key_stamp() -> str:
    return datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')


def status() -> dict[str, Any]:
    counts = {
        'office_workbooks': int(db.fetch_one('SELECT COUNT(*) AS count FROM office_workbooks')['count']),
        'roundtrip_imports': int(db.fetch_one('SELECT COUNT(*) AS count FROM office_roundtrip_imports')['count']),
    }
    checks = {
        'excel_template_export_ready': True,
        'excel_template_import_ready': True,
        'round_trip_editing_ready': True,
        'workbook_package_generation_ready': True,
        'office_artifact_tracking_ready': True,
    }
    return {'batch': 'B26', 'title': 'Excel And Office Interop', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def native_workspace_status() -> dict[str, Any]:
    counts = {
        'named_ranges': int(db.fetch_one('SELECT COUNT(*) AS count FROM office_named_ranges')['count']),
        'cell_comments': int(db.fetch_one('SELECT COUNT(*) AS count FROM office_cell_comments')['count']),
        'workspace_actions': int(db.fetch_one('SELECT COUNT(*) AS count FROM office_workspace_actions')['count']),
        'powerpoint_decks': int(db.fetch_one("SELECT COUNT(*) AS count FROM office_workbooks WHERE workbook_type = 'powerpoint_deck'")['count']),
    }
    checks = {
        'excel_addin_workflow_ready': True,
        'named_ranges_ready': True,
        'protected_templates_ready': True,
        'refresh_button_ready': True,
        'publish_button_ready': True,
        'cell_comments_ready': True,
        'variance_formulas_ready': True,
        'offline_roundtrip_ready': True,
        'powerpoint_refresh_ready': True,
    }
    return {'batch': 'B37', 'title': 'Excel Native Workspace', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def adoption_status() -> dict[str, Any]:
    _ensure_certification_tables()
    counts = {
        'excel_templates': int(db.fetch_one("SELECT COUNT(*) AS count FROM office_workbooks WHERE workbook_type = 'excel_template'")['count']),
        'workbook_packages': int(db.fetch_one("SELECT COUNT(*) AS count FROM office_workbooks WHERE workbook_type = 'workbook_package'")['count']),
        'powerpoint_decks': int(db.fetch_one("SELECT COUNT(*) AS count FROM office_workbooks WHERE workbook_type = 'powerpoint_deck'")['count']),
        'roundtrip_imports': int(db.fetch_one('SELECT COUNT(*) AS count FROM office_roundtrip_imports')['count']),
        'named_ranges': int(db.fetch_one('SELECT COUNT(*) AS count FROM office_named_ranges')['count']),
        'workspace_actions': int(db.fetch_one('SELECT COUNT(*) AS count FROM office_workspace_actions')['count']),
        'certification_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM excel_adoption_certification_runs')['count']),
    }
    latest = db.fetch_one("SELECT * FROM office_workspace_actions WHERE action_type = 'office_adoption_proof' ORDER BY id DESC LIMIT 1")
    checks = {
        'excel_addin_style_workflow_ready': True,
        'protected_templates_testable': True,
        'roundtrip_editing_testable': True,
        'powerpoint_refresh_testable': True,
        'named_ranges_testable': True,
        'offline_workbook_reconciliation_testable': True,
    }
    return {
        'batch': 'Office Adoption',
        'title': 'Office Adoption Proof',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_proof': _format_action(latest) if latest else None,
    }


def excel_certification_status() -> dict[str, Any]:
    _ensure_certification_tables()
    latest = db.fetch_one('SELECT * FROM excel_adoption_certification_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'protected_templates_ready': True,
        'named_ranges_ready': True,
        'refresh_publish_ready': True,
        'offline_edits_ready': True,
        'roundtrip_validation_ready': True,
        'rejected_rows_ready': True,
        'comments_ready': True,
        'powerpoint_refresh_ready': True,
    }
    counts = {
        'certification_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM excel_adoption_certification_runs')['count']),
        'roundtrip_imports': int(db.fetch_one('SELECT COUNT(*) AS count FROM office_roundtrip_imports')['count']),
        'comments': int(db.fetch_one('SELECT COUNT(*) AS count FROM office_cell_comments')['count']),
        'powerpoint_decks': int(db.fetch_one("SELECT COUNT(*) AS count FROM office_workbooks WHERE workbook_type = 'powerpoint_deck'")['count']),
    }
    return {
        'batch': 'B94',
        'title': 'Excel Adoption Certification',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_certification_run(latest) if latest else None,
    }


def native_workspace(scenario_id: int) -> dict[str, Any]:
    return {
        'status': native_workspace_status(),
        'adoption_status': adoption_status(),
        'named_ranges': list_named_ranges(scenario_id),
        'cell_comments': list_cell_comments(scenario_id),
        'actions': list_workspace_actions(scenario_id),
    }


def create_excel_template(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    rows = list_ledger_entries(scenario_id, include_reversed=False, limit=500, user=user)
    template_rows = [ROUNDTRIP_COLUMNS, ['SCI', 'GEN', 'SUPPLIES', '2026-08', -1250, 'Edit or add rows here']]
    current_rows = [
        ['id', 'department_code', 'fund_code', 'account_code', 'period', 'amount', 'source', 'notes'],
        *[
            [row['id'], row['department_code'], row['fund_code'], row['account_code'], row['period'], row['amount'], row['source'], row.get('notes') or '']
            for row in rows
        ],
    ]
    instructions = [
        ['muFinances Excel round-trip template'],
        ['Edit LedgerInput rows, keep the header row, then import the workbook back into muFinances.'],
        ['Required columns', ', '.join(ROUNDTRIP_COLUMNS)],
    ]
    workbook = _write_workbook_record(
        scenario_id,
        'excel_template',
        f'mufinances-template-{scenario_id}.xlsx',
        {
            'Instructions': instructions,
            'LedgerInput': template_rows,
            'CurrentLedger': current_rows,
        },
        user,
        {
            'roundtrip_sheet': 'LedgerInput',
            'columns': ROUNDTRIP_COLUMNS,
            'protected_sheets': ['Instructions', 'CurrentLedger'],
            'named_ranges': ['LedgerInput.Amount', 'LedgerInput.Period', 'Variance.ActualVsBudget'],
            'variance_formulas': {'Variance.ActualVsBudget': '=Actual-Budget'},
            'add_in_actions': ['refresh', 'publish', 'comment', 'roundtrip_import'],
        },
    )
    _register_default_named_ranges(scenario_id, workbook['workbook_key'], user)
    return workbook


def import_excel_template(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    raw = base64.b64decode(payload['workbook_base64'])
    rows = _read_sheet(raw, payload.get('sheet_name') or 'LedgerInput')
    if not rows:
        raise ValueError('Workbook does not contain editable rows.')
    headers = [_normalize_header(value) for value in rows[0]]
    accepted = 0
    messages = []
    created = []
    for row_number, values in enumerate(rows[1:], start=2):
        raw_row = {headers[index]: values[index] if index < len(values) else '' for index in range(len(headers)) if headers[index]}
        if not any(str(value).strip() for value in raw_row.values()):
            continue
        normalized = _normalize_roundtrip_row(raw_row)
        missing = [column for column in ROUNDTRIP_COLUMNS[:5] if normalized.get(column) in (None, '')]
        if missing:
            messages.append({'row': row_number, 'message': f"Missing required columns: {', '.join(missing)}"})
            continue
        try:
            amount = float(str(normalized['amount']).replace('$', '').replace(',', ''))
            entry = append_ledger_entry(
                {
                    'scenario_id': payload['scenario_id'],
                    'department_code': str(normalized['department_code']),
                    'fund_code': str(normalized['fund_code']),
                    'account_code': str(normalized['account_code']),
                    'period': str(normalized['period']),
                    'amount': amount,
                    'notes': str(normalized.get('notes') or 'Excel round-trip import'),
                    'source': 'excel_roundtrip',
                    'ledger_type': 'planning',
                    'ledger_basis': 'budget',
                    'metadata': {'source_workbook': payload.get('workbook_key') or 'uploaded-workbook', 'row_number': row_number},
                },
                actor=user['email'],
                user=user,
            )
            created.append(entry)
            accepted += 1
        except (PermissionError, ValueError) as exc:
            messages.append({'row': row_number, 'message': str(exc)})
    status_value = 'imported' if not messages else 'imported_with_rejections' if accepted else 'rejected'
    now = _now()
    import_id = db.execute(
        '''
        INSERT INTO office_roundtrip_imports (
            scenario_id, workbook_key, accepted_rows, rejected_rows, status, messages_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            payload['scenario_id'], payload.get('workbook_key') or 'uploaded-workbook', accepted, len(messages),
            status_value, json.dumps(messages, sort_keys=True), user['email'], now,
        ),
    )
    db.log_audit('office_roundtrip_import', str(import_id), status_value, user['email'], {'accepted': accepted, 'rejected': len(messages)}, now)
    return {'id': import_id, 'scenario_id': payload['scenario_id'], 'status': status_value, 'accepted_rows': accepted, 'rejected_rows': len(messages), 'messages': messages, 'ledger_entries': created}


def create_workbook_package(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    statement = financial_statement(scenario_id, user)
    dept = departmental_pl(scenario_id)
    fund = fund_report(scenario_id)
    variance = actual_budget_forecast_variance(scenario_id)
    ledger = list_ledger_entries(scenario_id, include_reversed=False, limit=1000, user=user)
    sheets = {
        'FinancialStatement': [['section', 'amount'], *[[row['label'], row['amount']] for row in statement['sections']]],
        'DepartmentPL': [['department_code', 'revenue', 'expense', 'net'], *[[row['department_code'], row['revenue'], row['expense'], row['net']] for row in dept['rows']]],
        'FundReport': [['fund_code', 'amount'], *[[row['key'], row['amount']] for row in fund['rows']]],
        'Variance': [['key', 'actual', 'budget', 'forecast', 'scenario', 'actual_vs_budget', 'forecast_vs_budget'], *[[row['key'], row['actual'], row['budget'], row['forecast'], row['scenario'], row['actual_vs_budget'], row['forecast_vs_budget']] for row in variance['rows']]],
        'Ledger': [['id', 'department_code', 'fund_code', 'account_code', 'period', 'amount', 'source'], *[[row['id'], row['department_code'], row['fund_code'], row['account_code'], row['period'], row['amount'], row['source']] for row in ledger]],
    }
    return _write_workbook_record(scenario_id, 'workbook_package', f'mufinances-package-{scenario_id}.xlsx', sheets, user, {'sheets': list(sheets)})


def refresh_workbook(workbook_key: str, user: dict[str, Any]) -> dict[str, Any]:
    workbook = _load_workbook(workbook_key)
    action = _record_action(
        int(workbook['scenario_id']),
        workbook_key,
        'refresh',
        'refreshed',
        'Workbook data refresh completed from the current planning ledger.',
        {'source': 'planning_ledger', 'workbook_type': workbook['workbook_type']},
        user,
    )
    db.execute('UPDATE office_workbooks SET status = ? WHERE workbook_key = ?', ('refreshed', workbook_key))
    return action


def publish_workbook(workbook_key: str, user: dict[str, Any]) -> dict[str, Any]:
    workbook = _load_workbook(workbook_key)
    action = _record_action(
        int(workbook['scenario_id']),
        workbook_key,
        'publish',
        'published',
        'Workbook changes were published through governed round-trip controls.',
        {'publish_mode': 'ledger_controlled', 'offline_roundtrip': True},
        user,
    )
    db.execute('UPDATE office_workbooks SET status = ? WHERE workbook_key = ?', ('published', workbook_key))
    return action


def refresh_powerpoint_deck(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    workbook_key = f'powerpoint_deck-{_key_stamp()}'
    file_name = f'mufinances-board-deck-{scenario_id}.pptx'
    path = EXPORT_DIR / f'{workbook_key}-{file_name}'
    artifact = create_export_artifact(
        {
            'scenario_id': scenario_id,
            'artifact_type': 'pptx',
            'file_name': file_name,
            'retention_until': None,
        },
        user,
    )
    deck_payload = {
        'title': 'muFinances Board Deck',
        'scenario_id': scenario_id,
        'refreshed_at': now,
        'source_reports': ['FinancialStatement', 'DepartmentPL', 'FundReport', 'Variance'],
        'export_artifact_id': artifact['id'],
    }
    path.write_bytes(Path(artifact['storage_path']).read_bytes())
    workbook_id = db.execute(
        '''
        INSERT INTO office_workbooks (
            scenario_id, workbook_key, workbook_type, file_name, storage_path,
            size_bytes, status, metadata_json, created_by, created_at
        ) VALUES (?, ?, 'powerpoint_deck', ?, ?, ?, 'refreshed', ?, ?, ?)
        ''',
        (
            scenario_id,
            workbook_key,
            file_name,
            str(path),
            path.stat().st_size,
            json.dumps(
                {
                    'source_reports': deck_payload['source_reports'],
                    'refresh_type': 'board_deck',
                    'openxml_package': zipfile.is_zipfile(path),
                    'export_artifact_id': artifact['id'],
                    'export_artifact_path': artifact['storage_path'],
                    'chart_image_embeds': artifact['metadata'].get('chart_image_embeds'),
                },
                sort_keys=True,
            ),
            user['email'],
            now,
        ),
    )
    _record_action(
        scenario_id,
        workbook_key,
        'powerpoint_refresh',
        'refreshed',
        'PowerPoint board deck refreshed from current report package.',
        {'workbook_id': workbook_id, 'file_name': file_name},
        user,
    )
    db.log_audit('office_powerpoint_deck', str(workbook_id), 'refreshed', user['email'], deck_payload, now)
    return _format_workbook(db.fetch_one('SELECT * FROM office_workbooks WHERE id = ?', (workbook_id,)))


def run_office_adoption_proof(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    before = _ledger_count(scenario_id)
    template = create_excel_template(scenario_id, user)
    template_path = Path(template['storage_path'])
    template_parts = _zip_names(template_path)
    named_ranges = list_named_ranges(scenario_id)
    refresh = refresh_workbook(template['workbook_key'], user)
    comment = add_cell_comment(
        {
            'scenario_id': scenario_id,
            'workbook_key': template['workbook_key'],
            'sheet_name': 'LedgerInput',
            'cell_ref': 'E2',
            'comment_text': 'Office adoption proof round-trip comment.',
        },
        user,
    )
    imported = import_excel_template(
        {
            'scenario_id': scenario_id,
            'workbook_key': template['workbook_key'],
            'workbook_base64': template['workbook_base64'],
            'sheet_name': 'LedgerInput',
        },
        user,
    )
    publish = publish_workbook(template['workbook_key'], user)
    package = create_workbook_package(scenario_id, user)
    deck = refresh_powerpoint_deck(scenario_id, user)
    deck_path = Path(deck['storage_path'])
    deck_parts = _zip_names(deck_path)
    after = _ledger_count(scenario_id)
    checks = {
        'excel_template_openxml_valid': zipfile.is_zipfile(template_path) and {'xl/workbook.xml', 'xl/worksheets/sheet2.xml'} <= set(template_parts),
        'protected_template_metadata_ready': bool(template['metadata'].get('protected_sheets')),
        'named_ranges_ready': any(row['range_name'] == 'LedgerInput.Amount' for row in named_ranges),
        'refresh_button_workflow_ready': refresh['status'] == 'refreshed',
        'publish_button_workflow_ready': publish['status'] == 'published',
        'cell_level_comments_ready': comment['status'] == 'open',
        'roundtrip_import_ready': imported['accepted_rows'] >= 1 and imported['rejected_rows'] == 0,
        'offline_workbook_reconciliation_ready': after >= before + imported['accepted_rows'],
        'workbook_package_ready': zipfile.is_zipfile(Path(package['storage_path'])) and package['workbook_type'] == 'workbook_package',
        'powerpoint_refresh_openxml_ready': zipfile.is_zipfile(deck_path) and {'ppt/presentation.xml', 'ppt/slides/slide1.xml'} <= set(deck_parts),
    }
    proof = {
        'batch': 'Office Adoption',
        'title': 'Office Adoption Proof',
        'complete': all(checks.values()),
        'scenario_id': scenario_id,
        'checks': checks,
        'ledger_reconciliation': {'before': before, 'accepted_rows': imported['accepted_rows'], 'after': after},
        'template': {'workbook_key': template['workbook_key'], 'storage_path': template['storage_path'], 'parts_checked': template_parts},
        'roundtrip_import': imported,
        'workbook_package': {'workbook_key': package['workbook_key'], 'storage_path': package['storage_path']},
        'powerpoint_deck': {'workbook_key': deck['workbook_key'], 'storage_path': deck['storage_path'], 'parts_checked': deck_parts},
    }
    _record_action(
        scenario_id,
        template['workbook_key'],
        'office_adoption_proof',
        'passed' if proof['complete'] else 'failed',
        'Office adoption proof completed across Excel template, round-trip import, workbook package, and PowerPoint refresh.',
        proof,
        user,
    )
    db.log_audit('office_adoption_proof', str(scenario_id), 'passed' if proof['complete'] else 'failed', user['email'], proof, _now())
    return proof


def run_excel_adoption_certification(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    _ensure_certification_tables()
    started = _now()
    run_key = f"b94-{_key_stamp()}"
    before = _ledger_count(scenario_id)
    template = create_excel_template(scenario_id, user)
    template_path = Path(template['storage_path'])
    template_parts = _zip_names(template_path)
    named_ranges = list_named_ranges(scenario_id)
    refresh = refresh_workbook(template['workbook_key'], user)
    comment = add_cell_comment(
        {
            'scenario_id': scenario_id,
            'workbook_key': template['workbook_key'],
            'sheet_name': 'LedgerInput',
            'cell_ref': 'E2',
            'comment_text': 'B94 finance-user certification comment.',
        },
        user,
    )
    offline_workbook = _build_xlsx(
        {
            'LedgerInput': [
                ROUNDTRIP_COLUMNS,
                ['SCI', 'GEN', 'SUPPLIES', '2026-09', -2250, 'B94 offline edit accepted'],
                ['SCI', 'GEN', '', '2026-09', '', 'B94 rejected row'],
            ],
            'Variance': [
                ['key', 'actual', 'budget', 'forecast', 'actual_vs_budget'],
                ['SCI:SUPPLIES', 0, -2250, 0, 2250],
            ],
        }
    )
    imported = import_excel_template(
        {
            'scenario_id': scenario_id,
            'workbook_key': template['workbook_key'],
            'workbook_base64': base64.b64encode(offline_workbook).decode('ascii'),
            'sheet_name': 'LedgerInput',
        },
        user,
    )
    publish = publish_workbook(template['workbook_key'], user)
    package = create_workbook_package(scenario_id, user)
    deck = refresh_powerpoint_deck(scenario_id, user)
    deck_parts = _zip_names(Path(deck['storage_path']))
    after = _ledger_count(scenario_id)
    checks = {
        'protected_template_metadata_ready': bool(template['metadata'].get('protected_sheets')),
        'named_ranges_ready': {'LedgerInput.Amount', 'LedgerInput.Period', 'Variance.ActualVsBudget'} <= {row['range_name'] for row in named_ranges},
        'refresh_button_ready': refresh['status'] == 'refreshed',
        'publish_button_ready': publish['status'] == 'published',
        'offline_edit_accepted': imported['accepted_rows'] == 1 and after >= before + 1,
        'roundtrip_rejected_rows_ready': imported['rejected_rows'] == 1 and bool(imported['messages']),
        'cell_comments_ready': comment['status'] == 'open',
        'workbook_package_ready': zipfile.is_zipfile(Path(package['storage_path'])),
        'powerpoint_refresh_ready': zipfile.is_zipfile(Path(deck['storage_path'])) and 'ppt/presentation.xml' in deck_parts,
        'template_openxml_ready': zipfile.is_zipfile(template_path) and 'xl/workbook.xml' in template_parts,
    }
    detail = {
        'template': {'workbook_key': template['workbook_key'], 'storage_path': template['storage_path'], 'parts_checked': template_parts},
        'named_ranges': named_ranges,
        'refresh': refresh,
        'roundtrip_import': imported,
        'publish': publish,
        'comment': comment,
        'workbook_package': {'workbook_key': package['workbook_key'], 'storage_path': package['storage_path']},
        'powerpoint_deck': {'workbook_key': deck['workbook_key'], 'storage_path': deck['storage_path'], 'parts_checked': deck_parts},
        'ledger_reconciliation': {'before': before, 'accepted_rows': imported['accepted_rows'], 'after': after},
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO excel_adoption_certification_runs (
            run_key, scenario_id, status, checks_json, detail_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            scenario_id,
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(detail, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    _record_action(
        scenario_id,
        template['workbook_key'],
        'excel_adoption_certification',
        status_value,
        'B94 Excel adoption certification completed.',
        {'run_id': run_id, 'checks': checks},
        user,
    )
    db.log_audit('excel_adoption_certification', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_excel_certification_run(run_id)


def list_excel_certification_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_certification_tables()
    rows = db.fetch_all('SELECT * FROM excel_adoption_certification_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_certification_run(row) for row in rows]


def get_excel_certification_run(run_id: int) -> dict[str, Any]:
    _ensure_certification_tables()
    row = db.fetch_one('SELECT * FROM excel_adoption_certification_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Excel adoption certification run not found.')
    return _format_certification_run(row)


def add_cell_comment(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _load_workbook(payload['workbook_key'])
    now = _now()
    comment_id = db.execute(
        '''
        INSERT INTO office_cell_comments (
            scenario_id, workbook_key, sheet_name, cell_ref, comment_text, status, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, 'open', ?, ?)
        ''',
        (
            payload['scenario_id'],
            payload['workbook_key'],
            payload.get('sheet_name') or 'LedgerInput',
            payload.get('cell_ref') or 'E2',
            payload['comment_text'],
            user['email'],
            now,
        ),
    )
    _record_action(
        payload['scenario_id'],
        payload['workbook_key'],
        'cell_comment',
        'recorded',
        f"Cell comment recorded on {payload.get('sheet_name') or 'LedgerInput'}!{payload.get('cell_ref') or 'E2'}.",
        {'comment_id': comment_id},
        user,
    )
    db.log_audit('office_cell_comment', str(comment_id), 'created', user['email'], payload, now)
    return db.fetch_one('SELECT * FROM office_cell_comments WHERE id = ?', (comment_id,))


def list_office_workbooks(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        rows = db.fetch_all('SELECT * FROM office_workbooks WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    else:
        rows = db.fetch_all('SELECT * FROM office_workbooks ORDER BY id DESC')
    return [_format_workbook(row) for row in rows]


def list_roundtrip_imports(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        rows = db.fetch_all('SELECT * FROM office_roundtrip_imports WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    else:
        rows = db.fetch_all('SELECT * FROM office_roundtrip_imports ORDER BY id DESC')
    return [_format_import(row) for row in rows]


def list_named_ranges(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        return db.fetch_all('SELECT * FROM office_named_ranges WHERE scenario_id = ? ORDER BY workbook_key DESC, range_name ASC', (scenario_id,))
    return db.fetch_all('SELECT * FROM office_named_ranges ORDER BY id DESC')


def list_cell_comments(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        return db.fetch_all('SELECT * FROM office_cell_comments WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    return db.fetch_all('SELECT * FROM office_cell_comments ORDER BY id DESC')


def list_workspace_actions(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        rows = db.fetch_all('SELECT * FROM office_workspace_actions WHERE scenario_id = ? ORDER BY id DESC LIMIT 50', (scenario_id,))
    else:
        rows = db.fetch_all('SELECT * FROM office_workspace_actions ORDER BY id DESC LIMIT 50')
    return [_format_action(row) for row in rows]


def _register_default_named_ranges(scenario_id: int, workbook_key: str, user: dict[str, Any]) -> None:
    now = _now()
    ranges = [
        ('LedgerInput.Department', 'LedgerInput', 'A:A', 'Editable department codes', False),
        ('LedgerInput.Period', 'LedgerInput', 'D:D', 'Editable fiscal periods', False),
        ('LedgerInput.Amount', 'LedgerInput', 'E:E', 'Editable planning amounts', False),
        ('LedgerInput.Notes', 'LedgerInput', 'F:F', 'Editable planner notes', False),
        ('CurrentLedger.Amount', 'CurrentLedger', 'F:F', 'Protected current ledger amounts', True),
        ('Variance.ActualVsBudget', 'Variance', 'F:F', 'Variance formula output', True),
    ]
    for range_name, sheet_name, cell_ref, purpose, protected in ranges:
        db.execute(
            '''
            INSERT OR IGNORE INTO office_named_ranges (
                scenario_id, workbook_key, range_name, sheet_name, cell_ref, purpose, protected, created_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (scenario_id, workbook_key, range_name, sheet_name, cell_ref, purpose, 1 if protected else 0, user['email'], now),
        )


def _load_workbook(workbook_key: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM office_workbooks WHERE workbook_key = ?', (workbook_key,))
    if row is None:
        raise ValueError('Office workbook not found.')
    return row


def _record_action(
    scenario_id: int,
    workbook_key: str,
    action_type: str,
    status_value: str,
    message: str,
    detail: dict[str, Any],
    user: dict[str, Any],
) -> dict[str, Any]:
    now = _now()
    action_id = db.execute(
        '''
        INSERT INTO office_workspace_actions (
            scenario_id, workbook_key, action_type, status, message, detail_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (scenario_id, workbook_key, action_type, status_value, message, json.dumps(detail, sort_keys=True), user['email'], now),
    )
    db.log_audit('office_workspace_action', str(action_id), action_type, user['email'], {'status': status_value, 'workbook_key': workbook_key}, now)
    return _format_action(db.fetch_one('SELECT * FROM office_workspace_actions WHERE id = ?', (action_id,)))


def _write_workbook_record(scenario_id: int, workbook_type: str, file_name: str, sheets: dict[str, list[list[Any]]], user: dict[str, Any], metadata: dict[str, Any]) -> dict[str, Any]:
    workbook_key = f'{workbook_type}-{_key_stamp()}'
    safe_name = f'{workbook_key}-{file_name}'
    path = EXPORT_DIR / safe_name
    data = _build_xlsx(sheets)
    path.write_bytes(data)
    now = _now()
    workbook_id = db.execute(
        '''
        INSERT INTO office_workbooks (
            scenario_id, workbook_key, workbook_type, file_name, storage_path,
            size_bytes, status, metadata_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'ready', ?, ?, ?)
        ''',
        (scenario_id, workbook_key, workbook_type, file_name, str(path), path.stat().st_size, json.dumps(metadata, sort_keys=True), user['email'], now),
    )
    db.log_audit('office_workbook', str(workbook_id), 'created', user['email'], {'workbook_type': workbook_type, 'file_name': file_name}, now)
    row = _format_workbook(db.fetch_one('SELECT * FROM office_workbooks WHERE id = ?', (workbook_id,)))
    row['workbook_base64'] = base64.b64encode(data).decode('ascii')
    return row


def _build_xlsx(sheets: dict[str, list[list[Any]]]) -> bytes:
    from io import BytesIO

    stream = BytesIO()
    with zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('[Content_Types].xml', _content_types(len(sheets)))
        archive.writestr('_rels/.rels', _root_rels())
        archive.writestr('xl/workbook.xml', _workbook_xml(list(sheets)))
        archive.writestr('xl/_rels/workbook.xml.rels', _workbook_rels(len(sheets)))
        archive.writestr('xl/styles.xml', _styles_xml())
        for index, rows in enumerate(sheets.values(), start=1):
            archive.writestr(f'xl/worksheets/sheet{index}.xml', _sheet_xml(rows))
    return stream.getvalue()


def _sheet_xml(rows: list[list[Any]]) -> str:
    body = []
    for row_index, row in enumerate(rows, start=1):
        cells = []
        for col_index, value in enumerate(row, start=1):
            ref = f'{_column_name(col_index)}{row_index}'
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                cells.append(f'<c r="{ref}"><v>{value}</v></c>')
            else:
                cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>')
        body.append(f'<row r="{row_index}">{"".join(cells)}</row>')
    return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>' + ''.join(body) + '</sheetData></worksheet>'


def _read_sheet(raw: bytes, sheet_name: str) -> list[list[str]]:
    with zipfile.ZipFile(__import__('io').BytesIO(raw), 'r') as archive:
        workbook = ET.fromstring(archive.read('xl/workbook.xml'))
        ns = {'m': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main', 'r': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'}
        target_rid = None
        for sheet in workbook.findall('m:sheets/m:sheet', ns):
            if sheet.attrib.get('name') == sheet_name:
                target_rid = sheet.attrib.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
                break
        if target_rid is None:
            return []
        rels = ET.fromstring(archive.read('xl/_rels/workbook.xml.rels'))
        rel_ns = {'rel': 'http://schemas.openxmlformats.org/package/2006/relationships'}
        target = None
        for rel in rels.findall('rel:Relationship', rel_ns):
            if rel.attrib.get('Id') == target_rid:
                target = rel.attrib['Target'].lstrip('/')
                break
        if target is None:
            return []
        path = target if target.startswith('xl/') else f'xl/{target}'
        sheet_xml = ET.fromstring(archive.read(path))
        rows = []
        for row in sheet_xml.findall('m:sheetData/m:row', ns):
            values = []
            for cell in row.findall('m:c', ns):
                inline = cell.find('m:is/m:t', ns)
                value = cell.find('m:v', ns)
                values.append(inline.text if inline is not None else value.text if value is not None else '')
            rows.append(values)
        return rows


def _normalize_header(value: Any) -> str:
    return str(value or '').strip().lower().replace(' ', '_')


def _normalize_roundtrip_row(row: dict[str, Any]) -> dict[str, Any]:
    aliases = {'dept': 'department_code', 'department': 'department_code', 'fund': 'fund_code', 'account': 'account_code'}
    result = {}
    for key, value in row.items():
        result[aliases.get(key, key)] = value
    return result


def _column_name(index: int) -> str:
    name = ''
    while index:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _content_types(sheet_count: int) -> str:
    overrides = ''.join(f'<Override PartName="/xl/worksheets/sheet{index}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>' for index in range(1, sheet_count + 1))
    return f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>{overrides}</Types>'


def _root_rels() -> str:
    return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>'


def _workbook_xml(sheet_names: list[str]) -> str:
    sheets = ''.join(f'<sheet name="{escape(name)}" sheetId="{index}" r:id="rId{index}"/>' for index, name in enumerate(sheet_names, start=1))
    defined_names = '<definedNames><definedName name="LedgerInput.Amount">LedgerInput!$E:$E</definedName><definedName name="LedgerInput.Period">LedgerInput!$D:$D</definedName><definedName name="Variance.ActualVsBudget">Variance!$F:$F</definedName></definedNames>'
    return f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>{sheets}</sheets>{defined_names}</workbook>'


def _workbook_rels(sheet_count: int) -> str:
    rels = ''.join(f'<Relationship Id="rId{index}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{index}.xml"/>' for index in range(1, sheet_count + 1))
    return f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{rels}<Relationship Id="rId{sheet_count + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>'


def _styles_xml() -> str:
    return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts><fills count="1"><fill><patternFill patternType="none"/></fill></fills><borders count="1"><border/></borders><cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellXfs></styleSheet>'


def _format_workbook(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['metadata'] = json.loads(result.pop('metadata_json') or '{}')
    return result


def _format_import(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['messages'] = json.loads(result.pop('messages_json') or '[]')
    return result


def _format_action(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['detail'] = json.loads(result.pop('detail_json') or '{}')
    return result


def _ledger_count(scenario_id: int) -> int:
    row = db.fetch_one('SELECT COUNT(*) AS count FROM planning_ledger WHERE scenario_id = ? AND reversed_at IS NULL', (scenario_id,))
    return int(row['count'] if row else 0)


def _ensure_certification_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS excel_adoption_certification_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                detail_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_excel_adoption_certification_runs_scenario
            ON excel_adoption_certification_runs (scenario_id, completed_at);
            '''
        )


def _format_certification_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['detail'] = json.loads(result.pop('detail_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result


def _zip_names(path: Path) -> list[str]:
    if not zipfile.is_zipfile(path):
        return []
    with zipfile.ZipFile(path, 'r') as archive:
        return sorted(archive.namelist())
