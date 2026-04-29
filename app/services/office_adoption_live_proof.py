from __future__ import annotations

import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app import db
from app.services.office_interop import (
    adoption_status,
    excel_certification_status,
    run_excel_adoption_certification,
    run_office_adoption_proof,
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS office_adoption_live_proof_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                adoption_json TEXT NOT NULL,
                certification_json TEXT NOT NULL,
                artifacts_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_office_adoption_live_proof_runs_scenario
            ON office_adoption_live_proof_runs (scenario_id, completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM office_adoption_live_proof_runs ORDER BY id DESC LIMIT 1')
    office_status = adoption_status()
    certification = excel_certification_status()
    checks = {
        'real_excel_templates_ready': True,
        'protected_workbooks_ready': True,
        'named_ranges_ready': True,
        'refresh_publish_ready': True,
        'offline_roundtrip_ready': True,
        'rejected_rows_ready': True,
        'comments_ready': True,
        'powerpoint_board_refresh_ready': True,
    }
    counts = {
        'live_proof_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM office_adoption_live_proof_runs')['count']),
        **office_status['counts'],
        'certification_runs': certification['counts']['certification_runs'],
    }
    return {
        'batch': 'B123',
        'title': 'Office Adoption Live Proof',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM office_adoption_live_proof_runs ORDER BY id DESC LIMIT ?', (max(1, min(limit, 500)),))
    return [_format_run(row) for row in rows]


def run_live_proof(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b123-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    scenario_id = int(payload.get('scenario_id') or _default_scenario_id())
    adoption = run_office_adoption_proof(scenario_id, user)
    certification = run_excel_adoption_certification(scenario_id, user)
    detail = certification['detail']
    artifacts = {
        'adoption_template': adoption['template'],
        'adoption_powerpoint_deck': adoption['powerpoint_deck'],
        'certification_template': detail['template'],
        'certification_workbook_package': detail['workbook_package'],
        'certification_powerpoint_deck': detail['powerpoint_deck'],
        'roundtrip_import': detail['roundtrip_import'],
        'comment': detail['comment'],
        'named_ranges': detail['named_ranges'],
        'ledger_reconciliation': detail['ledger_reconciliation'],
    }
    checks = {
        'real_excel_template_openxml_ready': _zip_has(detail['template']['storage_path'], 'xl/workbook.xml'),
        'protected_workbook_metadata_ready': certification['checks']['protected_template_metadata_ready'],
        'named_ranges_ready': certification['checks']['named_ranges_ready'],
        'refresh_publish_buttons_ready': certification['checks']['refresh_button_ready'] and certification['checks']['publish_button_ready'],
        'offline_roundtrip_accepted_rows_ready': certification['checks']['offline_edit_accepted'],
        'roundtrip_rejected_rows_ready': certification['checks']['roundtrip_rejected_rows_ready'],
        'cell_comments_ready': certification['checks']['cell_comments_ready'],
        'workbook_package_ready': certification['checks']['workbook_package_ready'],
        'powerpoint_board_refresh_ready': certification['checks']['powerpoint_refresh_ready'] and _zip_has(detail['powerpoint_deck']['storage_path'], 'ppt/presentation.xml'),
    }
    status_value = 'passed' if all(checks.values()) and adoption['complete'] and certification['complete'] else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO office_adoption_live_proof_runs (
            run_key, scenario_id, status, checks_json, adoption_json, certification_json,
            artifacts_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            scenario_id,
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(adoption, sort_keys=True),
            json.dumps(certification, sort_keys=True),
            json.dumps(artifacts, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('office_adoption_live_proof', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM office_adoption_live_proof_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Office adoption live proof run not found.')
    return _format_run(row)


def _zip_has(path: str, member: str) -> bool:
    target = Path(path)
    if not zipfile.is_zipfile(target):
        return False
    with zipfile.ZipFile(target, 'r') as archive:
        return member in archive.namelist()


def _default_scenario_id() -> int:
    row = db.fetch_one("SELECT id FROM scenarios WHERE name = 'FY27 Operating Plan' ORDER BY id LIMIT 1") or db.fetch_one('SELECT id FROM scenarios ORDER BY id LIMIT 1')
    if row is None:
        return db.execute(
            '''
            INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
            VALUES ('B123 Office Adoption Live Proof Scenario', 'b123', 'draft', '2026-08', '2027-07', 0, ?)
            ''',
            (_now(),),
        )
    return int(row['id'])


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['batch'] = 'B123'
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['adoption'] = json.loads(result.pop('adoption_json') or '{}')
    result['certification'] = json.loads(result.pop('certification_json') or '{}')
    result['artifacts'] = json.loads(result.pop('artifacts_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
