from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app import db
from app.services.guidance_training import start_training_mode
from app.services.ux_productivity import (
    bulk_paste_budget,
    create_notification,
    update_profile,
    validate_grid_rows,
)

ROOT = Path(__file__).resolve().parents[2]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS ux_finance_user_polish_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                artifacts_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_ux_finance_user_polish_runs_scenario
            ON ux_finance_user_polish_runs (scenario_id, completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM ux_finance_user_polish_runs ORDER BY id DESC LIMIT 1')
    asset = _asset_readiness()
    checks = {
        'simplified_navigation_ready': asset['command_deck_ready'],
        'workspace_toggles_ready': asset['workspace_toggles_ready'],
        'dock_undock_ready': asset['dock_undock_ready'],
        'import_export_discoverability_ready': asset['import_export_ready'],
        'guided_entry_ready': asset['guided_entry_ready'],
        'inline_validation_ready': asset['inline_validation_ready'],
        'keyboard_accessibility_ready': asset['keyboard_accessibility_ready'],
        'training_mode_ready': asset['training_mode_ready'],
    }
    counts = {
        'polish_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM ux_finance_user_polish_runs')['count']),
        'notifications': int(db.fetch_one('SELECT COUNT(*) AS count FROM notifications')['count']),
        'bulk_paste_imports': int(db.fetch_one('SELECT COUNT(*) AS count FROM bulk_paste_imports')['count']),
        'training_sessions': int(db.fetch_one('SELECT COUNT(*) AS count FROM training_mode_sessions')['count']),
    }
    return {
        'batch': 'B103',
        'title': 'UX Finance User Polish',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM ux_finance_user_polish_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_run(row) for row in rows]


def run_polish(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b103-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    scenario_id = int(payload.get('scenario_id') or _default_scenario_id())
    profile = update_profile(
        {
            'display_name': user.get('display_name') or user['email'],
            'default_scenario_id': scenario_id,
            'default_period': '2026-08',
            'preferences': {
                'workspace_menu': 'right_popout',
                'dock_mode': 'popout_window',
                'entry_mode': 'guided_first',
                'keyboard_shortcuts': 'focus_visible',
            },
        },
        user,
    )
    validation = validate_grid_rows(
        {
            'scenario_id': scenario_id,
            'rows': [
                {'department_code': 'SCI', 'fund_code': 'GEN', 'account_code': 'SUPPLIES', 'period': '2026-08', 'amount': '-1250'},
                {'department_code': '', 'fund_code': 'GEN', 'account_code': 'SUPPLIES', 'period': 'bad', 'amount': 'x'},
            ],
        }
    )
    bulk = bulk_paste_budget(
        {
            'scenario_id': scenario_id,
            'paste_text': 'department_code\tfund_code\taccount_code\tperiod\tamount\tnotes\nSCI\tGEN\tSUPPLIES\t2026-08\t-1250\tB103 guided import row',
        },
        user,
    )
    notification = create_notification(
        {
            'scenario_id': scenario_id,
            'notification_type': 'system',
            'title': 'Finance workspace polish ready',
            'message': 'Guided entry, validation, and training mode checks are available.',
            'severity': 'success',
            'link': '#guidance-training',
        },
        user,
    )
    training = start_training_mode({'mode_key': 'planner', 'scenario_id': scenario_id}, user)
    asset = _asset_readiness()
    checks = {
        'simplified_navigation_ready': asset['command_deck_ready'] and profile['preferences']['workspace_menu'] == 'right_popout',
        'workspace_toggles_ready': asset['workspace_toggles_ready'],
        'dock_undock_ready': asset['dock_undock_ready'],
        'import_export_discoverability_ready': asset['import_export_ready'],
        'guided_entry_ready': asset['guided_entry_ready'] and bulk['accepted_rows'] >= 1,
        'inline_validation_ready': asset['inline_validation_ready'] and validation['valid'] is False and len(validation['messages']) >= 1,
        'keyboard_accessibility_ready': asset['keyboard_accessibility_ready'],
        'training_mode_ready': asset['training_mode_ready'] and training['status'] == 'active',
    }
    artifacts = {
        'profile': profile,
        'validation': validation,
        'bulk_paste': bulk,
        'notification': notification,
        'training': training,
        'asset_readiness': asset,
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO ux_finance_user_polish_runs (
            run_key, scenario_id, status, checks_json, artifacts_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (run_key, scenario_id, status_value, json.dumps(checks, sort_keys=True), json.dumps(artifacts, sort_keys=True), user['email'], started, completed),
    )
    db.log_audit('ux_finance_user_polish', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM ux_finance_user_polish_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('UX finance polish run not found.')
    return _format_run(row)


def _default_scenario_id() -> int:
    row = db.fetch_one("SELECT id FROM scenarios WHERE name = 'FY27 Operating Plan' ORDER BY id LIMIT 1") or db.fetch_one('SELECT id FROM scenarios ORDER BY id LIMIT 1')
    if row is None:
        raise ValueError('No scenario exists for UX polish proof.')
    return int(row['id'])


def _asset_readiness() -> dict[str, bool]:
    index = (ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (ROOT / 'static' / 'app.js').read_text(encoding='utf-8')
    workspace_js = (ROOT / 'static' / 'js' / 'workspace-button-fallback.js').read_text(encoding='utf-8')
    dock_js = (ROOT / 'static' / 'js' / 'dockable-sections.js').read_text(encoding='utf-8')
    styles = (ROOT / 'static' / 'styles.css').read_text(encoding='utf-8')
    return {
        'command_deck_ready': 'class="sidebar command-deck"' in index and 'deck-footer' in index,
        'workspace_toggles_ready': 'workspaceMenuButton' in workspace_js and 'workspace-section-toggle' in workspace_js and 'aria-pressed' in workspace_js,
        'dock_undock_ready': 'window.open' in dock_js and 'dock-toggle-button' in dock_js and 'popoutContent' in dock_js,
        'import_export_ready': 'id="heroImportButton"' in index and 'id="heroExportButton"' in index and 'guidedImportButton' in index,
        'guided_entry_ready': (
            'id="guidedStart"' in index
            and 'id="guidedManualButton"' in index
            and 'id="guidedImportButton"' in index
            and 'id="guidedExportButton"' in index
            and 'handleGuidedManualSave' in app_js
            and 'handleGuidedImportRun' in app_js
            and 'handleGuidedExportRun' in app_js
        ),
        'inline_validation_ready': 'id="gridValidationMessage"' in index and '[aria-invalid="true"]' in styles,
        'keyboard_accessibility_ready': 'skip-link' in index and ':focus-visible' in styles and 'tabindex="0"' in index,
        'training_mode_ready': '/api/guidance/training/start' in app_js and 'startPlannerTrainingButton' in index,
    }


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['artifacts'] = json.loads(result.pop('artifacts_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
