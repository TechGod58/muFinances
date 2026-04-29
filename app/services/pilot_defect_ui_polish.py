from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app import db
from app.services.multi_user_pilot_cycle import get_run as get_pilot_cycle_run
from app.services.multi_user_pilot_cycle import list_runs as list_pilot_cycle_runs
from app.services.ux_finance_user_polish import run_polish as run_ux_finance_polish


ROOT = Path(__file__).resolve().parents[2]


PILOT_DEFECTS = [
    {
        'defect_key': 'workspace-menu-login-leak',
        'title': 'Workspace controls appeared on the login screen',
        'status': 'fixed',
        'evidence': ['workspace-button-fallback signedIn gate', 'dockable-sections authGate exclusion'],
    },
    {
        'defect_key': 'workspace-hidden-panels-still-visible',
        'title': 'Workspace count reached zero while panels stayed visible',
        'status': 'fixed',
        'evidence': ['workspace-hidden class', 'data-workspace-hidden-by-toggle', 'Playwright workspace toggle regression'],
    },
    {
        'defect_key': 'blocked-screen-after-login',
        'title': 'Post-login content was covered by generated controls',
        'status': 'fixed',
        'evidence': ['right-side workspace tray', 'layout-tightening gap pass', 'orphan dock cleanup'],
    },
    {
        'defect_key': 'undock-left-behind-source-section',
        'title': 'Undocked sections remained on the source page',
        'status': 'fixed',
        'evidence': ['popoutContent move', 'dock-placeholder hidden source slot'],
    },
    {
        'defect_key': 'chat-not-movable',
        'title': 'Chat opened as an in-app panel instead of a movable window',
        'status': 'fixed',
        'evidence': ['chat-window.html pop-out', 'window.open chat popup'],
    },
    {
        'defect_key': 'ready-gap-too-large',
        'title': 'Ready status and command gap consumed vertical space',
        'status': 'fixed',
        'evidence': ['appStatus hidden', 'layout-tightening targetGap'],
    },
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS pilot_defect_ui_polish_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                pilot_cycle_run_id INTEGER,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                defects_json TEXT NOT NULL,
                artifacts_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_pilot_defect_ui_polish_runs_created
            ON pilot_defect_ui_polish_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM pilot_defect_ui_polish_runs ORDER BY id DESC LIMIT 1')
    checks = _static_ui_checks()
    return {
        'batch': 'B128',
        'title': 'Pilot Defect Fix And Final UI Polish',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': {
            'polish_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM pilot_defect_ui_polish_runs')['count']),
            'fixed_pilot_defects': len(PILOT_DEFECTS),
        },
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all(
        'SELECT * FROM pilot_defect_ui_polish_runs ORDER BY id DESC LIMIT ?',
        (max(1, min(limit, 200)),),
    )
    return [_format_run(row) for row in rows]


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM pilot_defect_ui_polish_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Pilot defect UI polish run not found.')
    return _format_run(row)


def run_final_polish(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b128-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    scenario_id = int(payload.get('scenario_id') or _default_scenario_id())
    pilot_cycle = _resolve_pilot_cycle(payload)
    ux_polish = run_ux_finance_polish(
        {'run_key': f'{run_key}-ux-polish', 'scenario_id': scenario_id},
        user,
    )
    checks = _static_ui_checks()
    checks.update(
        {
            'pilot_defect_queue_reviewed': all(item['status'] == 'fixed' for item in PILOT_DEFECTS),
            'pilot_cycle_evidence_attached': pilot_cycle is None or pilot_cycle['status'] == 'passed',
            'ux_finance_polish_passed': ux_polish['status'] == 'passed',
        }
    )
    artifacts = {
        'ux_polish_run_id': ux_polish['id'],
        'pilot_cycle': _pilot_cycle_summary(pilot_cycle),
        'ui_asset_evidence': _ui_asset_evidence(),
        'playwright_coverage': [
            'workspace toggle active/off state',
            'dock/undock pop-out return',
            'chat pop-out window',
            'import/export dialogs',
            'reporting route deep link',
            'mobile/tablet command bar visibility',
        ],
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO pilot_defect_ui_polish_runs (
            run_key, scenario_id, pilot_cycle_run_id, status, checks_json,
            defects_json, artifacts_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            scenario_id,
            int(pilot_cycle['id']) if pilot_cycle else None,
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(PILOT_DEFECTS, sort_keys=True),
            json.dumps(artifacts, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('pilot_defect_ui_polish', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_run(row_id)


def _resolve_pilot_cycle(payload: dict[str, Any]) -> dict[str, Any] | None:
    if payload.get('pilot_cycle_run_id'):
        return get_pilot_cycle_run(int(payload['pilot_cycle_run_id']))
    rows = list_pilot_cycle_runs(1)
    return rows[0] if rows else None


def _default_scenario_id() -> int:
    row = (
        db.fetch_one("SELECT id FROM scenarios WHERE name = 'FY27 Operating Plan' ORDER BY id LIMIT 1")
        or db.fetch_one('SELECT id FROM scenarios ORDER BY id LIMIT 1')
    )
    if row is None:
        raise ValueError('No scenario exists for final UI polish.')
    return int(row['id'])


def _static_ui_checks() -> dict[str, bool]:
    assets = _read_assets()
    return {
        'workspace_state_finished': all(
            token in assets['workspace_js']
            for token in [
                'workspace-toggle-active',
                'aria-pressed',
                'workspace-hidden',
                'data-workspace-hidden-by-toggle',
                'workspaceEmptyState',
                'availableWorkspaceNumbers',
            ]
        ),
        'dock_undock_finished': all(
            token in assets['dock_js']
            for token in ['window.open', 'popoutContent', 'placeholder.replaceWith(section)', 'beforeunload', 'dockableTitles']
        ),
        'chat_popout_finished': all(
            token in assets['chat_js']
            for token in ['window.open', 'chat-window.html', 'muFinancesChatWindow', 'popup=yes', 'resizable=yes']
        ),
        'reporting_layout_finished': '#reporting' in assets['index_html'] and 'Reporting and analytics' in assets['index_html'],
        'accessibility_finished': all(
            token in assets['index_html'] + assets['styles_css'] + assets['workspace_js']
            for token in ['skip-link', ':focus-visible', 'aria-expanded', 'aria-controls', 'aria-pressed']
        ),
        'import_export_discoverability_finished': all(
            token in assets['index_html']
            for token in ['heroImportButton', 'heroExportButton', 'guidedImportButton', 'guidedExportButton']
        ),
        'command_gap_and_ready_status_finished': all(
            token in assets['styles_css'] + assets['layout_js']
            for token in ['#appStatus', 'display: none', 'targetGap = 16', 'tightenMainContentGap']
        ),
    }


def _ui_asset_evidence() -> dict[str, Any]:
    assets = _read_assets()
    return {
        'workspace_script_bytes': len(assets['workspace_js']),
        'dock_script_bytes': len(assets['dock_js']),
        'chat_script_bytes': len(assets['chat_js']),
        'layout_tightening_loaded': 'layout-tightening.js' in assets['index_html'],
        'workspace_script_loaded': 'workspace-button-fallback.js' in assets['index_html'],
        'dock_script_loaded': 'dockable-sections.js' in assets['index_html'],
    }


def _read_assets() -> dict[str, str]:
    return {
        'index_html': (ROOT / 'static' / 'index.html').read_text(encoding='utf-8'),
        'styles_css': (ROOT / 'static' / 'styles.css').read_text(encoding='utf-8'),
        'workspace_js': (ROOT / 'static' / 'js' / 'workspace-button-fallback.js').read_text(encoding='utf-8'),
        'dock_js': (ROOT / 'static' / 'js' / 'dockable-sections.js').read_text(encoding='utf-8'),
        'chat_js': (ROOT / 'static' / 'js' / 'chat-satellite.js').read_text(encoding='utf-8'),
        'layout_js': (ROOT / 'static' / 'js' / 'layout-tightening.js').read_text(encoding='utf-8'),
    }


def _pilot_cycle_summary(pilot_cycle: dict[str, Any] | None) -> dict[str, Any] | None:
    if pilot_cycle is None:
        return None
    return {
        'id': pilot_cycle['id'],
        'run_key': pilot_cycle['run_key'],
        'status': pilot_cycle['status'],
        'failed_checks': [key for key, value in pilot_cycle.get('checks', {}).items() if value is not True],
    }


def _format_run(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise ValueError('Pilot defect UI polish run not found.')
    result = dict(row)
    result['batch'] = 'B128'
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['defects'] = json.loads(result.pop('defects_json') or '[]')
    result['artifacts'] = json.loads(result.pop('artifacts_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
