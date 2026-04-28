from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app import db

ROOT = Path(__file__).resolve().parents[2]
GUIDES_DIR = ROOT / 'docs' / 'guides'

LOCKED_GUIDES = {
    'admin_guide': {
        'path': GUIDES_DIR / 'admin-guide.md',
        'sections': ['Daily Checks', 'User And Access Control', 'Security Operations', 'Audit Reporting', 'Incident Response'],
    },
    'planner_guide': {
        'path': GUIDES_DIR / 'planner-guide.md',
        'sections': ['Planning Workflow', 'Data Entry', 'Forecasts', 'Approvals And Evidence', 'Planner Closeout'],
    },
    'controller_guide': {
        'path': GUIDES_DIR / 'controller-guide.md',
        'sections': ['Controller Workspace', 'Review Cadence', 'Reconciliations', 'Consolidation', 'Signoff'],
    },
    'integration_guide': {
        'path': GUIDES_DIR / 'integration-guide.md',
        'sections': ['Connectors', 'Imports', 'Mapping And Validation', 'Secrets', 'Sync Logs', 'Recovery'],
    },
    'data_dictionary': {
        'path': GUIDES_DIR / 'data-dictionary.md',
        'sections': ['Core Tables', 'Planning Tables', 'Close And Reporting', 'Security And Audit', 'Operations', 'Retention Notes'],
    },
    'close_guide': {
        'path': GUIDES_DIR / 'close-process-guide.md',
        'sections': ['Period Close', 'Controls', 'Reconciliation Workflow', 'Consolidation Workflow', 'Audit Packet'],
    },
    'deployment_guide': {
        'path': GUIDES_DIR / 'deployment-guide.md',
        'sections': ['Purpose', 'Runtime Layout', 'Deployment Steps', 'Service Startup', 'Verification', 'Rollback'],
    },
    'recovery_guide': {
        'path': GUIDES_DIR / 'recovery-guide.md',
        'sections': ['Purpose', 'Recovery Triggers', 'Backup Sources', 'Restore Procedure', 'Data Integrity', 'Communication And Signoff', 'Post-Recovery Follow-Up'],
    },
    'security_guide': {
        'path': GUIDES_DIR / 'security-guide.md',
        'sections': ['Purpose', 'Identity And Access', 'Domain And VPN Enforcement', 'Permissions And Row-Level Access', 'Session Controls', 'Secrets', 'Audit And Support', 'Review Cadence'],
    },
}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS documentation_lock_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lock_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS documentation_lock_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                guide_key TEXT NOT NULL,
                path TEXT NOT NULL,
                checksum TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                status TEXT NOT NULL,
                missing_sections_json TEXT NOT NULL,
                locked_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES documentation_lock_runs(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_documentation_lock_items_run
            ON documentation_lock_items (run_id, guide_key);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    manifest = _build_manifest()
    latest = db.fetch_one('SELECT * FROM documentation_lock_runs ORDER BY id DESC LIMIT 1')
    checks = _checks(manifest)
    return {
        'batch': 'B109',
        'title': 'Documentation Lock',
        'complete': all(checks.values()),
        'checks': checks,
        'count': len(manifest),
        'guides': manifest,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM documentation_lock_runs ORDER BY id DESC LIMIT ?', (max(1, min(limit, 200)),))
    return [_format_run(row) for row in rows]


def run_lock(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    lock_key = payload.get('lock_key') or f"b109-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    manifest = _build_manifest()
    checks = _checks(manifest)
    status_value = 'locked' if all(checks.values()) else 'needs_review'
    now = _now()
    run_id = db.execute(
        '''
        INSERT INTO documentation_lock_runs (lock_key, status, manifest_json, checks_json, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (lock_key, status_value, json.dumps(manifest, sort_keys=True), json.dumps(checks, sort_keys=True), user['email'], now),
    )
    for guide in manifest:
        db.execute(
            '''
            INSERT INTO documentation_lock_items (
                run_id, guide_key, path, checksum, size_bytes, status, missing_sections_json, locked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                run_id,
                guide['guide_key'],
                guide['path'],
                guide['checksum'],
                guide['size_bytes'],
                'locked' if guide['ready'] else 'needs_review',
                json.dumps(guide['missing_sections'], sort_keys=True),
                now,
            ),
        )
    db.log_audit('documentation_lock', lock_key, status_value, user['email'], {'checks': checks, 'guide_count': len(manifest)}, now)
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM documentation_lock_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Documentation lock run not found.')
    result = _format_run(row)
    result['items'] = [_format_item(item) for item in db.fetch_all('SELECT * FROM documentation_lock_items WHERE run_id = ? ORDER BY guide_key', (run_id,))]
    result['complete'] = result['status'] == 'locked'
    return result


def _build_manifest() -> list[dict[str, Any]]:
    manifest = []
    for guide_key, config in LOCKED_GUIDES.items():
        path = config['path']
        text = path.read_text(encoding='utf-8') if path.exists() else ''
        missing = [section for section in config['sections'] if f'## {section}' not in text and f'# {section}' not in text]
        checksum = hashlib.sha256(text.encode('utf-8')).hexdigest() if text else ''
        manifest.append(
            {
                'guide_key': guide_key,
                'path': str(path.relative_to(ROOT)),
                'exists': path.exists(),
                'size_bytes': path.stat().st_size if path.exists() else 0,
                'checksum': checksum,
                'required_sections': config['sections'],
                'missing_sections': missing,
                'ready': path.exists() and not missing and len(text.strip()) >= 600,
            }
        )
    return manifest


def _checks(manifest: list[dict[str, Any]]) -> dict[str, bool]:
    by_key = {item['guide_key']: item for item in manifest}
    return {
        f'{guide_key}_locked': bool(by_key.get(guide_key, {}).get('ready'))
        for guide_key in LOCKED_GUIDES
    }


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['manifest'] = json.loads(result.pop('manifest_json') or '[]')
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    return result


def _format_item(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['missing_sections'] = json.loads(result.pop('missing_sections_json') or '[]')
    return result
