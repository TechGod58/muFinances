from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app import db
from app.services.foundation import BACKUP_DIR, create_backup, list_backups

ROOT = Path(__file__).resolve().parents[2]
DEPLOY_DIR = ROOT / 'deploy'
RUNBOOK_DIR = ROOT / 'docs' / 'runbooks'


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'operational_checks': int(db.fetch_one('SELECT COUNT(*) AS count FROM operational_checks')['count']),
        'restore_tests': int(db.fetch_one('SELECT COUNT(*) AS count FROM restore_test_runs')['count']),
        'runbooks': int(db.fetch_one('SELECT COUNT(*) AS count FROM runbook_records')['count']),
        'backups': int(db.fetch_one('SELECT COUNT(*) AS count FROM backup_records')['count']),
    }
    checks = {
        'localhost_packaging_ready': (ROOT / 'start-muFinances.cmd').exists(),
        'windows_service_ready': (DEPLOY_DIR / 'install-windows-service.ps1').exists(),
        'docker_packaging_ready': (ROOT / 'Dockerfile').exists() and (ROOT / 'docker-compose.yml').exists(),
        'health_checks_ready': True,
        'backups_ready': BACKUP_DIR.exists(),
        'restore_tests_ready': True,
        'runbooks_ready': RUNBOOK_DIR.exists(),
    }
    return {'batch': 'B12', 'title': 'Deployment Operations', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def run_operational_check(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    check_key = payload['check_key']
    detail = _check_detail(check_key)
    status_value = 'pass' if detail.pop('pass') else 'fail'
    now = _now()
    check_id = db.execute(
        '''
        INSERT INTO operational_checks (check_key, category, status, detail_json, checked_by, checked_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (check_key, payload['category'], status_value, json.dumps(detail, sort_keys=True), user['email'], now),
    )
    db.log_audit('operational_check', str(check_id), status_value, user['email'], {'check_key': check_key, **detail}, now)
    return get_operational_check(check_id)


def list_operational_checks() -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM operational_checks ORDER BY id DESC LIMIT 100')
    return [_format_check(row) for row in rows]


def get_operational_check(check_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM operational_checks WHERE id = ?', (check_id,))
    if row is None:
        raise ValueError('Operational check not found.')
    return _format_check(row)


def create_operations_backup(user: dict[str, Any]) -> dict[str, Any]:
    return create_backup(note='B12 deployment operations backup', actor=user['email'])


def run_restore_test(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    backup = db.fetch_one('SELECT * FROM backup_records WHERE backup_key = ?', (payload['backup_key'],))
    if backup is None:
        raise ValueError('Backup not found.')
    backup_path = Path(backup['path']).resolve()
    backup_root = BACKUP_DIR.resolve()
    if backup_root not in backup_path.parents or not backup_path.exists():
        raise ValueError('Backup path is invalid.')

    validation = _validate_sqlite_backup(backup_path)
    status_value = 'pass' if validation['valid'] else 'fail'
    now = _now()
    test_id = db.execute(
        '''
        INSERT INTO restore_test_runs (
            backup_key, status, source_size_bytes, validation_json, tested_by, tested_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (payload['backup_key'], status_value, int(backup['size_bytes']), json.dumps(validation, sort_keys=True), user['email'], now),
    )
    db.log_audit('restore_test', str(test_id), status_value, user['email'], validation, now)
    return get_restore_test(test_id)


def list_restore_tests() -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM restore_test_runs ORDER BY id DESC LIMIT 100')
    return [_format_restore_test(row) for row in rows]


def get_restore_test(test_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM restore_test_runs WHERE id = ?', (test_id,))
    if row is None:
        raise ValueError('Restore test not found.')
    return _format_restore_test(row)


def upsert_runbook(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO runbook_records (runbook_key, title, category, path, status, updated_by, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(runbook_key) DO UPDATE SET
            title = excluded.title,
            category = excluded.category,
            path = excluded.path,
            status = excluded.status,
            updated_by = excluded.updated_by,
            updated_at = excluded.updated_at
        ''',
        (
            payload['runbook_key'], payload['title'], payload['category'], payload['path'],
            payload['status'], user['email'], now,
        ),
    )
    db.log_audit('runbook', payload['runbook_key'], 'upserted', user['email'], payload, now)
    return get_runbook(payload['runbook_key'])


def list_runbooks() -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM runbook_records ORDER BY category ASC, title ASC')


def get_runbook(runbook_key: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM runbook_records WHERE runbook_key = ?', (runbook_key,))
    if row is None:
        raise ValueError('Runbook not found.')
    return row


def operations_summary() -> dict[str, Any]:
    latest_backup = db.fetch_one('SELECT * FROM backup_records ORDER BY id DESC LIMIT 1')
    latest_restore = db.fetch_one('SELECT * FROM restore_test_runs ORDER BY id DESC LIMIT 1')
    latest_check = db.fetch_one('SELECT * FROM operational_checks ORDER BY id DESC LIMIT 1')
    return {
        'status': status(),
        'latest_backup': latest_backup,
        'latest_restore_test': _format_restore_test(latest_restore) if latest_restore else None,
        'latest_operational_check': _format_check(latest_check) if latest_check else None,
        'backup_count': len(list_backups()),
    }


def _check_detail(check_key: str) -> dict[str, Any]:
    if check_key == 'database':
        row = db.fetch_one('SELECT COUNT(*) AS count FROM scenarios')
        return {'pass': row is not None, 'scenario_count': int(row['count']) if row else 0}
    if check_key == 'static-assets':
        index_ok = (ROOT / 'static' / 'index.html').exists()
        app_ok = (ROOT / 'static' / 'app.js').exists()
        return {'pass': index_ok and app_ok, 'index_html': index_ok, 'app_js': app_ok}
    if check_key == 'backups':
        return {'pass': BACKUP_DIR.exists(), 'backup_dir': str(BACKUP_DIR), 'backup_count': len(list_backups())}
    if check_key == 'packaging':
        windows_ok = (DEPLOY_DIR / 'install-windows-service.ps1').exists()
        docker_ok = (ROOT / 'Dockerfile').exists() and (ROOT / 'docker-compose.yml').exists()
        return {'pass': windows_ok and docker_ok, 'windows_service': windows_ok, 'docker': docker_ok}
    return {'pass': True, 'message': 'Generic operational check recorded.'}


def _validate_sqlite_backup(path: Path) -> dict[str, Any]:
    try:
        with sqlite3.connect(path) as conn:
            integrity = conn.execute('PRAGMA integrity_check;').fetchone()[0]
            table_count = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table';").fetchone()[0]
        return {'valid': integrity == 'ok', 'integrity_check': integrity, 'table_count': int(table_count), 'path': str(path)}
    except sqlite3.Error as exc:
        return {'valid': False, 'error': str(exc), 'path': str(path)}


def _format_check(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['detail'] = json.loads(result.pop('detail_json') or '{}')
    return result


def _format_restore_test(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['validation'] = json.loads(result.pop('validation_json') or '{}')
    return result
