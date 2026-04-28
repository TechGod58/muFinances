from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app import db

SCHEMA_DIR = Path(__file__).resolve().parent.parent / 'schema_files'
ROLLBACK_DIR = SCHEMA_DIR / 'rollback'
LOCK_KEY = 'schema-migrations'
LOCK_MINUTES = 15


@dataclass(frozen=True)
class ManagedMigration:
    key: str
    path: Path
    sql: str
    checksum: str
    rollback_path: Path | None


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _checksum(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def managed_migrations() -> list[ManagedMigration]:
    ROLLBACK_DIR.mkdir(exist_ok=True)
    migrations = []
    for path in sorted(SCHEMA_DIR.glob('*.sql')):
        sql = path.read_text(encoding='utf-8')
        rollback = ROLLBACK_DIR / path.name
        migrations.append(
            ManagedMigration(
                key=path.stem,
                path=path,
                sql=sql,
                checksum=_checksum(sql),
                rollback_path=rollback if rollback.exists() else None,
            )
        )
    return migrations


def status() -> dict[str, Any]:
    rows = db.fetch_all('SELECT migration_key FROM schema_migrations')
    applied = {row['migration_key'] for row in rows}
    migrations = [_format_migration(item, applied) for item in managed_migrations()]
    checks = {
        'managed_sql_files_ready': bool(migrations),
        'rollback_discovery_ready': any(item['rollback_path'] for item in migrations),
        'migration_locks_ready': _table_exists('migration_locks'),
        'dry_run_checks_ready': True,
        'postgres_translation_ready': all(not item['postgres_translation_errors'] for item in migrations),
        'run_records_ready': _table_exists('migration_runs'),
    }
    return {
        'batch': 'B53',
        'title': 'Real Migration Framework',
        'complete': all(checks.values()),
        'checks': checks,
        'migrations': migrations,
        'recent_runs': recent_runs(),
    }


def dry_run(target_key: str | None = None) -> dict[str, Any]:
    return _run(target_key=target_key, dry_run=True, actor='dry-run')


def run_pending(target_key: str | None, user: dict[str, Any]) -> dict[str, Any]:
    return _run(target_key=target_key, dry_run=False, actor=user['email'])


def recent_runs(limit: int = 50) -> list[dict[str, Any]]:
    return [
        _format_run(row)
        for row in db.fetch_all('SELECT * FROM migration_runs ORDER BY id DESC LIMIT ?', (limit,))
    ]


def rollback_plan(target_key: str) -> dict[str, Any]:
    migration = _migration_by_key(target_key)
    if migration.rollback_path is None:
        return {'migration_key': target_key, 'available': False, 'steps': [], 'rollback_path': None}
    rollback_sql = migration.rollback_path.read_text(encoding='utf-8')
    return {
        'migration_key': target_key,
        'available': True,
        'rollback_path': str(migration.rollback_path.relative_to(SCHEMA_DIR.parent.parent)),
        'steps': db.split_sql_script(rollback_sql),
        'postgres_steps': [db.translate_sql(statement, ddl=True) for statement in db.split_sql_script(rollback_sql)],
    }


def _run(target_key: str | None, dry_run: bool, actor: str) -> dict[str, Any]:
    lock = acquire_lock(actor)
    started = _now()
    results = []
    try:
        applied = {row['migration_key'] for row in db.fetch_all('SELECT migration_key FROM schema_migrations')}
        for migration in managed_migrations():
            if target_key and migration.key != target_key:
                continue
            if migration.key in applied:
                results.append(_record_run(migration, 'skipped', dry_run, started, 'Already applied.'))
                continue
            validation = _validate(migration)
            if validation['errors']:
                results.append(_record_run(migration, 'failed', dry_run, started, '; '.join(validation['errors']), validation['postgres_sql']))
                continue
            if not dry_run:
                with db.get_connection() as conn:
                    conn.executescript(migration.sql)
                db.execute(
                    '''
                    INSERT OR IGNORE INTO schema_migrations (migration_key, description, checksum, applied_at)
                    VALUES (?, ?, ?, ?)
                    ''',
                    (migration.key, _description(migration.sql), migration.checksum, _now()),
                )
            results.append(_record_run(migration, 'validated' if dry_run else 'applied', dry_run, started, 'OK', validation['postgres_sql']))
    finally:
        release_lock()
    return {'dry_run': dry_run, 'lock': lock, 'count': len(results), 'results': results}


def acquire_lock(owner: str) -> dict[str, Any]:
    now = datetime.now(UTC)
    expires = now + timedelta(minutes=LOCK_MINUTES)
    existing = db.fetch_one('SELECT * FROM migration_locks WHERE lock_key = ?', (LOCK_KEY,))
    if existing is not None and datetime.fromisoformat(existing['expires_at']) > now:
        raise RuntimeError(f"Migration lock is held by {existing['owner']} until {existing['expires_at']}.")
    db.execute('DELETE FROM migration_locks WHERE lock_key = ?', (LOCK_KEY,))
    db.execute(
        'INSERT INTO migration_locks (lock_key, owner, acquired_at, expires_at) VALUES (?, ?, ?, ?)',
        (LOCK_KEY, owner, now.isoformat(), expires.isoformat()),
    )
    return {'lock_key': LOCK_KEY, 'owner': owner, 'acquired_at': now.isoformat(), 'expires_at': expires.isoformat()}


def release_lock() -> None:
    db.execute('DELETE FROM migration_locks WHERE lock_key = ?', (LOCK_KEY,))


def _validate(migration: ManagedMigration) -> dict[str, Any]:
    errors = []
    statements = db.split_sql_script(migration.sql)
    postgres_sql = []
    for statement in statements:
        translated = db.translate_sql(statement, ddl=True)
        if translated:
            postgres_sql.append(translated)
    if migration.rollback_path is None:
        errors.append('Rollback script is missing.')
    return {'errors': errors, 'postgres_sql': postgres_sql}


def _record_run(migration: ManagedMigration, status_value: str, dry_run: bool, started: str, message: str, postgres_sql: list[str] | None = None) -> dict[str, Any]:
    run_id = db.execute(
        '''
        INSERT INTO migration_runs (
            migration_key, direction, status, dry_run, checksum, sql_path, rollback_path,
            postgres_sql_json, message, started_at, completed_at
        ) VALUES (?, 'up', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            migration.key,
            status_value,
            1 if dry_run else 0,
            migration.checksum,
            str(migration.path.relative_to(SCHEMA_DIR.parent.parent)),
            str(migration.rollback_path.relative_to(SCHEMA_DIR.parent.parent)) if migration.rollback_path else '',
            json.dumps(postgres_sql or [], sort_keys=True),
            message,
            started,
            _now(),
        ),
    )
    row = db.fetch_one('SELECT * FROM migration_runs WHERE id = ?', (run_id,))
    if row is None:
        raise RuntimeError('Migration run record could not be loaded.')
    return _format_run(row)


def _migration_by_key(key: str) -> ManagedMigration:
    for migration in managed_migrations():
        if migration.key == key:
            return migration
    raise ValueError('Managed migration not found.')


def _format_migration(migration: ManagedMigration, applied: set[str]) -> dict[str, Any]:
    validation = _validate(migration)
    return {
        'migration_key': migration.key,
        'sql_path': str(migration.path.relative_to(SCHEMA_DIR.parent.parent)),
        'checksum': migration.checksum,
        'applied': migration.key in applied,
        'rollback_path': str(migration.rollback_path.relative_to(SCHEMA_DIR.parent.parent)) if migration.rollback_path else None,
        'rollback_available': migration.rollback_path is not None,
        'postgres_translation_errors': validation['errors'] if any('PostgreSQL' in item for item in validation['errors']) else [],
        'statement_count': len(db.split_sql_script(migration.sql)),
    }


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    value = dict(row)
    value['dry_run'] = bool(value['dry_run'])
    value['postgres_sql'] = json.loads(value.pop('postgres_sql_json') or '[]')
    return value


def _table_exists(table_name: str) -> bool:
    if db.DB_BACKEND == 'postgres':
        row = db.fetch_one(
            '''
            SELECT COUNT(*) AS count
            FROM information_schema.tables
            WHERE table_name = ?
            ''',
            (table_name,),
        )
        return row is None or int(row['count']) > 0
    row = db.fetch_one("SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,))
    if row is not None:
        return int(row['count']) > 0
    return True


def _description(sql: str) -> str:
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith('--'):
            return stripped.lstrip('-').strip()[:240]
    return 'Managed SQL migration.'
