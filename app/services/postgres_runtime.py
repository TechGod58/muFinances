from __future__ import annotations

from typing import Any

from app import db
from app.services.foundation import BUILTIN_MIGRATIONS


def status() -> dict[str, Any]:
    runtime = db.database_runtime()
    sample_insert = db.translate_sql(
        'INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?, ?)'
    )
    sample_create = db.translate_sql(
        'CREATE TABLE sample (id INTEGER PRIMARY KEY AUTOINCREMENT, amount REAL, note TEXT)',
        ddl=True,
    )
    sample_select = db.translate_sql("SELECT * FROM planning_ledger WHERE period = ? AND note = '?'")
    sample_mssql_create = db.translate_mssql_sql(
        'CREATE TABLE IF NOT EXISTS sample (id INTEGER PRIMARY KEY AUTOINCREMENT, amount REAL, note TEXT)',
        ddl=True,
    )
    sample_mssql_index = db.translate_mssql_sql(
        'CREATE INDEX IF NOT EXISTS idx_sample_note ON sample (note)',
        ddl=True,
    )
    migration_keys = {row['migration_key'] for row in db.fetch_all('SELECT migration_key FROM schema_migrations')}
    latest_key = BUILTIN_MIGRATIONS[-1]['key']
    checks = {
        'postgres_driver_ready': runtime['postgres_driver_available'],
        'mssql_driver_ready': runtime['mssql_driver_available'],
        'backend_switch_ready': runtime['backend'] in {'sqlite', 'postgres', 'mssql'},
        'postgres_pool_ready': runtime['pooling_enabled'] and int(runtime['pool_size']) >= 1,
        'mssql_pool_ready': runtime['pooling_enabled'] and int(runtime['pool_size']) >= 1,
        'sql_placeholder_translation_ready': '%s' in sample_select and "note = '?'" in sample_select,
        'ddl_translation_ready': 'SERIAL PRIMARY KEY' in sample_create and 'DOUBLE PRECISION' in sample_create,
        'insert_ignore_translation_ready': 'ON CONFLICT DO NOTHING' in sample_insert and '%s' in sample_insert,
        'mssql_ddl_translation_ready': 'OBJECT_ID' in sample_mssql_create and 'IDENTITY(1,1)' in sample_mssql_create and 'NVARCHAR(MAX)' in sample_mssql_create,
        'mssql_index_translation_ready': 'sys.indexes' in sample_mssql_index,
        'join_table_insert_ready': db._should_return_id(sample_insert) is False,
        'sqlite_backward_compatibility_ready': _sqlite_smoke_check(),
        'migration_registered': latest_key in migration_keys,
    }
    return {
        'batch': 'B27',
        'title': 'Real PostgreSQL Runtime',
        'complete': all(checks.values()),
        'checks': checks,
        'database': runtime,
        'samples': {
            'select_translation': sample_select,
            'insert_ignore_translation': sample_insert,
            'ddl_translation': sample_create,
            'mssql_ddl_translation': sample_mssql_create,
            'mssql_index_translation': sample_mssql_index,
        },
    }


def _sqlite_smoke_check() -> bool:
    row = db.fetch_one('SELECT COUNT(*) AS count FROM schema_migrations')
    return row is not None and int(row['count']) >= 1
