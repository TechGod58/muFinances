from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_postgres_runtime.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)
os.environ['CAMPUS_FPM_DB_POOL_SIZE'] = '3'

from fastapi.testclient import TestClient

from app import db
from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def test_sql_translation_covers_postgres_runtime_contract() -> None:
    select_sql = db.translate_sql("SELECT * FROM planning_ledger WHERE period = ? AND note = '?'")
    assert 'period = %s' in select_sql
    assert "note = '?'" in select_sql

    create_sql = db.translate_sql(
        'CREATE TABLE sample (id INTEGER PRIMARY KEY AUTOINCREMENT, amount REAL, note TEXT)',
        ddl=True,
    )
    assert 'SERIAL PRIMARY KEY' in create_sql
    assert 'DOUBLE PRECISION' in create_sql

    insert_sql = db.translate_sql('INSERT OR IGNORE INTO roles (role_key, name) VALUES (?, ?)')
    assert insert_sql.startswith('INSERT INTO roles')
    assert 'VALUES (%s, %s)' in insert_sql
    assert 'ON CONFLICT DO NOTHING' in insert_sql


def test_postgres_conninfo_adds_sslmode_to_url_dsns(monkeypatch) -> None:
    monkeypatch.setattr(db, 'POSTGRES_DSN', 'postgresql://user:pass@127.0.0.1:5432/mufinances')
    monkeypatch.setattr(db, 'DB_SSL_MODE', 'disable')
    assert db.postgres_conninfo().endswith('/mufinances?sslmode=disable')

    monkeypatch.setattr(db, 'POSTGRES_DSN', 'postgresql://user:pass@127.0.0.1:5432/mufinances?connect_timeout=5')
    assert db.postgres_conninfo().endswith('connect_timeout=5&sslmode=disable')


def test_postgres_ddl_order_places_referenced_tables_first() -> None:
    ordered = db.order_postgres_ddl([
        'CREATE TABLE IF NOT EXISTS child (id SERIAL PRIMARY KEY, parent_id integer REFERENCES parent(id))',
        'CREATE INDEX IF NOT EXISTS ix_child_parent ON child (parent_id)',
        'CREATE TABLE IF NOT EXISTS parent (id SERIAL PRIMARY KEY)',
    ])
    assert ordered[0].startswith('CREATE TABLE IF NOT EXISTS parent')
    assert ordered[1].startswith('CREATE TABLE IF NOT EXISTS child')
    assert ordered[2].startswith('CREATE INDEX')


def test_mssql_runtime_translation_hooks() -> None:
    create_sql = db.translate_mssql_sql(
        'CREATE TABLE IF NOT EXISTS sample (id INTEGER PRIMARY KEY AUTOINCREMENT, amount REAL, note TEXT)',
        ddl=True,
    )
    assert create_sql.startswith("IF OBJECT_ID(N'sample', N'U') IS NULL CREATE TABLE sample")
    assert 'INT IDENTITY(1,1) PRIMARY KEY' in create_sql
    assert 'FLOAT' in create_sql
    assert 'NVARCHAR(MAX)' in create_sql

    index_sql = db.translate_mssql_sql(
        'CREATE INDEX IF NOT EXISTS idx_sample_note ON sample (note)',
        ddl=True,
    )
    assert 'sys.indexes' in index_sql
    assert 'CREATE INDEX idx_sample_note ON sample' in index_sql

    insert_sql = db.translate_mssql_sql('INSERT OR IGNORE INTO roles (role_key, name) VALUES (?, ?)')
    assert insert_sql == 'INSERT INTO roles (role_key, name) VALUES (?, ?)'


def test_join_table_inserts_do_not_expect_synthetic_id() -> None:
    role_permission_sql = db.translate_sql(
        'INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?, ?)'
    )
    user_role_sql = db.translate_sql('INSERT OR IGNORE INTO user_roles (user_id, role_id) VALUES (?, ?)')
    role_sql = db.translate_sql('INSERT INTO roles (role_key, name) VALUES (?, ?)')

    assert db._should_return_id(role_permission_sql) is False
    assert db._should_return_id(user_role_sql) is False
    assert db._should_return_id(role_sql) is True


def test_postgres_runtime_status_and_migration_are_registered() -> None:
    headers = admin_headers()
    status = client.get('/api/postgres-runtime/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B27'
    assert payload['complete'] is True
    assert payload['checks']['postgres_driver_ready'] is True
    assert payload['checks']['mssql_driver_ready'] is True
    assert payload['checks']['mssql_ddl_translation_ready'] is True
    assert payload['checks']['mssql_index_translation_ready'] is True
    assert payload['checks']['sql_placeholder_translation_ready'] is True
    assert payload['checks']['join_table_insert_ready'] is True
    assert payload['database']['postgres_driver_available'] is True
    assert payload['database']['mssql_driver_available'] is True

    database_status = client.get('/api/database-runtime/status', headers=headers)
    assert database_status.status_code == 200
    assert database_status.json()['checks']['mssql_ddl_translation_ready'] is True

    migrations = client.get('/api/foundation/migrations', headers=headers)
    assert migrations.status_code == 200
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0028_real_postgresql_runtime' in keys
