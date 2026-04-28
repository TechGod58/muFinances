from __future__ import annotations

import json
import hashlib
import os
import re
import sqlite3
import threading
from queue import Empty, Queue
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover - exercised only when PostgreSQL extras are absent.
    psycopg = None
    dict_row = None
    ConnectionPool = None

try:
    import pyodbc
except ImportError:  # pragma: no cover - exercised only when SQL Server extras are absent.
    pyodbc = None

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data'
DATA_DIR.mkdir(exist_ok=True)
DB_BACKEND = os.getenv('CAMPUS_FPM_DB_BACKEND', 'sqlite').lower()
DB_PATH = Path(os.getenv('CAMPUS_FPM_DB_PATH', DATA_DIR / 'campus_fpm.db'))
POSTGRES_DSN = os.getenv('CAMPUS_FPM_POSTGRES_DSN', '')
MSSQL_DSN = os.getenv('CAMPUS_FPM_MSSQL_DSN', '') or os.getenv('CAMPUS_FPM_SQLSERVER_DSN', '')
DB_POOL_SIZE = max(1, int(os.getenv('CAMPUS_FPM_DB_POOL_SIZE', '5')))
DB_SSL_MODE = os.getenv('CAMPUS_FPM_DB_SSL_MODE', 'prefer')
_SQLITE_POOL: Queue[sqlite3.Connection] = Queue(maxsize=DB_POOL_SIZE)
_SQLITE_POOL_LOCK = threading.Lock()
_POSTGRES_POOL: Any | None = None
_NO_RETURNING_ID_TABLES = {'user_roles', 'role_permissions'}


def row_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


@contextmanager
def get_connection() -> Iterable[Any]:
    if DB_BACKEND == 'postgres':
        pool = _postgres_pool()
        with pool.connection() as raw:
            conn = PostgresConnection(raw)
            try:
                yield conn
                raw.commit()
            except Exception:
                raw.rollback()
                raise
        return

    if DB_BACKEND == 'mssql':
        raw = _mssql_connection()
        conn = MssqlConnection(raw)
        try:
            yield conn
            raw.commit()
        except Exception:
            raw.rollback()
            raise
        finally:
            raw.close()
        return

    conn = _acquire_sqlite_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _release_sqlite_connection(conn)


@contextmanager
def transaction(*, immediate: bool = False) -> Iterable[Any]:
    if DB_BACKEND == 'postgres':
        pool = _postgres_pool()
        with pool.connection() as raw:
            conn = PostgresConnection(raw)
            try:
                yield conn
                raw.commit()
            except Exception:
                raw.rollback()
                raise
        return

    if DB_BACKEND == 'mssql':
        raw = _mssql_connection()
        conn = MssqlConnection(raw)
        try:
            yield conn
            raw.commit()
        except Exception:
            raw.rollback()
            raise
        finally:
            raw.close()
        return

    conn = _acquire_sqlite_connection()
    try:
        if immediate:
            conn.execute('BEGIN IMMEDIATE')
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _release_sqlite_connection(conn)


def _acquire_sqlite_connection() -> sqlite3.Connection:
    if DB_BACKEND != 'sqlite':
        raise RuntimeError('An external production database is configured; this local runtime is currently SQLite-backed.')
    try:
        conn = _SQLITE_POOL.get_nowait()
    except Empty:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = row_factory
        conn.execute('PRAGMA foreign_keys = ON;')
        conn.execute('PRAGMA busy_timeout = 5000;')
    return conn


def _postgres_pool() -> Any:
    global _POSTGRES_POOL
    if DB_BACKEND != 'postgres':
        raise RuntimeError('PostgreSQL pool requested while DB_BACKEND is not postgres.')
    if not POSTGRES_DSN:
        raise RuntimeError('CAMPUS_FPM_POSTGRES_DSN is required when CAMPUS_FPM_DB_BACKEND=postgres.')
    if ConnectionPool is None or dict_row is None:
        raise RuntimeError('PostgreSQL support requires psycopg and psycopg_pool. Install requirements.txt.')
    if _POSTGRES_POOL is None:
        _POSTGRES_POOL = ConnectionPool(
            conninfo=postgres_conninfo(),
            min_size=1,
            max_size=DB_POOL_SIZE,
            kwargs={'row_factory': dict_row},
            open=True,
        )
    return _POSTGRES_POOL


def postgres_conninfo() -> str:
    conninfo = POSTGRES_DSN
    if DB_SSL_MODE and 'sslmode=' not in conninfo:
        if conninfo.startswith(('postgresql://', 'postgres://')):
            separator = '&' if '?' in conninfo else '?'
            return f'{conninfo}{separator}sslmode={DB_SSL_MODE}'
        return f'{conninfo} sslmode={DB_SSL_MODE}'
    return conninfo


def _mssql_connection() -> Any:
    if DB_BACKEND != 'mssql':
        raise RuntimeError('SQL Server connection requested while DB_BACKEND is not mssql.')
    if not MSSQL_DSN:
        raise RuntimeError('CAMPUS_FPM_MSSQL_DSN is required when CAMPUS_FPM_DB_BACKEND=mssql.')
    if pyodbc is None:
        raise RuntimeError('SQL Server support requires pyodbc. Install requirements.txt and an ODBC Driver for SQL Server.')
    pyodbc.pooling = True
    return pyodbc.connect(mssql_conninfo(), autocommit=False)


def mssql_conninfo() -> str:
    return MSSQL_DSN


def _release_sqlite_connection(conn: sqlite3.Connection) -> None:
    try:
        _SQLITE_POOL.put_nowait(conn)
    except Exception:
        conn.close()


def database_runtime() -> dict[str, Any]:
    return {
        'backend': DB_BACKEND,
        'sqlite_path': str(DB_PATH) if DB_BACKEND == 'sqlite' else None,
        'postgres_dsn_configured': bool(POSTGRES_DSN),
        'postgres_ssl_mode': DB_SSL_MODE,
        'mssql_dsn_configured': bool(MSSQL_DSN),
        'pool_size': DB_POOL_SIZE,
        'pool_checked_in': _SQLITE_POOL.qsize() if DB_BACKEND == 'sqlite' else None,
        'pooling_enabled': True,
        'postgres_driver_available': bool(ConnectionPool is not None and psycopg is not None),
        'mssql_driver_available': pyodbc is not None,
        'sql_translation': f'{DB_BACKEND}_compatibility_layer' if DB_BACKEND in {'postgres', 'mssql'} else 'native_sqlite',
    }


class PostgresCursor:
    def __init__(self, cursor: Any, lastrowid: int = 0) -> None:
        self._cursor = cursor
        self.lastrowid = lastrowid

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self._cursor.fetchall())

    def fetchone(self) -> dict[str, Any] | None:
        return self._cursor.fetchone()


class PostgresConnection:
    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> PostgresCursor:
        translated = translate_sql(query)
        if not translated:
            return PostgresCursor(_EmptyCursor())
        return_id = _should_return_id(translated)
        if return_id:
            translated = _append_returning_id(translated)
        cursor = self._conn.execute(translated, params)
        lastrowid = 0
        if return_id:
            row = cursor.fetchone()
            lastrowid = int(row['id']) if row and row.get('id') is not None else 0
        return PostgresCursor(cursor, lastrowid)

    def executemany(self, query: str, rows: list[tuple[Any, ...]]) -> None:
        translated = translate_sql(query)
        if translated:
            self._conn.cursor().executemany(translated, rows)

    def executescript(self, script: str) -> None:
        statements = [translate_sql(statement, ddl=True) for statement in split_sql_script(script)]
        statements = [statement for statement in statements if statement]
        for statement in order_postgres_ddl(statements):
            self._conn.execute(statement)


class MssqlCursor:
    def __init__(self, cursor: Any, lastrowid: int = 0) -> None:
        self._cursor = cursor
        self.lastrowid = lastrowid

    def fetchall(self) -> list[dict[str, Any]]:
        rows = self._cursor.fetchall()
        columns = [column[0] for column in (self._cursor.description or [])]
        return [dict(zip(columns, row)) for row in rows]

    def fetchone(self) -> dict[str, Any] | None:
        row = self._cursor.fetchone()
        if row is None:
            return None
        columns = [column[0] for column in (self._cursor.description or [])]
        return dict(zip(columns, row))


class MssqlConnection:
    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> MssqlCursor:
        translated = translate_mssql_sql(query)
        if not translated:
            return MssqlCursor(_EmptyCursor())
        return_id = _should_return_id(translated)
        cursor = self._conn.cursor()
        cursor.execute(translated, params)
        lastrowid = 0
        if return_id:
            identity_cursor = self._conn.cursor()
            identity_cursor.execute('SELECT CAST(SCOPE_IDENTITY() AS int) AS id')
            row = identity_cursor.fetchone()
            lastrowid = int(row[0]) if row and row[0] is not None else 0
        return MssqlCursor(cursor, lastrowid)

    def executemany(self, query: str, rows: list[tuple[Any, ...]]) -> None:
        translated = translate_mssql_sql(query)
        if translated:
            self._conn.cursor().executemany(translated, rows)

    def executescript(self, script: str) -> None:
        statements = [translate_mssql_sql(statement, ddl=True) for statement in split_sql_script(script)]
        statements = [statement for statement in statements if statement]
        for statement in order_postgres_ddl(statements):
            self._conn.cursor().execute(statement)


class _EmptyCursor:
    def fetchall(self) -> list[dict[str, Any]]:
        return []

    def fetchone(self) -> dict[str, Any] | None:
        return None


def split_sql_script(script: str) -> list[str]:
    return [part.strip() for part in script.split(';') if part.strip()]


def order_postgres_ddl(statements: list[str]) -> list[str]:
    table_statements: list[tuple[str, str, set[str]]] = []
    other_statements: list[str] = []
    for statement in statements:
        table_name = _create_table_name(statement)
        if table_name:
            table_statements.append((table_name, statement, _referenced_tables(statement)))
        else:
            other_statements.append(statement)

    if not table_statements:
        return statements

    remaining = table_statements[:]
    emitted_tables: set[str] = set()
    ordered_tables: list[str] = []
    all_tables = {table for table, _, _ in table_statements}
    while remaining:
        ready = [
            item for item in remaining
            if not ((item[2] & all_tables) - emitted_tables - {item[0]})
        ]
        if not ready:
            ordered_tables.extend(statement for _, statement, _ in remaining)
            break
        for item in ready:
            emitted_tables.add(item[0])
            ordered_tables.append(item[1])
            remaining.remove(item)
    return ordered_tables + other_statements


def _create_table_name(statement: str) -> str | None:
    match = re.match(r'\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+"?([\w]+)"?', statement, re.IGNORECASE)
    return match.group(1).lower() if match else None


def _referenced_tables(statement: str) -> set[str]:
    return {match.group(1).lower() for match in re.finditer(r'\bREFERENCES\s+"?([\w]+)"?', statement, re.IGNORECASE)}


def translate_sql(query: str, ddl: bool = False) -> str:
    sql = query.strip().rstrip(';')
    if not sql:
        return ''
    if sql.upper().startswith('PRAGMA '):
        return ''
    sql = sql.replace('datetime(\'now\')', 'CURRENT_TIMESTAMP')
    sql = re.sub(r'\bINTEGER PRIMARY KEY AUTOINCREMENT\b', 'SERIAL PRIMARY KEY', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bREAL\b', 'DOUBLE PRECISION', sql, flags=re.IGNORECASE)
    insert_ignore = bool(re.match(r'^\s*INSERT\s+OR\s+IGNORE\s+INTO\b', sql, flags=re.IGNORECASE))
    sql = re.sub(r'^\s*INSERT\s+OR\s+IGNORE\s+INTO\b', 'INSERT INTO', sql, flags=re.IGNORECASE)
    sql = _replace_placeholders(sql)
    if insert_ignore and ' ON CONFLICT ' not in sql.upper():
        sql = f'{sql} ON CONFLICT DO NOTHING'
    return sql


def translate_mssql_sql(query: str, ddl: bool = False) -> str:
    sql = query.strip().rstrip(';')
    if not sql:
        return ''
    if sql.upper().startswith('PRAGMA '):
        return ''
    sql = sql.replace("datetime('now')", 'SYSUTCDATETIME()')
    sql = re.sub(r'\bINTEGER PRIMARY KEY AUTOINCREMENT\b', 'INT IDENTITY(1,1) PRIMARY KEY', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bREAL\b', 'FLOAT', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bBOOLEAN\b', 'BIT', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bTEXT\b', 'NVARCHAR(MAX)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'^\s*INSERT\s+OR\s+IGNORE\s+INTO\b', 'INSERT INTO', sql, flags=re.IGNORECASE)
    if ddl:
        sql = _wrap_mssql_create_table_if_needed(sql)
        sql = _wrap_mssql_create_index_if_needed(sql)
    return sql


def _wrap_mssql_create_table_if_needed(sql: str) -> str:
    match = re.match(r'\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+("?[\w]+"?)\s*(.*)$', sql, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return sql
    table = match.group(1).strip('"')
    definition = match.group(2)
    return f"IF OBJECT_ID(N'{table}', N'U') IS NULL CREATE TABLE {table} {definition}"


def _wrap_mssql_create_index_if_needed(sql: str) -> str:
    match = re.match(r'\s*CREATE\s+(UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS\s+("?[\w]+"?)\s+ON\s+("?[\w]+"?)\s*(.*)$', sql, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return sql
    unique = match.group(1) or ''
    index = match.group(2).strip('"')
    table = match.group(3).strip('"')
    definition = match.group(4)
    return f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{index}' AND object_id = OBJECT_ID(N'{table}')) CREATE {unique}INDEX {index} ON {table} {definition}"


def _replace_placeholders(sql: str) -> str:
    result = []
    in_single = False
    for char in sql:
        if char == "'":
            in_single = not in_single
            result.append(char)
        elif char == '?' and not in_single:
            result.append('%s')
        else:
            result.append(char)
    return ''.join(result)


def _should_return_id(sql: str) -> bool:
    upper = sql.lstrip().upper()
    if not upper.startswith('INSERT INTO ') or ' RETURNING ' in upper:
        return False
    match = re.match(r'\s*INSERT\s+INTO\s+("?[\w]+"?)', sql, flags=re.IGNORECASE)
    if not match:
        return False
    table_name = match.group(1).strip('"').lower()
    return table_name not in _NO_RETURNING_ID_TABLES


def _append_returning_id(sql: str) -> str:
    return f'{sql.rstrip()} RETURNING id'


def init_db() -> None:
    with get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS dimensions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                UNIQUE(kind, code)
            );

            CREATE TABLE IF NOT EXISTS scenarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                version TEXT NOT NULL,
                status TEXT NOT NULL,
                start_period TEXT NOT NULL,
                end_period TEXT NOT NULL,
                locked INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS plan_line_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                department_code TEXT NOT NULL,
                fund_code TEXT NOT NULL,
                account_code TEXT NOT NULL,
                period TEXT NOT NULL,
                amount REAL NOT NULL,
                notes TEXT DEFAULT '',
                source TEXT NOT NULL DEFAULT 'manual',
                driver_key TEXT DEFAULT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS fiscal_periods (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fiscal_year TEXT NOT NULL,
                period TEXT NOT NULL UNIQUE,
                period_index INTEGER NOT NULL,
                is_closed INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS dimension_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dimension_kind TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                parent_code TEXT DEFAULT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(dimension_kind, code)
            );

            CREATE TABLE IF NOT EXISTS master_data_change_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dimension_kind TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                parent_code TEXT DEFAULT NULL,
                change_type TEXT NOT NULL,
                effective_from TEXT NOT NULL,
                effective_to TEXT DEFAULT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                requested_by TEXT NOT NULL,
                requested_at TEXT NOT NULL,
                approved_by TEXT DEFAULT NULL,
                approved_at TEXT DEFAULT NULL
            );

            CREATE TABLE IF NOT EXISTS master_data_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mapping_key TEXT NOT NULL UNIQUE,
                source_system TEXT NOT NULL,
                source_dimension TEXT NOT NULL,
                source_code TEXT NOT NULL,
                target_dimension TEXT NOT NULL,
                target_code TEXT NOT NULL,
                effective_from TEXT NOT NULL,
                effective_to TEXT DEFAULT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS metadata_approval_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'pending',
                requested_by TEXT NOT NULL,
                requested_at TEXT NOT NULL,
                approved_by TEXT DEFAULT NULL,
                approved_at TEXT DEFAULT NULL
            );

            CREATE TABLE IF NOT EXISTS data_lineage_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                transform_type TEXT NOT NULL,
                target_type TEXT NOT NULL,
                target_id TEXT NOT NULL,
                record_count INTEGER NOT NULL DEFAULT 0,
                amount_total REAL NOT NULL DEFAULT 0,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS planning_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                entity_code TEXT NOT NULL DEFAULT 'CAMPUS',
                department_code TEXT NOT NULL,
                fund_code TEXT NOT NULL,
                account_code TEXT NOT NULL,
                program_code TEXT DEFAULT NULL,
                project_code TEXT DEFAULT NULL,
                grant_code TEXT DEFAULT NULL,
                period TEXT NOT NULL,
                amount REAL NOT NULL,
                source TEXT NOT NULL,
                driver_key TEXT DEFAULT NULL,
                notes TEXT DEFAULT '',
                ledger_type TEXT NOT NULL DEFAULT 'planning',
                ledger_basis TEXT NOT NULL DEFAULT 'budget',
                source_version TEXT DEFAULT NULL,
                source_record_id TEXT DEFAULT NULL,
                parent_ledger_entry_id INTEGER DEFAULT NULL,
                import_batch_id INTEGER DEFAULT NULL,
                legacy_line_item_id INTEGER DEFAULT NULL UNIQUE,
                idempotency_key TEXT DEFAULT NULL,
                posted_checksum TEXT DEFAULT NULL,
                immutable_posting INTEGER NOT NULL DEFAULT 1,
                posted_by TEXT NOT NULL DEFAULT 'system',
                posted_at TEXT NOT NULL,
                reversed_at TEXT DEFAULT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_planning_ledger_scenario_active
            ON planning_ledger (scenario_id, reversed_at, period);

            CREATE INDEX IF NOT EXISTS idx_planning_ledger_dimensions
            ON planning_ledger (department_code, fund_code, account_code, period);

            CREATE INDEX IF NOT EXISTS idx_planning_ledger_scenario_period_account
            ON planning_ledger (scenario_id, period, account_code, reversed_at);

            CREATE INDEX IF NOT EXISTS idx_planning_ledger_import_batch
            ON planning_ledger (import_batch_id);

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                must_change_password INTEGER NOT NULL DEFAULT 0,
                password_changed_at TEXT DEFAULT NULL,
                last_login_at TEXT DEFAULT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS drivers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                driver_key TEXT NOT NULL,
                label TEXT NOT NULL,
                expression TEXT DEFAULT NULL,
                value REAL DEFAULT NULL,
                unit TEXT NOT NULL DEFAULT 'currency',
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                UNIQUE(scenario_id, driver_key)
            );

            CREATE TABLE IF NOT EXISTS workflows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                step TEXT NOT NULL,
                status TEXT NOT NULL,
                owner TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS workflow_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_key TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS workflow_template_steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_id INTEGER NOT NULL,
                step_order INTEGER NOT NULL,
                step_key TEXT NOT NULL,
                label TEXT NOT NULL,
                approver_role TEXT DEFAULT NULL,
                approver_user_id INTEGER DEFAULT NULL,
                escalation_hours REAL DEFAULT NULL,
                escalation_user_id INTEGER DEFAULT NULL,
                notification_template TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                UNIQUE(template_id, step_order),
                UNIQUE(template_id, step_key),
                FOREIGN KEY (template_id) REFERENCES workflow_templates(id) ON DELETE CASCADE,
                FOREIGN KEY (approver_user_id) REFERENCES users(id) ON DELETE SET NULL,
                FOREIGN KEY (escalation_user_id) REFERENCES users(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS workflow_instances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_id INTEGER NOT NULL,
                scenario_id INTEGER NOT NULL,
                subject_type TEXT NOT NULL,
                subject_id TEXT NOT NULL,
                current_step_key TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                started_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT DEFAULT NULL,
                FOREIGN KEY (template_id) REFERENCES workflow_templates(id) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS workflow_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id INTEGER NOT NULL,
                step_id INTEGER NOT NULL,
                assigned_role TEXT DEFAULT NULL,
                assigned_user_id INTEGER DEFAULT NULL,
                delegated_from_user_id INTEGER DEFAULT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                decision TEXT DEFAULT NULL,
                note TEXT NOT NULL DEFAULT '',
                due_at TEXT DEFAULT NULL,
                created_at TEXT NOT NULL,
                completed_by TEXT DEFAULT NULL,
                completed_at TEXT DEFAULT NULL,
                escalated_at TEXT DEFAULT NULL,
                FOREIGN KEY (instance_id) REFERENCES workflow_instances(id) ON DELETE CASCADE,
                FOREIGN KEY (step_id) REFERENCES workflow_template_steps(id) ON DELETE CASCADE,
                FOREIGN KEY (assigned_user_id) REFERENCES users(id) ON DELETE SET NULL,
                FOREIGN KEY (delegated_from_user_id) REFERENCES users(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS workflow_delegations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_user_id INTEGER NOT NULL,
                to_user_id INTEGER NOT NULL,
                starts_at TEXT NOT NULL,
                ends_at TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (from_user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (to_user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS workflow_escalation_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                escalated_to_user_id INTEGER DEFAULT NULL,
                reason TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (task_id) REFERENCES workflow_tasks(id) ON DELETE CASCADE,
                FOREIGN KEY (escalated_to_user_id) REFERENCES users(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS workflow_visual_designs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_id INTEGER NOT NULL,
                layout_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(template_id),
                FOREIGN KEY (template_id) REFERENCES workflow_templates(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS process_calendars (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                calendar_key TEXT NOT NULL,
                process_type TEXT NOT NULL,
                period TEXT NOT NULL,
                milestone_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'planned',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(scenario_id, calendar_key),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS workflow_substitute_approvers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_user_id INTEGER NOT NULL,
                substitute_user_id INTEGER NOT NULL,
                process_type TEXT NOT NULL DEFAULT 'all',
                starts_at TEXT NOT NULL,
                ends_at TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (original_user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (substitute_user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS workflow_certification_packets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                packet_key TEXT NOT NULL,
                process_type TEXT NOT NULL,
                period TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'assembled',
                contents_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(scenario_id, packet_key),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS process_campaign_monitors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                campaign_key TEXT NOT NULL,
                process_type TEXT NOT NULL,
                period TEXT NOT NULL,
                total_items INTEGER NOT NULL DEFAULT 0,
                completed_items INTEGER NOT NULL DEFAULT 0,
                overdue_items INTEGER NOT NULL DEFAULT 0,
                escalated_items INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'monitoring',
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(scenario_id, campaign_key),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                action TEXT NOT NULL,
                actor TEXT NOT NULL,
                detail_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_log_hashes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                audit_log_id INTEGER NOT NULL UNIQUE,
                previous_hash TEXT NOT NULL,
                row_hash TEXT NOT NULL,
                sealed_at TEXT NOT NULL,
                FOREIGN KEY (audit_log_id) REFERENCES audit_logs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sod_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_key TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                conflict_type TEXT NOT NULL,
                left_value TEXT NOT NULL,
                right_value TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'medium',
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS retention_policies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                policy_key TEXT NOT NULL UNIQUE,
                entity_type TEXT NOT NULL,
                retention_years INTEGER NOT NULL,
                disposition_action TEXT NOT NULL,
                legal_hold INTEGER NOT NULL DEFAULT 0,
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS compliance_certifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER DEFAULT NULL,
                certification_key TEXT NOT NULL UNIQUE,
                control_area TEXT NOT NULL,
                period TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                owner TEXT NOT NULL,
                due_at TEXT DEFAULT NULL,
                certified_by TEXT DEFAULT NULL,
                certified_at TEXT DEFAULT NULL,
                evidence_json TEXT NOT NULL DEFAULT '{}',
                notes TEXT NOT NULL DEFAULT '',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS tax_activity_classifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                classification_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                ledger_entry_id INTEGER DEFAULT NULL,
                activity_name TEXT NOT NULL,
                tax_status TEXT NOT NULL,
                activity_tag TEXT NOT NULL,
                income_type TEXT NOT NULL,
                ubit_code TEXT DEFAULT NULL,
                regularly_carried_on INTEGER NOT NULL DEFAULT 0,
                substantially_related INTEGER NOT NULL DEFAULT 1,
                debt_financed INTEGER NOT NULL DEFAULT 0,
                amount REAL NOT NULL DEFAULT 0,
                expense_offset REAL NOT NULL DEFAULT 0,
                net_ubti REAL NOT NULL DEFAULT 0,
                form990_part TEXT DEFAULT NULL,
                form990_line TEXT DEFAULT NULL,
                form990_column TEXT DEFAULT NULL,
                review_status TEXT NOT NULL DEFAULT 'draft',
                reviewer TEXT DEFAULT NULL,
                reviewed_at TEXT DEFAULT NULL,
                notes TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                FOREIGN KEY (ledger_entry_id) REFERENCES planning_ledger(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS tax_rule_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_key TEXT NOT NULL UNIQUE,
                jurisdiction TEXT NOT NULL DEFAULT 'US',
                source_name TEXT NOT NULL,
                source_url TEXT NOT NULL,
                rule_area TEXT NOT NULL,
                latest_known_version TEXT NOT NULL DEFAULT '',
                check_frequency_days INTEGER NOT NULL DEFAULT 30,
                last_checked_at TEXT DEFAULT NULL,
                next_check_at TEXT DEFAULT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                notes TEXT NOT NULL DEFAULT '',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tax_update_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_key TEXT NOT NULL UNIQUE,
                source_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                detected_change INTEGER NOT NULL DEFAULT 0,
                previous_version TEXT NOT NULL DEFAULT '',
                detected_version TEXT NOT NULL DEFAULT '',
                detail_json TEXT NOT NULL DEFAULT '{}',
                checked_by TEXT NOT NULL,
                checked_at TEXT NOT NULL,
                FOREIGN KEY (source_id) REFERENCES tax_rule_sources(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS tax_change_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_key TEXT NOT NULL UNIQUE,
                source_id INTEGER NOT NULL,
                severity TEXT NOT NULL DEFAULT 'warning',
                status TEXT NOT NULL DEFAULT 'open',
                message TEXT NOT NULL,
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                acknowledged_by TEXT DEFAULT NULL,
                acknowledged_at TEXT DEFAULT NULL,
                resolved_by TEXT DEFAULT NULL,
                resolved_at TEXT DEFAULT NULL,
                FOREIGN KEY (source_id) REFERENCES tax_rule_sources(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS tax_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_key TEXT NOT NULL UNIQUE,
                classification_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                decision TEXT NOT NULL DEFAULT 'review',
                reviewer TEXT NOT NULL,
                note TEXT NOT NULL DEFAULT '',
                evidence_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                FOREIGN KEY (classification_id) REFERENCES tax_activity_classifications(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS form990_support_fields (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                support_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                form_part TEXT NOT NULL,
                line_number TEXT NOT NULL,
                column_code TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL,
                amount REAL NOT NULL DEFAULT 0,
                basis_json TEXT NOT NULL DEFAULT '{}',
                review_status TEXT NOT NULL DEFAULT 'draft',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS integrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                status TEXT NOT NULL,
                direction TEXT NOT NULL,
                endpoint_hint TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS schema_migrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_key TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS migration_locks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lock_key TEXT NOT NULL UNIQUE,
                owner TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS migration_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_key TEXT NOT NULL,
                direction TEXT NOT NULL DEFAULT 'up',
                status TEXT NOT NULL,
                dry_run INTEGER NOT NULL DEFAULT 0,
                checksum TEXT NOT NULL DEFAULT '',
                sql_path TEXT NOT NULL DEFAULT '',
                rollback_path TEXT NOT NULL DEFAULT '',
                postgres_sql_json TEXT NOT NULL DEFAULT '[]',
                message TEXT NOT NULL DEFAULT '',
                started_at TEXT NOT NULL,
                completed_at TEXT DEFAULT NULL
            );

            CREATE TABLE IF NOT EXISTS backup_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backup_key TEXT NOT NULL UNIQUE,
                path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                note TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                must_change_password INTEGER NOT NULL DEFAULT 0,
                password_changed_at TEXT DEFAULT NULL,
                last_login_at TEXT DEFAULT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS roles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                role_key TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                permission_key TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS user_roles (
                user_id INTEGER NOT NULL,
                role_id INTEGER NOT NULL,
                PRIMARY KEY (user_id, role_id),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS role_permissions (
                role_id INTEGER NOT NULL,
                permission_id INTEGER NOT NULL,
                PRIMARY KEY (role_id, permission_id),
                FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE,
                FOREIGN KEY (permission_id) REFERENCES permissions(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS user_dimension_access (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                dimension_kind TEXT NOT NULL,
                code TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(user_id, dimension_kind, code)
            );

            CREATE TABLE IF NOT EXISTS auth_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                revoked_at TEXT DEFAULT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sso_providers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider_key TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                protocol TEXT NOT NULL,
                issuer_url TEXT DEFAULT '',
                authorize_url TEXT DEFAULT '',
                token_url TEXT DEFAULT '',
                jwks_url TEXT DEFAULT '',
                client_id TEXT DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS user_external_identities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                provider_key TEXT NOT NULL,
                external_subject TEXT NOT NULL,
                email TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(provider_key, external_subject)
            );

            CREATE TABLE IF NOT EXISTS sso_production_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider_key TEXT NOT NULL UNIQUE,
                environment TEXT NOT NULL DEFAULT 'production',
                metadata_url TEXT NOT NULL DEFAULT '',
                required_claim TEXT NOT NULL DEFAULT 'email',
                group_claim TEXT NOT NULL DEFAULT 'groups',
                jit_provisioning INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'draft',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ad_ou_group_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mapping_key TEXT NOT NULL UNIQUE,
                ad_group_dn TEXT NOT NULL,
                allowed_ou_dn TEXT NOT NULL,
                role_key TEXT NOT NULL,
                dimension_kind TEXT DEFAULT NULL,
                dimension_code TEXT DEFAULT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS domain_vpn_enforcement_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_key TEXT NOT NULL UNIQUE,
                host TEXT NOT NULL,
                client_host TEXT NOT NULL,
                forwarded_host TEXT NOT NULL DEFAULT '',
                forwarded_for TEXT NOT NULL DEFAULT '',
                allowed INTEGER NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS admin_impersonation_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_user_id INTEGER NOT NULL,
                target_user_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'issued',
                token_expires_at TEXT DEFAULT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT DEFAULT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (admin_user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (target_user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS user_access_review_certifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER DEFAULT NULL,
                reviewer_user_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                scope_json TEXT NOT NULL DEFAULT '{}',
                findings_json TEXT NOT NULL DEFAULT '[]',
                certified_by TEXT DEFAULT NULL,
                certified_at TEXT DEFAULT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE SET NULL,
                FOREIGN KEY (reviewer_user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS budget_submissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                department_code TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft',
                owner TEXT NOT NULL,
                notes TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                submitted_at TEXT DEFAULT NULL,
                approved_at TEXT DEFAULT NULL,
                approved_by TEXT DEFAULT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                UNIQUE(scenario_id, department_code)
            );

            CREATE TABLE IF NOT EXISTS budget_assumptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                department_code TEXT DEFAULT NULL,
                assumption_key TEXT NOT NULL,
                label TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT NOT NULL DEFAULT 'ratio',
                notes TEXT DEFAULT '',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                UNIQUE(scenario_id, department_code, assumption_key)
            );

            CREATE TABLE IF NOT EXISTS operating_budget_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                submission_id INTEGER NOT NULL,
                ledger_entry_id INTEGER NOT NULL,
                line_type TEXT NOT NULL,
                recurrence TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (submission_id) REFERENCES budget_submissions(id) ON DELETE CASCADE,
                FOREIGN KEY (ledger_entry_id) REFERENCES planning_ledger(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS budget_transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                from_department_code TEXT NOT NULL,
                to_department_code TEXT NOT NULL,
                fund_code TEXT NOT NULL,
                account_code TEXT NOT NULL,
                period TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'requested',
                reason TEXT NOT NULL,
                requested_by TEXT NOT NULL,
                approved_by TEXT DEFAULT NULL,
                from_ledger_entry_id INTEGER DEFAULT NULL,
                to_ledger_entry_id INTEGER DEFAULT NULL,
                created_at TEXT NOT NULL,
                approved_at TEXT DEFAULT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS enrollment_terms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                term_code TEXT NOT NULL,
                term_name TEXT NOT NULL,
                period TEXT NOT NULL,
                census_date TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                UNIQUE(scenario_id, term_code)
            );

            CREATE TABLE IF NOT EXISTS tuition_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                program_code TEXT NOT NULL,
                residency TEXT NOT NULL,
                rate_per_credit REAL NOT NULL,
                default_credit_load REAL NOT NULL,
                effective_term TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                UNIQUE(scenario_id, program_code, residency, effective_term)
            );

            CREATE TABLE IF NOT EXISTS enrollment_forecast_inputs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                term_code TEXT NOT NULL,
                program_code TEXT NOT NULL,
                residency TEXT NOT NULL,
                headcount REAL NOT NULL,
                fte REAL NOT NULL,
                retention_rate REAL NOT NULL,
                yield_rate REAL NOT NULL,
                discount_rate REAL NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                UNIQUE(scenario_id, term_code, program_code, residency)
            );

            CREATE TABLE IF NOT EXISTS tuition_forecast_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                term_code TEXT NOT NULL,
                status TEXT NOT NULL,
                gross_revenue REAL NOT NULL,
                discount_amount REAL NOT NULL,
                net_revenue REAL NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS tuition_forecast_run_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                ledger_entry_id INTEGER NOT NULL,
                program_code TEXT NOT NULL,
                residency TEXT NOT NULL,
                headcount REAL NOT NULL,
                fte REAL NOT NULL,
                gross_revenue REAL NOT NULL,
                discount_amount REAL NOT NULL,
                net_revenue REAL NOT NULL,
                FOREIGN KEY (run_id) REFERENCES tuition_forecast_runs(id) ON DELETE CASCADE,
                FOREIGN KEY (ledger_entry_id) REFERENCES planning_ledger(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS workforce_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                position_code TEXT NOT NULL,
                title TEXT NOT NULL,
                department_code TEXT NOT NULL,
                employee_type TEXT NOT NULL,
                fte REAL NOT NULL,
                annual_salary REAL NOT NULL,
                benefit_rate REAL NOT NULL,
                vacancy_rate REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'planned',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                UNIQUE(scenario_id, position_code)
            );

            CREATE TABLE IF NOT EXISTS faculty_loads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                department_code TEXT NOT NULL,
                term_code TEXT NOT NULL,
                course_code TEXT NOT NULL,
                sections INTEGER NOT NULL,
                credit_hours REAL NOT NULL,
                faculty_fte REAL NOT NULL,
                adjunct_cost REAL NOT NULL DEFAULT 0,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                UNIQUE(scenario_id, department_code, term_code, course_code)
            );

            CREATE TABLE IF NOT EXISTS grant_budgets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                grant_code TEXT NOT NULL,
                department_code TEXT NOT NULL,
                sponsor TEXT NOT NULL,
                start_period TEXT NOT NULL,
                end_period TEXT NOT NULL,
                total_award REAL NOT NULL,
                direct_cost_budget REAL NOT NULL,
                indirect_cost_rate REAL NOT NULL,
                spent_to_date REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                UNIQUE(scenario_id, grant_code)
            );

            CREATE TABLE IF NOT EXISTS capital_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                request_code TEXT NOT NULL,
                department_code TEXT NOT NULL,
                project_name TEXT NOT NULL,
                asset_category TEXT NOT NULL,
                acquisition_period TEXT NOT NULL,
                capital_cost REAL NOT NULL,
                useful_life_years INTEGER NOT NULL,
                funding_source TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'requested',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                approved_by TEXT DEFAULT NULL,
                approved_at TEXT DEFAULT NULL,
                ledger_entry_id INTEGER DEFAULT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                FOREIGN KEY (ledger_entry_id) REFERENCES planning_ledger(id) ON DELETE SET NULL,
                UNIQUE(scenario_id, request_code)
            );

            CREATE TABLE IF NOT EXISTS typed_drivers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                driver_key TEXT NOT NULL,
                label TEXT NOT NULL,
                driver_type TEXT NOT NULL,
                unit TEXT NOT NULL,
                value REAL NOT NULL,
                locked INTEGER NOT NULL DEFAULT 0,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                UNIQUE(scenario_id, driver_key)
            );

            CREATE TABLE IF NOT EXISTS forecast_methods (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                method_key TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS forecast_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                method_key TEXT NOT NULL,
                driver_key TEXT DEFAULT NULL,
                account_code TEXT NOT NULL,
                department_code TEXT DEFAULT NULL,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                status TEXT NOT NULL,
                confidence_low REAL NOT NULL,
                confidence_high REAL NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS forecast_lineage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                forecast_run_id INTEGER NOT NULL,
                ledger_entry_id INTEGER NOT NULL,
                driver_key TEXT DEFAULT NULL,
                method_key TEXT NOT NULL,
                source_ledger_entry_id INTEGER DEFAULT NULL,
                confidence_low REAL NOT NULL,
                confidence_high REAL NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (forecast_run_id) REFERENCES forecast_runs(id) ON DELETE CASCADE,
                FOREIGN KEY (ledger_entry_id) REFERENCES planning_ledger(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS predictive_model_choices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                choice_key TEXT NOT NULL,
                account_code TEXT NOT NULL,
                department_code TEXT DEFAULT NULL,
                selected_method TEXT NOT NULL,
                seasonality_mode TEXT NOT NULL DEFAULT 'auto',
                confidence_level REAL NOT NULL DEFAULT 0.8,
                status TEXT NOT NULL DEFAULT 'selected',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                UNIQUE(scenario_id, choice_key)
            );

            CREATE TABLE IF NOT EXISTS forecast_backtests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                choice_id INTEGER NOT NULL,
                method_key TEXT NOT NULL,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                status TEXT NOT NULL,
                accuracy_score REAL NOT NULL DEFAULT 0,
                mape REAL NOT NULL DEFAULT 0,
                rmse REAL NOT NULL DEFAULT 0,
                result_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                FOREIGN KEY (choice_id) REFERENCES predictive_model_choices(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS forecast_tuning_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                choice_id INTEGER NOT NULL,
                seasonality_strength REAL NOT NULL DEFAULT 1,
                confidence_level REAL NOT NULL DEFAULT 0.8,
                confidence_spread REAL NOT NULL DEFAULT 0.2,
                driver_weights_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (choice_id) REFERENCES predictive_model_choices(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS forecast_recommendation_comparisons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                account_code TEXT NOT NULL,
                department_code TEXT DEFAULT NULL,
                comparison_json TEXT NOT NULL DEFAULT '{}',
                recommended_method TEXT NOT NULL,
                explanation_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS forecast_driver_explanations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                account_code TEXT NOT NULL,
                department_code TEXT DEFAULT NULL,
                driver_key TEXT NOT NULL,
                contribution_score REAL NOT NULL DEFAULT 0,
                explanation TEXT NOT NULL,
                evidence_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS report_definitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                report_type TEXT NOT NULL,
                row_dimension TEXT NOT NULL,
                column_dimension TEXT NOT NULL,
                filters_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dashboard_widgets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                widget_type TEXT NOT NULL,
                metric_key TEXT NOT NULL,
                scenario_id INTEGER NOT NULL,
                config_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS report_exports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_definition_id INTEGER NOT NULL,
                scenario_id INTEGER NOT NULL,
                export_format TEXT NOT NULL,
                schedule_cron TEXT NOT NULL,
                destination TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'scheduled',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (report_definition_id) REFERENCES report_definitions(id) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS report_layouts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER DEFAULT NULL,
                report_definition_id INTEGER DEFAULT NULL,
                layout_key TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                layout_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                FOREIGN KEY (report_definition_id) REFERENCES report_definitions(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS report_charts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                chart_key TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                chart_type TEXT NOT NULL,
                dataset_type TEXT NOT NULL,
                config_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS chart_render_artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chart_id INTEGER NOT NULL,
                scenario_id INTEGER NOT NULL,
                render_key TEXT NOT NULL UNIQUE,
                render_format TEXT NOT NULL,
                renderer TEXT NOT NULL DEFAULT 'mu-chart-renderer-v1',
                file_name TEXT NOT NULL,
                storage_path TEXT NOT NULL,
                content_type TEXT NOT NULL,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                width INTEGER NOT NULL DEFAULT 960,
                height INTEGER NOT NULL DEFAULT 540,
                visual_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (chart_id) REFERENCES report_charts(id) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_chart_render_artifacts_chart
                ON chart_render_artifacts(chart_id, created_at);

            CREATE INDEX IF NOT EXISTS idx_chart_render_artifacts_scenario
                ON chart_render_artifacts(scenario_id, render_format);

            CREATE TABLE IF NOT EXISTS dashboard_chart_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                chart_id INTEGER DEFAULT NULL,
                widget_id INTEGER DEFAULT NULL,
                render_id INTEGER DEFAULT NULL,
                snapshot_key TEXT NOT NULL UNIQUE,
                snapshot_type TEXT NOT NULL DEFAULT 'dashboard_chart',
                status TEXT NOT NULL DEFAULT 'retained',
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                FOREIGN KEY (chart_id) REFERENCES report_charts(id) ON DELETE SET NULL,
                FOREIGN KEY (widget_id) REFERENCES dashboard_widgets(id) ON DELETE SET NULL,
                FOREIGN KEY (render_id) REFERENCES chart_render_artifacts(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_dashboard_chart_snapshots_scenario
                ON dashboard_chart_snapshots(scenario_id, created_at);

            CREATE TABLE IF NOT EXISTS report_books (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                book_key TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                layout_id INTEGER DEFAULT NULL,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'assembled',
                contents_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                FOREIGN KEY (layout_id) REFERENCES report_layouts(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS report_burst_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                book_id INTEGER NOT NULL,
                burst_key TEXT NOT NULL UNIQUE,
                burst_dimension TEXT NOT NULL,
                recipients_json TEXT NOT NULL DEFAULT '[]',
                export_format TEXT NOT NULL DEFAULT 'pdf',
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                FOREIGN KEY (book_id) REFERENCES report_books(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS recurring_report_packages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                book_id INTEGER NOT NULL,
                package_key TEXT NOT NULL UNIQUE,
                schedule_cron TEXT NOT NULL,
                destination TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'scheduled',
                next_run_at TEXT DEFAULT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                FOREIGN KEY (book_id) REFERENCES report_books(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS recurring_report_package_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recurring_package_id INTEGER NOT NULL,
                artifact_id INTEGER DEFAULT NULL,
                status TEXT NOT NULL,
                recipient_count INTEGER NOT NULL DEFAULT 0,
                run_detail_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (recurring_package_id) REFERENCES recurring_report_packages(id) ON DELETE CASCADE,
                FOREIGN KEY (artifact_id) REFERENCES export_artifacts(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS report_footnotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                footnote_key TEXT NOT NULL UNIQUE,
                target_type TEXT NOT NULL,
                target_id INTEGER DEFAULT NULL,
                marker TEXT NOT NULL,
                footnote_text TEXT NOT NULL,
                display_order INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS report_page_breaks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_book_id INTEGER NOT NULL,
                section_key TEXT NOT NULL,
                page_number INTEGER NOT NULL,
                break_before INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (report_book_id) REFERENCES report_books(id) ON DELETE CASCADE,
                UNIQUE(report_book_id, section_key)
            );

            CREATE TABLE IF NOT EXISTS pdf_pagination_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER DEFAULT NULL,
                name TEXT NOT NULL,
                page_size TEXT NOT NULL DEFAULT 'Letter',
                orientation TEXT NOT NULL DEFAULT 'portrait',
                margin_top REAL NOT NULL DEFAULT 0.5,
                margin_right REAL NOT NULL DEFAULT 0.5,
                margin_bottom REAL NOT NULL DEFAULT 0.5,
                margin_left REAL NOT NULL DEFAULT 0.5,
                rows_per_page INTEGER NOT NULL DEFAULT 32,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS board_package_release_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recurring_package_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending_approval',
                approval_note TEXT NOT NULL DEFAULT '',
                approved_by TEXT DEFAULT NULL,
                approved_at TEXT DEFAULT NULL,
                released_by TEXT DEFAULT NULL,
                released_at TEXT DEFAULT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (recurring_package_id) REFERENCES recurring_report_packages(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS close_checklists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                checklist_key TEXT NOT NULL,
                title TEXT NOT NULL,
                owner TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                due_date TEXT DEFAULT NULL,
                completed_by TEXT DEFAULT NULL,
                completed_at TEXT DEFAULT NULL,
                evidence_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (scenario_id, period, checklist_key),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS account_reconciliations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                entity_code TEXT NOT NULL,
                account_code TEXT NOT NULL,
                book_balance REAL NOT NULL,
                source_balance REAL NOT NULL,
                variance REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                owner TEXT NOT NULL,
                notes TEXT NOT NULL DEFAULT '',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS intercompany_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                source_entity_code TEXT NOT NULL,
                target_entity_code TEXT NOT NULL,
                account_code TEXT NOT NULL,
                source_amount REAL NOT NULL,
                target_amount REAL NOT NULL,
                variance REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'matched',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS elimination_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                entity_code TEXT NOT NULL,
                account_code TEXT NOT NULL,
                amount REAL NOT NULL,
                reason TEXT NOT NULL,
                ledger_entry_id INTEGER DEFAULT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                FOREIGN KEY (ledger_entry_id) REFERENCES planning_ledger(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS consolidation_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                status TEXT NOT NULL,
                total_before_eliminations REAL NOT NULL,
                total_eliminations REAL NOT NULL,
                consolidated_total REAL NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS audit_packets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                consolidation_run_id INTEGER NOT NULL,
                packet_key TEXT NOT NULL,
                status TEXT NOT NULL,
                contents_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (consolidation_run_id) REFERENCES consolidation_runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS close_task_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_key TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                owner_role TEXT NOT NULL,
                due_day_offset INTEGER NOT NULL DEFAULT 0,
                dependency_keys_json TEXT NOT NULL DEFAULT '[]',
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS close_task_dependencies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                depends_on_task_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                UNIQUE (task_id, depends_on_task_id),
                FOREIGN KEY (task_id) REFERENCES close_checklists(id) ON DELETE CASCADE,
                FOREIGN KEY (depends_on_task_id) REFERENCES close_checklists(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS period_close_calendar (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                close_start TEXT NOT NULL,
                close_due TEXT NOT NULL,
                lock_state TEXT NOT NULL DEFAULT 'open',
                locked_by TEXT DEFAULT NULL,
                locked_at TEXT DEFAULT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (scenario_id, period),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS reconciliation_exceptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reconciliation_id INTEGER NOT NULL,
                exception_key TEXT NOT NULL,
                severity TEXT NOT NULL,
                aging_days INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'open',
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                resolved_by TEXT DEFAULT NULL,
                resolved_at TEXT DEFAULT NULL,
                UNIQUE (reconciliation_id, exception_key),
                FOREIGN KEY (reconciliation_id) REFERENCES account_reconciliations(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS entity_confirmations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                entity_code TEXT NOT NULL,
                confirmation_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'requested',
                requested_by TEXT NOT NULL,
                requested_at TEXT NOT NULL,
                confirmed_by TEXT DEFAULT NULL,
                confirmed_at TEXT DEFAULT NULL,
                response_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE (scenario_id, period, entity_code, confirmation_type),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS consolidation_entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_code TEXT NOT NULL UNIQUE,
                entity_name TEXT NOT NULL,
                parent_entity_code TEXT DEFAULT NULL,
                base_currency TEXT NOT NULL DEFAULT 'USD',
                gaap_basis TEXT NOT NULL DEFAULT 'US_GAAP',
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS entity_ownerships (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                parent_entity_code TEXT NOT NULL,
                child_entity_code TEXT NOT NULL,
                ownership_percent REAL NOT NULL,
                effective_period TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (scenario_id, parent_entity_code, child_entity_code, effective_period),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS consolidation_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                gaap_basis TEXT NOT NULL DEFAULT 'US_GAAP',
                reporting_currency TEXT NOT NULL DEFAULT 'USD',
                translation_method TEXT NOT NULL DEFAULT 'placeholder',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (scenario_id, gaap_basis, reporting_currency),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS consolidation_audit_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                consolidation_run_id INTEGER NOT NULL,
                report_key TEXT NOT NULL UNIQUE,
                report_type TEXT NOT NULL,
                contents_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (consolidation_run_id) REFERENCES consolidation_runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS consolidation_currency_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                from_currency TEXT NOT NULL,
                to_currency TEXT NOT NULL,
                rate REAL NOT NULL,
                rate_type TEXT NOT NULL DEFAULT 'closing',
                source TEXT NOT NULL DEFAULT 'manual',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(scenario_id, period, from_currency, to_currency, rate_type),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS gaap_book_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                source_gaap_basis TEXT NOT NULL,
                target_gaap_basis TEXT NOT NULL,
                source_account_code TEXT NOT NULL,
                target_account_code TEXT NOT NULL,
                adjustment_percent REAL NOT NULL DEFAULT 100,
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(scenario_id, source_gaap_basis, target_gaap_basis, source_account_code),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS consolidation_journals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                consolidation_run_id INTEGER NOT NULL,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                journal_type TEXT NOT NULL,
                entity_code TEXT NOT NULL,
                account_code TEXT NOT NULL,
                debit_amount REAL NOT NULL DEFAULT 0,
                credit_amount REAL NOT NULL DEFAULT 0,
                reporting_currency TEXT NOT NULL DEFAULT 'USD',
                gaap_basis TEXT NOT NULL DEFAULT 'US_GAAP',
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (consolidation_run_id) REFERENCES consolidation_runs(id) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ownership_chain_calculations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                consolidation_run_id INTEGER NOT NULL,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                parent_entity_code TEXT NOT NULL,
                child_entity_code TEXT NOT NULL,
                ownership_chain_json TEXT NOT NULL DEFAULT '[]',
                effective_ownership_percent REAL NOT NULL,
                minority_interest_percent REAL NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (consolidation_run_id) REFERENCES consolidation_runs(id) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS currency_translation_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                consolidation_run_id INTEGER NOT NULL,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                entity_code TEXT NOT NULL,
                account_code TEXT NOT NULL,
                source_currency TEXT NOT NULL,
                reporting_currency TEXT NOT NULL,
                average_rate REAL NOT NULL,
                closing_rate REAL NOT NULL,
                translated_average_amount REAL NOT NULL,
                translated_closing_amount REAL NOT NULL,
                cta_amount REAL NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (consolidation_run_id) REFERENCES consolidation_runs(id) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS statutory_report_packs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                consolidation_run_id INTEGER NOT NULL,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                pack_key TEXT NOT NULL UNIQUE,
                book_basis TEXT NOT NULL,
                reporting_currency TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'assembled',
                contents_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (consolidation_run_id) REFERENCES consolidation_runs(id) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS supplemental_schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                consolidation_run_id INTEGER NOT NULL,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                schedule_key TEXT NOT NULL,
                schedule_type TEXT NOT NULL,
                contents_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(consolidation_run_id, schedule_key),
                FOREIGN KEY (consolidation_run_id) REFERENCES consolidation_runs(id) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS consolidation_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                rule_key TEXT NOT NULL,
                rule_type TEXT NOT NULL,
                source_filter_json TEXT NOT NULL DEFAULT '{}',
                action_json TEXT NOT NULL DEFAULT '{}',
                priority INTEGER NOT NULL DEFAULT 100,
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(scenario_id, rule_key),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS profitability_cost_pools (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                pool_key TEXT NOT NULL,
                name TEXT NOT NULL,
                source_department_code TEXT NOT NULL,
                source_account_code TEXT NOT NULL,
                allocation_basis TEXT NOT NULL DEFAULT 'revenue',
                target_type TEXT NOT NULL DEFAULT 'department',
                target_codes_json TEXT NOT NULL DEFAULT '[]',
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(scenario_id, pool_key),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS profitability_allocation_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                run_key TEXT NOT NULL,
                status TEXT NOT NULL,
                total_source_cost REAL NOT NULL DEFAULT 0,
                total_allocated_cost REAL NOT NULL DEFAULT 0,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(scenario_id, run_key),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS profitability_allocation_trace_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                allocation_run_id INTEGER NOT NULL,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                pool_key TEXT NOT NULL,
                source_department_code TEXT NOT NULL,
                source_account_code TEXT NOT NULL,
                target_type TEXT NOT NULL,
                target_code TEXT NOT NULL,
                basis_value REAL NOT NULL DEFAULT 0,
                allocation_percent REAL NOT NULL DEFAULT 0,
                allocated_amount REAL NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY (allocation_run_id) REFERENCES profitability_allocation_runs(id) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS profitability_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                snapshot_key TEXT NOT NULL,
                snapshot_type TEXT NOT NULL,
                contents_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(scenario_id, snapshot_key),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS external_connectors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_key TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                system_type TEXT NOT NULL,
                direction TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'configured',
                config_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS import_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                connector_key TEXT NOT NULL,
                source_format TEXT NOT NULL,
                import_type TEXT NOT NULL,
                status TEXT NOT NULL,
                total_rows INTEGER NOT NULL DEFAULT 0,
                accepted_rows INTEGER NOT NULL DEFAULT 0,
                rejected_rows INTEGER NOT NULL DEFAULT 0,
                source_name TEXT NOT NULL DEFAULT '',
                stream_chunks INTEGER NOT NULL DEFAULT 1,
                mapping_template_key TEXT DEFAULT NULL,
                mapping_version INTEGER DEFAULT NULL,
                contract_validated INTEGER NOT NULL DEFAULT 0,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS import_rejections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                import_batch_id INTEGER NOT NULL,
                row_number INTEGER NOT NULL,
                reason TEXT NOT NULL,
                row_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (import_batch_id) REFERENCES import_batches(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS import_staging_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                connector_key TEXT NOT NULL,
                source_format TEXT NOT NULL,
                import_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'previewed',
                total_rows INTEGER NOT NULL DEFAULT 0,
                valid_rows INTEGER NOT NULL DEFAULT 0,
                warning_rows INTEGER NOT NULL DEFAULT 0,
                rejected_rows INTEGER NOT NULL DEFAULT 0,
                approved_rows INTEGER NOT NULL DEFAULT 0,
                source_name TEXT NOT NULL DEFAULT '',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                approved_by TEXT DEFAULT NULL,
                approved_at TEXT DEFAULT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS import_staging_rows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                staging_batch_id INTEGER NOT NULL,
                row_number INTEGER NOT NULL,
                raw_json TEXT NOT NULL DEFAULT '{}',
                mapped_json TEXT NOT NULL DEFAULT '{}',
                validation_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'valid',
                decision_note TEXT NOT NULL DEFAULT '',
                import_batch_id INTEGER DEFAULT NULL,
                created_at TEXT NOT NULL,
                decided_by TEXT DEFAULT NULL,
                decided_at TEXT DEFAULT NULL,
                FOREIGN KEY (staging_batch_id) REFERENCES import_staging_batches(id) ON DELETE CASCADE,
                FOREIGN KEY (import_batch_id) REFERENCES import_batches(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS sync_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_key TEXT NOT NULL,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT DEFAULT NULL,
                records_processed INTEGER NOT NULL DEFAULT 0,
                records_rejected INTEGER NOT NULL DEFAULT 0,
                message TEXT NOT NULL DEFAULT '',
                created_by TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS powerbi_exports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                dataset_name TEXT NOT NULL,
                status TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                manifest_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS import_mapping_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_key TEXT NOT NULL UNIQUE,
                connector_key TEXT NOT NULL,
                import_type TEXT NOT NULL,
                mapping_json TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                version INTEGER NOT NULL DEFAULT 1,
                previous_template_key TEXT DEFAULT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS validation_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_key TEXT NOT NULL UNIQUE,
                import_type TEXT NOT NULL,
                field_name TEXT NOT NULL,
                operator TEXT NOT NULL,
                expected_value TEXT DEFAULT NULL,
                severity TEXT NOT NULL DEFAULT 'error',
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS credential_vault (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_key TEXT NOT NULL,
                credential_key TEXT NOT NULL,
                secret_ref TEXT NOT NULL,
                masked_value TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'stored',
                secret_type TEXT NOT NULL DEFAULT 'api_key',
                expires_at TEXT DEFAULT NULL,
                rotated_at TEXT DEFAULT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (connector_key, credential_key)
            );

            CREATE TABLE IF NOT EXISTS integration_retry_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_key TEXT NOT NULL,
                operation_type TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                error_message TEXT NOT NULL DEFAULT '',
                next_retry_at TEXT DEFAULT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS connector_sync_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_key TEXT NOT NULL,
                sync_job_id INTEGER DEFAULT NULL,
                event_type TEXT NOT NULL,
                status TEXT NOT NULL,
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                FOREIGN KEY (sync_job_id) REFERENCES sync_jobs(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS connector_adapters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                adapter_key TEXT NOT NULL UNIQUE,
                system_type TEXT NOT NULL,
                display_name TEXT NOT NULL,
                auth_type TEXT NOT NULL,
                capabilities_json TEXT NOT NULL DEFAULT '[]',
                default_direction TEXT NOT NULL DEFAULT 'inbound',
                status TEXT NOT NULL DEFAULT 'available',
                contract_json TEXT NOT NULL DEFAULT '{}',
                credential_schema_json TEXT NOT NULL DEFAULT '{}',
                max_stream_rows INTEGER NOT NULL DEFAULT 100000,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS connector_auth_flows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_key TEXT NOT NULL,
                adapter_key TEXT NOT NULL,
                auth_type TEXT NOT NULL,
                credential_ref TEXT DEFAULT NULL,
                status TEXT NOT NULL,
                auth_url TEXT NOT NULL DEFAULT '',
                oauth_state TEXT NOT NULL DEFAULT '',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT DEFAULT NULL
            );

            CREATE TABLE IF NOT EXISTS connector_health_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_key TEXT NOT NULL,
                adapter_key TEXT NOT NULL,
                status TEXT NOT NULL,
                latency_ms INTEGER NOT NULL DEFAULT 0,
                message TEXT NOT NULL DEFAULT '',
                checked_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS connector_mapping_presets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                preset_key TEXT NOT NULL UNIQUE,
                adapter_key TEXT NOT NULL,
                import_type TEXT NOT NULL,
                mapping_json TEXT NOT NULL DEFAULT '{}',
                description TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS connector_source_drillbacks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_key TEXT NOT NULL,
                source_record_id TEXT NOT NULL,
                source_url TEXT NOT NULL DEFAULT '',
                source_payload_json TEXT NOT NULL DEFAULT '{}',
                target_type TEXT NOT NULL,
                target_id TEXT NOT NULL,
                validation_status TEXT NOT NULL DEFAULT 'pending',
                validation_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS banking_cash_imports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                connector_key TEXT NOT NULL,
                bank_account TEXT NOT NULL,
                transaction_date TEXT NOT NULL,
                amount REAL NOT NULL,
                description TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'imported',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS crm_enrollment_imports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                connector_key TEXT NOT NULL,
                pipeline_stage TEXT NOT NULL,
                term TEXT NOT NULL,
                headcount INTEGER NOT NULL DEFAULT 0,
                yield_rate REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'imported',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS user_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                default_scenario_id INTEGER DEFAULT NULL,
                default_period TEXT DEFAULT NULL,
                preferences_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (default_scenario_id) REFERENCES scenarios(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER DEFAULT NULL,
                scenario_id INTEGER DEFAULT NULL,
                notification_type TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'info',
                status TEXT NOT NULL DEFAULT 'unread',
                link TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                read_at TEXT DEFAULT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_user_id INTEGER NOT NULL,
                recipient_user_id INTEGER NOT NULL,
                body TEXT NOT NULL,
                sent_at TEXT NOT NULL,
                read_at TEXT DEFAULT NULL,
                notification_id INTEGER DEFAULT NULL,
                FOREIGN KEY (sender_user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (recipient_user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (notification_id) REFERENCES notifications(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS guidance_task_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                checklist_key TEXT NOT NULL,
                task_key TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                completed_at TEXT DEFAULT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(user_id, checklist_key, task_key)
            );

            CREATE TABLE IF NOT EXISTS training_mode_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                scenario_id INTEGER DEFAULT NULL,
                mode_key TEXT NOT NULL,
                role_key TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                started_at TEXT NOT NULL,
                completed_at TEXT DEFAULT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS bulk_paste_imports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                import_type TEXT NOT NULL,
                row_count INTEGER NOT NULL DEFAULT 0,
                accepted_rows INTEGER NOT NULL DEFAULT 0,
                rejected_rows INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                messages_json TEXT NOT NULL DEFAULT '[]',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS automation_recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                assistant_type TEXT NOT NULL,
                subject_type TEXT NOT NULL,
                subject_key TEXT NOT NULL,
                severity TEXT NOT NULL,
                recommendation TEXT NOT NULL,
                rationale_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'pending_review',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                reviewed_by TEXT DEFAULT NULL,
                reviewed_at TEXT DEFAULT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS automation_approval_gates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recommendation_id INTEGER NOT NULL,
                gate_key TEXT NOT NULL,
                required_permission TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                decided_by TEXT DEFAULT NULL,
                decided_at TEXT DEFAULT NULL,
                decision_note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                FOREIGN KEY (recommendation_id) REFERENCES automation_recommendations(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ai_agent_prompts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                agent_type TEXT NOT NULL,
                prompt_text TEXT NOT NULL,
                parsed_intent_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'received',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ai_agent_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt_id INTEGER NOT NULL,
                scenario_id INTEGER NOT NULL,
                agent_type TEXT NOT NULL,
                action_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending_approval',
                guard_status TEXT NOT NULL DEFAULT 'passed',
                proposal_json TEXT NOT NULL DEFAULT '{}',
                result_json TEXT NOT NULL DEFAULT '{}',
                approved_by TEXT DEFAULT NULL,
                approved_at TEXT DEFAULT NULL,
                posted_at TEXT DEFAULT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (prompt_id) REFERENCES ai_agent_prompts(id) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS university_agent_clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_key TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                shared_secret_hash TEXT NOT NULL,
                scopes_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'active',
                callback_url TEXT NOT NULL DEFAULT '',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS university_agent_tools (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tool_key TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                required_scope TEXT NOT NULL,
                action_type TEXT NOT NULL,
                approval_required INTEGER NOT NULL DEFAULT 1,
                enabled INTEGER NOT NULL DEFAULT 1,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS university_agent_policies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                policy_key TEXT NOT NULL UNIQUE,
                client_key TEXT NOT NULL,
                tool_key TEXT NOT NULL,
                allowed_actions_json TEXT NOT NULL DEFAULT '[]',
                max_amount REAL DEFAULT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (client_key) REFERENCES university_agent_clients(client_key) ON DELETE CASCADE,
                FOREIGN KEY (tool_key) REFERENCES university_agent_tools(tool_key) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS university_agent_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_key TEXT NOT NULL UNIQUE,
                client_key TEXT NOT NULL,
                tool_key TEXT NOT NULL,
                scenario_id INTEGER DEFAULT NULL,
                signature_status TEXT NOT NULL,
                policy_status TEXT NOT NULL,
                approval_status TEXT NOT NULL DEFAULT 'not_required',
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                result_json TEXT NOT NULL DEFAULT '{}',
                callback_url TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                completed_at TEXT DEFAULT NULL,
                FOREIGN KEY (client_key) REFERENCES university_agent_clients(client_key) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS university_agent_callbacks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL,
                callback_url TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                payload_json TEXT NOT NULL DEFAULT '{}',
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                delivered_at TEXT DEFAULT NULL,
                FOREIGN KEY (request_id) REFERENCES university_agent_requests(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS university_agent_audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER DEFAULT NULL,
                client_key TEXT NOT NULL,
                event_type TEXT NOT NULL,
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                FOREIGN KEY (request_id) REFERENCES university_agent_requests(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS operational_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_key TEXT NOT NULL,
                category TEXT NOT NULL,
                status TEXT NOT NULL,
                detail_json TEXT NOT NULL DEFAULT '{}',
                checked_by TEXT NOT NULL,
                checked_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS restore_test_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backup_key TEXT NOT NULL,
                status TEXT NOT NULL,
                source_size_bytes INTEGER NOT NULL,
                validation_json TEXT NOT NULL DEFAULT '{}',
                tested_by TEXT NOT NULL,
                tested_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runbook_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                runbook_key TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                category TEXT NOT NULL,
                path TEXT NOT NULL,
                status TEXT NOT NULL,
                updated_by TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS deployment_environment_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                environment_key TEXT NOT NULL UNIQUE,
                tenant_key TEXT NOT NULL DEFAULT 'campus',
                base_url TEXT NOT NULL DEFAULT '',
                database_backend TEXT NOT NULL DEFAULT 'sqlite',
                sso_required INTEGER NOT NULL DEFAULT 0,
                domain_guard_required INTEGER NOT NULL DEFAULT 0,
                settings_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'draft',
                updated_by TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS deployment_promotions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                promotion_key TEXT NOT NULL UNIQUE,
                from_environment TEXT NOT NULL,
                to_environment TEXT NOT NULL,
                release_version TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'planned',
                checklist_json TEXT NOT NULL DEFAULT '{}',
                promoted_by TEXT NOT NULL,
                promoted_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS deployment_config_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_key TEXT NOT NULL UNIQUE,
                environment_key TEXT NOT NULL,
                direction TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'ready',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS migration_rollback_plans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plan_key TEXT NOT NULL UNIQUE,
                migration_key TEXT NOT NULL,
                rollback_strategy TEXT NOT NULL,
                verification_steps_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'draft',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS deployment_release_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                release_key TEXT NOT NULL UNIQUE,
                release_version TEXT NOT NULL,
                title TEXT NOT NULL,
                notes_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'draft',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                published_by TEXT DEFAULT NULL,
                published_at TEXT DEFAULT NULL
            );

            CREATE TABLE IF NOT EXISTS admin_diagnostic_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                diagnostic_key TEXT NOT NULL UNIQUE,
                scope TEXT NOT NULL,
                status TEXT NOT NULL,
                result_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS operational_readiness_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_key TEXT NOT NULL UNIQUE,
                category TEXT NOT NULL,
                title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                evidence_json TEXT NOT NULL DEFAULT '{}',
                updated_by TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS application_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                correlation_id TEXT NOT NULL DEFAULT '',
                actor TEXT NOT NULL DEFAULT 'system',
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS observability_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_key TEXT NOT NULL,
                metric_type TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT NOT NULL DEFAULT 'count',
                labels_json TEXT NOT NULL DEFAULT '{}',
                trace_id TEXT NOT NULL DEFAULT '',
                recorded_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS health_probe_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                probe_key TEXT NOT NULL,
                status TEXT NOT NULL,
                latency_ms INTEGER NOT NULL DEFAULT 0,
                detail_json TEXT NOT NULL DEFAULT '{}',
                trace_id TEXT NOT NULL DEFAULT '',
                checked_by TEXT NOT NULL DEFAULT 'system',
                checked_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS alert_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_key TEXT NOT NULL,
                severity TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                message TEXT NOT NULL,
                source TEXT NOT NULL,
                trace_id TEXT NOT NULL DEFAULT '',
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                acknowledged_by TEXT DEFAULT NULL,
                acknowledged_at TEXT DEFAULT NULL
            );

            CREATE TABLE IF NOT EXISTS backup_restore_drill_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                drill_key TEXT NOT NULL UNIQUE,
                backup_key TEXT NOT NULL,
                status TEXT NOT NULL,
                backup_size_bytes INTEGER NOT NULL DEFAULT 0,
                validation_json TEXT NOT NULL DEFAULT '{}',
                trace_id TEXT NOT NULL DEFAULT '',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS performance_load_tests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                test_key TEXT NOT NULL UNIQUE,
                backend TEXT NOT NULL,
                scenario_id INTEGER DEFAULT NULL,
                test_type TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                elapsed_ms INTEGER NOT NULL,
                throughput_per_second REAL NOT NULL,
                status TEXT NOT NULL,
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS index_strategy_recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recommendation_key TEXT NOT NULL UNIQUE,
                table_name TEXT NOT NULL,
                index_name TEXT NOT NULL,
                columns_json TEXT NOT NULL DEFAULT '[]',
                reason TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'recommended',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS background_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_key TEXT NOT NULL UNIQUE,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                priority INTEGER NOT NULL DEFAULT 100,
                payload_json TEXT NOT NULL DEFAULT '{}',
                result_json TEXT NOT NULL DEFAULT '{}',
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                backoff_seconds INTEGER NOT NULL DEFAULT 60,
                scheduled_for TEXT DEFAULT NULL,
                cancelled_at TEXT DEFAULT NULL,
                dead_lettered_at TEXT DEFAULT NULL,
                worker_id TEXT DEFAULT NULL,
                queued_at TEXT NOT NULL,
                started_at TEXT DEFAULT NULL,
                completed_at TEXT DEFAULT NULL,
                created_by TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS background_job_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                message TEXT NOT NULL,
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                FOREIGN KEY (job_id) REFERENCES background_jobs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS background_dead_letters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                job_key TEXT NOT NULL,
                reason TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                result_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                FOREIGN KEY (job_id) REFERENCES background_jobs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS cache_invalidation_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cache_key TEXT NOT NULL,
                scope TEXT NOT NULL,
                reason TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'invalidated',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS restore_automation_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                backup_key TEXT NOT NULL,
                status TEXT NOT NULL,
                verify_only INTEGER NOT NULL DEFAULT 1,
                result_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS performance_benchmark_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER DEFAULT NULL,
                dataset_key TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                backend TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                thresholds_json TEXT NOT NULL DEFAULT '{}',
                results_json TEXT NOT NULL DEFAULT '{}',
                query_plans_json TEXT NOT NULL DEFAULT '{}',
                indexes_json TEXT NOT NULL DEFAULT '[]',
                regression_failures_json TEXT NOT NULL DEFAULT '[]',
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT DEFAULT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS performance_benchmark_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                metric_key TEXT NOT NULL,
                metric_type TEXT NOT NULL,
                elapsed_ms INTEGER NOT NULL,
                row_count INTEGER NOT NULL DEFAULT 0,
                threshold_ms INTEGER DEFAULT NULL,
                status TEXT NOT NULL,
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES performance_benchmark_runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS parallel_cubed_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER DEFAULT NULL,
                work_type TEXT NOT NULL,
                partition_strategy TEXT NOT NULL,
                executor_kind TEXT NOT NULL,
                logical_cores INTEGER NOT NULL,
                requested_workers INTEGER NOT NULL,
                worker_count INTEGER NOT NULL,
                partition_count INTEGER NOT NULL,
                row_count INTEGER NOT NULL DEFAULT 0,
                elapsed_ms INTEGER NOT NULL DEFAULT 0,
                throughput_per_second REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'running',
                reduce_status TEXT NOT NULL DEFAULT 'pending',
                result_json TEXT NOT NULL DEFAULT '{}',
                benchmark_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT DEFAULT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS parallel_cubed_partitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                partition_key TEXT NOT NULL,
                work_type TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                input_count INTEGER NOT NULL DEFAULT 0,
                output_count INTEGER NOT NULL DEFAULT 0,
                elapsed_ms INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                result_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES parallel_cubed_runs(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_parallel_cubed_runs_scenario
            ON parallel_cubed_runs (scenario_id, started_at);

            CREATE INDEX IF NOT EXISTS idx_parallel_cubed_partitions_run
            ON parallel_cubed_partitions (run_id, work_type);

            CREATE TABLE IF NOT EXISTS office_workbooks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                workbook_key TEXT NOT NULL UNIQUE,
                workbook_type TEXT NOT NULL,
                file_name TEXT NOT NULL,
                storage_path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'ready',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS office_roundtrip_imports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                workbook_key TEXT NOT NULL,
                accepted_rows INTEGER NOT NULL DEFAULT 0,
                rejected_rows INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                messages_json TEXT NOT NULL DEFAULT '[]',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS office_named_ranges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                workbook_key TEXT NOT NULL,
                range_name TEXT NOT NULL,
                sheet_name TEXT NOT NULL,
                cell_ref TEXT NOT NULL,
                purpose TEXT NOT NULL DEFAULT '',
                protected INTEGER NOT NULL DEFAULT 0,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                UNIQUE(workbook_key, range_name)
            );

            CREATE TABLE IF NOT EXISTS office_cell_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                workbook_key TEXT NOT NULL,
                sheet_name TEXT NOT NULL,
                cell_ref TEXT NOT NULL,
                comment_text TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS office_workspace_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                workbook_key TEXT NOT NULL,
                action_type TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT NOT NULL,
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS journal_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                entity_code TEXT NOT NULL DEFAULT 'CAMPUS',
                department_code TEXT NOT NULL,
                fund_code TEXT NOT NULL,
                account_code TEXT NOT NULL,
                amount REAL NOT NULL,
                ledger_basis TEXT NOT NULL,
                reason TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft',
                ledger_entry_id INTEGER DEFAULT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                submitted_by TEXT DEFAULT NULL,
                submitted_at TEXT DEFAULT NULL,
                approved_by TEXT DEFAULT NULL,
                approved_at TEXT DEFAULT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                FOREIGN KEY (ledger_entry_id) REFERENCES planning_ledger(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS entity_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                comment_text TEXT NOT NULL,
                visibility TEXT NOT NULL DEFAULT 'internal',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                resolved_at TEXT DEFAULT NULL
            );

            CREATE TABLE IF NOT EXISTS evidence_attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                file_name TEXT NOT NULL,
                storage_path TEXT NOT NULL,
                content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
                size_bytes INTEGER NOT NULL DEFAULT 0,
                retention_until TEXT DEFAULT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS forecast_actual_variances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                department_code TEXT NOT NULL,
                account_code TEXT NOT NULL,
                forecast_amount REAL NOT NULL,
                actual_amount REAL NOT NULL,
                variance_amount REAL NOT NULL,
                variance_percent REAL DEFAULT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS planning_models (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                model_key TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'draft',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                UNIQUE(scenario_id, model_key)
            );

            CREATE TABLE IF NOT EXISTS model_formulas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id INTEGER NOT NULL,
                formula_key TEXT NOT NULL,
                label TEXT NOT NULL,
                expression TEXT NOT NULL,
                target_account_code TEXT NOT NULL,
                target_department_code TEXT DEFAULT NULL,
                target_fund_code TEXT NOT NULL DEFAULT 'GEN',
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (model_id) REFERENCES planning_models(id) ON DELETE CASCADE,
                UNIQUE(model_id, formula_key)
            );

            CREATE TABLE IF NOT EXISTS allocation_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id INTEGER NOT NULL,
                rule_key TEXT NOT NULL,
                label TEXT NOT NULL,
                source_account_code TEXT NOT NULL,
                source_department_code TEXT DEFAULT NULL,
                target_account_code TEXT NOT NULL,
                target_fund_code TEXT NOT NULL DEFAULT 'GEN',
                basis_account_code TEXT DEFAULT NULL,
                basis_driver_key TEXT DEFAULT NULL,
                target_department_codes TEXT NOT NULL,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (model_id) REFERENCES planning_models(id) ON DELETE CASCADE,
                UNIQUE(model_id, rule_key)
            );

            CREATE TABLE IF NOT EXISTS model_recalculation_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id INTEGER NOT NULL,
                scenario_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                formula_count INTEGER NOT NULL DEFAULT 0,
                allocation_count INTEGER NOT NULL DEFAULT 0,
                ledger_entry_count INTEGER NOT NULL DEFAULT 0,
                dependency_graph_json TEXT NOT NULL DEFAULT '{}',
                messages_json TEXT NOT NULL DEFAULT '[]',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT DEFAULT NULL,
                FOREIGN KEY (model_id) REFERENCES planning_models(id) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS enterprise_cube_dimensions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id INTEGER NOT NULL,
                dimension_key TEXT NOT NULL,
                role TEXT NOT NULL,
                density TEXT NOT NULL,
                member_count INTEGER NOT NULL DEFAULT 0,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                FOREIGN KEY (model_id) REFERENCES planning_models(id) ON DELETE CASCADE,
                UNIQUE(model_id, dimension_key)
            );

            CREATE TABLE IF NOT EXISTS enterprise_cube_cells (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id INTEGER NOT NULL,
                scenario_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                account_code TEXT NOT NULL,
                department_code TEXT NOT NULL,
                fund_code TEXT NOT NULL,
                amount REAL NOT NULL,
                sparsity_signature TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (model_id) REFERENCES planning_models(id) ON DELETE CASCADE,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS model_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id INTEGER NOT NULL,
                version_key TEXT NOT NULL,
                status TEXT NOT NULL,
                dependency_graph_json TEXT NOT NULL DEFAULT '{}',
                calculation_order_json TEXT NOT NULL DEFAULT '[]',
                dimension_strategy_json TEXT NOT NULL DEFAULT '{}',
                published_by TEXT NOT NULL,
                published_at TEXT NOT NULL,
                FOREIGN KEY (model_id) REFERENCES planning_models(id) ON DELETE CASCADE,
                UNIQUE(model_id, version_key)
            );

            CREATE TABLE IF NOT EXISTS model_dependency_invalidations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id INTEGER NOT NULL,
                formula_key TEXT NOT NULL,
                reason TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'invalidated',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (model_id) REFERENCES planning_models(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS model_performance_tests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id INTEGER NOT NULL,
                test_key TEXT NOT NULL UNIQUE,
                cube_cell_count INTEGER NOT NULL,
                formula_count INTEGER NOT NULL,
                estimated_dense_cells INTEGER NOT NULL,
                elapsed_ms INTEGER NOT NULL,
                status TEXT NOT NULL,
                messages_json TEXT NOT NULL DEFAULT '[]',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (model_id) REFERENCES planning_models(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS board_packages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                package_name TEXT NOT NULL,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'assembled',
                contents_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS report_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                snapshot_key TEXT NOT NULL UNIQUE,
                snapshot_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                retention_until TEXT DEFAULT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS export_artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                artifact_key TEXT NOT NULL UNIQUE,
                artifact_type TEXT NOT NULL,
                package_id INTEGER DEFAULT NULL,
                report_definition_id INTEGER DEFAULT NULL,
                file_name TEXT NOT NULL,
                content_type TEXT NOT NULL,
                storage_path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'ready',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                FOREIGN KEY (package_id) REFERENCES board_packages(id) ON DELETE SET NULL,
                FOREIGN KEY (report_definition_id) REFERENCES report_definitions(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS export_artifact_validations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                artifact_id INTEGER NOT NULL,
                validation_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL DEFAULT '{}',
                issues_json TEXT NOT NULL DEFAULT '[]',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (artifact_id) REFERENCES export_artifacts(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_export_artifact_validations_artifact
                ON export_artifact_validations(artifact_id, created_at);

            CREATE TABLE IF NOT EXISTS scheduled_extract_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                export_id INTEGER DEFAULT NULL,
                extract_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                destination TEXT NOT NULL,
                status TEXT NOT NULL,
                row_count INTEGER NOT NULL DEFAULT 0,
                artifact_id INTEGER DEFAULT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (export_id) REFERENCES report_exports(id) ON DELETE SET NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                FOREIGN KEY (artifact_id) REFERENCES export_artifacts(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS variance_thresholds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                threshold_key TEXT NOT NULL,
                amount_threshold REAL NOT NULL,
                percent_threshold REAL DEFAULT NULL,
                require_explanation INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (scenario_id, threshold_key),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS variance_explanations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                variance_key TEXT NOT NULL,
                department_code TEXT NOT NULL,
                account_code TEXT NOT NULL,
                variance_type TEXT NOT NULL,
                variance_amount REAL NOT NULL,
                threshold_amount REAL NOT NULL,
                explanation_text TEXT NOT NULL DEFAULT '',
                ai_draft_text TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'required',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                submitted_by TEXT DEFAULT NULL,
                submitted_at TEXT DEFAULT NULL,
                approved_by TEXT DEFAULT NULL,
                approved_at TEXT DEFAULT NULL,
                rejection_note TEXT NOT NULL DEFAULT '',
                UNIQUE (scenario_id, variance_key),
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS narrative_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                package_id INTEGER DEFAULT NULL,
                narrative_key TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft',
                narrative_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                approved_by TEXT DEFAULT NULL,
                approved_at TEXT DEFAULT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE,
                FOREIGN KEY (package_id) REFERENCES board_packages(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS ai_explanations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                explanation_key TEXT NOT NULL UNIQUE,
                subject_type TEXT NOT NULL,
                subject_key TEXT NOT NULL,
                explanation_text TEXT NOT NULL,
                confidence REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft',
                model_name TEXT NOT NULL DEFAULT 'local-explainability',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                submitted_by TEXT DEFAULT NULL,
                submitted_at TEXT DEFAULT NULL,
                approved_by TEXT DEFAULT NULL,
                approved_at TEXT DEFAULT NULL,
                rejection_note TEXT NOT NULL DEFAULT '',
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ai_explanation_citations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                explanation_id INTEGER NOT NULL,
                citation_key TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                source_label TEXT NOT NULL,
                source_excerpt TEXT NOT NULL DEFAULT '',
                confidence REAL NOT NULL DEFAULT 1.0,
                created_at TEXT NOT NULL,
                FOREIGN KEY (explanation_id) REFERENCES ai_explanations(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ai_source_traces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                explanation_id INTEGER NOT NULL,
                trace_order INTEGER NOT NULL,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                transformation TEXT NOT NULL,
                value_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                FOREIGN KEY (explanation_id) REFERENCES ai_explanations(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS market_quote_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                change_amount REAL NOT NULL,
                change_percent REAL NOT NULL,
                provider TEXT NOT NULL,
                provider_delay_minutes INTEGER NOT NULL DEFAULT 15,
                as_of TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS market_watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(user_id, symbol)
            );

            CREATE TABLE IF NOT EXISTS paper_trading_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                account_key TEXT NOT NULL,
                cash_balance REAL NOT NULL,
                starting_cash REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(user_id, account_key)
            );

            CREATE TABLE IF NOT EXISTS paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity REAL NOT NULL,
                price REAL NOT NULL,
                notional REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'filled',
                created_at TEXT NOT NULL,
                FOREIGN KEY (account_id) REFERENCES paper_trading_accounts(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS brokerage_connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                provider_key TEXT NOT NULL,
                connection_name TEXT NOT NULL,
                credential_ref TEXT DEFAULT NULL,
                credential_type TEXT NOT NULL DEFAULT '',
                mode TEXT NOT NULL DEFAULT 'sandbox',
                provider_environment TEXT NOT NULL DEFAULT 'sandbox',
                auth_flow_status TEXT NOT NULL DEFAULT 'not_started',
                auth_url TEXT NOT NULL DEFAULT '',
                consent_status TEXT NOT NULL DEFAULT 'not_requested',
                read_only_ack INTEGER NOT NULL DEFAULT 0,
                sync_warning TEXT NOT NULL DEFAULT '',
                trading_enabled INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'needs_credentials',
                last_test_at TEXT DEFAULT NULL,
                last_sync_at TEXT DEFAULT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS brokerage_consent_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                consent_version TEXT NOT NULL,
                read_only_ack INTEGER NOT NULL DEFAULT 0,
                real_money_trading_ack INTEGER NOT NULL DEFAULT 0,
                data_scope_ack INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                consent_text TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (connection_id) REFERENCES brokerage_connections(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS brokerage_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_id INTEGER NOT NULL,
                external_account_id TEXT NOT NULL,
                account_name TEXT NOT NULL,
                account_type TEXT NOT NULL,
                currency TEXT NOT NULL DEFAULT 'USD',
                cash_balance REAL NOT NULL DEFAULT 0,
                buying_power REAL NOT NULL DEFAULT 0,
                synced_at TEXT NOT NULL,
                FOREIGN KEY (connection_id) REFERENCES brokerage_connections(id) ON DELETE CASCADE,
                UNIQUE(connection_id, external_account_id)
            );

            CREATE TABLE IF NOT EXISTS brokerage_holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brokerage_account_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                quantity REAL NOT NULL,
                average_cost REAL NOT NULL,
                market_value REAL NOT NULL,
                unrealized_pnl REAL NOT NULL,
                synced_at TEXT NOT NULL,
                FOREIGN KEY (brokerage_account_id) REFERENCES brokerage_accounts(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS brokerage_sync_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_id INTEGER NOT NULL,
                run_type TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT NOT NULL,
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (connection_id) REFERENCES brokerage_connections(id) ON DELETE CASCADE
            );
            '''
        )
        _ensure_column(conn, 'planning_ledger', 'ledger_basis', "TEXT NOT NULL DEFAULT 'budget'")
        _ensure_column(conn, 'planning_ledger', 'source_version', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'planning_ledger', 'source_record_id', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'planning_ledger', 'parent_ledger_entry_id', 'INTEGER DEFAULT NULL')
        _ensure_column(conn, 'planning_ledger', 'idempotency_key', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'planning_ledger', 'posted_checksum', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'planning_ledger', 'immutable_posting', 'INTEGER NOT NULL DEFAULT 1')
        _ensure_column(conn, 'connector_adapters', 'contract_json', "TEXT NOT NULL DEFAULT '{}'")
        _ensure_column(conn, 'connector_adapters', 'credential_schema_json', "TEXT NOT NULL DEFAULT '{}'")
        _ensure_column(conn, 'connector_adapters', 'max_stream_rows', 'INTEGER NOT NULL DEFAULT 100000')
        _ensure_column(conn, 'connector_auth_flows', 'oauth_state', 'TEXT NOT NULL DEFAULT ""')
        _ensure_column(conn, 'brokerage_connections', 'credential_type', "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, 'brokerage_connections', 'provider_environment', "TEXT NOT NULL DEFAULT 'sandbox'")
        _ensure_column(conn, 'brokerage_connections', 'auth_flow_status', "TEXT NOT NULL DEFAULT 'not_started'")
        _ensure_column(conn, 'brokerage_connections', 'auth_url', "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, 'brokerage_connections', 'consent_status', "TEXT NOT NULL DEFAULT 'not_requested'")
        _ensure_column(conn, 'brokerage_connections', 'read_only_ack', 'INTEGER NOT NULL DEFAULT 0')
        _ensure_column(conn, 'brokerage_connections', 'sync_warning', "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, 'credential_vault', 'secret_type', "TEXT NOT NULL DEFAULT 'api_key'")
        _ensure_column(conn, 'credential_vault', 'expires_at', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'credential_vault', 'rotated_at', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'import_mapping_templates', 'version', 'INTEGER NOT NULL DEFAULT 1')
        _ensure_column(conn, 'import_mapping_templates', 'previous_template_key', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'import_batches', 'source_name', "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, 'import_batches', 'stream_chunks', 'INTEGER NOT NULL DEFAULT 1')
        _ensure_column(conn, 'import_batches', 'mapping_template_key', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'import_batches', 'mapping_version', 'INTEGER DEFAULT NULL')
        _ensure_column(conn, 'import_batches', 'contract_validated', 'INTEGER NOT NULL DEFAULT 0')
        _ensure_column(conn, 'connector_source_drillbacks', 'validation_status', "TEXT NOT NULL DEFAULT 'pending'")
        _ensure_column(conn, 'connector_source_drillbacks', 'validation_json', "TEXT NOT NULL DEFAULT '{}'")
        conn.execute(
            '''
            CREATE UNIQUE INDEX IF NOT EXISTS idx_planning_ledger_idempotency
            ON planning_ledger (idempotency_key)
            WHERE idempotency_key IS NOT NULL AND idempotency_key <> ''
            '''
        )
        conn.executescript(
            '''
            CREATE INDEX IF NOT EXISTS idx_planning_ledger_scenario_period_account
            ON planning_ledger (scenario_id, period, account_code, reversed_at);

            CREATE INDEX IF NOT EXISTS idx_planning_ledger_import_batch
            ON planning_ledger (import_batch_id);

            CREATE INDEX IF NOT EXISTS idx_import_batches_scenario_connector
            ON import_batches (scenario_id, connector_key, created_at);

            CREATE INDEX IF NOT EXISTS idx_connector_sync_logs_connector_created
            ON connector_sync_logs (connector_key, created_at);

            CREATE INDEX IF NOT EXISTS idx_chat_messages_recipient_unread
            ON chat_messages (recipient_user_id, read_at, id);

            CREATE INDEX IF NOT EXISTS idx_chat_messages_thread
            ON chat_messages (sender_user_id, recipient_user_id, id);

            CREATE INDEX IF NOT EXISTS idx_audit_logs_entity_created
            ON audit_logs (entity_type, entity_id, created_at);

            CREATE INDEX IF NOT EXISTS idx_performance_benchmark_metrics_run
            ON performance_benchmark_metrics (run_id, metric_key);

            CREATE INDEX IF NOT EXISTS idx_observability_metrics_key_time
            ON observability_metrics (metric_key, recorded_at);

            CREATE INDEX IF NOT EXISTS idx_health_probe_runs_key_time
            ON health_probe_runs (probe_key, checked_at);

            CREATE INDEX IF NOT EXISTS idx_alert_events_status_severity
            ON alert_events (status, severity, created_at);

            CREATE INDEX IF NOT EXISTS idx_tax_classifications_scenario_status
            ON tax_activity_classifications (scenario_id, tax_status, review_status);

            CREATE INDEX IF NOT EXISTS idx_tax_sources_next_check
            ON tax_rule_sources (status, next_check_at);

            CREATE INDEX IF NOT EXISTS idx_tax_alerts_status
            ON tax_change_alerts (status, severity, created_at);
            '''
        )
        _ensure_column(conn, 'users', 'must_change_password', 'INTEGER NOT NULL DEFAULT 0')
        _ensure_column(conn, 'users', 'password_changed_at', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'users', 'last_login_at', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'background_jobs', 'max_attempts', 'INTEGER NOT NULL DEFAULT 3')
        _ensure_column(conn, 'background_jobs', 'backoff_seconds', 'INTEGER NOT NULL DEFAULT 60')
        _ensure_column(conn, 'background_jobs', 'scheduled_for', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'background_jobs', 'cancelled_at', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'background_jobs', 'dead_lettered_at', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'background_jobs', 'worker_id', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'close_checklists', 'template_key', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'close_checklists', 'dependency_status', "TEXT NOT NULL DEFAULT 'clear'")
        _ensure_column(conn, 'account_reconciliations', 'preparer', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'account_reconciliations', 'prepared_at', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'account_reconciliations', 'reviewer', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'account_reconciliations', 'reviewed_at', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'account_reconciliations', 'review_note', "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, 'account_reconciliations', 'aging_days', 'INTEGER NOT NULL DEFAULT 0')
        _ensure_column(conn, 'elimination_entries', 'review_status', "TEXT NOT NULL DEFAULT 'draft'")
        _ensure_column(conn, 'elimination_entries', 'reviewed_by', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'elimination_entries', 'reviewed_at', 'TEXT DEFAULT NULL')
        _ensure_column(conn, 'elimination_entries', 'review_note', "TEXT NOT NULL DEFAULT ''")
        conn.executescript(
            '''
            INSERT OR IGNORE INTO dimension_members (dimension_kind, code, name, active)
            SELECT kind, code, name, active
            FROM dimensions;

            INSERT OR IGNORE INTO planning_ledger (
                scenario_id, entity_code, department_code, fund_code, account_code,
                period, amount, source, driver_key, notes, ledger_type,
                legacy_line_item_id, posted_by, posted_at, metadata_json
            )
            SELECT
                scenario_id,
                'CAMPUS',
                department_code,
                fund_code,
                account_code,
                period,
                amount,
                source,
                driver_key,
                COALESCE(notes, ''),
                CASE WHEN source = 'forecast' THEN 'forecast' ELSE 'planning' END,
                id,
                CASE WHEN source = 'forecast' THEN 'planner.bot' ELSE 'legacy.migration' END,
                datetime('now'),
                '{}'
            FROM plan_line_items;
            '''
        )

        conn.execute(
            '''
            UPDATE planning_ledger
            SET ledger_basis = CASE
                WHEN ledger_type = 'actual' THEN 'actual'
                WHEN ledger_type = 'forecast' THEN 'forecast'
                WHEN ledger_type = 'scenario' THEN 'scenario'
                ELSE 'budget'
            END
            WHERE ledger_basis IS NULL OR ledger_basis = ''
            '''
        )


def _ensure_column(conn: Any, table: str, column: str, definition: str) -> None:
    if DB_BACKEND == 'postgres':
        existing = {
            row['column_name']
            for row in conn.execute(
                '''
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ''',
                (table,),
            ).fetchall()
        }
        if column not in existing:
            conn.execute(f'ALTER TABLE {table} ADD COLUMN {column} {translate_sql(definition, ddl=True)}')
        return
    existing = {row['name'] for row in conn.execute(f'PRAGMA table_info({table})').fetchall()}
    if column not in existing:
        conn.execute(f'ALTER TABLE {table} ADD COLUMN {column} {definition}')


def fetch_all(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with get_connection() as conn:
        return list(conn.execute(query, params).fetchall())


def fetch_one(query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with get_connection() as conn:
        return conn.execute(query, params).fetchone()


def execute(query: str, params: tuple[Any, ...] = ()) -> int:
    with get_connection() as conn:
        cur = conn.execute(query, params)
        return int(cur.lastrowid)


def executemany(query: str, rows: list[tuple[Any, ...]]) -> None:
    with get_connection() as conn:
        conn.executemany(query, rows)


def log_audit(entity_type: str, entity_id: str, action: str, actor: str, detail: dict[str, Any], created_at: str, conn: Any | None = None) -> None:
    detail_json = json.dumps(detail, sort_keys=True)
    if conn is None:
        with transaction(immediate=True) as tx:
            _log_audit_with_connection(tx, entity_type, entity_id, action, actor, detail_json, created_at)
        return
    _log_audit_with_connection(conn, entity_type, entity_id, action, actor, detail_json, created_at)


def _log_audit_with_connection(conn: Any, entity_type: str, entity_id: str, action: str, actor: str, detail_json: str, created_at: str) -> None:
    audit_log_id = int(conn.execute(
        '''
        INSERT INTO audit_logs (entity_type, entity_id, action, actor, detail_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (entity_type, entity_id, action, actor, detail_json, created_at),
    ).lastrowid)
    seal_audit_log(audit_log_id, conn=conn)


def seal_audit_log(audit_log_id: int, conn: Any | None = None) -> str:
    if conn is None:
        with transaction(immediate=True) as tx:
            return _seal_audit_log_with_connection(audit_log_id, tx)
    return _seal_audit_log_with_connection(audit_log_id, conn)


def _seal_audit_log_with_connection(audit_log_id: int, conn: Any) -> str:
    existing = conn.execute('SELECT row_hash FROM audit_log_hashes WHERE audit_log_id = ?', (audit_log_id,)).fetchone()
    if existing is not None:
        return str(existing['row_hash'])
    row = conn.execute('SELECT * FROM audit_logs WHERE id = ?', (audit_log_id,)).fetchone()
    if row is None:
        raise ValueError('Audit log not found.')
    previous = conn.execute('SELECT row_hash FROM audit_log_hashes ORDER BY audit_log_id DESC LIMIT 1').fetchone()
    previous_hash = str(previous['row_hash']) if previous else 'GENESIS'
    row_hash = audit_row_hash(row, previous_hash)
    conn.execute(
        '''
        INSERT OR IGNORE INTO audit_log_hashes (audit_log_id, previous_hash, row_hash, sealed_at)
        VALUES (?, ?, ?, datetime('now'))
        ''',
        (audit_log_id, previous_hash, row_hash),
    )
    return row_hash


def audit_row_hash(row: dict[str, Any], previous_hash: str) -> str:
    payload = {
        'id': row['id'],
        'entity_type': row['entity_type'],
        'entity_id': row['entity_id'],
        'action': row['action'],
        'actor': row['actor'],
        'detail_json': row['detail_json'],
        'created_at': row['created_at'],
        'previous_hash': previous_hash,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')).hexdigest()


def log_application(log_type: str, severity: str, message: str, actor: str = 'system', detail: dict[str, Any] | None = None, correlation_id: str = '') -> None:
    execute(
        '''
        INSERT INTO application_logs (log_type, severity, message, correlation_id, actor, detail_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ''',
        (log_type, severity, message, correlation_id, actor, json.dumps(detail or {}, sort_keys=True)),
    )
