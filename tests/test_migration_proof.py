from migration_proof.runner import MigrationPlan, PostgreSqlMigrationRunner


class FakeCursor:
    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class FakeConnection:
    def __init__(self):
        self.applied = set()
        self.statements = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, sql, parameters=()):
        self.statements.append((sql, parameters))
        normalized = " ".join(sql.lower().split())
        if "from information_schema.columns" in normalized:
            return FakeCursor([("migration_key",)])
        if normalized.startswith("select migration_id from schema_migrations") or normalized.startswith("select migration_key from schema_migrations"):
            return FakeCursor([(migration_id,) for migration_id in sorted(self.applied)])
        if normalized.startswith("insert into schema_migrations"):
            self.applied.add(parameters[0])
        if normalized.startswith("delete from schema_migrations"):
            self.applied.discard(parameters[0])
        return FakeCursor()

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_dry_run_lists_pending_migrations_without_applying():
    conn = FakeConnection()
    runner = PostgreSqlMigrationRunner(conn)
    plans = [MigrationPlan("001", "CREATE TABLE example(id int)")]

    results = runner.apply(plans, dry_run=True)

    assert results[0].migration_id == "001"
    assert results[0].status == "pending"
    assert results[0].dry_run is True
    assert "001" not in conn.applied


def test_apply_records_migration_and_uses_lock():
    conn = FakeConnection()
    runner = PostgreSqlMigrationRunner(conn)
    plans = [MigrationPlan("001", "CREATE TABLE example(id int)")]

    results = runner.apply(plans)

    assert results[0].status == "applied"
    assert "001" in conn.applied
    assert any("pg_advisory_lock" in sql for sql, _ in conn.statements)
    assert any("pg_advisory_unlock" in sql for sql, _ in conn.statements)


def test_rollback_removes_migration_when_down_sql_exists():
    conn = FakeConnection()
    conn.applied.add("001")
    runner = PostgreSqlMigrationRunner(conn)

    result = runner.rollback(MigrationPlan("001", "CREATE TABLE example(id int)", "DROP TABLE example"))

    assert result.status == "rolled_back"
    assert "001" not in conn.applied
