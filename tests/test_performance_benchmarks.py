from services.base import ServiceContext
from services.performance_benchmarks import (
    BenchmarkDefinition,
    BenchmarkStatus,
    PerformanceBenchmarkService,
)


class FakeCursor:
    def __init__(self, rows=None, columns=None):
        self._rows = rows or []
        self.description = [(column,) for column in (columns or [])]

    def fetchall(self):
        return self._rows


class FakeDb:
    def __init__(self):
        self.statements = []

    def execute(self, sql, parameters=()):
        self.statements.append((sql, parameters))
        return FakeCursor([], [])

    def executemany(self, sql, parameters):
        self.statements.append((sql, list(parameters)))

    def commit(self):
        pass

    def rollback(self):
        pass


def test_seed_plan_estimates_rows():
    service = PerformanceBenchmarkService(FakeDb())

    plan = service.build_seed_plan({"departments": 2, "accounts": 3, "periods": 4, "scenarios": 5})

    assert plan.estimated_ledger_rows == 120


def test_result_status_pass_warning_fail():
    service = PerformanceBenchmarkService(FakeDb())
    definition = BenchmarkDefinition("q", "ledger", "select 1", 100)

    assert service.evaluate_result(definition, 90).status is BenchmarkStatus.PASSED
    assert service.evaluate_result(definition, 120).status is BenchmarkStatus.WARNING
    assert service.evaluate_result(definition, 200).status is BenchmarkStatus.FAILED


def test_overall_status_uses_worst_result():
    service = PerformanceBenchmarkService(FakeDb())
    definition = BenchmarkDefinition("q", "ledger", "select 1", 100)

    results = [
        service.evaluate_result(definition, 90),
        service.evaluate_result(definition, 200),
    ]

    assert service.overall_status(results) is BenchmarkStatus.FAILED


def test_record_run_writes_benchmark_manifest():
    db = FakeDb()
    service = PerformanceBenchmarkService(db)
    context = ServiceContext(user_id="admin", roles=("admin",))
    plan = service.build_seed_plan({"departments": 1, "accounts": 1, "periods": 1, "scenarios": 1})
    result = service.evaluate_result(BenchmarkDefinition("q", "ledger", "select 1", 100), 50)

    service.record_run(context, "run-1", plan, [result])

    assert any("insert into performance_benchmark_runs" in " ".join(sql.lower().split()) for sql, _ in db.statements)


def test_index_recommendations_include_ledger_index():
    service = PerformanceBenchmarkService(FakeDb())

    recommendations = service.index_recommendations()

    assert any(item["table"] == "ledger_lines" for item in recommendations)

