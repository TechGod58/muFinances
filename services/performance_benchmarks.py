from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Sequence

from .base import DatabaseConnection, ServiceContext, ValidationError, fetch_all


class BenchmarkStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"


@dataclass(frozen=True)
class SeedPlan:
    departments: int = 100
    accounts: int = 500
    periods: int = 36
    scenarios: int = 6
    rows_per_combination: int = 1

    @property
    def estimated_ledger_rows(self) -> int:
        return self.departments * self.accounts * self.periods * self.scenarios * self.rows_per_combination


@dataclass(frozen=True)
class BenchmarkDefinition:
    name: str
    category: str
    sql: str
    threshold_ms: int
    parameters: tuple[Any, ...] = ()
    explain: bool = True


@dataclass(frozen=True)
class BenchmarkResult:
    name: str
    category: str
    duration_ms: float
    threshold_ms: int
    status: BenchmarkStatus
    rows_returned: int = 0
    plan_json: str | None = None
    notes: Mapping[str, Any] = field(default_factory=dict)


DEFAULT_BENCHMARKS = (
    BenchmarkDefinition(
        "ledger_period_department_account",
        "ledger",
        """
        SELECT department_code, account_code, SUM(amount) AS total
        FROM ledger_lines
        WHERE fiscal_period BETWEEN ? AND ?
        GROUP BY department_code, account_code
        """,
        1500,
        ("2026-01", "2026-12"),
    ),
    BenchmarkDefinition(
        "import_staged_rows_batch_lookup",
        "imports",
        """
        SELECT row_number, row_hash, status
        FROM import_staged_rows
        WHERE batch_id = ?
        ORDER BY row_number
        """,
        800,
        ("benchmark-batch",),
    ),
    BenchmarkDefinition(
        "report_artifacts_recent",
        "reports",
        """
        SELECT artifact_id, export_type, valid, created_at
        FROM export_artifacts
        WHERE export_type = ?
        ORDER BY created_at DESC
        LIMIT 100
        """,
        500,
        ("pdf",),
    ),
    BenchmarkDefinition(
        "background_jobs_ready_queue",
        "jobs",
        """
        SELECT job_id, job_type, priority
        FROM background_jobs
        WHERE status = 'queued' AND run_after <= CURRENT_TIMESTAMP
        ORDER BY priority DESC, created_at
        LIMIT 50
        """,
        300,
        (),
    ),
)


class PerformanceBenchmarkService:
    def __init__(self, db: DatabaseConnection):
        self.db = db

    def build_seed_plan(self, payload: Mapping[str, Any] | None = None) -> SeedPlan:
        payload = payload or {}
        plan = SeedPlan(
            departments=int(payload.get("departments", 100)),
            accounts=int(payload.get("accounts", 500)),
            periods=int(payload.get("periods", 36)),
            scenarios=int(payload.get("scenarios", 6)),
            rows_per_combination=int(payload.get("rows_per_combination", 1)),
        )
        if plan.estimated_ledger_rows <= 0:
            raise ValidationError("Seed plan must create at least one ledger row")
        return plan

    def seed_manifest(self, plan: SeedPlan) -> dict[str, Any]:
        return {
            "departments": plan.departments,
            "accounts": plan.accounts,
            "periods": plan.periods,
            "scenarios": plan.scenarios,
            "rows_per_combination": plan.rows_per_combination,
            "estimated_ledger_rows": plan.estimated_ledger_rows,
        }

    def index_recommendations(self) -> list[dict[str, str]]:
        return [
            {
                "name": "ix_ledger_lines_period_department_account",
                "table": "ledger_lines",
                "columns": "fiscal_period, department_code, account_code",
                "reason": "Ledger report grouping by period, department, and account.",
            },
            {
                "name": "ix_ledger_lines_scenario_period",
                "table": "ledger_lines",
                "columns": "scenario_id, fiscal_period",
                "reason": "Scenario comparison and period filtering.",
            },
            {
                "name": "ix_import_staged_rows_batch_status",
                "table": "import_staged_rows",
                "columns": "batch_id, status, row_number",
                "reason": "Import preview, approval, and drill-back.",
            },
            {
                "name": "ix_export_artifacts_type_valid_created",
                "table": "export_artifacts",
                "columns": "export_type, valid, created_at DESC",
                "reason": "Recent report/export validation dashboard.",
            },
            {
                "name": "ix_background_jobs_status_run_after_priority",
                "table": "background_jobs",
                "columns": "status, run_after, priority DESC",
                "reason": "Worker queue leasing.",
            },
        ]

    def evaluate_result(
        self,
        definition: BenchmarkDefinition,
        duration_ms: float,
        rows_returned: int = 0,
        plan_json: str | None = None,
    ) -> BenchmarkResult:
        if duration_ms <= definition.threshold_ms:
            status = BenchmarkStatus.PASSED
        elif duration_ms <= definition.threshold_ms * 1.5:
            status = BenchmarkStatus.WARNING
        else:
            status = BenchmarkStatus.FAILED
        return BenchmarkResult(
            name=definition.name,
            category=definition.category,
            duration_ms=duration_ms,
            threshold_ms=definition.threshold_ms,
            status=status,
            rows_returned=rows_returned,
            plan_json=plan_json,
        )

    def record_run(
        self,
        context: ServiceContext,
        run_id: str,
        seed_plan: SeedPlan,
        results: Sequence[BenchmarkResult],
    ) -> None:
        self.db.execute(
            """
            INSERT INTO performance_benchmark_runs (
                run_id, seed_plan_json, result_json, status, created_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                json.dumps(self.seed_manifest(seed_plan), default=str, sort_keys=True),
                json.dumps([self._result_dict(result) for result in results], default=str, sort_keys=True),
                self.overall_status(results).value,
                context.user_id,
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    def recent_runs(self) -> list[dict[str, Any]]:
        return fetch_all(
            self.db.execute(
                """
                SELECT run_id, status, created_by, created_at
                FROM performance_benchmark_runs
                ORDER BY created_at DESC
                LIMIT 50
                """
            )
        )

    def overall_status(self, results: Sequence[BenchmarkResult]) -> BenchmarkStatus:
        if any(result.status is BenchmarkStatus.FAILED for result in results):
            return BenchmarkStatus.FAILED
        if any(result.status is BenchmarkStatus.WARNING for result in results):
            return BenchmarkStatus.WARNING
        return BenchmarkStatus.PASSED

    def _result_dict(self, result: BenchmarkResult) -> dict[str, Any]:
        return {
            "name": result.name,
            "category": result.category,
            "duration_ms": result.duration_ms,
            "threshold_ms": result.threshold_ms,
            "status": result.status.value,
            "rows_returned": result.rows_returned,
            "plan_json": result.plan_json,
            "notes": dict(result.notes),
        }

