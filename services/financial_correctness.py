from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping, Sequence

from .base import ValidationError


Money = Decimal


def money(value: Any) -> Money:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class LedgerAmount:
    basis: str
    account: str
    department: str
    period: str
    amount: Money
    scenario: str | None = None


@dataclass(frozen=True)
class VarianceResult:
    key: str
    actual: Money
    comparison: Money
    variance: Money
    variance_percent: Money | None


class FinancialCorrectnessService:
    VALID_BASES = {"actual", "budget", "forecast", "scenario"}

    def normalize_ledger(self, rows: Sequence[Mapping[str, Any]]) -> list[LedgerAmount]:
        normalized: list[LedgerAmount] = []
        for row in rows:
            basis = str(row.get("basis", "")).lower()
            if basis not in self.VALID_BASES:
                raise ValidationError(f"Invalid ledger basis: {basis}")
            normalized.append(
                LedgerAmount(
                    basis=basis,
                    account=str(row["account"]),
                    department=str(row["department"]),
                    period=str(row["period"]),
                    amount=money(row["amount"]),
                    scenario=str(row["scenario"]) if row.get("scenario") else None,
                )
            )
        return normalized

    def total_by_basis(self, rows: Sequence[LedgerAmount], basis: str) -> Money:
        return sum((row.amount for row in rows if row.basis == basis), Decimal("0.00"))

    def variance(self, rows: Sequence[LedgerAmount], comparison_basis: str) -> VarianceResult:
        actual = self.total_by_basis(rows, "actual")
        comparison = self.total_by_basis(rows, comparison_basis)
        variance = actual - comparison
        percent = None if comparison == 0 else (variance / abs(comparison) * Decimal("100.00")).quantize(Decimal("0.01"))
        return VarianceResult("actual_vs_" + comparison_basis, actual, comparison, variance, percent)

    def reconciliation_difference(self, ledger_total: Any, source_total: Any) -> Money:
        return money(ledger_total) - money(source_total)

    def is_reconciled(self, ledger_total: Any, source_total: Any, tolerance: Any = "0.00") -> bool:
        return abs(self.reconciliation_difference(ledger_total, source_total)) <= money(tolerance)

    def consolidation_net(self, entity_totals: Mapping[str, Any], eliminations: Sequence[Mapping[str, Any]]) -> Money:
        total = sum((money(value) for value in entity_totals.values()), Decimal("0.00"))
        eliminated = sum((money(row["amount"]) for row in eliminations), Decimal("0.00"))
        return total + eliminated

    def allocate(self, pool_amount: Any, drivers: Mapping[str, Any]) -> dict[str, Money]:
        total_driver = sum((money(value) for value in drivers.values()), Decimal("0.00"))
        if total_driver <= 0:
            raise ValidationError("Allocation drivers must total more than zero")
        pool = money(pool_amount)
        allocation: dict[str, Money] = {}
        remaining = pool
        items = list(drivers.items())
        for department, driver in items[:-1]:
            amount = (pool * money(driver) / total_driver).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            allocation[department] = amount
            remaining -= amount
        allocation[items[-1][0]] = remaining.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return allocation

    def approval_complete(self, approvals: Sequence[Mapping[str, Any]], required_roles: Sequence[str]) -> bool:
        approved_roles = {
            str(row.get("role", "")).lower()
            for row in approvals
            if str(row.get("status", "")).lower() == "approved"
        }
        return all(role.lower() in approved_roles for role in required_roles)

    def close_ready(self, tasks: Sequence[Mapping[str, Any]]) -> bool:
        return all(str(task.get("status", "")).lower() in {"complete", "completed", "approved"} for task in tasks)

    def evaluate_fixture(self, fixture: Mapping[str, Any]) -> dict[str, Any]:
        rows = self.normalize_ledger(fixture.get("ledger", []))
        result: dict[str, Any] = {
            "actual_total": str(self.total_by_basis(rows, "actual")),
            "budget_total": str(self.total_by_basis(rows, "budget")),
            "forecast_total": str(self.total_by_basis(rows, "forecast")),
            "scenario_total": str(self.total_by_basis(rows, "scenario")),
        }
        if fixture.get("variance_basis"):
            variance = self.variance(rows, str(fixture["variance_basis"]))
            result["variance"] = str(variance.variance)
            result["variance_percent"] = None if variance.variance_percent is None else str(variance.variance_percent)
        if "reconciliation" in fixture:
            recon = fixture["reconciliation"]
            result["reconciled"] = self.is_reconciled(
                recon["ledger_total"],
                recon["source_total"],
                recon.get("tolerance", "0.00"),
            )
        if "consolidation" in fixture:
            consolidation = fixture["consolidation"]
            result["consolidation_net"] = str(
                self.consolidation_net(consolidation["entity_totals"], consolidation.get("eliminations", []))
            )
        if "allocation" in fixture:
            allocation = fixture["allocation"]
            result["allocation"] = {
                key: str(value)
                for key, value in self.allocate(allocation["pool_amount"], allocation["drivers"]).items()
            }
        if "approvals" in fixture:
            result["approval_complete"] = self.approval_complete(
                fixture["approvals"],
                fixture.get("required_approval_roles", []),
            )
        if "close_tasks" in fixture:
            result["close_ready"] = self.close_ready(fixture["close_tasks"])
        return result

