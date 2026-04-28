from __future__ import annotations

from dataclasses import dataclass

from .audit import AuditService
from .audit_chain import AuditChainService
from .background_jobs import BackgroundJobService
from .backup_restore import BackupRestoreService
from .base import DatabaseConnection
from .budget import BudgetService
from .export_validation import ExportValidationService
from .financial_correctness import FinancialCorrectnessService
from .forecast import ForecastService
from .idempotency import IdempotencyService
from .import_pipeline import ImportPipelineService
from .imports import ImportService
from .ledger import LedgerService
from .performance_benchmarks import PerformanceBenchmarkService
from .production_dashboard import ProductionDashboardService
from .production_readiness import ProductionReadinessService
from .release_governance import ReleaseGovernanceService
from .reports import ReportService
from .security import SecurityService
from .transactions import TransactionManager
from .workflow import WorkflowService


@dataclass(frozen=True)
class ServiceRegistry:
    audit: AuditService
    audit_chain: AuditChainService
    background_jobs: BackgroundJobService
    backup_restore: BackupRestoreService
    budget: BudgetService
    export_validation: ExportValidationService
    financial_correctness: FinancialCorrectnessService
    forecast: ForecastService
    idempotency: IdempotencyService
    imports: ImportService
    import_pipeline: ImportPipelineService
    ledger: LedgerService
    performance_benchmarks: PerformanceBenchmarkService
    production_dashboard: ProductionDashboardService
    production_readiness: ProductionReadinessService
    release_governance: ReleaseGovernanceService
    reports: ReportService
    security: SecurityService
    transactions: TransactionManager
    workflow: WorkflowService


def build_services(db: DatabaseConnection) -> ServiceRegistry:
    audit = AuditService(db)
    audit_chain = AuditChainService(db)
    idempotency = IdempotencyService(db)
    security = SecurityService()
    transactions = TransactionManager(db)
    return ServiceRegistry(
        audit=audit,
        audit_chain=audit_chain,
        background_jobs=BackgroundJobService(db, audit=audit),
        backup_restore=BackupRestoreService(db, audit=audit),
        budget=BudgetService(db, audit=audit),
        export_validation=ExportValidationService(db, audit=audit),
        financial_correctness=FinancialCorrectnessService(),
        forecast=ForecastService(db, audit=audit),
        idempotency=idempotency,
        imports=ImportService(db, audit=audit),
        import_pipeline=ImportPipelineService(db, audit=audit, security=security, transactions=transactions),
        ledger=LedgerService(
            db,
            audit=audit,
            security=security,
            idempotency=idempotency,
            audit_chain=audit_chain,
            transactions=transactions,
        ),
        performance_benchmarks=PerformanceBenchmarkService(db),
        production_dashboard=ProductionDashboardService(db),
        production_readiness=ProductionReadinessService(),
        release_governance=ReleaseGovernanceService(db, audit=audit),
        reports=ReportService(db, audit=audit),
        security=security,
        transactions=transactions,
        workflow=WorkflowService(db, audit=audit, security=security),
    )

