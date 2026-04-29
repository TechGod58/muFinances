from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


Period = str
Code = str


class StrictFinancialContract(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)


class LedgerPostContract(StrictFinancialContract):
    scenario_id: int = Field(ge=1)
    department_code: Code = Field(min_length=1, max_length=40)
    fund_code: Code = Field(min_length=1, max_length=40)
    account_code: Code = Field(min_length=1, max_length=40)
    period: Period = Field(pattern=r'^\d{4}-\d{2}$')
    amount: float
    notes: str = Field(default='', max_length=1000)
    source: str = Field(default='manual', min_length=1, max_length=80)
    driver_key: str | None = Field(default=None, max_length=120)
    ledger_type: str = Field(default='planning', min_length=1, max_length=40)
    ledger_basis: Literal['actual', 'budget', 'forecast', 'scenario'] | None = None
    source_version: str | None = Field(default=None, max_length=120)
    source_record_id: str | None = Field(default=None, max_length=160)
    parent_ledger_entry_id: int | None = Field(default=None, ge=1)
    idempotency_key: str | None = Field(default=None, max_length=220)
    entity_code: Code = Field(default='CAMPUS', min_length=1, max_length=40)
    program_code: Code | None = Field(default=None, max_length=40)
    project_code: Code | None = Field(default=None, max_length=40)
    grant_code: Code | None = Field(default=None, max_length=40)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator('amount')
    @classmethod
    def finite_non_zero_amount(cls, value: float) -> float:
        if value != value or value in {float('inf'), float('-inf')}:
            raise ValueError('Amount must be finite.')
        if round(float(value), 2) == 0:
            raise ValueError('Amount cannot round to zero.')
        return float(value)


class BudgetSubmissionContract(StrictFinancialContract):
    scenario_id: int = Field(ge=1)
    department_code: Code = Field(min_length=1, max_length=40)
    owner: str = Field(min_length=1, max_length=160)
    notes: str = Field(default='', max_length=1000)


class OperatingBudgetLineContract(StrictFinancialContract):
    fund_code: Code = Field(min_length=1, max_length=40)
    account_code: Code = Field(min_length=1, max_length=40)
    period: Period = Field(pattern=r'^\d{4}-\d{2}$')
    amount: float
    line_type: Literal['revenue', 'expense', 'transfer', 'adjustment', 'position', 'capital']
    recurrence: Literal['recurring', 'one_time']
    notes: str = Field(default='', max_length=1000)

    @field_validator('amount')
    @classmethod
    def finite_amount(cls, value: float) -> float:
        if value != value or value in {float('inf'), float('-inf')}:
            raise ValueError('Amount must be finite.')
        return float(value)


class ForecastRunContract(StrictFinancialContract):
    scenario_id: int = Field(ge=1)
    method_key: str = Field(min_length=1, max_length=80)
    account_code: Code = Field(min_length=1, max_length=40)
    department_code: Code | None = Field(default=None, max_length=40)
    period_start: Period = Field(pattern=r'^\d{4}-\d{2}$')
    period_end: Period = Field(pattern=r'^\d{4}-\d{2}$')
    driver_key: str | None = Field(default=None, max_length=120)
    confidence: float = Field(default=0.8, ge=0.01, le=0.99)

    @model_validator(mode='after')
    def period_range_ordered(self) -> ForecastRunContract:
        if self.period_end < self.period_start:
            raise ValueError('period_end must be on or after period_start.')
        return self


class CloseReconciliationContract(StrictFinancialContract):
    scenario_id: int = Field(ge=1)
    period: Period = Field(pattern=r'^\d{4}-\d{2}$')
    entity_code: Code = Field(min_length=1, max_length=40)
    account_code: Code = Field(min_length=1, max_length=40)
    source_balance: float
    owner: str = Field(min_length=1, max_length=160)
    notes: str = Field(default='', max_length=1000)


class ConsolidationRunContract(StrictFinancialContract):
    scenario_id: int = Field(ge=1)
    period: Period = Field(pattern=r'^\d{4}-\d{2}$')


class ReportDefinitionContract(StrictFinancialContract):
    name: str = Field(min_length=1, max_length=160)
    report_type: Literal['ledger_matrix', 'financial_statement', 'variance']
    row_dimension: str = Field(min_length=1, max_length=80)
    column_dimension: str = Field(min_length=1, max_length=80)
    filters: dict[str, Any] = Field(default_factory=dict)


class ImportBatchContract(StrictFinancialContract):
    scenario_id: int = Field(ge=1)
    connector_key: str = Field(min_length=1, max_length=120)
    source_format: Literal['csv', 'xlsx', 'json', 'api']
    import_type: Literal['ledger', 'banking_cash', 'crm_enrollment']
    source_name: str = Field(default='', max_length=240)
    stream_chunk_size: int = Field(default=1000, ge=1, le=100000)
    rows: list[dict[str, Any]] = Field(default_factory=list)


class FinancialAuditEventContract(StrictFinancialContract):
    entity_type: str = Field(min_length=1, max_length=120)
    entity_id: str = Field(min_length=1, max_length=160)
    action: str = Field(min_length=1, max_length=80)
    actor: str = Field(min_length=1, max_length=160)
    detail: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(min_length=1, max_length=80)


class SecurityWorkflowContract(StrictFinancialContract):
    user_id: int | None = Field(default=None, ge=1)
    permission_key: str | None = Field(default=None, max_length=120)
    role_keys: list[str] = Field(default_factory=list)
    dimension_access: dict[str, str] = Field(default_factory=dict)


CONTRACT_REGISTRY = {
    'ledger.post': LedgerPostContract,
    'budget.submission': BudgetSubmissionContract,
    'budget.line': OperatingBudgetLineContract,
    'forecast.run': ForecastRunContract,
    'close.reconciliation': CloseReconciliationContract,
    'consolidation.run': ConsolidationRunContract,
    'report.definition': ReportDefinitionContract,
    'integration.import_batch': ImportBatchContract,
    'audit.financial_event': FinancialAuditEventContract,
    'security.workflow': SecurityWorkflowContract,
}


def contract_status() -> dict[str, Any]:
    return {
        'batch': 'B114',
        'title': 'Financial Service Contract Hardening',
        'complete': True,
        'contracts': sorted(CONTRACT_REGISTRY),
        'checks': {
            'ledger_contract_ready': 'ledger.post' in CONTRACT_REGISTRY,
            'budget_contract_ready': 'budget.line' in CONTRACT_REGISTRY,
            'forecast_contract_ready': 'forecast.run' in CONTRACT_REGISTRY,
            'close_contract_ready': 'close.reconciliation' in CONTRACT_REGISTRY,
            'consolidation_contract_ready': 'consolidation.run' in CONTRACT_REGISTRY,
            'reporting_contract_ready': 'report.definition' in CONTRACT_REGISTRY,
            'integration_contract_ready': 'integration.import_batch' in CONTRACT_REGISTRY,
            'audit_contract_ready': 'audit.financial_event' in CONTRACT_REGISTRY,
            'security_contract_ready': 'security.workflow' in CONTRACT_REGISTRY,
        },
    }
