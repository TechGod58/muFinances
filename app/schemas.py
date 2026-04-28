from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ScenarioCreate(BaseModel):
    name: str = Field(min_length=3, max_length=120)
    version: str = Field(default='v1', min_length=1, max_length=20)
    start_period: str = Field(pattern=r'^\d{4}-\d{2}$')
    end_period: str = Field(pattern=r'^\d{4}-\d{2}$')


class ScenarioOut(BaseModel):
    id: int
    name: str
    version: str
    status: str
    start_period: str
    end_period: str
    locked: bool
    created_at: str


class PlanLineItemCreate(BaseModel):
    department_code: str
    fund_code: str
    account_code: str
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    amount: float
    notes: str = ''


class PlanLineItemOut(BaseModel):
    id: int
    scenario_id: int
    department_code: str
    fund_code: str
    account_code: str
    period: str
    amount: float
    notes: str
    source: str
    driver_key: str | None = None


class LedgerEntryCreate(BaseModel):
    scenario_id: int
    department_code: str
    fund_code: str
    account_code: str
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    amount: float
    notes: str = ''
    source: str = Field(default='manual', max_length=40)
    ledger_type: str = Field(default='planning', max_length=40)
    ledger_basis: Literal['actual', 'budget', 'forecast', 'scenario'] | None = None
    source_version: str | None = Field(default=None, max_length=80)
    source_record_id: str | None = Field(default=None, max_length=120)
    parent_ledger_entry_id: int | None = None
    idempotency_key: str | None = Field(default=None, max_length=160)
    entity_code: str = Field(default='CAMPUS', max_length=40)
    program_code: str | None = None
    project_code: str | None = None
    grant_code: str | None = None
    metadata: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class DimensionMemberCreate(BaseModel):
    dimension_kind: str = Field(min_length=2, max_length=40)
    code: str = Field(min_length=1, max_length=40)
    name: str = Field(min_length=1, max_length=160)
    parent_code: str | None = Field(default=None, max_length=40)
    metadata: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class BackupCreate(BaseModel):
    note: str = Field(default='', max_length=240)


class FiscalPeriodCreate(BaseModel):
    fiscal_year: str = Field(min_length=2, max_length=20)
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    period_index: int = Field(ge=1, le=60)
    is_closed: bool = False


class LedgerReverseCreate(BaseModel):
    reason: str = Field(min_length=3, max_length=240)


class AuthLogin(BaseModel):
    email: str = Field(min_length=3, max_length=160)
    password: str = Field(min_length=8, max_length=200)


class PasswordChangeCreate(BaseModel):
    current_password: str = Field(min_length=8, max_length=200)
    new_password: str = Field(min_length=12, max_length=200)


class UserCreate(BaseModel):
    email: str = Field(min_length=3, max_length=160)
    display_name: str = Field(min_length=1, max_length=160)
    password: str = Field(min_length=8, max_length=200)
    role_keys: list[str] = Field(default_factory=lambda: ['department.planner'])


class UserDimensionAccessCreate(BaseModel):
    dimension_kind: str = Field(min_length=2, max_length=40)
    code: str = Field(min_length=1, max_length=40)


class SSOProductionSettingCreate(BaseModel):
    provider_key: str = Field(default='campus-sso', min_length=1, max_length=80)
    environment: Literal['test', 'production'] = 'production'
    metadata_url: str = Field(default='', max_length=500)
    required_claim: str = Field(default='email', min_length=1, max_length=80)
    group_claim: str = Field(default='groups', min_length=1, max_length=80)
    jit_provisioning: bool = True
    status: Literal['draft', 'ready', 'enabled'] = 'draft'


class ADOUGroupMappingCreate(BaseModel):
    mapping_key: str = Field(min_length=1, max_length=120)
    ad_group_dn: str = Field(min_length=1, max_length=500)
    allowed_ou_dn: str = Field(min_length=1, max_length=500)
    role_key: str = Field(min_length=1, max_length=80)
    dimension_kind: str | None = Field(default=None, max_length=40)
    dimension_code: str | None = Field(default=None, max_length=80)
    active: bool = True


class DomainVPNCheckCreate(BaseModel):
    check_key: str | None = Field(default=None, max_length=120)
    host: str = Field(min_length=1, max_length=240)
    client_host: str = Field(default='', max_length=120)
    forwarded_host: str = Field(default='', max_length=240)
    forwarded_for: str = Field(default='', max_length=120)


class AdminImpersonationCreate(BaseModel):
    target_user_id: int
    reason: str = Field(min_length=3, max_length=500)


class SoDPolicyCreate(BaseModel):
    rule_key: str = Field(min_length=1, max_length=120)
    name: str = Field(min_length=1, max_length=240)
    conflict_type: Literal['role_pair', 'action_pair'] = 'role_pair'
    left_value: str = Field(min_length=1, max_length=120)
    right_value: str = Field(min_length=1, max_length=120)
    severity: Literal['low', 'medium', 'high', 'critical'] = 'medium'
    active: bool = True


class UserAccessReviewCreate(BaseModel):
    review_key: str = Field(min_length=1, max_length=120)
    reviewer_user_id: int
    scenario_id: int | None = None
    scope: dict[str, Any] = Field(default_factory=dict)


class UserAccessReviewDecision(BaseModel):
    findings: list[dict[str, Any]] = Field(default_factory=list)


class MasterDataChangeCreate(BaseModel):
    dimension_kind: Literal['account', 'department', 'entity', 'fund', 'program', 'project', 'grant']
    code: str = Field(min_length=1, max_length=40)
    name: str = Field(min_length=1, max_length=160)
    parent_code: str | None = Field(default=None, max_length=40)
    change_type: Literal['create', 'update', 'deactivate', 'reactivate'] = 'create'
    effective_from: str = Field(pattern=r'^\d{4}-\d{2}$')
    effective_to: str | None = Field(default=None, pattern=r'^\d{4}-\d{2}$')
    metadata: dict[str, Any] = Field(default_factory=dict)


class MasterDataMappingCreate(BaseModel):
    mapping_key: str = Field(min_length=1, max_length=120)
    source_system: str = Field(min_length=1, max_length=80)
    source_dimension: str = Field(min_length=1, max_length=40)
    source_code: str = Field(min_length=1, max_length=80)
    target_dimension: str = Field(min_length=1, max_length=40)
    target_code: str = Field(min_length=1, max_length=80)
    effective_from: str = Field(pattern=r'^\d{4}-\d{2}$')
    effective_to: str | None = Field(default=None, pattern=r'^\d{4}-\d{2}$')
    active: bool = True


class MetadataApprovalCreate(BaseModel):
    entity_type: str = Field(min_length=1, max_length=80)
    entity_id: str = Field(min_length=1, max_length=160)
    metadata: dict[str, Any] = Field(default_factory=dict)


class BudgetSubmissionCreate(BaseModel):
    scenario_id: int
    department_code: str = Field(min_length=1, max_length=40)
    owner: str = Field(min_length=1, max_length=160)
    notes: str = Field(default='', max_length=500)


class BudgetAssumptionCreate(BaseModel):
    scenario_id: int
    assumption_key: str = Field(min_length=1, max_length=80)
    label: str = Field(min_length=1, max_length=160)
    value: float
    unit: str = Field(default='ratio', max_length=40)
    department_code: str | None = Field(default=None, max_length=40)
    notes: str = Field(default='', max_length=500)


class OperatingBudgetLineCreate(BaseModel):
    fund_code: str = Field(min_length=1, max_length=40)
    account_code: str = Field(min_length=1, max_length=40)
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    amount: float
    line_type: Literal['revenue', 'expense', 'transfer', 'adjustment'] = 'expense'
    recurrence: Literal['recurring', 'one_time'] = 'recurring'
    notes: str = Field(default='', max_length=500)


class BudgetTransferCreate(BaseModel):
    scenario_id: int
    from_department_code: str = Field(min_length=1, max_length=40)
    to_department_code: str = Field(min_length=1, max_length=40)
    fund_code: str = Field(min_length=1, max_length=40)
    account_code: str = Field(min_length=1, max_length=40)
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    amount: float = Field(gt=0)
    reason: str = Field(min_length=3, max_length=500)


class ApprovalAction(BaseModel):
    note: str = Field(default='', max_length=500)


class EnrollmentTermCreate(BaseModel):
    scenario_id: int
    term_code: str = Field(min_length=1, max_length=40)
    term_name: str = Field(min_length=1, max_length=160)
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    census_date: str = Field(default='', max_length=40)


class TuitionRateCreate(BaseModel):
    scenario_id: int
    program_code: str = Field(min_length=1, max_length=40)
    residency: str = Field(min_length=1, max_length=40)
    rate_per_credit: float = Field(gt=0)
    default_credit_load: float = Field(gt=0)
    effective_term: str = Field(min_length=1, max_length=40)


class EnrollmentForecastInputCreate(BaseModel):
    scenario_id: int
    term_code: str = Field(min_length=1, max_length=40)
    program_code: str = Field(min_length=1, max_length=40)
    residency: str = Field(min_length=1, max_length=40)
    headcount: float = Field(ge=0)
    fte: float = Field(ge=0)
    retention_rate: float = Field(ge=0, le=1)
    yield_rate: float = Field(ge=0, le=1)
    discount_rate: float = Field(ge=0, le=1)


class TuitionForecastRunCreate(BaseModel):
    scenario_id: int
    term_code: str = Field(min_length=1, max_length=40)


class WorkforcePositionCreate(BaseModel):
    scenario_id: int
    position_code: str = Field(min_length=1, max_length=40)
    title: str = Field(min_length=1, max_length=160)
    department_code: str = Field(min_length=1, max_length=40)
    employee_type: str = Field(min_length=1, max_length=40)
    fte: float = Field(ge=0)
    annual_salary: float = Field(ge=0)
    benefit_rate: float = Field(ge=0, le=1)
    vacancy_rate: float = Field(default=0, ge=0, le=1)


class FacultyLoadCreate(BaseModel):
    scenario_id: int
    department_code: str = Field(min_length=1, max_length=40)
    term_code: str = Field(min_length=1, max_length=40)
    course_code: str = Field(min_length=1, max_length=40)
    sections: int = Field(ge=0)
    credit_hours: float = Field(ge=0)
    faculty_fte: float = Field(ge=0)
    adjunct_cost: float = Field(default=0, ge=0)


class GrantBudgetCreate(BaseModel):
    scenario_id: int
    grant_code: str = Field(min_length=1, max_length=40)
    department_code: str = Field(min_length=1, max_length=40)
    sponsor: str = Field(min_length=1, max_length=160)
    start_period: str = Field(pattern=r'^\d{4}-\d{2}$')
    end_period: str = Field(pattern=r'^\d{4}-\d{2}$')
    total_award: float = Field(ge=0)
    direct_cost_budget: float = Field(ge=0)
    indirect_cost_rate: float = Field(ge=0, le=1)
    spent_to_date: float = Field(default=0, ge=0)


class CapitalRequestCreate(BaseModel):
    scenario_id: int
    request_code: str = Field(min_length=1, max_length=40)
    department_code: str = Field(min_length=1, max_length=40)
    project_name: str = Field(min_length=1, max_length=160)
    asset_category: str = Field(min_length=1, max_length=80)
    acquisition_period: str = Field(pattern=r'^\d{4}-\d{2}$')
    capital_cost: float = Field(gt=0)
    useful_life_years: int = Field(ge=1, le=80)
    funding_source: str = Field(min_length=1, max_length=80)


class TypedDriverCreate(BaseModel):
    scenario_id: int
    driver_key: str = Field(min_length=1, max_length=80)
    label: str = Field(min_length=1, max_length=160)
    driver_type: Literal['ratio', 'currency', 'count', 'percent', 'index'] = 'ratio'
    unit: str = Field(min_length=1, max_length=40)
    value: float
    locked: bool = False


class ScenarioCloneCreate(BaseModel):
    name: str = Field(min_length=3, max_length=120)
    version: str = Field(default='v1', min_length=1, max_length=20)


class ScenarioMergeCreate(BaseModel):
    source_scenario_id: int
    note: str = Field(default='', max_length=500)


class JournalAdjustmentCreate(BaseModel):
    scenario_id: int
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    department_code: str = Field(min_length=1, max_length=40)
    fund_code: str = Field(min_length=1, max_length=40)
    account_code: str = Field(min_length=1, max_length=40)
    amount: float
    ledger_basis: Literal['actual', 'budget', 'forecast', 'scenario'] = 'budget'
    reason: str = Field(min_length=3, max_length=500)
    entity_code: str = Field(default='CAMPUS', max_length=40)


class EntityCommentCreate(BaseModel):
    entity_type: Literal['budget_line', 'report', 'reconciliation', 'close_task', 'ledger_entry', 'audit_packet']
    entity_id: str = Field(min_length=1, max_length=80)
    comment_text: str = Field(min_length=1, max_length=2000)
    visibility: Literal['internal', 'audit', 'executive'] = 'internal'


class EvidenceAttachmentCreate(BaseModel):
    entity_type: Literal['budget_line', 'report', 'reconciliation', 'close_task', 'ledger_entry', 'audit_packet']
    entity_id: str = Field(min_length=1, max_length=80)
    file_name: str = Field(min_length=1, max_length=240)
    storage_path: str = Field(min_length=1, max_length=500)
    content_type: str = Field(default='application/octet-stream', max_length=120)
    size_bytes: int = Field(default=0, ge=0)
    retention_until: str | None = Field(default=None, max_length=40)
    metadata: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class ForecastRunCreate(BaseModel):
    scenario_id: int
    method_key: Literal['straight_line', 'growth_rate', 'rolling_average', 'driver_based', 'seasonal', 'historical_trend']
    account_code: str = Field(min_length=1, max_length=40)
    period_start: str = Field(pattern=r'^\d{4}-\d{2}$')
    period_end: str = Field(pattern=r'^\d{4}-\d{2}$')
    department_code: str | None = Field(default=None, max_length=40)
    driver_key: str | None = Field(default=None, max_length=80)
    confidence: float = Field(default=0.8, ge=0, le=1)


class PredictiveModelChoiceCreate(BaseModel):
    scenario_id: int
    choice_key: str = Field(min_length=1, max_length=80)
    account_code: str = Field(min_length=1, max_length=40)
    selected_method: Literal['straight_line', 'growth_rate', 'rolling_average', 'driver_based', 'seasonal', 'historical_trend']
    department_code: str | None = Field(default=None, max_length=40)
    seasonality_mode: Literal['auto', 'off', 'monthly', 'academic_term'] = 'auto'
    confidence_level: float = Field(default=0.8, ge=0.5, le=0.99)


class ForecastTuningProfileCreate(BaseModel):
    choice_id: int
    seasonality_strength: float = Field(default=1, ge=0, le=3)
    confidence_level: float = Field(default=0.8, ge=0.5, le=0.99)
    confidence_spread: float = Field(default=0.2, ge=0.01, le=1)
    driver_weights: dict[str, float] = Field(default_factory=dict)


class ForecastBacktestCreate(BaseModel):
    choice_id: int
    period_start: str = Field(pattern=r'^\d{4}-\d{2}$')
    period_end: str = Field(pattern=r'^\d{4}-\d{2}$')


class ForecastRecommendationCompareCreate(BaseModel):
    scenario_id: int
    account_code: str = Field(min_length=1, max_length=40)
    department_code: str | None = Field(default=None, max_length=40)
    methods: list[Literal['straight_line', 'growth_rate', 'rolling_average', 'driver_based', 'seasonal', 'historical_trend']] = Field(default_factory=list)


class ActualsIngestCreate(BaseModel):
    scenario_id: int
    source_version: str = Field(default='actuals-upload', max_length=80)
    rows: list[LedgerEntryCreate] = Field(default_factory=list)


class DriverDefinitionCreate(BaseModel):
    scenario_id: int
    driver_key: str = Field(min_length=1, max_length=80)
    label: str = Field(min_length=1, max_length=160)
    expression: str | None = Field(default=None, max_length=500)
    value: float | None = None
    unit: str = Field(default='ratio', max_length=40)


class PlanningModelCreate(BaseModel):
    scenario_id: int
    model_key: str = Field(min_length=1, max_length=80)
    name: str = Field(min_length=1, max_length=160)
    description: str = Field(default='', max_length=1000)
    status: Literal['draft', 'active', 'retired'] = 'draft'


class ModelFormulaCreate(BaseModel):
    model_id: int
    formula_key: str = Field(min_length=1, max_length=80)
    label: str = Field(min_length=1, max_length=160)
    expression: str = Field(min_length=1, max_length=500)
    target_account_code: str = Field(min_length=1, max_length=40)
    target_department_code: str | None = Field(default=None, max_length=40)
    target_fund_code: str = Field(default='GEN', min_length=1, max_length=40)
    period_start: str = Field(pattern=r'^\d{4}-\d{2}$')
    period_end: str = Field(pattern=r'^\d{4}-\d{2}$')
    active: bool = True


class FormulaLintRequest(BaseModel):
    expression: str = Field(min_length=1, max_length=500)
    context: dict[str, float] = Field(default_factory=dict)
    evaluate: bool = False


class AllocationRuleCreate(BaseModel):
    model_id: int
    rule_key: str = Field(min_length=1, max_length=80)
    label: str = Field(min_length=1, max_length=160)
    source_account_code: str = Field(min_length=1, max_length=40)
    source_department_code: str | None = Field(default=None, max_length=40)
    target_account_code: str = Field(min_length=1, max_length=40)
    target_fund_code: str = Field(default='GEN', min_length=1, max_length=40)
    basis_account_code: str | None = Field(default=None, max_length=40)
    basis_driver_key: str | None = Field(default=None, max_length=80)
    target_department_codes: list[str] = Field(min_length=1)
    period_start: str = Field(pattern=r'^\d{4}-\d{2}$')
    period_end: str = Field(pattern=r'^\d{4}-\d{2}$')
    active: bool = True


class ProfitabilityCostPoolCreate(BaseModel):
    scenario_id: int
    pool_key: str = Field(min_length=1, max_length=80)
    name: str = Field(min_length=1, max_length=160)
    source_department_code: str = Field(min_length=1, max_length=40)
    source_account_code: str = Field(min_length=1, max_length=40)
    allocation_basis: Literal['revenue', 'expense', 'headcount', 'equal'] = 'revenue'
    target_type: Literal['department', 'program', 'fund', 'grant'] = 'department'
    target_codes: list[str] = Field(default_factory=list)
    active: bool = True


class ProfitabilityAllocationRunCreate(BaseModel):
    scenario_id: int
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    pool_keys: list[str] = Field(default_factory=list)


class RetentionPolicyCreate(BaseModel):
    policy_key: str = Field(min_length=1, max_length=80)
    entity_type: str = Field(min_length=1, max_length=80)
    retention_years: int = Field(ge=1, le=100)
    disposition_action: Literal['retain', 'archive', 'purge_review'] = 'archive'
    legal_hold: bool = False
    active: bool = True


class ComplianceCertificationCreate(BaseModel):
    certification_key: str = Field(min_length=1, max_length=120)
    control_area: Literal['budget', 'close', 'consolidation', 'reporting', 'security', 'integration', 'audit'] = 'audit'
    period: str = Field(min_length=1, max_length=40)
    owner: str = Field(min_length=1, max_length=160)
    scenario_id: int | None = None
    due_at: str | None = Field(default=None, max_length=40)
    notes: str = Field(default='', max_length=1000)


class ComplianceCertificationDecision(BaseModel):
    evidence: dict[str, str | int | float | bool | None] = Field(default_factory=dict)
    notes: str = Field(default='', max_length=1000)


class TaxActivityClassificationCreate(BaseModel):
    classification_key: str | None = Field(default=None, max_length=120)
    scenario_id: int
    ledger_entry_id: int | None = None
    activity_name: str = Field(min_length=1, max_length=200)
    tax_status: Literal['exempt', 'taxable', 'mixed', 'review'] = 'review'
    activity_tag: Literal['mission_related', 'unrelated_business', 'excluded_ubi', 'contribution', 'investment', 'rental', 'royalty', 'sponsorship', 'other'] = 'other'
    income_type: Literal['program_service', 'contribution', 'investment', 'unrelated_business', 'rental', 'royalty', 'grant', 'other'] = 'other'
    ubit_code: str | None = Field(default=None, max_length=80)
    regularly_carried_on: bool = False
    substantially_related: bool = True
    debt_financed: bool = False
    amount: float | None = None
    expense_offset: float = 0
    form990_part: str | None = Field(default=None, max_length=40)
    form990_line: str | None = Field(default=None, max_length=40)
    form990_column: str | None = Field(default=None, max_length=20)
    review_status: Literal['draft', 'needs_review', 'approved', 'rejected'] = 'draft'
    notes: str = Field(default='', max_length=1000)
    metadata: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class Form990SupportFieldCreate(BaseModel):
    support_key: str | None = Field(default=None, max_length=120)
    scenario_id: int
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    form_part: str = Field(min_length=1, max_length=40)
    line_number: str = Field(min_length=1, max_length=40)
    column_code: str = Field(default='', max_length=20)
    description: str = Field(min_length=1, max_length=300)
    amount: float = 0
    basis: dict[str, str | int | float | bool | None] = Field(default_factory=dict)
    review_status: Literal['draft', 'needs_review', 'approved'] = 'draft'


class TaxRuleSourceCreate(BaseModel):
    source_key: str = Field(min_length=1, max_length=120)
    jurisdiction: str = Field(default='US', max_length=80)
    source_name: str = Field(min_length=1, max_length=240)
    source_url: str = Field(min_length=1, max_length=500)
    rule_area: Literal['form_990', 'ubit', 'state', 'sales_tax', 'payroll_tax', 'other'] = 'other'
    latest_known_version: str = Field(default='', max_length=120)
    check_frequency_days: int = Field(default=30, ge=1, le=366)
    next_check_at: str | None = Field(default=None, max_length=80)
    status: Literal['active', 'paused', 'retired'] = 'active'
    notes: str = Field(default='', max_length=1000)


class TaxUpdateCheckCreate(BaseModel):
    source_key: str = Field(min_length=1, max_length=120)
    observed_version: str | None = Field(default=None, max_length=120)
    detail: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class TaxReviewDecision(BaseModel):
    decision: Literal['approve', 'reject', 'request_changes'] = 'approve'
    note: str = Field(default='', max_length=1000)
    evidence: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class TaxAlertDecision(BaseModel):
    status: Literal['acknowledged', 'resolved'] = 'acknowledged'
    note: str = Field(default='', max_length=1000)


class ReportDefinitionCreate(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    report_type: Literal['ledger_matrix', 'financial_statement', 'variance'] = 'ledger_matrix'
    row_dimension: Literal['department_code', 'account_code', 'fund_code', 'period'] = 'department_code'
    column_dimension: Literal['account_code', 'department_code', 'fund_code', 'period'] = 'account_code'
    filters: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class DashboardWidgetCreate(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    widget_type: Literal['metric', 'variance', 'trend'] = 'metric'
    metric_key: Literal['revenue_total', 'expense_total', 'net_total'] = 'net_total'
    scenario_id: int
    config: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class ScheduledExportCreate(BaseModel):
    report_definition_id: int
    scenario_id: int
    export_format: Literal['json', 'csv', 'xlsx', 'pdf'] = 'json'
    schedule_cron: str = Field(min_length=3, max_length=80)
    destination: str = Field(min_length=1, max_length=240)


class BoardPackageCreate(BaseModel):
    scenario_id: int
    package_name: str = Field(min_length=1, max_length=160)
    period_start: str = Field(pattern=r'^\d{4}-\d{2}$')
    period_end: str = Field(pattern=r'^\d{4}-\d{2}$')


class ExportArtifactCreate(BaseModel):
    scenario_id: int
    artifact_type: Literal['excel', 'pdf', 'email', 'bi_api', 'png', 'svg', 'pptx']
    file_name: str = Field(min_length=1, max_length=180)
    package_id: int | None = None
    report_definition_id: int | None = None
    chart_id: int | None = None
    retention_until: str | None = None


class ReportSnapshotCreate(BaseModel):
    scenario_id: int
    snapshot_type: Literal['board_package', 'report_export', 'bi_api', 'dashboard_chart']
    retention_until: str | None = None


class ScheduledExtractRunCreate(BaseModel):
    scenario_id: int
    export_id: int | None = None
    destination: str = Field(min_length=1, max_length=240)


class ReportLayoutCreate(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    layout_key: str | None = Field(default=None, max_length=120)
    scenario_id: int | None = None
    report_definition_id: int | None = None
    layout: dict[str, Any] = Field(default_factory=dict)


class ReportChartCreate(BaseModel):
    scenario_id: int
    name: str = Field(min_length=1, max_length=160)
    chart_key: str | None = Field(default=None, max_length=120)
    chart_type: Literal['bar', 'line', 'waterfall', 'pie', 'kpi'] = 'bar'
    dataset_type: Literal['financial_statement', 'variance', 'period_range', 'departmental_pl'] = 'period_range'
    config: dict[str, Any] = Field(default_factory=dict)


class ChartRenderCreate(BaseModel):
    render_format: Literal['png', 'svg'] = 'svg'
    file_name: str | None = Field(default=None, max_length=180)
    width: int = Field(default=960, ge=320, le=2400)
    height: int = Field(default=540, ge=220, le=1600)


class DashboardChartSnapshotCreate(BaseModel):
    scenario_id: int
    chart_id: int | None = None
    widget_id: int | None = None
    render_id: int | None = None
    snapshot_key: str | None = Field(default=None, max_length=120)


class ReportBookCreate(BaseModel):
    scenario_id: int
    name: str = Field(min_length=1, max_length=160)
    book_key: str | None = Field(default=None, max_length=120)
    layout_id: int | None = None
    period_start: str = Field(pattern=r'^\d{4}-\d{2}$')
    period_end: str = Field(pattern=r'^\d{4}-\d{2}$')
    report_definition_ids: list[int] = Field(default_factory=list)
    chart_ids: list[int] = Field(default_factory=list)


class ReportBurstRuleCreate(BaseModel):
    scenario_id: int
    book_id: int
    burst_key: str | None = Field(default=None, max_length=120)
    burst_dimension: Literal['department_code', 'fund_code', 'account_code'] = 'department_code'
    recipients: list[str] = Field(default_factory=list)
    export_format: Literal['pdf', 'excel', 'email'] = 'pdf'
    active: bool = True


class RecurringReportPackageCreate(BaseModel):
    scenario_id: int
    book_id: int
    package_key: str | None = Field(default=None, max_length=120)
    schedule_cron: str = Field(min_length=3, max_length=80)
    destination: str = Field(min_length=1, max_length=240)
    next_run_at: str | None = None


class ReportFootnoteCreate(BaseModel):
    scenario_id: int
    target_type: Literal['financial_statement', 'report_book', 'board_package', 'chart'] = 'financial_statement'
    target_id: int | None = None
    footnote_key: str | None = Field(default=None, max_length=120)
    marker: str = Field(default='1', min_length=1, max_length=20)
    footnote_text: str = Field(min_length=1, max_length=1000)
    display_order: int = Field(default=1, ge=1, le=1000)


class ReportPageBreakCreate(BaseModel):
    report_book_id: int
    section_key: str = Field(min_length=1, max_length=120)
    page_number: int = Field(default=1, ge=1, le=500)
    break_before: bool = True


class PdfPaginationProfileCreate(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    profile_key: str | None = Field(default=None, max_length=120)
    scenario_id: int | None = None
    page_size: Literal['Letter', 'A4', 'Legal'] = 'Letter'
    orientation: Literal['portrait', 'landscape'] = 'portrait'
    margin_top: float = Field(default=0.5, ge=0, le=3)
    margin_right: float = Field(default=0.5, ge=0, le=3)
    margin_bottom: float = Field(default=0.5, ge=0, le=3)
    margin_left: float = Field(default=0.5, ge=0, le=3)
    rows_per_page: int = Field(default=32, ge=5, le=120)


class ChartFormatCreate(BaseModel):
    format: dict[str, str | int | float | bool | list[str] | None] = Field(default_factory=dict)


class BoardPackageReleaseDecision(BaseModel):
    note: str = Field(default='', max_length=500)


class VarianceThresholdCreate(BaseModel):
    scenario_id: int
    threshold_key: str = Field(default='material-variance', min_length=1, max_length=80)
    amount_threshold: float = Field(default=10000, ge=0)
    percent_threshold: float | None = Field(default=None, ge=0)
    require_explanation: bool = True


class VarianceExplanationCreate(BaseModel):
    scenario_id: int
    variance_key: str = Field(min_length=1, max_length=160)
    explanation_text: str = Field(min_length=1, max_length=2000)


class NarrativeDraftCreate(BaseModel):
    scenario_id: int
    title: str = Field(default='Board Narrative', min_length=1, max_length=160)
    package_id: int | None = None


class NarrativeDecisionCreate(BaseModel):
    note: str = Field(default='', max_length=500)


class AIExplanationDecision(BaseModel):
    note: str = Field(default='', max_length=500)


class MarketWatchSymbolCreate(BaseModel):
    symbol: str = Field(min_length=1, max_length=16)


class PaperTradingAccountCreate(BaseModel):
    starting_cash: float = Field(default=100000, gt=0, le=10000000)


class PaperTradeCreate(BaseModel):
    symbol: str = Field(min_length=1, max_length=16)
    side: Literal['buy', 'sell']
    quantity: float = Field(gt=0, le=1000000)


class BrokerageConnectionCreate(BaseModel):
    provider_key: str = Field(min_length=1, max_length=80)
    connection_name: str = Field(min_length=1, max_length=160)
    credential_ref: str | None = Field(default=None, max_length=240)
    credential_type: Literal['', 'api_key', 'oauth_client', 'bearer_token', 'institutional_export'] = ''
    mode: Literal['sandbox', 'read_only', 'live'] = 'sandbox'
    provider_environment: Literal['sandbox', 'live'] = 'sandbox'
    read_only_ack: bool | None = None
    consent_status: Literal['not_requested', 'accepted'] | None = None
    trading_enabled: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class BrokerageCredentialSetupCreate(BaseModel):
    credential_ref: str | None = Field(default=None, max_length=240)
    credential_type: Literal['api_key', 'oauth_client', 'bearer_token', 'institutional_export'] = 'api_key'
    redirect_uri: str | None = Field(default=None, max_length=500)


class BrokerageConsentCreate(BaseModel):
    consent_version: str = Field(default='2026.04.b66', min_length=1, max_length=80)
    read_only_ack: bool = True
    real_money_trading_ack: bool = True
    data_scope_ack: bool = True
    consent_text: str = Field(default='I consent to read-only brokerage account access for holdings and balance sync. Real-money trading is disabled.', max_length=1000)


class CloseChecklistCreate(BaseModel):
    scenario_id: int
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    checklist_key: str = Field(min_length=1, max_length=80)
    title: str = Field(min_length=1, max_length=160)
    owner: str = Field(min_length=1, max_length=160)
    due_date: str | None = None


class CloseTaskTemplateCreate(BaseModel):
    template_key: str = Field(min_length=1, max_length=80)
    title: str = Field(min_length=1, max_length=160)
    owner_role: str = Field(min_length=1, max_length=120)
    due_day_offset: int = 0
    dependency_keys: list[str] = Field(default_factory=list)
    active: bool = True


class CloseTaskDependencyCreate(BaseModel):
    task_id: int
    depends_on_task_id: int


class PeriodCloseCalendarCreate(BaseModel):
    scenario_id: int
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    close_start: str
    close_due: str


class PeriodLockAction(BaseModel):
    lock_state: Literal['open', 'locked']


class CloseChecklistComplete(BaseModel):
    evidence: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class AccountReconciliationCreate(BaseModel):
    scenario_id: int
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    entity_code: str = Field(min_length=1, max_length=40)
    account_code: str = Field(min_length=1, max_length=40)
    source_balance: float
    owner: str = Field(min_length=1, max_length=160)
    notes: str = ''


class ReconciliationWorkflowAction(BaseModel):
    note: str = Field(default='', max_length=500)


class EntityConfirmationCreate(BaseModel):
    scenario_id: int
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    entity_code: str = Field(min_length=1, max_length=40)
    confirmation_type: Literal['balance', 'intercompany', 'subledger', 'management'] = 'balance'


class EntityConfirmationResponse(BaseModel):
    response: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class ConsolidationEntityCreate(BaseModel):
    entity_code: str = Field(min_length=1, max_length=40)
    entity_name: str = Field(min_length=1, max_length=160)
    parent_entity_code: str | None = Field(default=None, max_length=40)
    base_currency: str = Field(default='USD', min_length=3, max_length=3)
    gaap_basis: str = Field(default='US_GAAP', min_length=1, max_length=40)
    active: bool = True


class EntityOwnershipCreate(BaseModel):
    scenario_id: int
    parent_entity_code: str = Field(min_length=1, max_length=40)
    child_entity_code: str = Field(min_length=1, max_length=40)
    ownership_percent: float = Field(ge=0, le=100)
    effective_period: str = Field(pattern=r'^\d{4}-\d{2}$')


class ConsolidationSettingCreate(BaseModel):
    scenario_id: int
    gaap_basis: str = Field(default='US_GAAP', min_length=1, max_length=40)
    reporting_currency: str = Field(default='USD', min_length=3, max_length=3)
    translation_method: str = Field(default='placeholder', min_length=1, max_length=80)
    enabled: bool = True


class CurrencyRateCreate(BaseModel):
    scenario_id: int
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    from_currency: str = Field(min_length=3, max_length=3)
    to_currency: str = Field(min_length=3, max_length=3)
    rate: float = Field(gt=0)
    rate_type: Literal['closing', 'average', 'historical'] = 'closing'
    source: str = Field(default='manual', min_length=1, max_length=80)


class GaapBookMappingCreate(BaseModel):
    scenario_id: int
    source_gaap_basis: str = Field(min_length=1, max_length=40)
    target_gaap_basis: str = Field(min_length=1, max_length=40)
    source_account_code: str = Field(min_length=1, max_length=40)
    target_account_code: str = Field(min_length=1, max_length=40)
    adjustment_percent: float = Field(default=100)
    active: bool = True


class EliminationReviewAction(BaseModel):
    note: str = Field(default='', max_length=500)


class IntercompanyMatchCreate(BaseModel):
    scenario_id: int
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    source_entity_code: str = Field(min_length=1, max_length=40)
    target_entity_code: str = Field(min_length=1, max_length=40)
    account_code: str = Field(min_length=1, max_length=40)
    source_amount: float
    target_amount: float


class EliminationEntryCreate(BaseModel):
    scenario_id: int
    period: str = Field(pattern=r'^\d{4}-\d{2}$')
    entity_code: str = Field(min_length=1, max_length=40)
    account_code: str = Field(min_length=1, max_length=40)
    amount: float
    reason: str = Field(min_length=1, max_length=240)


class ConsolidationRunCreate(BaseModel):
    scenario_id: int
    period: str = Field(pattern=r'^\d{4}-\d{2}$')


class ConsolidationRuleCreate(BaseModel):
    scenario_id: int
    rule_key: str = Field(min_length=1, max_length=80)
    rule_type: Literal['elimination', 'ownership', 'currency', 'gaap', 'statutory_schedule'] = 'elimination'
    source_filter: dict[str, str | int | float | bool | None] = Field(default_factory=dict)
    action: dict[str, str | int | float | bool | None] = Field(default_factory=dict)
    priority: int = Field(default=100, ge=1, le=1000)
    active: bool = True


class StatutoryPackCreate(BaseModel):
    consolidation_run_id: int
    book_basis: str = Field(default='US_GAAP', min_length=1, max_length=40)
    reporting_currency: str = Field(default='USD', min_length=3, max_length=3)


class ConnectorCreate(BaseModel):
    connector_key: str = Field(min_length=1, max_length=80)
    name: str = Field(min_length=1, max_length=160)
    system_type: Literal['erp', 'sis', 'hr', 'payroll', 'grants', 'powerbi', 'file', 'banking', 'crm', 'brokerage'] = 'file'
    direction: Literal['inbound', 'outbound', 'bidirectional'] = 'inbound'
    config: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class ConnectorAuthFlowCreate(BaseModel):
    connector_key: str = Field(min_length=1, max_length=80)
    adapter_key: str = Field(min_length=1, max_length=80)
    credential_ref: str | None = Field(default=None, max_length=240)
    redirect_uri: str | None = Field(default=None, max_length=500)


class MappingPresetApplyCreate(BaseModel):
    connector_key: str = Field(min_length=1, max_length=80)
    preset_key: str = Field(min_length=1, max_length=120)
    template_key: str | None = Field(default=None, max_length=120)


class ImportBatchCreate(BaseModel):
    scenario_id: int
    connector_key: str = Field(min_length=1, max_length=80)
    source_format: Literal['csv', 'xlsx'] = 'csv'
    import_type: Literal['ledger', 'banking_cash', 'crm_enrollment'] = 'ledger'
    rows: list[dict[str, str | int | float | bool | None]] = Field(default_factory=list)
    source_name: str = Field(default='', max_length=180)
    stream_chunk_size: int = Field(default=1000, ge=1, le=10000)


class ImportStagingPreviewCreate(BaseModel):
    scenario_id: int
    connector_key: str = Field(min_length=1, max_length=80)
    source_format: Literal['csv', 'xlsx'] = 'csv'
    import_type: Literal['ledger', 'banking_cash', 'crm_enrollment'] = 'ledger'
    source_name: str = Field(default='', max_length=180)
    rows: list[dict[str, str | int | float | bool | None]] = Field(default_factory=list)
    stream_chunk_size: int = Field(default=1000, ge=1, le=10000)


class ImportStagingDecisionCreate(BaseModel):
    note: str = Field(default='', max_length=500)


class SyncJobCreate(BaseModel):
    connector_key: str = Field(min_length=1, max_length=80)
    job_type: Literal['erp_sync', 'sis_sync', 'hr_sync', 'payroll_sync', 'grants_sync', 'banking_sync', 'crm_sync', 'validation'] = 'erp_sync'


class ImportMappingTemplateCreate(BaseModel):
    template_key: str = Field(min_length=1, max_length=80)
    connector_key: str = Field(min_length=1, max_length=80)
    import_type: Literal['ledger', 'banking_cash', 'crm_enrollment'] = 'ledger'
    mapping: dict[str, str] = Field(default_factory=dict)
    active: bool = True


class ValidationRuleCreate(BaseModel):
    rule_key: str = Field(min_length=1, max_length=80)
    import_type: Literal['ledger', 'banking_cash', 'crm_enrollment'] = 'ledger'
    field_name: str = Field(min_length=1, max_length=80)
    operator: Literal['required', 'numeric', 'period', 'date', 'in'] = 'required'
    expected_value: str | None = Field(default=None, max_length=240)
    severity: Literal['error', 'warning'] = 'error'
    active: bool = True


class CredentialVaultCreate(BaseModel):
    connector_key: str = Field(min_length=1, max_length=80)
    credential_key: str = Field(min_length=1, max_length=80)
    secret_value: str = Field(min_length=1, max_length=500)
    secret_type: Literal['api_key', 'oauth_client', 'bearer_token', 'sftp_key'] = 'api_key'
    expires_at: str | None = Field(default=None, max_length=80)


class RetryEventCreate(BaseModel):
    connector_key: str = Field(min_length=1, max_length=80)
    operation_type: Literal['import', 'sync', 'export', 'credential_check'] = 'sync'
    error_message: str = Field(default='', max_length=500)
    attempts: int = Field(default=1, ge=0)


class UserProfileUpdate(BaseModel):
    display_name: str | None = Field(default=None, max_length=160)
    default_scenario_id: int | None = None
    default_period: str | None = Field(default=None, pattern=r'^\d{4}-\d{2}$')
    preferences: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class NotificationCreate(BaseModel):
    scenario_id: int | None = None
    user_id: int | None = None
    notification_type: Literal['review', 'validation', 'system', 'workflow', 'chat'] = 'system'
    title: str = Field(min_length=1, max_length=160)
    message: str = Field(min_length=1, max_length=500)
    severity: Literal['info', 'warning', 'error', 'success'] = 'info'
    link: str = Field(default='', max_length=240)


class ChatMessageCreate(BaseModel):
    recipient_user_id: int
    body: str = Field(min_length=1, max_length=2000)


class ChatReadRequest(BaseModel):
    peer_user_id: int | None = None


class GridValidationRequest(BaseModel):
    scenario_id: int
    rows: list[dict[str, str | int | float | bool | None]] = Field(default_factory=list)


class BulkPasteImportCreate(BaseModel):
    scenario_id: int
    paste_text: str = Field(min_length=1)


class ExcelTemplateImportCreate(BaseModel):
    scenario_id: int
    workbook_base64: str = Field(min_length=1)
    workbook_key: str | None = Field(default=None, max_length=120)
    sheet_name: str = Field(default='LedgerInput', max_length=80)


class OfficeCellCommentCreate(BaseModel):
    scenario_id: int
    workbook_key: str = Field(min_length=1, max_length=120)
    sheet_name: str = Field(default='LedgerInput', max_length=80)
    cell_ref: str = Field(default='E2', min_length=1, max_length=20)
    comment_text: str = Field(min_length=1, max_length=1000)


class PowerBIExportCreate(BaseModel):
    scenario_id: int
    dataset_name: str = Field(min_length=1, max_length=160)


class AutomationRunCreate(BaseModel):
    scenario_id: int
    assistant_type: Literal['variance', 'anomaly', 'budget', 'reconciliation']


class AutomationDecisionCreate(BaseModel):
    note: str = Field(default='', max_length=500)


class AIPlanningAgentRunCreate(BaseModel):
    scenario_id: int
    agent_type: Literal['budget_update', 'bulk_adjustment', 'report_question', 'anomaly_explanation']
    prompt_text: str = Field(min_length=1, max_length=2000)


class AIPlanningAgentDecision(BaseModel):
    note: str = Field(default='', max_length=500)


class UniversityAgentClientCreate(BaseModel):
    client_key: str = Field(min_length=1, max_length=120)
    display_name: str = Field(min_length=1, max_length=200)
    shared_secret: str = Field(min_length=8, max_length=500)
    scopes: list[str] = Field(default_factory=list)
    status: Literal['active', 'disabled'] = 'active'
    callback_url: str = Field(default='', max_length=500)


class UniversityAgentPolicyCreate(BaseModel):
    policy_key: str | None = Field(default=None, max_length=160)
    client_key: str = Field(min_length=1, max_length=120)
    tool_key: str = Field(min_length=1, max_length=120)
    allowed_actions: list[str] = Field(default_factory=list)
    max_amount: float | None = None
    status: Literal['active', 'disabled'] = 'active'


class OperationalCheckCreate(BaseModel):
    check_key: str = Field(min_length=1, max_length=80)
    category: Literal['health', 'deployment', 'backup', 'restore', 'security'] = 'health'


class RestoreTestCreate(BaseModel):
    backup_key: str = Field(min_length=1, max_length=120)


class RunbookRecordCreate(BaseModel):
    runbook_key: str = Field(min_length=1, max_length=80)
    title: str = Field(min_length=1, max_length=160)
    category: Literal['deployment', 'backup', 'restore', 'security', 'operations'] = 'operations'
    path: str = Field(min_length=1, max_length=260)
    status: Literal['draft', 'ready', 'needs_review'] = 'ready'


class ApplicationLogCreate(BaseModel):
    log_type: Literal['application', 'job', 'sync', 'admin', 'security'] = 'application'
    severity: Literal['debug', 'info', 'warning', 'error', 'critical'] = 'info'
    message: str = Field(min_length=1, max_length=500)
    correlation_id: str = Field(default='', max_length=120)
    detail: dict[str, Any] = Field(default_factory=dict)


class DeploymentEnvironmentSettingCreate(BaseModel):
    environment_key: Literal['local', 'test', 'staging', 'production'] = 'staging'
    tenant_key: str = Field(default='manchester', min_length=1, max_length=120)
    base_url: str = Field(default='http://localhost:3200', max_length=300)
    database_backend: Literal['sqlite', 'postgres', 'mssql'] = 'sqlite'
    sso_required: bool = False
    domain_guard_required: bool = False
    settings: dict[str, Any] = Field(default_factory=dict)
    status: Literal['draft', 'ready', 'locked'] = 'draft'


class DeploymentPromotionCreate(BaseModel):
    from_environment: str = Field(min_length=1, max_length=80)
    to_environment: str = Field(min_length=1, max_length=80)
    release_version: str = Field(min_length=1, max_length=80)
    checklist: dict[str, bool | str | int | float | None] = Field(default_factory=dict)


class ConfigSnapshotCreate(BaseModel):
    environment_key: str = Field(min_length=1, max_length=80)
    direction: Literal['export', 'import'] = 'export'
    payload: dict[str, Any] = Field(default_factory=dict)


class MigrationRollbackPlanCreate(BaseModel):
    migration_key: str = Field(min_length=1, max_length=120)
    rollback_strategy: str = Field(min_length=1, max_length=1000)
    verification_steps: list[str] = Field(default_factory=list)
    status: Literal['draft', 'reviewed', 'approved'] = 'draft'


class ReleaseNoteCreate(BaseModel):
    release_version: str = Field(min_length=1, max_length=80)
    title: str = Field(min_length=1, max_length=200)
    notes: dict[str, Any] = Field(default_factory=dict)
    status: Literal['draft', 'published'] = 'draft'


class ReadinessItemCreate(BaseModel):
    item_key: str = Field(min_length=1, max_length=120)
    category: Literal['security', 'database', 'backup', 'integration', 'reporting', 'operations'] = 'operations'
    title: str = Field(min_length=1, max_length=240)
    status: Literal['open', 'ready', 'blocked', 'waived'] = 'open'
    evidence: dict[str, Any] = Field(default_factory=dict)


class PerformanceLoadTestCreate(BaseModel):
    scenario_id: int | None = None
    test_type: Literal['postgres_load', 'large_import', 'calculation_benchmark'] = 'postgres_load'
    row_count: int = Field(default=5000, ge=1, le=250000)
    backend: Literal['sqlite', 'postgres', 'runtime'] = 'runtime'


class PerformanceBenchmarkRunCreate(BaseModel):
    scenario_id: int | None = None
    dataset_key: str = Field(default='campus-realistic-benchmark', min_length=1, max_length=120)
    row_count: int = Field(default=10000, ge=1, le=250000)
    backend: Literal['sqlite', 'postgres', 'runtime'] = 'runtime'
    thresholds: dict[str, int] = Field(default_factory=dict)
    include_import: bool = True
    include_reports: bool = True


class EnterpriseScaleBenchmarkRunCreate(BaseModel):
    run_key: str | None = Field(default=None, max_length=120)
    years: int = Field(default=5, ge=5, le=10)
    scenario_count: int = Field(default=6, ge=3, le=50)
    department_count: int = Field(default=40, ge=10, le=500)
    grant_count: int = Field(default=25, ge=5, le=500)
    employee_count: int = Field(default=500, ge=50, le=10000)
    account_count: int = Field(default=80, ge=20, le=1000)
    ledger_row_count: int = Field(default=12000, ge=1000, le=250000)
    benchmark_row_count: int = Field(default=10000, ge=1000, le=250000)
    backend: Literal['sqlite', 'postgres', 'runtime'] = 'runtime'
    thresholds: dict[str, int] = Field(default_factory=dict)


class IndexRecommendationCreate(BaseModel):
    recommendation_key: str = Field(min_length=1, max_length=120)
    table_name: str = Field(min_length=1, max_length=120)
    index_name: str = Field(min_length=1, max_length=160)
    columns: list[str] = Field(min_length=1)
    reason: str = Field(min_length=1, max_length=500)
    status: Literal['recommended', 'approved', 'implemented', 'rejected'] = 'recommended'


class BackgroundJobCreate(BaseModel):
    job_key: str | None = Field(default=None, max_length=120)
    job_type: Literal['cache_invalidation', 'backup_restore_test', 'calculation_benchmark', 'large_import_stress'] = 'cache_invalidation'
    priority: int = Field(default=100, ge=1, le=1000)
    max_attempts: int = Field(default=3, ge=1, le=10)
    backoff_seconds: int = Field(default=60, ge=1, le=86400)
    scheduled_for: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class CacheInvalidationCreate(BaseModel):
    cache_key: str = Field(min_length=1, max_length=200)
    scope: Literal['global', 'scenario', 'report', 'model', 'integration'] = 'scenario'
    reason: str = Field(min_length=1, max_length=500)


class RestoreAutomationCreate(BaseModel):
    backup_key: str = Field(min_length=1, max_length=120)
    verify_only: bool = True


class ParallelCubedRunCreate(BaseModel):
    scenario_id: int | None = None
    work_type: Literal['calculation', 'import', 'report', 'mixed'] = 'mixed'
    partition_strategy: Literal['balanced', 'department', 'account', 'period'] = 'balanced'
    max_workers: int | None = Field(default=None, ge=1, le=64)
    row_count: int = Field(default=5000, ge=1, le=100000)
    include_import: bool = True
    include_reports: bool = True


class ProductionDataCutoverRunCreate(BaseModel):
    run_key: str | None = Field(default=None, max_length=120)
    target_backend: Literal['runtime', 'sqlite', 'postgres', 'mssql'] = 'runtime'
    create_backup: bool = True
    run_restore_validation: bool = True
    apply_indexes: bool = True


class CampusDataValidationRunCreate(BaseModel):
    scenario_id: int | None = None
    run_key: str | None = Field(default=None, max_length=120)
    include_default_exports: bool = True
    exports: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)


class FPAWorkflowCertificationRunCreate(BaseModel):
    run_key: str | None = Field(default=None, max_length=120)
    scenario_id: int | None = None


class ForecastingAccuracyProofRunCreate(BaseModel):
    run_key: str | None = Field(default=None, max_length=120)
    scenario_id: int | None = None


class FinancialCloseCertificationRunCreate(BaseModel):
    run_key: str | None = Field(default=None, max_length=120)
    scenario_id: int | None = None
    period: str = Field(default='2026-08', pattern=r'^\d{4}-\d{2}$')


class ModelScenarioBranchCreate(BaseModel):
    source_scenario_id: int | None = None
    branch_key: str | None = Field(default=None, max_length=120)
    name: str | None = Field(default=None, max_length=160)
    version: str | None = Field(default=None, max_length=20)
    metadata: dict[str, Any] = Field(default_factory=dict)


class GuidanceTaskComplete(BaseModel):
    checklist_key: str = Field(min_length=1, max_length=120)
    task_key: str = Field(min_length=1, max_length=120)


class TrainingModeStart(BaseModel):
    mode_key: Literal['admin', 'planner', 'controller'] = 'planner'
    scenario_id: int | None = None


class WorkflowCreate(BaseModel):
    scenario_id: int
    name: str
    owner: str


class WorkflowAdvance(BaseModel):
    step: Literal['draft', 'review', 'approved', 'published']
    status: Literal['pending', 'active', 'blocked', 'done']


class WorkflowTemplateStepCreate(BaseModel):
    step_key: str = Field(min_length=1, max_length=80)
    label: str = Field(min_length=1, max_length=160)
    approver_role: str | None = Field(default=None, max_length=80)
    approver_user_id: int | None = None
    escalation_hours: float | None = Field(default=None, ge=0)
    escalation_user_id: int | None = None
    notification_template: str = Field(default='', max_length=500)


class WorkflowTemplateCreate(BaseModel):
    template_key: str | None = Field(default=None, max_length=120)
    name: str = Field(min_length=1, max_length=160)
    entity_type: str = Field(min_length=1, max_length=80)
    active: bool = True
    steps: list[WorkflowTemplateStepCreate] = Field(default_factory=list)


class WorkflowInstanceCreate(BaseModel):
    template_id: int
    scenario_id: int
    subject_type: str = Field(min_length=1, max_length=80)
    subject_id: str = Field(min_length=1, max_length=120)


class WorkflowTaskDecision(BaseModel):
    decision: Literal['approved', 'rejected']
    note: str = Field(default='', max_length=500)


class WorkflowDelegationCreate(BaseModel):
    from_user_id: int
    to_user_id: int
    starts_at: str
    ends_at: str
    reason: str = Field(default='', max_length=500)
    active: bool = True


class WorkflowVisualDesignCreate(BaseModel):
    template_id: int
    layout: dict[str, Any] = Field(default_factory=dict)


class ProcessCalendarCreate(BaseModel):
    scenario_id: int
    calendar_key: str = Field(min_length=1, max_length=120)
    process_type: Literal['close', 'budget', 'forecast', 'certification'] = 'close'
    period: str = Field(min_length=1, max_length=40)
    milestones: list[dict[str, Any]] = Field(default_factory=list)
    status: Literal['planned', 'active', 'complete'] = 'planned'


class WorkflowSubstituteApproverCreate(BaseModel):
    original_user_id: int
    substitute_user_id: int
    process_type: str = Field(default='all', max_length=80)
    starts_at: str
    ends_at: str
    active: bool = True


class WorkflowCertificationPacketCreate(BaseModel):
    scenario_id: int
    process_type: Literal['close', 'budget', 'forecast', 'certification'] = 'close'
    period: str = Field(min_length=1, max_length=40)
    packet_key: str | None = Field(default=None, max_length=120)


class ProcessCampaignMonitorCreate(BaseModel):
    scenario_id: int
    process_type: Literal['close', 'budget'] = 'close'
    period: str = Field(min_length=1, max_length=40)
    campaign_key: str | None = Field(default=None, max_length=120)


class DriverOut(BaseModel):
    driver_key: str
    label: str
    expression: str | None = None
    value: float | None = None
    unit: str


class ForecastRunResult(BaseModel):
    scenario_id: int
    resolved_drivers: dict[str, float]
    created_line_items: list[PlanLineItemOut]


class SummaryReport(BaseModel):
    scenario_id: int
    revenue_total: float
    expense_total: float
    net_total: float
    by_department: dict[str, float]
    by_account: dict[str, float]


class AuditLogOut(BaseModel):
    id: int
    entity_type: str
    entity_id: str
    action: str
    actor: str
    detail_json: str
    created_at: str


class IntegrationOut(BaseModel):
    id: int
    name: str
    category: str
    status: str
    direction: str
    endpoint_hint: str
