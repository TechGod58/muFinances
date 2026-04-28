from __future__ import annotations

import json
import os
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app import db
from app.routers import auth, health
from app.schemas import (
    AuditLogOut,
    ApprovalAction,
    ActualsIngestCreate,
    ADOUGroupMappingCreate,
    AIExplanationDecision,
    AIPlanningAgentDecision,
    AIPlanningAgentRunCreate,
    AdminImpersonationCreate,
    AutomationDecisionCreate,
    AutomationRunCreate,
    ApplicationLogCreate,
    BackgroundJobCreate,
    ComplianceCertificationCreate,
    ComplianceCertificationDecision,
    BackupCreate,
    BoardPackageCreate,
    BoardPackageReleaseDecision,
    BrokerageConnectionCreate,
    BrokerageConsentCreate,
    BrokerageCredentialSetupCreate,
    BulkPasteImportCreate,
    BudgetAssumptionCreate,
    BudgetSubmissionCreate,
    BudgetTransferCreate,
    CapitalRequestCreate,
    CacheInvalidationCreate,
    ChartFormatCreate,
    ChartRenderCreate,
    ChatMessageCreate,
    ChatReadRequest,
    AccountReconciliationCreate,
    CloseChecklistComplete,
    CloseChecklistCreate,
    CloseTaskDependencyCreate,
    CloseTaskTemplateCreate,
    ConsolidationCertificationRunCreate,
    ConsolidationRunCreate,
    ConsolidationRuleCreate,
    ConsolidationEntityCreate,
    ConsolidationSettingCreate,
    ConnectorCreate,
    ConnectorAuthFlowCreate,
    ConfigSnapshotCreate,
    CredentialVaultCreate,
    CurrencyRateCreate,
    DashboardChartSnapshotCreate,
    DashboardWidgetCreate,
    DeploymentEnvironmentSettingCreate,
    DeploymentPromotionCreate,
    DimensionMemberCreate,
    DomainVPNCheckCreate,
    DriverOut,
    DriverDefinitionCreate,
    EliminationEntryCreate,
    EliminationReviewAction,
    EnrollmentForecastInputCreate,
    EnrollmentTermCreate,
    EnterpriseScaleBenchmarkRunCreate,
    EntityCommentCreate,
    EvidenceAttachmentCreate,
    ExcelTemplateImportCreate,
    FacultyLoadCreate,
    FiscalPeriodCreate,
    Form990SupportFieldCreate,
    ForecastRunResult,
    ForecastBacktestCreate,
    ForecastingAccuracyProofRunCreate,
    ForecastRecommendationCompareCreate,
    ForecastTuningProfileCreate,
    FinancialCloseCertificationRunCreate,
    FPAWorkflowCertificationRunCreate,
    FormulaLintRequest,
    GaapBookMappingCreate,
    GridValidationRequest,
    GrantBudgetCreate,
    GuidanceTaskComplete,
    IntegrationOut,
    IndexRecommendationCreate,
    IntercompanyMatchCreate,
    ImportBatchCreate,
    ImportMappingTemplateCreate,
    ImportStagingDecisionCreate,
    ImportStagingPreviewCreate,
    EntityConfirmationCreate,
    EntityConfirmationResponse,
    EntityOwnershipCreate,
    JournalAdjustmentCreate,
    LedgerEntryCreate,
    LedgerReverseCreate,
    AllocationRuleCreate,
    MarketWatchSymbolCreate,
    MappingPresetApplyCreate,
    MasterDataChangeCreate,
    MasterDataMappingCreate,
    MetadataApprovalCreate,
    MigrationRollbackPlanCreate,
    ModelFormulaCreate,
    ModelScenarioBranchCreate,
    OperatingBudgetLineCreate,
    OperationalCheckCreate,
    OfficeCellCommentCreate,
    PeriodCloseCalendarCreate,
    PeriodLockAction,
    PaperTradeCreate,
    PaperTradingAccountCreate,
    ParallelCubedRunCreate,
    PerformanceBenchmarkRunCreate,
    PerformanceLoadTestCreate,
    PlanLineItemCreate,
    PlanLineItemOut,
    ProductionDataCutoverRunCreate,
    CampusDataValidationRunCreate,
    RealConnectorActivationRunCreate,
    ExportArtifactCreate,
    NarrativeDecisionCreate,
    NarrativeDraftCreate,
    NotificationCreate,
    ReadinessItemCreate,
    RecurringReportPackageCreate,
    ReleaseNoteCreate,
    ReportFootnoteCreate,
    ReportPageBreakCreate,
    PdfPaginationProfileCreate,
    ReportSnapshotCreate,
    ReportBookCreate,
    ReportBurstRuleCreate,
    ReportChartCreate,
    ReportLayoutCreate,
    ReportingPixelPolishRunCreate,
    SecurityActivationCertificationRunCreate,
    VarianceExplanationCreate,
    VarianceThresholdCreate,
    ScenarioCreate,
    ScenarioCloneCreate,
    TaxActivityClassificationCreate,
    TaxAlertDecision,
    TaxReviewDecision,
    TaxRuleSourceCreate,
    TaxUpdateCheckCreate,
    ScenarioMergeCreate,
    ScenarioOut,
    SummaryReport,
    SSOProductionSettingCreate,
    SoDPolicyCreate,
    ForecastRunCreate,
    PredictiveModelChoiceCreate,
    ReportDefinitionCreate,
    RetentionPolicyCreate,
    RestoreAutomationCreate,
    RestoreTestCreate,
    RunbookRecordCreate,
    ReconciliationWorkflowAction,
    ScheduledExtractRunCreate,
    ScheduledExportCreate,
    PowerBIExportCreate,
    PlanningModelCreate,
    ProfitabilityAllocationRunCreate,
    ProfitabilityCostPoolCreate,
    StatutoryPackCreate,
    TrainingModeStart,
    TuitionForecastRunCreate,
    TuitionRateCreate,
    TypedDriverCreate,
    UniversityAgentClientCreate,
    UniversityAgentPolicyCreate,
    UserCreate,
    UserAccessReviewCreate,
    UserAccessReviewDecision,
    UserDimensionAccessCreate,
    UserProfileUpdate,
    WorkforcePositionCreate,
    WorkflowAdvance,
    WorkflowCertificationPacketCreate,
    WorkflowCreate,
    WorkflowDelegationCreate,
    WorkflowInstanceCreate,
    ProcessCalendarCreate,
    ProcessCampaignMonitorCreate,
    WorkflowSubstituteApproverCreate,
    WorkflowTaskDecision,
    WorkflowTemplateCreate,
    WorkflowVisualDesignCreate,
    SyncJobCreate,
    RetryEventCreate,
    ValidationRuleCreate,
)
from app.services.forecast_engine import run_forecast
from app.services.enrollment import (
    list_forecast_inputs,
    list_runs,
    list_terms,
    list_tuition_rates,
    run_tuition_forecast,
    status as enrollment_status,
    upsert_forecast_input,
    upsert_term,
    upsert_tuition_rate,
)
from app.services.campus_planning import (
    approve_capital_request,
    list_capital_requests,
    list_faculty_loads,
    list_grant_budgets,
    list_positions,
    status as campus_planning_status,
    upsert_capital_request,
    upsert_faculty_load,
    upsert_grant_budget,
    upsert_position,
)
from app.services.scenario_engine import (
    calculate_forecast_actual_variance,
    clone_scenario,
    compare_forecast_recommendations,
    compare_scenarios,
    driver_dependency_graph,
    explain_forecast_drivers,
    ingest_actuals,
    list_forecast_backtests,
    list_forecast_driver_explanations,
    list_forecast_runs as list_scenario_forecast_runs,
    list_forecast_actual_variances,
    list_forecast_recommendation_comparisons,
    list_forecast_tuning_profiles,
    list_lineage,
    list_methods,
    list_predictive_model_choices,
    list_typed_drivers,
    predictive_status as scenario_engine_predictive_status,
    predictive_workspace,
    run_forecast_backtest,
    run_forecast as run_scenario_forecast,
    status as scenario_engine_status,
    upsert_forecast_tuning_profile,
    upsert_predictive_model_choice,
    upsert_driver_definition,
    upsert_typed_driver,
)
from app.services.model_builder import (
    build_cube as build_enterprise_cube,
    calculation_order as model_calculation_order,
    create_model_scenario_branch,
    deepening_status as model_deepening_status,
    dependency_graph as model_dependency_graph,
    enterprise_status as enterprise_modeling_status,
    enterprise_workspace as enterprise_modeling_workspace,
    invalidate_dependencies as invalidate_model_dependencies,
    list_allocation_rules,
    list_cube_optimization_profiles,
    list_dependency_invalidations as list_model_invalidations,
    list_formulas as list_model_formulas,
    list_models,
    list_model_versions,
    list_performance_tests as list_model_performance_tests,
    list_model_scenario_branches,
    optimize_cube_strategy,
    list_recalculation_runs,
    publish_model_version,
    recalculate_model,
    run_deepening_proof as run_model_deepening_proof,
    run_performance_test as run_model_performance_test,
    status as model_builder_status,
    upsert_allocation_rule,
    upsert_formula,
    upsert_model,
)
from app.services.formula_engine import evaluate_formula, lint_formula
from app.services.reporting import (
    account_rollups,
    actual_budget_forecast_variance,
    approve_narrative_report,
    approve_variance_explanation,
    approve_board_package_release,
    assemble_board_package,
    assemble_narrative_report,
    balance_sheet,
    cash_flow_statement,
    bi_api_manifest,
    chart_rendering_status,
    chart_rendering_workspace,
    create_dashboard_chart_snapshot,
    create_export_artifact,
    create_export,
    create_report_definition,
    create_report_snapshot,
    create_widget,
    create_burst_rule,
    create_report_chart,
    create_recurring_report_package,
    apply_chart_format,
    designer_distribution_status,
    departmental_pl,
    draft_variance_narratives,
    financial_statement,
    fund_report,
    generate_required_variance_explanations,
    grant_report,
    list_chart_renders,
    list_dashboard_chart_snapshots,
    get_export_artifact,
    list_export_artifacts,
    list_export_artifact_validations,
    list_board_packages,
    list_exports,
    list_burst_rules,
    list_report_definitions,
    list_report_books,
    list_report_charts,
    list_report_layouts,
    list_report_snapshots,
    list_recurring_report_package_runs,
    list_recurring_report_packages,
    list_board_package_release_reviews,
    list_page_breaks,
    list_pagination_profiles,
    list_report_footnotes,
    list_scheduled_extract_runs,
    list_narrative_reports,
    list_variance_explanations,
    list_variance_thresholds,
    list_widgets,
    period_range_report,
    reject_variance_explanation,
    pixel_financial_statement,
    production_reporting_status,
    production_reporting_workspace,
    production_pdf_status,
    production_pdf_workspace,
    reporting_output_completion_status,
    render_chart,
    run_reporting_output_completion,
    run_recurring_report_package,
    release_board_package,
    request_board_package_release,
    run_scheduled_extract,
    run_report,
    assemble_report_book,
    save_report_layout,
    upsert_page_break,
    upsert_pagination_profile,
    upsert_report_footnote,
    status as reporting_status,
    submit_variance_explanation,
    update_variance_explanation,
    upsert_variance_threshold,
    validate_export_artifact,
    variance_report,
)
from app.services.fpa_workflow_certification import (
    list_runs as list_fpa_workflow_certification_runs,
    run_certification as run_fpa_workflow_certification,
    status as fpa_workflow_certification_status,
)
from app.services.consolidation_certification import (
    list_runs as list_consolidation_certification_runs,
    run_certification as run_consolidation_certification,
    status as consolidation_certification_status,
)
from app.services.forecasting_accuracy_proof import (
    list_runs as list_forecasting_accuracy_proof_runs,
    run_proof as run_forecasting_accuracy_proof,
    status as forecasting_accuracy_proof_status,
)
from app.services.reporting_pixel_polish_certification import (
    list_runs as list_reporting_pixel_polish_runs,
    run_certification as run_reporting_pixel_polish,
    status as reporting_pixel_polish_status,
)
from app.services.profitability import (
    before_after_allocation_comparison,
    create_snapshot as create_profitability_snapshot,
    fund_profitability_report,
    grant_profitability_report,
    list_allocation_runs as list_profitability_allocation_runs,
    list_cost_pools as list_profitability_cost_pools,
    list_snapshots as list_profitability_snapshots,
    list_trace_lines as list_profitability_trace_lines,
    program_margin_report,
    run_service_center_allocation,
    status as profitability_status,
    upsert_cost_pool as upsert_profitability_cost_pool,
    workspace as profitability_workspace,
)
from app.services.close_consolidation import (
    approve_elimination,
    approve_reconciliation,
    advanced_consolidation_status,
    assemble_statutory_pack,
    confirm_entity,
    complete_checklist_item,
    create_consolidation_audit_report,
    create_close_task_template,
    create_checklist_item,
    create_elimination,
    create_entity_confirmation,
    create_intercompany_match,
    create_reconciliation,
    create_task_dependency,
    instantiate_close_templates,
    list_audit_packets,
    list_checklist_items,
    list_close_task_templates,
    list_consolidation_audit_reports,
    list_consolidation_entities,
    list_consolidation_journals,
    list_consolidation_runs,
    list_consolidation_rules,
    list_consolidation_settings,
    list_currency_translation_adjustments,
    list_currency_rates,
    list_eliminations,
    list_entity_confirmations,
    list_entity_ownerships,
    list_gaap_book_mappings,
    list_ownership_chain_calculations,
    list_intercompany_matches,
    list_period_close_calendar,
    list_reconciliations,
    list_reconciliation_exceptions,
    list_statutory_packs,
    list_supplemental_schedules,
    list_task_dependencies,
    reject_reconciliation,
    reject_elimination,
    run_consolidation,
    run_financial_correctness_depth,
    set_period_lock,
    status as close_consolidation_status,
    submit_reconciliation,
    submit_elimination,
    upsert_consolidation_entity,
    upsert_consolidation_rule,
    upsert_consolidation_setting,
    upsert_currency_rate,
    upsert_entity_ownership,
    upsert_gaap_book_mapping,
    upsert_period_close_calendar,
)
from app.services.financial_close_certification import (
    list_runs as list_financial_close_certification_runs,
    run_certification as run_financial_close_certification,
    status as financial_close_certification_status,
)
from app.services.campus_integrations import (
    adapter_contracts as list_adapter_contracts,
    approve_staging_batch,
    apply_mapping_preset,
    connector_health_dashboard,
    create_staging_preview,
    create_powerbi_export,
    create_retry_event,
    get_source_drillback,
    get_staging_batch,
    list_adapters,
    list_auth_flows,
    list_banking_cash_imports,
    list_connectors,
    list_credentials,
    list_crm_enrollment_imports,
    list_import_batches,
    list_mapping_presets,
    list_mapping_templates,
    list_powerbi_exports,
    list_rejections,
    list_retry_events,
    list_source_drillbacks,
    list_staging_batches,
    list_staging_rows,
    list_sync_logs,
    list_sync_jobs,
    list_validation_rules,
    marketplace_status,
    marketplace_workspace,
    production_status as connector_production_status,
    reject_staging_row,
    run_real_connector_proof,
    run_health_check,
    run_import,
    run_sync_job,
    staging_drillback,
    staging_status,
    start_auth_flow,
    status as campus_integrations_status,
    store_credential,
    upsert_connector,
    upsert_mapping_template,
    upsert_validation_rule,
    validate_source_drillback,
)
from app.services.real_connector_activation import (
    list_runs as list_real_connector_activation_runs,
    run_activation as run_real_connector_activation,
    status as real_connector_activation_status,
)
from app.services.governed_automation import (
    ai_guardrails_status,
    approve_agent_action,
    approve_recommendation,
    list_agent_actions,
    list_agent_prompts,
    list_approval_gates,
    list_recommendations,
    planning_agents_status,
    reject_recommendation,
    reject_agent_action,
    run_ai_guardrails_proof,
    run_planning_agent,
    run_assistant,
    status as governed_automation_status,
)
from app.services.ai_explainability import (
    approve_explanation as approve_ai_explanation,
    draft_variance_explanations as draft_ai_variance_explanations,
    list_explanations as list_ai_explanations,
    reject_explanation as reject_ai_explanation,
    status as ai_explainability_status,
    submit_explanation as submit_ai_explanation,
)
from app.services.market_lab import (
    add_watchlist_symbol,
    ensure_account as ensure_paper_account,
    market_lab,
    place_trade,
    quote as market_quote,
    reset_account as reset_paper_account,
    search_symbols,
    status as market_lab_status,
)
from app.services.brokerage_connectors import (
    brokerage_audit_trail,
    brokerage_workspace,
    create_connection as create_brokerage_connection,
    list_consents as list_brokerage_consents,
    list_accounts as list_brokerage_accounts,
    list_connections as list_brokerage_connections,
    list_holdings as list_brokerage_holdings,
    list_sync_runs as list_brokerage_sync_runs,
    provider_catalog as brokerage_provider_catalog,
    provider_readiness_status as brokerage_provider_readiness_status,
    record_consent as record_brokerage_consent,
    setup_credentials as setup_brokerage_credentials,
    status as brokerage_status,
    sync_connection as sync_brokerage_connection,
    test_connection as test_brokerage_connection,
)
from app.services.data_hub import (
    approve_dimension_change,
    approve_metadata,
    build_lineage as build_data_lineage,
    list_change_requests as list_master_data_changes,
    list_lineage_records as list_data_lineage_records,
    list_mappings as list_master_data_mappings,
    list_metadata_approvals,
    request_dimension_change,
    request_metadata_approval,
    status as data_hub_status,
    upsert_mapping as upsert_master_data_mapping,
    workspace as data_hub_workspace,
)
from app.services.workspaces import role_workspaces, status as workspace_status
from app.services.workflow_designer import (
    assemble_certification_packet as assemble_workflow_certification_packet,
    create_delegation as create_workflow_delegation,
    create_substitute_approver,
    create_template as create_workflow_template,
    decide_task as decide_workflow_task,
    list_campaign_monitors,
    list_certification_packets as list_workflow_certification_packets,
    list_delegations as list_workflow_delegations,
    list_escalation_events as list_workflow_escalation_events,
    list_instances as list_workflow_instances,
    list_process_calendars,
    list_substitute_approvers,
    list_tasks as list_workflow_tasks,
    list_templates as list_workflow_templates,
    list_visual_designs,
    monitor_campaign,
    run_escalations as run_workflow_escalations,
    start_instance as start_workflow_instance,
    status as workflow_designer_status,
    upsert_process_calendar,
    upsert_visual_design,
    workspace as workflow_orchestration_workspace,
)
from app.services.ux_productivity import (
    bulk_paste_budget,
    create_notification,
    department_comparison,
    get_profile,
    list_bulk_imports,
    list_notifications,
    mark_notification_read,
    missing_submissions,
    productivity_bootstrap,
    status as ux_productivity_status,
    update_profile,
    validate_grid_rows,
)
from app.services.chat import (
    chat_summary,
    list_chat_users,
    list_messages as list_chat_messages,
    mark_messages_read,
    send_message,
)
from app.services.accessibility_testing import status as accessibility_testing_status
from app.services.production_operations import (
    admin_audit_report,
    documentation_readiness as production_documentation_readiness,
    ensure_production_ops_ready,
    guides_manifest,
    list_application_logs,
    record_application_log,
    status as production_operations_status,
)
from app.services.compliance import (
    certify as certify_compliance_control,
    create_certification,
    ensure_compliance_ready,
    list_certifications,
    list_retention_policies,
    retention_review,
    seal_audit_backlog,
    sod_report,
    status as compliance_status,
    upsert_retention_policy,
    verify_audit_chain,
)
from app.services.tax_compliance import (
    classify_activity as classify_tax_activity,
    classification_summary as tax_classification_summary,
    decide_tax_alert,
    ensure_tax_compliance_ready,
    list_classifications as list_tax_classifications,
    list_form990_support_fields,
    list_reviews as list_tax_reviews,
    list_rule_sources as list_tax_rule_sources,
    list_tax_alerts,
    list_update_checks as list_tax_update_checks,
    review_classification as review_tax_classification,
    run_due_update_checks as run_due_tax_update_checks,
    run_tax_update_check,
    status as tax_compliance_status,
    upsert_form990_support,
    upsert_rule_source as upsert_tax_rule_source,
    workspace as tax_compliance_workspace,
)
from app.services.postgres_runtime import status as postgres_runtime_status
from app.services.migration_runner import (
    dry_run as dry_run_managed_migrations,
    recent_runs as recent_migration_runs,
    rollback_plan as managed_migration_rollback_plan,
    run_pending as run_managed_migrations,
    status as migration_framework_status,
)
from app.services.guided_entry import status as guided_entry_status
from app.services.guidance_training import (
    complete_task as complete_guidance_task,
    start_training_mode,
    status as guidance_training_status,
    workspace as guidance_training_workspace,
)
from app.services.university_agent import (
    handle_signed_request as handle_university_agent_signed_request,
    list_audit_logs as list_university_agent_audit_logs,
    list_callbacks as list_university_agent_callbacks,
    list_clients as list_university_agent_clients,
    list_policies as list_university_agent_policies,
    list_requests as list_university_agent_requests,
    list_tools as list_university_agent_tools,
    status as university_agent_status,
    upsert_client as upsert_university_agent_client,
    upsert_policy as upsert_university_agent_policy,
    workspace as university_agent_workspace,
)
from app.services.office_interop import (
    add_cell_comment as add_office_cell_comment,
    adoption_status as office_adoption_status,
    create_excel_template,
    create_workbook_package,
    excel_certification_status,
    list_excel_certification_runs,
    list_cell_comments as list_office_cell_comments,
    list_named_ranges as list_office_named_ranges,
    import_excel_template,
    list_office_workbooks,
    list_roundtrip_imports,
    list_workspace_actions as list_office_workspace_actions,
    native_workspace as office_native_workspace,
    native_workspace_status as office_native_status,
    publish_workbook as publish_office_workbook,
    refresh_powerpoint_deck,
    refresh_workbook as refresh_office_workbook,
    run_excel_adoption_certification,
    run_office_adoption_proof,
    status as office_interop_status,
)
from app.services.deployment_operations import (
    create_operations_backup,
    list_operational_checks,
    list_restore_tests,
    list_runbooks,
    operations_summary,
    run_operational_check,
    run_restore_test,
    status as deployment_operations_status,
    upsert_runbook,
)
from app.services.production_data_platform import (
    list_cutover_runs as list_production_data_cutover_runs,
    run_cutover_rehearsal as run_production_data_cutover_rehearsal,
    status as production_data_platform_status,
)
from app.services.deployment_governance import (
    create_config_snapshot as create_deployment_config_snapshot,
    create_promotion as create_deployment_promotion,
    list_config_snapshots as list_deployment_config_snapshots,
    list_diagnostics as list_deployment_diagnostics,
    list_environments as list_deployment_environments,
    list_promotions as list_deployment_promotions,
    list_readiness_items as list_deployment_readiness_items,
    list_release_notes as list_deployment_release_notes,
    list_rollback_plans as list_deployment_rollback_plans,
    run_admin_diagnostics as run_deployment_admin_diagnostics,
    status as deployment_governance_status,
    upsert_environment as upsert_deployment_environment,
    upsert_readiness_item as upsert_deployment_readiness_item,
    upsert_release_note as upsert_deployment_release_note,
    upsert_rollback_plan as upsert_deployment_rollback_plan,
    workspace as deployment_governance_workspace,
)
from app.services.performance_reliability import (
    benchmark_status as performance_benchmark_status,
    cancel_job as cancel_performance_job,
    enqueue_job as enqueue_performance_job,
    get_benchmark_run as get_performance_benchmark_run,
    invalidate_cache as invalidate_performance_cache,
    list_background_jobs as list_performance_jobs,
    list_benchmark_runs as list_performance_benchmark_runs,
    list_cache_invalidations as list_performance_cache_invalidations,
    list_dead_letters as list_performance_dead_letters,
    list_index_recommendations as list_performance_index_recommendations,
    list_job_logs as list_performance_job_logs,
    list_load_tests as list_performance_load_tests,
    list_restore_automations as list_performance_restore_automations,
    performance_proof_status,
    promote_due_jobs as promote_performance_jobs,
    run_benchmark_harness as run_performance_benchmark_harness,
    run_load_test as run_performance_load_test,
    run_next_job as run_performance_next_job,
    run_performance_proof,
    run_restore_automation as run_performance_restore_automation,
    seed_index_strategy as seed_performance_index_strategy,
    status as performance_reliability_status,
    upsert_index_recommendation as upsert_performance_index_recommendation,
    workspace as performance_reliability_workspace,
)
from app.services.enterprise_scale_benchmark import (
    get_run as get_enterprise_scale_benchmark_run,
    list_runs as list_enterprise_scale_benchmark_runs,
    run_enterprise_scale_benchmark,
    status as enterprise_scale_benchmark_status,
)
from app.services.campus_data_validation import (
    list_validation_runs as list_campus_data_validation_runs,
    run_validation as run_campus_data_validation,
    status as campus_data_validation_status,
)
from app.services.parallel_cubed_engine import (
    cpu_topology as parallel_cubed_cpu_topology,
    get_run as get_parallel_cubed_run,
    list_partitions as list_parallel_cubed_partitions,
    list_runs as list_parallel_cubed_runs,
    run_parallel_engine as run_parallel_cubed_engine,
    status as parallel_cubed_engine_status,
    workspace as parallel_cubed_engine_workspace,
)
from app.services.observability_operations import (
    acknowledge_alert as acknowledge_observability_alert,
    list_alerts as list_observability_alerts,
    list_backup_restore_drills as list_observability_backup_restore_drills,
    list_health_probes as list_observability_health_probes,
    list_metrics as list_observability_metrics,
    run_observability_evidence,
    run_backup_restore_drill as run_observability_backup_restore_drill,
    run_health_probes as run_observability_health_probes,
    status as observability_status,
    workspace as observability_workspace,
)
from app.services.ledger_depth import (
    approve_journal_adjustment,
    approve_scenario,
    create_journal_adjustment,
    ledger_basis_summary,
    list_journal_adjustments,
    lock_scenario,
    merge_approved_changes,
    publish_scenario,
    reject_journal_adjustment,
    status as ledger_depth_status,
    submit_journal_adjustment,
    unlock_scenario,
)
from app.services.evidence import (
    create_attachment,
    create_comment,
    entity_evidence,
    list_attachments,
    list_comments,
    resolve_comment,
    status as evidence_status,
)
from app.services.foundation import (
    append_ledger_entry,
    create_backup,
    create_dimension_member,
    dimension_hierarchy,
    ensure_foundation_ready,
    foundation_status,
    list_backups,
    list_fiscal_periods,
    list_ledger_entries,
    list_migrations,
    reverse_ledger_entry,
    restore_backup,
    set_period_closed,
    summary_by_dimensions,
    upsert_fiscal_period,
)
from app.services.parallel_cubed import batches_as_dicts, finance_flow
from app.services.seed import seed_if_empty
from app.services.operating_budget import (
    add_budget_line,
    approve_submission,
    approve_transfer,
    create_assumption,
    create_submission,
    list_assumptions,
    list_submissions,
    list_transfers,
    reject_submission,
    request_transfer,
    status as operating_budget_status,
    submit_submission,
)
from app.services.security import (
    activate_security_controls,
    certify_access_review,
    create_user,
    assert_production_security_ready,
    create_access_review,
    end_impersonation,
    ensure_security_ready,
    enterprise_admin_status,
    enterprise_admin_workspace,
    grant_dimension_access,
    list_access_reviews,
    list_ad_ou_group_mappings,
    list_domain_vpn_checks,
    list_impersonation_sessions,
    list_sso_production_settings,
    list_users as list_security_users,
    record_domain_vpn_check,
    require_permission,
    security_status,
    start_impersonation,
    trusted_header_login,
    upsert_ad_ou_group_mapping,
    upsert_sod_policy,
    upsert_sso_production_setting,
    user_from_token,
)
from app.services.security_activation_certification import (
    list_runs as list_security_activation_certification_runs,
    run_certification as run_security_activation_certification,
    status as security_activation_certification_status,
)
from app.services.access_guard import (
    access_guard_status,
    assert_ad_ou_allowed,
    assert_network_request_allowed,
)

app = FastAPI(title='Campus FPM Base', version='0.1.0')
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

STATIC_DIR = Path(__file__).resolve().parent.parent / 'static'


def init_application() -> None:
    assert_production_security_ready()
    db.init_db()
    ensure_foundation_ready()
    ensure_security_ready()
    ensure_production_ops_ready()
    ensure_compliance_ready()
    ensure_tax_compliance_ready()
    seed_if_empty()


init_application()
app.include_router(health.router)
app.include_router(auth.router)

PUBLIC_API_PATHS = {
    '/api/health',
    '/api/health/live',
    '/api/health/ready',
    '/api/auth/login',
    '/api/auth/bootstrap',
    '/api/auth/sso/config',
    '/api/auth/sso/login',
    '/api/auth/sso/callback',
    '/api/university-agent/requests',
}

PASSWORD_CHANGE_ALLOWED_PATHS = {
    '/api/auth/me',
    '/api/auth/password',
}


@app.middleware('http')
async def require_api_auth(request: Request, call_next):
    trace_id = request.headers.get('x-trace-id') or request.headers.get('x-request-id') or uuid.uuid4().hex
    request.state.trace_id = trace_id
    started = time.perf_counter()
    try:
        assert_network_request_allowed(request)
    except PermissionError as exc:
        response = JSONResponse(status_code=403, content={'detail': str(exc), 'trace_id': trace_id})
        response.headers['X-Trace-Id'] = trace_id
        return _with_security_headers(response)

    path = request.url.path
    response = None
    try:
        if path.startswith('/api') and path not in PUBLIC_API_PATHS:
            auth_header = request.headers.get('authorization', '')
            scheme, _, token = auth_header.partition(' ')
            if scheme.lower() != 'bearer' or not token:
                sso_email = request.headers.get('x-mufinances-sso-email')
                trusted_result = trusted_header_login(sso_email) if sso_email else None
                if trusted_result is not None:
                    try:
                        assert_ad_ou_allowed(trusted_result['user'])
                    except PermissionError as exc:
                        response = JSONResponse(status_code=403, content={'detail': str(exc), 'trace_id': trace_id})
                    else:
                        request.state.user = trusted_result['user']
                        request.state.issued_token = trusted_result['token']
                        response = await call_next(request)
                else:
                    response = JSONResponse(status_code=401, content={'detail': 'Authentication required.', 'trace_id': trace_id})
            else:
                user = user_from_token(token)
                if user is None:
                    response = JSONResponse(status_code=401, content={'detail': 'Invalid or expired session.', 'trace_id': trace_id})
                else:
                    try:
                        assert_ad_ou_allowed(user)
                    except PermissionError as exc:
                        response = JSONResponse(status_code=403, content={'detail': str(exc), 'trace_id': trace_id})
                    else:
                        if user.get('must_change_password') and path not in PASSWORD_CHANGE_ALLOWED_PATHS:
                            response = JSONResponse(status_code=403, content={'detail': 'Password change required before continuing.', 'code': 'password_change_required', 'trace_id': trace_id})
                        else:
                            request.state.user = user
                            response = await call_next(request)
        if response is None:
            response = await call_next(request)
    except Exception as exc:
        db.log_application('application', 'critical', f'Unhandled request failure: {path}', getattr(getattr(request, 'state', None), 'user', {}).get('email', 'system') if hasattr(request, 'state') else 'system', {'error': str(exc), 'path': path}, trace_id)
        response = JSONResponse(status_code=500, content={'detail': 'Internal server error.', 'trace_id': trace_id})
    elapsed_ms = max(1, int((time.perf_counter() - started) * 1000))
    response.headers['X-Trace-Id'] = trace_id
    response.headers['Server-Timing'] = f'app;dur={elapsed_ms}'
    if path.startswith('/api') and response.status_code >= 500:
        db.log_application('application', 'error', f'HTTP {response.status_code}: {path}', getattr(getattr(request, 'state', None), 'user', {}).get('email', 'system') if hasattr(request, 'state') else 'system', {'path': path, 'elapsed_ms': elapsed_ms}, trace_id)
    return _with_security_headers(response)


def _with_security_headers(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['Referrer-Policy'] = 'same-origin'
    response.headers['Permissions-Policy'] = 'camera=(), microphone=(), geolocation=()'
    response.headers['Cache-Control'] = 'no-store'
    app_env = os.getenv('CAMPUS_FPM_ENV', os.getenv('APP_ENV', 'development')).lower()
    if app_env in {'prod', 'production'}:
        response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    return response

@app.get('/api/ux/status')
def ux_status_endpoint() -> dict[str, Any]:
    return ux_productivity_status()


@app.get('/api/accessibility/status')
def accessibility_status_endpoint() -> dict[str, Any]:
    return accessibility_testing_status()


@app.get('/api/production-ops/status')
def production_ops_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return production_operations_status()


@app.get('/api/production-ops/application-logs')
def production_ops_application_logs(request: Request, limit: int = Query(100, ge=1, le=500), severity: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return list_application_logs(limit, severity)


@app.post('/api/production-ops/application-logs')
def production_ops_record_application_log(payload: ApplicationLogCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return record_application_log(payload.model_dump(), request.state.user)


@app.get('/api/production-ops/admin-audit-report')
def production_ops_admin_audit_report(request: Request, limit: int = Query(250, ge=1, le=1000)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return admin_audit_report(limit)


@app.get('/api/production-ops/guides')
def production_ops_guides(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return guides_manifest()


@app.get('/api/production-ops/documentation-readiness')
def production_ops_documentation_readiness(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return production_documentation_readiness()


@app.get('/api/observability/status')
def observability_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return observability_status()


@app.get('/api/observability/workspace')
def observability_workspace_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return observability_workspace()


@app.post('/api/observability/evidence/run')
def observability_evidence_run_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return run_observability_evidence(request.state.user, getattr(request.state, 'trace_id', ''))


@app.get('/api/observability/metrics')
def observability_metrics_endpoint(request: Request, limit: int = Query(100, ge=1, le=500)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_observability_metrics(limit)
    return {'count': len(rows), 'metrics': rows}


@app.post('/api/observability/health-probes/run')
def observability_run_health_probes_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return run_observability_health_probes(request.state.user, getattr(request.state, 'trace_id', ''))


@app.get('/api/observability/health-probes')
def observability_health_probes_endpoint(request: Request, limit: int = Query(100, ge=1, le=500)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_observability_health_probes(limit)
    return {'count': len(rows), 'health_probes': rows}


@app.get('/api/observability/alerts')
def observability_alerts_endpoint(request: Request, status: str | None = Query(None), limit: int = Query(100, ge=1, le=500)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_observability_alerts(status, limit)
    return {'count': len(rows), 'alerts': rows}


@app.post('/api/observability/alerts/{alert_id}/acknowledge')
def observability_acknowledge_alert_endpoint(alert_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return acknowledge_observability_alert(alert_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/observability/backup-restore-drills/run')
def observability_run_backup_restore_drill_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return run_observability_backup_restore_drill(request.state.user, getattr(request.state, 'trace_id', ''))


@app.get('/api/observability/backup-restore-drills')
def observability_backup_restore_drills_endpoint(request: Request, limit: int = Query(100, ge=1, le=500)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_observability_backup_restore_drills(limit)
    return {'count': len(rows), 'backup_restore_drills': rows}


@app.get('/api/compliance/status')
def compliance_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return compliance_status()


@app.post('/api/compliance/audit/seal')
def compliance_seal_audit_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return seal_audit_backlog(request.state.user)


@app.get('/api/compliance/audit/verify')
def compliance_verify_audit_endpoint(request: Request, limit: int = Query(1000, ge=1, le=5000)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return verify_audit_chain(limit)


@app.get('/api/compliance/sod-report')
def compliance_sod_report_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    return sod_report()


@app.get('/api/compliance/retention-policies')
def compliance_retention_policies_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_retention_policies()
    return {'count': len(rows), 'policies': rows}


@app.post('/api/compliance/retention-policies')
def compliance_upsert_retention_policy_endpoint(payload: RetentionPolicyCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return upsert_retention_policy(payload.model_dump(), request.state.user)


@app.get('/api/compliance/retention-review')
def compliance_retention_review_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return retention_review()


@app.get('/api/compliance/certifications')
def compliance_certifications_endpoint(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_certifications(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'certifications': rows}


@app.post('/api/compliance/certifications')
def compliance_create_certification_endpoint(payload: ComplianceCertificationCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return create_certification(payload.model_dump(), request.state.user)


@app.post('/api/compliance/certifications/{certification_id}/certify')
def compliance_certify_endpoint(certification_id: int, payload: ComplianceCertificationDecision, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return certify_compliance_control(certification_id, payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/compliance/tax/status')
def tax_compliance_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return tax_compliance_status()


@app.get('/api/compliance/tax/workspace')
def tax_compliance_workspace_endpoint(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return tax_compliance_workspace(scenario_id)


@app.get('/api/compliance/tax/classifications')
def tax_classifications_endpoint(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_tax_classifications(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'classifications': rows}


@app.post('/api/compliance/tax/classifications')
def tax_classify_activity_endpoint(payload: TaxActivityClassificationCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return classify_tax_activity(payload.model_dump(), request.state.user)


@app.get('/api/compliance/tax/summary')
def tax_summary_endpoint(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return tax_classification_summary(scenario_id)


@app.post('/api/compliance/tax/classifications/{classification_id}/review')
def tax_review_classification_endpoint(classification_id: int, payload: TaxReviewDecision, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return review_tax_classification(classification_id, payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/compliance/tax/reviews')
def tax_reviews_endpoint(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_tax_reviews(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'reviews': rows}


@app.get('/api/compliance/tax/form990')
def tax_form990_fields_endpoint(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_form990_support_fields(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'fields': rows}


@app.post('/api/compliance/tax/form990')
def tax_upsert_form990_field_endpoint(payload: Form990SupportFieldCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return upsert_form990_support(payload.model_dump(), request.state.user)


@app.get('/api/compliance/tax/rule-sources')
def tax_rule_sources_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_tax_rule_sources()
    return {'count': len(rows), 'sources': rows}


@app.post('/api/compliance/tax/rule-sources')
def tax_upsert_rule_source_endpoint(payload: TaxRuleSourceCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return upsert_tax_rule_source(payload.model_dump(), request.state.user)


@app.get('/api/compliance/tax/update-checks')
def tax_update_checks_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_tax_update_checks()
    return {'count': len(rows), 'checks': rows}


@app.post('/api/compliance/tax/update-checks')
def tax_run_update_check_endpoint(payload: TaxUpdateCheckCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return run_tax_update_check(payload.model_dump(), request.state.user)


@app.post('/api/compliance/tax/update-checks/run-due')
def tax_run_due_update_checks_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return run_due_tax_update_checks(request.state.user)


@app.get('/api/compliance/tax/alerts')
def tax_alerts_endpoint(request: Request, status: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_tax_alerts(status)
    return {'count': len(rows), 'alerts': rows}


@app.post('/api/compliance/tax/alerts/{alert_id}/decision')
def tax_alert_decision_endpoint(alert_id: int, payload: TaxAlertDecision, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return decide_tax_alert(alert_id, payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/postgres-runtime/status')
def postgres_runtime_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return postgres_runtime_status()


@app.get('/api/database-runtime/status')
def database_runtime_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return postgres_runtime_status()


@app.get('/api/production-data-platform/status')
def production_data_platform_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return production_data_platform_status()


@app.get('/api/production-data-platform/rehearsals')
def production_data_platform_rehearsals_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_production_data_cutover_runs()
    return {'count': len(rows), 'rehearsals': rows}


@app.post('/api/production-data-platform/rehearsals/run')
def production_data_platform_run_rehearsal_endpoint(payload: ProductionDataCutoverRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return run_production_data_cutover_rehearsal(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get('/api/migrations/status')
def migration_framework_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return migration_framework_status()


@app.get('/api/migrations/runs')
def migration_runs_endpoint(request: Request, limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    runs = recent_migration_runs(limit)
    return {'count': len(runs), 'runs': runs}


@app.post('/api/migrations/dry-run')
def migration_dry_run_endpoint(request: Request, target_key: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return dry_run_managed_migrations(target_key)
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post('/api/migrations/run')
def migration_run_endpoint(request: Request, target_key: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return run_managed_migrations(target_key, request.state.user)
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get('/api/migrations/rollback-plan/{migration_key}')
def migration_rollback_plan_endpoint(migration_key: str, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return managed_migration_rollback_plan(migration_key)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/guided-entry/status')
def guided_entry_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return guided_entry_status()


@app.get('/api/guidance/status')
def guidance_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'workspaces.view')
    return guidance_training_status()


@app.get('/api/guidance/workspace')
def guidance_workspace_endpoint(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'workspaces.view')
    return guidance_training_workspace(request.state.user, scenario_id)


@app.post('/api/guidance/tasks/complete')
def guidance_complete_task_endpoint(payload: GuidanceTaskComplete, request: Request) -> dict[str, Any]:
    _require(request, 'workspaces.view')
    try:
        return complete_guidance_task(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/guidance/training/start')
def guidance_start_training_endpoint(payload: TrainingModeStart, request: Request) -> dict[str, Any]:
    _require(request, 'workspaces.view')
    try:
        return start_training_mode(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/office/status')
def office_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return office_interop_status()


@app.get('/api/office/native-status')
def office_native_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return office_native_status()


@app.get('/api/office/adoption/status')
def office_adoption_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return office_adoption_status()


@app.get('/api/office/excel-certification/status')
def office_excel_certification_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return excel_certification_status()


@app.get('/api/office/excel-certification/runs')
def office_excel_certification_runs_endpoint(request: Request, limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    rows = list_excel_certification_runs(limit)
    return {'count': len(rows), 'certification_runs': rows}


@app.post('/api/office/excel-certification/run')
def office_excel_certification_run_endpoint(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    try:
        return run_excel_adoption_certification(scenario_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/office/adoption/proof')
def office_adoption_proof_endpoint(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    try:
        return run_office_adoption_proof(scenario_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/office/native-workspace')
def office_native_workspace_endpoint(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reports.read')
    return office_native_workspace(scenario_id)


@app.post('/api/office/excel-template')
def office_create_excel_template(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    return create_excel_template(scenario_id, request.state.user)


@app.post('/api/office/excel-import')
def office_import_excel_template(payload: ExcelTemplateImportCreate, request: Request) -> dict[str, Any]:
    _require(request, 'ledger.write')
    try:
        return import_excel_template(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/office/workbook-package')
def office_create_workbook_package(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    return create_workbook_package(scenario_id, request.state.user)


@app.post('/api/office/workbooks/{workbook_key}/refresh')
def office_refresh_workbook_endpoint(workbook_key: str, request: Request) -> dict[str, Any]:
    _require(request, 'exports.manage')
    try:
        return refresh_office_workbook(workbook_key, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/office/workbooks/{workbook_key}/publish')
def office_publish_workbook_endpoint(workbook_key: str, request: Request) -> dict[str, Any]:
    _require(request, 'ledger.write')
    try:
        return publish_office_workbook(workbook_key, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/office/powerpoint-refresh')
def office_powerpoint_refresh_endpoint(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    return refresh_powerpoint_deck(scenario_id, request.state.user)


@app.post('/api/office/cell-comments')
def office_add_cell_comment_endpoint(payload: OfficeCellCommentCreate, request: Request) -> dict[str, Any]:
    _require(request, 'ledger.write')
    try:
        return add_office_cell_comment(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/office/workbooks')
def office_workbooks(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    rows = list_office_workbooks(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'workbooks': rows}


@app.get('/api/office/roundtrip-imports')
def office_roundtrip_imports(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    rows = list_roundtrip_imports(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'imports': rows}


@app.get('/api/office/named-ranges')
def office_named_ranges(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'reports.read')
    rows = list_office_named_ranges(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'named_ranges': rows}


@app.get('/api/office/cell-comments')
def office_cell_comments(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'reports.read')
    rows = list_office_cell_comments(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'cell_comments': rows}


@app.get('/api/office/workspace-actions')
def office_workspace_actions(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'reports.read')
    rows = list_office_workspace_actions(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'actions': rows}


@app.get('/api/ux/bootstrap')
def ux_bootstrap(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reports.read')
    return productivity_bootstrap(scenario_id, request.state.user)


@app.get('/api/ux/profile')
def ux_profile(request: Request) -> dict[str, Any]:
    return get_profile(request.state.user)


@app.post('/api/ux/profile')
def ux_update_profile(payload: UserProfileUpdate, request: Request) -> dict[str, Any]:
    return update_profile(payload.model_dump(), request.state.user)


@app.get('/api/ux/notifications')
def ux_notifications(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    rows = list_notifications(request.state.user, scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'notifications': rows}


@app.post('/api/ux/notifications')
def ux_create_notification(payload: NotificationCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return create_notification(payload.model_dump(), request.state.user)


@app.post('/api/ux/notifications/{notification_id}/read')
def ux_read_notification(notification_id: int, request: Request) -> dict[str, Any]:
    try:
        return mark_notification_read(notification_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/chat/users')
def chat_users(request: Request) -> dict[str, Any]:
    rows = list_chat_users(request.state.user)
    return {'count': len(rows), 'users': rows}


@app.get('/api/chat/summary')
def chat_unread_summary(request: Request) -> dict[str, Any]:
    return chat_summary(request.state.user)


@app.get('/api/chat/messages')
def chat_messages(request: Request, peer_user_id: int = Query(..., ge=1), limit: int = Query(100, ge=1, le=250)) -> dict[str, Any]:
    try:
        rows = list_chat_messages(peer_user_id, request.state.user, limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {'peer_user_id': peer_user_id, 'count': len(rows), 'messages': rows}


@app.post('/api/chat/messages')
def chat_send_message(payload: ChatMessageCreate, request: Request) -> dict[str, Any]:
    try:
        return send_message(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/chat/messages/read')
def chat_mark_messages_read(payload: ChatReadRequest, request: Request) -> dict[str, Any]:
    try:
        return mark_messages_read(request.state.user, payload.peer_user_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/ux/grids/validate')
def ux_validate_grid(payload: GridValidationRequest, request: Request) -> dict[str, Any]:
    _require(request, 'operating_budget.manage')
    return validate_grid_rows(payload.model_dump())


@app.post('/api/ux/bulk-paste')
def ux_bulk_paste(payload: BulkPasteImportCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operating_budget.manage')
    try:
        return bulk_paste_budget(payload.model_dump(), request.state.user)
    except (PermissionError, ValueError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/ux/bulk-paste')
def ux_bulk_paste_history(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'operating_budget.manage')
    rows = list_bulk_imports(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'imports': rows}


@app.get('/api/ux/missing-submissions')
def ux_missing_submissions(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'operating_budget.manage')
    return missing_submissions(scenario_id, request.state.user)


@app.get('/api/ux/department-comparison')
def ux_department_comparison(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reports.read')
    return department_comparison(scenario_id, request.state.user)


@app.get('/api/security/status')
def security_status_endpoint() -> dict[str, Any]:
    return security_status()


@app.get('/api/security/enterprise-status')
def security_enterprise_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    return enterprise_admin_status()


@app.get('/api/security/enterprise-workspace')
def security_enterprise_workspace_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    return enterprise_admin_workspace()


@app.post('/api/security/activation/run')
def security_activation_run_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    return activate_security_controls(request.state.user)


@app.get('/api/security/activation-certification/status')
def security_activation_certification_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    return security_activation_certification_status()


@app.get('/api/security/activation-certification/runs')
def security_activation_certification_runs_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    rows = list_security_activation_certification_runs()
    return {'count': len(rows), 'activation_certification_runs': rows}


@app.post('/api/security/activation-certification/run')
def security_activation_certification_run_endpoint(payload: SecurityActivationCertificationRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    return run_security_activation_certification(payload.model_dump(), request.state.user)


@app.get('/api/security/access-guard/status')
def security_access_guard_status_endpoint(request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'security.manage')
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return access_guard_status()


@app.get('/api/security/users')
def security_users(request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    rows = list_security_users()
    return {'count': len(rows), 'users': rows}


@app.post('/api/security/users')
def security_create_user(payload: UserCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'security.manage')
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return create_user(payload.model_dump(), actor=request.state.user['email'])


@app.post('/api/security/users/{user_id}/dimension-access')
def security_grant_dimension_access(user_id: int, payload: UserDimensionAccessCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'security.manage')
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return grant_dimension_access(user_id, payload.model_dump(), actor=request.state.user['email'])


@app.get('/api/security/sso-production-settings')
def security_sso_production_settings(request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    rows = list_sso_production_settings()
    return {'count': len(rows), 'sso_production_settings': rows}


@app.post('/api/security/sso-production-settings')
def security_upsert_sso_production_setting(payload: SSOProductionSettingCreate, request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    return upsert_sso_production_setting(payload.model_dump(), request.state.user)


@app.get('/api/security/ad-ou-group-mappings')
def security_ad_ou_group_mappings(request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    rows = list_ad_ou_group_mappings()
    return {'count': len(rows), 'ad_ou_group_mappings': rows}


@app.post('/api/security/ad-ou-group-mappings')
def security_upsert_ad_ou_group_mapping(payload: ADOUGroupMappingCreate, request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    return upsert_ad_ou_group_mapping(payload.model_dump(), request.state.user)


@app.get('/api/security/domain-vpn-checks')
def security_domain_vpn_checks(request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    rows = list_domain_vpn_checks()
    return {'count': len(rows), 'domain_vpn_checks': rows}


@app.post('/api/security/domain-vpn-checks')
def security_record_domain_vpn_check(payload: DomainVPNCheckCreate, request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    return record_domain_vpn_check(payload.model_dump(), request.state.user)


@app.get('/api/security/impersonations')
def security_impersonations(request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    rows = list_impersonation_sessions()
    return {'count': len(rows), 'impersonations': rows}


@app.post('/api/security/impersonations')
def security_start_impersonation(payload: AdminImpersonationCreate, request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    try:
        return start_impersonation(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/security/impersonations/{impersonation_id}/end')
def security_end_impersonation(impersonation_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    return end_impersonation(impersonation_id, request.state.user)


@app.post('/api/security/sod-policies')
def security_upsert_sod_policy(payload: SoDPolicyCreate, request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    return upsert_sod_policy(payload.model_dump(), request.state.user)


@app.get('/api/security/access-reviews')
def security_access_reviews(request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    rows = list_access_reviews()
    return {'count': len(rows), 'access_reviews': rows}


@app.post('/api/security/access-reviews')
def security_create_access_review(payload: UserAccessReviewCreate, request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    return create_access_review(payload.model_dump(), request.state.user)


@app.post('/api/security/access-reviews/{review_id}/certify')
def security_certify_access_review(review_id: int, payload: UserAccessReviewDecision, request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    return certify_access_review(review_id, payload.model_dump(), request.state.user)


@app.get('/api/operating-budget/status')
def operating_budget_status_endpoint() -> dict[str, Any]:
    return operating_budget_status()


@app.get('/api/operating-budget/submissions')
def operating_budget_submissions(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'operating_budget.manage')
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    rows = list_submissions(scenario_id, request.state.user)
    return {'scenario_id': scenario_id, 'count': len(rows), 'submissions': rows}


@app.post('/api/operating-budget/submissions')
def operating_budget_create_submission(payload: BudgetSubmissionCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'operating_budget.manage')
        return create_submission(payload.model_dump(), request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/operating-budget/submissions/{submission_id}/lines')
def operating_budget_add_line(submission_id: int, payload: OperatingBudgetLineCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'operating_budget.manage')
        return add_budget_line(submission_id, payload.model_dump(), request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/operating-budget/submissions/{submission_id}/submit')
def operating_budget_submit(submission_id: int, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'operating_budget.manage')
        return submit_submission(submission_id, request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/operating-budget/submissions/{submission_id}/approve')
def operating_budget_approve(submission_id: int, payload: ApprovalAction, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'operating_budget.approve')
        return approve_submission(submission_id, request.state.user, note=payload.note)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/operating-budget/submissions/{submission_id}/reject')
def operating_budget_reject(submission_id: int, payload: ApprovalAction, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'operating_budget.approve')
        return reject_submission(submission_id, request.state.user, note=payload.note)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/operating-budget/assumptions')
def operating_budget_assumptions(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'operating_budget.manage')
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    rows = list_assumptions(scenario_id, request.state.user)
    return {'scenario_id': scenario_id, 'count': len(rows), 'assumptions': rows}


@app.post('/api/operating-budget/assumptions')
def operating_budget_create_assumption(payload: BudgetAssumptionCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'operating_budget.manage')
        return create_assumption(payload.model_dump(), request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@app.get('/api/operating-budget/transfers')
def operating_budget_transfers(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    rows = list_transfers(scenario_id, request.state.user)
    return {'scenario_id': scenario_id, 'count': len(rows), 'transfers': rows}


@app.post('/api/operating-budget/transfers')
def operating_budget_request_transfer(payload: BudgetTransferCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'operating_budget.manage')
        return request_transfer(payload.model_dump(), request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@app.post('/api/operating-budget/transfers/{transfer_id}/approve')
def operating_budget_approve_transfer(transfer_id: int, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'operating_budget.approve')
        return approve_transfer(transfer_id, request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/enrollment/status')
def enrollment_status_endpoint() -> dict[str, Any]:
    return enrollment_status()


@app.get('/api/enrollment/terms')
def enrollment_terms(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'enrollment.manage')
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    rows = list_terms(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'terms': rows}


@app.post('/api/enrollment/terms')
def enrollment_upsert_term(payload: EnrollmentTermCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'enrollment.manage')
        return upsert_term(payload.model_dump(), request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@app.get('/api/enrollment/tuition-rates')
def enrollment_tuition_rates(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'enrollment.manage')
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    rows = list_tuition_rates(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'rates': rows}


@app.post('/api/enrollment/tuition-rates')
def enrollment_upsert_tuition_rate(payload: TuitionRateCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'enrollment.manage')
        return upsert_tuition_rate(payload.model_dump(), request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@app.get('/api/enrollment/forecast-inputs')
def enrollment_forecast_inputs(
    request: Request,
    scenario_id: int = Query(..., ge=1),
    term_code: str | None = Query(None),
) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'enrollment.manage')
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    rows = list_forecast_inputs(scenario_id, term_code)
    return {'scenario_id': scenario_id, 'count': len(rows), 'inputs': rows}


@app.post('/api/enrollment/forecast-inputs')
def enrollment_upsert_forecast_input(payload: EnrollmentForecastInputCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'enrollment.manage')
        return upsert_forecast_input(payload.model_dump(), request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@app.get('/api/enrollment/tuition-forecast-runs')
def enrollment_tuition_runs(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'enrollment.forecast')
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    rows = list_runs(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'runs': rows}


@app.post('/api/enrollment/tuition-forecast-runs')
def enrollment_run_tuition_forecast(payload: TuitionForecastRunCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'enrollment.forecast')
        return run_tuition_forecast(payload.model_dump(), request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/campus-planning/status')
def campus_planning_status_endpoint() -> dict[str, Any]:
    return campus_planning_status()


@app.get('/api/campus-planning/positions')
def campus_planning_positions(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'campus_planning.manage')
    rows = list_positions(scenario_id, request.state.user)
    return {'scenario_id': scenario_id, 'count': len(rows), 'positions': rows}


@app.post('/api/campus-planning/positions')
def campus_planning_upsert_position(payload: WorkforcePositionCreate, request: Request) -> dict[str, Any]:
    _require(request, 'campus_planning.manage')
    try:
        return upsert_position(payload.model_dump(), request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@app.get('/api/campus-planning/faculty-loads')
def campus_planning_faculty_loads(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'campus_planning.manage')
    rows = list_faculty_loads(scenario_id, request.state.user)
    return {'scenario_id': scenario_id, 'count': len(rows), 'faculty_loads': rows}


@app.post('/api/campus-planning/faculty-loads')
def campus_planning_upsert_faculty_load(payload: FacultyLoadCreate, request: Request) -> dict[str, Any]:
    _require(request, 'campus_planning.manage')
    try:
        return upsert_faculty_load(payload.model_dump(), request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@app.get('/api/campus-planning/grants')
def campus_planning_grants(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'campus_planning.manage')
    rows = list_grant_budgets(scenario_id, request.state.user)
    return {'scenario_id': scenario_id, 'count': len(rows), 'grants': rows}


@app.post('/api/campus-planning/grants')
def campus_planning_upsert_grant(payload: GrantBudgetCreate, request: Request) -> dict[str, Any]:
    _require(request, 'campus_planning.manage')
    try:
        return upsert_grant_budget(payload.model_dump(), request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@app.get('/api/campus-planning/capital-requests')
def campus_planning_capital_requests(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'campus_planning.manage')
    rows = list_capital_requests(scenario_id, request.state.user)
    return {'scenario_id': scenario_id, 'count': len(rows), 'capital_requests': rows}


@app.post('/api/campus-planning/capital-requests')
def campus_planning_upsert_capital(payload: CapitalRequestCreate, request: Request) -> dict[str, Any]:
    _require(request, 'campus_planning.manage')
    try:
        return upsert_capital_request(payload.model_dump(), request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@app.post('/api/campus-planning/capital-requests/{request_id}/approve')
def campus_planning_approve_capital(request_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'campus_planning.approve')
    try:
        return approve_capital_request(request_id, request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/scenario-engine/status')
def scenario_engine_status_endpoint() -> dict[str, Any]:
    return scenario_engine_status()


@app.get('/api/scenario-engine/drivers')
def scenario_engine_drivers(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_typed_drivers(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'drivers': rows}


@app.post('/api/scenario-engine/drivers')
def scenario_engine_upsert_driver(payload: TypedDriverCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    try:
        return upsert_typed_driver(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/scenario-engine/planning-drivers')
def scenario_engine_upsert_planning_driver(payload: DriverDefinitionCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return upsert_driver_definition(payload.model_dump(), request.state.user)


@app.get('/api/scenario-engine/driver-graph')
def scenario_engine_driver_graph(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return driver_dependency_graph(scenario_id)


@app.post('/api/scenario-engine/scenarios/{scenario_id}/clone')
def scenario_engine_clone_scenario(scenario_id: int, payload: ScenarioCloneCreate, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return clone_scenario(scenario_id, payload.model_dump(), request.state.user)


@app.get('/api/scenario-engine/compare')
def scenario_engine_compare(request: Request, base_scenario_id: int = Query(..., ge=1), compare_scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return compare_scenarios(base_scenario_id, compare_scenario_id)


@app.get('/api/scenario-engine/methods')
def scenario_engine_methods(request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    methods = list_methods()
    return {'count': len(methods), 'methods': methods}


@app.get('/api/scenario-engine/forecast-runs')
def scenario_engine_forecast_runs(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_scenario_forecast_runs(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'runs': rows}


@app.post('/api/scenario-engine/forecast-runs')
def scenario_engine_run_forecast(payload: ForecastRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    try:
        return run_scenario_forecast(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/scenario-engine/forecast-runs/{forecast_run_id}/lineage')
def scenario_engine_lineage(forecast_run_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_lineage(forecast_run_id)
    return {'forecast_run_id': forecast_run_id, 'count': len(rows), 'lineage': rows}


@app.post('/api/scenario-engine/actuals')
def scenario_engine_ingest_actuals(payload: ActualsIngestCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    try:
        return ingest_actuals(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/scenario-engine/forecast-actual-variances')
def scenario_engine_forecast_actual_variances(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_forecast_actual_variances(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'variances': rows}


@app.post('/api/scenario-engine/forecast-actual-variances/run')
def scenario_engine_run_forecast_actual_variances(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return calculate_forecast_actual_variance(scenario_id)


@app.get('/api/scenario-engine/predictive-status')
def scenario_engine_predictive_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return scenario_engine_predictive_status()


@app.get('/api/scenario-engine/predictive-workspace')
def scenario_engine_predictive_workspace(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return predictive_workspace(scenario_id)


@app.get('/api/scenario-engine/model-choices')
def scenario_engine_model_choices(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_predictive_model_choices(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'model_choices': rows}


@app.post('/api/scenario-engine/model-choices')
def scenario_engine_upsert_model_choice(payload: PredictiveModelChoiceCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    try:
        return upsert_predictive_model_choice(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/scenario-engine/tuning-profiles')
def scenario_engine_upsert_tuning_profile(payload: ForecastTuningProfileCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    try:
        return upsert_forecast_tuning_profile(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/scenario-engine/tuning-profiles')
def scenario_engine_tuning_profiles(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_forecast_tuning_profiles(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'tuning_profiles': rows}


@app.post('/api/scenario-engine/backtests')
def scenario_engine_run_backtest(payload: ForecastBacktestCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    try:
        return run_forecast_backtest(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/scenario-engine/backtests')
def scenario_engine_backtests(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_forecast_backtests(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'backtests': rows}


@app.post('/api/scenario-engine/recommendations/compare')
def scenario_engine_compare_recommendations(payload: ForecastRecommendationCompareCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    try:
        return compare_forecast_recommendations(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/scenario-engine/recommendations')
def scenario_engine_recommendations(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_forecast_recommendation_comparisons(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'recommendations': rows}


@app.post('/api/scenario-engine/driver-explanations/run')
def scenario_engine_run_driver_explanations(
    request: Request,
    scenario_id: int = Query(..., ge=1),
    account_code: str = Query(..., min_length=1, max_length=40),
    department_code: str | None = Query(None, max_length=40),
) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return explain_forecast_drivers(scenario_id, account_code, request.state.user, department_code)


@app.get('/api/scenario-engine/driver-explanations')
def scenario_engine_driver_explanations(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_forecast_driver_explanations(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'driver_explanations': rows}


@app.get('/api/scenario-engine/forecasting-accuracy-proof/status')
def scenario_engine_forecasting_accuracy_proof_status(request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return forecasting_accuracy_proof_status()


@app.get('/api/scenario-engine/forecasting-accuracy-proof/runs')
def scenario_engine_forecasting_accuracy_proof_runs(request: Request, limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_forecasting_accuracy_proof_runs(limit)
    return {'count': len(rows), 'proof_runs': rows}


@app.post('/api/scenario-engine/forecasting-accuracy-proof/run')
def scenario_engine_forecasting_accuracy_proof_run(payload: ForecastingAccuracyProofRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    try:
        return run_forecasting_accuracy_proof(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/model-builder/status')
def model_builder_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return model_builder_status()


@app.get('/api/model-builder/enterprise-status')
def model_builder_enterprise_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return enterprise_modeling_status()


@app.get('/api/model-builder/deepening/status')
def model_builder_deepening_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return model_deepening_status()


@app.get('/api/model-builder/models')
def model_builder_models(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_models(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'models': rows}


@app.post('/api/model-builder/models')
def model_builder_upsert_model(payload: PlanningModelCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return upsert_model(payload.model_dump(), request.state.user)


@app.get('/api/model-builder/models/{model_id}/formulas')
def model_builder_formulas(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_model_formulas(model_id)
    return {'model_id': model_id, 'count': len(rows), 'formulas': rows}


@app.post('/api/model-builder/formulas')
def model_builder_upsert_formula(payload: ModelFormulaCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    try:
        return upsert_formula(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/model-builder/formulas/lint')
def model_builder_lint_formula(payload: FormulaLintRequest, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    result = lint_formula(payload.expression)
    if payload.evaluate and result['ok']:
        try:
            result['evaluation'] = evaluate_formula(
                payload.expression,
                payload.context,
                default_missing_names_to_zero=True,
                rounding=2,
            )
        except (NameError, ValueError) as exc:
            result['ok'] = False
            result['errors'] = [*result['errors'], str(exc)]
    return result


@app.get('/api/model-builder/models/{model_id}/allocation-rules')
def model_builder_allocation_rules(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_allocation_rules(model_id)
    return {'model_id': model_id, 'count': len(rows), 'allocation_rules': rows}


@app.post('/api/model-builder/allocation-rules')
def model_builder_upsert_allocation_rule(payload: AllocationRuleCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return upsert_allocation_rule(payload.model_dump(), request.state.user)


@app.get('/api/model-builder/models/{model_id}/dependency-graph')
def model_builder_dependency_graph(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return model_dependency_graph(model_id)


@app.get('/api/model-builder/models/{model_id}/enterprise-workspace')
def model_builder_enterprise_workspace(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return enterprise_modeling_workspace(model_id)


@app.post('/api/model-builder/models/{model_id}/cube/build')
def model_builder_build_cube(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return build_enterprise_cube(model_id, request.state.user)


@app.post('/api/model-builder/models/{model_id}/cube/optimize')
def model_builder_optimize_cube(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return optimize_cube_strategy(model_id, request.state.user)


@app.get('/api/model-builder/models/{model_id}/cube/optimization-profiles')
def model_builder_cube_optimization_profiles(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_cube_optimization_profiles(model_id)
    return {'model_id': model_id, 'count': len(rows), 'optimization_profiles': rows}


@app.get('/api/model-builder/models/{model_id}/calculation-order')
def model_builder_calculation_order(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return model_calculation_order(model_id)


@app.post('/api/model-builder/models/{model_id}/dependencies/invalidate')
def model_builder_invalidate_dependencies(model_id: int, request: Request, reason: str = Query('manual_model_change', max_length=160)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return invalidate_model_dependencies(model_id, reason, request.state.user)


@app.get('/api/model-builder/models/{model_id}/dependencies/invalidations')
def model_builder_dependency_invalidations(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_model_invalidations(model_id)
    return {'model_id': model_id, 'count': len(rows), 'invalidations': rows}


@app.post('/api/model-builder/models/{model_id}/publish')
def model_builder_publish_model(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    try:
        return publish_model_version(model_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/model-builder/models/{model_id}/versions')
def model_builder_versions(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_model_versions(model_id)
    return {'model_id': model_id, 'count': len(rows), 'versions': rows}


@app.post('/api/model-builder/models/{model_id}/scenario-branches')
def model_builder_create_scenario_branch(model_id: int, payload: ModelScenarioBranchCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    try:
        return create_model_scenario_branch(model_id, payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/model-builder/models/{model_id}/scenario-branches')
def model_builder_scenario_branches(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_model_scenario_branches(model_id)
    return {'model_id': model_id, 'count': len(rows), 'scenario_branches': rows}


@app.post('/api/model-builder/models/{model_id}/deepening-proof')
def model_builder_deepening_proof(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    try:
        return run_model_deepening_proof(model_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/model-builder/models/{model_id}/performance-test')
def model_builder_performance_test(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return run_model_performance_test(model_id, request.state.user)


@app.get('/api/model-builder/models/{model_id}/performance-tests')
def model_builder_performance_tests(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_model_performance_tests(model_id)
    return {'model_id': model_id, 'count': len(rows), 'performance_tests': rows}


@app.get('/api/profitability/status')
def profitability_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return profitability_status()


@app.get('/api/profitability/workspace')
def profitability_workspace_endpoint(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return profitability_workspace(scenario_id)


@app.get('/api/profitability/cost-pools')
def profitability_cost_pools(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_profitability_cost_pools(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'cost_pools': rows}


@app.post('/api/profitability/cost-pools')
def profitability_upsert_cost_pool(payload: ProfitabilityCostPoolCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return upsert_profitability_cost_pool(payload.model_dump(), request.state.user)


@app.post('/api/profitability/allocation-runs')
def profitability_run_allocation(payload: ProfitabilityAllocationRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    try:
        return run_service_center_allocation(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/profitability/allocation-runs')
def profitability_allocation_runs(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_profitability_allocation_runs(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'allocation_runs': rows}


@app.get('/api/profitability/trace-lines')
def profitability_trace_lines(request: Request, scenario_id: int = Query(..., ge=1), run_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_profitability_trace_lines(scenario_id, run_id)
    return {'scenario_id': scenario_id, 'run_id': run_id, 'count': len(rows), 'trace_lines': rows}


@app.get('/api/profitability/program-margin')
def profitability_program_margin(request: Request, scenario_id: int = Query(..., ge=1), period_start: str | None = Query(None), period_end: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return program_margin_report(scenario_id, period_start, period_end)


@app.get('/api/profitability/fund-profitability')
def profitability_fund_report_endpoint(request: Request, scenario_id: int = Query(..., ge=1), period_start: str | None = Query(None), period_end: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return fund_profitability_report(scenario_id, period_start, period_end)


@app.get('/api/profitability/grant-profitability')
def profitability_grant_report_endpoint(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return grant_profitability_report(scenario_id)


@app.get('/api/profitability/before-after')
def profitability_before_after(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return before_after_allocation_comparison(scenario_id)


@app.post('/api/profitability/snapshots')
def profitability_snapshot(request: Request, scenario_id: int = Query(..., ge=1), period_start: str = Query(...), period_end: str = Query(...), snapshot_type: str = Query('profitability_package')) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return create_profitability_snapshot(scenario_id, period_start, period_end, snapshot_type, request.state.user)


@app.get('/api/profitability/snapshots')
def profitability_snapshots(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_profitability_snapshots(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'snapshots': rows}


@app.post('/api/model-builder/models/{model_id}/recalculate')
def model_builder_recalculate(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    return recalculate_model(model_id, request.state.user)


@app.get('/api/model-builder/models/{model_id}/recalculation-runs')
def model_builder_recalculation_runs(model_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'forecast.manage')
    rows = list_recalculation_runs(model_id)
    return {'model_id': model_id, 'count': len(rows), 'runs': rows}


@app.get('/api/reporting/status')
def reporting_status_endpoint() -> dict[str, Any]:
    return reporting_status()


@app.get('/api/fpa-workflow-certification/status')
def fpa_workflow_certification_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return fpa_workflow_certification_status()


@app.get('/api/fpa-workflow-certification/runs')
def fpa_workflow_certification_runs_endpoint(request: Request, limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_fpa_workflow_certification_runs(limit)
    return {'count': len(rows), 'certification_runs': rows}


@app.post('/api/fpa-workflow-certification/run')
def fpa_workflow_certification_run_endpoint(payload: FPAWorkflowCertificationRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    try:
        return run_fpa_workflow_certification(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/reporting/pixel-polish-certification/status')
def reporting_pixel_polish_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return reporting_pixel_polish_status()


@app.get('/api/reporting/pixel-polish-certification/runs')
def reporting_pixel_polish_runs_endpoint(request: Request, limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_reporting_pixel_polish_runs(limit)
    return {'count': len(rows), 'certification_runs': rows}


@app.post('/api/reporting/pixel-polish-certification/run')
def reporting_pixel_polish_run_endpoint(payload: ReportingPixelPolishRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    try:
        return run_reporting_pixel_polish(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/reporting/designer-distribution/status')
def reporting_designer_distribution_status(request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return designer_distribution_status()


@app.get('/api/reporting/production-polish/status')
def reporting_production_polish_status(request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return production_reporting_status()


@app.get('/api/reporting/production-polish/workspace')
def reporting_production_polish_workspace(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return production_reporting_workspace(scenario_id, request.state.user)


@app.get('/api/reporting/pixel-financial-statement')
def reporting_pixel_financial_statement(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return pixel_financial_statement(scenario_id, request.state.user)


@app.get('/api/reporting/footnotes')
def reporting_footnotes(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_report_footnotes(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'footnotes': rows}


@app.post('/api/reporting/footnotes')
def reporting_upsert_footnote(payload: ReportFootnoteCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return upsert_report_footnote(payload.model_dump(), request.state.user)


@app.get('/api/reporting/page-breaks')
def reporting_page_breaks(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_page_breaks(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'page_breaks': rows}


@app.post('/api/reporting/page-breaks')
def reporting_upsert_page_break(payload: ReportPageBreakCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    try:
        return upsert_page_break(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/reporting/pagination-profiles')
def reporting_pagination_profiles(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_pagination_profiles(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'profiles': rows}


@app.post('/api/reporting/pagination-profiles')
def reporting_upsert_pagination_profile(payload: PdfPaginationProfileCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return upsert_pagination_profile(payload.model_dump(), request.state.user)


@app.get('/api/reporting/reports')
def reporting_reports(request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_report_definitions()
    return {'count': len(rows), 'reports': rows}


@app.post('/api/reporting/reports')
def reporting_create_report(payload: ReportDefinitionCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return create_report_definition(payload.model_dump(), request.state.user)


@app.get('/api/reporting/reports/{report_id}/run')
def reporting_run_report(report_id: int, request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    try:
        return run_report(report_id, scenario_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/reporting/layouts')
def reporting_layouts(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_report_layouts(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'layouts': rows}


@app.post('/api/reporting/layouts')
def reporting_save_layout(payload: ReportLayoutCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return save_report_layout(payload.model_dump(), request.state.user)


@app.get('/api/reporting/charts')
def reporting_charts(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_report_charts(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'charts': rows}


@app.post('/api/reporting/charts')
def reporting_create_chart(payload: ReportChartCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return create_report_chart(payload.model_dump(), request.state.user)


@app.post('/api/reporting/charts/{chart_id}/format')
def reporting_format_chart(chart_id: int, payload: ChartFormatCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    try:
        return apply_chart_format(chart_id, payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/reporting/chart-rendering/status')
def reporting_chart_rendering_status(request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return chart_rendering_status()


@app.get('/api/reporting/chart-rendering/workspace')
def reporting_chart_rendering_workspace(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return chart_rendering_workspace(scenario_id, request.state.user)


@app.get('/api/reporting/production-pdf/status')
def reporting_production_pdf_status(request: Request) -> dict[str, Any]:
    _require(request, 'exports.manage')
    return production_pdf_status()


@app.get('/api/reporting/production-pdf/workspace')
def reporting_production_pdf_workspace(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    return production_pdf_workspace(scenario_id, request.state.user)


@app.get('/api/reporting/output-completion/status')
def reporting_output_completion_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'exports.manage')
    return reporting_output_completion_status()


@app.post('/api/reporting/output-completion/run')
def reporting_output_completion_run_endpoint(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    return run_reporting_output_completion(scenario_id, request.state.user)


@app.get('/api/reporting/chart-renders')
def reporting_chart_renders(request: Request, scenario_id: int | None = Query(None, ge=1), chart_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_chart_renders(scenario_id=scenario_id, chart_id=chart_id)
    return {'scenario_id': scenario_id, 'chart_id': chart_id, 'count': len(rows), 'renders': rows}


@app.post('/api/reporting/charts/{chart_id}/render')
def reporting_render_chart(chart_id: int, payload: ChartRenderCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    try:
        return render_chart(chart_id, payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/reporting/dashboard-chart-snapshots')
def reporting_dashboard_chart_snapshots(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_dashboard_chart_snapshots(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'snapshots': rows}


@app.post('/api/reporting/dashboard-chart-snapshots')
def reporting_create_dashboard_chart_snapshot(payload: DashboardChartSnapshotCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    try:
        return create_dashboard_chart_snapshot(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/reporting/report-books')
def reporting_report_books(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_report_books(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'books': rows}


@app.post('/api/reporting/report-books')
def reporting_assemble_report_book(payload: ReportBookCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    try:
        return assemble_report_book(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/reporting/burst-rules')
def reporting_burst_rules(request: Request, scenario_id: int = Query(..., ge=1), book_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_burst_rules(scenario_id, book_id)
    return {'scenario_id': scenario_id, 'book_id': book_id, 'count': len(rows), 'burst_rules': rows}


@app.post('/api/reporting/burst-rules')
def reporting_create_burst_rule(payload: ReportBurstRuleCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    try:
        return create_burst_rule(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/reporting/recurring-packages')
def reporting_recurring_packages(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    rows = list_recurring_report_packages(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'recurring_packages': rows}


@app.post('/api/reporting/recurring-packages')
def reporting_create_recurring_package(payload: RecurringReportPackageCreate, request: Request) -> dict[str, Any]:
    _require(request, 'exports.manage')
    try:
        return create_recurring_report_package(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/reporting/recurring-packages/{package_id}/run')
def reporting_run_recurring_package(package_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'exports.manage')
    try:
        return run_recurring_report_package(package_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/reporting/recurring-package-releases')
def reporting_recurring_package_releases(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    rows = list_board_package_release_reviews(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'releases': rows}


@app.post('/api/reporting/recurring-packages/{package_id}/release-request')
def reporting_request_recurring_release(package_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'exports.manage')
    try:
        return request_board_package_release(package_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/reporting/recurring-packages/{package_id}/approve-release')
def reporting_approve_recurring_release(package_id: int, payload: BoardPackageReleaseDecision, request: Request) -> dict[str, Any]:
    _require(request, 'automation.approve')
    try:
        return approve_board_package_release(package_id, payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/reporting/recurring-packages/{package_id}/release')
def reporting_release_recurring_package(package_id: int, payload: BoardPackageReleaseDecision, request: Request) -> dict[str, Any]:
    _require(request, 'exports.manage')
    try:
        return release_board_package(package_id, payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/reporting/recurring-package-runs')
def reporting_recurring_package_runs(request: Request, package_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    rows = list_recurring_report_package_runs(package_id)
    return {'package_id': package_id, 'count': len(rows), 'runs': rows}


@app.get('/api/reporting/financial-statement')
def reporting_financial_statement(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return financial_statement(scenario_id, request.state.user)


@app.get('/api/reporting/variance')
def reporting_variance(request: Request, base_scenario_id: int = Query(..., ge=1), compare_scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return variance_report(base_scenario_id, compare_scenario_id, request.state.user)


@app.get('/api/reporting/account-rollups')
def reporting_account_rollups(request: Request, scenario_id: int = Query(..., ge=1), period_start: str | None = Query(None), period_end: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return account_rollups(scenario_id, period_start, period_end)


@app.get('/api/reporting/period-range')
def reporting_period_range(request: Request, scenario_id: int = Query(..., ge=1), period_start: str = Query(...), period_end: str = Query(...), dimension: str = Query('account_code')) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    try:
        return period_range_report(scenario_id, period_start, period_end, dimension)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get('/api/reporting/actual-budget-forecast-variance')
def reporting_actual_budget_forecast_variance(request: Request, scenario_id: int = Query(..., ge=1), period_start: str | None = Query(None), period_end: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return actual_budget_forecast_variance(scenario_id, period_start, period_end)


@app.get('/api/reporting/balance-sheet')
def reporting_balance_sheet(request: Request, scenario_id: int = Query(..., ge=1), period_start: str | None = Query(None), period_end: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return balance_sheet(scenario_id, period_start, period_end)


@app.get('/api/reporting/cash-flow')
def reporting_cash_flow(request: Request, scenario_id: int = Query(..., ge=1), period_start: str | None = Query(None), period_end: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return cash_flow_statement(scenario_id, period_start, period_end)


@app.get('/api/reporting/fund-report')
def reporting_fund_report(request: Request, scenario_id: int = Query(..., ge=1), period_start: str | None = Query(None), period_end: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return fund_report(scenario_id, period_start, period_end)


@app.get('/api/reporting/grant-report')
def reporting_grant_report(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return grant_report(scenario_id)


@app.get('/api/reporting/departmental-pl')
def reporting_departmental_pl(request: Request, scenario_id: int = Query(..., ge=1), period_start: str | None = Query(None), period_end: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return departmental_pl(scenario_id, period_start, period_end)


@app.get('/api/reporting/board-packages')
def reporting_board_packages(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_board_packages(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'packages': rows}


@app.post('/api/reporting/board-packages')
def reporting_assemble_board_package(payload: BoardPackageCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return assemble_board_package(payload.model_dump(), request.state.user)


@app.get('/api/reporting/widgets')
def reporting_widgets(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_widgets(scenario_id, request.state.user)
    return {'scenario_id': scenario_id, 'count': len(rows), 'widgets': rows}


@app.post('/api/reporting/widgets')
def reporting_create_widget(payload: DashboardWidgetCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return create_widget(payload.model_dump(), request.state.user)


@app.get('/api/reporting/exports')
def reporting_exports(request: Request) -> dict[str, Any]:
    _require(request, 'exports.manage')
    rows = list_exports()
    return {'count': len(rows), 'exports': rows}


@app.post('/api/reporting/exports')
def reporting_create_export(payload: ScheduledExportCreate, request: Request) -> dict[str, Any]:
    _require(request, 'exports.manage')
    return create_export(payload.model_dump(), request.state.user)


@app.get('/api/reporting/artifacts')
def reporting_artifacts(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    rows = list_export_artifacts(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'artifacts': rows}


@app.post('/api/reporting/artifacts')
def reporting_create_artifact(payload: ExportArtifactCreate, request: Request) -> dict[str, Any]:
    _require(request, 'exports.manage')
    try:
        return create_export_artifact(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/reporting/artifacts/{artifact_id}/download')
def reporting_download_artifact(artifact_id: int, request: Request) -> FileResponse:
    _require(request, 'exports.manage')
    try:
        artifact = get_export_artifact(artifact_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    path = Path(artifact['storage_path'])
    if not path.exists():
        raise HTTPException(status_code=404, detail='Artifact file not found.')
    return FileResponse(path, media_type=artifact['content_type'], filename=artifact['file_name'])


@app.get('/api/reporting/artifact-validations')
def reporting_artifact_validations(
    request: Request,
    artifact_id: int | None = Query(None, ge=1),
    scenario_id: int | None = Query(None, ge=1),
) -> dict[str, Any]:
    _require(request, 'exports.manage')
    rows = list_export_artifact_validations(artifact_id=artifact_id, scenario_id=scenario_id)
    return {'artifact_id': artifact_id, 'scenario_id': scenario_id, 'count': len(rows), 'validations': rows}


@app.post('/api/reporting/artifacts/{artifact_id}/validate')
def reporting_validate_artifact(artifact_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'exports.manage')
    try:
        return validate_export_artifact(artifact_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/reporting/snapshots')
def reporting_snapshots(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    rows = list_report_snapshots(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'snapshots': rows}


@app.post('/api/reporting/snapshots')
def reporting_create_snapshot(payload: ReportSnapshotCreate, request: Request) -> dict[str, Any]:
    _require(request, 'exports.manage')
    return create_report_snapshot(payload.model_dump(), request.state.user)


@app.get('/api/reporting/scheduled-extract-runs')
def reporting_scheduled_extract_runs(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    rows = list_scheduled_extract_runs(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'runs': rows}


@app.post('/api/reporting/scheduled-extract-runs')
def reporting_run_scheduled_extract(payload: ScheduledExtractRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'exports.manage')
    return run_scheduled_extract(payload.model_dump(), request.state.user)


@app.get('/api/reporting/bi-api-manifest')
def reporting_bi_api_manifest(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'exports.manage')
    return bi_api_manifest(scenario_id, request.state.user)


@app.get('/api/reporting/variance-thresholds')
def reporting_variance_thresholds(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_variance_thresholds(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'thresholds': rows}


@app.post('/api/reporting/variance-thresholds')
def reporting_upsert_variance_threshold(payload: VarianceThresholdCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return upsert_variance_threshold(payload.model_dump(), request.state.user)


@app.get('/api/reporting/variance-explanations')
def reporting_variance_explanations(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_variance_explanations(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'explanations': rows}


@app.post('/api/reporting/variance-explanations/generate')
def reporting_generate_variance_explanations(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return generate_required_variance_explanations(scenario_id, request.state.user)


@app.post('/api/reporting/variance-explanations')
def reporting_update_variance_explanation(payload: VarianceExplanationCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    try:
        return update_variance_explanation(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/reporting/variance-explanations/draft')
def reporting_draft_variance_narratives(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return draft_variance_narratives(scenario_id, request.state.user)


@app.post('/api/reporting/variance-explanations/{explanation_id}/submit')
def reporting_submit_variance_explanation(explanation_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    try:
        return submit_variance_explanation(explanation_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/reporting/variance-explanations/{explanation_id}/approve')
def reporting_approve_variance_explanation(explanation_id: int, payload: NarrativeDecisionCreate, request: Request) -> dict[str, Any]:
    _require(request, 'automation.approve')
    try:
        return approve_variance_explanation(explanation_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/reporting/variance-explanations/{explanation_id}/reject')
def reporting_reject_variance_explanation(explanation_id: int, payload: NarrativeDecisionCreate, request: Request) -> dict[str, Any]:
    _require(request, 'automation.approve')
    try:
        return reject_variance_explanation(explanation_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/reporting/narratives')
def reporting_narratives(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    rows = list_narrative_reports(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'narratives': rows}


@app.post('/api/reporting/narratives')
def reporting_assemble_narrative(payload: NarrativeDraftCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reporting.manage')
    return assemble_narrative_report(payload.model_dump(), request.state.user)


@app.post('/api/reporting/narratives/{narrative_id}/approve')
def reporting_approve_narrative(narrative_id: int, payload: NarrativeDecisionCreate, request: Request) -> dict[str, Any]:
    _require(request, 'automation.approve')
    try:
        return approve_narrative_report(narrative_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/ai-explainability/status')
def ai_explainability_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'automation.manage')
    return ai_explainability_status()


@app.get('/api/ai-explainability/explanations')
def ai_explanations_endpoint(request: Request, scenario_id: int = Query(..., ge=1), status: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'automation.manage')
    rows = list_ai_explanations(scenario_id, status)
    return {'scenario_id': scenario_id, 'count': len(rows), 'explanations': rows}


@app.post('/api/ai-explainability/explanations/draft')
def ai_draft_variance_explanations_endpoint(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'automation.manage')
    return draft_ai_variance_explanations(scenario_id, request.state.user)


@app.post('/api/ai-explainability/explanations/{explanation_id}/submit')
def ai_submit_explanation_endpoint(explanation_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'automation.manage')
    try:
        return submit_ai_explanation(explanation_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/ai-explainability/explanations/{explanation_id}/approve')
def ai_approve_explanation_endpoint(explanation_id: int, payload: AIExplanationDecision, request: Request) -> dict[str, Any]:
    _require(request, 'automation.approve')
    try:
        return approve_ai_explanation(explanation_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/ai-explainability/explanations/{explanation_id}/reject')
def ai_reject_explanation_endpoint(explanation_id: int, payload: AIExplanationDecision, request: Request) -> dict[str, Any]:
    _require(request, 'automation.approve')
    try:
        return reject_ai_explanation(explanation_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/market-lab/status')
def market_lab_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return market_lab_status()


@app.get('/api/market-lab')
def market_lab_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return market_lab(request.state.user)


@app.get('/api/market-lab/search')
def market_lab_search_endpoint(request: Request, q: str = Query('', max_length=80)) -> dict[str, Any]:
    _require(request, 'reports.read')
    return search_symbols(q)


@app.get('/api/market-lab/quote/{symbol}')
def market_lab_quote_endpoint(symbol: str, request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return market_quote(symbol)


@app.post('/api/market-lab/watchlist')
def market_lab_watchlist_endpoint(payload: MarketWatchSymbolCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return add_watchlist_symbol(request.state.user, payload.symbol)


@app.post('/api/market-lab/account')
def market_lab_account_endpoint(payload: PaperTradingAccountCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return reset_paper_account(request.state.user, payload.starting_cash)


@app.get('/api/market-lab/account')
def market_lab_get_account_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return ensure_paper_account(request.state.user)


@app.post('/api/market-lab/trades')
def market_lab_trade_endpoint(payload: PaperTradeCreate, request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    try:
        return place_trade(request.state.user, payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/brokerage/status')
def brokerage_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return brokerage_status()


@app.get('/api/brokerage/provider-readiness/status')
def brokerage_provider_readiness_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return brokerage_provider_readiness_status()


@app.get('/api/brokerage/providers')
def brokerage_providers_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return brokerage_provider_catalog()


@app.get('/api/brokerage')
def brokerage_workspace_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return brokerage_workspace(request.state.user)


@app.get('/api/brokerage/connections')
def brokerage_connections_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return list_brokerage_connections(request.state.user)


@app.post('/api/brokerage/connections')
def brokerage_create_connection_endpoint(payload: BrokerageConnectionCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return create_brokerage_connection(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/brokerage/connections/{connection_id}/credential-setup')
def brokerage_credential_setup_endpoint(connection_id: int, payload: BrokerageCredentialSetupCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return setup_brokerage_credentials(connection_id, payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/brokerage/connections/{connection_id}/consent')
def brokerage_consent_endpoint(connection_id: int, payload: BrokerageConsentCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return record_brokerage_consent(connection_id, payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/brokerage/connections/{connection_id}/test')
def brokerage_test_connection_endpoint(connection_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return test_brokerage_connection(connection_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/brokerage/connections/{connection_id}/sync')
def brokerage_sync_connection_endpoint(connection_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return sync_brokerage_connection(connection_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/brokerage/accounts')
def brokerage_accounts_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return list_brokerage_accounts(request.state.user)


@app.get('/api/brokerage/holdings')
def brokerage_holdings_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return list_brokerage_holdings(request.state.user)


@app.get('/api/brokerage/sync-runs')
def brokerage_sync_runs_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return list_brokerage_sync_runs(request.state.user)


@app.get('/api/brokerage/consents')
def brokerage_consents_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return list_brokerage_consents(request.state.user)


@app.get('/api/brokerage/audit-trail')
def brokerage_audit_trail_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return brokerage_audit_trail(request.state.user)


@app.get('/api/close/status')
def close_status_endpoint() -> dict[str, Any]:
    return close_consolidation_status()


@app.get('/api/close/certification/status')
def close_certification_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    return financial_close_certification_status()


@app.get('/api/close/certification/runs')
def close_certification_runs_endpoint(request: Request, limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
    _require(request, 'close.manage')
    rows = list_financial_close_certification_runs(limit)
    return {'count': len(rows), 'certification_runs': rows}


@app.post('/api/close/certification/run')
def close_certification_run_endpoint(payload: FinancialCloseCertificationRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    try:
        return run_financial_close_certification(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/close/consolidation-certification/status')
def close_consolidation_certification_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    return consolidation_certification_status()


@app.get('/api/close/consolidation-certification/runs')
def close_consolidation_certification_runs_endpoint(request: Request, limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
    _require(request, 'close.manage')
    rows = list_consolidation_certification_runs(limit)
    return {'count': len(rows), 'certification_runs': rows}


@app.post('/api/close/consolidation-certification/run')
def close_consolidation_certification_run_endpoint(payload: ConsolidationCertificationRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    try:
        return run_consolidation_certification(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/close/templates')
def close_templates(request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    rows = list_close_task_templates()
    return {'count': len(rows), 'templates': rows}


@app.post('/api/close/templates')
def close_create_template(payload: CloseTaskTemplateCreate, request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    return create_close_task_template(payload.model_dump(), request.state.user)


@app.post('/api/close/templates/instantiate')
def close_instantiate_templates(request: Request, scenario_id: int = Query(..., ge=1), period: str = Query(...)) -> dict[str, Any]:
    _require(request, 'close.manage')
    try:
        return instantiate_close_templates(scenario_id, period, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/close/task-dependencies')
def close_task_dependencies(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'close.manage')
    rows = list_task_dependencies(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'dependencies': rows}


@app.post('/api/close/task-dependencies')
def close_create_task_dependency(payload: CloseTaskDependencyCreate, request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    return create_task_dependency(payload.model_dump())


@app.get('/api/close/calendar')
def close_calendar(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'close.manage')
    rows = list_period_close_calendar(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'calendar': rows}


@app.post('/api/close/calendar')
def close_upsert_calendar(payload: PeriodCloseCalendarCreate, request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    return upsert_period_close_calendar(payload.model_dump(), request.state.user)


@app.post('/api/close/calendar/{period}/lock')
def close_set_period_lock(period: str, payload: PeriodLockAction, request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'close.manage')
    return set_period_lock(scenario_id, period, payload.lock_state, request.state.user)


@app.get('/api/close/checklists')
def close_checklists(request: Request, scenario_id: int = Query(..., ge=1), period: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'close.manage')
    rows = list_checklist_items(scenario_id, period)
    return {'scenario_id': scenario_id, 'period': period, 'count': len(rows), 'items': rows}


@app.post('/api/close/checklists')
def close_create_checklist(payload: CloseChecklistCreate, request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    try:
        return create_checklist_item(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/close/checklists/{item_id}/complete')
def close_complete_checklist(item_id: int, payload: CloseChecklistComplete, request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    try:
        return complete_checklist_item(item_id, payload.evidence, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/close/reconciliations')
def close_reconciliations(request: Request, scenario_id: int = Query(..., ge=1), period: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'close.manage')
    rows = list_reconciliations(scenario_id, period)
    return {'scenario_id': scenario_id, 'period': period, 'count': len(rows), 'reconciliations': rows}


@app.post('/api/close/reconciliations')
def close_create_reconciliation(payload: AccountReconciliationCreate, request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    try:
        return create_reconciliation(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/close/reconciliations/{rec_id}/submit')
def close_submit_reconciliation(rec_id: int, payload: ReconciliationWorkflowAction, request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    try:
        return submit_reconciliation(rec_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/close/reconciliations/{rec_id}/approve')
def close_approve_reconciliation(rec_id: int, payload: ReconciliationWorkflowAction, request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    try:
        return approve_reconciliation(rec_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/close/reconciliations/{rec_id}/reject')
def close_reject_reconciliation(rec_id: int, payload: ReconciliationWorkflowAction, request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    try:
        return reject_reconciliation(rec_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/close/reconciliation-exceptions')
def close_reconciliation_exceptions(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'close.manage')
    rows = list_reconciliation_exceptions(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'exceptions': rows}


@app.get('/api/close/entity-confirmations')
def close_entity_confirmations(request: Request, scenario_id: int = Query(..., ge=1), period: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'close.manage')
    rows = list_entity_confirmations(scenario_id, period)
    return {'scenario_id': scenario_id, 'period': period, 'count': len(rows), 'confirmations': rows}


@app.post('/api/close/entity-confirmations')
def close_create_entity_confirmation(payload: EntityConfirmationCreate, request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    try:
        return create_entity_confirmation(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/close/entity-confirmations/{confirmation_id}/confirm')
def close_confirm_entity(confirmation_id: int, payload: EntityConfirmationResponse, request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    try:
        return confirm_entity(confirmation_id, payload.response, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/close/consolidation-entities')
def close_consolidation_entities(request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_consolidation_entities()
    return {'count': len(rows), 'entities': rows}


@app.post('/api/close/consolidation-entities')
def close_upsert_consolidation_entity(payload: ConsolidationEntityCreate, request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    return upsert_consolidation_entity(payload.model_dump(), request.state.user)


@app.get('/api/close/entity-ownerships')
def close_entity_ownerships(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_entity_ownerships(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'ownerships': rows}


@app.post('/api/close/entity-ownerships')
def close_upsert_entity_ownership(payload: EntityOwnershipCreate, request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    return upsert_entity_ownership(payload.model_dump(), request.state.user)


@app.get('/api/close/consolidation-settings')
def close_consolidation_settings(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_consolidation_settings(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'settings': rows}


@app.post('/api/close/consolidation-settings')
def close_upsert_consolidation_setting(payload: ConsolidationSettingCreate, request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    return upsert_consolidation_setting(payload.model_dump(), request.state.user)


@app.get('/api/close/advanced-consolidation/status')
def close_advanced_consolidation_status(request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    return advanced_consolidation_status()


@app.post('/api/close/financial-correctness-depth/run')
def close_run_financial_correctness_depth(request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    try:
        return run_financial_correctness_depth(request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/close/currency-rates')
def close_currency_rates(request: Request, scenario_id: int = Query(..., ge=1), period: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_currency_rates(scenario_id, period)
    return {'scenario_id': scenario_id, 'period': period, 'count': len(rows), 'currency_rates': rows}


@app.post('/api/close/currency-rates')
def close_upsert_currency_rate(payload: CurrencyRateCreate, request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    return upsert_currency_rate(payload.model_dump(), request.state.user)


@app.get('/api/close/gaap-book-mappings')
def close_gaap_book_mappings(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_gaap_book_mappings(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'gaap_book_mappings': rows}


@app.post('/api/close/gaap-book-mappings')
def close_upsert_gaap_book_mapping(payload: GaapBookMappingCreate, request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    return upsert_gaap_book_mapping(payload.model_dump(), request.state.user)


@app.get('/api/close/consolidation-journals')
def close_consolidation_journals(request: Request, scenario_id: int = Query(..., ge=1), run_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_consolidation_journals(scenario_id, run_id)
    return {'scenario_id': scenario_id, 'run_id': run_id, 'count': len(rows), 'journals': rows}


@app.get('/api/close/consolidation-rules')
def close_consolidation_rules(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_consolidation_rules(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'rules': rows}


@app.post('/api/close/consolidation-rules')
def close_upsert_consolidation_rule(payload: ConsolidationRuleCreate, request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    return upsert_consolidation_rule(payload.model_dump(), request.state.user)


@app.get('/api/close/ownership-chain-calculations')
def close_ownership_chain_calculations(request: Request, scenario_id: int = Query(..., ge=1), run_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_ownership_chain_calculations(scenario_id, run_id)
    return {'scenario_id': scenario_id, 'run_id': run_id, 'count': len(rows), 'ownership_chains': rows}


@app.get('/api/close/currency-translation-adjustments')
def close_currency_translation_adjustments(request: Request, scenario_id: int = Query(..., ge=1), run_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_currency_translation_adjustments(scenario_id, run_id)
    return {'scenario_id': scenario_id, 'run_id': run_id, 'count': len(rows), 'currency_translation_adjustments': rows}


@app.post('/api/close/statutory-packs')
def close_assemble_statutory_pack(payload: StatutoryPackCreate, request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    try:
        return assemble_statutory_pack(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/close/statutory-packs')
def close_statutory_packs(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_statutory_packs(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'statutory_packs': rows}


@app.get('/api/close/supplemental-schedules')
def close_supplemental_schedules(request: Request, scenario_id: int = Query(..., ge=1), run_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_supplemental_schedules(scenario_id, run_id)
    return {'scenario_id': scenario_id, 'run_id': run_id, 'count': len(rows), 'supplemental_schedules': rows}


@app.get('/api/close/intercompany-matches')
def close_intercompany_matches(request: Request, scenario_id: int = Query(..., ge=1), period: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'close.manage')
    rows = list_intercompany_matches(scenario_id, period)
    return {'scenario_id': scenario_id, 'period': period, 'count': len(rows), 'matches': rows}


@app.post('/api/close/intercompany-matches')
def close_create_intercompany_match(payload: IntercompanyMatchCreate, request: Request) -> dict[str, Any]:
    _require(request, 'close.manage')
    try:
        return create_intercompany_match(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/close/eliminations')
def close_eliminations(request: Request, scenario_id: int = Query(..., ge=1), period: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_eliminations(scenario_id, period)
    return {'scenario_id': scenario_id, 'period': period, 'count': len(rows), 'eliminations': rows}


@app.post('/api/close/eliminations')
def close_create_elimination(payload: EliminationEntryCreate, request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    try:
        return create_elimination(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/close/eliminations/{elimination_id}/submit')
def close_submit_elimination(elimination_id: int, payload: EliminationReviewAction, request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    try:
        return submit_elimination(elimination_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/close/eliminations/{elimination_id}/approve')
def close_approve_elimination(elimination_id: int, payload: EliminationReviewAction, request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    try:
        return approve_elimination(elimination_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/close/eliminations/{elimination_id}/reject')
def close_reject_elimination(elimination_id: int, payload: EliminationReviewAction, request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    try:
        return reject_elimination(elimination_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/close/consolidation-runs')
def close_consolidation_runs(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_consolidation_runs(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'runs': rows}


@app.post('/api/close/consolidation-runs')
def close_run_consolidation(payload: ConsolidationRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    try:
        return run_consolidation(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/close/audit-packets')
def close_audit_packets(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_audit_packets(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'audit_packets': rows}


@app.get('/api/close/consolidation-audit-reports')
def close_consolidation_audit_reports(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    rows = list_consolidation_audit_reports(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'audit_reports': rows}


@app.post('/api/close/consolidation-runs/{run_id}/audit-report')
def close_create_consolidation_audit_report(run_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'consolidation.manage')
    return create_consolidation_audit_report(run_id, request.state.user)


@app.get('/api/capabilities')
def capabilities() -> dict[str, Any]:
    return {
        'platform_name': 'Campus FPM Base',
        'different_from_prophix': [
            'Driver graph engine instead of a template-first planning workflow.',
            'API-first modules for scenario, workflow, audit, and integrations.',
            'Local-first single-node deployment for campus internal hosting on port 3200.',
            'Composable dimension model for departments, funds, accounts, grants, projects, and enrollment drivers.',
        ],
        'current_modules': [
            'budgeting',
            'forecasting',
            'scenario planning',
            'workflow approvals',
            'reporting',
            'audit trail',
            'data integration registry',
            'security-ready role boundaries',
        ],
        'campus_extensions_to_build_next': [
            'faculty load planning',
            'enrollment and tuition modeling',
            'grant budgeting',
            'capital planning',
            'cash flow planning',
            'close and consolidation',
            'variance narratives',
            'governed AI copilots',
        ],
    }


@app.get('/api/dimensions')
def dimensions() -> dict[str, list[dict[str, Any]]]:
    rows = db.fetch_all('SELECT * FROM dimensions ORDER BY kind, code')
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row['kind'], []).append(row)
    return grouped


@app.get('/api/scenarios', response_model=list[ScenarioOut])
def get_scenarios() -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM scenarios ORDER BY id DESC')
    for row in rows:
        row['locked'] = bool(row['locked'])
    return rows


@app.post('/api/scenarios', response_model=ScenarioOut)
def create_scenario(payload: ScenarioCreate) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    scenario_id = db.execute(
        '''
        INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
        VALUES (?, ?, 'draft', ?, ?, 0, ?)
        ''',
        (payload.name, payload.version, payload.start_period, payload.end_period, now),
    )
    db.log_audit(
        entity_type='scenario',
        entity_id=str(scenario_id),
        action='created',
        actor='api.user',
        detail=payload.model_dump(),
        created_at=now,
    )
    row = db.fetch_one('SELECT * FROM scenarios WHERE id = ?', (scenario_id,))
    if row is None:
        raise HTTPException(status_code=500, detail='Scenario was created but could not be reloaded.')
    row['locked'] = bool(row['locked'])
    return row


@app.post('/api/scenarios/{scenario_id}/lock')
def scenario_lock(scenario_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return lock_scenario(scenario_id, request.state.user)


@app.post('/api/scenarios/{scenario_id}/unlock')
def scenario_unlock(scenario_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return unlock_scenario(scenario_id, request.state.user)


@app.post('/api/scenarios/{scenario_id}/approve')
def scenario_approve(scenario_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return approve_scenario(scenario_id, request.state.user)


@app.post('/api/scenarios/{scenario_id}/publish')
def scenario_publish(scenario_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return publish_scenario(scenario_id, request.state.user)


@app.post('/api/scenarios/{target_scenario_id}/merge-approved')
def scenario_merge_approved(target_scenario_id: int, payload: ScenarioMergeCreate, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    try:
        return merge_approved_changes(target_scenario_id, payload.source_scenario_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/scenarios/{scenario_id}/drivers', response_model=list[DriverOut])
def get_drivers(scenario_id: int) -> list[dict[str, Any]]:
    scenario = db.fetch_one('SELECT id FROM scenarios WHERE id = ?', (scenario_id,))
    if scenario is None:
        raise HTTPException(status_code=404, detail='Scenario not found.')
    return db.fetch_all(
        'SELECT driver_key, label, expression, value, unit FROM drivers WHERE scenario_id = ? ORDER BY id ASC',
        (scenario_id,),
    )


@app.get('/api/scenarios/{scenario_id}/line-items', response_model=list[PlanLineItemOut])
def get_line_items(scenario_id: int, request: Request) -> list[dict[str, Any]]:
    scenario = db.fetch_one('SELECT id FROM scenarios WHERE id = ?', (scenario_id,))
    if scenario is None:
        raise HTTPException(status_code=404, detail='Scenario not found.')
    return [_ledger_to_line_item(row) for row in list_ledger_entries(scenario_id, user=request.state.user)]


@app.post('/api/scenarios/{scenario_id}/line-items', response_model=PlanLineItemOut)
def create_line_item(scenario_id: int, payload: PlanLineItemCreate, request: Request) -> dict[str, Any]:
    scenario = db.fetch_one('SELECT * FROM scenarios WHERE id = ?', (scenario_id,))
    if scenario is None:
        raise HTTPException(status_code=404, detail='Scenario not found.')
    if bool(scenario['locked']):
        raise HTTPException(status_code=409, detail='Scenario is locked.')

    try:
        require_permission(request.state.user, 'ledger.write')
        row = append_ledger_entry(
            {'scenario_id': scenario_id, **payload.model_dump()},
            actor=request.state.user['email'],
            user=request.state.user,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _ledger_to_line_item(row)


@app.post('/api/scenarios/{scenario_id}/forecast/run', response_model=ForecastRunResult)
def forecast_scenario(scenario_id: int) -> dict[str, Any]:
    try:
        return run_forecast(scenario_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get('/api/reports/summary', response_model=SummaryReport)
def summary_report(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    scenario = db.fetch_one('SELECT id FROM scenarios WHERE id = ?', (scenario_id,))
    if scenario is None:
        raise HTTPException(status_code=404, detail='Scenario not found.')

    try:
        require_permission(request.state.user, 'reports.read')
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return summary_by_dimensions(scenario_id, user=request.state.user)


@app.get('/api/workflows')
def get_workflows(scenario_id: int | None = Query(None, ge=1)) -> list[dict[str, Any]]:
    if scenario_id is None:
        return db.fetch_all('SELECT * FROM workflows ORDER BY scenario_id, id')
    return db.fetch_all('SELECT * FROM workflows WHERE scenario_id = ? ORDER BY id', (scenario_id,))


@app.post('/api/workflows')
def create_workflow(payload: WorkflowCreate) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    workflow_id = db.execute(
        '''
        INSERT INTO workflows (scenario_id, name, step, status, owner, updated_at)
        VALUES (?, ?, 'draft', 'pending', ?, ?)
        ''',
        (payload.scenario_id, payload.name, payload.owner, now),
    )
    db.log_audit(
        entity_type='workflow',
        entity_id=str(workflow_id),
        action='created',
        actor='api.user',
        detail=payload.model_dump(),
        created_at=now,
    )
    row = db.fetch_one('SELECT * FROM workflows WHERE id = ?', (workflow_id,))
    if row is None:
        raise HTTPException(status_code=500, detail='Workflow was created but could not be reloaded.')
    return row


@app.post('/api/workflows/{workflow_id}/advance')
def advance_workflow(workflow_id: int, payload: WorkflowAdvance) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    workflow = db.fetch_one('SELECT * FROM workflows WHERE id = ?', (workflow_id,))
    if workflow is None:
        raise HTTPException(status_code=404, detail='Workflow not found.')
    db.execute(
        'UPDATE workflows SET step = ?, status = ?, updated_at = ? WHERE id = ?',
        (payload.step, payload.status, now, workflow_id),
    )
    db.log_audit(
        entity_type='workflow',
        entity_id=str(workflow_id),
        action='advanced',
        actor='api.user',
        detail=payload.model_dump(),
        created_at=now,
    )
    row = db.fetch_one('SELECT * FROM workflows WHERE id = ?', (workflow_id,))
    if row is None:
        raise HTTPException(status_code=500, detail='Workflow was advanced but could not be reloaded.')
    return row


@app.get('/api/workflow-designer/status')
def workflow_designer_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return workflow_designer_status()


@app.get('/api/workflow-designer/workspace')
def workflow_designer_workspace(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return workflow_orchestration_workspace(scenario_id)


@app.get('/api/workflow-designer/templates')
def workflow_designer_templates(request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    rows = list_workflow_templates()
    return {'count': len(rows), 'templates': rows}


@app.post('/api/workflow-designer/templates')
def workflow_designer_create_template(payload: WorkflowTemplateCreate, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    try:
        return create_workflow_template(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/workflow-designer/instances')
def workflow_designer_instances(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    rows = list_workflow_instances(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'instances': rows}


@app.post('/api/workflow-designer/instances')
def workflow_designer_start_instance(payload: WorkflowInstanceCreate, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    try:
        return start_workflow_instance(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/workflow-designer/tasks')
def workflow_designer_tasks(request: Request, scenario_id: int | None = Query(None, ge=1), status: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    rows = list_workflow_tasks(scenario_id, status)
    return {'scenario_id': scenario_id, 'status': status, 'count': len(rows), 'tasks': rows}


@app.post('/api/workflow-designer/tasks/{task_id}/decision')
def workflow_designer_task_decision(task_id: int, payload: WorkflowTaskDecision, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    try:
        return decide_workflow_task(task_id, payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/workflow-designer/delegations')
def workflow_designer_delegations(request: Request, active_only: bool = Query(False)) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    rows = list_workflow_delegations(active_only)
    return {'count': len(rows), 'delegations': rows}


@app.post('/api/workflow-designer/delegations')
def workflow_designer_create_delegation(payload: WorkflowDelegationCreate, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return create_workflow_delegation(payload.model_dump(), request.state.user)


@app.post('/api/workflow-designer/escalations/run')
def workflow_designer_run_escalations(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return run_workflow_escalations(scenario_id, request.state.user)


@app.get('/api/workflow-designer/escalations')
def workflow_designer_escalations(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    rows = list_workflow_escalation_events(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'escalations': rows}


@app.get('/api/workflow-designer/visual-designs')
def workflow_designer_visual_designs(request: Request, template_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    rows = list_visual_designs(template_id)
    return {'template_id': template_id, 'count': len(rows), 'visual_designs': rows}


@app.post('/api/workflow-designer/visual-designs')
def workflow_designer_save_visual_design(payload: WorkflowVisualDesignCreate, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return upsert_visual_design(payload.model_dump(), request.state.user)


@app.get('/api/workflow-designer/process-calendars')
def workflow_designer_process_calendars(request: Request, scenario_id: int = Query(..., ge=1), process_type: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    rows = list_process_calendars(scenario_id, process_type)
    return {'scenario_id': scenario_id, 'process_type': process_type, 'count': len(rows), 'process_calendars': rows}


@app.post('/api/workflow-designer/process-calendars')
def workflow_designer_upsert_process_calendar(payload: ProcessCalendarCreate, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return upsert_process_calendar(payload.model_dump(), request.state.user)


@app.get('/api/workflow-designer/substitute-approvers')
def workflow_designer_substitute_approvers(request: Request, active_only: bool = Query(False)) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    rows = list_substitute_approvers(active_only)
    return {'count': len(rows), 'substitute_approvers': rows}


@app.post('/api/workflow-designer/substitute-approvers')
def workflow_designer_create_substitute_approver(payload: WorkflowSubstituteApproverCreate, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return create_substitute_approver(payload.model_dump(), request.state.user)


@app.post('/api/workflow-designer/certification-packets')
def workflow_designer_assemble_certification_packet(payload: WorkflowCertificationPacketCreate, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return assemble_workflow_certification_packet(payload.model_dump(), request.state.user)


@app.get('/api/workflow-designer/certification-packets')
def workflow_designer_certification_packets(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    rows = list_workflow_certification_packets(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'certification_packets': rows}


@app.post('/api/workflow-designer/campaign-monitors')
def workflow_designer_monitor_campaign(payload: ProcessCampaignMonitorCreate, request: Request) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    return monitor_campaign(payload.model_dump(), request.state.user)


@app.get('/api/workflow-designer/campaign-monitors')
def workflow_designer_campaign_monitors(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'scenario.manage')
    rows = list_campaign_monitors(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'campaign_monitors': rows}


@app.get('/api/audit-logs', response_model=list[AuditLogOut])
def get_audit_logs(limit: int = Query(50, ge=1, le=250)) -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM audit_logs ORDER BY id DESC LIMIT ?', (limit,))


@app.get('/api/integrations', response_model=list[IntegrationOut])
def get_integrations() -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM integrations ORDER BY id ASC')


@app.get('/api/integrations/status')
def integrations_status_endpoint() -> dict[str, Any]:
    return campus_integrations_status()


@app.get('/api/integrations/staging/status')
def integrations_staging_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return staging_status()


@app.get('/api/integrations/marketplace/status')
def integrations_marketplace_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return marketplace_status()


@app.get('/api/integrations/production/status')
def integrations_production_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return connector_production_status()


@app.get('/api/integrations/real-connector-activation/status')
def integrations_real_connector_activation_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return real_connector_activation_status()


@app.get('/api/integrations/real-connector-activation/runs')
def integrations_real_connector_activation_runs_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_real_connector_activation_runs()
    return {'count': len(rows), 'activation_runs': rows}


@app.post('/api/integrations/real-connector-activation/run')
def integrations_real_connector_activation_run_endpoint(payload: RealConnectorActivationRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return run_real_connector_activation(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get('/api/integrations/campus-data-validation/status')
def integrations_campus_data_validation_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return campus_data_validation_status()


@app.get('/api/integrations/campus-data-validation/runs')
def integrations_campus_data_validation_runs_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_campus_data_validation_runs()
    return {'count': len(rows), 'validation_runs': rows}


@app.post('/api/integrations/campus-data-validation/run')
def integrations_run_campus_data_validation_endpoint(payload: CampusDataValidationRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return run_campus_data_validation(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post('/api/integrations/connector-proof/run')
def integrations_connector_proof_run_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return run_real_connector_proof(request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get('/api/integrations/marketplace')
def integrations_marketplace_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return marketplace_workspace()


@app.get('/api/integrations/adapters')
def integrations_adapters(request: Request, system_type: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_adapters(system_type)
    return {'system_type': system_type, 'count': len(rows), 'adapters': rows}


@app.get('/api/integrations/adapter-contracts')
def integrations_adapter_contracts(request: Request, system_type: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return list_adapter_contracts(system_type)


@app.get('/api/integrations/connectors')
def integrations_connectors(request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_connectors()
    return {'count': len(rows), 'connectors': rows}


@app.post('/api/integrations/connectors')
def integrations_create_connector(payload: ConnectorCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return upsert_connector(payload.model_dump(), request.state.user)


@app.post('/api/integrations/auth-flows')
def integrations_auth_flow(payload: ConnectorAuthFlowCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return start_auth_flow(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/integrations/auth-flows')
def integrations_auth_flows(request: Request, connector_key: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_auth_flows(connector_key)
    return {'connector_key': connector_key, 'count': len(rows), 'auth_flows': rows}


@app.post('/api/integrations/connectors/{connector_key}/health')
def integrations_connector_health(connector_key: str, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return run_health_check(connector_key, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/integrations/health')
def integrations_health(request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return connector_health_dashboard()


@app.get('/api/integrations/mapping-presets')
def integrations_mapping_presets(request: Request, adapter_key: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_mapping_presets(adapter_key)
    return {'adapter_key': adapter_key, 'count': len(rows), 'presets': rows}


@app.post('/api/integrations/mapping-presets/apply')
def integrations_apply_mapping_preset(payload: MappingPresetApplyCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return apply_mapping_preset(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/integrations/source-drillbacks')
def integrations_source_drillbacks(request: Request, connector_key: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_source_drillbacks(connector_key)
    return {'connector_key': connector_key, 'count': len(rows), 'source_drillbacks': rows}


@app.get('/api/integrations/source-drillbacks/{connector_key}/{source_record_id}')
def integrations_source_drillback(connector_key: str, source_record_id: str, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return get_source_drillback(connector_key, source_record_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/integrations/source-drillbacks/{drillback_id}/validate')
def integrations_validate_source_drillback(drillback_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return validate_source_drillback(drillback_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/integrations/imports')
def integrations_import_batches(
    request: Request,
    scenario_id: int | None = Query(None, ge=1),
) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_import_batches(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'imports': rows}


@app.post('/api/integrations/imports')
def integrations_run_import(payload: ImportBatchCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return run_import(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/integrations/staging')
def integrations_staging_batches(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_staging_batches(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'staging_batches': rows}


@app.get('/api/integrations/staging/{batch_id}')
def integrations_staging_batch(batch_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return get_staging_batch(batch_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/integrations/staging/preview')
def integrations_staging_preview(payload: ImportStagingPreviewCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return create_staging_preview(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/integrations/staging/{batch_id}/rows')
def integrations_staging_rows(batch_id: int, request: Request, status: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_staging_rows(batch_id, status)
    return {'staging_batch_id': batch_id, 'status': status, 'count': len(rows), 'rows': rows}


@app.post('/api/integrations/staging/{batch_id}/approve')
def integrations_staging_approve(batch_id: int, payload: ImportStagingDecisionCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return approve_staging_batch(batch_id, payload.note, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/integrations/staging/rows/{row_id}/reject')
def integrations_staging_reject_row(row_id: int, payload: ImportStagingDecisionCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return reject_staging_row(row_id, payload.note, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/integrations/staging/rows/{row_id}/drillback')
def integrations_staging_row_drillback(row_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return staging_drillback(row_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/integrations/mapping-templates')
def integrations_mapping_templates(request: Request, connector_key: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_mapping_templates(connector_key)
    return {'connector_key': connector_key, 'count': len(rows), 'templates': rows}


@app.post('/api/integrations/mapping-templates')
def integrations_upsert_mapping_template(payload: ImportMappingTemplateCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return upsert_mapping_template(payload.model_dump(), request.state.user)


@app.get('/api/integrations/validation-rules')
def integrations_validation_rules(request: Request, import_type: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_validation_rules(import_type)
    return {'import_type': import_type, 'count': len(rows), 'rules': rows}


@app.post('/api/integrations/validation-rules')
def integrations_upsert_validation_rule(payload: ValidationRuleCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return upsert_validation_rule(payload.model_dump(), request.state.user)


@app.get('/api/integrations/credentials')
def integrations_credentials(request: Request, connector_key: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_credentials(connector_key)
    return {'connector_key': connector_key, 'count': len(rows), 'credentials': rows}


@app.post('/api/integrations/credentials')
def integrations_store_credential(payload: CredentialVaultCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return store_credential(payload.model_dump(), request.state.user)


@app.get('/api/integrations/retry-events')
def integrations_retry_events(request: Request, connector_key: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_retry_events(connector_key)
    return {'connector_key': connector_key, 'count': len(rows), 'retry_events': rows}


@app.post('/api/integrations/retry-events')
def integrations_create_retry_event(payload: RetryEventCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return create_retry_event(payload.model_dump(), request.state.user)


@app.get('/api/integrations/sync-logs')
def integrations_sync_logs(request: Request, connector_key: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_sync_logs(connector_key)
    return {'connector_key': connector_key, 'count': len(rows), 'sync_logs': rows}


@app.get('/api/integrations/banking-cash-imports')
def integrations_banking_cash_imports(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_banking_cash_imports(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'cash_imports': rows}


@app.get('/api/integrations/crm-enrollment-imports')
def integrations_crm_enrollment_imports(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_crm_enrollment_imports(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'crm_imports': rows}


@app.get('/api/integrations/rejections')
def integrations_rejections(request: Request, import_batch_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_rejections(import_batch_id)
    return {'import_batch_id': import_batch_id, 'count': len(rows), 'rejections': rows}


@app.get('/api/integrations/sync-jobs')
def integrations_sync_jobs(request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_sync_jobs()
    return {'count': len(rows), 'sync_jobs': rows}


@app.post('/api/integrations/sync-jobs')
def integrations_run_sync_job(payload: SyncJobCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    try:
        return run_sync_job(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/integrations/powerbi-exports')
def integrations_powerbi_exports(
    request: Request,
    scenario_id: int | None = Query(None, ge=1),
) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_powerbi_exports(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'exports': rows}


@app.post('/api/integrations/powerbi-exports')
def integrations_create_powerbi_export(payload: PowerBIExportCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return create_powerbi_export(payload.model_dump(), request.state.user)


@app.get('/api/data-hub/status')
def data_hub_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'reports.read')
    return data_hub_status()


@app.get('/api/data-hub/workspace')
def data_hub_workspace_endpoint(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'reports.read')
    return data_hub_workspace(scenario_id)


@app.get('/api/data-hub/change-requests')
def data_hub_change_requests(request: Request, status: str | None = Query(None)) -> dict[str, Any]:
    _require(request, 'dimensions.manage')
    rows = list_master_data_changes(status)
    return {'status': status, 'count': len(rows), 'change_requests': rows}


@app.post('/api/data-hub/change-requests')
def data_hub_request_change(payload: MasterDataChangeCreate, request: Request) -> dict[str, Any]:
    _require(request, 'dimensions.manage')
    try:
        return request_dimension_change(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/data-hub/change-requests/{change_id}/approve')
def data_hub_approve_change(change_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'dimensions.manage')
    try:
        return approve_dimension_change(change_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/data-hub/mappings')
def data_hub_mappings(request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    rows = list_master_data_mappings()
    return {'count': len(rows), 'mappings': rows}


@app.post('/api/data-hub/mappings')
def data_hub_upsert_mapping(payload: MasterDataMappingCreate, request: Request) -> dict[str, Any]:
    _require(request, 'integrations.manage')
    return upsert_master_data_mapping(payload.model_dump(), request.state.user)


@app.get('/api/data-hub/metadata-approvals')
def data_hub_metadata_approvals(request: Request) -> dict[str, Any]:
    _require(request, 'dimensions.manage')
    rows = list_metadata_approvals()
    return {'count': len(rows), 'metadata_approvals': rows}


@app.post('/api/data-hub/metadata-approvals')
def data_hub_request_metadata(payload: MetadataApprovalCreate, request: Request) -> dict[str, Any]:
    _require(request, 'dimensions.manage')
    return request_metadata_approval(payload.model_dump(), request.state.user)


@app.post('/api/data-hub/metadata-approvals/{approval_id}/approve')
def data_hub_approve_metadata(approval_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'dimensions.manage')
    try:
        return approve_metadata(approval_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/data-hub/lineage')
def data_hub_lineage(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'reports.read')
    rows = list_data_lineage_records(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'lineage': rows}


@app.post('/api/data-hub/lineage/build')
def data_hub_build_lineage(
    request: Request,
    scenario_id: int = Query(..., ge=1),
    target_type: str = Query('report'),
    target_id: str = Query('financial_statement'),
) -> dict[str, Any]:
    _require(request, 'reports.read')
    return build_data_lineage(scenario_id, target_type, target_id, request.state.user)


@app.get('/api/automation/status')
def automation_status_endpoint() -> dict[str, Any]:
    return governed_automation_status()


@app.get('/api/automation/planning-agents/status')
def automation_planning_agents_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'automation.manage')
    return planning_agents_status()


@app.get('/api/automation/ai-guardrails/status')
def automation_ai_guardrails_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'automation.manage')
    return ai_guardrails_status()


@app.post('/api/automation/ai-guardrails/run')
def automation_ai_guardrails_run_endpoint(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'automation.manage')
    return run_ai_guardrails_proof(scenario_id, request.state.user)


@app.post('/api/automation/planning-agents/run')
def automation_planning_agents_run(payload: AIPlanningAgentRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'automation.manage')
    return run_planning_agent(payload.model_dump(), request.state.user)


@app.get('/api/automation/planning-agents/prompts')
def automation_planning_agent_prompts(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'automation.manage')
    rows = list_agent_prompts(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'prompts': rows}


@app.get('/api/automation/planning-agents/actions')
def automation_planning_agent_actions(
    request: Request,
    scenario_id: int | None = Query(None, ge=1),
    status: str | None = Query(None),
) -> dict[str, Any]:
    _require(request, 'automation.manage')
    rows = list_agent_actions(scenario_id, status)
    return {'scenario_id': scenario_id, 'status': status, 'count': len(rows), 'actions': rows}


@app.post('/api/automation/planning-agents/actions/{action_id}/approve')
def automation_planning_agent_approve(action_id: int, payload: AIPlanningAgentDecision, request: Request) -> dict[str, Any]:
    _require(request, 'automation.approve')
    try:
        return approve_agent_action(action_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/automation/planning-agents/actions/{action_id}/reject')
def automation_planning_agent_reject(action_id: int, payload: AIPlanningAgentDecision, request: Request) -> dict[str, Any]:
    _require(request, 'automation.approve')
    try:
        return reject_agent_action(action_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/university-agent/status')
def university_agent_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'automation.manage')
    return university_agent_status()


@app.get('/api/university-agent/workspace')
def university_agent_workspace_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'automation.manage')
    return university_agent_workspace()


@app.get('/api/university-agent/tools')
def university_agent_tools_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'automation.manage')
    rows = list_university_agent_tools()
    return {'count': len(rows), 'tools': rows}


@app.get('/api/university-agent/clients')
def university_agent_clients_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    rows = list_university_agent_clients()
    return {'count': len(rows), 'clients': rows}


@app.post('/api/university-agent/clients')
def university_agent_upsert_client_endpoint(payload: UniversityAgentClientCreate, request: Request) -> dict[str, Any]:
    _require(request, 'security.manage')
    return upsert_university_agent_client(payload.model_dump(), request.state.user)


@app.get('/api/university-agent/policies')
def university_agent_policies_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'automation.manage')
    rows = list_university_agent_policies()
    return {'count': len(rows), 'policies': rows}


@app.post('/api/university-agent/policies')
def university_agent_upsert_policy_endpoint(payload: UniversityAgentPolicyCreate, request: Request) -> dict[str, Any]:
    _require(request, 'automation.manage')
    return upsert_university_agent_policy(payload.model_dump(), request.state.user)


@app.get('/api/university-agent/requests')
def university_agent_requests_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'automation.manage')
    rows = list_university_agent_requests()
    return {'count': len(rows), 'requests': rows}


@app.post('/api/university-agent/requests')
async def university_agent_signed_request_endpoint(request: Request) -> dict[str, Any]:
    raw_body = await request.body()
    return handle_university_agent_signed_request(dict(request.headers), raw_body)


@app.get('/api/university-agent/callbacks')
def university_agent_callbacks_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'automation.manage')
    rows = list_university_agent_callbacks()
    return {'count': len(rows), 'callbacks': rows}


@app.get('/api/university-agent/audit-logs')
def university_agent_audit_logs_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'automation.manage')
    rows = list_university_agent_audit_logs()
    return {'count': len(rows), 'audit_logs': rows}


@app.get('/api/automation/recommendations')
def automation_recommendations(
    request: Request,
    scenario_id: int = Query(..., ge=1),
    status: str | None = Query(None),
) -> dict[str, Any]:
    _require(request, 'automation.manage')
    rows = list_recommendations(scenario_id, status)
    return {'scenario_id': scenario_id, 'status': status, 'count': len(rows), 'recommendations': rows}


@app.post('/api/automation/run')
def automation_run(payload: AutomationRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'automation.manage')
    return run_assistant(payload.model_dump(), request.state.user)


@app.get('/api/automation/approval-gates')
def automation_approval_gates(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'automation.manage')
    rows = list_approval_gates(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'approval_gates': rows}


@app.post('/api/automation/recommendations/{recommendation_id}/approve')
def automation_approve(recommendation_id: int, payload: AutomationDecisionCreate, request: Request) -> dict[str, Any]:
    _require(request, 'automation.approve')
    try:
        return approve_recommendation(recommendation_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/automation/recommendations/{recommendation_id}/reject')
def automation_reject(recommendation_id: int, payload: AutomationDecisionCreate, request: Request) -> dict[str, Any]:
    _require(request, 'automation.approve')
    try:
        return reject_recommendation(recommendation_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/workspaces/status')
def workspaces_status_endpoint() -> dict[str, Any]:
    return workspace_status()


@app.get('/api/workspaces')
def workspaces(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'workspaces.view')
    return role_workspaces(scenario_id, request.state.user)


@app.get('/api/operations/status')
def operations_status_endpoint() -> dict[str, Any]:
    return deployment_operations_status()


@app.get('/api/operations/summary')
def operations_summary_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return operations_summary()


@app.get('/api/operations/checks')
def operations_checks(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_operational_checks()
    return {'count': len(rows), 'checks': rows}


@app.post('/api/operations/checks')
def operations_run_check(payload: OperationalCheckCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return run_operational_check(payload.model_dump(), request.state.user)


@app.post('/api/operations/backups')
def operations_create_backup(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return create_operations_backup(request.state.user)


@app.get('/api/operations/restore-tests')
def operations_restore_tests(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_restore_tests()
    return {'count': len(rows), 'restore_tests': rows}


@app.post('/api/operations/restore-tests')
def operations_run_restore_test(payload: RestoreTestCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return run_restore_test(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/operations/runbooks')
def operations_runbooks(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_runbooks()
    return {'count': len(rows), 'runbooks': rows}


@app.post('/api/operations/runbooks')
def operations_upsert_runbook(payload: RunbookRecordCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return upsert_runbook(payload.model_dump(), request.state.user)


@app.get('/api/deployment-governance/status')
def deployment_governance_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return deployment_governance_status()


@app.get('/api/deployment-governance/workspace')
def deployment_governance_workspace_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return deployment_governance_workspace()


@app.get('/api/deployment-governance/environments')
def deployment_governance_environments_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_deployment_environments()
    return {'count': len(rows), 'environments': rows}


@app.post('/api/deployment-governance/environments')
def deployment_governance_upsert_environment_endpoint(payload: DeploymentEnvironmentSettingCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return upsert_deployment_environment(payload.model_dump(), request.state.user)


@app.get('/api/deployment-governance/promotions')
def deployment_governance_promotions_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_deployment_promotions()
    return {'count': len(rows), 'promotions': rows}


@app.post('/api/deployment-governance/promotions')
def deployment_governance_create_promotion_endpoint(payload: DeploymentPromotionCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return create_deployment_promotion(payload.model_dump(), request.state.user)


@app.get('/api/deployment-governance/config-snapshots')
def deployment_governance_config_snapshots_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_deployment_config_snapshots()
    return {'count': len(rows), 'config_snapshots': rows}


@app.post('/api/deployment-governance/config-snapshots')
def deployment_governance_create_config_snapshot_endpoint(payload: ConfigSnapshotCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return create_deployment_config_snapshot(payload.model_dump(), request.state.user)


@app.get('/api/deployment-governance/rollback-plans')
def deployment_governance_rollback_plans_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_deployment_rollback_plans()
    return {'count': len(rows), 'rollback_plans': rows}


@app.post('/api/deployment-governance/rollback-plans')
def deployment_governance_upsert_rollback_plan_endpoint(payload: MigrationRollbackPlanCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return upsert_deployment_rollback_plan(payload.model_dump(), request.state.user)


@app.get('/api/deployment-governance/release-notes')
def deployment_governance_release_notes_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_deployment_release_notes()
    return {'count': len(rows), 'release_notes': rows}


@app.post('/api/deployment-governance/release-notes')
def deployment_governance_upsert_release_note_endpoint(payload: ReleaseNoteCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return upsert_deployment_release_note(payload.model_dump(), request.state.user)


@app.get('/api/deployment-governance/diagnostics')
def deployment_governance_diagnostics_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_deployment_diagnostics()
    return {'count': len(rows), 'diagnostics': rows}


@app.post('/api/deployment-governance/diagnostics/run')
def deployment_governance_run_diagnostics_endpoint(request: Request, scope: str = Query('release', min_length=1, max_length=80)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return run_deployment_admin_diagnostics(scope, request.state.user)


@app.get('/api/deployment-governance/readiness')
def deployment_governance_readiness_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_deployment_readiness_items()
    return {'count': len(rows), 'readiness_items': rows}


@app.post('/api/deployment-governance/readiness')
def deployment_governance_upsert_readiness_endpoint(payload: ReadinessItemCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return upsert_deployment_readiness_item(payload.model_dump(), request.state.user)


@app.get('/api/performance/status')
def performance_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return performance_reliability_status()


@app.get('/api/performance/workspace')
def performance_workspace_endpoint(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return performance_reliability_workspace(scenario_id)


@app.get('/api/performance/load-tests')
def performance_load_tests_endpoint(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_performance_load_tests(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'load_tests': rows}


@app.post('/api/performance/load-tests')
def performance_run_load_test_endpoint(payload: PerformanceLoadTestCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return run_performance_load_test(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/performance/benchmarks/status')
def performance_benchmark_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return performance_benchmark_status()


@app.get('/api/performance/proof/status')
def performance_proof_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return performance_proof_status()


@app.get('/api/performance/enterprise-scale/status')
def performance_enterprise_scale_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return enterprise_scale_benchmark_status()


@app.get('/api/performance/enterprise-scale/runs')
def performance_enterprise_scale_runs_endpoint(request: Request, limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_enterprise_scale_benchmark_runs(limit)
    return {'count': len(rows), 'enterprise_scale_runs': rows}


@app.get('/api/performance/enterprise-scale/runs/{run_id}')
def performance_enterprise_scale_run_endpoint(run_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return get_enterprise_scale_benchmark_run(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/performance/enterprise-scale/run')
def performance_run_enterprise_scale_endpoint(payload: EnterpriseScaleBenchmarkRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return run_enterprise_scale_benchmark(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post('/api/performance/proof/run')
def performance_proof_run_endpoint(payload: PerformanceBenchmarkRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return run_performance_proof(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/performance/benchmarks')
def performance_benchmark_runs_endpoint(request: Request, scenario_id: int | None = Query(None, ge=1), limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_performance_benchmark_runs(scenario_id, limit)
    return {'scenario_id': scenario_id, 'count': len(rows), 'benchmark_runs': rows}


@app.get('/api/performance/benchmarks/{run_id}')
def performance_benchmark_run_endpoint(run_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return get_performance_benchmark_run(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/performance/benchmarks/run')
def performance_run_benchmark_harness_endpoint(payload: PerformanceBenchmarkRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return run_performance_benchmark_harness(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/performance/index-recommendations')
def performance_index_recommendations_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_performance_index_recommendations()
    return {'count': len(rows), 'recommendations': rows}


@app.post('/api/performance/index-recommendations')
def performance_upsert_index_recommendation_endpoint(payload: IndexRecommendationCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return upsert_performance_index_recommendation(payload.model_dump(), request.state.user)


@app.post('/api/performance/index-recommendations/seed')
def performance_seed_index_strategy_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return seed_performance_index_strategy(request.state.user)


@app.get('/api/performance/jobs')
def performance_jobs_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_performance_jobs()
    return {'count': len(rows), 'jobs': rows}


@app.post('/api/performance/jobs')
def performance_enqueue_job_endpoint(payload: BackgroundJobCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return enqueue_performance_job(payload.model_dump(), request.state.user)


@app.post('/api/performance/jobs/run-next')
def performance_run_next_job_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return run_performance_next_job(request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/performance/jobs/promote-due')
def performance_promote_due_jobs_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    promoted = promote_performance_jobs()
    return {'promoted': promoted}


@app.post('/api/performance/jobs/{job_id}/cancel')
def performance_cancel_job_endpoint(job_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return cancel_performance_job(job_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/performance/job-logs')
def performance_job_logs_endpoint(request: Request, job_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_performance_job_logs(job_id)
    return {'job_id': job_id, 'count': len(rows), 'logs': rows}


@app.get('/api/performance/dead-letters')
def performance_dead_letters_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_performance_dead_letters()
    return {'count': len(rows), 'dead_letters': rows}


@app.get('/api/performance/cache-invalidations')
def performance_cache_invalidations_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_performance_cache_invalidations()
    return {'count': len(rows), 'invalidations': rows}


@app.post('/api/performance/cache-invalidations')
def performance_invalidate_cache_endpoint(payload: CacheInvalidationCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return invalidate_performance_cache(payload.model_dump(), request.state.user)


@app.get('/api/performance/restore-automations')
def performance_restore_automations_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_performance_restore_automations()
    return {'count': len(rows), 'restore_automations': rows}


@app.post('/api/performance/restore-automations')
def performance_run_restore_automation_endpoint(payload: RestoreAutomationCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return run_performance_restore_automation(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/performance/parallel-cubed/status')
def performance_parallel_cubed_status_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return parallel_cubed_engine_status()


@app.get('/api/performance/parallel-cubed/cpu')
def performance_parallel_cubed_cpu_endpoint(request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return parallel_cubed_cpu_topology()


@app.get('/api/performance/parallel-cubed/workspace')
def performance_parallel_cubed_workspace_endpoint(request: Request, scenario_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    return parallel_cubed_engine_workspace(scenario_id)


@app.get('/api/performance/parallel-cubed/runs')
def performance_parallel_cubed_runs_endpoint(request: Request, scenario_id: int | None = Query(None, ge=1), limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_parallel_cubed_runs(scenario_id, limit)
    return {'scenario_id': scenario_id, 'count': len(rows), 'runs': rows}


@app.get('/api/performance/parallel-cubed/runs/{run_id}')
def performance_parallel_cubed_run_endpoint(run_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return get_parallel_cubed_run(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/performance/parallel-cubed/partitions')
def performance_parallel_cubed_partitions_endpoint(request: Request, run_id: int | None = Query(None, ge=1)) -> dict[str, Any]:
    _require(request, 'operations.manage')
    rows = list_parallel_cubed_partitions(run_id)
    return {'run_id': run_id, 'count': len(rows), 'partitions': rows}


@app.post('/api/performance/parallel-cubed/run')
def performance_run_parallel_cubed_endpoint(payload: ParallelCubedRunCreate, request: Request) -> dict[str, Any]:
    _require(request, 'operations.manage')
    try:
        return run_parallel_cubed_engine(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/ledger-depth/status')
def ledger_depth_status_endpoint() -> dict[str, Any]:
    return ledger_depth_status()


@app.get('/api/ledger-depth/basis-summary')
def ledger_depth_basis_summary(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'ledger.read')
    return ledger_basis_summary(scenario_id)


@app.get('/api/ledger-depth/journals')
def ledger_depth_journals(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    _require(request, 'ledger.read')
    rows = list_journal_adjustments(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'journals': rows}


@app.post('/api/ledger-depth/journals')
def ledger_depth_create_journal(payload: JournalAdjustmentCreate, request: Request) -> dict[str, Any]:
    _require(request, 'ledger.write')
    try:
        return create_journal_adjustment(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/ledger-depth/journals/{journal_id}/submit')
def ledger_depth_submit_journal(journal_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'ledger.write')
    try:
        return submit_journal_adjustment(journal_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/ledger-depth/journals/{journal_id}/approve')
def ledger_depth_approve_journal(journal_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'ledger.write')
    try:
        return approve_journal_adjustment(journal_id, request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/ledger-depth/journals/{journal_id}/reject')
def ledger_depth_reject_journal(journal_id: int, payload: ApprovalAction, request: Request) -> dict[str, Any]:
    _require(request, 'ledger.write')
    try:
        return reject_journal_adjustment(journal_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/evidence/status')
def evidence_status_endpoint() -> dict[str, Any]:
    return evidence_status()


@app.get('/api/evidence/comments')
def evidence_comments(
    request: Request,
    entity_type: str | None = Query(None),
    entity_id: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    _require(request, 'reports.read')
    rows = list_comments(entity_type, entity_id, limit)
    return {'count': len(rows), 'comments': rows}


@app.post('/api/evidence/comments')
def evidence_create_comment(payload: EntityCommentCreate, request: Request) -> dict[str, Any]:
    _require(request, 'ledger.write')
    return create_comment(payload.model_dump(), request.state.user)


@app.post('/api/evidence/comments/{comment_id}/resolve')
def evidence_resolve_comment(comment_id: int, request: Request) -> dict[str, Any]:
    _require(request, 'ledger.write')
    try:
        return resolve_comment(comment_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/evidence/attachments')
def evidence_attachments(
    request: Request,
    entity_type: str | None = Query(None),
    entity_id: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    _require(request, 'reports.read')
    rows = list_attachments(entity_type, entity_id, limit)
    return {'count': len(rows), 'attachments': rows}


@app.post('/api/evidence/attachments')
def evidence_create_attachment(payload: EvidenceAttachmentCreate, request: Request) -> dict[str, Any]:
    _require(request, 'ledger.write')
    return create_attachment(payload.model_dump(), request.state.user)


@app.get('/api/evidence/entity')
def evidence_entity(request: Request, entity_type: str = Query(...), entity_id: str = Query(...)) -> dict[str, Any]:
    _require(request, 'reports.read')
    return entity_evidence(entity_type, entity_id)


@app.get('/api/foundation/ledger')
def foundation_ledger(
    request: Request,
    scenario_id: int = Query(..., ge=1),
    include_reversed: bool = Query(False),
    limit: int = Query(500, ge=1, le=5000),
) -> dict[str, Any]:
    scenario = db.fetch_one('SELECT id FROM scenarios WHERE id = ?', (scenario_id,))
    if scenario is None:
        raise HTTPException(status_code=404, detail='Scenario not found.')
    try:
        require_permission(request.state.user, 'ledger.read')
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    rows = list_ledger_entries(scenario_id, include_reversed=include_reversed, limit=limit, user=request.state.user)
    return {'scenario_id': scenario_id, 'count': len(rows), 'entries': rows}


@app.post('/api/foundation/ledger')
def foundation_post_ledger(payload: LedgerEntryCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'ledger.write')
        return append_ledger_entry(payload.model_dump(), actor=request.state.user['email'], user=request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post('/api/foundation/ledger/{entry_id}/reverse')
def foundation_reverse_ledger(entry_id: int, payload: LedgerReverseCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'ledger.reverse')
        return reverse_ledger_entry(entry_id, payload.reason, actor=request.state.user['email'])
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get('/api/foundation/fiscal-periods')
def foundation_fiscal_periods(fiscal_year: str | None = Query(None)) -> dict[str, Any]:
    periods = list_fiscal_periods(fiscal_year)
    return {'count': len(periods), 'periods': periods}


@app.post('/api/foundation/fiscal-periods')
def foundation_upsert_fiscal_period(payload: FiscalPeriodCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'periods.manage')
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return upsert_fiscal_period(payload.model_dump(), actor=request.state.user['email'])


@app.post('/api/foundation/fiscal-periods/{period}/close')
def foundation_close_fiscal_period(period: str, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'periods.manage')
        return set_period_closed(period, True, actor=request.state.user['email'])
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post('/api/foundation/fiscal-periods/{period}/reopen')
def foundation_reopen_fiscal_period(period: str, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'periods.manage')
        return set_period_closed(period, False, actor=request.state.user['email'])
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/foundation/dimensions/hierarchy')
def foundation_dimension_hierarchy() -> dict[str, list[dict[str, Any]]]:
    return dimension_hierarchy()


@app.post('/api/foundation/dimensions')
def foundation_upsert_dimension(payload: DimensionMemberCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'dimensions.manage')
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return create_dimension_member(payload.model_dump(), actor=request.state.user['email'])


@app.get('/api/foundation/migrations')
def foundation_migrations() -> dict[str, Any]:
    migrations = list_migrations()
    return {'count': len(migrations), 'migrations': migrations}


@app.post('/api/foundation/migrations/run')
def foundation_run_migrations() -> dict[str, Any]:
    ensure_foundation_ready()
    migrations = list_migrations()
    return {'applied': True, 'count': len(migrations), 'migrations': migrations}


@app.get('/api/foundation/status')
def foundation_status_endpoint() -> dict[str, Any]:
    return foundation_status()


@app.get('/api/foundation/backups')
def foundation_backups() -> dict[str, Any]:
    backups = list_backups()
    return {'count': len(backups), 'backups': backups}


@app.post('/api/foundation/backups')
def foundation_create_backup(payload: BackupCreate, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'backups.manage')
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return create_backup(note=payload.note, actor=request.state.user['email'])


@app.post('/api/foundation/backups/{backup_key}/restore')
def foundation_restore_backup(backup_key: str, request: Request) -> dict[str, Any]:
    try:
        require_permission(request.state.user, 'backups.manage')
        return restore_backup(backup_key, actor=request.state.user['email'])
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get('/api/parallel-cubed/status')
def parallel_cubed_status() -> dict[str, Any]:
    return finance_flow.status()


@app.post('/api/parallel-cubed/reload')
def parallel_cubed_reload() -> dict[str, Any]:
    finance_flow.reload()
    db.log_audit(
        entity_type='parallel_cubed',
        entity_id='finance_flow',
        action='reloaded',
        actor='api.user',
        detail={'genome': finance_flow.status().get('genomeId')},
        created_at=datetime.now(UTC).isoformat(),
    )
    return finance_flow.status()


@app.post('/api/parallel-cubed/route')
def parallel_cubed_route(payload: dict[str, Any]) -> dict[str, Any]:
    seed_region = str(payload.get('seedRegion', 'foundation')).strip() or 'foundation'
    intent = str(payload.get('intent', '')).strip()
    result = finance_flow.route(
        seed_region=seed_region,
        intent=intent,
        feedback=float(payload.get('feedback', 0.0) or 0.0),
        entropy=float(payload.get('entropy', 0.0) or 0.0),
    )
    db.log_audit(
        entity_type='parallel_cubed',
        entity_id=result['seedRegion'],
        action='route',
        actor='api.user',
        detail={'intent': intent, 'activeRegions': result['activeRegions']},
        created_at=datetime.now(UTC).isoformat(),
    )
    return result


@app.post('/api/parallel-cubed/guard')
def parallel_cubed_guard(payload: dict[str, Any]) -> dict[str, Any]:
    result = finance_flow.guard(payload)
    db.log_audit(
        entity_type='parallel_cubed',
        entity_id='guard',
        action=result['decision']['action'],
        actor='api.user',
        detail=result,
        created_at=datetime.now(UTC).isoformat(),
    )
    return result


@app.get('/api/parallel-cubed/batches')
def parallel_cubed_batches() -> dict[str, Any]:
    return {
        'genome': finance_flow.status(),
        'batches': batches_as_dicts(),
    }


@app.get('/')
def root() -> FileResponse:
    return FileResponse(STATIC_DIR / 'index.html')


@app.get('/api/bootstrap')
def bootstrap(request: Request) -> dict[str, Any]:
    scenarios = get_scenarios()
    active_scenario = scenarios[0] if scenarios else None
    scenario_id = active_scenario['id'] if active_scenario else None
    return {
        'scenarios': scenarios,
        'activeScenario': active_scenario,
        'dimensions': dimensions(),
        'workflows': get_workflows(scenario_id) if scenario_id else [],
        'drivers': get_drivers(scenario_id) if scenario_id else [],
        'summary': summary_report(request, scenario_id) if scenario_id else None,
        'lineItems': get_line_items(scenario_id, request) if scenario_id else [],
        'integrations': get_integrations(),
        'user': request.state.user,
    }


@app.get('/api/roadmap')
def roadmap() -> dict[str, Any]:
    return {
        'phase_1': [
            'scenario manager',
            'budget entry',
            'approval workflow',
            'summary reporting',
            'audit trail',
            'seeded integrations',
        ],
        'phase_2': [
            'enrollment planning',
            'faculty planning',
            'position control',
            'cash flow',
            'variance explanations',
        ],
        'phase_3': [
            'consolidation',
            'reconciliation',
            'close management',
            'governed AI narrative generation',
            'self-service report builder',
        ],
    }


app.mount('/static', StaticFiles(directory=STATIC_DIR), name='static')


def _ledger_to_line_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        'id': row['id'],
        'scenario_id': row['scenario_id'],
        'department_code': row['department_code'],
        'fund_code': row['fund_code'],
        'account_code': row['account_code'],
        'period': row['period'],
        'amount': row['amount'],
        'notes': row['notes'],
        'source': row['source'],
        'driver_key': row.get('driver_key'),
    }


def _require(request: Request, permission: str) -> None:
    try:
        require_permission(request.state.user, permission)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
