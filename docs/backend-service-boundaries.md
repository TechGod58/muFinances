# Backend Service Boundaries

B73 introduces service modules so route handlers can become thin request/response adapters.

## Services

- `LedgerService`: ledger query and idempotent posting workflow.
- `BudgetService`: department submissions and budget workflow persistence.
- `ForecastService`: scenarios, cloning, and forecast lifecycle actions.
- `ImportService`: import mappings and import batch registration.
- `ReportService`: saved reports, snapshots, and export artifacts.
- `WorkflowService`: tasks, approvals, and review decisions.
- `SecurityService`: role checks and high-risk action gates.
- `AuditService`: central audit logging.

## Route Migration Rule

Route handlers should:

1. Parse and validate HTTP input.
2. Build a `ServiceContext`.
3. Call one service method.
4. Return the service result.

Route handlers should not:

- Contain ledger posting rules.
- Decide approval eligibility.
- Write audit rows directly.
- Duplicate import validation rules.
- Build financial report calculations inline.

## B73 Completion Criteria

B73 is complete when the major API routes call these services directly and route-level business logic has been removed. The first service boundary modules are in place; route migration should happen once the local runtime can run tests again.

