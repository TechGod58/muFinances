# Parallel Cubed for muFinances

Parallel Cubed is the wiring layer for muFinances. It gives dimension to flow by
turning finance capability areas into active regions, connecting those regions
with weighted synapses, and routing work through bound modules with guard checks.

The muStatistics version is built around qualitative retrieval prefiltering. The
muFinances version is redesigned around financial control flow:

- regions: finance capability areas
- synapses: dependency and data-flow strength between capabilities
- bindings: local tables, services, APIs, or future modules
- activation: which finance regions need to wake up for a requested operation
- energy: recent activity/load per region
- guard: finance risk decision for continue/checkpoint/halt
- batches: delivery sequence derived from the Prophix replacement action plan

## Region Map

| Region | Purpose |
| --- | --- |
| `foundation` | Ledger, dimensions, fiscal periods, audit, migrations |
| `planning` | Operating budget, enrollment, workforce, grants, capital |
| `drivers` | Driver graph, scenarios, forecasts, predictive methods |
| `reporting` | Reports, dashboards, statements, narrative reporting |
| `close` | Reconciliation, intercompany, consolidation, close checklist |
| `integrations` | File import, ERP sync, Power BI export, connector jobs |
| `security` | Auth, roles, row-level access, masking |
| `automation` | Variance, anomaly, budget, and reconciliation assistants |
| `experience` | Role workspaces and module shell |
| `operations` | Deployment, backups, health checks, runbooks |

## Batch Plan

### B01 - Foundation Ledger Hardening

Phase 1. Seed region: `foundation`.

Make dimensional ledger, fiscal periods, dimensions, audit, and migrations the
durable core.

Deliverables:
- ledger service
- dimension hierarchy
- migration runner
- backup/restore hooks

Completion signal: all current APIs run from the planning ledger.

### B02 - Security And Control Baseline

Phase 1. Seed region: `security`.

Add identity, roles, row-level access, and masking before sensitive planning
modules expand.

Deliverables:
- local auth
- role permissions
- row-level filters
- sensitive compensation masking

Dependencies: `B01`.

Completion signal: every API can evaluate actor, role, and allowed dimensions.

### B03 - Operating Budget Workspace

Phase 2. Seed region: `planning`.

Replace spreadsheet budget collection with governed department planning.

Deliverables:
- submission workflow
- budget assumptions
- transfers
- one-time vs recurring lines

Dependencies: `B01`, `B02`.

Completion signal: departments can submit and route operating budgets.

### B04 - Enrollment Tuition Planning

Phase 2. Seed region: `planning`.

Model headcount, rates, discounting, retention, and tuition revenue.

Deliverables:
- enrollment drivers
- tuition rates
- discounting
- term forecast

Dependencies: `B03`.

Completion signal: tuition revenue flows into the ledger by scenario and period.

### B05 - Workforce Faculty Grants Capital

Phase 2. Seed region: `planning`.

Add the major campus planning subledgers.

Deliverables:
- position control
- faculty load
- grant budgets
- capital requests

Dependencies: `B03`.

Completion signal: major campus planning lines post through controlled subledgers.

### B06 - Forecast And Scenario Engine

Phase 3. Seed region: `drivers`.

Expand driver graph, scenario cloning, comparison, and forecast methods.

Deliverables:
- typed drivers
- scenario compare
- rolling forecast
- confidence intervals

Dependencies: `B01`, `B03`.

Completion signal: scenario outputs can be compared and explained by driver lineage.

### B07 - Reporting Analytics Layer

Phase 4. Seed region: `reporting`.

Build report definitions, dashboards, statements, and export packages.

Deliverables:
- report builder
- dashboard builder
- financial statements
- scheduled exports

Dependencies: `B01`, `B06`.

Completion signal: board-ready reports run from saved definitions.

### B08 - Close Reconciliation Consolidation

Phase 5. Seed region: `close`.

Add close checklist, account reconciliation, intercompany, and consolidation.

Deliverables:
- close calendar
- reconciliation matching
- eliminations
- consolidation runs

Dependencies: `B01`, `B02`, `B07`.

Completion signal: close cycle can be tracked, reconciled, consolidated, and audited.

### B09 - Campus Integrations

Phase 6. Seed region: `integrations`.

Bring real source systems into controlled imports and exports.

Deliverables:
- CSV/XLSX import
- connector jobs
- sync logs
- Power BI export

Dependencies: `B01`, `B02`.

Completion signal: imports have mappings, validation, rejection handling, and audit
lineage.

### B10 - Governed Automation

Phase 7. Seed region: `automation`.

Add variance, anomaly, budget, and reconciliation assistants with human approval.

Deliverables:
- variance assistant
- anomaly detection
- budget assistant
- match suggestions

Dependencies: `B06`, `B07`, `B08`.

Completion signal: automation produces cited recommendations and never posts silently.

### B11 - Workspace UX Completion

Phase 8. Seed region: `experience`.

Turn the prototype into role-specific campus finance workspaces.

Deliverables:
- module shell
- planner workspace
- controller workspace
- executive dashboard

Dependencies: `B03`, `B07`.

Completion signal: users can complete core workflows without raw API knowledge.

### B12 - Deployment Operations

Phase 9. Seed region: `operations`.

Package, monitor, back up, and document campus-ready operation.

Deliverables:
- Windows service or Docker
- health checks
- restore tests
- runbooks

Dependencies: `B01`, `B02`, `B09`.

Completion signal: the app is recoverable, observable, and runnable on localhost or
an internal campus host.

## API Surface

- `GET /api/parallel-cubed/status`
- `POST /api/parallel-cubed/reload`
- `POST /api/parallel-cubed/route`
- `POST /api/parallel-cubed/guard`
- `GET /api/parallel-cubed/batches`

