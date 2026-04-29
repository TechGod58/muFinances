# B151-B152 Parity Matrix And UAT Expansion

## B151 Minimum Viable Prophix Parity Matrix

B151 records a pass/fail/evidence matrix for the minimum viable Prophix-class workflow set:

- budgeting;
- forecasting;
- reporting;
- close;
- consolidation;
- intercompany;
- integrations;
- security;
- workflow;
- AI;
- Excel/Office;
- audit;
- operations.

Endpoints:

- `GET /api/parity/minimum-viable/status`
- `POST /api/parity/minimum-viable/run`
- `GET /api/parity/minimum-viable/runs`
- `GET /api/parity/minimum-viable/runs/{run_id}`

## B152 UAT Script Expansion

B152 expands role-based UAT from six roles to eight:

- budget office;
- controller;
- department planner;
- grants;
- executive;
- IT admin;
- auditor;
- integration admin.

The UAT run records scripts, results, failures, fixes, retests, and signoffs. Failures are not considered closed until the retest record is passed and the role signoff is captured.

Endpoints:

- `GET /api/user-acceptance/status`
- `POST /api/user-acceptance/run`
- `GET /api/user-acceptance/runs`
- `GET /api/user-acceptance/runs/{run_id}`

