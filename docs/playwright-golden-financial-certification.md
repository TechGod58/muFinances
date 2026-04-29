# B142-B144 Playwright And Golden Finance Certification

## B142 Playwright CI Coverage

The Playwright workflow runs from `.github/workflows/playwright-ci.yml`.

Coverage areas:

- Login and no-blank-screen startup.
- Workspace toggle menu and visible section recovery.
- Dock/undock smoke coverage.
- Chat undock/pop-out fallback coverage.
- Import and export dialog discoverability.
- Reporting workspace routing.
- Mobile and tablet viewport checks.
- Accessibility smoke coverage through the existing Playwright specs.

## B143 Golden Financial Test Packs

Golden packs live in `app/fixtures/golden_financial_test_packs.json`.

The fixture covers actuals, budget, forecast, close readiness, reconciliation, consolidation eliminations, FX translation, allocation, reporting outputs, and secure audit trail expectations.

API:

- `GET /api/reporting/golden-test-packs/status`
- `GET /api/reporting/golden-test-packs`
- `POST /api/reporting/golden-test-packs/run`
- `GET /api/reporting/golden-test-packs/runs`

## B144 Statement Accuracy Certification

The statement accuracy certification seeds a golden scenario, generates financial statements, fund/grant reports, departmental P&L, board package artifacts, footnotes, and charts, then compares results against the golden pack.

API:

- `GET /api/reporting/statement-accuracy-certification/status`
- `POST /api/reporting/statement-accuracy-certification/run`
- `GET /api/reporting/statement-accuracy-certification/runs`
