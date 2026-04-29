# B145-B147 Consolidation, COA, And Seed Governance

## B145 Consolidation Golden Cases

The consolidation golden case proves the advanced consolidation path against fixed expected outputs:

- multi-entity ownership chains and effective ownership;
- minority interest;
- intercompany matching and approved eliminations;
- FX translation and CTA;
- GAAP/book bridge placeholders;
- consolidation journals and supplemental schedules;
- audit-ready consolidation report sections.

Endpoints:

- `GET /api/close/consolidation-golden-cases/status`
- `POST /api/close/consolidation-golden-cases/run`
- `GET /api/close/consolidation-golden-cases/runs`

Fixture:

- `app/fixtures/consolidation_golden_cases.json`

## B146 Chart Of Accounts And Sign Convention Governance

COA governance records formal account metadata: hierarchy, account type, normal balance, sign multiplier, statement mapping, effective dating, and validation rules.

Endpoints:

- `GET /api/data-hub/chart-of-accounts/status`
- `GET /api/data-hub/chart-of-accounts/accounts`
- `POST /api/data-hub/chart-of-accounts/accounts`
- `GET /api/data-hub/chart-of-accounts/statement-mappings`
- `POST /api/data-hub/chart-of-accounts/seed`
- `POST /api/data-hub/chart-of-accounts/validate`
- `GET /api/data-hub/chart-of-accounts/validation-runs`

## B147 Seed And Demo Data Separation Enforcement

Startup now checks seed/demo safety before demo seed data can be inserted. Production mode blocks demo seed, sample logins, mock connectors, and unsafe defaults.

Important environment settings:

- `CAMPUS_FPM_ENV=production` or `MUFINANCES_MODE=production`
- `MUFINANCES_SEED_MODE=none` for production
- `MUFINANCES_ALLOW_DEMO_SEED` must not be enabled in production
- `MUFINANCES_ALLOW_SAMPLE_LOGINS` must not be enabled in production
- `MUFINANCES_ALLOW_MOCK_CONNECTORS` must not be enabled in production
- `MUFINANCES_ALLOW_UNSAFE_DEFAULTS` must not be enabled in production

Endpoint:

- `GET /api/production-ops/seed-demo-enforcement/status`

