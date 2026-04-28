# Campus FPM Base

A local-first financial performance management scaffold for replacing Prophix on campus.

This is not a Prophix clone. The core design is intentionally different:

- **Driver graph engine** instead of spreadsheet-template-first planning.
- **Scenario ledger** with immutable-style audit logging for every material change.
- **API-first modules** so ERP, HRIS, CRM, SIS, and BI connectors stay separate from planning logic.
- **Single-node local deployment** for internal campus hosting on `http://localhost:3200`.

## What this base already includes

- Scenario manager
- Budget line entry API + UI
- Driver store and formula evaluation
- Forecast runner
- Summary reporting
- Workflow registry + state advancement
- Integration registry
- Audit log
- Seeded campus-oriented demo data

## What you build next for deeper parity

- Enrollment and tuition planning
- Faculty load planning
- Position control and workforce planning
- Grant and project budgeting
- Capital planning
- Cash flow planning
- Consolidation and intercompany logic
- Reconciliation and close management
- Narrative reporting / variance commentary
- Governed AI assistant
- Report builder and scheduled distribution
- SSO / LDAP / AD auth
- Row-level security and sensitive-compensation masking

## Working directory

```bash
cd /mnt/data/campus-fpm-base
```

## File layout

```text
campus-fpm-base/
├── app/
│   ├── __init__.py
│   ├── db.py
│   ├── main.py
│   ├── schemas.py
│   └── services/
│       ├── forecast_engine.py
│       └── seed.py
├── static/
│   ├── app.js
│   ├── index.html
│   └── styles.css
├── tests/
│   └── test_api.py
├── requirements.txt
└── README.md
```

## Run it

Create a virtual environment if needed, then install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Start the app on port `3200`:

```bash
uvicorn app.main:app --host 127.0.0.1 --port 3200 --reload
```

Open:

```text
http://localhost:3200
```

## Minimal test

```bash
pytest -q
```

## API snapshot

- `GET /api/health`
- `GET /api/bootstrap`
- `GET /api/scenarios`
- `POST /api/scenarios`
- `GET /api/scenarios/{scenario_id}/drivers`
- `GET /api/scenarios/{scenario_id}/line-items`
- `POST /api/scenarios/{scenario_id}/line-items`
- `POST /api/scenarios/{scenario_id}/forecast/run`
- `GET /api/reports/summary?scenario_id=1`
- `GET /api/workflows?scenario_id=1`
- `POST /api/workflows`
- `POST /api/workflows/{workflow_id}/advance`
- `GET /api/integrations`
- `GET /api/audit-logs`

## Next recommendation

Swap SQLite for PostgreSQL once the first 3 campus modules are stable, then split the forecast runner into its own service so planning runs do not block the UI.
