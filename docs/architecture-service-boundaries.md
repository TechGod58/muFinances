# muFinances Service Boundaries

## API layer

New endpoint groups belong in `app/routers/` as `APIRouter` modules. `app/main.py`
remains the composition root for startup, middleware, static files, and legacy
routes while older service areas are extracted in small batches.

Current router slices:

- `app/routers/health.py`: public health probe.
- `app/routers/auth.py`: local login, password-change enforcement, SSO bootstrap,
  SSO placeholders, and current-user profile.
- `app/routers/security_admin.py`, `budget.py`, `ledger.py`, `reporting.py`,
  `close.py`, `integrations.py`, `operations.py`, `ai.py`, `workflow.py`, and
  `contracts.py`: authoritative router slices for their matching API domains.

## B154 route retirement rule

`app/main.py` still contains legacy route functions while the remaining service
areas are retired in place. At startup, `_deduplicate_api_routes()` keeps the
first registered API method/path and removes later duplicates from the runtime
route table. Because router modules are included before legacy route functions,
router-owned endpoints win over the older `app.main` definitions.

New work should not add `@app.get`, `@app.post`, or other API route decorators
to `app/main.py`. Add a router module or extend the matching router instead.

## Service layer

Business rules stay in `app/services/`. Routers should validate transport-level
input, call one service function, and translate service exceptions into HTTP
responses.

## Database layer

`app/db.py` remains the runtime compatibility layer for SQLite/PostgreSQL.
New schema work should be staged under `app/schema_files/` as focused fragments
before being registered in `app/services/foundation.py`.

## Browser layer

`static/app.js` is still the legacy shell. New browser-side code should be added
under `static/modules/` and exposed through `window.muFinancesModules` until the
shell is converted to native ES modules.

## Extraction rule

Do not move unrelated endpoints together. Extract one service area at a time,
add a focused regression test, and keep the public URL contract unchanged.
