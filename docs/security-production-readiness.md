# Security Review And Production Readiness

B82 adds production-readiness review checks before muFinances is deployed internally.

## Reviewed Areas

- Required production secrets.
- Unsafe default secret values.
- Required response security headers.
- Production HSTS requirement.
- Permission registry completeness.
- Sensitive-field masking policy.
- Audit/admin separation of duties.
- Production fail-fast behavior.

## Files

- `services/production_readiness.py`
- `tests/test_production_readiness.py`
- `schema/postgresql/0082_security_production_readiness.up.sql`
- `schema/postgresql/0082_security_production_readiness.down.sql`

## Production Rule

When `MUFINANCES_MODE=production`, blocker findings must prevent startup. Warnings can be shown in the admin readiness dashboard, but blockers must stop deployment.

## Field Encryption And Secrets

B136/B137 replaces legacy `enc:v1` XOR-style field protection with authenticated `enc:v2` field encryption using Fernet from `cryptography`. Existing `enc:v1` values remain readable only so the migration hook can re-encrypt them.

Required production setup:

- Set `CAMPUS_FPM_FIELD_KEY_FILE` to a mounted secret file, or set `CAMPUS_FPM_FIELD_KEY` through the production secret manager.
- Do not use `local-dev-field-key-change-before-production` outside local development.
- Set `CAMPUS_FPM_FIELD_KEY_VERSION` when rotating keys.
- Put old keys in `CAMPUS_FPM_FIELD_KEY_PREVIOUS` during the rotation window so old values can be decrypted and migrated.
- Optionally set `CAMPUS_FPM_FIELD_KEY_ENVELOPE_MASTER_FILE` for envelope-style key derivation from a mounted master secret.

Production startup fails closed when field encryption secrets are missing, unreadable, defaulted, or when authenticated encryption dependencies are unavailable.

Operators can check `/api/security/encryption/status` and run `/api/security/encryption/migrate` as a security administrator after loading the new key.

## Browser Security Controls

B138 restricts CORS to `CAMPUS_FPM_ALLOWED_ORIGINS` in production, adds request rate limiting, login lockout tracking, cookie/CSRF-capable session mode, and stricter response headers.

For safer browser sessions, set `CAMPUS_FPM_SESSION_COOKIE_MODE=true` in production. In that mode the session token is stored in an HttpOnly cookie, browser mutations must send `X-CSRF-Token`, and the frontend avoids persisting the bearer token in `localStorage`.
