# Production Auth And Access Enforcement

B74 adds backend primitives for production access enforcement. The UI can show access status, but the server must enforce these rules before returning financial data or accepting postings.

## Enforcement Points

- Domain: user identities must belong to an allowed domain, defaulting to `manchester.edu`.
- Network: optional campus/VPN CIDR ranges can be configured and checked from the request client host.
- Active Directory OU: optional allowed OU path fragments can be required after SSO/LDAP lookup.
- Permissions: routes should check named permissions rather than checking hard-coded roles inline.
- Sessions: session tokens should be stored as hashes, expire, and be tied to request metadata.
- SSO: production SSO configuration must include provider, issuer, client ID, callback URL, and claim names.

## Role Matrix

| Permission | Roles | Purpose |
| --- | --- | --- |
| `ledger.read` | admin, controller, budget_office, department_planner, executive | Read ledger data |
| `ledger.post` | admin, controller, budget_office | Post ledger lines and adjustments |
| `budget.submit` | admin, budget_office, department_planner | Submit department budget data |
| `budget.approve` | admin, budget_office, controller | Approve budget submissions |
| `forecast.publish` | admin, budget_office, controller | Publish forecast/scenario outputs |
| `close.manage` | admin, controller | Manage close and reconciliation workflows |
| `imports.approve` | admin, controller, integration_admin | Approve staged imports |
| `security.admin` | admin, security_admin | Manage users, roles, SSO, and access policy |
| `audit.read` | admin, controller, auditor | Read audit records and evidence |

## Route Migration Rule

Every route returning or mutating financial data should:

1. Resolve the signed-in identity.
2. Build a `ServiceContext`.
3. Validate domain, network, and OU requirements at session creation.
4. Check the named permission needed by the route.
5. Record high-risk changes in the audit service.

## Files

- `services/access_enforcement.py`
- `services/session_security.py`
- `services/sso_readiness.py`
- `services/security.py`

