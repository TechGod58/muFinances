# muFinances Admin Guide

## Responsibilities

- Manage users, roles, permissions, and SSO readiness.
- Confirm Manchester domain, VPN/network, and AD/OU access rules.
- Monitor migrations, workers, backups, health checks, and audit logs.
- Review production readiness blockers before promotion.
- Run access review certification and SoD checks.

## Daily Checks

- Production readiness dashboard has no blocker alerts.
- Worker queue has no dead-letter jobs.
- Latest backup is verified.
- Last restore drill is current.
- Audit chain has no verification failures.
- No unapproved admin impersonation sessions are open.

## High-Risk Actions

- Production restore requires explicit approval.
- Admin impersonation must include a reason and audit trail.
- Demo seed data, sample logins, and mock connectors must be blocked in production.
- Security headers and secret checks must pass before release.

