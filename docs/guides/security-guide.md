# muFinances Security Guide

## Purpose

This guide locks the production security operating model for muFinances. It covers identity, SSO handoff, AD/OU mapping, Manchester domain/VPN enforcement, secrets, sessions, permissions, masking, audit, and support diagnostics.

## Identity And Access

Production access should use Manchester SSO and AD/OU group mapping. Local accounts are retained for controlled fallback only. Role assignment must map to the least permission set required by the user's finance responsibility.

## Domain And VPN Enforcement

The application must only be reachable from allowed Manchester hosts, on-prem networks, or VPN-approved addresses. Domain/VPN checks should be visible in the security administration workspace and reviewed during release readiness.

## Permissions And Row-Level Access

Every API should evaluate server-side permission checks. Row-level access should restrict department, fund, entity, grant, and other dimensional views. Support staff should use permission simulation before changing user access.

## Session Controls

Sessions should expire, support revocation, and produce diagnostic evidence for active sessions, revoked sessions, and last login. Admin impersonation must be audited and limited to support cases.

## Secrets

Production secrets must not live in source code. DSNs, field keys, connector credentials, AI provider settings, and brokerage credentials should be loaded from protected secret storage. Credential values must remain masked in UI and logs.

## Audit And Support

Audit logs should capture login, user changes, role changes, posting, import, export, support bundle creation, failed job replay, connector test mode, and issue reports. Support bundles must redact secrets and include replay IDs so operators can trace incidents without exposing sensitive values.

## Review Cadence

Run access reviews before major releases and at a scheduled interval. Review SoD findings, admin users, stale accounts, connector credentials, and open support issues. Security signoff is required before production promotion.
