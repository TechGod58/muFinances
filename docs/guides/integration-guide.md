# Integration Guide

## Purpose And Scope

This guide is for operators who configure imports, connector metadata, mappings, validation rules, credentials, sync jobs, and source drill-back. It applies to ERP, SIS, HR, payroll, grants, banking, CRM, BI/API export, and brokerage read-only connector paths.

## Connectors

Create connectors for each source system. Each connector should have:

- Connector key and display name.
- System type and direction.
- Adapter key and adapter contract.
- Mapping template and version.
- Validation rules.
- Credential vault reference.
- Health check history.

Do not use a connector in production until health check status is healthy and a test import has passed validation.

## Imports

Use mapping templates to normalize source columns into muFinances fields. Import batches should show source name, row count, accepted rows, rejected rows, stream chunks, and mapping version.

For large files, use streaming import paths and review chunk logs. Rejections should be corrected at the source system when possible. If a correction is made only in muFinances, document why in the import batch notes.

## Mapping And Validation

Mapping templates should be versioned. When source layouts change:

1. Create a new mapping version.
2. Run preview import.
3. Validate required dimensions, periods, amounts, and source record IDs.
4. Review rejection handling.
5. Approve only after a reviewer confirms field-level mappings.

Use drill-back validation to confirm imported ledger rows can be traced to the source record.

## Secrets

Store only credential references and masked values in muFinances. Production credentials belong in Docker secrets, Windows service environment variables, campus vault tooling, or the approved credential provider.

Rotate API keys and OAuth secrets on the schedule required by the source system owner. Update credential metadata with expiration and rotation dates.

## Sync Logs

Review connector sync logs after every scheduled job. Failed jobs should create retry events and human-readable error messages. Include connector key, sync job key, trace ID, and source file or endpoint in support tickets.

## Recovery

For failed imports:

1. Stop scheduled retries if repeated failures would duplicate data.
2. Review rejected rows and sync logs.
3. Correct source data or mapping rules.
4. Re-run preview before posting.
5. Use idempotency keys to avoid duplicate ledger rows.
6. Attach the resolution note to the connector or import batch.

For credential failures, rotate the credential and run connector health before resuming sync jobs.
