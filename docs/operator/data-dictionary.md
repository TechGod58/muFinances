# muFinances Data Dictionary

## Core Dimensions

| Field | Meaning |
| --- | --- |
| `scenario_id` | Planning version or what-if scenario |
| `fiscal_period` | Fiscal calendar period |
| `department_code` | Campus responsibility center |
| `account_code` | Chart of accounts code |
| `fund_code` | Funding source or restriction bucket |
| `entity_code` | Legal/reporting entity |
| `basis` | Actual, budget, forecast, or scenario |

## Control Tables

| Table | Purpose |
| --- | --- |
| `schema_migrations` | Applied database migrations |
| `idempotency_keys` | Duplicate-post prevention |
| `audit_chain` | Tamper-evident record chain |
| `import_batches` | Import lifecycle header |
| `import_staged_rows` | Validated import rows |
| `export_artifacts` | Export validation records |
| `background_jobs` | Durable job queue |
| `backup_manifests` | Backup evidence |
| `restore_drills` | Restore proof records |

## Sensitive Data

Passwords, tokens, secrets, API keys, SSNs, account numbers, and similar fields must be masked and excluded from plain logs.

