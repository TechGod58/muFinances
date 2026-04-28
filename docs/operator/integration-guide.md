# muFinances Integration Guide

## Supported Integration Patterns

- CSV/XLSX import.
- Versioned import mappings.
- Staged validation and rejection workflow.
- Connector framework for ERP, SIS, HR, payroll, grants, banking, brokerage, and agent integrations.
- BI/API export manifests.

## Import Lifecycle

```text
uploaded -> staged -> validated -> approved -> posted
```

## Production Requirements

- Credentials must be stored in the vault.
- Mapping versions must be retained.
- Rejections must remain auditable.
- Connector syncs should run as background jobs.
- Drill-back must identify source batch and row hash.

