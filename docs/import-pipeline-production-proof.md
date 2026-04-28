# Import Pipeline Production Proof

B78 adds production controls around imports.

## Capabilities

- Versioned import mappings.
- Chunked CSV row streaming for large files.
- Row validation and rejection capture.
- Staged accepted rows before posting.
- Approval and rejection workflow.
- Rollback handling.
- Source drill-back by batch and row hash.
- Audit records for mapping creation, preview, approval, rejection, and rollback.

## Files

- `services/import_pipeline.py`
- `tests/test_import_pipeline.py`
- `schema/postgresql/0078_import_pipeline_production_proof.up.sql`
- `schema/postgresql/0078_import_pipeline_production_proof.down.sql`

## Production Rule

Imported rows should not post directly into the ledger. They should move through:

```text
uploaded -> staged -> validated -> approved -> posted
```

Rejected rows and rollback events must remain available for audit.

