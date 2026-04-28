# Report And Export Validation

B79 adds artifact validation for production exports.

## Covered Artifacts

- PDF reports.
- Excel workbooks.
- PowerPoint decks.
- PNG/SVG chart exports.
- Board package ZIPs.
- BI/API JSON exports.

## Validation Checks

- Content type matches export type.
- Artifact is non-empty.
- PDF/board package pagination metadata is captured.
- Excel sheet count is captured.
- PowerPoint slide count is captured.
- Chart exports include a chart spec hash.
- Board packages contain required artifacts.
- Every validation produces an audit record.

## Files

- `services/export_validation.py`
- `tests/test_export_validation.py`
- `schema/postgresql/0079_report_export_validation.up.sql`
- `schema/postgresql/0079_report_export_validation.down.sql`

## Production Rule

Export jobs should not mark artifacts as release-ready until `ExportValidationService` marks them valid. Warnings may be allowed for draft artifacts, but board-package release should require zero errors.

