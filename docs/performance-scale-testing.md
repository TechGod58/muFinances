# Performance And Scale Testing

B83 adds a repeatable performance harness for muFinances.

## Benchmarked Areas

- Ledger queries.
- Import staging and drill-back.
- Report/export artifact lookups.
- Background job queue leasing.
- Formula/model calculations.
- Allocations.
- Consolidation.

## Seed Plan

The default large-data seed plan estimates:

```text
100 departments * 500 accounts * 36 periods * 6 scenarios = 10,800,000 ledger rows
```

The harness records the seed plan used for every benchmark run so performance regressions can be compared fairly.

## Required Indexes

The service recommends indexes for:

- Ledger period/department/account queries.
- Scenario/period comparisons.
- Import batch status and row lookup.
- Export artifact dashboards.
- Background job queue leasing.

## Files

- `services/performance_benchmarks.py`
- `tests/test_performance_benchmarks.py`
- `schema/postgresql/0083_performance_scale_testing.up.sql`
- `schema/postgresql/0083_performance_scale_testing.down.sql`

## Production Rule

Any release that changes ledger calculations, imports, report generation, formulas, allocations, or consolidation should record a benchmark run and compare it against thresholds before promotion.

