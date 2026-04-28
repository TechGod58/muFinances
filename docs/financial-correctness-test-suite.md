# Financial Correctness Test Suite

B84 adds fixture-based financial correctness tests.

## Covered Areas

- Actual, budget, forecast, and scenario separation.
- Variance amount and variance percentage.
- Close readiness.
- Reconciliation tolerance checks.
- Consolidation eliminations.
- Allocation math and rounding.
- Approval completion.

## Files

- `services/financial_correctness.py`
- `tests/fixtures/financial_correctness_cases.json`
- `tests/test_financial_correctness.py`

## Rule

Every future change to ledger posting, forecast/scenario handling, close, reconciliation, consolidation, allocations, approvals, or variance reporting should add or update fixture cases.

