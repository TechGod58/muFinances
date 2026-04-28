import json
from pathlib import Path

import pytest

from services.base import ValidationError
from services.financial_correctness import FinancialCorrectnessService


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "financial_correctness_cases.json"


@pytest.mark.parametrize("case", json.loads(FIXTURE_PATH.read_text(encoding="utf-8")))
def test_financial_correctness_fixture(case):
    service = FinancialCorrectnessService()

    result = service.evaluate_fixture(case)

    for key, expected_value in case["expected"].items():
        assert result[key] == expected_value


def test_allocation_rejects_zero_driver_total():
    service = FinancialCorrectnessService()

    with pytest.raises(ValidationError):
        service.allocate("100.00", {"ART": 0, "SCI": 0})


def test_invalid_basis_is_rejected():
    service = FinancialCorrectnessService()

    with pytest.raises(ValidationError):
        service.normalize_ledger(
            [
                {
                    "basis": "made_up",
                    "account": "TUITION",
                    "department": "SCI",
                    "period": "2026-01",
                    "amount": "100.00",
                }
            ]
        )

