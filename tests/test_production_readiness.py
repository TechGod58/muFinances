import pytest

from services.base import ValidationError
from services.production_readiness import (
    ProductionReadinessPolicy,
    ProductionReadinessService,
    ReadinessSeverity,
)


def test_missing_required_secret_is_blocker():
    service = ProductionReadinessService(ProductionReadinessPolicy(production_mode=True))

    findings = service.review_environment({})

    assert any(finding.severity is ReadinessSeverity.BLOCKER for finding in findings)


def test_short_or_default_secret_is_blocker():
    service = ProductionReadinessService(ProductionReadinessPolicy(production_mode=True))

    findings = service.review_environment(
        {
            "MUFINANCES_SECRET_KEY": "changeme",
            "MUFINANCES_DATABASE_URL": "postgresql://example",
        }
    )

    assert any(finding.code.startswith("secret.unsafe") for finding in findings)


def test_required_security_headers_are_checked():
    service = ProductionReadinessService(ProductionReadinessPolicy(production_mode=True))

    findings = service.review_headers({"X-Content-Type-Options": "nosniff"})

    assert any(finding.code == "header.x-frame-options" for finding in findings)
    assert any(finding.code == "header.hsts" for finding in findings)


def test_fail_fast_raises_for_production_blockers():
    service = ProductionReadinessService(ProductionReadinessPolicy(production_mode=True))
    findings = service.review_environment({})

    with pytest.raises(ValidationError):
        service.fail_fast(findings)


def test_masking_policy_reports_missing_sensitive_fields():
    service = ProductionReadinessService(ProductionReadinessPolicy(production_mode=False))

    findings = service.review_masking(["password"])

    assert any(finding.code == "masking.missing.token" for finding in findings)

