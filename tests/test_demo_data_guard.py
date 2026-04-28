import pytest

from services.base import PermissionDenied, ValidationError
from services.demo_data import DemoDataGuard, DemoDataPolicy, RuntimeMode, policy_from_environment


def test_demo_seed_allowed_in_development_when_explicitly_enabled():
    guard = DemoDataGuard(DemoDataPolicy(RuntimeMode.DEVELOPMENT, allow_demo_seed=True))

    guard.require_demo_seed_allowed()
    assert guard.seed_namespace() == "demo:development"


def test_demo_seed_blocked_without_explicit_toggle():
    guard = DemoDataGuard(DemoDataPolicy(RuntimeMode.DEVELOPMENT))

    with pytest.raises(PermissionDenied):
        guard.require_demo_seed_allowed()


def test_production_blocks_unsafe_demo_settings():
    guard = DemoDataGuard(
        DemoDataPolicy(
            RuntimeMode.PRODUCTION,
            allow_demo_seed=True,
            allow_sample_logins=True,
            allow_mock_connectors=True,
            allow_unsafe_defaults=True,
        )
    )

    with pytest.raises(ValidationError):
        guard.assert_production_safe()


def test_policy_from_environment_reads_flags():
    policy = policy_from_environment(
        {
            "MUFINANCES_MODE": "test",
            "MUFINANCES_ALLOW_DEMO_SEED": "true",
            "MUFINANCES_ALLOW_SAMPLE_LOGINS": "1",
            "MUFINANCES_ALLOW_MOCK_CONNECTORS": "yes",
        }
    )

    assert policy.runtime_mode is RuntimeMode.TEST
    assert policy.allow_demo_seed is True
    assert policy.allow_sample_logins is True
    assert policy.allow_mock_connectors is True

