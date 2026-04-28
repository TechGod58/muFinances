from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Mapping

from .base import PermissionDenied, ValidationError


class RuntimeMode(str, Enum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


@dataclass(frozen=True)
class DemoDataPolicy:
    runtime_mode: RuntimeMode
    allow_demo_seed: bool = False
    allow_sample_logins: bool = False
    allow_mock_connectors: bool = False
    allow_unsafe_defaults: bool = False

    @property
    def production(self) -> bool:
        return self.runtime_mode is RuntimeMode.PRODUCTION


def policy_from_environment(env: Mapping[str, str] | None = None) -> DemoDataPolicy:
    env = env or os.environ
    mode = RuntimeMode(str(env.get("MUFINANCES_MODE", "development")).lower())
    return DemoDataPolicy(
        runtime_mode=mode,
        allow_demo_seed=str(env.get("MUFINANCES_ALLOW_DEMO_SEED", "")).lower() in {"1", "true", "yes"},
        allow_sample_logins=str(env.get("MUFINANCES_ALLOW_SAMPLE_LOGINS", "")).lower() in {"1", "true", "yes"},
        allow_mock_connectors=str(env.get("MUFINANCES_ALLOW_MOCK_CONNECTORS", "")).lower() in {"1", "true", "yes"},
        allow_unsafe_defaults=str(env.get("MUFINANCES_ALLOW_UNSAFE_DEFAULTS", "")).lower() in {"1", "true", "yes"},
    )


class DemoDataGuard:
    def __init__(self, policy: DemoDataPolicy | None = None):
        self.policy = policy or policy_from_environment()

    def require_demo_seed_allowed(self) -> None:
        if self.policy.production or not self.policy.allow_demo_seed:
            raise PermissionDenied("Demo seed data is disabled for this runtime")

    def require_sample_login_allowed(self) -> None:
        if self.policy.production or not self.policy.allow_sample_logins:
            raise PermissionDenied("Sample logins are disabled for this runtime")

    def require_mock_connector_allowed(self) -> None:
        if self.policy.production or not self.policy.allow_mock_connectors:
            raise PermissionDenied("Mock connectors are disabled for this runtime")

    def assert_production_safe(self) -> None:
        if not self.policy.production:
            return
        unsafe = []
        if self.policy.allow_demo_seed:
            unsafe.append("MUFINANCES_ALLOW_DEMO_SEED")
        if self.policy.allow_sample_logins:
            unsafe.append("MUFINANCES_ALLOW_SAMPLE_LOGINS")
        if self.policy.allow_mock_connectors:
            unsafe.append("MUFINANCES_ALLOW_MOCK_CONNECTORS")
        if self.policy.allow_unsafe_defaults:
            unsafe.append("MUFINANCES_ALLOW_UNSAFE_DEFAULTS")
        if unsafe:
            raise ValidationError(f"Production mode blocks unsafe settings: {', '.join(unsafe)}")

    def seed_namespace(self) -> str:
        if self.policy.production:
            raise PermissionDenied("Production runtime cannot use demo seed namespace")
        return f"demo:{self.policy.runtime_mode.value}"

