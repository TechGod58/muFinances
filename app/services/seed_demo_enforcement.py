from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any, Mapping

from services.demo_data import DemoDataGuard, DemoDataPolicy, RuntimeMode, policy_from_environment


def _now() -> str:
    return datetime.now(UTC).isoformat()


def current_policy(env: Mapping[str, str] | None = None) -> DemoDataPolicy:
    return policy_from_environment(env or os.environ)


def seed_mode(env: Mapping[str, str] | None = None) -> str:
    env = env or os.environ
    return str(env.get('MUFINANCES_SEED_MODE', 'demo')).lower()


def production_blockers(env: Mapping[str, str] | None = None) -> list[str]:
    env = env or os.environ
    policy = current_policy(env)
    blockers: list[str] = []
    if policy.production:
        if seed_mode(env) not in {'none', 'production-reference'}:
            blockers.append('MUFINANCES_SEED_MODE must be none or production-reference in production.')
        if policy.allow_demo_seed:
            blockers.append('MUFINANCES_ALLOW_DEMO_SEED is not allowed in production.')
        if policy.allow_sample_logins:
            blockers.append('MUFINANCES_ALLOW_SAMPLE_LOGINS is not allowed in production.')
        if policy.allow_mock_connectors:
            blockers.append('MUFINANCES_ALLOW_MOCK_CONNECTORS is not allowed in production.')
        if policy.allow_unsafe_defaults:
            blockers.append('MUFINANCES_ALLOW_UNSAFE_DEFAULTS is not allowed in production.')
    return blockers


def assert_seed_demo_safe(env: Mapping[str, str] | None = None) -> None:
    blockers = production_blockers(env)
    if blockers:
        raise RuntimeError('Production seed/demo enforcement failed: ' + '; '.join(blockers))


def seed_allowed(env: Mapping[str, str] | None = None) -> bool:
    env = env or os.environ
    policy = current_policy(env)
    mode = seed_mode(env)
    if policy.production:
        return False
    return mode in {'demo', 'sample', 'development', 'test'}


def status(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    env = env or os.environ
    policy = current_policy(env)
    mode = seed_mode(env)
    blockers = production_blockers(env)
    checks = {
        'production_blocks_demo_seed': not policy.production or not policy.allow_demo_seed,
        'production_blocks_sample_logins': not policy.production or not policy.allow_sample_logins,
        'production_blocks_mock_connectors': not policy.production or not policy.allow_mock_connectors,
        'production_seed_mode_safe': not policy.production or mode in {'none', 'production-reference'},
        'unsafe_defaults_blocked': not policy.production or not policy.allow_unsafe_defaults,
        'startup_gate_ready': len(blockers) == 0,
    }
    return {
        'batch': 'B147',
        'title': 'Seed And Demo Data Separation Enforcement',
        'complete': all(checks.values()),
        'runtime_mode': policy.runtime_mode.value,
        'seed_mode': mode,
        'seed_allowed': seed_allowed(env),
        'checks': checks,
        'blockers': blockers,
        'checked_at': _now(),
    }


def guard() -> DemoDataGuard:
    return DemoDataGuard(current_policy())

