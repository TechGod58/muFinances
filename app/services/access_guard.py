from __future__ import annotations

import ipaddress
import importlib.util
import os
from dataclasses import dataclass
from typing import Any


def _flag(name: str, default: str = 'false') -> bool:
    return os.getenv(name, default).strip().lower() in {'1', 'true', 'yes', 'on'}


def _csv(name: str, default: str = '') -> list[str]:
    return [item.strip() for item in os.getenv(name, default).split(',') if item.strip()]


@dataclass(frozen=True)
class NetworkGuardConfig:
    enabled: bool
    allowed_host_suffixes: tuple[str, ...]
    allowed_client_cidrs: tuple[str, ...]
    allow_localhost: bool


def network_guard_config() -> NetworkGuardConfig:
    return NetworkGuardConfig(
        enabled=_flag('CAMPUS_FPM_DOMAIN_GUARD_ENABLED'),
        allowed_host_suffixes=tuple(_csv('CAMPUS_FPM_ALLOWED_HOST_SUFFIXES', 'manchester.edu')),
        allowed_client_cidrs=tuple(_csv('CAMPUS_FPM_ALLOWED_CLIENT_CIDRS')),
        allow_localhost=_flag('CAMPUS_FPM_ALLOW_LOCALHOST', 'true'),
    )


def _host_without_port(value: str) -> str:
    value = value.strip().lower()
    if not value:
        return ''
    if '://' in value:
        value = value.split('://', 1)[1]
    value = value.split('/', 1)[0]
    if value.startswith('['):
        return value.split(']', 1)[0].lstrip('[')
    return value.rsplit(':', 1)[0] if ':' in value else value


def _is_local_host(host: str) -> bool:
    normalized = _host_without_port(host)
    return normalized in {'localhost', '127.0.0.1', '::1', 'testserver'}


def _host_matches(host: str, suffixes: tuple[str, ...]) -> bool:
    normalized = _host_without_port(host)
    for suffix in suffixes:
        suffix_value = suffix.strip().lower().lstrip('.')
        if normalized == suffix_value or normalized.endswith(f'.{suffix_value}'):
            return True
    return False


def _ip_matches(client_host: str, cidrs: tuple[str, ...]) -> bool:
    if not client_host or not cidrs:
        return False
    try:
        address = ipaddress.ip_address(client_host)
    except ValueError:
        return False
    for cidr in cidrs:
        try:
            if address in ipaddress.ip_network(cidr, strict=False):
                return True
        except ValueError:
            continue
    return False


def is_network_allowed(host: str, client_host: str, headers: dict[str, str], config: NetworkGuardConfig | None = None) -> bool:
    config = config or network_guard_config()
    if not config.enabled:
        return True

    candidates = [
        host,
        headers.get('host', ''),
        headers.get('x-forwarded-host', '').split(',', 1)[0],
        headers.get('origin', ''),
        headers.get('referer', ''),
    ]
    if config.allow_localhost and any(_is_local_host(candidate) for candidate in candidates):
        return True
    if any(_host_matches(candidate, config.allowed_host_suffixes) for candidate in candidates):
        return True

    forwarded_for = headers.get('x-forwarded-for', '').split(',', 1)[0].strip()
    return _ip_matches(forwarded_for or client_host, config.allowed_client_cidrs)


def assert_network_request_allowed(request: Any) -> None:
    headers = {key.lower(): value for key, value in request.headers.items()}
    host = headers.get('host', '')
    client_host = request.client.host if request.client else ''
    if not is_network_allowed(host, client_host, headers):
        raise PermissionError('muFinances is restricted to Manchester University domain, on-prem, or VPN access.')


def dn_is_under_ou(user_dn: str, allowed_ou_dn: str) -> bool:
    user_value = ','.join(part.strip().lower() for part in user_dn.split(',') if part.strip())
    ou_value = ','.join(part.strip().lower() for part in allowed_ou_dn.split(',') if part.strip())
    return bool(user_value and ou_value and (user_value == ou_value or user_value.endswith(f',{ou_value}')))


def ad_guard_enabled() -> bool:
    return _flag('CAMPUS_FPM_AD_OU_GUARD_ENABLED')


def _ad_configured() -> bool:
    required = [
        'CAMPUS_FPM_AD_SERVER_URI',
        'CAMPUS_FPM_AD_BIND_DN',
        'CAMPUS_FPM_AD_BIND_PASSWORD',
        'CAMPUS_FPM_AD_ALLOWED_OU_DN',
    ]
    return all(os.getenv(name, '').strip() for name in required)


def _ldap_driver_available() -> bool:
    return importlib.util.find_spec('ldap3') is not None


def _load_ldap_driver() -> tuple[Any, Any, Any] | None:
    try:
        from ldap3 import Connection, Server
        from ldap3.utils.conv import escape_filter_chars
    except ImportError:  # pragma: no cover - exercised through status/tests without requiring LDAP locally.
        return None
    return Connection, Server, escape_filter_chars


def _ad_identifier(user: dict[str, Any], attribute: str) -> str:
    email = str(user.get('email') or '').strip()
    if attribute.lower() == 'samaccountname' and '@' in email:
        return email.split('@', 1)[0]
    return email


def is_ad_ou_allowed(user: dict[str, Any]) -> bool:
    if not ad_guard_enabled():
        return True
    ldap_driver = _load_ldap_driver()
    if ldap_driver is None:
        raise PermissionError('AD OU guard is enabled, but ldap3 is not installed.')
    if not _ad_configured():
        raise PermissionError('AD OU guard is enabled, but Active Directory settings are incomplete.')

    Connection, Server, escape_filter_chars = ldap_driver
    server_uri = os.getenv('CAMPUS_FPM_AD_SERVER_URI', '').strip()
    bind_dn = os.getenv('CAMPUS_FPM_AD_BIND_DN', '').strip()
    bind_password = os.getenv('CAMPUS_FPM_AD_BIND_PASSWORD', '')
    allowed_ou_dn = os.getenv('CAMPUS_FPM_AD_ALLOWED_OU_DN', '').strip()
    search_base = os.getenv('CAMPUS_FPM_AD_USER_SEARCH_BASE', allowed_ou_dn).strip() or allowed_ou_dn
    attribute = os.getenv('CAMPUS_FPM_AD_USER_ATTRIBUTE', 'userPrincipalName').strip() or 'userPrincipalName'
    identifier = _ad_identifier(user, attribute)
    if not identifier:
        return False

    server = Server(server_uri)
    with Connection(server, user=bind_dn, password=bind_password, auto_bind=True) as connection:
        search_filter = f'({attribute}={escape_filter_chars(identifier)})'
        connection.search(search_base, search_filter, attributes=['distinguishedName'])
        if not connection.entries:
            return False
        user_dn = str(connection.entries[0].entry_dn)
    return dn_is_under_ou(user_dn, allowed_ou_dn)


def assert_ad_ou_allowed(user: dict[str, Any]) -> None:
    if not is_ad_ou_allowed(user):
        raise PermissionError('User is not in the allowed Active Directory OU for muFinances.')


def access_guard_status() -> dict[str, Any]:
    config = network_guard_config()
    ad_enabled = ad_guard_enabled()
    ldap_available = _ldap_driver_available()
    checks = {
        'network_guard_configurable': True,
        'manchester_host_suffix_configured': 'manchester.edu' in {item.lower().lstrip('.') for item in config.allowed_host_suffixes},
        'cidr_guard_configurable': True,
        'ad_ou_guard_configurable': True,
        'ldap_driver_available_when_enabled': (not ad_enabled) or ldap_available,
        'ad_settings_complete_when_enabled': (not ad_enabled) or _ad_configured(),
    }
    return {
        'title': 'Manchester Domain And AD OU Access Guard',
        'complete': all(checks.values()),
        'network_guard': {
            'enabled': config.enabled,
            'allowed_host_suffixes': list(config.allowed_host_suffixes),
            'allowed_client_cidrs_configured': len(config.allowed_client_cidrs),
            'allow_localhost': config.allow_localhost,
        },
        'ad_ou_guard': {
            'enabled': ad_enabled,
            'ldap_driver_available': ldap_available,
            'server_configured': bool(os.getenv('CAMPUS_FPM_AD_SERVER_URI', '').strip()),
            'search_base_configured': bool(os.getenv('CAMPUS_FPM_AD_USER_SEARCH_BASE', '').strip()),
            'allowed_ou_configured': bool(os.getenv('CAMPUS_FPM_AD_ALLOWED_OU_DN', '').strip()),
            'user_attribute': os.getenv('CAMPUS_FPM_AD_USER_ATTRIBUTE', 'userPrincipalName'),
        },
        'checks': checks,
    }
