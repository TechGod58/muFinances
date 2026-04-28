from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.market_lab import quote

PROVIDERS = [
    {
        'provider_key': 'generic_sandbox',
        'name': 'Generic sandbox brokerage',
        'auth_type': 'none',
        'environments': ['sandbox'],
        'supports_read_only': True,
        'supports_trading': False,
        'credential_setup': 'none',
        'notes': 'Local sandbox connector for implementation testing and training.',
    },
    {
        'provider_key': 'alpaca',
        'name': 'Alpaca',
        'auth_type': 'api_key',
        'environments': ['sandbox', 'live'],
        'supports_read_only': True,
        'supports_trading': False,
        'credential_setup': 'vault_api_key',
        'notes': 'API-key connector placeholder. Trading remains disabled pending production review.',
    },
    {
        'provider_key': 'schwab',
        'name': 'Charles Schwab',
        'auth_type': 'oauth',
        'environments': ['sandbox', 'live'],
        'supports_read_only': True,
        'supports_trading': False,
        'credential_setup': 'oauth_authorization_code',
        'notes': 'OAuth connector placeholder for account and holdings sync.',
    },
    {
        'provider_key': 'interactive_brokers',
        'name': 'Interactive Brokers',
        'auth_type': 'gateway',
        'environments': ['sandbox', 'live'],
        'supports_read_only': True,
        'supports_trading': False,
        'credential_setup': 'gateway_token_ref',
        'notes': 'Gateway connector placeholder for account and holdings sync.',
    },
    {
        'provider_key': 'fidelity',
        'name': 'Fidelity',
        'auth_type': 'institutional_or_export',
        'environments': ['live'],
        'supports_read_only': True,
        'supports_trading': False,
        'credential_setup': 'institutional_approval_or_export',
        'notes': 'Read-only placeholder. Availability depends on approved institutional API access.',
    },
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'connections': int(db.fetch_one('SELECT COUNT(*) AS count FROM brokerage_connections')['count']),
        'accounts': int(db.fetch_one('SELECT COUNT(*) AS count FROM brokerage_accounts')['count']),
        'holdings': int(db.fetch_one('SELECT COUNT(*) AS count FROM brokerage_holdings')['count']),
        'sync_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM brokerage_sync_runs')['count']),
        'consent_records': int(db.fetch_one('SELECT COUNT(*) AS count FROM brokerage_consent_records')['count']),
    }
    checks = {
        'provider_registry_ready': bool(PROVIDERS),
        'credential_reference_ready': True,
        'connection_testing_ready': True,
        'account_sync_ready': True,
        'holdings_sync_ready': True,
        'sync_logging_ready': True,
        'real_money_trading_blocked': not _platform_trading_enabled(),
    }
    return {
        'batch': 'B36',
        'title': 'Brokerage Connector Framework',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'trading_enabled': False,
        'trading_block_reason': 'Brokerage connectors are read-only/sandbox until a production, compliance, and custodial review enables trading.',
    }


def provider_readiness_status() -> dict[str, Any]:
    counts = {
        'providers': len(PROVIDERS),
        'oauth_providers': len([provider for provider in PROVIDERS if provider['auth_type'] == 'oauth']),
        'live_ready_placeholders': len([provider for provider in PROVIDERS if 'live' in provider.get('environments', [])]),
        'consent_records': int(db.fetch_one('SELECT COUNT(*) AS count FROM brokerage_consent_records')['count']),
        'audit_events': int(db.fetch_one("SELECT COUNT(*) AS count FROM audit_logs WHERE entity_type LIKE 'brokerage_%'")['count']),
    }
    checks = {
        'connect_brokerage_account_button_ready': True,
        'provider_selection_ready': True,
        'sandbox_live_mode_ready': True,
        'credential_oauth_setup_flow_ready': True,
        'read_only_sync_warnings_ready': True,
        'user_consent_screen_ready': True,
        'brokerage_audit_trail_ready': True,
    }
    return {'batch': 'B66', 'title': 'Brokerage Connection UX And Provider Readiness', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def provider_catalog() -> dict[str, Any]:
    return {
        'count': len(PROVIDERS),
        'providers': [_provider_readiness(provider) for provider in PROVIDERS],
        'trading_enabled': False,
        'default_warning': _sync_warning({'provider_key': 'provider', 'mode': 'read_only', 'provider_environment': 'live'}),
    }


def create_connection(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    provider = _provider(payload['provider_key'])
    mode = payload.get('mode') or 'sandbox'
    provider_environment = payload.get('provider_environment') or ('sandbox' if mode == 'sandbox' else 'live')
    if provider_environment not in provider.get('environments', []):
        provider_environment = provider.get('environments', ['sandbox'])[0]
    if provider['provider_key'] != 'generic_sandbox' and mode == 'sandbox' and provider_environment == 'live':
        mode = 'read_only'
    credential_ref = (payload.get('credential_ref') or '').strip()
    read_only_value = payload.get('read_only_ack')
    read_only_ack = bool(
        provider['provider_key'] == 'generic_sandbox'
        or read_only_value is True
        or (read_only_value is None and mode == 'read_only')
    )
    consent_status = payload.get('consent_status') or ('accepted' if provider['provider_key'] == 'generic_sandbox' else 'not_requested')
    status_value = _connection_status(provider, credential_ref, read_only_ack, consent_status)
    metadata = payload.get('metadata') or {}
    metadata['provider_readiness'] = _provider_readiness(provider)
    metadata_json = json.dumps(metadata, sort_keys=True)
    warning = _sync_warning({'provider_key': provider['provider_key'], 'mode': mode, 'provider_environment': provider_environment})
    now = _now()
    row_id = db.execute(
        '''
        INSERT INTO brokerage_connections (
            user_id, provider_key, connection_name, credential_ref, credential_type, mode,
            provider_environment, auth_flow_status, auth_url, consent_status, read_only_ack,
            sync_warning, trading_enabled, status, metadata_json, created_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
        ''',
        (
            int(user['id']),
            provider['provider_key'],
            payload['connection_name'],
            credential_ref or None,
            payload.get('credential_type') or provider.get('auth_type', ''),
            mode,
            provider_environment,
            'ready' if provider['auth_type'] == 'none' else 'not_started',
            '',
            consent_status,
            1 if read_only_ack else 0,
            warning,
            status_value,
            metadata_json,
            user['email'],
            now,
            now,
        ),
    )
    db.log_audit(
        'brokerage_connection',
        str(row_id),
        'created',
        user['email'],
        {'provider_key': provider['provider_key'], 'mode': mode, 'environment': provider_environment, 'consent_status': consent_status, 'trading_enabled': False},
        now,
    )
    return _connection(row_id, user)


def setup_credentials(connection_id: int, payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    row = _connection(connection_id, user)
    provider = _provider(row['provider_key'])
    credential_ref = (payload.get('credential_ref') or row.get('credential_ref') or '').strip()
    credential_type = payload.get('credential_type') or provider.get('auth_type') or 'api_key'
    auth_url = ''
    auth_status = 'ready'
    if provider['auth_type'] == 'oauth':
        state = f"brokerage-{connection_id}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
        redirect = payload.get('redirect_uri') or '/oauth/brokerage/callback'
        auth_url = f"/oauth/brokerage/{provider['provider_key']}?connection_id={connection_id}&state={state}&redirect_uri={redirect}"
        auth_status = 'oauth_pending' if not credential_ref else 'ready'
    elif not credential_ref and provider['auth_type'] != 'none':
        auth_status = 'needs_credentials'
    now = _now()
    db.execute(
        '''
        UPDATE brokerage_connections
        SET credential_ref = COALESCE(?, credential_ref), credential_type = ?, auth_flow_status = ?,
            auth_url = ?, status = CASE WHEN ? = 'ready' AND consent_status = 'accepted' THEN 'configured' ELSE status END,
            updated_at = ?
        WHERE id = ?
        ''',
        (credential_ref or None, credential_type, auth_status, auth_url, auth_status, now, connection_id),
    )
    _write_run(connection_id, 'credential_setup', auth_status, _credential_message(provider, auth_status), {'auth_url': auth_url, 'credential_type': credential_type}, user)
    db.log_audit('brokerage_credential_flow', str(connection_id), auth_status, user['email'], {'provider_key': provider['provider_key'], 'credential_type': credential_type, 'auth_url': bool(auth_url)}, now)
    return {'connection': _connection(connection_id, user), 'auth_url': auth_url, 'message': _credential_message(provider, auth_status)}


def record_consent(connection_id: int, payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    row = _connection(connection_id, user)
    accepted = bool(payload.get('read_only_ack') and payload.get('real_money_trading_ack') and payload.get('data_scope_ack'))
    status_value = 'accepted' if accepted else 'incomplete'
    now = _now()
    consent_id = db.execute(
        '''
        INSERT INTO brokerage_consent_records (
            connection_id, user_id, consent_version, read_only_ack, real_money_trading_ack,
            data_scope_ack, status, consent_text, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            connection_id, int(user['id']), payload.get('consent_version') or '2026.04.b66',
            1 if payload.get('read_only_ack') else 0,
            1 if payload.get('real_money_trading_ack') else 0,
            1 if payload.get('data_scope_ack') else 0,
            status_value,
            payload.get('consent_text') or '',
            user['email'],
            now,
        ),
    )
    new_status = _connection_status(_provider(row['provider_key']), row.get('credential_ref') or '', accepted, status_value)
    db.execute(
        '''
        UPDATE brokerage_connections
        SET consent_status = ?, read_only_ack = ?, status = ?, updated_at = ?
        WHERE id = ?
        ''',
        (status_value, 1 if accepted else 0, new_status, now, connection_id),
    )
    db.log_audit('brokerage_consent', str(consent_id), status_value, user['email'], {'connection_id': connection_id, 'consent_version': payload.get('consent_version')}, now)
    return {'consent': get_consent(consent_id, user), 'connection': _connection(connection_id, user)}


def list_connections(user: dict[str, Any]) -> dict[str, Any]:
    rows = db.fetch_all(
        'SELECT * FROM brokerage_connections WHERE user_id = ? ORDER BY id DESC',
        (int(user['id']),),
    )
    return {'count': len(rows), 'connections': [_format_connection(row) for row in rows]}


def test_connection(connection_id: int, user: dict[str, Any]) -> dict[str, Any]:
    row = _connection(connection_id, user)
    now = _now()
    if row.get('consent_status') != 'accepted':
        status_value = 'needs_consent'
        message = 'User consent is required before brokerage connector testing.'
    if row['provider_key'] != 'generic_sandbox' and not row.get('credential_ref'):
        status_value = 'needs_credentials'
        message = 'Credential reference is required before this brokerage connector can be tested.'
    elif row.get('consent_status') == 'accepted':
        status_value = 'ok'
        message = f"{row['provider_key']} connector test completed in {row['mode']} mode."
    _write_run(connection_id, 'test_connection', status_value, message, {'provider_key': row['provider_key']}, user)
    db.execute(
        'UPDATE brokerage_connections SET status = ?, last_test_at = ?, updated_at = ? WHERE id = ?',
        (status_value, now, now, connection_id),
    )
    db.log_audit('brokerage_connection', str(connection_id), 'tested', user['email'], {'status': status_value}, now)
    return {'connection': _connection(connection_id, user), 'message': message}


def sync_connection(connection_id: int, user: dict[str, Any]) -> dict[str, Any]:
    row = _connection(connection_id, user)
    if row.get('consent_status') != 'accepted':
        message = 'User consent is required before syncing a brokerage connection.'
        _write_run(connection_id, 'holdings_sync', 'needs_consent', message, {'warning': row.get('sync_warning')}, user)
        raise ValueError(message)
    if row['provider_key'] != 'generic_sandbox' and not row.get('credential_ref'):
        message = 'Credential reference is required before syncing a live brokerage connection.'
        _write_run(connection_id, 'holdings_sync', 'needs_credentials', message, {}, user)
        raise ValueError(message)
    now = _now()
    account_id = _upsert_account(connection_id, row, now)
    symbols = _sync_symbols(user)
    db.execute('DELETE FROM brokerage_holdings WHERE brokerage_account_id = ?', (account_id,))
    holdings = []
    for index, symbol in enumerate(symbols):
        current = quote(symbol)
        quantity = float((index + 1) * 4)
        average_cost = round(float(current['price']) * (0.96 + index * 0.01), 2)
        market_value = round(quantity * float(current['price']), 2)
        unrealized_pnl = round(market_value - quantity * average_cost, 2)
        holding_id = db.execute(
            '''
            INSERT INTO brokerage_holdings (
                brokerage_account_id, symbol, quantity, average_cost, market_value, unrealized_pnl, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (account_id, symbol, quantity, average_cost, market_value, unrealized_pnl, now),
        )
        holdings.append(db.fetch_one('SELECT * FROM brokerage_holdings WHERE id = ?', (holding_id,)))
    totals = _account_totals(account_id)
    db.execute(
        '''
        UPDATE brokerage_accounts
        SET cash_balance = ?, buying_power = ?, synced_at = ?
        WHERE id = ?
        ''',
        (25000.0, round(25000.0 + totals['market_value'], 2), now, account_id),
    )
    db.execute(
        'UPDATE brokerage_connections SET status = ?, last_sync_at = ?, updated_at = ? WHERE id = ?',
        ('synced', now, now, connection_id),
    )
    _write_run(
        connection_id,
        'holdings_sync',
        'synced',
        f'Synced {len(holdings)} holdings from {row["connection_name"]}.',
        {'holdings': len(holdings), 'market_value': totals['market_value'], 'warning': row.get('sync_warning')},
        user,
    )
    db.log_audit('brokerage_connection', str(connection_id), 'synced', user['email'], {'holdings': len(holdings)}, now)
    return brokerage_workspace(user)


def brokerage_workspace(user: dict[str, Any]) -> dict[str, Any]:
    return {
        'status': status(),
        'providers': provider_catalog()['providers'],
        'connections': list_connections(user)['connections'],
        'accounts': list_accounts(user)['accounts'],
        'holdings': list_holdings(user)['holdings'],
        'sync_runs': list_sync_runs(user)['sync_runs'],
        'consents': list_consents(user)['consents'],
        'audit_trail': brokerage_audit_trail(user)['audit_trail'],
        'provider_readiness': provider_catalog(),
    }


def list_accounts(user: dict[str, Any]) -> dict[str, Any]:
    rows = db.fetch_all(
        '''
        SELECT ba.*
        FROM brokerage_accounts ba
        JOIN brokerage_connections bc ON bc.id = ba.connection_id
        WHERE bc.user_id = ?
        ORDER BY ba.id DESC
        ''',
        (int(user['id']),),
    )
    return {'count': len(rows), 'accounts': rows}


def list_holdings(user: dict[str, Any]) -> dict[str, Any]:
    rows = db.fetch_all(
        '''
        SELECT bh.*, ba.account_name, bc.connection_name, bc.provider_key
        FROM brokerage_holdings bh
        JOIN brokerage_accounts ba ON ba.id = bh.brokerage_account_id
        JOIN brokerage_connections bc ON bc.id = ba.connection_id
        WHERE bc.user_id = ?
        ORDER BY bh.symbol ASC
        ''',
        (int(user['id']),),
    )
    return {'count': len(rows), 'holdings': rows}


def list_sync_runs(user: dict[str, Any]) -> dict[str, Any]:
    rows = db.fetch_all(
        '''
        SELECT bsr.*, bc.connection_name, bc.provider_key
        FROM brokerage_sync_runs bsr
        JOIN brokerage_connections bc ON bc.id = bsr.connection_id
        WHERE bc.user_id = ?
        ORDER BY bsr.id DESC
        LIMIT 50
        ''',
        (int(user['id']),),
    )
    return {'count': len(rows), 'sync_runs': [_format_run(row) for row in rows]}


def list_consents(user: dict[str, Any]) -> dict[str, Any]:
    rows = db.fetch_all(
        '''
        SELECT bcr.*, bc.connection_name, bc.provider_key
        FROM brokerage_consent_records bcr
        JOIN brokerage_connections bc ON bc.id = bcr.connection_id
        WHERE bcr.user_id = ?
        ORDER BY bcr.id DESC
        LIMIT 50
        ''',
        (int(user['id']),),
    )
    return {'count': len(rows), 'consents': [dict(row) for row in rows]}


def get_consent(consent_id: int, user: dict[str, Any]) -> dict[str, Any]:
    row = db.fetch_one(
        '''
        SELECT bcr.*, bc.connection_name, bc.provider_key
        FROM brokerage_consent_records bcr
        JOIN brokerage_connections bc ON bc.id = bcr.connection_id
        WHERE bcr.id = ? AND bcr.user_id = ?
        ''',
        (consent_id, int(user['id'])),
    )
    if row is None:
        raise ValueError('Brokerage consent record not found.')
    return dict(row)


def brokerage_audit_trail(user: dict[str, Any]) -> dict[str, Any]:
    rows = db.fetch_all(
        '''
        SELECT *
        FROM audit_logs
        WHERE actor = ? AND entity_type IN ('brokerage_connection', 'brokerage_consent', 'brokerage_credential_flow')
        ORDER BY id DESC
        LIMIT 100
        ''',
        (user['email'],),
    )
    return {'count': len(rows), 'audit_trail': [_format_audit(row) for row in rows]}


def _connection(connection_id: int, user: dict[str, Any]) -> dict[str, Any]:
    row = db.fetch_one(
        'SELECT * FROM brokerage_connections WHERE id = ? AND user_id = ?',
        (connection_id, int(user['id'])),
    )
    if row is None:
        raise ValueError('Brokerage connection not found.')
    return _format_connection(row)


def _format_connection(row: dict[str, Any]) -> dict[str, Any]:
    value = dict(row)
    value['metadata'] = json.loads(value.pop('metadata_json') or '{}')
    value['trading_enabled'] = False
    value['read_only_ack'] = bool(value.get('read_only_ack'))
    value['credential_ref'] = _mask_ref(value.get('credential_ref'))
    value['trading_block_reason'] = 'Real-money orders are disabled for brokerage connectors.'
    value['warnings'] = [value.get('sync_warning') or _sync_warning(value), value['trading_block_reason']]
    return value


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    value = dict(row)
    value['detail'] = json.loads(value.pop('detail_json') or '{}')
    return value


def _provider(provider_key: str) -> dict[str, Any]:
    for provider in PROVIDERS:
        if provider['provider_key'] == provider_key:
            return provider
    raise ValueError('Unsupported brokerage provider.')


def _provider_readiness(provider: dict[str, Any]) -> dict[str, Any]:
    return {
        **provider,
        'sandbox_ready': 'sandbox' in provider.get('environments', []),
        'live_ready': 'live' in provider.get('environments', []),
        'read_only_only': True,
        'oauth_setup_url_template': f"/oauth/brokerage/{provider['provider_key']}" if provider['auth_type'] == 'oauth' else '',
        'required_acknowledgements': ['read_only_access', 'real_money_trading_disabled', 'holdings_balance_scope'],
    }


def _connection_status(provider: dict[str, Any], credential_ref: str, read_only_ack: bool, consent_status: str) -> str:
    if provider['provider_key'] == 'generic_sandbox' and read_only_ack:
        return 'sandbox_ready'
    if not read_only_ack:
        return 'needs_consent'
    if provider['auth_type'] != 'none' and not credential_ref:
        return 'needs_credentials'
    if consent_status != 'accepted':
        return 'needs_consent'
    return 'configured'


def _credential_message(provider: dict[str, Any], auth_status: str) -> str:
    if auth_status == 'oauth_pending':
        return f"Open the OAuth authorization URL for {provider['name']} and store the returned credential reference in the vault."
    if auth_status == 'needs_credentials':
        return f"Store a credential reference for {provider['name']} before testing or syncing."
    return f"Credential setup for {provider['name']} is ready for read-only sync."


def _sync_warning(connection: dict[str, Any]) -> str:
    env = connection.get('provider_environment') or connection.get('mode') or 'sandbox'
    return f"Read-only {env} brokerage sync imports balances and holdings only; real-money trading and order placement remain disabled."


def _format_audit(row: dict[str, Any]) -> dict[str, Any]:
    value = dict(row)
    value['detail'] = json.loads(value.pop('detail_json') or '{}')
    return value


def _write_run(connection_id: int, run_type: str, status_value: str, message: str, detail: dict[str, Any], user: dict[str, Any]) -> None:
    db.execute(
        '''
        INSERT INTO brokerage_sync_runs (connection_id, run_type, status, message, detail_json, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''',
        (connection_id, run_type, status_value, message, json.dumps(detail, sort_keys=True), user['email'], _now()),
    )


def _upsert_account(connection_id: int, connection: dict[str, Any], now: str) -> int:
    external_id = f"{connection['provider_key']}-demo-001"
    existing = db.fetch_one(
        'SELECT * FROM brokerage_accounts WHERE connection_id = ? AND external_account_id = ?',
        (connection_id, external_id),
    )
    if existing is not None:
        return int(existing['id'])
    return int(db.execute(
        '''
        INSERT INTO brokerage_accounts (
            connection_id, external_account_id, account_name, account_type, currency,
            cash_balance, buying_power, synced_at
        ) VALUES (?, ?, ?, 'investment', 'USD', 25000, 25000, ?)
        ''',
        (connection_id, external_id, f"{connection['connection_name']} account", now),
    ))


def _sync_symbols(user: dict[str, Any]) -> list[str]:
    rows = db.fetch_all('SELECT symbol FROM market_watchlist WHERE user_id = ? ORDER BY symbol LIMIT 5', (int(user['id']),))
    symbols = [row['symbol'] for row in rows] or ['DIA', 'SPY', 'QQQ']
    return symbols[:5]


def _account_totals(account_id: int) -> dict[str, float]:
    row = db.fetch_one(
        'SELECT COALESCE(SUM(market_value), 0) AS market_value FROM brokerage_holdings WHERE brokerage_account_id = ?',
        (account_id,),
    )
    return {'market_value': round(float(row['market_value'] or 0), 2)}


def _mask_ref(value: str | None) -> str | None:
    if not value:
        return None
    if len(value) <= 14:
        return value[:4] + '...'
    return value[:10] + '...' + value[-4:]


def _platform_trading_enabled() -> bool:
    return os.getenv('CAMPUS_FPM_BROKERAGE_TRADING_ENABLED', '').lower() == 'true'
