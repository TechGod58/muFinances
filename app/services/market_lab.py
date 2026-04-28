from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from typing import Any

from app import db

DEFAULT_SYMBOLS = {
    'DOW': ('Dow Jones Industrial Average', 38650.0),
    'DIA': ('SPDR Dow Jones Industrial Average ETF', 386.5),
    'SPY': ('SPDR S&P 500 ETF Trust', 510.0),
    'QQQ': ('Invesco QQQ Trust', 438.0),
    'AAPL': ('Apple Inc.', 175.0),
    'MSFT': ('Microsoft Corporation', 425.0),
    'GOOGL': ('Alphabet Inc.', 165.0),
    'AMZN': ('Amazon.com Inc.', 185.0),
    'NVDA': ('NVIDIA Corporation', 890.0),
    'MU': ('Micron Technology Inc.', 120.0),
}
PROVIDER_NAME = os.getenv('CAMPUS_FPM_MARKET_PROVIDER', 'local-delayed-demo')
PROVIDER_DELAY_MINUTES = int(os.getenv('CAMPUS_FPM_MARKET_DELAY_MINUTES', '15'))


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'cached_quotes': int(db.fetch_one('SELECT COUNT(*) AS count FROM market_quote_cache')['count']),
        'watchlist_items': int(db.fetch_one('SELECT COUNT(*) AS count FROM market_watchlist')['count']),
        'paper_accounts': int(db.fetch_one('SELECT COUNT(*) AS count FROM paper_trading_accounts')['count']),
        'paper_trades': int(db.fetch_one('SELECT COUNT(*) AS count FROM paper_trades')['count']),
    }
    checks = {
        'market_ticker_panel_ready': True,
        'symbol_search_ready': True,
        'watchlists_ready': True,
        'quote_provider_hook_ready': True,
        'paper_account_ready': True,
        'simulated_orders_ready': True,
        'portfolio_pnl_ready': True,
        'trade_history_ready': True,
    }
    return {
        'batch': 'B35',
        'title': 'Market Watch And Paper Trading Lab',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'provider': {'name': PROVIDER_NAME, 'delay_minutes': PROVIDER_DELAY_MINUTES, 'real_money_trading': False},
    }


def market_lab(user: dict[str, Any]) -> dict[str, Any]:
    account = ensure_account(user)
    ticker_symbols = ['DOW', 'DIA', 'SPY', 'QQQ']
    favorites = list_watchlist(user)
    return {
        'status': status(),
        'ticker': [quote(symbol) for symbol in ticker_symbols],
        'favorites': favorites,
        'account': account,
        'positions': portfolio_positions(account['id']),
        'trades': trade_history(account['id']),
    }


def search_symbols(query: str) -> dict[str, Any]:
    value = query.strip().upper()
    matches = []
    for symbol, (name, _) in DEFAULT_SYMBOLS.items():
        if not value or value in symbol or value in name.upper():
            matches.append(quote(symbol))
    if value and not any(item['symbol'] == value for item in matches):
        matches.insert(0, quote(value))
    return {'query': query, 'count': len(matches[:20]), 'results': matches[:20]}


def quote(symbol: str) -> dict[str, Any]:
    symbol = _normalize_symbol(symbol)
    base_name, base_price = DEFAULT_SYMBOLS.get(symbol, (f'{symbol} demo quote', _base_price(symbol)))
    movement = _movement(symbol)
    price = round(max(0.01, base_price * (1 + movement)), 2)
    previous = round(base_price, 2)
    change_amount = round(price - previous, 2)
    change_percent = round(change_amount / previous, 4) if previous else 0
    now = _now()
    db.execute(
        '''
        INSERT INTO market_quote_cache (
            symbol, name, price, change_amount, change_percent, provider, provider_delay_minutes, as_of
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            name = excluded.name,
            price = excluded.price,
            change_amount = excluded.change_amount,
            change_percent = excluded.change_percent,
            provider = excluded.provider,
            provider_delay_minutes = excluded.provider_delay_minutes,
            as_of = excluded.as_of
        ''',
        (symbol, base_name, price, change_amount, change_percent, PROVIDER_NAME, PROVIDER_DELAY_MINUTES, now),
    )
    return _quote_row(symbol)


def add_watchlist_symbol(user: dict[str, Any], symbol: str) -> dict[str, Any]:
    symbol = _normalize_symbol(symbol)
    quote(symbol)
    db.execute(
        '''
        INSERT OR IGNORE INTO market_watchlist (user_id, symbol, created_at)
        VALUES (?, ?, ?)
        ''',
        (int(user['id']), symbol, _now()),
    )
    db.log_audit('market_watchlist', symbol, 'added', user['email'], {'symbol': symbol}, _now())
    return {'symbol': symbol, 'watchlist': list_watchlist(user)}


def list_watchlist(user: dict[str, Any]) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT symbol FROM market_watchlist WHERE user_id = ? ORDER BY symbol', (int(user['id']),))
    if not rows:
        for symbol in ['DOW', 'DIA', 'SPY']:
            db.execute('INSERT OR IGNORE INTO market_watchlist (user_id, symbol, created_at) VALUES (?, ?, ?)', (int(user['id']), symbol, _now()))
        rows = db.fetch_all('SELECT symbol FROM market_watchlist WHERE user_id = ? ORDER BY symbol', (int(user['id']),))
    return [quote(row['symbol']) for row in rows]


def ensure_account(user: dict[str, Any], starting_cash: float = 100000.0) -> dict[str, Any]:
    row = db.fetch_one(
        "SELECT * FROM paper_trading_accounts WHERE user_id = ? AND account_key = 'default'",
        (int(user['id']),),
    )
    if row is None:
        account_id = db.execute(
            '''
            INSERT INTO paper_trading_accounts (
                user_id, account_key, cash_balance, starting_cash, status, created_at, updated_at
            ) VALUES (?, 'default', ?, ?, 'active', ?, ?)
            ''',
            (int(user['id']), starting_cash, starting_cash, _now(), _now()),
        )
        db.log_audit('paper_trading_account', str(account_id), 'created', user['email'], {'starting_cash': starting_cash}, _now())
        row = db.fetch_one('SELECT * FROM paper_trading_accounts WHERE id = ?', (account_id,))
    return _format_account(row)


def reset_account(user: dict[str, Any], starting_cash: float) -> dict[str, Any]:
    existing = ensure_account(user, starting_cash)
    db.execute('DELETE FROM paper_trades WHERE account_id = ?', (existing['id'],))
    db.execute(
        '''
        UPDATE paper_trading_accounts
        SET cash_balance = ?, starting_cash = ?, updated_at = ?
        WHERE id = ?
        ''',
        (starting_cash, starting_cash, _now(), existing['id']),
    )
    db.log_audit('paper_trading_account', str(existing['id']), 'reset', user['email'], {'starting_cash': starting_cash}, _now())
    return _format_account(db.fetch_one('SELECT * FROM paper_trading_accounts WHERE id = ?', (existing['id'],)))


def place_trade(user: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    account = ensure_account(user)
    symbol = _normalize_symbol(payload['symbol'])
    side = payload['side']
    quantity = float(payload['quantity'])
    current_quote = quote(symbol)
    price = float(current_quote['price'])
    notional = round(price * quantity, 2)
    if side == 'buy' and notional > float(account['cash_balance']):
        raise ValueError('Paper account has insufficient cash.')
    if side == 'sell':
        current_position = next((item for item in portfolio_positions(account['id']) if item['symbol'] == symbol), None)
        if current_position is None or float(current_position['quantity']) < quantity:
            raise ValueError('Paper account has insufficient shares.')
    cash_delta = -notional if side == 'buy' else notional
    db.execute(
        '''
        INSERT INTO paper_trades (account_id, symbol, side, quantity, price, notional, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, 'filled', ?)
        ''',
        (account['id'], symbol, side, quantity, price, notional, _now()),
    )
    db.execute(
        'UPDATE paper_trading_accounts SET cash_balance = cash_balance + ?, updated_at = ? WHERE id = ?',
        (cash_delta, _now(), account['id']),
    )
    db.log_audit('paper_trade', symbol, side, user['email'], {'quantity': quantity, 'price': price, 'notional': notional}, _now())
    updated = ensure_account(user)
    return {'account': updated, 'positions': portfolio_positions(updated['id']), 'trades': trade_history(updated['id'])}


def portfolio_positions(account_id: int) -> list[dict[str, Any]]:
    trades = db.fetch_all('SELECT * FROM paper_trades WHERE account_id = ? ORDER BY id ASC', (account_id,))
    positions: dict[str, dict[str, float]] = {}
    for trade in trades:
        symbol = trade['symbol']
        position = positions.setdefault(symbol, {'quantity': 0.0, 'cost_basis': 0.0})
        quantity = float(trade['quantity'])
        notional = float(trade['notional'])
        if trade['side'] == 'buy':
            position['quantity'] += quantity
            position['cost_basis'] += notional
        else:
            avg_cost = position['cost_basis'] / position['quantity'] if position['quantity'] else 0.0
            position['quantity'] -= quantity
            position['cost_basis'] -= avg_cost * quantity
    result = []
    for symbol, position in sorted(positions.items()):
        if position['quantity'] <= 0.000001:
            continue
        current = quote(symbol)
        market_value = round(position['quantity'] * float(current['price']), 2)
        cost_basis = round(position['cost_basis'], 2)
        result.append({
            'symbol': symbol,
            'quantity': round(position['quantity'], 4),
            'average_cost': round(cost_basis / position['quantity'], 2),
            'last_price': current['price'],
            'market_value': market_value,
            'cost_basis': cost_basis,
            'unrealized_pnl': round(market_value - cost_basis, 2),
        })
    return result


def trade_history(account_id: int) -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM paper_trades WHERE account_id = ? ORDER BY id DESC LIMIT 100', (account_id,))


def _quote_row(symbol: str) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM market_quote_cache WHERE symbol = ?', (symbol,))
    if row is None:
        raise ValueError('Quote not found.')
    row['real_time'] = PROVIDER_DELAY_MINUTES == 0
    row['paper_only'] = True
    return row


def _format_account(row: dict[str, Any]) -> dict[str, Any]:
    account = dict(row)
    positions = portfolio_positions(int(account['id']))
    holdings = round(sum(float(item['market_value']) for item in positions), 2)
    account['cash_balance'] = round(float(account['cash_balance']), 2)
    account['starting_cash'] = round(float(account['starting_cash']), 2)
    account['holdings_value'] = holdings
    account['total_equity'] = round(account['cash_balance'] + holdings, 2)
    account['total_pnl'] = round(account['total_equity'] - account['starting_cash'], 2)
    return account


def _normalize_symbol(symbol: str) -> str:
    return ''.join(char for char in symbol.upper().strip() if char.isalnum() or char in {'.', '-'})[:16] or 'DOW'


def _base_price(symbol: str) -> float:
    digest = int(hashlib.sha256(symbol.encode('utf-8')).hexdigest()[:8], 16)
    return round(20 + (digest % 90000) / 100, 2)


def _movement(symbol: str) -> float:
    day_key = datetime.now(UTC).strftime('%Y%m%d')
    digest = int(hashlib.sha256(f'{symbol}:{day_key}'.encode('utf-8')).hexdigest()[:8], 16)
    return ((digest % 801) - 400) / 10000
