from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_market_lab.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def test_market_lab_status_reports_b35_complete() -> None:
    response = client.get('/api/market-lab/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B35'
    assert payload['complete'] is True
    assert payload['checks']['paper_account_ready'] is True
    assert payload['provider']['real_money_trading'] is False


def test_search_watchlist_and_quote_provider_hook() -> None:
    headers = admin_headers()
    search = client.get('/api/market-lab/search?q=AAPL', headers=headers)
    assert search.status_code == 200
    assert search.json()['results'][0]['symbol'] == 'AAPL'
    assert search.json()['results'][0]['provider'] == 'local-delayed-demo'

    favorite = client.post('/api/market-lab/watchlist', headers=headers, json={'symbol': 'AAPL'})
    assert favorite.status_code == 200
    assert any(item['symbol'] == 'AAPL' for item in favorite.json()['watchlist'])

    lab = client.get('/api/market-lab', headers=headers)
    assert lab.status_code == 200
    assert len(lab.json()['ticker']) == 4
    assert any(item['symbol'] == 'AAPL' for item in lab.json()['favorites'])


def test_paper_trading_account_positions_pnl_and_trade_history() -> None:
    headers = admin_headers()
    reset = client.post('/api/market-lab/account', headers=headers, json={'starting_cash': 100000})
    assert reset.status_code == 200
    assert reset.json()['cash_balance'] == 100000

    buy = client.post('/api/market-lab/trades', headers=headers, json={'symbol': 'DIA', 'side': 'buy', 'quantity': 3})
    assert buy.status_code == 200
    payload = buy.json()
    assert payload['account']['cash_balance'] < 100000
    assert payload['positions'][0]['symbol'] == 'DIA'
    assert payload['positions'][0]['quantity'] == 3
    assert payload['trades'][0]['side'] == 'buy'

    sell = client.post('/api/market-lab/trades', headers=headers, json={'symbol': 'DIA', 'side': 'sell', 'quantity': 1})
    assert sell.status_code == 200
    assert sell.json()['positions'][0]['quantity'] == 2
    assert len(sell.json()['trades']) == 2

    blocked = client.post('/api/market-lab/trades', headers=headers, json={'symbol': 'DIA', 'side': 'sell', 'quantity': 100})
    assert blocked.status_code == 409
