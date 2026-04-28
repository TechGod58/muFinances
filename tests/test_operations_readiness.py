from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_operations_readiness.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post('/api/auth/login', json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'})
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}", 'X-Trace-Id': 'test-trace-b105'}


def test_operations_readiness_dashboard_routes_run_and_worker_evidence() -> None:
    headers = admin_headers()

    dashboard = client.get('/api/admin/production-readiness-dashboard', headers=headers)
    assert dashboard.status_code == 200
    dashboard_payload = dashboard.json()
    assert dashboard_payload['batch'] == 'B105'
    component_names = {component['name'] for component in dashboard_payload['components']}
    assert {'Database mode', 'Migration status', 'Worker status', 'Backup status', 'Health checks', 'Logs', 'Alerts'} <= component_names

    route = client.post(
        '/api/operations-readiness/alert-routes',
        headers=headers,
        json={
            'route_key': 'ops-warning',
            'severity': 'warning',
            'destination': 'ops-team@mufinances.local',
            'status': 'ready',
            'evidence': {'channel': 'email'},
        },
    )
    assert route.status_code == 200
    assert route.json()['destination'] == 'ops-team@mufinances.local'

    run = client.post('/api/operations-readiness/run', headers=headers, json={'run_key': 'b105-regression'})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['health_checks_ready'] is True
    assert payload['checks']['metrics_ready'] is True
    assert payload['checks']['logs_ready'] is True
    assert payload['checks']['alert_routing_ready'] is True
    assert payload['checks']['backup_drill_records_ready'] is True
    assert payload['checks']['job_diagnostics_ready'] is True
    assert payload['checks']['worker_status_ready'] is True
    assert payload['checks']['production_readiness_dashboard_ready'] is True
    assert payload['artifacts']['health']['status'] == 'pass'
    assert payload['artifacts']['backup_drill']['status'] == 'pass'
    assert payload['artifacts']['job_run']['ran'] is True
    assert payload['artifacts']['job_logs']
    assert payload['artifacts']['dashboard']['batch'] == 'B105'

    status = client.get('/api/operations-readiness/status', headers=headers)
    assert status.status_code == 200
    status_payload = status.json()
    assert status_payload['batch'] == 'B105'
    assert status_payload['complete'] is True
    assert status_payload['latest_run']['run_key'] == 'b105-regression'
    assert status_payload['counts']['alert_routes'] >= 1

    rows = client.get('/api/operations-readiness/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1


def test_operations_readiness_ui_uses_authenticated_real_dashboard_endpoint() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    dashboard_js = (PROJECT_ROOT / 'static' / 'js' / 'production-readiness-dashboard.js').read_text(encoding='utf-8')

    assert '/static/js/production-readiness-dashboard.js?v=105' in index
    assert "localStorage.getItem('mufinances.token')" in dashboard_js
    assert '/api/admin/production-readiness-dashboard' in dashboard_js
    assert 'productionReadinessButton' in dashboard_js
