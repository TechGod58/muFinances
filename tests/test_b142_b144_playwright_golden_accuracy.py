from __future__ import annotations

import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_b142_b144_playwright_golden_accuracy.db'
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


def test_b142_playwright_ci_workflow_covers_required_ui_paths() -> None:
    workflow = (PROJECT_ROOT / '.github' / 'workflows' / 'playwright-ci.yml').read_text(encoding='utf-8')
    package = json.loads((PROJECT_ROOT / 'package.json').read_text(encoding='utf-8'))
    spec = (PROJECT_ROOT / 'tests' / 'playwright' / 'production_workflows.spec.js').read_text(encoding='utf-8')

    assert 'npm run test:playwright:ci' in workflow
    assert 'python -m uvicorn app.main:app' in workflow
    assert '@playwright/test' in package['devDependencies']
    for marker in ['workspaces', 'dock', 'chat', 'import data', 'export data', 'Reporting and analytics', 'mobile', 'tablet', 'not.toBeEmpty']:
        assert marker in spec


def test_b143_golden_financial_test_pack_runs_against_expected_outputs() -> None:
    headers = admin_headers()
    status = client.get('/api/reporting/golden-test-packs/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B143'
    assert status.json()['complete'] is True
    assert 'secure_audit_trail' in status.json()['covered_domains']

    run = client.post('/api/reporting/golden-test-packs/run', headers=headers, json={'run_key': 'b143-golden-test'})
    assert run.status_code == 200, run.text
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['all_expected_values_matched'] is True
    assert payload['checks']['secure_audit_events_recorded'] is True
    comparisons = payload['results'][0]['comparisons']
    assert comparisons
    assert all(item['matched'] for item in comparisons)


def test_b144_statement_accuracy_certifies_reports_against_golden_pack() -> None:
    headers = admin_headers()
    status = client.get('/api/reporting/statement-accuracy-certification/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B144'
    assert status.json()['complete'] is True

    run = client.post('/api/reporting/statement-accuracy-certification/run', headers=headers, json={'run_key': 'b144-accuracy'})
    assert run.status_code == 200, run.text
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['income_statement_matches_golden'] is True
    assert payload['checks']['balance_sheet_matches_golden'] is True
    assert payload['checks']['cash_flow_matches_golden'] is True
    assert payload['checks']['fund_report_matches_golden'] is True
    assert payload['checks']['grant_report_matches_golden'] is True
    assert payload['checks']['departmental_pl_matches_golden'] is True
    assert payload['checks']['board_package_ready'] is True
    assert payload['checks']['footnotes_ready'] is True
    assert payload['checks']['charts_ready'] is True
    assert payload['checks']['secure_audit_trail_matches_golden'] is True
    assert payload['artifacts']['board_artifact']['status'] == 'ready'
