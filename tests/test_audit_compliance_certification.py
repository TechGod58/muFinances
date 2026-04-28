from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_audit_compliance_certification.db'
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


def scenario_id(headers: dict[str, str]) -> int:
    response = client.get('/api/scenarios', headers=headers)
    assert response.status_code == 200
    return int(response.json()[0]['id'])


def test_audit_compliance_certification_proves_lineage_retention_tax_and_evidence() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)

    status = client.get('/api/compliance/audit-certification/status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B101'
    assert status.json()['complete'] is True

    run = client.post('/api/compliance/audit-certification/run', headers=headers, json={'scenario_id': sid})
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'passed'
    assert payload['complete'] is True
    assert payload['checks']['immutable_audit_chain_ready'] is True
    assert payload['checks']['source_to_report_lineage_ready'] is True
    assert payload['checks']['retention_policies_ready'] is True
    assert payload['checks']['certification_workflows_ready'] is True
    assert payload['checks']['admin_audit_reports_ready'] is True
    assert payload['checks']['tax_npo_tagging_ready'] is True
    assert payload['checks']['evidence_retention_ready'] is True
    assert payload['artifacts']['audit_chain']['valid'] is True
    assert payload['artifacts']['lineage']['count'] >= 1
    assert payload['artifacts']['certification']['status'] == 'certified'
    assert payload['artifacts']['tax_review']['status'] == 'approved'
    assert payload['artifacts']['evidence_attachment']['retention_until'] == '2034-06-30'

    rows = client.get('/api/compliance/audit-certification/runs', headers=headers)
    assert rows.status_code == 200
    assert rows.json()['count'] >= 1
