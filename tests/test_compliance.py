from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_compliance.db'
if TEST_DB.exists():
    TEST_DB.unlink()
os.environ['CAMPUS_FPM_DB_PATH'] = str(TEST_DB)

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def admin_headers() -> dict[str, str]:
    response = client.post(
        '/api/auth/login',
        json={'email': 'admin@mufinances.local', 'password': 'ChangeMe!3200'},
    )
    assert response.status_code == 200
    return {'Authorization': f"Bearer {response.json()['token']}"}


def seeded_scenario_id(headers: dict[str, str]) -> int:
    scenarios = client.get('/api/scenarios', headers=headers).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def test_compliance_status_reports_b33_complete() -> None:
    response = client.get('/api/compliance/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B33'
    assert payload['complete'] is True
    assert payload['checks']['immutable_audit_ready'] is True
    assert payload['counts']['sod_rules'] >= 3


def test_audit_hash_chain_can_be_sealed_and_verified() -> None:
    headers = admin_headers()
    seal = client.post('/api/compliance/audit/seal', headers=headers)
    assert seal.status_code == 200
    verify = client.get('/api/compliance/audit/verify', headers=headers)
    assert verify.status_code == 200
    payload = verify.json()
    assert payload['valid'] is True
    assert payload['unsealed'] == 0
    assert payload['verified'] >= 1


def test_retention_policy_and_certification_controls() -> None:
    headers = admin_headers()
    scenario_id = seeded_scenario_id(headers)

    policy = client.post(
        '/api/compliance/retention-policies',
        headers=headers,
        json={
            'policy_key': 'audit-seven-year',
            'entity_type': 'audit_logs',
            'retention_years': 7,
            'disposition_action': 'archive',
            'legal_hold': False,
            'active': True,
        },
    )
    assert policy.status_code == 200
    assert policy.json()['retention_years'] == 7

    review = client.get('/api/compliance/retention-review', headers=headers)
    assert review.status_code == 200
    assert review.json()['policy_count'] >= 1
    assert review.json()['coverage'][0]['current_records'] >= 1

    created = client.post(
        '/api/compliance/certifications',
        headers=headers,
        json={
            'scenario_id': scenario_id,
            'certification_key': 'fy27-close-cert',
            'control_area': 'close',
            'period': '2026-09',
            'owner': 'controller@mufinances.local',
            'notes': 'Close package reviewed.',
        },
    )
    assert created.status_code == 200
    certified = client.post(
        f"/api/compliance/certifications/{created.json()['id']}/certify",
        headers=headers,
        json={'evidence': {'audit_packet': 'packet-001'}, 'notes': 'Approved for board package.'},
    )
    assert certified.status_code == 200
    assert certified.json()['status'] == 'certified'
    assert certified.json()['evidence']['audit_packet'] == 'packet-001'


def test_sod_report_flags_same_actor_create_and_approve() -> None:
    headers = admin_headers()
    scenario_id = seeded_scenario_id(headers)
    submission = client.post(
        '/api/operating-budget/submissions',
        headers=headers,
        json={'scenario_id': scenario_id, 'department_code': 'B33', 'owner': 'Admin', 'notes': 'SoD test'},
    )
    assert submission.status_code == 200
    approved = client.post(f"/api/operating-budget/submissions/{submission.json()['id']}/approve", headers=headers, json={'note': 'Approved by same actor'})
    assert approved.status_code == 200

    report = client.get('/api/compliance/sod-report', headers=headers)
    assert report.status_code == 200
    violations = report.json()['violations']
    assert any(item['type'] == 'action_pair' and item['entity_type'] == 'budget_submission' for item in violations)
