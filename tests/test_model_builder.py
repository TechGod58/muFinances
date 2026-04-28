from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_model_builder.db'
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


def test_model_builder_status_reports_b32_complete() -> None:
    response = client.get('/api/model-builder/status', headers=admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload['batch'] == 'B32'
    assert payload['complete'] is True
    assert payload['checks']['allocation_rules_ready'] is True


def test_enterprise_modeling_engine_cube_publish_invalidation_and_performance() -> None:
    headers = admin_headers()
    scenario_id = seeded_scenario_id(headers)
    for department_code, account_code, amount in [
        ('ART', 'B38_TUITION', 1000),
        ('SCI', 'B38_TUITION', 2000),
        ('OPS', 'B38_EXPENSE', -750),
    ]:
        response = client.post(
            '/api/foundation/ledger',
            headers=headers,
            json={
                'scenario_id': scenario_id,
                'department_code': department_code,
                'fund_code': 'GEN',
                'account_code': account_code,
                'period': '2026-08',
                'amount': amount,
                'source': 'b38-test',
            },
        )
        assert response.status_code == 200

    model = client.post(
        '/api/model-builder/models',
        headers=headers,
        json={'scenario_id': scenario_id, 'model_key': 'enterprise-cube', 'name': 'Enterprise Cube', 'status': 'active'},
    )
    assert model.status_code == 200
    model_id = model.json()['id']

    base = client.post(
        '/api/model-builder/formulas',
        headers=headers,
        json={
            'model_id': model_id,
            'formula_key': 'base_margin',
            'label': 'Base margin',
            'expression': 'ACCOUNT_B38_TUITION + ACCOUNT_B38_EXPENSE',
            'target_account_code': 'B38_MARGIN',
            'period_start': '2026-08',
            'period_end': '2026-08',
        },
    )
    assert base.status_code == 200
    dependent = client.post(
        '/api/model-builder/formulas',
        headers=headers,
        json={
            'model_id': model_id,
            'formula_key': 'margin_with_growth',
            'label': 'Margin with growth',
            'expression': 'base_margin * 1.05',
            'target_account_code': 'B38_MARGIN_GROWTH',
            'period_start': '2026-08',
            'period_end': '2026-08',
        },
    )
    assert dependent.status_code == 200

    status = client.get('/api/model-builder/enterprise-status', headers=headers)
    assert status.status_code == 200
    assert status.json()['batch'] == 'B38'
    assert status.json()['checks']['sparse_dense_dimension_ready'] is True

    cube = client.post(f'/api/model-builder/models/{model_id}/cube/build', headers=headers)
    assert cube.status_code == 200
    assert cube.json()['cell_count'] >= 3
    assert 'period' in cube.json()['dense_dimensions']
    assert 'department' in cube.json()['sparse_dimensions'] or 'department' in cube.json()['dense_dimensions']

    order = client.get(f'/api/model-builder/models/{model_id}/calculation-order', headers=headers)
    assert order.status_code == 200
    keys = [step['key'] for step in order.json()['steps']]
    assert keys.index('base_margin') < keys.index('margin_with_growth')

    invalidated = client.post(f'/api/model-builder/models/{model_id}/dependencies/invalidate?reason=driver_change', headers=headers)
    assert invalidated.status_code == 200
    assert invalidated.json()['invalidated'] == 2

    published = client.post(f'/api/model-builder/models/{model_id}/publish', headers=headers)
    assert published.status_code == 200
    assert published.json()['status'] == 'published'
    assert published.json()['version_key'] == 'enterprise-cube-v1'
    assert published.json()['calculation_order'][0]['key'] == 'base_margin'

    perf = client.post(f'/api/model-builder/models/{model_id}/performance-test', headers=headers)
    assert perf.status_code == 200
    assert perf.json()['status'] == 'passed'
    assert perf.json()['cube_cell_count'] >= 3

    workspace = client.get(f'/api/model-builder/models/{model_id}/enterprise-workspace', headers=headers)
    assert workspace.status_code == 200
    assert workspace.json()['versions'][0]['version_key'] == 'enterprise-cube-v1'
    assert workspace.json()['performance_tests'][0]['status'] == 'passed'


def test_formula_recalculation_posts_derived_ledger_entries() -> None:
    headers = admin_headers()
    scenario_id = seeded_scenario_id(headers)

    driver = client.post(
        '/api/scenario-engine/drivers',
        headers=headers,
        json={
            'scenario_id': scenario_id,
            'driver_key': 'headcount',
            'label': 'Headcount',
            'driver_type': 'count',
            'unit': 'students',
            'value': 25,
        },
    )
    assert driver.status_code == 200
    model = client.post(
        '/api/model-builder/models',
        headers=headers,
        json={'scenario_id': scenario_id, 'model_key': 'student-fees', 'name': 'Student Fee Model', 'status': 'active'},
    )
    assert model.status_code == 200
    model_id = model.json()['id']

    formula = client.post(
        '/api/model-builder/formulas',
        headers=headers,
        json={
            'model_id': model_id,
            'formula_key': 'activity_fee',
            'label': 'Activity fee',
            'expression': 'headcount * 120',
            'target_account_code': 'FEES',
            'target_department_code': 'ART',
            'period_start': '2026-08',
            'period_end': '2026-08',
        },
    )
    assert formula.status_code == 200

    run = client.post(f'/api/model-builder/models/{model_id}/recalculate', headers=headers)
    assert run.status_code == 200
    payload = run.json()
    assert payload['status'] == 'posted'
    assert payload['formula_count'] == 1
    assert payload['ledger_entry_count'] == 1
    assert payload['created_entries'][0]['amount'] == 3000
    assert payload['created_entries'][0]['metadata']['formula_id'] == formula.json()['id']
    assert payload['created_entries'][0]['metadata']['formula_trace']
    assert payload['messages'][0]['formula_key'] == 'activity_fee'
    assert payload['messages'][0]['trace']


def test_allocation_rule_distributes_source_amount_by_basis_account() -> None:
    headers = admin_headers()
    scenario_id = seeded_scenario_id(headers)
    for department_code, amount in [('ART', 100), ('SCI', 300)]:
        response = client.post(
            '/api/foundation/ledger',
            headers=headers,
            json={
                'scenario_id': scenario_id,
                'department_code': department_code,
                'fund_code': 'GEN',
                'account_code': 'B32_HEADCOUNT',
                'period': '2026-09',
                'amount': amount,
                'source': 'test',
            },
        )
        assert response.status_code == 200
    source = client.post(
        '/api/foundation/ledger',
        headers=headers,
        json={
            'scenario_id': scenario_id,
            'department_code': 'OPS',
            'fund_code': 'GEN',
            'account_code': 'B32_UTILITIES',
            'period': '2026-09',
            'amount': -4000,
            'source': 'test',
        },
    )
    assert source.status_code == 200

    model = client.post(
        '/api/model-builder/models',
        headers=headers,
        json={'scenario_id': scenario_id, 'model_key': 'shared-costs', 'name': 'Shared Costs', 'status': 'active'},
    )
    model_id = model.json()['id']
    rule = client.post(
        '/api/model-builder/allocation-rules',
        headers=headers,
        json={
            'model_id': model_id,
            'rule_key': 'utilities-by-headcount',
            'label': 'Utilities by headcount',
            'source_account_code': 'B32_UTILITIES',
            'source_department_code': 'OPS',
            'target_account_code': 'UTILITIES_ALLOC',
            'basis_account_code': 'B32_HEADCOUNT',
            'target_department_codes': ['ART', 'SCI'],
            'period_start': '2026-09',
            'period_end': '2026-09',
        },
    )
    assert rule.status_code == 200

    run = client.post(f'/api/model-builder/models/{model_id}/recalculate', headers=headers)
    assert run.status_code == 200
    entries = sorted(run.json()['created_entries'], key=lambda item: item['department_code'])
    assert [entry['department_code'] for entry in entries] == ['ART', 'SCI']
    assert [entry['amount'] for entry in entries] == [-1000, -3000]


def test_circular_formula_dependency_fails_without_posting_entries() -> None:
    headers = admin_headers()
    scenario_id = seeded_scenario_id(headers)
    model = client.post(
        '/api/model-builder/models',
        headers=headers,
        json={'scenario_id': scenario_id, 'model_key': 'cycle-test', 'name': 'Cycle Test', 'status': 'active'},
    )
    model_id = model.json()['id']
    for key, expression in [('first', 'second + 1'), ('second', 'first + 1')]:
        response = client.post(
            '/api/model-builder/formulas',
            headers=headers,
            json={
                'model_id': model_id,
                'formula_key': key,
                'label': key.title(),
                'expression': expression,
                'target_account_code': key.upper(),
                'period_start': '2026-10',
                'period_end': '2026-10',
            },
        )
        assert response.status_code == 200

    graph = client.get(f'/api/model-builder/models/{model_id}/dependency-graph', headers=headers)
    assert graph.status_code == 200
    assert graph.json()['has_cycles'] is True

    run = client.post(f'/api/model-builder/models/{model_id}/recalculate', headers=headers)
    assert run.status_code == 200
    assert run.json()['status'] == 'failed'
    assert run.json()['ledger_entry_count'] == 0
    assert run.json()['messages'][0]['severity'] == 'error'


def test_formula_lint_endpoint_and_circular_stress_detection() -> None:
    headers = admin_headers()
    scenario_id = seeded_scenario_id(headers)

    lint = client.post(
        '/api/model-builder/formulas/lint',
        headers=headers,
        json={'expression': 'headcount * round(rate, 2)', 'context': {'headcount': 25, 'rate': 4.567}, 'evaluate': True},
    )
    assert lint.status_code == 200
    assert lint.json()['ok'] is True
    assert lint.json()['evaluation']['value'] == 114.25

    blocked = client.post(
        '/api/model-builder/formulas/lint',
        headers=headers,
        json={'expression': '__import__("os").system("dir")'},
    )
    assert blocked.status_code == 200
    assert blocked.json()['ok'] is False

    model = client.post(
        '/api/model-builder/models',
        headers=headers,
        json={'scenario_id': scenario_id, 'model_key': 'cycle-stress', 'name': 'Cycle Stress', 'status': 'active'},
    )
    model_id = model.json()['id']
    for index in range(1, 13):
        next_key = f'f{index + 1}' if index < 12 else 'f1'
        response = client.post(
            '/api/model-builder/formulas',
            headers=headers,
            json={
                'model_id': model_id,
                'formula_key': f'f{index}',
                'label': f'Formula {index}',
                'expression': f'{next_key} + {index}',
                'target_account_code': f'F{index}',
                'period_start': '2026-10',
                'period_end': '2026-10',
            },
        )
        assert response.status_code == 200

    graph = client.get(f'/api/model-builder/models/{model_id}/dependency-graph', headers=headers)
    assert graph.status_code == 200
    assert graph.json()['has_cycles'] is True
    assert len(graph.json()['cycles'][0]) >= 12


def test_enterprise_modeling_ui_contract() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')

    assert 'id="buildModelCubeButton"' in index
    assert 'id="publishModelButton"' in index
    assert 'id="testModelPerformanceButton"' in index
    assert 'id="modelCubeDimensionTable"' in index
    assert 'id="modelCalculationOrderTable"' in index
    assert 'id="modelVersionTable"' in index
    assert 'handleModelCubeBuild' in app_js
    assert 'handleModelPublish' in app_js
    assert 'handleModelPerformanceTest' in app_js
