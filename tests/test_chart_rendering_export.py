from __future__ import annotations

import os
import sys
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB = Path(__file__).resolve().parent / 'test_chart_rendering_export.db'
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
    scenarios = client.get('/api/scenarios', headers=headers).json()
    return int(next(item for item in scenarios if item['name'] == 'FY27 Operating Plan')['id'])


def create_chart(headers: dict[str, str], sid: int) -> dict[str, object]:
    response = client.post(
        '/api/reporting/charts',
        headers=headers,
        json={
            'scenario_id': sid,
            'name': 'B64 Department Chart',
            'chart_type': 'bar',
            'dataset_type': 'period_range',
            'config': {
                'dimension': 'department_code',
                'period_start': '2026-07',
                'period_end': '2026-12',
                'format': {'palette': ['#7df0c6', '#f6c453'], 'show_data_labels': True},
            },
        },
    )
    assert response.status_code == 200
    return response.json()


def test_chart_rendering_png_svg_snapshots_and_package_exports() -> None:
    headers = admin_headers()
    sid = scenario_id(headers)
    chart = create_chart(headers, sid)

    svg = client.post(
        f"/api/reporting/charts/{chart['id']}/render",
        headers=headers,
        json={'render_format': 'svg', 'width': 960, 'height': 540},
    )
    assert svg.status_code == 200
    svg_payload = svg.json()
    assert svg_payload['content_type'] == 'image/svg+xml'
    assert svg_payload['renderer'] == 'mu-chart-renderer-v1'
    assert Path(svg_payload['storage_path']).read_text(encoding='utf-8').startswith('<svg')

    png = client.post(
        f"/api/reporting/charts/{chart['id']}/render",
        headers=headers,
        json={'render_format': 'png', 'width': 640, 'height': 360},
    )
    assert png.status_code == 200
    png_payload = png.json()
    assert png_payload['content_type'] == 'image/png'
    assert Path(png_payload['storage_path']).read_bytes().startswith(b'\x89PNG\r\n\x1a\n')

    snapshot = client.post(
        '/api/reporting/dashboard-chart-snapshots',
        headers=headers,
        json={'scenario_id': sid, 'chart_id': chart['id'], 'render_id': svg_payload['id']},
    )
    assert snapshot.status_code == 200
    assert snapshot.json()['snapshot_type'] == 'dashboard_chart'
    assert snapshot.json()['payload']['render']['visual_hash'] == svg_payload['visual_hash']

    svg_artifact = client.post(
        '/api/reporting/artifacts',
        headers=headers,
        json={'scenario_id': sid, 'artifact_type': 'svg', 'file_name': 'b64-chart', 'chart_id': chart['id']},
    )
    assert svg_artifact.status_code == 200
    assert svg_artifact.json()['content_type'] == 'image/svg+xml'
    assert '<svg' in Path(svg_artifact.json()['storage_path']).read_text(encoding='utf-8')

    pptx = client.post(
        '/api/reporting/artifacts',
        headers=headers,
        json={'scenario_id': sid, 'artifact_type': 'pptx', 'file_name': 'b64-board-charts', 'chart_id': chart['id']},
    )
    assert pptx.status_code == 200
    with zipfile.ZipFile(Path(pptx.json()['storage_path'])) as archive:
        names = set(archive.namelist())
        assert 'ppt/media/chart1.svg' in names
        assert '<svg' in archive.read('ppt/media/chart1.svg').decode('utf-8')

    package = client.post(
        '/api/reporting/board-packages',
        headers=headers,
        json={'scenario_id': sid, 'package_name': 'B64 Board Package', 'period_start': '2026-07', 'period_end': '2026-12'},
    )
    assert package.status_code == 200
    pdf = client.post(
        '/api/reporting/artifacts',
        headers=headers,
        json={'scenario_id': sid, 'artifact_type': 'pdf', 'file_name': 'b64-board', 'package_id': package.json()['id']},
    )
    assert pdf.status_code == 200
    assert pdf.json()['metadata']['chart_image_embeds'] >= 1
    assert b'Chart image:' in Path(pdf.json()['storage_path']).read_bytes()


def test_b64_status_migration_and_ui_surface() -> None:
    headers = admin_headers()
    status = client.get('/api/reporting/chart-rendering/status', headers=headers)
    assert status.status_code == 200
    payload = status.json()
    assert payload['batch'] == 'B64'
    assert payload['complete'] is True
    assert payload['checks']['png_svg_export_ready'] is True

    migrations = client.get('/api/foundation/migrations', headers=headers)
    keys = {row['migration_key'] for row in migrations.json()['migrations']}
    assert '0064_real_chart_rendering_export_engine' in keys

    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')
    assert 'id="chartRenderTable"' in index
    assert 'id="dashboardChartSnapshotTable"' in index
    assert '/api/reporting/chart-rendering/workspace' in app_js
    assert 'handleReportChartRender' in app_js
