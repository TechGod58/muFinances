from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_external_export_import_ui_supports_prophix_style_rows() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')

    assert 'Import exported data' in index
    assert 'Prophix export' in index
    assert 'name="export_rows"' in index
    assert 'CSV or tab-delimited rows' in index
    assert 'function parseExportRows' in app_js
    assert 'function normalizeImportHeader' in app_js
    assert "cost_center: 'department_code'" in app_js
    assert "gl_account: 'account_code'" in app_js
    assert "mode: 'export_file'" in app_js
