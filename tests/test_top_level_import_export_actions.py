from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_command_deck_import_export_buttons_are_visible_and_wired() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')

    assert 'id="heroImportButton"' in index
    assert 'Import data' in index
    assert 'id="heroExportButton"' in index
    assert 'Export data' in index
    assert 'class="deck-footer"' in index
    assert index.index('id="heroImportButton"') < index.index('id="commandDeckToggle"')
    assert "$('#heroImportButton').addEventListener('click', () => importDialog.showModal())" in app_js
    assert "$('#heroExportButton').addEventListener('click', () => powerBiExportDialog.showModal())" in app_js
