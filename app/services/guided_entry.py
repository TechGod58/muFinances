from __future__ import annotations

from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
STATIC_DIR = ROOT / 'static'


def status() -> dict[str, Any]:
    index = _read(STATIC_DIR / 'index.html')
    app = _read(STATIC_DIR / 'app.js')
    styles = _read(STATIC_DIR / 'styles.css')
    checks = {
        'start_here_panel_ready': 'id="guidedStart"' in index and 'What do you want to do first?' in index,
        'manual_entry_wizard_ready': 'id="guidedManualDialog"' in index and 'function handleGuidedManualSave' in app,
        'import_wizard_ready': 'id="guidedImportDialog"' in index and 'function handleGuidedImportRun' in app,
        'export_wizard_ready': 'id="guidedExportDialog"' in index and 'function handleGuidedExportRun' in app,
        'non_user_copy_ready': 'No software knowledge needed' in index,
        'wizard_accessibility_ready': 'aria-labelledby="guidedStartTitle"' in index and '.guide-card' in styles,
    }
    counts = {
        'guided_buttons': index.count('guided'),
        'wizard_dialogs': index.count('guided') and sum(index.count(item) for item in ['guidedManualDialog', 'guidedImportDialog', 'guidedExportDialog']),
    }
    return {'batch': 'B25', 'title': 'Guided Data Entry And Import Wizard', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def _read(path: Path) -> str:
    if not path.exists():
        return ''
    return path.read_text(encoding='utf-8')
