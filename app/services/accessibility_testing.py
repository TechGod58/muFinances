from __future__ import annotations

from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
STATIC_DIR = ROOT / 'static'
TESTS_DIR = ROOT / 'tests'


def status() -> dict[str, Any]:
    index = _read(STATIC_DIR / 'index.html')
    styles = _read(STATIC_DIR / 'styles.css')
    app = _read(STATIC_DIR / 'app.js')
    smoke = _read(TESTS_DIR / 'test_playwright_ui_smoke.py')
    checks = {
        'keyboard_navigation_ready': all(token in index + styles for token in ['skip-link', 'tabindex="0"', ':focus-visible']),
        'screen_reader_labels_ready': all(token in index + app for token in ['aria-label', 'aria-live', '<caption']),
        'mobile_tablet_review_layout_ready': '@media (max-width: 760px)' in styles and '.table-wrap' in styles,
        'high_contrast_table_checks_ready': '@media (forced-colors: active)' in styles and 'CanvasText' in styles,
        'playwright_ui_smoke_tests_ready': 'playwright.sync_api' in smoke and 'test_authenticated_ui_smoke' in smoke,
    }
    counts = {
        'aria_labels': index.count('aria-label'),
        'aria_describedby': index.count('aria-describedby'),
        'focus_rules': styles.count(':focus-visible'),
        'playwright_smoke_tests': smoke.count('def test_'),
    }
    return {'batch': 'B23', 'title': 'Accessibility And UI Smoke Testing', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def _read(path: Path) -> str:
    if not path.exists():
        return ''
    return path.read_text(encoding='utf-8')
