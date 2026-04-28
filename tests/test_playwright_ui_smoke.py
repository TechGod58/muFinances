from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

playwright_sync = pytest.importorskip('playwright.sync_api')
sync_playwright = playwright_sync.sync_playwright
PlaywrightError = playwright_sync.Error

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _wait_for_server(url: str, process: subprocess.Popen[bytes]) -> None:
    deadline = time.time() + 20
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError('Uvicorn exited before the UI smoke test could start.')
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except Exception:
            time.sleep(0.25)
    raise TimeoutError(f'Timed out waiting for {url}.')


def test_authenticated_ui_smoke(tmp_path: Path) -> None:
    port = '3320'
    env = os.environ.copy()
    env['CAMPUS_FPM_DB_PATH'] = str(tmp_path / 'playwright_ui.db')
    process = subprocess.Popen(
        [sys.executable, '-m', 'uvicorn', 'app.main:app', '--host', '127.0.0.1', '--port', port],
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        base_url = f'http://127.0.0.1:{port}'
        _wait_for_server(f'{base_url}/api/health', process)
        with sync_playwright() as playwright:
            try:
                browser = playwright.chromium.launch(headless=True)
            except PlaywrightError as exc:
                pytest.skip(f'Playwright Chromium runtime is not installed: {exc}')
            page = browser.new_page(viewport={'width': 1366, 'height': 900})
            page.goto(f'{base_url}/?v=23', wait_until='networkidle')
            page.locator('#loginForm input[name="email"]').fill('admin@mufinances.local')
            page.locator('#loginForm input[name="password"]').fill('ChangeMe!3200')
            page.get_by_role('button', name='Sign in').click()
            page.get_by_role('heading', name='UX productivity').wait_for(timeout=10000)
            assert page.get_by_label('Primary sections').is_visible()
            assert page.locator('#periodSelect').is_visible()
            assert page.locator('#appStatus').evaluate("node => getComputedStyle(node).display") == 'none'

            page.evaluate("window.location.hash = '#reporting'")
            page.get_by_role('heading', name='Reporting and analytics').wait_for(timeout=10000)
            assert page.locator('nav[aria-label="Primary sections"] a[aria-current="true"]').inner_text().startswith('Reporting')

            page.locator('#heroImportButton').click()
            page.get_by_role('heading', name='Import exported data').wait_for(timeout=5000)
            page.keyboard.press('Escape')
            page.locator('#heroExportButton').click()
            page.get_by_role('heading', name='Create Power BI export').wait_for(timeout=5000)
            page.keyboard.press('Escape')

            page.get_by_role('button', name='Market Watch').click()
            page.get_by_role('heading', name='Market Watch').wait_for(timeout=10000)
            page.get_by_label('Close Market Watch').click()
            assert page.locator('#marketSatellite').evaluate("node => node.classList.contains('hidden')")

            for hash_value, heading in [
                ('#model-builder', 'Model builder and allocations'),
                ('#integrations', 'Campus integrations'),
                ('#operations', 'Deployment operations'),
                ('#compliance', 'Compliance and audit hardening'),
            ]:
                page.evaluate(f"window.location.hash = '{hash_value}'")
                page.get_by_role('heading', name=heading).wait_for(timeout=10000)
            page.evaluate("window.location.hash = '#productivity'")
            page.set_viewport_size({'width': 390, 'height': 844})
            assert page.get_by_role('heading', name='UX productivity').is_visible()
            browser.close()
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
