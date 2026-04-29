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
            raise RuntimeError('Uvicorn exited before the frontend hardening test could start.')
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except Exception:
            time.sleep(0.25)
    raise TimeoutError(f'Timed out waiting for {url}.')


def _login(page, base_url: str) -> None:
    page.goto(f'{base_url}/?v=120', wait_until='networkidle')
    page.locator('#loginForm input[name="email"]').fill('admin@mufinances.local')
    page.locator('#loginForm input[name="password"]').fill('ChangeMe!3200')
    page.get_by_role('button', name='Sign in').click()
    page.locator('#workspaceMenuButton').wait_for(timeout=10000)
    page.get_by_role('heading', name='What do you want to do first?').wait_for(timeout=10000)


def test_frontend_state_workspace_dock_chat_and_layout_regression(tmp_path: Path) -> None:
    port = '3321'
    env = os.environ.copy()
    env['CAMPUS_FPM_DB_PATH'] = str(tmp_path / 'playwright_state_hardening.db')
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
            context = browser.new_context(viewport={'width': 1440, 'height': 950})
            page = context.new_page()
            _login(page, base_url)

            assert page.locator('#appStatus').evaluate("node => getComputedStyle(node).display") == 'none'
            start_section = page.get_by_role('heading', name='What do you want to do first?').locator('xpath=ancestor::section[1]')
            gap = page.evaluate(
                """() => {
                  const openHide = document.querySelector('#commandDeckToggle');
                  const bar = openHide?.parentElement;
                  const heading = Array.from(document.querySelectorAll('h1,h2,h3')).find((node) => node.textContent.includes('What do you want to do first?'));
                  const section = heading?.closest('section');
                  if (!bar || !section) return 9999;
                  return section.getBoundingClientRect().top - bar.getBoundingClientRect().bottom;
                }"""
            )
            assert 0 <= gap <= 140
            assert start_section.is_visible()

            page.locator('#workspaceMenuButton').click()
            tray = page.locator('#workspaceToggleTray')
            assert tray.is_visible()
            reporting_toggle = tray.locator('button', has_text='Reporting and analytics').first
            assert reporting_toggle.get_attribute('aria-pressed') == 'true'
            assert 'workspace-toggle-active' in (reporting_toggle.get_attribute('class') or '')
            reporting_heading = page.get_by_role('heading', name='Reporting and analytics')
            reporting_heading.wait_for(timeout=10000)

            reporting_toggle.click()
            page.wait_for_function(
                """() => {
                  const heading = Array.from(document.querySelectorAll('h1,h2,h3')).find((node) => node.textContent.trim() === 'Reporting and analytics');
                  const section = heading?.closest('section');
                  return section && (section.hidden || getComputedStyle(section).display === 'none');
                }"""
            )
            assert reporting_toggle.get_attribute('aria-pressed') == 'false'
            assert 'workspace-toggle-active' not in (reporting_toggle.get_attribute('class') or '')

            reporting_toggle.click()
            page.get_by_role('heading', name='Reporting and analytics').wait_for(timeout=10000)
            assert reporting_toggle.get_attribute('aria-pressed') == 'true'

            page.locator('#workspaceMenuButton').click()
            with page.expect_popup() as chat_popup_info:
                page.locator('#chatButton').click()
            chat_popup = chat_popup_info.value
            chat_popup.wait_for_load_state('domcontentloaded')
            assert 'chat-window.html' in chat_popup.url
            chat_popup.close()

            dock_button = page.get_by_role('heading', name='Guidance and finance training').locator('xpath=ancestor::section[1]').locator('.dock-toggle-button').first
            with page.expect_popup() as dock_popup_info:
                dock_button.click()
            dock_popup = dock_popup_info.value
            dock_popup.wait_for_load_state('domcontentloaded')
            dock_popup.get_by_role('heading', name='Guidance and finance training').wait_for(timeout=10000)
            assert not page.get_by_role('heading', name='Guidance and finance training').is_visible()
            dock_popup.close()
            page.get_by_role('heading', name='Guidance and finance training').wait_for(timeout=10000)

            page.locator('#heroImportButton').click()
            page.get_by_role('heading', name='Import exported data').wait_for(timeout=5000)
            page.keyboard.press('Escape')
            page.locator('#heroExportButton').click()
            page.get_by_role('heading', name='Create Power BI export').wait_for(timeout=5000)
            page.keyboard.press('Escape')

            page.evaluate("window.location.hash = '#reporting'")
            page.get_by_role('heading', name='Reporting and analytics').wait_for(timeout=10000)
            assert page.locator('nav[aria-label="Primary sections"] a[aria-current="true"]').inner_text().startswith('Reporting')

            page.set_viewport_size({'width': 768, 'height': 1024})
            assert page.locator('#workspaceMenuButton').is_visible()
            page.set_viewport_size({'width': 390, 'height': 844})
            assert page.get_by_role('heading', name='Reporting and analytics').is_visible()

            context.close()
            browser.close()
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
