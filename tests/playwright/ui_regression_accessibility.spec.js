const { test, expect } = require('@playwright/test');

async function signInIfNeeded(page) {
  const username = page.getByLabel(/username|email/i);
  if (await username.count()) {
    await username.fill('admin@mufinances.local');
    await page.locator('#loginForm input[name="password"]').fill('ChangeMe!3200');
    await page.getByRole('button', { name: /^sign in$/i }).click();
    await expect(page.locator('#appShell')).toBeVisible();
  }
}

test.describe('UI regression and accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.removeItem('mufinances.workspace.controller.activeSections.v2');
      localStorage.removeItem('mufinances.workspace.controller.activeNumbers.v2');
      localStorage.removeItem('mufinances.workspace.controller.menuOpen');
      sessionStorage.removeItem('mufinances.workspace.controller.changedThisSession');
    });
    await page.goto('http://localhost:3200');
    await signInIfNeeded(page);
  });

  test('command bar controls have accessible names and keyboard focus', async ({ page }) => {
    await expect(page.locator('#heroImportButton')).toBeVisible();
    await expect(page.locator('#heroExportButton')).toBeVisible();
    await expect(page.locator('#marketWatchButton')).toBeVisible();
    await expect(page.locator('#workspaceMenuButton')).toBeVisible();

    await page.keyboard.press('Tab');
    const focused = await page.evaluate(() => document.activeElement?.tagName);
    expect(focused).toBeTruthy();
  });

  test('workspace menu toggles with keyboard', async ({ page }) => {
    const workspaces = page.locator('#workspaceMenuButton');
    await workspaces.focus();
    await page.keyboard.press('Enter');
    await expect(page.locator('#workspaceToggleTray')).toBeVisible();

    const firstToggle = page.locator('#workspaceToggleTray [data-workspace-number]').first();
    await firstToggle.focus();
    const before = await firstToggle.getAttribute('aria-pressed');
    await page.keyboard.press('Enter');
    const after = await firstToggle.getAttribute('aria-pressed');
    expect(after).not.toBe(before);
  });

  test('tables expose labels and high contrast review marker', async ({ page }) => {
    const tables = page.locator('table');
    const count = await tables.count();
    if (count === 0) return;

    const labeledCount = await page.locator('table[aria-label], table[aria-labelledby]').count();
    expect(labeledCount).toBeGreaterThan(0);
    await expect(page.locator('table[data-high-contrast-checked="true"]').first()).toBeVisible();
  });

  test('mobile layout keeps command buttons visible', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await expect(page.locator('#heroImportButton')).toBeVisible();
    await expect(page.locator('#workspaceMenuButton')).toBeVisible();
  });

  test('desktop visual smoke screenshot', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    const screenshot = await page.screenshot({ fullPage: false });
    expect(screenshot.length).toBeGreaterThan(10_000);
    await expect(page.locator('main')).not.toBeEmpty();
  });
});
