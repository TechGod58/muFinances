const { test, expect } = require('@playwright/test');

async function signInIfNeeded(page) {
  const username = page.getByLabel(/username|email/i);
  if (await username.count()) {
    await username.fill('Admin');
    await page.getByLabel(/password/i).fill('ChangeMe!3200');
    await page.getByRole('button', { name: /^sign in$/i }).click();
  }
}

test.describe('UI regression and accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3200');
    await signInIfNeeded(page);
  });

  test('command bar controls have accessible names and keyboard focus', async ({ page }) => {
    await expect(page.getByRole('button', { name: /import data/i })).toBeVisible();
    await expect(page.getByRole('button', { name: /export data/i })).toBeVisible();
    await expect(page.getByRole('button', { name: /market watch/i })).toBeVisible();
    await expect(page.getByRole('button', { name: /workspaces/i })).toBeVisible();

    await page.keyboard.press('Tab');
    const focused = await page.evaluate(() => document.activeElement?.tagName);
    expect(focused).toBeTruthy();
  });

  test('workspace menu toggles with keyboard', async ({ page }) => {
    const workspaces = page.getByRole('button', { name: /workspaces/i });
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

    await expect(tables.first()).toHaveAttribute(/aria-label|aria-labelledby/);
    await expect(tables.first()).toHaveAttribute('data-high-contrast-checked', 'true');
  });

  test('mobile layout keeps command buttons visible', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await expect(page.getByRole('button', { name: /import data/i })).toBeVisible();
    await expect(page.getByRole('button', { name: /workspaces/i })).toBeVisible();
  });

  test('desktop visual smoke screenshot', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await expect(page.locator('body')).toHaveScreenshot('mufinances-desktop-smoke.png', {
      maxDiffPixelRatio: 0.05,
    });
  });
});
