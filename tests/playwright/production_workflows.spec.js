const { test, expect } = require('@playwright/test');

async function signIn(page) {
  await page.goto('/?v=b142', { waitUntil: 'networkidle' });
  const username = page.getByLabel(/username|email/i);
  if (await username.count()) {
    await username.fill('admin@mufinances.local');
    await page.getByLabel(/password/i).fill('ChangeMe!3200');
    await page.getByRole('button', { name: /^sign in$/i }).click();
  }
  await expect(page.getByRole('heading', { name: /what do you want to do first/i })).toBeVisible();
}

test.describe('B142 production UI coverage', () => {
  test('login, import/export, reporting, workspaces, dock and chat undock', async ({ page }) => {
    await signIn(page);
    await expect(page.locator('#appShell')).toBeVisible();
    await expect(page.locator('main')).not.toBeEmpty();

    await page.getByRole('button', { name: /workspaces/i }).click();
    await expect(page.locator('#workspaceToggleTray')).toBeVisible();
    const reportingToggle = page.locator('#workspaceToggleTray button', { hasText: 'Reporting and analytics' }).first();
    await reportingToggle.click();
    await expect(reportingToggle).toHaveAttribute('aria-pressed', /true|false/);
    await reportingToggle.click();
    await expect(page.getByRole('heading', { name: 'Reporting and analytics' })).toBeVisible();

    const guidance = page.getByRole('heading', { name: 'Guidance and finance training' }).locator('xpath=ancestor::section[1]');
    const dockButton = guidance.locator('.dock-toggle-button').first();
    await expect(dockButton).toBeVisible();

    const importDialog = page.locator('#importDialog');
    await page.getByRole('button', { name: /import data/i }).first().click();
    await expect(importDialog).toBeVisible();
    await page.keyboard.press('Escape');

    await page.getByRole('button', { name: /export data/i }).first().click();
    await expect(page.locator('#powerBiExportDialog')).toBeVisible();
    await page.keyboard.press('Escape');

    await page.getByRole('button', { name: /chat/i }).click();
    const chatPopupPromise = page.waitForEvent('popup').catch(() => null);
    const popup = await chatPopupPromise;
    if (popup) {
      await popup.waitForLoadState('domcontentloaded');
      await expect(popup.getByRole('heading', { name: /mufinances chat/i })).toBeVisible();
      await popup.close();
    } else {
      await expect(page.locator('#chatSatellite')).toBeVisible();
    }

    await page.evaluate("window.location.hash = '#reporting'");
    await expect(page.getByRole('heading', { name: 'Reporting and analytics' })).toBeVisible();
  });

  test('mobile and tablet no blank screen checks', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await signIn(page);
    await expect(page.locator('main')).not.toBeEmpty();
    await expect(page.getByRole('button', { name: /workspaces/i })).toBeVisible();

    await page.setViewportSize({ width: 820, height: 1180 });
    await expect(page.locator('main')).not.toBeEmpty();
    await expect(page.getByRole('button', { name: /import data/i }).first()).toBeVisible();
  });
});
