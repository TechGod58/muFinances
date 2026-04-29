const { test, expect } = require('@playwright/test');

async function signIn(page) {
  await page.addInitScript(() => {
    localStorage.removeItem('mufinances.workspace.controller.activeSections.v2');
    localStorage.removeItem('mufinances.workspace.controller.activeNumbers.v2');
    localStorage.removeItem('mufinances.workspace.controller.menuOpen');
    sessionStorage.removeItem('mufinances.workspace.controller.changedThisSession');
  });
  await page.goto('/?v=b142', { waitUntil: 'networkidle' });
  const username = page.getByLabel(/username|email/i);
  if (await username.count()) {
    await username.fill('admin@mufinances.local');
    await page.locator('#loginForm input[name="password"]').fill('ChangeMe!3200');
    await page.getByRole('button', { name: /^sign in$/i }).click();
  }
  await expect(page.getByRole('heading', { name: /what do you want to do first/i })).toBeVisible();
}

async function clickOrDispatch(locator) {
  await locator.click({ timeout: 5_000 }).catch(() => locator.dispatchEvent('click'));
}

test.describe('B142 production UI coverage', () => {
  test('login, import/export, reporting, workspaces, dock and chat undock', async ({ page }) => {
    await signIn(page);
    await expect(page.locator('#appShell')).toBeVisible();
    await expect(page.locator('main')).not.toBeEmpty();

    const workspaceButton = page.locator('#workspaceMenuButton');
    await clickOrDispatch(workspaceButton);
    await expect(page.locator('#workspaceToggleTray')).toBeVisible();
    const reportingToggle = page.locator('#workspaceToggleTray button', { hasText: 'Reporting and analytics' }).first();
    await clickOrDispatch(reportingToggle);
    await expect(reportingToggle).toHaveAttribute('aria-pressed', /true|false/);
    await clickOrDispatch(reportingToggle);
    await expect(page.getByRole('heading', { name: 'Reporting and analytics' })).toBeVisible();

    const guidance = page.getByRole('heading', { name: 'Guidance and finance training' }).locator('xpath=ancestor::section[1]');
    const dockButton = guidance.locator('.dock-toggle-button').first();
    await expect(dockButton).toBeVisible();

    const importDialog = page.locator('#importDialog');
    await expect(page.locator('#heroImportButton')).toHaveAccessibleName(/import data/i);
    await clickOrDispatch(page.locator('#heroImportButton'));
    await expect(importDialog).toBeVisible();
    await page.keyboard.press('Escape');

    await expect(page.locator('#heroExportButton')).toHaveAccessibleName(/export data/i);
    await clickOrDispatch(page.locator('#heroExportButton'));
    await expect(page.locator('#powerBiExportDialog')).toBeVisible();
    await page.keyboard.press('Escape');

    const chatPopupPromise = page.waitForEvent('popup').catch(() => null);
    await clickOrDispatch(page.locator('#chatButton'));
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
    await expect(page.locator('#workspaceMenuButton')).toBeVisible();

    await page.setViewportSize({ width: 820, height: 1180 });
    await expect(page.locator('main')).not.toBeEmpty();
    await expect(page.locator('#heroImportButton')).toBeVisible();
  });
});
