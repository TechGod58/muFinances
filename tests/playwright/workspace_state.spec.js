const { test, expect } = require('@playwright/test');

test.describe('workspace state reliability', () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.removeItem('mufinances.workspace.controller.activeSections.v2');
      localStorage.removeItem('mufinances.workspace.controller.activeNumbers.v2');
      localStorage.removeItem('mufinances.workspace.controller.menuOpen');
      sessionStorage.removeItem('mufinances.workspace.controller.changedThisSession');
    });
    await page.goto('http://localhost:3200');
  });

  test('workspace menu opens from the command bar and toggles sections without clearing state', async ({ page }) => {
    const username = page.getByLabel(/username|email/i);
    const password = page.locator('#loginForm input[name="password"]');

    if (await username.count()) {
      await username.fill('admin@mufinances.local');
      await password.fill('ChangeMe!3200');
      await page.getByRole('button', { name: /^sign in$/i }).click();
      await expect(page.locator('#appShell')).toBeVisible();
    }

    const workspaceButton = page.locator('#workspaceMenuButton');
    await expect(workspaceButton).toBeVisible();

    await workspaceButton.click({ timeout: 5_000 }).catch(() => workspaceButton.dispatchEvent('click'));
    const tray = page.locator('#workspaceToggleTray');
    await expect(tray).toBeVisible();
    await expect(workspaceButton).toHaveAttribute('aria-expanded', 'true');

    const firstToggle = tray.locator('[data-workspace-number]').first();
    const toggleName = await firstToggle.textContent();
    await expect(firstToggle).toHaveAttribute('aria-pressed', 'true');
    await firstToggle.click({ timeout: 5_000 }).catch(() => firstToggle.dispatchEvent('click'));
    await expect(firstToggle).toHaveAttribute('aria-pressed', 'false');

    const visiblePanelCount = await page.locator('main section:visible, [data-workspace-panel]:visible, .workspace-panel:visible').count();
    expect(visiblePanelCount).toBeGreaterThan(0);

    await firstToggle.click({ timeout: 5_000 }).catch(() => firstToggle.dispatchEvent('click'));
    await expect(firstToggle).toHaveAttribute('aria-pressed', 'true');
    await expect(firstToggle).toContainText((toggleName || '').replace(/^\d+\s*/, '').trim().split(/\s+/)[0]);
  });
});
