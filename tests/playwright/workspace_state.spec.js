const { test, expect } = require('@playwright/test');

test.describe('workspace state reliability', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3200');
  });

  test('workspace menu opens from the command bar and toggles sections without clearing state', async ({ page }) => {
    const username = page.getByLabel(/username|email/i);
    const password = page.getByLabel(/password/i);

    if (await username.count()) {
      await username.fill('Admin');
      await password.fill('ChangeMe!3200');
      await page.getByRole('button', { name: /^sign in$/i }).click();
    }

    const workspaceButton = page.getByRole('button', { name: /workspaces/i }).first();
    await expect(workspaceButton).toBeVisible();

    await workspaceButton.click();
    const tray = page.locator('#workspaceToggleTray');
    await expect(tray).toBeVisible();
    await expect(workspaceButton).toHaveAttribute('aria-expanded', 'true');

    const firstToggle = tray.locator('[data-workspace-number]').first();
    const toggleName = await firstToggle.textContent();
    await firstToggle.click();
    await expect(firstToggle).toHaveAttribute('aria-pressed', 'true');

    const visiblePanelCount = await page.locator('main section:visible, [data-workspace-panel]:visible, .workspace-panel:visible').count();
    expect(visiblePanelCount).toBeGreaterThan(0);

    await firstToggle.click();
    await expect(firstToggle).toHaveAttribute('aria-pressed', 'false');
    await expect(firstToggle).toContainText((toggleName || '').replace(/^\d+\s*/, '').trim().split(/\s+/)[0]);
  });
});
