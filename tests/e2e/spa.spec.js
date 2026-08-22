const { test, expect } = require('@playwright/test');

test('candidate landing renders access-code entry instead of a blank screen', async ({ page }) => {
  await page.goto('/');
  await expect(page.locator('#code-inp')).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Enter Your Access Code' })).toBeVisible();
  await expect(page.locator('#app')).not.toBeEmpty();
});

test('admin route falls back to configured login when no SSO cookie exists', async ({ page }) => {
  await page.goto('/?admin=1');
  await expect(page.getByRole('heading', { name: 'Admin Access' })).toBeVisible();
  await expect(page.locator('#pwd')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Access Console' })).toBeVisible();
});

test('missing required client module leaves a visible recovery path', async ({ page }) => {
  await page.route('**/client/code-entry.js', (route) => route.abort());
  await page.goto('/');
  await expect(page.locator('body')).toContainText(/failed|reload|unavailable/i);
  await expect(page.locator('body')).not.toHaveText('');
});
