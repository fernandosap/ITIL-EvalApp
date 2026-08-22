const { test, expect } = require('@playwright/test');

async function enterValidCode(page) {
  await page.goto('/');
  await page.locator('#code-inp').fill('ABC234');
  await page.getByRole('button', { name: /Continue/ }).click();
  await expect(page.getByRole('heading', { name: 'Before You Begin' })).toBeVisible();
}

async function startExam(page) {
  await enterValidCode(page);
  await page.locator('#cb-consent').check();
  await page.getByRole('button', { name: /Continue/ }).click();
  await expect(page.locator('.q-stem')).toContainText('Which framework is being assessed?');
}

test('candidate landing renders access-code entry instead of a blank screen', async ({ page }) => {
  await page.goto('/');
  await expect(page.locator('#code-inp')).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Enter Your Access Code' })).toBeVisible();
  await expect(page.locator('#app')).not.toBeEmpty();
});

test('short access code is rejected locally with a visible recovery action', async ({ page }) => {
  await page.goto('/');
  await page.locator('#code-inp').fill('ABC');
  await page.getByRole('button', { name: /Continue/ }).click();
  await expect(page.locator('#m-title')).toHaveText('Invalid Code');
  await expect(page.getByRole('button', { name: 'Try Again' })).toBeVisible();
});

test('unknown full access code is rejected without blanking the app', async ({ page }) => {
  await page.goto('/');
  await page.locator('#code-inp').fill('ZZZ999');
  await page.getByRole('button', { name: /Continue/ }).click();
  await expect(page.locator('#m-title')).toHaveText('Code Not Recognised');
  await expect(page.locator('#app')).not.toBeEmpty();
});

test('valid access code reaches consent and enforces consent', async ({ page }) => {
  await enterValidCode(page);
  await page.getByRole('button', { name: /Continue/ }).click();
  await expect(page.locator('#m-title')).toHaveText('Consent Required');
});

test('candidate can start a non-proctored exam and render the first question', async ({ page }) => {
  await startExam(page);
  await expect(page.locator('.header-code')).toContainText('ABC234');
  await expect(page.locator('#timer')).toBeVisible();
  await expect(page.locator('.option')).toHaveCount(3);
});

test('candidate can navigate, answer all questions and submit', async ({ page }) => {
  await startExam(page);

  await page.locator('.option').nth(0).click();
  await page.getByRole('button', { name: /Next/ }).click();
  await expect(page.locator('.q-stem')).toContainText('Select the two practices');

  await page.locator('.option').nth(0).click();
  await page.locator('.option').nth(1).click();
  await page.getByRole('button', { name: /Next/ }).click();
  await expect(page.locator('.q-stem')).toContainText('purpose of the E2E flow');

  await page.locator('.option').nth(0).click();
  await page.getByRole('button', { name: 'Submit Exam' }).click();
  await expect(page.locator('#m-title')).toHaveText('Submit Exam');
  await page.getByRole('button', { name: 'Submit Now' }).click();

  await expect(page.getByRole('heading', { name: 'Exam Submitted' })).toBeVisible();
  await expect(page.locator('#app')).toContainText('PASS');
  await expect(page.locator('#app')).toContainText('100%');
});

test('page refresh always recovers to a usable candidate entry screen', async ({ page }) => {
  await page.goto('/');
  await page.reload();
  await expect(page.locator('#code-inp')).toBeVisible();
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