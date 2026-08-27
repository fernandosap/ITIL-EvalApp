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

test('multi-select enforces the exact selection cap and shows required count', async ({ page }) => {
  await startExam(page);
  await page.locator('.option').nth(0).click();
  await page.getByRole('button', { name: /Next/ }).click();
  await expect(page.locator('.q-stem')).toContainText('Select the two practices');
  await expect(page.locator('[data-selection-requirement]')).toContainText('Select exactly 2');

  await page.locator('.option').nth(0).click();
  await page.locator('.option').nth(1).click();
  await expect(page.locator('.sel-count')).toContainText('2/2 selected');
  await page.locator('.option').nth(2).click();
  await expect(page.locator('#m-title')).toHaveText('Selection limit reached');
  await expect(page.locator('.option.selected')).toHaveCount(2);
});

test('exam question and option text meet practical AA contrast threshold', async ({ page }) => {
  await startExam(page);
  const ratios = await page.evaluate(() => {
    function rgbToLum(rgb) {
      const m = rgb.match(/\d+/g).slice(0, 3).map(Number).map((v) => v / 255)
        .map((v) => v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4));
      return 0.2126 * m[0] + 0.7152 * m[1] + 0.0722 * m[2];
    }
    function ratio(el) {
      const cs = getComputedStyle(el);
      const fg = rgbToLum(cs.color);
      const bg = rgbToLum(cs.backgroundColor === 'rgba(0, 0, 0, 0)' ? 'rgb(255,255,255)' : cs.backgroundColor);
      return (Math.max(fg, bg) + 0.05) / (Math.min(fg, bg) + 0.05);
    }
    return { stem: ratio(document.querySelector('.q-stem')), option: ratio(document.querySelector('.option')) };
  });
  expect(ratios.stem).toBeGreaterThanOrEqual(4.5);
  expect(ratios.option).toBeGreaterThanOrEqual(4.5);
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
