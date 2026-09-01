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

async function loginAdmin(page) {
  await page.goto('/?admin=1');
  await expect(page.getByRole('heading', { name: 'Admin Access' })).toBeVisible();
  await page.locator('#pwd').fill('fixture-password');
  await page.getByRole('button', { name: 'Access Console' }).click();
  await expect(page.locator('.admin-page-title')).toHaveText('Admin Console');
}

function adminRoster(page) {
  return page.getByRole('columnheader', { name: 'Code', exact: true }).locator('xpath=ancestor::table');
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

test('exam reading surface is opaque and cannot be washed out by decorative background overlays', async ({ page }) => {
  await startExam(page);
  await expect(page.locator('[data-question-surface="opaque"]')).toBeVisible();

  const visual = await page.evaluate(() => {
    const stem = document.querySelector('.q-stem');
    const surface = document.querySelector('[data-question-surface="opaque"]');
    const examRoot = document.querySelector('[data-exam-surface-root="opaque"]') || stem?.closest('.no-select');
    const option = document.querySelector('.option');
    const optText = document.querySelector('.opt-text');
    const pseudo = examRoot ? getComputedStyle(examRoot, '::before') : null;
    let minAncestorOpacity = 1;
    let node = stem;
    while (node && node !== document.documentElement) {
      const value = Number(getComputedStyle(node).opacity || 1);
      minAncestorOpacity = Math.min(minAncestorOpacity, Number.isFinite(value) ? value : 1);
      node = node.parentElement;
    }
    return {
      hasLegacyOverlayClass: !!examRoot?.classList.contains('exam-shell'),
      pseudoContent: pseudo?.content || 'none',
      pseudoBackground: pseudo?.backgroundImage || 'none',
      surfaceBackground: surface ? getComputedStyle(surface).backgroundColor : '',
      surfaceOpacity: surface ? getComputedStyle(surface).opacity : '',
      surfaceFilter: surface ? getComputedStyle(surface).filter : '',
      stemColor: stem ? getComputedStyle(stem).color : '',
      optionBackground: option ? getComputedStyle(option).backgroundColor : '',
      optionColor: option ? getComputedStyle(option).color : '',
      optTextColor: optText ? getComputedStyle(optText).color : '',
      minAncestorOpacity
    };
  });

  expect(visual.hasLegacyOverlayClass).toBe(false);
  expect(visual.pseudoContent).toBe('none');
  expect(visual.pseudoBackground).toBe('none');
  expect(visual.surfaceBackground).toBe('rgb(255, 255, 255)');
  expect(visual.surfaceOpacity).toBe('1');
  expect(visual.surfaceFilter).toBe('none');
  expect(visual.stemColor).toBe('rgb(23, 32, 51)');
  expect(visual.optionBackground).toBe('rgb(255, 255, 255)');
  expect(visual.optionColor).toBe('rgb(36, 52, 77)');
  expect(visual.optTextColor).toBe('rgb(36, 52, 77)');
  expect(visual.minAncestorOpacity).toBe(1);
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

test('admin dashboard compacts status, limits notifications and moves audit activity below roster', async ({ page }) => {
  await loginAdmin(page);
  await expect(page.getByText('Technical details', { exact: true })).toBeVisible();
  const notificationToggle = page.locator('[data-admin-notification-toggle]');
  await expect(notificationToggle).toHaveText('Show 2 more');
  await expect(notificationToggle).toBeVisible();
  const notificationsCard = notificationToggle.locator('xpath=../..');
  const visibleBefore = await notificationsCard.locator(':scope > div:nth-child(2) > div:not([hidden])').count();
  expect(visibleBefore).toBe(3);
  await notificationToggle.click();
  const visibleAfter = await notificationsCard.locator(':scope > div:nth-child(2) > div:not([hidden])').count();
  expect(visibleAfter).toBe(5);

  const order = await page.evaluate(() => [...document.querySelector('.admin-wrap').children]
    .filter((el) => el.classList.contains('card'))
    .map((el) => (el.textContent || '').trim().slice(0, 80)));
  const auditIndex = order.findIndex((text) => text.startsWith('Recent Admin Activity'));
  const rosterIndex = order.findIndex((text) => text.includes('Sorted by seat number'));
  expect(auditIndex).toBeGreaterThan(rosterIndex);
});

test('admin roster filters by seat and preserves the historical exam set after default changes', async ({ page }) => {
  await loginAdmin(page);
  const roster = adminRoster(page);
  const historicRow = roster.locator('tbody tr').filter({ hasText: 'ADM202' });
  await expect(historicRow).toHaveCount(1);
  await expect(historicRow).toContainText('Historic Exam');
  await expect(historicRow).toContainText('Version 2');
  const examCellText = await historicRow.locator('td').nth(3).innerText();
  expect(examCellText).not.toContain('New Default Exam');

  await page.getByPlaceholder('Seat from').fill('15');
  await page.getByPlaceholder('Seat from').press('Tab');
  await page.getByPlaceholder('Seat to').fill('25');
  await page.getByPlaceholder('Seat to').press('Tab');
  await expect(roster.locator('tbody tr').filter({ hasText: 'ADM203' })).toBeVisible();
  await expect(roster.locator('tbody tr').filter({ hasText: 'ADM202' })).toBeHidden();
  await expect(roster.locator('tbody tr').filter({ hasText: 'ADM201' })).toBeHidden();
  await expect(page.locator('#admin-filter-summary')).toContainText('Showing 1 of 4 records');
});

test('completed results can be archived and restored without deletion', async ({ page }) => {
  await loginAdmin(page);
  let roster = adminRoster(page);
  let row = roster.locator('tbody tr').filter({ hasText: 'ADM203' });
  await expect(row).toBeVisible();
  await row.getByRole('button', { name: 'Archive' }).click();
  roster = adminRoster(page);
  await expect(roster.locator('tbody tr').filter({ hasText: 'ADM203' })).toBeHidden();

  const archiveFilter = page.locator('select[data-args="archive,__value__"]');
  await archiveFilter.selectOption('archived');
  await expect(roster.locator('tbody tr').filter({ hasText: 'ADM203' })).toBeVisible();
  await expect(roster.locator('tbody tr').filter({ hasText: 'ADM204' })).toBeVisible();
  await roster.locator('tbody tr').filter({ hasText: 'ADM203' }).getByRole('button', { name: 'Restore' }).click();
  roster = adminRoster(page);
  await expect(roster.locator('tbody tr').filter({ hasText: 'ADM203' })).toBeHidden();
  await page.locator('select[data-args="archive,__value__"]').selectOption('current');
  await expect(roster.locator('tbody tr').filter({ hasText: 'ADM203' })).toBeVisible();
});

test('missing required client module leaves a visible recovery path', async ({ page }) => {
  await page.route('**/client/code-entry.js', (route) => route.abort());
  await page.goto('/');
  await expect(page.locator('body')).toContainText(/failed|reload|unavailable/i);
  await expect(page.locator('body')).not.toHaveText('');
});
