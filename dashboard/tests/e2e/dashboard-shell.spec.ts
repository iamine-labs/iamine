import { expect, test } from '@playwright/test';
import { resolve } from 'node:path';

test('runs the dashboard shell without browser or layout failures', async ({
  page,
}, testInfo) => {
  const consoleErrors: string[] = [];
  const failedRequests: string[] = [];

  page.on('console', (message) => {
    if (message.type() === 'error') consoleErrors.push(message.text());
  });
  page.on('requestfailed', (request) => {
    const reason = request.failure()?.errorText ?? 'unknown';
    failedRequests.push(`${request.method()} ${request.url()} (${reason})`);
  });

  const wallpaperLoaded = page.waitForResponse(
    (response) =>
      response.url().endsWith('/assets/iamine-network-wallpaper.png') &&
      response.ok(),
  );
  await page.goto('/#/overview');
  await wallpaperLoaded;
  await page.waitForLoadState('networkidle');

  await expect(page.getByText('Preview data')).toBeVisible();
  await expect(
    page.getByRole('heading', { name: 'System operational' }),
  ).toBeVisible();
  await expect(page.getByText('NODE-LOCAL-01').first()).toBeVisible();

  const viewportWidth = page.viewportSize()?.width ?? 0;
  const tabKey = testInfo.project.name.startsWith('webkit') ? 'Alt+Tab' : 'Tab';

  await page.keyboard.press(tabKey);
  await expect(
    page.getByRole('link', { name: 'Skip to dashboard content' }),
  ).toBeFocused();
  await page.keyboard.press(tabKey);

  if (viewportWidth <= 760) {
    await expect(
      page.getByRole('button', { name: 'Open navigation' }),
    ).toBeFocused();
    await page.getByRole('button', { name: 'Open navigation' }).click();
    await page.getByRole('link', { name: 'Open Agents from sidebar' }).click();
  } else {
    await expect(
      page.getByRole('link', { name: 'Open Overview from sidebar' }),
    ).toBeFocused();
    await page.getByRole('link', { name: 'Agents', exact: true }).click();
  }

  await expect(
    page.getByRole('heading', { name: 'Agent catalog', exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText('Preview catalog; not local node state'),
  ).toBeVisible();
  await expect(page).toHaveURL(/#\/agents$/);
  await page.reload();
  await page.waitForLoadState('networkidle');
  await expect(
    page.getByRole('heading', { name: 'Agent catalog', exact: true }),
  ).toBeVisible();

  await page.locator('#dashboard-content').focus();
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({
    path: testInfo.outputPath(`${testInfo.project.name}-agents.png`),
    fullPage: true,
  });

  await page.getByRole('button', { name: 'Review permission preview' }).click();
  await expect(page).toHaveURL(/#\/agents\/node-doctor\/permissions$/);
  await expect(
    page.getByRole('heading', { name: 'Permission review' }),
  ).toBeVisible();
  await expect(
    page.getByRole('button', { name: 'Confirm preview' }),
  ).toBeDisabled();

  await page
    .getByRole('checkbox', { name: /I reviewed this preview request/i })
    .check();
  await page.getByRole('button', { name: 'Confirm preview' }).click();
  await expect(
    page.getByText('No permission or runtime authority was created.'),
  ).toBeVisible();
  await expect(page.getByText('Preview confirmation recorded')).toBeVisible();

  await page.locator('#dashboard-content').focus();
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({
    path: testInfo.outputPath(`${testInfo.project.name}-permissions.png`),
    fullPage: true,
  });

  await page.reload();
  await page.waitForLoadState('networkidle');
  await expect(
    page.getByRole('heading', { name: 'Permission review' }),
  ).toBeVisible();
  await expect(page.getByText('Pending review')).toBeVisible();
  await expect(
    page.getByRole('button', { name: 'Confirm preview' }),
  ).toBeDisabled();

  await page.getByRole('button', { name: 'Agent catalog' }).click();
  await expect(page).toHaveURL(/#\/agents$/);

  await page
    .getByRole('searchbox', { name: 'Search agents' })
    .fill('Windows Optimizer');
  await expect(page.getByText('1 of 6')).toBeVisible();
  await expect(
    page.getByRole('heading', { name: 'Windows Optimizer Assistant' }),
  ).toBeVisible();

  if (viewportWidth <= 760) {
    await page.getByRole('button', { name: 'Open navigation' }).click();
    await page
      .getByRole('link', { name: 'Open Overview from sidebar' })
      .click();
  } else {
    await page.getByRole('link', { name: 'Overview', exact: true }).click();
  }
  await expect(page).toHaveURL(/#\/overview$/);

  await expect(
    page.getByRole('button', {
      name: 'Notifications unavailable in preview',
    }),
  ).toBeDisabled();

  const documentWidth = await page.evaluate(
    () => document.documentElement.scrollWidth,
  );
  expect(documentWidth).toBeLessThanOrEqual(viewportWidth);

  const topbar = await page.getByRole('banner').boundingBox();
  const firstPanel = await page
    .getByRole('heading', { name: 'System operational' })
    .locator('..')
    .boundingBox();
  expect(topbar).not.toBeNull();
  expect(firstPanel).not.toBeNull();
  expect(firstPanel?.y ?? 0).toBeGreaterThanOrEqual(topbar?.height ?? 0);

  expect(consoleErrors).toEqual([]);
  expect(failedRequests).toEqual([]);

  await page.addScriptTag({
    path: resolve(process.cwd(), 'node_modules/axe-core/axe.min.js'),
  });
  const accessibilityViolations = await page.evaluate(async () => {
    const axe = (
      window as unknown as {
        axe: {
          run: () => Promise<{
            violations: Array<{
              id: string;
              impact: string | null;
              nodes: Array<{
                target: string[];
                failureSummary: string | undefined;
              }>;
            }>;
          }>;
        };
      }
    ).axe;
    const result = await axe.run();
    return result.violations.map((violation) => ({
      id: violation.id,
      impact: violation.impact,
      nodes: violation.nodes.map((node) => ({
        target: node.target,
        failureSummary: node.failureSummary,
      })),
    }));
  });
  expect(accessibilityViolations).toEqual([]);

  await page.locator('#dashboard-content').focus();
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({
    path: testInfo.outputPath(`${testInfo.project.name}.png`),
    fullPage: true,
  });
});
