import { expect, test } from '@playwright/test';
import { resolve } from 'node:path';

test('renders the official dashboard preview without browser or layout failures', async ({
  page,
}, testInfo) => {
  const consoleErrors: string[] = [];
  const failedRequests: string[] = [];

  page.on('console', (message) => {
    if (message.type() === 'error') consoleErrors.push(message.text());
  });
  page.on('requestfailed', (request) => {
    failedRequests.push(`${request.method()} ${request.url()}`);
  });

  await page.goto('/');

  await expect(page.getByText('Preview data')).toBeVisible();
  await expect(
    page.getByRole('heading', { name: 'System operational' }),
  ).toBeVisible();
  await expect(page.getByText('NODE-LOCAL-01').first()).toBeVisible();

  const viewportWidth = page.viewportSize()?.width ?? 0;
  if (viewportWidth <= 760) {
    await page.keyboard.press(
      testInfo.project.name.startsWith('webkit') ? 'Alt+Tab' : 'Tab',
    );
    await expect(
      page.getByRole('button', { name: 'Open navigation' }),
    ).toBeFocused();
    await page.getByRole('button', { name: 'Open navigation' }).click();
    await page
      .getByRole('button', { name: 'Open Agents from sidebar' })
      .click();
  } else {
    await page.keyboard.press('Tab');
    await expect(
      page.getByRole('button', { name: 'Open Overview from sidebar' }),
    ).toBeFocused();
    await page.getByRole('button', { name: 'Agents', exact: true }).click();
  }

  await expect(
    page.getByRole('heading', { name: 'Agents', exact: true }),
  ).toBeVisible();
  await page.getByRole('button', { name: 'Return to Overview' }).click();

  const documentWidth = await page.evaluate(
    () => document.documentElement.scrollWidth,
  );
  expect(documentWidth).toBeLessThanOrEqual(viewportWidth);

  const topbar = await page.locator('header').boundingBox();
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

  await page.screenshot({
    path: testInfo.outputPath(`${testInfo.project.name}.png`),
    fullPage: true,
  });
});
