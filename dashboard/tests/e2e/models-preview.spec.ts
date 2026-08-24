import { expect, test } from '@playwright/test';
import { resolve } from 'node:path';

test('runs the models preview without authority or layout failures', async ({
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

  await page.goto('/#/models');

  await expect(
    page.getByRole('heading', { name: 'Models', exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText('Preview fixture; no model registry was read'),
  ).toBeVisible();
  await expect(page.getByText('5 shown')).toBeVisible();
  await expect(page).toHaveURL(/#\/models$/);

  await page.reload();
  await expect(
    page.getByRole('heading', { name: 'Models', exact: true }),
  ).toBeVisible();

  await page
    .getByRole('searchbox', { name: 'Search models' })
    .fill('Preview Model B');
  await expect(page.getByText('1 of 5')).toBeVisible();
  await expect(
    page.getByRole('heading', { name: 'Preview Model B' }),
  ).toBeVisible();

  await page.getByRole('button', { name: 'Clear models filters' }).click();
  await page.getByRole('button', { name: 'Review', exact: true }).click();
  await expect(page.getByText('1 of 5')).toBeVisible();
  await expect(
    page.getByRole('heading', { name: 'Preview Model C' }),
  ).toBeVisible();

  await page.getByRole('button', { name: 'Clear models filters' }).click();
  await page
    .getByRole('combobox', { name: 'Filter models by category label' })
    .selectOption('general');
  await expect(page.getByText('2 of 5')).toBeVisible();
  await page.getByRole('button', { name: 'Select Preview Model E' }).click();
  await expect(
    page.getByRole('heading', { name: 'Preview Model E' }),
  ).toBeVisible();

  for (const action of [
    /download/i,
    /install/i,
    /activate/i,
    /select model/i,
    /run model/i,
    /remove/i,
  ]) {
    await expect(page.getByRole('button', { name: action })).toHaveCount(0);
  }

  const viewportWidth = page.viewportSize()?.width ?? 0;
  const documentWidth = await page.evaluate(
    () => document.documentElement.scrollWidth,
  );
  expect(documentWidth).toBeLessThanOrEqual(viewportWidth);

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
    path: testInfo.outputPath(`${testInfo.project.name}-models.png`),
    fullPage: true,
  });
});
