import { expect, test } from '@playwright/test';
import { resolve } from 'node:path';

test('runs the nodes preview without authority or layout failures', async ({
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

  await page.goto('/#/nodes');

  await expect(
    page.getByRole('heading', { name: 'Nodes', exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText('Preview fixture; no node discovery was performed'),
  ).toBeVisible();
  await expect(page.getByText('5 shown')).toBeVisible();
  await expect(page).toHaveURL(/#\/nodes$/);

  await page.reload();
  await expect(
    page.getByRole('heading', { name: 'Nodes', exact: true }),
  ).toBeVisible();

  await page
    .getByRole('searchbox', { name: 'Search nodes' })
    .fill('Preview Node B');
  await expect(page.getByText('1 of 5')).toBeVisible();
  await expect(
    page.getByRole('heading', { name: 'Preview Node B' }),
  ).toBeVisible();

  await page.getByRole('button', { name: 'Clear nodes filters' }).click();
  await page.getByRole('button', { name: 'Limited' }).click();
  await expect(page.getByText('1 of 5')).toBeVisible();
  await expect(
    page.getByRole('heading', { name: 'Preview Node C' }),
  ).toBeVisible();

  await page.getByRole('button', { name: 'Clear nodes filters' }).click();
  await page
    .getByRole('combobox', { name: 'Filter nodes by capability label' })
    .selectOption('acceleration');
  await expect(page.getByText('2 of 5')).toBeVisible();
  await page.getByRole('button', { name: 'Select Preview Node E' }).click();
  await expect(
    page.getByRole('heading', { name: 'Preview Node E' }),
  ).toBeVisible();

  await expect(page.getByRole('button', { name: /discover/i })).toHaveCount(0);
  await expect(page.getByRole('button', { name: /connect/i })).toHaveCount(0);
  await expect(page.getByRole('button', { name: /configure/i })).toHaveCount(0);
  await expect(page.getByRole('button', { name: /start/i })).toHaveCount(0);

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
    path: testInfo.outputPath(`${testInfo.project.name}-nodes.png`),
    fullPage: true,
  });
});
