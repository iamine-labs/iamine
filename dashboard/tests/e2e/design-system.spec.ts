import { expect, test } from '@playwright/test';
import { resolve } from 'node:path';

test('renders the design system without browser or layout failures', async ({
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

  await expect(page.getByLabel('IAMINE')).toBeVisible();
  await expect(page.getByText('Preview data')).toBeVisible();
  await expect(
    page.getByRole('heading', { name: 'Design system review' }),
  ).toBeVisible();

  await page.keyboard.press(
    testInfo.project.name.startsWith('webkit') ? 'Alt+Tab' : 'Tab',
  );
  await expect(
    page.getByRole('button', { name: 'Preview notifications' }),
  ).toBeFocused();

  await page.getByRole('button', { name: 'Compact' }).click();
  await expect(page.getByRole('button', { name: 'Compact' })).toHaveAttribute(
    'aria-pressed',
    'true',
  );

  const viewportWidth = page.viewportSize()?.width ?? 0;
  const documentWidth = await page.evaluate(
    () => document.documentElement.scrollWidth,
  );
  expect(documentWidth).toBeLessThanOrEqual(viewportWidth);

  const topbar = await page.locator('header').boundingBox();
  const main = await page.locator('main').boundingBox();
  expect(topbar).not.toBeNull();
  expect(main).not.toBeNull();
  expect((main?.y ?? 0) + 1).toBeGreaterThanOrEqual(
    (topbar?.y ?? 0) + (topbar?.height ?? 0),
  );

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
