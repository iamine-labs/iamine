import { expect, test, type Page } from '@playwright/test';
import { resolve } from 'node:path';

interface AccessibilityViolation {
  id: string;
  impact: string | null;
  nodes: Array<{
    target: string[];
    failureSummary: string | undefined;
  }>;
}

async function accessibilityViolations(
  page: Page,
): Promise<AccessibilityViolation[]> {
  await page.addScriptTag({
    path: resolve(process.cwd(), 'node_modules/axe-core/axe.min.js'),
  });

  return page.evaluate(async () => {
    const axe = (
      window as unknown as {
        axe: {
          run: () => Promise<{ violations: AccessibilityViolation[] }>;
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
}

async function expectNoHorizontalOverflow(page: Page): Promise<void> {
  const viewportWidth = page.viewportSize()?.width ?? 0;
  const documentWidth = await page.evaluate(
    () => document.documentElement.scrollWidth,
  );

  expect(documentWidth).toBeLessThanOrEqual(viewportWidth);
}

test('keeps reserved and unknown routes inside the preview boundary', async ({
  page,
}) => {
  const consoleErrors: string[] = [];
  const failedRequests: string[] = [];

  page.on('console', (message) => {
    if (message.type() === 'error') consoleErrors.push(message.text());
  });
  page.on('requestfailed', (request) => {
    const reason = request.failure()?.errorText ?? 'unknown';
    failedRequests.push(`${request.method()} ${request.url()} (${reason})`);
  });

  await page.goto('/#/marketplace');
  await expect(
    page.getByRole('heading', { name: 'Marketplace', exact: true }),
  ).toBeVisible();
  await expect(page.getByText('Preview boundary')).toBeVisible();
  await expect(
    page.getByText(/No node request, mutation, or fictitious endpoint/i),
  ).toBeVisible();
  await expect(
    page.getByRole('button', { name: /install|connect|run/i }),
  ).toHaveCount(0);
  await expectNoHorizontalOverflow(page);
  expect(await accessibilityViolations(page)).toEqual([]);

  await page.getByRole('button', { name: 'Return to Overview' }).click();
  await expect(page).toHaveURL(/#\/overview$/);
  await expect(
    page.getByRole('heading', { name: 'System operational' }),
  ).toBeVisible();

  await page.goto('/#/outside-approved-shell');
  await expect(
    page.getByRole('heading', { name: 'Page not found' }),
  ).toBeVisible();
  await expect(page.getByText('Unknown route', { exact: true })).toBeVisible();
  await expect(page.getByText(/not part of the approved shell/i)).toBeVisible();
  await expectNoHorizontalOverflow(page);
  expect(await accessibilityViolations(page)).toEqual([]);

  await page.getByRole('button', { name: 'Return to Overview' }).click();
  await expect(page).toHaveURL(/#\/overview$/);
  expect(consoleErrors).toEqual([]);
  expect(failedRequests).toEqual([]);
});
