import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e',
  outputDir: './test-results',
  fullyParallel: true,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 2 : 0,
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: 'http://127.0.0.1:4173',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  projects: [
    {
      name: 'chromium-1440',
      use: { browserName: 'chromium', viewport: { width: 1440, height: 900 } },
    },
    {
      name: 'firefox-1024',
      use: { browserName: 'firefox', viewport: { width: 1024, height: 768 } },
    },
    {
      name: 'webkit-390',
      use: { browserName: 'webkit', viewport: { width: 390, height: 844 } },
    },
    {
      name: 'chromium-360',
      use: { browserName: 'chromium', viewport: { width: 360, height: 800 } },
    },
  ],
  webServer: {
    command: 'npm run dev -- --port 4173',
    url: 'http://127.0.0.1:4173',
    reuseExistingServer: !process.env.CI,
  },
});
