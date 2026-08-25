import process from 'node:process';
import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
	testDir: 'e2e',
	fullyParallel: true,
	forbidOnly: !!process.env.CI,
	retries: process.env.CI ? 2 : 0,
	use: {
		baseURL: 'http://127.0.0.1:4173',
		trace: 'on-first-retry'
	},
	webServer: {
		command: 'npx vite dev --host 127.0.0.1 --port 4173',
		url: 'http://127.0.0.1:4173',
		reuseExistingServer: !process.env.CI,
		// Closed port: +layout.server.ts fail-opens instead of asking a local
		// API that may have the gate on and would 303 every existing spec.
		env: { API_INTERNAL_URL: 'http://127.0.0.1:9' }
	},
	projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }]
});
