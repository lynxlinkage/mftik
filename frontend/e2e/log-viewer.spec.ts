import { expect, test, type Page } from '@playwright/test';

/**
 * Session log pane — REST seed when the live socket dies on first paint.
 * Every /api and /ws call is intercepted.
 */

type LogRow = {
	id: string;
	db_id: number;
	ts: number;
	source: string;
	level: string;
	message: string;
};

async function mockAuth(page: Page) {
	await page.route('**/api/auth/status', (route) =>
		route.fulfill({
			json: {
				enabled: false,
				setup_required: false,
				providers: [],
				authenticated: false,
				username: null,
				min_password_length: 8
			}
		})
	);
	await page.route('**/api/auth/me', (route) =>
		route.fulfill({
			json: {
				user_id: 1,
				username: 'owner',
				display_name: 'owner',
				email: null,
				via: 'none'
			}
		})
	);
}

async function mockLogPage(page: Page, logs: LogRow[]) {
	await mockAuth(page);
	await page.route('**/api/sts/sessions/*/eventlog/info', (route) =>
		route.fulfill({
			json: {
				session_id: 'sess-closed',
				available: false,
				enabled: false,
				parts: 0,
				total_bytes: 0,
				live: false
			}
		})
	);
	await page.route('**/api/logs/**', (route) =>
		route.fulfill({ json: { logs, has_more: false } })
	);
	await page.routeWebSocket('**/ws/**', (ws) => {
		// First paint and every reconnect: stay closed so the pane cannot
		// hide behind a later open. Close code 1008 is what the API auth
		// gate sends; the viewer must not collapse that to the waiting copy.
		ws.close({ code: 1008, reason: 'authentication required' });
	});
}

const SEEDED: LogRow = {
	id: 'env-1',
	db_id: 1,
	ts: 1_710_000_000,
	source: 'CrossArb',
	level: 'info',
	message: 'hedge filled on Binance'
};

test('seeds REST lines when the socket closes on first paint', async ({ page }) => {
	await mockLogPage(page, [SEEDED]);
	await page.goto('/sts/sess-closed');

	const term = page.getByLabel('STS log terminal');
	await expect(term).toContainText('hedge filled on Binance');
	await expect(term).not.toContainText('Waiting for log lines…');
	await expect(page.getByRole('heading', { name: 'STS log' })).toBeVisible();
});

test('closed empty pane is not the waiting placeholder', async ({ page }) => {
	await mockLogPage(page, []);
	await page.goto('/sts/sess-empty');

	const term = page.getByLabel('STS log terminal');
	await expect(term).toContainText(/Disconnected|Reconnecting|authentication required/);
	await expect(term).not.toContainText('Waiting for log lines…');
});
