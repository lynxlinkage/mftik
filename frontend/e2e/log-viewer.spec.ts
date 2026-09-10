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

type Mocks = { handshakes: number; pings: number };

type LogPageOptions = {
	/** Passed straight to Playwright's close; `{}` sends no status code. */
	close?: { code?: number; reason?: string };
	/** False makes `/auth/me` answer 401, which is the only place a refused
	 *  handshake is visible. */
	signedIn?: boolean;
};

async function mockAuth(page: Page, mocks: Mocks, signedIn: boolean) {
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
	await page.route('**/api/auth/me', (route) => {
		mocks.pings += 1;
		if (!signedIn) {
			return route.fulfill({ status: 401, json: { detail: 'authentication required' } });
		}
		return route.fulfill({
			json: {
				user_id: 1,
				username: 'owner',
				display_name: 'owner',
				email: null,
				via: 'none'
			}
		});
	});
}

async function mockLogPage(
	page: Page,
	logs: LogRow[],
	options: LogPageOptions = {}
): Promise<Mocks> {
	const { close = { code: 1008, reason: 'authentication required' }, signedIn = true } = options;
	const mocks: Mocks = { handshakes: 0, pings: 0 };
	await mockAuth(page, mocks, signedIn);
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
		// First paint and every reconnect: stay closed so the pane cannot hide
		// behind a later open. Only the log socket is counted — the page opens
		// a status socket too, and it would otherwise pass a count on its own.
		if (new URL(ws.url()).pathname.startsWith('/ws/sts/')) mocks.handshakes += 1;
		ws.close(close);
	});
	return mocks;
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

test('a refused handshake is not retried in a loop', async ({ page }) => {
	const socket = await mockLogPage(page, []);
	await page.goto('/sts/sess-refused');

	const term = page.getByLabel('STS log terminal');
	await expect(term).toContainText('authentication required');
	// Well past the first two backoff steps (1s, 2s). Retrying a login the gate
	// has refused cannot succeed, and the loop would hammer /ws and /auth/me.
	await page.waitForTimeout(3_500);
	expect(socket.handshakes).toBe(1);
	await expect(term).not.toContainText('Reconnecting…');
});

test('a close with no status keeps reconnecting while the session is alive', async ({ page }) => {
	const socket = await mockLogPage(page, [], { close: {} });
	await page.goto('/sts/sess-flap');

	// The shape the gate actually produces: it closes before accepting, uvicorn
	// answers the handshake with 403, and the browser reports a close carrying
	// no status at all. Indistinguishable from the API restarting — which is
	// the case backoff exists for, so it has to keep trying.
	await expect.poll(() => socket.handshakes, { timeout: 8_000 }).toBeGreaterThan(1);
});

test('a 401 on the close path routes to /login instead of reopening', async ({ page }) => {
	const socket = await mockLogPage(page, [], { close: {}, signedIn: false });
	await page.goto('/sts/sess-401');

	await expect(page).toHaveURL(/\/login\?next=/);
	await page.waitForTimeout(3_500);
	expect(socket.handshakes).toBe(1);
});

/**
 * What the three above do and do not prove, each checked against a tree
 * regressed on purpose.
 *
 * Leaving for /login is what ends the retry here: it unmounts the pane, and
 * the disposer clears the pending timer. So the test above stays green even
 * when `shouldReopen` ignores the verdict and answers true — it pins that the
 * close path still asks `/auth/me` and acts on a 401, and it fails when that
 * ask is dropped. The verdict check itself is covered by the 1008 test, and
 * only guards the window before the navigation lands.
 */
