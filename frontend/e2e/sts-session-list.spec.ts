import { expect, test, type Page, type WebSocketRoute } from '@playwright/test';

/**
 * STS session list — tabs, cursor, and the socket that must not reset one.
 *
 * Every /api and /ws call is intercepted. These tests do not start STS and
 * do not share an operator database.
 */

type StrategyRow = {
	type: string | null;
	config: Record<string, unknown>;
	created_by: number;
	created_at: number;
	session_id: string;
	status: string;
	paused: boolean | null;
	reason: string | null;
};

function row(session_id: string, status: string, created_at = 1): StrategyRow {
	return {
		type: 'NoopStrategy',
		config: {},
		created_by: 1,
		created_at,
		session_id,
		status,
		paused: false,
		reason: status === 'failed' || status === 'interrupted' ? 'boom' : null
	};
}

function statusEvent(session_id: string, status: string, ts: number): string {
	return JSON.stringify({
		type: 'sts.session.status',
		ts,
		payload: {
			session_id,
			status,
			paused: false,
			strategy: 'NoopStrategy',
			reason: null
		}
	});
}

const LIVE = [row('s-live', 'live', 30)];
const ATTENTION = [
	row('s-fail', 'failed', 20),
	row('s-int', 'interrupted', 10)
];
const HISTORY = [
	row('s-done-new', 'done', 3),
	row('s-done-mid', 'done', 2),
	row('s-ack', 'ack', 1)
];

async function mockStsPage(
	page: Page,
	opts: { live?: StrategyRow[] } = {}
): Promise<{ urls: URL[]; send: (data: string) => void }> {
	const urls: URL[] = [];
	let statusWs: WebSocketRoute | null = null;
	const liveRows = opts.live ?? LIVE;

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
	await page.route('**/api/sts/types', (route) =>
		route.fulfill({
			json: {
				types: ['NoopStrategy'],
				templates: [
					{
						type: 'NoopStrategy',
						label: 'Noop',
						description: 'noop',
						yaml: 'sts: {}\n',
						source: 'bundled'
					}
				],
				default: 'NoopStrategy'
			}
		})
	);
	await page.route('**/api/sts/strategies**', async (route) => {
		const url = new URL(route.request().url());
		urls.push(url);
		const status = url.searchParams.get('status') ?? '';
		const before = url.searchParams.get('before');
		if (status === 'live') {
			await route.fulfill({ json: { strategies: liveRows, has_more: false } });
			return;
		}
		if (status === 'failed,interrupted') {
			await route.fulfill({ json: { strategies: ATTENTION, has_more: false } });
			return;
		}
		if (status === 'done,ack') {
			if (before === 's-done-mid') {
				await route.fulfill({
					json: { strategies: [HISTORY[2]], has_more: false }
				});
				return;
			}
			await route.fulfill({
				json: { strategies: HISTORY.slice(0, 2), has_more: true }
			});
			return;
		}
		await route.fulfill({ json: { strategies: [], has_more: false } });
	});
	await page.routeWebSocket('**/ws/status/sts', (ws) => {
		statusWs = ws;
	});

	await page.goto('/sts');
	await expect.poll(() => statusWs).not.toBeNull();

	return {
		urls,
		send: (data: string) => {
			if (statusWs === null) throw new Error('status socket is not open');
			statusWs.send(data);
		}
	};
}

test('the default tab is Live', async ({ page }) => {
	const { urls } = await mockStsPage(page);

	await expect(page.getByRole('link', { name: 's-live' })).toBeVisible();
	await expect(page.getByRole('button', { name: 'Pause' })).toBeVisible();
	await expect(page.getByRole('button', { name: 'Stop' })).toBeVisible();
	await expect(page.getByRole('button', { name: 'Ack' })).toHaveCount(0);
	expect(urls[0]?.searchParams.get('status')).toBe('live');
	expect(urls[0]?.searchParams.has('before')).toBe(false);
});

test('Attention and History list the rows that belong there', async ({ page }) => {
	await mockStsPage(page);

	await page.getByRole('tablist').getByRole('button', { name: 'Attention' }).click();
	await expect(page.getByRole('link', { name: 's-fail' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-int' })).toBeVisible();
	await expect(page.getByRole('button', { name: 'Ack' })).toHaveCount(2);
	await expect(page.getByRole('button', { name: 'Pause' })).toHaveCount(0);

	await page.getByRole('tablist').getByRole('button', { name: 'History' }).click();
	await expect(page.getByRole('link', { name: 's-done-new' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-done-mid' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-ack' })).toHaveCount(0);
	await expect(page.getByRole('button', { name: 'Ack' })).toHaveCount(0);
});

test('History Load more sends the last row as before and keeps page one', async ({
	page
}) => {
	const { urls } = await mockStsPage(page);

	await page.getByRole('tablist').getByRole('button', { name: 'History' }).click();
	await expect(page.getByRole('link', { name: 's-done-new' })).toBeVisible();
	await page.getByRole('button', { name: 'Load more' }).click();

	await expect(page.getByRole('link', { name: 's-ack' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-done-new' })).toBeVisible();
	await expect(page.getByRole('button', { name: 'Load more' })).toHaveCount(0);

	const more = urls.find((u) => u.searchParams.get('before') === 's-done-mid');
	expect(more).toBeTruthy();
	expect(more?.searchParams.get('status')).toBe('done,ack');
});

test('a status that leaves the tab removes the row', async ({ page }) => {
	const { send } = await mockStsPage(page);
	await expect(page.getByRole('link', { name: 's-live' })).toBeVisible();

	send(statusEvent('s-live', 'done', 2));

	await expect(page.getByRole('link', { name: 's-live' })).toHaveCount(0);
});

test('a live event does not reload History', async ({ page }) => {
	const { urls, send } = await mockStsPage(page);

	await page.getByRole('tablist').getByRole('button', { name: 'History' }).click();
	await page.getByRole('button', { name: 'Load more' }).click();
	await expect(page.getByRole('link', { name: 's-ack' })).toBeVisible();
	const afterLoad = urls.length;

	send(statusEvent('s-other', 'live', 5));
	await page.waitForTimeout(200);

	expect(urls.length).toBe(afterLoad);
	await expect(page.getByRole('link', { name: 's-ack' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-done-new' })).toBeVisible();
});

test('Refresh and a tab switch replace the list without a cursor', async ({
	page
}) => {
	const { urls } = await mockStsPage(page);

	await page.getByRole('tablist').getByRole('button', { name: 'History' }).click();
	await page.getByRole('button', { name: 'Refresh' }).click();
	await expect(page.getByRole('link', { name: 's-done-new' })).toBeVisible();

	const last = urls.at(-1);
	expect(last?.searchParams.get('status')).toBe('done,ack');
	expect(last?.searchParams.has('before')).toBe(false);
});

test('two unseen live sessions cause one reload', async ({ page }) => {
	const { urls, send } = await mockStsPage(page);
	await expect(page.getByRole('link', { name: 's-live' })).toBeVisible();
	const afterLoad = urls.length;

	send(statusEvent('s-rebuilt-a', 'live', 10));
	send(statusEvent('s-rebuilt-b', 'live', 11));

	await expect.poll(() => urls.length).toBe(afterLoad + 1);
	await page.waitForTimeout(150);
	expect(urls.length).toBe(afterLoad + 1);
});
