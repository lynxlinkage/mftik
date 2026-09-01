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
	opts: {
		live?: StrategyRow[];
		/** Snapshot taken when the request arrives, so a later push is not in it. */
		liveAt?: () => StrategyRow[];
		holdLiveAfter?: number;
		failHistory?: boolean;
		attentionPage?: { first: StrategyRow[]; rest: StrategyRow[]; total: number };
	} = {}
): Promise<{
	urls: URL[];
	send: (data: string) => void;
	releaseLive: () => void;
}> {
	const urls: URL[] = [];
	let statusWs: WebSocketRoute | null = null;
	const liveRows = opts.live ?? LIVE;
	let releaseLive: (() => void) | null = null;
	const held = new Promise<void>((resolve) => {
		releaseLive = resolve;
	});

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
		const offset = Number(url.searchParams.get('offset') ?? '0');
		if (status === 'live') {
			// Copy at request time so a later push is not smuggled into this page.
			const live = [...(opts.liveAt ? opts.liveAt() : liveRows)];
			const liveIndex = urls.filter((u) => u.searchParams.get('status') === 'live')
				.length;
			if (opts.holdLiveAfter !== undefined && liveIndex > opts.holdLiveAfter) {
				await held;
			}
			await route.fulfill({
				json: { strategies: live, total: live.length, has_more: false }
			});
			return;
		}
		if (status === 'failed,interrupted') {
			if (opts.attentionPage) {
				const rows =
					offset > 0 ? opts.attentionPage.rest : opts.attentionPage.first;
				await route.fulfill({
					json: {
						strategies: rows,
						total: opts.attentionPage.total,
						has_more: offset + rows.length < opts.attentionPage.total
					}
				});
				return;
			}
			await route.fulfill({
				json: { strategies: ATTENTION, total: ATTENTION.length, has_more: false }
			});
			return;
		}
		if (status === 'done,ack' && opts.failHistory) {
			await route.fulfill({ status: 502, json: { detail: 'STS list failed' } });
			return;
		}
		if (status === 'done,ack') {
			const rows = offset > 0 ? [HISTORY[2]] : HISTORY.slice(0, 2);
			await route.fulfill({
				json: { strategies: rows, total: 51, has_more: offset === 0 }
			});
			return;
		}
		await route.fulfill({ json: { strategies: [], total: 0, has_more: false } });
	});
	await page.routeWebSocket('**/ws/status/sts', (ws) => {
		statusWs = ws;
	});

	await page.goto('/sts');
	// Layout withholds chrome until /auth/status returns; the socket is
	// opened from the page's onMount, so wait for the page before the poll.
	await expect(page.getByRole('button', { name: 'Live' })).toBeVisible();
	await expect.poll(() => statusWs).not.toBeNull();

	return {
		urls,
		send: (data: string) => {
			if (statusWs === null) throw new Error('status socket is not open');
			statusWs.send(data);
		},
		releaseLive: () => releaseLive?.()
	};
}

test('the default tab is Live', async ({ page }) => {
	const { urls } = await mockStsPage(page);

	await expect(page.getByRole('link', { name: 's-live' })).toBeVisible();
	await expect(page.getByRole('button', { name: 'Pause' })).toHaveCount(0);
	await expect(page.getByRole('button', { name: 'Stop' })).toBeVisible();
	await expect(page.getByRole('button', { name: 'Ack' })).toHaveCount(0);
	expect(urls[0]?.searchParams.get('status')).toBe('live');
	expect(urls[0]?.searchParams.has('offset')).toBe(false);
});

test('Attention and History list the rows that belong there', async ({ page }) => {
	await mockStsPage(page);

	await page.getByRole('tablist').getByRole('button', { name: 'Attention' }).click();
	await expect(page.getByRole('link', { name: 's-fail' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-int' })).toBeVisible();
	await expect(page.getByRole('button', { name: 'Ack' })).toHaveCount(2);

	await page.getByRole('tablist').getByRole('button', { name: 'History' }).click();
	await expect(page.getByRole('link', { name: 's-done-new' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-done-mid' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-ack' })).toHaveCount(0);
	await expect(page.getByRole('button', { name: 'Ack' })).toHaveCount(0);
});

test('History page 2 replaces page one and sends offset', async ({ page }) => {
	const { urls } = await mockStsPage(page);

	await page.getByRole('tablist').getByRole('button', { name: 'History' }).click();
	await expect(page.getByRole('link', { name: 's-done-new' })).toBeVisible();
	await page.getByRole('button', { name: 'Page 2' }).click();

	await expect(page.getByRole('link', { name: 's-ack' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-done-new' })).toHaveCount(0);

	const more = urls.find((u) => u.searchParams.get('offset') === '50');
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
	await page.getByRole('button', { name: 'Page 2' }).click();
	await expect(page.getByRole('link', { name: 's-ack' })).toBeVisible();
	const afterLoad = urls.length;

	send(statusEvent('s-other', 'live', 5));
	await page.waitForTimeout(200);

	expect(urls.length).toBe(afterLoad);
	await expect(page.getByRole('link', { name: 's-ack' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-done-new' })).toHaveCount(0);
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
	expect(last?.searchParams.has('offset')).toBe(false);
});

test('two unseen live sessions share an in-flight fetch plus at most one trailing reload', async ({
	page
}) => {
	const { urls, send } = await mockStsPage(page);
	await expect(page.getByRole('link', { name: 's-live' })).toBeVisible();
	const afterLoad = urls.length;

	send(statusEvent('s-rebuilt-a', 'live', 10));
	send(statusEvent('s-rebuilt-b', 'live', 11));

	await expect.poll(() => urls.length).toBeGreaterThan(afterLoad);
	await page.waitForTimeout(200);
	// One extra if both land before the request leaves; two if the second
	// queues a trailing reload. A request per event would be three or more.
	expect(urls.length).toBeLessThanOrEqual(afterLoad + 2);
});

test('an unseen session that misses the in-flight fetch still appears', async ({
	page
}) => {
	const liveBox = [row('s-live', 'live', 30)];
	const { urls, send, releaseLive } = await mockStsPage(page, {
		liveAt: () => liveBox,
		holdLiveAfter: 1
	});
	await expect(page.getByRole('link', { name: 's-live' })).toBeVisible();

	liveBox.push(row('s-rebuilt-a', 'live', 40));
	send(statusEvent('s-rebuilt-a', 'live', 10));
	await expect
		.poll(() => urls.filter((u) => u.searchParams.get('status') === 'live').length)
		.toBe(2);

	liveBox.push(row('s-rebuilt-b', 'live', 41));
	send(statusEvent('s-rebuilt-b', 'live', 11));
	releaseLive();

	await expect(page.getByRole('link', { name: 's-rebuilt-a' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-rebuilt-b' })).toBeVisible();
	await expect
		.poll(() => urls.filter((u) => u.searchParams.get('status') === 'live').length)
		.toBe(3);
});

test('acking a full Attention page still offers the next page', async ({ page }) => {
	const { send } = await mockStsPage(page, {
		attentionPage: {
			first: [row('s-fail', 'failed', 20)],
			rest: [row('s-int', 'interrupted', 10)],
			total: 51
		}
	});

	await page.getByRole('tablist').getByRole('button', { name: 'Attention' }).click();
	await expect(page.getByRole('link', { name: 's-fail' })).toBeVisible();

	send(statusEvent('s-fail', 'ack', 2));
	await expect(page.getByRole('link', { name: 's-fail' })).toHaveCount(0);
	await expect(page.getByRole('button', { name: 'Page 2' })).toBeVisible();
	// The tab is not empty, only this page of it. Claiming otherwise over
	// a pager that still has another page is the failure.
	await expect(page.getByText('Nothing needs attention.')).toHaveCount(0);
	await expect(page.getByText('Nothing on this page.')).toBeVisible();

	await page.getByRole('button', { name: 'Page 2' }).click();
	await expect(page.getByRole('link', { name: 's-int' })).toBeVisible();
});

test('a failed tab switch does not keep the previous tab\'s rows', async ({
	page
}) => {
	await mockStsPage(page, { failHistory: true });
	await expect(page.getByRole('link', { name: 's-live' })).toBeVisible();

	await page.getByRole('tablist').getByRole('button', { name: 'History' }).click();
	await expect(page.getByRole('link', { name: 's-live' })).toHaveCount(0);
	await expect(page.getByRole('button', { name: 'Stop' })).toHaveCount(0);
	await expect(page.locator('.error-banner')).toBeVisible();
});
