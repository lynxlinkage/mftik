import { expect, test, type Page } from '@playwright/test';

/**
 * Board session list — Finished (history) pages on a session cursor.
 *
 * Every /api and /ws call is intercepted. These tests do not start the API.
 */

type BoardSession = {
	session_id: string;
	strategy: string | null;
	status: string;
	reason: string | null;
	created_at: number;
	finished_at: number | null;
	duration_s: number;
	running: boolean;
	fills: number;
	td_api_ids: number[];
	confirmed_through_ts: number | null;
	settled: boolean;
	tickers: string[];
};

function session(session_id: string, created_at: number): BoardSession {
	return {
		session_id,
		strategy: session_id,
		status: 'done',
		reason: null,
		created_at,
		finished_at: created_at + 60,
		duration_s: 60,
		running: false,
		fills: 0,
		td_api_ids: [],
		confirmed_through_ts: null,
		settled: true,
		tickers: []
	};
}

const FINISHED = [
	session('s-done-new', 3),
	session('s-done-mid', 2),
	session('s-ack', 1)
];

async function mockBoardPage(page: Page): Promise<{ urls: URL[] }> {
	const urls: URL[] = [];

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
	await page.route('**/api/board/sessions**', async (route) => {
		const url = new URL(route.request().url());
		urls.push(url);
		const status = url.searchParams.get('status') ?? '';
		const offset = Number(url.searchParams.get('offset') ?? '0');
		if (status === 'done,ack') {
			const rows = offset > 0 ? [FINISHED[2]] : FINISHED.slice(0, 2);
			await route.fulfill({
				json: { sessions: rows, total: 51, has_more: offset === 0 }
			});
			return;
		}
		await route.fulfill({ json: { sessions: [], total: 0, has_more: false } });
	});
	await page.route('**/api/board/fills/external**', (route) =>
		route.fulfill({ json: { fills: [], has_more: false } })
	);
	await page.routeWebSocket('**/ws/**', () => {
		/* accept and stay silent */
	});

	await page.goto('/board');
	await expect(page.getByRole('button', { name: 'Finished' })).toBeVisible();

	return { urls };
}

test('Finished page 2 replaces page one and sends offset', async ({
	page
}) => {
	const { urls } = await mockBoardPage(page);

	await page.getByRole('tablist').getByRole('button', { name: 'Finished' }).click();
	await expect(page.getByRole('link', { name: 's-done-new' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-done-mid' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-ack' })).toHaveCount(0);

	await page.getByRole('button', { name: 'Page 2' }).click();
	await expect(page.getByRole('link', { name: 's-ack' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-done-new' })).toHaveCount(0);

	const more = urls.find((u) => u.searchParams.get('offset') === '50');
	expect(more).toBeTruthy();
	expect(more?.searchParams.get('status')).toBe('done,ack');
});

test('Refresh and a tab switch replace the list without a cursor', async ({
	page
}) => {
	const { urls } = await mockBoardPage(page);

	await page.getByRole('tablist').getByRole('button', { name: 'Finished' }).click();
	await expect(page.getByRole('link', { name: 's-done-new' })).toBeVisible();
	await page.getByRole('button', { name: 'Refresh' }).click();
	await expect(page.getByRole('link', { name: 's-done-new' })).toBeVisible();

	const last = urls.at(-1);
	expect(last?.searchParams.get('status')).toBe('done,ack');
	expect(last?.searchParams.has('offset')).toBe(false);
});
