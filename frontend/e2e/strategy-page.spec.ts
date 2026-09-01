import { expect, test, type Page } from '@playwright/test';

/**
 * Strategy page — one nav entry that lists a deploy and the TD/MD it attached.
 * /sts stays. /td and /md list pages are gone; log routes remain.
 */

type StrategyRow = {
	type: string | null;
	config: Record<string, unknown>;
	created_by: number;
	created_at: number;
	session_id: string;
	status: string;
	reason: string | null;
	td_api_ids: number[];
	md_ids: string[];
};

function row(session_id: string, status: string): StrategyRow {
	return {
		type: 'NoopStrategy',
		config: {},
		created_by: 1,
		created_at: 1,
		session_id,
		status,
		reason: null,
		td_api_ids: [3],
		md_ids: ['orderbook.Paper_Spot_BTCUSDT']
	};
}

async function mockStrategyPage(page: Page, live: StrategyRow[]) {
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
	await page.route('**/api/apis', (route) =>
		route.fulfill({
			json: {
				apis: [
					{
						id: 3,
						account_id: 1,
						name: 'alpha',
						venue: 'Paper',
						api_key: 'k',
						type: 'HMAC',
						created_at: 1,
						created_by: 1
					}
				]
			}
		})
	);
	await page.route('**/api/sts/strategies**', (route) =>
		route.fulfill({ json: { strategies: live, has_more: false } })
	);
	await page.route('**/api/sts/sessions/*/eventlog/info', (route) =>
		route.fulfill({
			json: {
				session_id: 's-live',
				available: false,
				enabled: false,
				parts: 0,
				total_bytes: 0,
				live: false
			}
		})
	);
	await page.route(/\/api\/sts\/sessions\/[^/]+$/, (route) => {
		const id = new URL(route.request().url()).pathname.split('/').pop() ?? '';
		const found = live.find((s) => s.session_id === id) ?? live[0];
		return route.fulfill({ json: found });
	});
	await page.route('**/api/logs/**', (route) =>
		route.fulfill({ json: { logs: [], has_more: false } })
	);
	await page.routeWebSocket('**/ws/**', () => {
		/* accept and stay silent — same as the STS list tests */
	});

	await page.goto('/strategy');
}

test('nav offers Strategy instead of STS / TD / MD', async ({ page }) => {
	await mockStrategyPage(page, [row('s-live', 'live')]);

	const nav = page.getByRole('navigation');
	await expect(nav.getByRole('link', { name: 'Strategy' })).toBeVisible();
	await expect(nav.getByRole('link', { name: 'STS' })).toHaveCount(0);
	await expect(nav.getByRole('link', { name: 'TD' })).toHaveCount(0);
	await expect(nav.getByRole('link', { name: 'MD' })).toHaveCount(0);
});

test('the list shows the deploy and the TD / MD it attached', async ({ page }) => {
	await mockStrategyPage(page, [row('s-live', 'live')]);

	await expect(page.getByRole('heading', { name: 'Strategy' })).toBeVisible();
	await expect(page.getByRole('columnheader', { name: 'TD' })).toBeVisible();
	await expect(page.getByRole('columnheader', { name: 'MD' })).toBeVisible();
	await expect(page.getByRole('link', { name: 'Paper/alpha' })).toBeVisible();
	await expect(page.getByRole('link', { name: 'Paper' }).first()).toBeVisible();
	await expect(page.getByRole('link', { name: 'Open' })).toHaveAttribute(
		'href',
		'/strategy/s-live'
	);
});

test('opening a run shows STS plus the attached accounts and venues', async ({
	page
}) => {
	await mockStrategyPage(page, [row('s-live', 'live')]);
	await page.getByRole('link', { name: 'Open' }).click();

	await expect(page.getByRole('heading', { name: 'NoopStrategy' })).toBeVisible();
	await expect(page.getByRole('heading', { name: 'Trading accounts' })).toBeVisible();
	await expect(page.getByRole('heading', { name: 'Market data' })).toBeVisible();
	await expect(page.getByRole('link', { name: 'Paper/alpha' })).toHaveAttribute(
		'href',
		'/td/3'
	);
	await expect(page.getByRole('link', { name: 'Paper', exact: true })).toHaveAttribute(
		'href',
		'/md/Paper'
	);
	await expect(page.getByRole('heading', { name: 'STS log' })).toBeVisible();
});

test('the editor lists this node\'s account names under td:', async ({ page }) => {
	await mockStrategyPage(page, [row('s-live', 'live')]);
	await page.route('**/api/sts/types', (route) =>
		route.fulfill({
			json: {
				types: ['NoopStrategy'],
				templates: [
					{
						type: 'NoopStrategy',
						label: 'Noop',
						description: 'noop',
						yaml: 'td:\n  paper trader:\nmd:\n  - orderbook.Paper_Spot_BTCUSDT\nsts: {}\n',
						source: 'bundled'
					}
				],
				default: 'NoopStrategy'
			}
		})
	);
	await page.route('**/api/apis', (route) =>
		route.fulfill({
			json: {
				apis: [
					{
						id: 3,
						account_id: 1,
						name: 'alpha',
						venue: 'Paper',
						api_key: 'k',
						type: 'HMAC',
						created_at: 1,
						created_by: 1
					},
					{
						id: 4,
						account_id: 2,
						name: 'gate hedger',
						venue: 'Gate',
						api_key: 'g',
						type: 'HMAC',
						created_at: 1,
						created_by: 1
					}
				]
			}
		})
	);
	await page.goto('/strategy');

	const editor = page.getByRole('textbox', { name: 'strategy.yml editor' });
	await expect(editor).toBeVisible();
	await expect(page.getByRole('button', { name: 'Paper/alpha' })).toBeVisible();
	await expect(page.getByRole('button', { name: 'Gate/gate hedger' })).toBeVisible();

	await editor.click();
	await editor.evaluate((el: HTMLTextAreaElement) => {
		const at = el.value.indexOf('paper trader');
		el.focus();
		el.setSelectionRange(at, at);
	});
	await editor.press('Control+Space');

	const list = page.getByRole('listbox');
	await expect(list).toBeVisible();
	await expect(list.getByRole('option', { name: /alpha/ })).toBeVisible();
	await expect(list.getByRole('option', { name: /gate hedger/ })).toBeVisible();

	await list.getByRole('option', { name: /alpha/ }).click();
	await expect(editor).toHaveValue(/td:\n  alpha:/);
});

test('clicking an account chip inserts it under td:', async ({ page }) => {
	await mockStrategyPage(page, [row('s-live', 'live')]);

	const editor = page.getByRole('textbox', { name: 'strategy.yml editor' });
	await expect(editor).toHaveValue('sts: {}\n');

	await page.getByRole('button', { name: 'Paper/alpha' }).click();
	await expect(editor).toHaveValue(/^td:\n  alpha:\n/);
});
