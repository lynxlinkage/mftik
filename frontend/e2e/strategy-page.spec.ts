import { expect, test, type Page } from '@playwright/test';
import { captureDeploy } from './capture-deploy';

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

async function mockStrategyPage(
	page: Page,
	live: StrategyRow[],
	opts: {
		history?: { first: StrategyRow[]; rest: StrategyRow[]; cursor: string };
		stsInstances?: { name: string; region?: string | null; enabled?: boolean }[];
	} = {}
) {
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
	const listed = opts.stsInstances ?? [{ name: 'sts' }];
	await page.route('**/api/instances**', (route) =>
		route.fulfill({
			json: {
				instances: listed.map((i, idx) => ({
					id: idx + 1,
					name: i.name,
					domain: 'sts',
					region: i.region ?? null,
					enabled: i.enabled ?? true,
					created_at: 1,
					created_by: 1
				}))
			}
		})
	);
	await page.route('**/api/sts/strategies**', (route) => {
		const url = new URL(route.request().url());
		const status = url.searchParams.get('status') ?? '';
		const before = url.searchParams.get('before');
		if (status === 'done,ack' && opts.history) {
			if (before === opts.history.cursor || Number(url.searchParams.get('offset') ?? '0') > 0) {
				return route.fulfill({
					json: { strategies: opts.history.rest, total: 51, has_more: false }
				});
			}
			return route.fulfill({
				json: { strategies: opts.history.first, total: 51, has_more: true }
			});
		}
		return route.fulfill({
			json: { strategies: live, total: live.length, has_more: false }
		});
	});
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
	await page.route('**/api/sts/sessions/*/yaml', (route) => {
		const parts = new URL(route.request().url()).pathname.split('/');
		const id = parts[parts.length - 2] ?? '';
		const found = live.find((s) => s.session_id === id) ?? live[0];
		return route.fulfill({
			json: {
				type: found?.type ?? 'NoopStrategy',
				session_id: id,
				yaml: `sts:\n  from: ${id}\n`
			}
		});
	});
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

	const instancesLoaded = page.waitForResponse((r) => r.url().includes('/api/instances'));
	await page.goto('/strategy');
	await instancesLoaded;
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
	await expect(page.getByRole('button', { name: 'Load yml' })).toBeVisible();
});

test('Load yml puts the session document in the editor', async ({ page }) => {
	await mockStrategyPage(page, [row('s-live', 'live')]);

	const editor = page.getByLabel('strategy.yml editor');
	await expect(editor).toHaveValue('sts: {}\n');

	await page.getByRole('button', { name: 'Load yml' }).click();
	await expect(editor).toHaveValue('sts:\n  from: s-live\n');

	await page.getByRole('button', { name: 'Refresh' }).click();
	await expect(editor).toHaveValue('sts:\n  from: s-live\n');
});

test('Load yml asks before replacing an edited document', async ({ page }) => {
	await mockStrategyPage(page, [row('s-live', 'live')]);

	const editor = page.getByLabel('strategy.yml editor');
	await expect(editor).toHaveValue('sts: {}\n');
	await editor.fill('sts:\n  qty: 9\n');

	page.once('dialog', (dialog) => dialog.dismiss());
	await page.getByRole('button', { name: 'Load yml' }).click();
	await expect(editor).toHaveValue('sts:\n  qty: 9\n');

	page.once('dialog', (dialog) => dialog.accept());
	await page.getByRole('button', { name: 'Load yml' }).click();
	await expect(editor).toHaveValue('sts:\n  from: s-live\n');
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

const TWO_STS = [
	{ name: 'sts', region: 'jp' },
	{ name: 'sts-2', region: 'tw' }
];

test('Deploy pins the chosen STS instance and omits anycast', async ({ page }) => {
	await mockStrategyPage(page, [row('s-live', 'live')], { stsInstances: TWO_STS });

	const select = page.getByLabel('STS instance');
	await expect(select).toBeVisible();
	await expect(select).toHaveValue('');
	await expect(page.getByRole('button', { name: 'Deploy' })).toBeEnabled();

	const { body } = await captureDeploy(page);
	await page.getByRole('button', { name: 'Deploy' }).click();
	expect((await body).instance).toBeUndefined();
});

test('Deploy sends instance when an STS is chosen', async ({ page }) => {
	await mockStrategyPage(page, [row('s-live', 'live')], { stsInstances: TWO_STS });

	const select = page.getByLabel('STS instance');
	await expect(select).toBeVisible();
	await select.selectOption('sts-2');
	await expect(page.getByRole('button', { name: 'Deploy' })).toBeEnabled();

	const { body } = await captureDeploy(page);
	await page.getByRole('button', { name: 'Deploy' }).click();
	expect((await body).instance).toBe('sts-2');
});

test('a single STS instance hides the picker', async ({ page }) => {
	await mockStrategyPage(page, [row('s-live', 'live')]);
	await expect(page.getByLabel('STS instance')).toHaveCount(0);
});

test('History page 2 replaces page one', async ({
	page
}) => {
	const history = {
		first: [row('s-done-new', 'done'), row('s-done-mid', 'done')],
		rest: [row('s-ack', 'ack')],
		cursor: 's-done-mid'
	};
	await mockStrategyPage(page, [row('s-live', 'live')], { history });

	await page.getByRole('tablist').getByRole('button', { name: 'History' }).click();
	await expect(page.getByRole('link', { name: 's-done-new' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-done-mid' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-ack' })).toHaveCount(0);

	await page.getByRole('button', { name: 'Page 2' }).click();
	await expect(page.getByRole('link', { name: 's-ack' })).toBeVisible();
	await expect(page.getByRole('link', { name: 's-done-new' })).toHaveCount(0);
});
