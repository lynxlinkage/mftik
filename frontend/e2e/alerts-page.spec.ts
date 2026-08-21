import { expect, test, type Page, type Route } from '@playwright/test';

/**
 * Alert page — three-lane Svelte Flow editor. Stubs the API the same way
 * strategy-page.spec.ts does: authenticated: false, never the live backend.
 */

const HEX_SESSION = 'deadbeefdeadbeefdeadbeefdeadbeef';
const WEBHOOK = 'https://discord.com/api/webhooks/111/super-secret-token';

type Source = {
	id: number;
	created_by: number;
	domain: string;
	selector: string;
	matcher_ids: number[];
};

type Matcher = {
	id: number;
	created_by: number;
	name: string;
	kind: string;
	spec: Record<string, unknown>;
	source_ids: number[];
	alert_ids: number[];
	disabled_reason: string | null;
};

type AlertRow = {
	id: number;
	created_by: number;
	name: string;
	kind: string;
	webhook_masked: string;
	enabled: boolean;
	flush_interval_s: number;
	max_events_in_payload: number;
	max_buffer_events: number;
	dedupe: boolean;
	matcher_ids: number[];
};

function mask(url: string): string {
	const host = new URL(url).host;
	return `https://${host}/api/webhooks/…/***`;
}

function jsonHasWebhook(body: unknown): boolean {
	return JSON.stringify(body).includes('super-secret-token') || JSON.stringify(body).includes(WEBHOOK);
}

async function fulfillJson(route: Route, body: unknown) {
	if (jsonHasWebhook(body)) {
		throw new Error('mock leaked webhook_url');
	}
	await route.fulfill({ json: body });
}

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
	await page.route('**/api/auth/keys', (route) => route.fulfill({ json: { keys: [] } }));
	await page.route('**/api/auth/identities', (route) =>
		route.fulfill({ json: { identities: [] } })
	);
	await page.route('**/api/environment', (route) =>
		route.fulfill({
			json: {
				generation: 1,
				python: [3, 12],
				platform: 'darwin',
				bytes: 0,
				packages: {},
				installed: [],
				abi_ok: true,
				runtime_python: [3, 12],
				runtime_platform: 'darwin',
				restart_required: false,
				loaded: true,
				load_error: null,
				broken: []
			}
		})
	);
}

async function mockAlertsPage(page: Page) {
	await mockAuth(page);

	const sources: Source[] = [];
	const matchers: Matcher[] = [];
	const alerts: AlertRow[] = [];
	let nextId = 1;
	const getBodies: unknown[] = [];

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
	await page.route('**/api/sts/strategies**', (route) =>
		route.fulfill({
			json: {
				strategies: [
					{
						type: 'NoopStrategy',
						config: {},
						created_by: 1,
						created_at: 1,
						session_id: HEX_SESSION,
						status: 'live',
						reason: null,
						td_api_ids: [],
						md_ids: []
					}
				],
				has_more: false
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
	await page.route('**/api/venues', (route) =>
		route.fulfill({
			json: {
				venues: [
					{
						name: 'Paper',
						label: 'Paper',
						api_types: ['HMAC'],
						categories: ['Spot'],
						requires_passphrase: false,
						simulated: true,
						ticker_example: 'BTCUSDT'
					}
				]
			}
		})
	);

	await page.route('**/api/alerts**', async (route) => {
		const req = route.request();
		const url = new URL(req.url());
		const path = url.pathname.replace(/^\/api/, '');
		const method = req.method();

		const sourceWire = path.match(/^\/alerts\/sources\/(\d+)\/matchers\/(\d+)$/);
		const matcherWire = path.match(/^\/alerts\/matchers\/(\d+)\/alerts\/(\d+)$/);
		const deliveries = path.match(/^\/alerts\/(\d+)\/deliveries$/);
		const testFire = path.match(/^\/alerts\/(\d+)\/test$/);

		if (path === '/alerts/sources' && method === 'GET') {
			const body = { sources };
			getBodies.push(body);
			return fulfillJson(route, body);
		}
		if (path === '/alerts/sources' && method === 'POST') {
			const posted = req.postDataJSON() as { domain: string; selector: string };
			const row: Source = {
				id: nextId++,
				created_by: 1,
				domain: posted.domain,
				selector: posted.selector,
				matcher_ids: []
			};
			sources.push(row);
			return fulfillJson(route, row);
		}
		if (path === '/alerts/matchers' && method === 'GET') {
			const body = { matchers };
			getBodies.push(body);
			return fulfillJson(route, body);
		}
		if (path === '/alerts/matchers' && method === 'POST') {
			const posted = req.postDataJSON() as {
				name: string;
				kind: string;
				spec: Record<string, unknown>;
			};
			const row: Matcher = {
				id: nextId++,
				created_by: 1,
				name: posted.name,
				kind: posted.kind,
				spec: posted.spec,
				source_ids: [],
				alert_ids: [],
				disabled_reason: posted.name === 'timeouts' ? 'regex timeout' : null
			};
			matchers.push(row);
			return fulfillJson(route, row);
		}
		if (path === '/alerts' && method === 'GET') {
			const body = { alerts };
			getBodies.push(body);
			return fulfillJson(route, body);
		}
		if (path === '/alerts' && method === 'POST') {
			const posted = req.postDataJSON() as {
				name: string;
				webhook_url: string;
				flush_interval_s?: number;
				max_events_in_payload?: number;
				max_buffer_events?: number;
				dedupe?: boolean;
				enabled?: boolean;
			};
			const row: AlertRow = {
				id: nextId++,
				created_by: 1,
				name: posted.name,
				kind: 'discord_webhook',
				webhook_masked: mask(posted.webhook_url),
				enabled: posted.enabled ?? true,
				flush_interval_s: posted.flush_interval_s ?? 30,
				max_events_in_payload: posted.max_events_in_payload ?? 15,
				max_buffer_events: posted.max_buffer_events ?? 200,
				dedupe: posted.dedupe ?? true,
				matcher_ids: []
			};
			alerts.push(row);
			return fulfillJson(route, row);
		}
		if (sourceWire && (method === 'PUT' || method === 'DELETE')) {
			const sourceId = Number(sourceWire[1]);
			const matcherId = Number(sourceWire[2]);
			const source = sources.find((s) => s.id === sourceId);
			const matcher = matchers.find((m) => m.id === matcherId);
			if (source && matcher && method === 'PUT') {
				if (!source.matcher_ids.includes(matcherId)) source.matcher_ids.push(matcherId);
				if (!matcher.source_ids.includes(sourceId)) matcher.source_ids.push(sourceId);
			}
			return fulfillJson(route, {
				wired: method === 'PUT',
				source_id: sourceId,
				matcher_id: matcherId,
				alert_id: null
			});
		}
		if (matcherWire && (method === 'PUT' || method === 'DELETE')) {
			const matcherId = Number(matcherWire[1]);
			const alertId = Number(matcherWire[2]);
			const matcher = matchers.find((m) => m.id === matcherId);
			const alert = alerts.find((a) => a.id === alertId);
			if (matcher && alert && method === 'PUT') {
				if (!matcher.alert_ids.includes(alertId)) matcher.alert_ids.push(alertId);
				if (!alert.matcher_ids.includes(matcherId)) alert.matcher_ids.push(matcherId);
			}
			return fulfillJson(route, {
				wired: method === 'PUT',
				source_id: null,
				matcher_id: matcherId,
				alert_id: alertId
			});
		}
		if (deliveries && method === 'GET') {
			return fulfillJson(route, { deliveries: [] });
		}
		if (testFire && method === 'POST') {
			return fulfillJson(route, {
				delivery: {
					id: nextId++,
					alert_id: Number(testFire[1]),
					window_start: 1,
					event_count: 0,
					dropped_count: 0,
					http_status: 204,
					error: null,
					ts: 1
				}
			});
		}
		return route.fulfill({ status: 404, json: { detail: path } });
	});

	await page.goto('/alerts');
	return { getBodies };
}

test('nav shows Alert and the page wires a graph without exposing the webhook', async ({
	page
}) => {
	const { getBodies } = await mockAlertsPage(page);

	await expect(page).toHaveTitle('Alert · MFTIK Control');
	const nav = page.getByRole('navigation');
	await expect(nav.getByRole('link', { name: 'Alert' })).toBeVisible();

	await expect(page.getByRole('heading', { name: 'Alerts', level: 1 })).toBeVisible();
	await expect(page.getByRole('heading', { name: 'Sources' })).toBeVisible();
	await expect(page.getByRole('heading', { name: 'Matchers' })).toBeVisible();
	await expect(page.getByText('No graph yet.')).toBeVisible();

	await page.getByRole('button', { name: 'Add source node' }).click();
	const picker = page.getByTestId('sts-picker');
	await expect(picker.locator('option', { hasText: 'NoopStrategy' })).toHaveCount(1);
	await expect(picker).not.toContainText(HEX_SESSION);
	await page.getByRole('button', { name: 'Add source', exact: true }).click();
	await expect(page.getByRole('heading', { name: 'New source' })).toHaveCount(0);
	await expect(page.locator('code', { hasText: 'sts:*' })).toBeVisible();

	await page.getByRole('button', { name: 'Add matcher node' }).click();
	await expect(page.getByRole('heading', { name: 'New matcher' })).toBeVisible();
	await page.getByLabel('Matcher name').fill('warns');
	const matcherPosted = page.waitForRequest(
		(req) => req.method() === 'POST' && req.url().includes('/api/alerts/matchers')
	);
	await page.getByTestId('submit-matcher').click();
	await matcherPosted;
	await expect(page.locator('strong', { hasText: 'warns' })).toBeVisible();

	await page.getByRole('button', { name: 'Add alert node' }).click();
	await expect(page.getByRole('heading', { name: 'New alert' })).toBeVisible();
	await page.getByLabel('Alert name').fill('ops');
	await page.getByLabel('Webhook URL').fill(WEBHOOK);
	await page.getByRole('button', { name: 'Add alert', exact: true }).click();

	await expect(page.locator('code', { hasText: 'sts:*' })).toBeVisible();
	await expect(page.locator('strong', { hasText: 'warns' })).toBeVisible();
	await expect(page.locator('strong', { hasText: 'ops' })).toBeVisible();
	await expect(page.getByText('https://discord.com/api/webhooks/…/***')).toBeVisible();
	await expect(page.getByText('No graph yet.')).toHaveCount(0);

	await page.evaluate(async () => {
		await fetch('/api/alerts/sources/1/matchers/2', { method: 'PUT' });
		await fetch('/api/alerts/matchers/2/alerts/3', { method: 'PUT' });
	});
	await page.getByRole('button', { name: 'Refresh' }).click();
	await expect(page.locator('.svelte-flow__node[data-id="source-1"]')).toBeVisible();
	await expect(page.locator('[data-id="sm-1-2"]')).toBeVisible();
	await expect(page.locator('[data-id="ma-2-3"]')).toBeVisible();

	for (const body of getBodies) {
		expect(jsonHasWebhook(body)).toBe(false);
		expect(JSON.stringify(body)).not.toContain('webhook_url');
	}

	await page.getByRole('link', { name: 'Settings' }).click();
	await expect(page.getByRole('heading', { name: 'Settings' })).toBeVisible();
	await expect(page.getByRole('heading', { name: 'Identities' })).toBeVisible();
	await expect(page.getByRole('heading', { name: 'Keys' })).toBeVisible();
	await expect(page.getByRole('heading', { name: 'Alert', exact: true })).toHaveCount(0);
	await expect(page.getByRole('heading', { name: 'Alerts', exact: true })).toHaveCount(0);
});
