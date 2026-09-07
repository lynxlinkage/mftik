import { expect, test, type Page } from '@playwright/test';

/**
 * Home — declare / annotate / drain / retire. Every /api call is intercepted.
 * These tests do not start the API.
 */

type Instance = {
	id: number;
	name: string;
	domain: string;
	region: string | null;
	enabled: boolean;
	created_at: number;
	created_by: number | null;
};

function seed(): Instance[] {
	return [
		{
			id: 1,
			name: 'sts',
			domain: 'sts',
			region: null,
			enabled: true,
			created_at: 1,
			created_by: null
		},
		{
			id: 2,
			name: 'td',
			domain: 'td',
			region: null,
			enabled: true,
			created_at: 1,
			created_by: null
		},
		{
			id: 3,
			name: 'md',
			domain: 'md',
			region: null,
			enabled: true,
			created_at: 1,
			created_by: null
		}
	];
}

function statsOf(instances: Instance[], down: Set<string>) {
	const seen = new Set<string>();
	return {
		domains: instances.map((row) => {
			const first = !seen.has(row.domain);
			seen.add(row.domain);
			const connected = !down.has(row.name);
			return {
				domain: row.domain,
				instance: row.name,
				region: row.region,
				enabled: row.enabled,
				state: connected ? 'connected' : 'down',
				version: connected ? '0.7.1' : null,
				venues: [] as string[],
				api_ids: [] as number[],
				live: first ? 1 : 0,
				done: 0,
				failed: 0,
				interrupted: 0,
				ack: 0,
				healthy: connected
			};
		})
	};
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
}

async function mockHome(
	page: Page,
	opts: { blocked?: Set<string> } = {}
): Promise<{ instances: Instance[]; down: Set<string>; patched: number[]; deleted: number[] }> {
	const instances = seed();
	const down = new Set<string>();
	const blocked = opts.blocked ?? new Set(['td']);
	const patched: number[] = [];
	const deleted: number[] = [];
	let nextId = 4;

	await mockAuth(page);
	await page.route('**/api/stats', (route) =>
		route.fulfill({ json: statsOf(instances, down) })
	);
	await page.route('**/api/instances**', async (route) => {
		const req = route.request();
		const url = new URL(req.url());
		const idMatch = url.pathname.match(/\/instances\/(\d+)$/);
		if (idMatch && req.method() === 'PATCH') {
			const id = Number(idMatch[1]);
			const row = instances.find((i) => i.id === id);
			if (!row) {
				await route.fulfill({ status: 404, json: { detail: `unknown instance: ${id}` } });
				return;
			}
			const body = req.postDataJSON() as { region?: string; enabled?: boolean };
			if (body.region !== undefined) row.region = body.region || null;
			if (body.enabled !== undefined) row.enabled = body.enabled;
			patched.push(id);
			await route.fulfill({ json: row });
			return;
		}
		if (idMatch && req.method() === 'DELETE') {
			const id = Number(idMatch[1]);
			const row = instances.find((i) => i.id === id);
			if (!row) {
				await route.fulfill({ status: 404, json: { detail: `unknown instance: ${id}` } });
				return;
			}
			if (blocked.has(row.name)) {
				await route.fulfill({
					status: 409,
					json: {
						detail:
							`instance '${row.name}' is still named by a credential — ` +
							'move those apis to another instance first'
					}
				});
				return;
			}
			const idx = instances.indexOf(row);
			instances.splice(idx, 1);
			down.delete(row.name);
			deleted.push(id);
			await route.fulfill({ json: { id, deleted: true } });
			return;
		}
		if (req.method() === 'POST') {
			const body = req.postDataJSON() as {
				name: string;
				domain: string;
				region?: string;
			};
			if (instances.some((i) => i.name === body.name)) {
				await route.fulfill({
					status: 409,
					json: { detail: `instance already exists: ${body.name}` }
				});
				return;
			}
			const row: Instance = {
				id: nextId++,
				name: body.name,
				domain: body.domain,
				region: body.region || null,
				enabled: true,
				created_at: 2,
				created_by: 1
			};
			instances.push(row);
			down.add(row.name);
			await route.fulfill({ status: 201, json: row });
			return;
		}
		await route.fulfill({ json: { instances } });
	});

	await page.goto('/');
	await expect(page.getByRole('heading', { name: 'Home' })).toBeVisible();
	return { instances, down, patched, deleted };
}

function plane(page: Page, domain: string) {
	return page.locator('section.plane').filter({
		has: page.getByRole('heading', { name: domain, exact: true })
	});
}

function card(page: Page, name: string) {
	return page.locator('.stat').filter({
		has: page.locator('.domain', { hasText: new RegExp(`^${name}$`) })
	});
}

test('declare, annotate, drain and retire', async ({ page }) => {
	page.on('dialog', (dialog) => dialog.accept());
	await mockHome(page);

	await expect(plane(page, 'sts')).toBeVisible();
	await expect(plane(page, 'td')).toBeVisible();
	await expect(plane(page, 'md')).toBeVisible();
	await expect(page.getByRole('link', { name: 'sts' })).toHaveAttribute('href', '/strategy');

	const md = plane(page, 'md');
	await md.getByLabel('Instance name').fill('md-jp-1');
	await md.getByLabel('Declare region').fill('tokyo');
	await md.getByRole('button', { name: 'Declare' }).click();

	const declared = card(page, 'md-jp-1');
	await expect(declared).toBeVisible();
	await expect(declared.locator('.badge')).toHaveText('down');
	await expect(declared.getByRole('button', { name: 'tokyo' })).toBeVisible();

	await declared.getByRole('button', { name: 'tokyo' }).click();
	await declared.getByLabel('Annotate region for md-jp-1').fill('ap-northeast-1');
	await declared.getByLabel('Annotate region for md-jp-1').press('Enter');
	await expect(declared.getByRole('button', { name: 'ap-northeast-1' })).toBeVisible();

	await declared.getByRole('button', { name: 'Drain' }).click();
	await expect(declared.locator('.badge')).toHaveText('draining');
	await declared.getByRole('button', { name: 'Enable' }).click();
	await expect(declared.locator('.badge')).toHaveText('down');

	await declared.getByRole('button', { name: 'Retire' }).click();
	await expect(declared).toHaveCount(0);
});

test('retire blocked by a credential shows the 409', async ({ page }) => {
	page.on('dialog', (dialog) => dialog.accept());
	await mockHome(page);

	await card(page, 'td').getByRole('button', { name: 'Retire' }).click();
	await expect(page.locator('.error-banner')).toContainText(
		"instance 'td' is still named by a credential"
	);
	await expect(card(page, 'td')).toBeVisible();
});

test('editing region then clicking Retire still retires', async ({ page }) => {
	const dialogs: string[] = [];
	page.on('dialog', (dialog) => {
		dialogs.push(dialog.message());
		dialog.accept();
	});
	const { patched, deleted } = await mockHome(page);

	const mdCard = card(page, 'md');
	await mdCard.getByRole('button', { name: 'region' }).click();
	await mdCard.getByLabel('Annotate region for md').fill('tokyo');
	await mdCard.getByRole('button', { name: 'Retire' }).click();

	await expect(mdCard).toHaveCount(0);
	expect(deleted).toEqual([3]);
	expect(dialogs).toHaveLength(1);
	expect(patched, 'the abandoned label is not saved onto a row being deleted').toEqual(
		[]
	);
});

test('editing region then clicking Drain saves the region too', async ({ page }) => {
	// The other half of the same blur: here the edit is not abandoned, so the
	// button that stole the focus has to commit it before it changes enabled.
	const { instances, patched } = await mockHome(page);

	const mdCard = card(page, 'md');
	await mdCard.getByRole('button', { name: 'region' }).click();
	await mdCard.getByLabel('Annotate region for md').fill('tokyo');
	await mdCard.getByRole('button', { name: 'Drain' }).click();

	await expect(mdCard.getByRole('button', { name: 'Enable' })).toBeVisible();
	const md = instances.find((i) => i.name === 'md');
	expect(md?.region).toBe('tokyo');
	expect(md?.enabled).toBe(false);
	expect(patched).toEqual([3, 3]);
});

test('a failed instances list still shows health cards', async ({ page }) => {
	const instances = seed();
	await mockAuth(page);
	await page.route('**/api/stats', (route) =>
		route.fulfill({ json: statsOf(instances, new Set()) })
	);
	await page.route('**/api/instances**', (route) =>
		route.fulfill({ status: 500, json: { detail: 'instances unavailable' } })
	);

	await page.goto('/');
	await expect(page.getByRole('heading', { name: 'Home' })).toBeVisible();
	await expect(card(page, 'sts')).toBeVisible();
	await expect(card(page, 'td')).toBeVisible();
	await expect(card(page, 'md')).toBeVisible();
	await expect(page.getByRole('button', { name: 'Drain' })).toHaveCount(0);
	await expect(page.getByText('No instances declared.')).toHaveCount(0);
	await expect(page.locator('.error-banner')).toContainText('instances unavailable');
});

test('a failed stats load does not claim nothing is declared', async ({ page }) => {
	await mockAuth(page);
	await page.route('**/api/stats', (route) =>
		route.fulfill({ status: 500, json: { detail: 'stats down' } })
	);
	await page.route('**/api/instances**', (route) =>
		route.fulfill({ json: { instances: seed() } })
	);

	await page.goto('/');
	await expect(page.locator('.error-banner')).toContainText('stats down');
	await expect(page.getByText('No instances declared.')).toHaveCount(0);
});

test('an illegal name cannot be declared', async ({ page }) => {
	await mockHome(page);
	const md = plane(page, 'md');
	await md.getByLabel('Instance name').fill('md.jp');
	await expect(md.getByRole('button', { name: 'Declare' })).toBeDisabled();
});
