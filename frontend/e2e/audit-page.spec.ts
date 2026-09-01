import { expect, test, type Page } from '@playwright/test';

/**
 * Audit log — numbered pages.
 *
 * Every /api call is intercepted. These tests do not start the API.
 */

type Audit = {
	id: number;
	user_id: number;
	operation: string;
	result: string;
	created_at: number;
	via: string | null;
	key_kind: string | null;
	key_id: number | null;
};

function row(id: number, operation: string): Audit {
	return {
		id,
		user_id: 1,
		operation,
		result: 'ok',
		created_at: id,
		via: 'password',
		key_kind: null,
		key_id: null
	};
}

const PAGE = [row(3, 'op.new'), row(2, 'op.mid')];
const REST = [row(1, 'op.old')];

async function mockAuditPage(page: Page): Promise<{ urls: URL[] }> {
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
	await page.route('**/api/audits**', async (route) => {
		const url = new URL(route.request().url());
		urls.push(url);
		const offset = Number(url.searchParams.get('offset') ?? '0');
		if (offset > 0) {
			await route.fulfill({ json: { audits: REST, total: 51, has_more: false } });
			return;
		}
		await route.fulfill({ json: { audits: PAGE, total: 51, has_more: true } });
	});

	await page.goto('/audit');
	await expect(page.getByRole('heading', { name: 'Audit' })).toBeVisible();

	return { urls };
}

test('page 2 replaces page one and sends offset', async ({ page }) => {
	const { urls } = await mockAuditPage(page);

	await expect(page.getByText('op.new')).toBeVisible();
	await expect(page.getByText('op.mid')).toBeVisible();
	await expect(page.getByText('op.old')).toHaveCount(0);

	await page.getByRole('button', { name: 'Page 2' }).click();
	await expect(page.getByText('op.old')).toBeVisible();
	await expect(page.getByText('op.new')).toHaveCount(0);

	const more = urls.find((u) => u.searchParams.get('offset') === '50');
	expect(more).toBeTruthy();
});

test('Refresh keeps the current page', async ({ page }) => {
	const { urls } = await mockAuditPage(page);

	await page.getByRole('button', { name: 'Page 2' }).click();
	await expect(page.getByText('op.old')).toBeVisible();

	await page.getByRole('button', { name: 'Refresh' }).click();
	await expect(page.getByText('op.old')).toBeVisible();
	await expect(page.getByText('op.new')).toHaveCount(0);

	const last = urls.at(-1);
	expect(last?.searchParams.get('offset')).toBe('50');
});
