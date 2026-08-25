import { expect, test, type Page } from '@playwright/test';

import { authEnvEnabled, documentNeedsLogin } from '../src/lib/document-gate';
import { loginUrl, safeNextPath } from '../src/lib/login-path';

/**
 * Issues #17 and #18 — unauthenticated gated routes must reach /login with a
 * return path, never a SvelteKit 500, and must not paint the control chrome
 * (nav, registry cards, `Failed to fetch`) while they wait.
 *
 * Playwright's Vite server points API_INTERNAL_URL at a closed port and does
 * not set MFTIK_AUTH_ENABLED, so the document 303 cannot fire (a live local
 * API with the gate on would 303 every other spec). These tests exercise the
 * remaining client path: the layout withholds chrome until `/auth/status`
 * answers, then routes to /login.
 */

const GATED = [
	'/',
	'/board',
	'/registry',
	'/strategy',
	'/audit',
	'/settings',
	'/sym',
	'/alerts',
	'/sts',
	'/keys'
] as const;

async function mockGateOnAndSignedOut(page: Page) {
	const unauthenticated = {
		enabled: true,
		setup_required: false,
		providers: ['password'],
		authenticated: false,
		username: 'owner',
		min_password_length: 8
	};
	await page.route('**/api/**', (route) => {
		if (route.request().url().includes('/api/auth/status')) {
			return route.fulfill({ json: unauthenticated });
		}
		return route.fulfill({
			status: 401,
			contentType: 'application/json',
			headers: { 'x-mftik-auth': 'login-required' },
			json: { detail: 'authentication required' }
		});
	});
}

test.describe('login return path helpers', () => {
	test('safeNextPath only honours in-app relative paths', () => {
		expect(safeNextPath('/board')).toBe('/board');
		expect(safeNextPath('/board/abc?tab=live')).toBe('/board/abc?tab=live');
		expect(safeNextPath('https://evil.example/board')).toBe('/');
		expect(safeNextPath('//evil.example/board')).toBe('/');
		expect(safeNextPath('/login')).toBe('/');
		expect(safeNextPath('/login?next=/board')).toBe('/');
		expect(safeNextPath(null)).toBe('/');
	});

	test('loginUrl encodes next', () => {
		expect(loginUrl('/board')).toBe('/login?next=%2Fboard');
	});

	test('documentNeedsLogin matches the gate', () => {
		expect(documentNeedsLogin({ enabled: true, authenticated: false }, '/board')).toBe(true);
		expect(documentNeedsLogin({ enabled: true, authenticated: false }, '/login')).toBe(false);
		expect(documentNeedsLogin({ enabled: false, authenticated: true }, '/board')).toBe(false);
		expect(documentNeedsLogin({ enabled: true, authenticated: true }, '/board')).toBe(false);
	});

	test('authEnvEnabled matches the API flag', () => {
		expect(authEnvEnabled('1')).toBe(true);
		expect(authEnvEnabled('true')).toBe(true);
		expect(authEnvEnabled('0')).toBe(false);
		expect(authEnvEnabled(undefined)).toBe(false);
	});
});

test.describe('unauthenticated documents', () => {
	for (const path of GATED) {
		test(`cold ${path} goes to login with next and never paints chrome`, async ({ page }) => {
			const pageErrors: string[] = [];
			page.on('pageerror', (err) => pageErrors.push(err.message));
			await mockGateOnAndSignedOut(page);

			await page.goto(path);

			await expect(page).toHaveURL(/\/login/);
			const next = path === '/' ? '/' : path;
			expect(new URL(page.url()).searchParams.get('next')).toBe(next);
			await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();
			await expect(page.getByText('Internal Error')).toHaveCount(0);
			await expect(page.getByText('Failed to fetch')).toHaveCount(0);
			expect(pageErrors).toEqual([]);
		});
	}

	test('cold /board/:id keeps the session path in next', async ({ page }) => {
		const pageErrors: string[] = [];
		page.on('pageerror', (err) => pageErrors.push(err.message));
		await mockGateOnAndSignedOut(page);

		await page.goto('/board/sess-1');

		await expect(page).toHaveURL(/\/login/);
		expect(new URL(page.url()).searchParams.get('next')).toBe('/board/sess-1');
		await expect(page.getByText('Internal Error')).toHaveCount(0);
		expect(pageErrors).toEqual([]);
	});

	test('fail-open SSR of /registry does not include registry chrome', async ({ request }) => {
		const res = await request.get('/registry');
		expect(res.status()).toBe(200);
		const html = await res.text();
		expect(html).not.toContain('Failed to fetch');
		expect(html).not.toContain('this node');
		expect(html).not.toMatch(/<h1>Registry<\/h1>/);
	});

	test('legacy /apis redirects to /keys', async ({ request }) => {
		const res = await request.get('/apis', { maxRedirects: 0 });
		expect(res.status()).toBe(308);
		expect(res.headers().location).toBe('/keys');
	});
});
