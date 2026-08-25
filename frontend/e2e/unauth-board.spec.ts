import { expect, test, type Page } from '@playwright/test';

import { loginUrl, safeNextPath } from '../src/lib/login-path';
import { documentNeedsLogin } from '../src/lib/server/document-gate';

/**
 * Issue #17 — unauthenticated /board must reach /login, never a SvelteKit 500.
 *
 * Playwright's Vite server points API_INTERNAL_URL at a closed port so the
 * document gate fails open (a live local API with the gate on would 303 every
 * other spec). These tests exercise the client path that remains: /auth/status
 * says the visitor is not signed in, and the page must route to /login
 * without throwing.
 */

async function mockGateOnAndSignedOut(page: Page) {
	const unauthenticated = {
		enabled: true,
		setup_required: false,
		providers: ['password'],
		authenticated: false,
		username: 'owner',
		min_password_length: 8
	};
	await page.route('**/api/auth/status', (route) => route.fulfill({ json: unauthenticated }));
	await page.route('**/api/auth/me', (route) =>
		route.fulfill({
			status: 401,
			contentType: 'application/json',
			headers: { 'x-mftik-auth': 'login-required' },
			json: { detail: 'authentication required' }
		})
	);
	await page.route('**/api/board/**', (route) =>
		route.fulfill({
			status: 401,
			contentType: 'application/json',
			headers: { 'x-mftik-auth': 'login-required' },
			json: { detail: 'authentication required' }
		})
	);
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
});

test.describe('unauthenticated board', () => {
	test('cold /board goes to login with a return path and never 500s', async ({ page }) => {
		const pageErrors: string[] = [];
		page.on('pageerror', (err) => pageErrors.push(err.message));
		await mockGateOnAndSignedOut(page);

		await page.goto('/board');

		await expect(page).toHaveURL(/\/login/);
		expect(new URL(page.url()).searchParams.get('next')).toBe('/board');
		await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();
		await expect(page.getByText('Internal Error')).toHaveCount(0);
		expect(pageErrors).toEqual([]);
	});

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
});
