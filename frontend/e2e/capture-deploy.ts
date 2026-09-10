import type { Page } from '@playwright/test';

const DEPLOYED = {
	session_id: 's-new',
	type: 'NoopStrategy',
	config: {},
	td: [],
	md: [],
	status: 'live'
};

/**
 * Arm a `/sts/deploy` interceptor and return a promise for the POST body.
 *
 * The route is registered before this resolves, so Deploy cannot slip past
 * to the vite proxy. The inner `body` promise is what the POST settles.
 */
export async function captureDeploy(page: Page): Promise<{
	body: Promise<Record<string, unknown>>;
}> {
	let resolve!: (v: Record<string, unknown>) => void;
	let reject!: (e: unknown) => void;
	const body = new Promise<Record<string, unknown>>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	await page.route('**/api/sts/deploy/**', async (route) => {
		try {
			resolve(route.request().postDataJSON() as Record<string, unknown>);
			await route.fulfill({ json: DEPLOYED });
		} catch (e) {
			reject(e);
		}
	});
	return { body };
}
