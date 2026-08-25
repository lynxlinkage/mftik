import { env } from '$env/dynamic/private';
import type { DocumentAuth } from '$lib/document-gate';

/** Same name the API sets. Duplicated so this file does not import the client. */
const SESSION_COOKIE = 'mftik_session';

/**
 * Origin the frontend process uses to ask the API a question.
 *
 * Browser calls go to `/api` on the document origin and the edge forwards
 * them. This process is not behind that edge: a relative `/api/auth/status`
 * would hit the SvelteKit server and 404. Compose already has
 * `API_PROXY_TARGET` for the Vite proxy; production images have no proxy, so
 * they set `API_INTERNAL_URL` to the same place (`http://api:8000`).
 */
export function internalApiOrigin(): string {
	const raw = env.API_INTERNAL_URL || env.API_PROXY_TARGET || 'http://127.0.0.1:8000';
	return raw.replace(/\/+$/, '');
}

/**
 * Public `/auth/status`, with the browser's session cookie if it sent one.
 *
 * `null` when the API cannot be reached or the body is not a status. The
 * document gate then fails open: a down API must not turn every page into a
 * 500, and the client 401 handler still routes to /login.
 */
export async function fetchAuthStatus(
	cookie: string | undefined,
	fetcher: typeof fetch = fetch
): Promise<DocumentAuth | null> {
	try {
		const res = await fetcher(`${internalApiOrigin()}/auth/status`, {
			headers: cookie ? { cookie: `${SESSION_COOKIE}=${cookie}` } : {},
			signal: AbortSignal.timeout(2_000)
		});
		if (!res.ok) return null;
		const body = (await res.json()) as Partial<DocumentAuth>;
		if (typeof body.enabled !== 'boolean' || typeof body.authenticated !== 'boolean') {
			return null;
		}
		return { enabled: body.enabled, authenticated: body.authenticated };
	} catch {
		return null;
	}
}
