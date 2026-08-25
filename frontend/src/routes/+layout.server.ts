import { redirect } from '@sveltejs/kit';
import { authEnvEnabled, documentNeedsLogin } from '$lib/document-gate';
import { loginUrl } from '$lib/login-path';
import { fetchAuthStatus } from '$lib/server/auth-status';
import { env } from '$env/dynamic/private';
import type { LayoutServerLoad } from './$types';

/**
 * Gate the document, not the API.
 *
 * Issue #17: /board was SSRing and then hydrating against 401 REST and a
 * refused `/ws/board` handshake. Issue #18: every gated route was doing the
 * same thing more slowly — 200 HTML with the control chrome, then a client
 * bounce to /login with no return path.
 *
 * Two questions, cheapest first:
 *
 * 1. This process was told the gate is on (`MFTIK_AUTH_ENABLED`) and the
 *    browser sent no session cookie. No API call. 303.
 * 2. `/auth/status` on the API's internal origin, cookie forwarded. Catches
 *    an expired cookie, and the case this process was not given the flag
 *    but the API's gate is on.
 *
 * `/login` is reachable either way. A down API fails open so the document
 * is not a 500; the layout then withholds the chrome until the browser
 * can ask `/api/auth/status` itself.
 */
export const load: LayoutServerLoad = async ({ cookies, url, fetch }) => {
	const cookie = cookies.get('mftik_session');
	if (
		authEnvEnabled(env.MFTIK_AUTH_ENABLED) &&
		!cookie &&
		documentNeedsLogin({ enabled: true, authenticated: false }, url.pathname)
	) {
		redirect(303, loginUrl(url.pathname + url.search));
	}

	const auth = await fetchAuthStatus(cookie, fetch);
	if (auth && documentNeedsLogin(auth, url.pathname)) {
		redirect(303, loginUrl(url.pathname + url.search));
	}
	return { auth };
};
