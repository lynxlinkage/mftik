import { redirect } from '@sveltejs/kit';
import { loginUrl } from '$lib/login-path';
import { fetchAuthStatus } from '$lib/server/auth-status';
import { documentNeedsLogin } from '$lib/server/document-gate';
import type { LayoutServerLoad } from './$types';

/**
 * Gate the document, not the API.
 *
 * An unauthenticated cold load of /board used to SSR the board and then
 * hydrate against 401 REST and a refused `/ws/board` handshake. That is how
 * issue #17 rendered a SvelteKit 500 inside the app shell. Asking
 * `/auth/status` here — public, cookie-aware — means those pages never
 * render when the gate is on and the visitor has not proved anything.
 */
export const load: LayoutServerLoad = async ({ cookies, url }) => {
	const auth = await fetchAuthStatus(cookies.get('mftik_session'));
	if (auth && documentNeedsLogin(auth, url.pathname)) {
		redirect(303, loginUrl(url.pathname + url.search));
	}
	return { auth };
};
