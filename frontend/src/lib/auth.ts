import { browser } from '$app/environment';
import { goto } from '$app/navigation';

/**
 * Keeping the login session alive, and recovering when it dies anyway.
 *
 * There is one gate now, and it is this app's own (see docs/Auth.md). An
 * expired session is answered with 401 whatever asked, and the answer to a
 * 401 is to route to /login — a page this app serves, reachable without
 * leaving it.
 *
 * Everything that used to live here existed because the gate was outside the
 * app: a reload marker in `sessionStorage`, a cooldown to stop it spinning,
 * and a comment about a sub-resource clobbering the single CSRF cookie slot
 * the document's login redirect needed. Re-login meant reaching a redirect
 * the SPA could not issue, so a full page reload was the missing document
 * navigation. There is no such redirect to reach any more.
 */

/** Where the app's own gate sends someone who has not proved anything. */
export const LOGIN_PATH = '/login';

/**
 * How often to prove the session is still in use.
 *
 * A session's idle window slides on the requests that use it, so it stays
 * alive only as long as requests keep arriving. A WebSocket touches it once,
 * at the handshake, and never again no matter how much data flows over it.
 * Since this UI has no polling and pushes everything over WebSockets, a
 * dashboard someone is actively watching otherwise generates no requests at
 * all and idles out mid-use. Hence a deliberate heartbeat.
 *
 * Comfortably under the 30-minute idle TTL, and cheap: the API only writes
 * `last_seen_at` back when the window has actually moved by about a minute.
 */
const KEEPALIVE_INTERVAL_MS = 5 * 60_000;

/**
 * Send an unauthenticated caller to the login page.
 *
 * Client-side, because the page is part of this app. Always reports true: the
 * caller's request is over either way and the navigation is already on its
 * way, so a caller can use this to decide whether to raise its own error or
 * stay quiet while the page changes underneath it.
 */
export function handleUnauthorized(): boolean {
	if (!browser) return false;
	if (location.pathname !== LOGIN_PATH) void goto(LOGIN_PATH);
	return true;
}

/**
 * One authenticated request: slides the idle window when the session is
 * alive, and routes to login when it is not.
 *
 * Also the only way to learn that a *WebSocket* lost its session. The gate
 * refuses a handshake before accepting it, which the browser surfaces as a
 * close with no status — indistinguishable from the API restarting — so the
 * socket cannot report its own 401. This puts the question to a plain
 * request, where the answer is visible.
 *
 * `/auth/me` rather than `/health`: health is deliberately public so compose
 * and CI can probe it, which means it answers 200 to an expired session and
 * can never be the request that notices one.
 *
 * Best-effort: a network error means we genuinely do not know, so it is left
 * to the caller's reconnect backoff rather than treated as a verdict.
 */
export async function pingSession(): Promise<void> {
	if (!browser) return;
	try {
		const res = await fetch('/api/auth/me', { cache: 'no-store' });
		if (res.status === 401) handleUnauthorized();
	} catch {
		/* offline or API down — not an auth verdict */
	}
}

/**
 * Hold the session open for as long as this tab is actually being watched.
 * Returns a disposer.
 *
 * Gated on visibility on purpose: a tab left open in the background is the
 * abandoned session the idle timeout exists to collect, and keeping it alive
 * would defeat it. Becoming visible pings immediately, so a tab returned to
 * after a long absence lands on the login page on the spot instead of failing
 * on whatever the user clicks first.
 */
export function startSessionKeepalive(): () => void {
	if (!browser) return () => {};

	const ping = () => {
		if (document.visibilityState !== 'visible') return;
		void pingSession();
	};

	const timer = setInterval(ping, KEEPALIVE_INTERVAL_MS);
	document.addEventListener('visibilitychange', ping);

	return () => {
		clearInterval(timer);
		document.removeEventListener('visibilitychange', ping);
	};
}
