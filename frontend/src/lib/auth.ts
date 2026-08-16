import { browser } from '$app/environment';

/**
 * Keeping the login session alive, and recovering when it dies anyway.
 *
 * Nothing in this app authenticates anyone. The whole origin sits behind a
 * Traefik ForwardAuth chain (discord-forward-auth), which answers an expired
 * session two different ways depending on what asked:
 *
 *   - a top-level *document* navigation (`Sec-Fetch-Mode: navigate`) gets a
 *     302 into the Discord OAuth flow, which is what re-login is;
 *   - anything else — every `fetch` and every WebSocket handshake this SPA
 *     opens — gets a 401, deliberately, so a sub-resource cannot clobber the
 *     single CSRF cookie slot the document's login redirect needs.
 *
 * Client-side routing never issues a document navigation, so the SPA cannot
 * reach the redirect on its own. Reloading is the missing document navigation.
 */

/** Set once a reload is committed, so N concurrent 401s cause one reload. */
let reloading = false;

/**
 * Marker for the reload we are about to perform, read back after it lands.
 * `sessionStorage` rather than a module variable because the reload is exactly
 * the event that discards module state.
 */
const RELOAD_MARKER = 'mft:auth-reload-at';

/**
 * A 401 arriving this soon after an auth reload means reloading did not fix it
 * — the document navigation came back authenticated but requests still fail.
 * Reloading again would spin, so past this point the error is left to surface.
 */
const RELOAD_COOLDOWN_MS = 15_000;

/**
 * How often to prove the session is still in use.
 *
 * The auth chain slides its idle window inside its ForwardAuth handler, so a
 * session stays alive only as long as *HTTP requests* keep arriving. Traefik
 * runs ForwardAuth once per request, which for a WebSocket means once, at the
 * handshake — an open socket does not touch the session no matter how much
 * data flows over it. Since this UI has no polling and pushes everything over
 * WebSockets, a dashboard someone is actively watching otherwise generates no
 * requests at all and idles out mid-use. Hence a deliberate heartbeat.
 *
 * Comfortably under the chain's idle TTL (30 minutes as deployed), and cheap:
 * the chain skips the expiry write unless the window moves by over a minute.
 */
const KEEPALIVE_INTERVAL_MS = 5 * 60_000;

/**
 * Reload to hand the expired session back to the auth chain. Returns true when
 * a reload is happening, false when it was suppressed as a likely loop — the
 * caller should surface its error normally in that case.
 *
 * Deliberately a reload rather than a redirect to the `login_url` the chain now
 * returns on 401. That URL is the *original protected resource*, which for the
 * requests this SPA makes is an `/api/...` endpoint — following it would log
 * the user back in and land them on raw JSON instead of the page they were on.
 * Reloading re-requests the current document, so re-login returns them to the
 * route they were already looking at.
 */
export function reloadForLogin(): boolean {
	if (!browser) return false;
	if (reloading) return true;

	const last = Number(sessionStorage.getItem(RELOAD_MARKER) ?? 0);
	if (last && Date.now() - last < RELOAD_COOLDOWN_MS) return false;

	reloading = true;
	sessionStorage.setItem(RELOAD_MARKER, String(Date.now()));
	location.reload();
	return true;
}

/**
 * One authenticated request: slides the idle window when the session is alive,
 * and reloads into re-login when it is not.
 *
 * Also the only way to learn that a *WebSocket* lost its session. A handshake
 * the auth chain rejects surfaces in the browser as a close with code 1006 and
 * no status — indistinguishable from the API restarting — so the socket cannot
 * report its own 401. This puts the question to a plain request, where it is
 * visible.
 *
 * Best-effort: a network error means we genuinely do not know, so it is left
 * to the caller's reconnect backoff rather than treated as a verdict.
 */
export async function reloadIfSessionExpired(): Promise<void> {
	if (!browser || reloading) return;
	try {
		// `/auth/me` rather than `/health`: health is deliberately public so
		// compose and CI can probe it, which means it answers 200 to an expired
		// session and can never be the request that notices one.
		const res = await fetch('/api/auth/me', { cache: 'no-store' });
		if (res.status === 401) reloadForLogin();
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
 * after a long absence re-logs in on the spot instead of failing on whatever
 * the user clicks first.
 */
export function startSessionKeepalive(): () => void {
	if (!browser) return () => {};

	const ping = () => {
		if (document.visibilityState !== 'visible') return;
		void reloadIfSessionExpired();
	};

	const timer = setInterval(ping, KEEPALIVE_INTERVAL_MS);
	document.addEventListener('visibilitychange', ping);

	return () => {
		clearInterval(timer);
		document.removeEventListener('visibilitychange', ping);
	};
}
