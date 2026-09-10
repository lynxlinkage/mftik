import { pingSession } from '$lib/auth';

/**
 * Origin for WebSocket connections (`/ws/...` on the API).
 *
 * Always the document's own origin, and deliberately not configurable.
 *
 * A WebSocket handshake carries cookies under the same rules as any other
 * request, which means it carries the session cookie only when it goes to the
 * origin that cookie belongs to. Pointed at the API's own port instead — which
 * is what `PUBLIC_API_URL` used to do here — the socket is cross-origin, the
 * cookie is withheld, and the handshake is refused as unauthenticated. It
 * looked fine before only because nothing authenticated these sockets.
 *
 * Production already satisfies this: one hostname serves the document, `/api`
 * and `/ws`. Locally the Vite proxy forwards `/ws` to the API for the same
 * reason it forwards `/api`, so both sides of the app now reach it the same
 * way. See docs/Auth.md.
 */
export function wsBaseUrl(): string {
	const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
	return `${proto}//${window.location.host}`;
}

/**
 * Close code the auth gate names when it refuses a handshake.
 *
 * Rarely the code the browser reports. The gate closes *before* accepting,
 * which uvicorn turns into an HTTP 403 on the handshake, and a handshake the
 * browser never completed is reported as 1006 with no reason — the status is
 * deliberately withheld from script. Worth checking anyway: a post-accept
 * close, or a proxy that terminates the socket itself, does carry it.
 */
export const WS_AUTH_REFUSED = 1008;

/**
 * Whether a closed socket is worth reopening.
 *
 * Every stream here reconnects with backoff, because a socket that dies
 * quietly is worse than no socket at all — it looks live while showing frozen
 * state. A refused login is the one close that backoff cannot fix: no number
 * of retries produces a credential, and the loop spends forever hammering
 * `/ws` and `/auth/me` behind a UI still claiming to be connecting.
 *
 * Since 1006 is all the socket itself will say, the verdict comes from
 * `pingSession`, which is the request that can see a 401 — and which routes to
 * /login as a side effect. An inconclusive answer keeps the retry: an API
 * that is down owes us no verdict, and that is exactly the case backoff is
 * for.
 */
export async function shouldReopen(closeCode: number | undefined): Promise<boolean> {
	if (closeCode === WS_AUTH_REFUSED) return false;
	return (await pingSession()) !== 'unauthenticated';
}
