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
