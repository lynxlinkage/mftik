import { env } from '$env/dynamic/public';

/**
 * Origin for WebSocket connections (`/ws/...` on the API).
 *
 * REST goes through the Vite `/api` proxy, but a WebSocket cannot — it is
 * opened straight from the browser — so this needs the API's real origin.
 *
 * Read through `$env/dynamic/public` rather than `import.meta.env`: Vite only
 * exposes variables matching its `envPrefix` (`VITE_` by default), so
 * `import.meta.env.PUBLIC_API_URL` was always undefined and this silently fell
 * back to `hostname:8000` no matter what the deployment set. Dynamic rather
 * than static because the container is built once and given PUBLIC_API_URL at
 * run time, which a build-time substitution would ignore.
 */
export function wsBaseUrl(): string {
	const api = env.PUBLIC_API_URL;
	if (api) {
		const u = new URL(api);
		u.protocol = u.protocol === 'https:' ? 'wss:' : 'ws:';
		return u.origin;
	}
	const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
	return `${proto}//${window.location.hostname}:8000`;
}
