import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

// In Docker, point at the compose service name (api). Locally, hit the published port.
const apiProxyTarget = process.env.API_PROXY_TARGET || 'http://localhost:8000';

export default defineConfig({
	plugins: [sveltekit()],
	server: {
		host: '0.0.0.0',
		port: 5173,
		proxy: {
			// Trailing slash on purpose. Vite's prefix match is string-prefix,
			// so `/api` also swallows `/apis` — the UI document — and that is
			// issue #19 on production Traefik (`PathPrefix(/api)`). The page
			// now lives at `/keys`; this keeps local `/apis` on SvelteKit so
			// the 308 to `/keys` can fire. Browser API calls are `/api/…`.
			'/api/': {
				target: apiProxyTarget,
				changeOrigin: true,
				rewrite: (path) => path.replace(/^\/api/, '')
			},
			// WebSockets go through the proxy too, so the socket is opened on
			// the same origin as the document and the session cookie rides the
			// handshake. Opened straight at the API's port instead, it is
			// cross-origin and the cookie is never sent — every /ws endpoint
			// then fails auth locally while working in production, where one
			// hostname serves all three. See docs/Auth.md.
			//
			// No `rewrite`, unlike /api: Traefik does not strip these paths
			// either, and the API mounts them verbatim.
			'/ws': {
				target: apiProxyTarget,
				changeOrigin: true,
				ws: true
			}
		}
	}
});
