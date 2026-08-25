import adapter from '@sveltejs/adapter-node';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

/** @type {import('@sveltejs/kit').Config} */
const config = {
	preprocess: vitePreprocess(),
	kit: {
		adapter: adapter(),
		// Content-Security-Policy (issue #21).
		//
		// This lives here and not on the Traefik middleware next to HSTS and
		// the frame/referrer headers, because the document carries an inline
		// hydration script. A policy written at the edge could only allow that
		// with `script-src 'unsafe-inline'`, which is the exact thing CSP is
		// for. SvelteKit knows which inline blocks it emitted and nonces them,
		// so `script-src` stays closed.
		csp: {
			directives: {
				'default-src': ['self'],
				'script-src': ['self'],
				// `unsafe-inline` is load-bearing and cannot be swapped for a
				// nonce: `app.html` wraps the app in `<div style="display:
				// contents">`, and a style *attribute* is not noncible — only
				// `unsafe-inline` (or `unsafe-hashes`) permits one. Note that
				// naming a nonce or hash here would make browsers ignore
				// `unsafe-inline` and blank that wrapper, so this directive is
				// deliberately left without one.
				'style-src': ['self', 'unsafe-inline', 'https://fonts.googleapis.com'],
				// The webfonts the stylesheet above resolves to.
				'font-src': ['self', 'https://fonts.gstatic.com'],
				'img-src': ['self', 'data:'],
				// REST on the same origin, and the /ws/* sockets — `self`
				// covers same-origin ws:/wss: under CSP3, which is the only
				// place this app opens one (`lib/ws.ts` derives the URL from
				// `window.location`), so the policy stays host-agnostic for
				// self-hosted nodes.
				'connect-src': ['self'],
				'base-uri': ['self'],
				'object-src': ['none'],
				'form-action': ['self'],
				// The header equivalent of X-Frame-Options: DENY, which the
				// edge also sends for the sake of the API responses.
				'frame-ancestors': ['none']
			}
		}
	}
};

export default config;
