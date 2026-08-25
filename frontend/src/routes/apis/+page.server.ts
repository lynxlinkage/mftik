import { redirect } from '@sveltejs/kit';

/**
 * The UI used to live at `/apis`. Traefik's `PathPrefix(/api)` also
 * matches `/apis`, so production sent this document to the API and the
 * browser showed raw JSON 401 (issue #19). The page is `/keys` now.
 *
 * This redirect is for local / Caddy, where `/api/*` requires the slash
 * and `/apis` still reaches the frontend. Production Traefik never gets
 * here — the sidebar no longer points at `/apis`.
 */
export const load = () => {
	redirect(308, '/keys');
};
