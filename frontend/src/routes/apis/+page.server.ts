import { redirect } from '@sveltejs/kit';

/**
 * The UI used to live at `/apis`. Traefik's `PathPrefix(/api)` is a plain
 * string prefix, so it also matched `/apis` and production sent this
 * document to the API — the browser got a raw JSON 401 (issue #19).
 *
 * Both halves of that are fixed: the page is `/keys`, and the production
 * rule is `PathPrefix(/api/)`, so `/apis` reaches the frontend now. This
 * redirect is what an old bookmark or an external link lands on.
 */
export const load = () => {
	redirect(308, '/keys');
};
