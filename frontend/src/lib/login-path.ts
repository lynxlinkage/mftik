/**
 * The app's own gate, as a path. Shared by the document redirect and the
 * client 401 handler so the two cannot drift onto different pages.
 */

export const LOGIN_PATH = '/login';

/**
 * A return path the login form may honour.
 *
 * Only in-app relative paths. Protocol-relative and absolute URLs are how an
 * open redirect walks someone off the origin after they sign in.
 */
export function safeNextPath(raw: string | null | undefined, fallback = '/'): string {
	if (!raw) return fallback;
	if (!raw.startsWith('/')) return fallback;
	if (raw.startsWith('//')) return fallback;
	if (raw.includes('://')) return fallback;
	const path = raw.split(/[?#]/, 1)[0] ?? raw;
	if (path === LOGIN_PATH || path.startsWith(`${LOGIN_PATH}/`)) return fallback;
	return raw;
}

/** `/login` with a safe `next`, so a cold load of /board comes back here. */
export function loginUrl(next: string): string {
	const params = new URLSearchParams({ next: safeNextPath(next) });
	return `${LOGIN_PATH}?${params.toString()}`;
}
