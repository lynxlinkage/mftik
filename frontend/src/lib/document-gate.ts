import { LOGIN_PATH } from './login-path';

export type DocumentAuth = {
	enabled: boolean;
	authenticated: boolean;
};

/** Same values the API accepts for `MFTIK_AUTH_ENABLED`. */
export function authEnvEnabled(value: string | undefined): boolean {
	return ['1', 'true', 'yes'].includes((value ?? '').trim().toLowerCase());
}

/**
 * Whether this document should 303 to /login rather than render.
 *
 * The API still answers 401 and never redirects — it does not see a
 * navigation to /board. The frontend container does, and that is the only
 * place a document redirect can live. See docs/Auth.md.
 */
export function documentNeedsLogin(status: DocumentAuth, pathname: string): boolean {
	if (pathname === LOGIN_PATH || pathname.startsWith(`${LOGIN_PATH}/`)) return false;
	return status.enabled && !status.authenticated;
}
