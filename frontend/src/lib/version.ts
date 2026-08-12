import { env } from '$env/dynamic/public';

/**
 * The build this frontend container came from.
 *
 * Production pins images by `MFT_VERSION` (CI writes `sha-<commit>` into the
 * host's .env), and deploy/docker-compose.yml hands that same value to the
 * frontend as PUBLIC_APP_VERSION. Read dynamically for the same reason as
 * PUBLIC_API_URL in ws.ts: one image is built once and told at run time which
 * deployment it is, so a build-time substitution would be stale.
 */
export function appVersion(): string {
	return env.PUBLIC_APP_VERSION || 'dev';
}

/** Short form for the corner badge: `sha-<40 hex>` reads better as `bde571a`. */
export function appVersionShort(): string {
	const full = appVersion();
	const bare = full.startsWith('sha-') ? full.slice(4) : full;
	return /^[0-9a-f]{8,40}$/.test(bare) ? bare.slice(0, 7) : bare;
}
