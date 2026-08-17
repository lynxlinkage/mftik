import { env } from '$env/dynamic/public';

/**
 * The build this frontend container came from.
 *
 * Production pins images by `MFTIK_VERSION` (CI writes `sha-<commit>` into the
 * host's .env), and deploy/docker-compose.yml hands that same value to the
 * frontend as PUBLIC_APP_VERSION. Read through `$env/dynamic/public` rather
 * than `import.meta.env`, and dynamically rather than statically: one image is
 * built once and told at run time which deployment it is, so a build-time
 * substitution would be stale — and Vite only exposes `VITE_`-prefixed names
 * to `import.meta.env` anyway, so that read would silently be undefined.
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
