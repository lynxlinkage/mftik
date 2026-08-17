import { env } from '$env/dynamic/public';

/**
 * Absolute origin this deployment is reached at, for the metadata that cannot
 * be relative: `og:url` and `og:image` are read by crawlers that have no page
 * to resolve a path against.
 *
 * Set through PUBLIC_SITE_URL, read the same way as PUBLIC_APP_VERSION and for
 * the same reason (see `version.ts`): one image is built once and told at run
 * time which deployment it is.
 *
 * The fallback is the origin the request actually arrived on — correct in the
 * browser always, and correct under adapter-node whenever ORIGIN is set, which
 * deploy/docker-compose.yml does. So the variable is an override for the case
 * the origin lies: a proxy, or a canonical domain that differs from the one
 * being served.
 */
export function siteUrl(requestOrigin: string): string {
	return (env.PUBLIC_SITE_URL || requestOrigin).replace(/\/+$/, '');
}
