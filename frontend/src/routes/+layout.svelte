<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import { api } from '$lib/api';
	import { handleUnauthorized, LOGIN_PATH, startSessionKeepalive } from '$lib/auth';
	import { GITHUB_REPO } from '$lib/brands';
	import { documentNeedsLogin } from '$lib/document-gate';
	import BrandMark from '$lib/components/BrandMark.svelte';
	import NavGlyph from '$lib/components/NavGlyph.svelte';
	import { siteUrl } from '$lib/site';
	import { appVersion, appVersionShort } from '$lib/version';
	import '../app.css';

	let { children, data } = $props();

	/**
	 * Only offer to sign out where signing out means something. With the gate
	 * off every request is already the Owner, so the button would end a
	 * session that does not exist and land back on a page that redirects home.
	 */
	let signedIn = $state(false);
	/**
	 * Fail-open SSR must not paint the control chrome. Issue #18 is that
	 * leak: nav, page copy, and a `Failed to fetch` banner before /login.
	 * The server 303s when it can; this is the case it cannot (API down,
	 * Playwright's closed-port gate, a cookie the API has not answered yet).
	 */
	let clientReady = $state(false);

	const onLogin = $derived(
		page.url.pathname === LOGIN_PATH || page.url.pathname.startsWith(`${LOGIN_PATH}/`)
	);
	const serverAllows = $derived(
		data.auth != null && !documentNeedsLogin(data.auth, page.url.pathname)
	);
	const showApp = $derived(onLogin || serverAllows || clientReady);

	$effect(() => {
		const auth = data.auth;
		if (auth) signedIn = auth.enabled && auth.authenticated;
	});

	onMount(async () => {
		if (data.auth) {
			if (documentNeedsLogin(data.auth, page.url.pathname)) handleUnauthorized();
			return;
		}
		try {
			const status = await api.authStatus();
			signedIn = status.enabled && status.authenticated;
			if (documentNeedsLogin(status, page.url.pathname)) {
				handleUnauthorized();
				return;
			}
			clientReady = true;
		} catch {
			clientReady = true;
		}
	});

	async function signOut() {
		try {
			await api.authLogout();
		} catch {
			/* already gone is the outcome we wanted */
		}
		signedIn = false;
		await goto(LOGIN_PATH);
	}

	// Every route is behind the same login session, and the pages people leave
	// open longest are the ones that talk over WebSockets and so never touch
	// it. Held here so the heartbeat covers the whole app rather than the
	// handful of pages that happen to make requests.
	$effect(() => startSessionKeepalive());

	const SITE_NAME = 'MFTIK Control';
	const SITE_DESCRIPTION =
		'Control plane for the Mid-Frequency Algo Trading platform — strategy sessions, alerts, API keys, and audit.';
	// Absolute, and not written down here — see `$lib/site`.
	const siteOrigin = $derived(siteUrl(page.url.origin));
	const ogImage = $derived(`${siteOrigin}/og-image.png`);

	const nav = [
		{ href: '/', label: 'Home' },
		{ href: '/board', label: 'Board' },
		{ href: '/keys', label: 'API Key' },
		{ href: '/strategy', label: 'Strategy' },
		{ href: '/alerts', label: 'Alert' },
		{ href: '/registry', label: 'Registry' },
		{ href: '/sym', label: 'Symbol' },
		{ href: '/audit', label: 'Audit' },
		{ href: '/settings', label: 'Settings' }
	] as const;

	function sectionLabel(pathname: string): string {
		if (pathname === '/') return 'Home';
		const hit = nav.find(
			(item) => item.href !== '/' && (pathname === item.href || pathname.startsWith(`${item.href}/`))
		);
		return hit?.label ?? SITE_NAME;
	}

	const documentTitle = $derived(
		page.url.pathname === '/' ? SITE_NAME : `${sectionLabel(page.url.pathname)} · ${SITE_NAME}`
	);

	function isActive(href: string, pathname: string): boolean {
		if (href === '/') return pathname === '/';
		return pathname === href || pathname.startsWith(`${href}/`);
	}
</script>

<svelte:head>
	<title>{documentTitle}</title>
	<meta name="title" content={documentTitle} />
	<meta name="description" content={SITE_DESCRIPTION} />
	<meta name="application-name" content={SITE_NAME} />

	<meta property="og:type" content="website" />
	<meta property="og:site_name" content={SITE_NAME} />
	<meta property="og:title" content={documentTitle} />
	<meta property="og:description" content={SITE_DESCRIPTION} />
	<meta property="og:url" content={`${siteOrigin}${page.url.pathname}`} />
	<meta property="og:image" content={ogImage} />
	<meta property="og:image:alt" content="MFTIK logo" />

	<meta name="twitter:card" content="summary" />
	<meta name="twitter:title" content={documentTitle} />
	<meta name="twitter:description" content={SITE_DESCRIPTION} />
	<meta name="twitter:image" content={ogImage} />
</svelte:head>

{#if showApp}
<div class="shell">
	<aside class="nav">
		<a class="brand" href="/">
			<span class="mark">MFTIK</span>
			<span class="tag">control</span>
		</a>
		<nav>
			{#each nav as item}
				<a
					href={item.href}
					class:active={isActive(item.href, page.url.pathname)}
					data-sveltekit-preload-data="hover"
				>
					<NavGlyph href={item.href} />
					{item.label}
				</a>
			{/each}
		</nav>

		<div class="foot">
			{#if signedIn && page.url.pathname !== LOGIN_PATH}
				<button type="button" class="signout" onclick={signOut}>Sign out</button>
			{/if}
			<div class="meta">
				<a
					class="social"
					href={GITHUB_REPO}
					target="_blank"
					rel="noreferrer"
					aria-label="GitHub"
					title="GitHub"
				>
					<BrandMark name="github" size={16} />
				</a>
				<span class="version" title={`build ${appVersion()}`}>{appVersionShort()}</span>
			</div>
		</div>
	</aside>

	<main class="content">
		{@render children()}
	</main>
</div>
{/if}

<style>
	.shell {
		display: grid;
		grid-template-columns: var(--nav-width) 1fr;
		min-height: 100vh;
	}

	.nav {
		position: sticky;
		top: 0;
		align-self: start;
		height: 100vh;
		padding: 1.4rem 1rem;
		border-right: 1px solid var(--border);
		background: linear-gradient(180deg, rgba(14, 20, 28, 0.96), rgba(10, 14, 20, 0.92));
		display: flex;
		flex-direction: column;
		gap: 1.75rem;
	}

	.brand {
		display: flex;
		align-items: baseline;
		gap: 0.55rem;
		text-decoration: none;
		color: inherit;
	}

	.mark {
		font-family: var(--font);
		font-size: 1.55rem;
		font-weight: 500;
		letter-spacing: 0.14em;
	}

	.tag {
		font-size: 0.7rem;
		color: var(--muted);
		text-transform: uppercase;
		letter-spacing: 0.12em;
	}

	nav {
		display: grid;
		gap: 0.25rem;
	}

	nav a {
		display: flex;
		align-items: center;
		gap: 0.55rem;
		padding: 0.55rem 0.7rem;
		border-radius: var(--radius);
		color: var(--muted);
		text-decoration: none;
		font-weight: 500;
		letter-spacing: 0.02em;
		transition:
			background 160ms ease,
			color 160ms ease,
			transform 160ms ease;
	}

	nav a:hover {
		color: var(--text);
		background: rgba(255, 255, 255, 0.03);
		text-decoration: none;
	}

	nav a.active {
		color: var(--text);
		background: var(--accent-dim);
		transform: translateX(2px);
	}

	/* Pushed to the bottom of the sidebar — visible on every page, out of the
	   way of the content column. */
	.foot {
		margin-top: auto;
		display: grid;
		gap: 0.5rem;
		justify-items: start;
	}

	.signout {
		padding: 0.3rem 0.6rem;
		font-size: 0.72rem;
		letter-spacing: 0.04em;
		color: var(--muted);
		background: transparent;
		border: 1px solid var(--border);
		border-radius: var(--radius);
		cursor: pointer;
	}

	.signout:hover {
		color: var(--text);
		border-color: var(--muted);
	}

	.meta {
		display: flex;
		align-items: center;
		gap: 0.55rem;
	}

	.social {
		display: inline-flex;
		align-items: center;
		color: var(--muted);
		opacity: 0.7;
		line-height: 0;
	}

	.social:hover {
		color: var(--text);
		opacity: 1;
		text-decoration: none;
	}

	.version {
		font-family: var(--font);
		font-size: 0.68rem;
		letter-spacing: 0.04em;
		color: var(--muted);
		opacity: 0.65;
		user-select: text;
	}

	.content {
		padding: 1.75rem 1.75rem 2.5rem;
		min-width: 0;
	}

	@media (max-width: 800px) {
		.shell {
			grid-template-columns: 1fr;
		}

		.nav {
			position: static;
			height: auto;
			border-right: none;
			border-bottom: 1px solid var(--border);
			padding: 1rem;
			gap: 1rem;
		}

		nav {
			grid-auto-flow: column;
			grid-auto-columns: max-content;
			overflow-x: auto;
		}

		.content {
			padding: 1.25rem 1rem 2rem;
		}
	}
</style>
