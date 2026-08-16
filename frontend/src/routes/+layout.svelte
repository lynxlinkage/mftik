<script lang="ts">
	import { page } from '$app/state';
	import { startSessionKeepalive } from '$lib/auth';
	import { appVersion, appVersionShort } from '$lib/version';
	import '../app.css';

	let { children } = $props();

	// Every route is behind the same login session, and the pages people leave
	// open longest are the ones that talk over WebSockets and so never touch
	// it. Held here so the heartbeat covers the whole app rather than the
	// handful of pages that happen to make requests.
	$effect(() => startSessionKeepalive());

	const SITE_NAME = 'MFT Control';
	const SITE_DESCRIPTION =
		'Control plane for the Mid-Frequency Algo Trading platform — STS, TD, MD sessions, APIs, and audit.';
	const SITE_URL = 'https://mft.lynkora.com';
	const OG_IMAGE = `${SITE_URL}/og-image.png`;

	const nav = [
		{ href: '/', label: 'Home' },
		{ href: '/board', label: 'Board' },
		{ href: '/apis', label: 'APIs' },
		{ href: '/sts', label: 'STS' },
		{ href: '/td', label: 'TD' },
		{ href: '/md', label: 'MD' },
		{ href: '/registry', label: 'Registry' },
		{ href: '/sym', label: 'Sym' },
		{ href: '/audit', label: 'Audit' }
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
	<meta property="og:url" content={`${SITE_URL}${page.url.pathname}`} />
	<meta property="og:image" content={OG_IMAGE} />
	<meta property="og:image:alt" content="MFT logo" />

	<meta name="twitter:card" content="summary" />
	<meta name="twitter:title" content={documentTitle} />
	<meta name="twitter:description" content={SITE_DESCRIPTION} />
	<meta name="twitter:image" content={OG_IMAGE} />
</svelte:head>

<div class="shell">
	<aside class="nav">
		<a class="brand" href="/">
			<span class="mark">MFT</span>
			<span class="tag">control</span>
		</a>
		<nav>
			{#each nav as item}
				<a
					href={item.href}
					class:active={isActive(item.href, page.url.pathname)}
					data-sveltekit-preload-data="hover"
				>
					{item.label}
				</a>
			{/each}
		</nav>

		<span class="version" title={`build ${appVersion()}`}>{appVersionShort()}</span>
	</aside>

	<main class="content">
		{@render children()}
	</main>
</div>

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
		display: block;
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
	.version {
		margin-top: auto;
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
